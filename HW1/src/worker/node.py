import os
import random
import socket
import threading
import time

from common.constants import (
    QUEUE_MAX, AVG_PROC_TIME, LB_THRESHOLD,
    LB_CHECK_MIN, LB_CHECK_MAX, NET_LATENCY,
)
from common.clock import VirtualClock
from common.logger import NodeLogger
from common.protocol import send_msg, recv_msg, Task
from worker.ready_queue import ReadyQueue
from worker.processor import process
from worker.p2p import P2PServer, query_peer, transfer_tasks
from worker.stats import WorkerStats


class MasterReceiver(threading.Thread):
    def __init__(self, sock, send_lock, ready_queue, clock, logger,
                 terminate_event, stats):
        super().__init__(daemon=True)
        self.sock = sock
        self.send_lock = send_lock
        self.ready_queue = ready_queue
        self.clock = clock
        self.logger = logger
        self.terminate_event = terminate_event
        self.stats = stats

    def run(self):
        while not self.terminate_event.is_set():
            try:
                msg = recv_msg(self.sock)
            except socket.timeout:
                continue
            except Exception:
                break
            if msg is None:
                break

            self.clock.sync_recv(msg["clock"])

            if msg["type"] == "TASK":
                task = Task.from_dict(msg)
                task.enqueue_clock = self.clock.now()
                self.stats.received += 1

                if not self.ready_queue.push(task):
                    self.logger.fail(
                        "QUEUE",
                        f"Queue full ({QUEUE_MAX}/{QUEUE_MAX}). Task {task.task_id} REJECTED"
                    )
                    reject = {
                        "type": "RESULT",
                        "task_id": task.task_id,
                        "status": "REJECT",
                        "worker_id": self.stats._worker_id,
                        "proc_time": 0,
                        "wait_time": 0,
                        "queue_size": len(self.ready_queue),
                        "clock": self.clock.stamp_send(),
                    }
                    send_msg(self.sock, reject, self.send_lock)
                    self.stats.reject += 1
                else:
                    self.logger.info(
                        "RECV",
                        f"Task {task.task_id} received (key={task.key}, queue:{len(self.ready_queue)}/{QUEUE_MAX})"
                        f"{' [RETRY #' + str(task.retry) + ']' if task.retry > 0 else ''}"
                    )

            elif msg["type"] == "TERMINATE":
                self.logger.raw("Received TERMINATE from Master")
                self.terminate_event.set()
                self.ready_queue.wakeup()
                break


class WorkerNode(threading.Thread):
    def __init__(self, worker_id, master_ip, master_port, p2p_port, log_dir):
        super().__init__(daemon=False)
        self.worker_id = worker_id
        self.master_ip = master_ip
        self.master_port = master_port
        self.p2p_port = p2p_port
        self.log_dir = log_dir

        self.clock = VirtualClock()
        self.logger = None
        self.sock = None
        self.send_lock = threading.Lock()
        self.terminate_event = threading.Event()
        self.stats = WorkerStats()
        self.stats._worker_id = worker_id
        self.peers = []
        self.ready_queue = None

    def run(self):
        node_name = f"WORKER{self.worker_id}"
        self.logger = NodeLogger(
            node_name, self.clock,
            os.path.join(self.log_dir, f"Worker{self.worker_id}.txt")
        )
        self.ready_queue = ReadyQueue(self.worker_id, self.logger, self.clock)

        self.logger.info("INIT", f"Worker{self.worker_id} starting (P2P port {self.p2p_port})")

        # Start P2P server early
        p2p_server = P2PServer(
            self.p2p_port, self.ready_queue, self.clock, self.logger,
            self.worker_id, self.terminate_event, self.stats
        )
        p2p_server.start()

        # Connect to master
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.master_ip, self.master_port))
            self.sock.settimeout(1.0)
        except Exception as e:
            self.logger.fail("CONNECT", f"Failed to connect to Master: {e}")
            p2p_server.stop()
            self.logger.close()
            return

        self.logger.success("CONNECT", f"Connected to Master at {self.master_ip}:{self.master_port}")

        # HELLO
        send_msg(self.sock, {
            "type": "HELLO",
            "worker_id": self.worker_id,
            "p2p_port": self.p2p_port,
            "clock": self.clock.stamp_send(),
        }, self.send_lock)

        # Receive PEERS
        peers_msg = self._blocking_recv()
        if peers_msg and peers_msg["type"] == "PEERS":
            self.clock.sync_recv(peers_msg["clock"])
            self.peers = [p for p in peers_msg["peers"] if p["worker_id"] != self.worker_id]
            self.logger.info("CONNECT", f"Received peer list: {len(self.peers)} peers")

        # Receive START
        start_msg = self._blocking_recv()
        if start_msg and start_msg["type"] == "START":
            self.clock.sync_recv(start_msg["clock"])
            self.logger.success("INIT", "Received START signal. Beginning processing.")

        # Start receiver
        receiver = MasterReceiver(
            self.sock, self.send_lock, self.ready_queue,
            self.clock, self.logger, self.terminate_event, self.stats
        )
        receiver.start()

        # Main processing loop
        next_lb_check = self.clock.now() + random.uniform(LB_CHECK_MIN, LB_CHECK_MAX)

        while not self.terminate_event.is_set():
            task = self.ready_queue.pop(timeout=0.05)
            if task is None:
                continue

            wait_time = self.clock.now() - task.enqueue_clock
            success, proc_time = process(task, self.clock)
            self.stats.processed += 1
            self.stats.total_wait_time += wait_time
            self.stats.total_proc_time += proc_time

            if success:
                self.stats.success += 1
                self.logger.success(
                    "PROC",
                    f"Task {task.task_id} SUCCESS (proc={proc_time:.2f}s, wait={wait_time:.2f}s)"
                )
                status = "SUCCESS"
            else:
                self.stats.fail += 1
                self.logger.fail(
                    "PROC",
                    f"Task {task.task_id} FAIL (proc={proc_time:.2f}s, wait={wait_time:.2f}s)"
                )
                status = "FAIL"

            result = {
                "type": "RESULT",
                "task_id": task.task_id,
                "status": status,
                "worker_id": self.worker_id,
                "proc_time": round(proc_time, 4),
                "wait_time": round(wait_time, 4),
                "queue_size": len(self.ready_queue),
                "clock": self.clock.stamp_send(),
            }
            send_msg(self.sock, result, self.send_lock)

            # Brief yield to let MasterReceiver thread enqueue incoming tasks
            time.sleep(0.005)

            # P2P load balancing check
            if self.clock.now() >= next_lb_check:
                self._maybe_balance()
                next_lb_check = self.clock.now() + random.uniform(LB_CHECK_MIN, LB_CHECK_MAX)

        # Graceful shutdown
        self._print_stats()

        bye = {
            "type": "BYE",
            "worker_id": self.worker_id,
            "stats": self.stats.to_dict(),
            "clock": self.clock.stamp_send(),
        }
        try:
            send_msg(self.sock, bye, self.send_lock)
        except Exception:
            pass

        self.logger.raw(f"Worker{self.worker_id} terminated gracefully.")

        p2p_server.stop()
        try:
            self.sock.close()
        except Exception:
            pass
        self.logger.close()

    def _blocking_recv(self):
        old_timeout = self.sock.gettimeout()
        self.sock.settimeout(30.0)
        try:
            return recv_msg(self.sock)
        except Exception:
            return None
        finally:
            self.sock.settimeout(old_timeout)

    def _maybe_balance(self):
        qsize = len(self.ready_queue)
        expected_wait = qsize * AVG_PROC_TIME
        if expected_wait <= LB_THRESHOLD:
            return
        if not self.peers:
            return

        self.logger.info("LB", f"Expected wait {expected_wait:.1f}s > {LB_THRESHOLD}s, querying peers...")
        self.stats.p2p_queries += 1

        sizes = {}
        for peer in self.peers:
            result = query_peer(peer["p2p_port"], self.worker_id, self.clock, self.logger)
            if result is not None:
                sizes[peer["worker_id"]] = (result, peer["p2p_port"])

        if not sizes:
            self.logger.info("LB", "No peers responded")
            return

        target_id, (tsize, tport) = min(sizes.items(), key=lambda kv: kv[1][0])
        my_size = len(self.ready_queue)
        diff = my_size - tsize
        if diff <= 0:
            self.logger.info("LB", f"No transfer needed (my={my_size}, target Worker{target_id}={tsize})")
            return
        k = min(max(1, diff // 2), QUEUE_MAX - tsize)
        if k <= 0:
            self.logger.info("LB", f"No transfer needed (target Worker{target_id} full)")
            return

        tasks = self.ready_queue.pop_tail(k)
        if not tasks:
            return

        self.logger.info("LB", f"Transferring {len(tasks)} task(s) to Worker{target_id}")

        accepted, rejected = transfer_tasks(tport, tasks, self.worker_id, self.clock)
        accepted_set = set(accepted)

        # Put back rejected tasks (force_push to avoid losing them)
        for t in tasks:
            if t.task_id not in accepted_set:
                t.enqueue_clock = self.clock.now()
                self.ready_queue.force_push(t)

        if accepted:
            self.stats.p2p_sent += len(accepted)
            # Notify master
            moved = {
                "type": "P2P_MOVED",
                "task_ids": accepted,
                "from_worker": self.worker_id,
                "to_worker": target_id,
                "clock": self.clock.stamp_send(),
            }
            try:
                send_msg(self.sock, moved, self.send_lock)
            except Exception:
                pass

            self.logger.success(
                "LB",
                f"P2P transfer done: {len(accepted)} to Worker{target_id}, {len(rejected)} rejected"
            )

    def _print_stats(self):
        s = self.stats
        SEP = "=" * 90
        DIV = "-" * 90
        self.logger.raw(SEP)
        self.logger.raw(f"Worker{self.worker_id} FINAL STATISTICS")
        self.logger.raw(SEP)
        self.logger.raw(f"[1] Tasks processed          : {s.processed}")
        self.logger.raw(f"[2] SUCCESS                  : {s.success}")
        self.logger.raw(f"    FAIL                     : {s.fail}")
        self.logger.raw(f"    REJECT                   : {s.reject}")
        self.logger.raw(f"[3] Avg wait time            : {s.avg_wait_time:.2f} sec")
        self.logger.raw(DIV)
        self.logger.raw(f"[4] P2P sent                 : {s.p2p_sent}")
        self.logger.raw(f"    P2P received             : {s.p2p_received}")
        self.logger.raw(f"    P2P queries              : {s.p2p_queries}")
        self.logger.raw(f"[5] Tasks received           : {s.received}")
        self.logger.raw(f"[6] Total elapsed time       : {self.clock.now():.2f} sec (System Clock)")
        self.logger.raw(SEP)
