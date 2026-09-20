import socket
import threading
from common.protocol import send_msg, recv_msg, Task
from common.constants import QUEUE_MAX


class P2PServer(threading.Thread):
    def __init__(self, port, ready_queue, clock, logger, worker_id,
                 terminate_event, stats):
        super().__init__(daemon=True)
        self.port = port
        self.ready_queue = ready_queue
        self.clock = clock
        self.logger = logger
        self.worker_id = worker_id
        self.terminate_event = terminate_event
        self.stats = stats
        self.server_sock = None

    def run(self):
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_sock.bind(("0.0.0.0", self.port))
            self.server_sock.listen(10)
            self.server_sock.settimeout(0.5)
            self.logger.info("INIT", f"P2P server listening on 0.0.0.0:{self.port}")
        except Exception as e:
            self.logger.fail("INIT", f"P2P server bind failed on port {self.port}: {e}")
            return

        while not self.terminate_event.is_set():
            try:
                conn, _ = self.server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            # Handle each connection in a separate thread to avoid blocking
            threading.Thread(
                target=self._handle_conn, args=(conn,), daemon=True
            ).start()

        try:
            self.server_sock.close()
        except Exception:
            pass

    def _handle_conn(self, conn):
        try:
            conn.settimeout(5.0)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._process_p2p(conn)
        except Exception as e:
            self.logger.fail("LB", f"P2P handle error: {e}")
        finally:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _process_p2p(self, conn):
        msg = recv_msg(conn)
        if msg is None:
            return
        self.clock.sync_recv_local(msg["clock"])

        if msg["type"] == "P2P_QUERY":
            resp = {
                "type": "P2P_QUERY_RES",
                "worker_id": self.worker_id,
                "queue_size": len(self.ready_queue),
                "clock": self.clock.stamp_send(),
            }
            send_msg(conn, resp)
            self.logger.info(
                "LB",
                f"P2P query from Worker{msg['from_worker']}, replied queue_size={resp['queue_size']}"
            )

        elif msg["type"] == "P2P_TRANSFER":
            accepted = []
            rejected = []
            for td in msg["tasks"]:
                task = Task.from_dict(td)
                task.enqueue_clock = self.clock.now()
                if self.ready_queue.push(task):
                    accepted.append(task.task_id)
                else:
                    rejected.append(task.task_id)
            ack = {
                "type": "P2P_ACK",
                "worker_id": self.worker_id,
                "accepted": accepted,
                "rejected": rejected,
                "clock": self.clock.stamp_send(),
            }
            send_msg(conn, ack)
            self.stats.p2p_received += len(accepted)
            self.logger.info(
                "LB",
                f"P2P transfer from Worker{msg['from_worker']}: "
                f"accepted={len(accepted)} rejected={len(rejected)}"
            )

    def stop(self):
        try:
            self.server_sock.close()
        except Exception:
            pass


def _make_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    return sock


def query_peer(peer_port, worker_id, clock, logger=None):
    sock = _make_socket()
    try:
        sock.connect(("127.0.0.1", peer_port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send_msg(sock, {
            "type": "P2P_QUERY",
            "from_worker": worker_id,
            "clock": clock.stamp_send(),
        })
        resp = recv_msg(sock)
        if resp:
            clock.sync_recv_local(resp["clock"])
            return resp.get("queue_size")
        return None
    except Exception as e:
        if logger:
            logger.info("LB", f"P2P query to port {peer_port} failed: {e}")
        return None
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        sock.close()


def transfer_tasks(peer_port, tasks, worker_id, clock, logger=None):
    sock = _make_socket()
    try:
        sock.connect(("127.0.0.1", peer_port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send_msg(sock, {
            "type": "P2P_TRANSFER",
            "from_worker": worker_id,
            "tasks": [t.to_dict() for t in tasks],
            "clock": clock.stamp_send(),
        })
        ack = recv_msg(sock)
        if ack:
            clock.sync_recv_local(ack["clock"])
            return ack.get("accepted", []), ack.get("rejected", [])
        return [], [t.task_id for t in tasks]
    except Exception as e:
        if logger:
            logger.info("LB", f"P2P transfer to port {peer_port} failed: {e}")
        return [], [t.task_id for t in tasks]
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        sock.close()
