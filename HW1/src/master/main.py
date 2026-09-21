import argparse
import json
import os
import socket
import queue
import threading

from common.constants import NUM_KV, NUM_WORKERS
from common.clock import VirtualClock
from common.logger import NodeLogger
from common.protocol import recv_msg

from master.kv_store import generate_kv_pairs, KVStore
from master.worker_session import WorkerSession
from master.scheduler import Scheduler
from master.fault_handler import PriorityRequeue
from master.stats import print_master_stats


def load_config(args):
    config = {}
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
    if args.port is not None:
        config["master_port"] = args.port
    if args.num_kv is not None:
        config["num_kv"] = args.num_kv
    if args.log_dir is not None:
        config["log_dir"] = args.log_dir
    config.setdefault("master_port", 9000)
    config.setdefault("num_kv", NUM_KV)
    config.setdefault("log_dir", "./logs")
    return config


def main():
    parser = argparse.ArgumentParser(description="Master Node")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--num-kv", type=int, default=None)
    parser.add_argument("--log-dir", type=str, default=None)
    parser.add_argument("--lb-demo", action="store_true",
                        help="Bias initial tasks to Worker1 to trigger P2P LB")
    args = parser.parse_args()

    config = load_config(args)
    port = config["master_port"]
    num_kv = config["num_kv"]
    log_dir = config["log_dir"]

    os.makedirs(log_dir, exist_ok=True)

    clock = VirtualClock()
    logger = NodeLogger("MASTER", clock, os.path.join(log_dir, "Master.txt"))

    logger.info("INIT", f"System Clock started at 0.00")
    logger.info("INIT", f"Generating {num_kv} KV pairs...")
    kv_pairs = generate_kv_pairs(num_kv)
    kv_store = KVStore()
    logger.success("INIT", f"Generated {num_kv} KV pairs (keys: hex 4-digit, values: 1~100)")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", port))
    server_sock.listen(NUM_WORKERS)
    logger.info("INIT", f"Listening on 0.0.0.0:{port}, waiting for {NUM_WORKERS} workers...")

    sessions = []
    for _ in range(NUM_WORKERS):
        conn, addr = server_sock.accept()
        hello = recv_msg(conn)
        clock.sync_recv(hello["clock"])
        wid = hello["worker_id"]
        p2p_port = hello["p2p_port"]
        session = WorkerSession(wid, conn, p2p_port, clock, logger)
        sessions.append(session)
        logger.success("CONNECT", f"Worker{wid} connected from {addr[0]}:{addr[1]}, P2P port {p2p_port}")

    sessions.sort(key=lambda s: s.worker_id)

    peers = [{"worker_id": s.worker_id, "p2p_port": s.p2p_port} for s in sessions]
    for s in sessions:
        s.send({"type": "PEERS", "peers": peers, "clock": clock.stamp_send()})

    for s in sessions:
        s.send({"type": "START", "clock": clock.stamp_send()})

    logger.success("INIT", f"All {NUM_WORKERS} workers connected. Starting task distribution.")

    result_queue = queue.Queue()
    result_event = threading.Event()
    for s in sessions:
        s.start_receiver(result_queue, result_event)

    fault_handler = PriorityRequeue()
    scheduler = Scheduler(
        sessions, kv_pairs, kv_store, fault_handler,
        result_queue, result_event, clock, logger, num_kv,
        lb_demo=args.lb_demo
    )
    scheduler.run()

    print_master_stats(sessions, kv_store, fault_handler, scheduler, clock, logger)

    for s in sessions:
        s.send({"type": "TERMINATE", "clock": clock.stamp_send()})
    logger.raw("Sent TERMINATE to all workers")

    for s in sessions:
        s.wait_bye(timeout=10)

    logger.raw("All workers terminated. Master shutting down.")

    for s in sessions:
        s.close()
    server_sock.close()
    logger.close()


if __name__ == "__main__":
    main()
