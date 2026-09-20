import argparse
import json
import os

from common.constants import NUM_WORKERS, P2P_PORT_BASE
from worker.node import WorkerNode


def load_config(args):
    config = {}
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
    if args.master_ip is not None:
        config["master_ip"] = args.master_ip
    if args.master_port is not None:
        config["master_port"] = args.master_port
    if args.num_workers is not None:
        config["num_workers"] = args.num_workers
    if args.p2p_port_base is not None:
        config["p2p_port_base"] = args.p2p_port_base
    if args.log_dir is not None:
        config["log_dir"] = args.log_dir
    config.setdefault("master_ip", "127.0.0.1")
    config.setdefault("master_port", 9000)
    config.setdefault("num_workers", NUM_WORKERS)
    config.setdefault("p2p_port_base", P2P_PORT_BASE)
    config.setdefault("log_dir", "./logs")
    return config


def main():
    parser = argparse.ArgumentParser(description="Worker Nodes (4 Threads)")
    parser.add_argument("--master-ip", type=str, default=None)
    parser.add_argument("--master-port", type=int, default=None)
    parser.add_argument("--num-workers", type=int, default=None)
    parser.add_argument("--p2p-port-base", type=int, default=None)
    parser.add_argument("--log-dir", type=str, default=None)
    args = parser.parse_args()

    config = load_config(args)
    master_ip = config["master_ip"]
    master_port = config["master_port"]
    num_workers = config["num_workers"]
    p2p_port_base = config["p2p_port_base"]
    log_dir = config["log_dir"]

    os.makedirs(log_dir, exist_ok=True)

    workers = []
    for i in range(1, num_workers + 1):
        w = WorkerNode(
            worker_id=i,
            master_ip=master_ip,
            master_port=master_port,
            p2p_port=p2p_port_base + i,
            log_dir=log_dir,
        )
        workers.append(w)

    print(f"Starting {num_workers} worker threads -> Master {master_ip}:{master_port}")
    for w in workers:
        w.start()

    for w in workers:
        w.join()

    print("All worker threads finished.")


if __name__ == "__main__":
    main()
