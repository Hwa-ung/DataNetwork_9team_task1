import threading
import os
from .clock import VirtualClock


class NodeLogger:
    def __init__(self, node_name: str, clock: VirtualClock, path: str):
        self._node = node_name
        self._clock = clock
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        self._fp = open(path, "w", encoding="utf-8")
        self._lock = threading.Lock()

    def log(self, event: str, status: str, message: str, clock_val: float = None):
        c = clock_val if clock_val is not None else self._clock.now()
        line = f"[{c:>10.2f}] {self._node:<8} | {event:<9} | {status:<7} | {message}"
        with self._lock:
            print(line)
            self._fp.write(line + "\n")
            self._fp.flush()

    def info(self, event: str, msg: str):
        self.log(event, "INFO", msg)

    def success(self, event: str, msg: str):
        self.log(event, "SUCCESS", msg)

    def fail(self, event: str, msg: str):
        self.log(event, "FAIL", msg)

    def warn(self, event: str, msg: str):
        self.log(event, "WARN", msg)

    def raw(self, msg: str):
        with self._lock:
            print(msg)
            self._fp.write(msg + "\n")
            self._fp.flush()

    def close(self):
        with self._lock:
            self._fp.close()
