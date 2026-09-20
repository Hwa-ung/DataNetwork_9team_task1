import threading
from .constants import NET_LATENCY, P2P_LATENCY


class VirtualClock:
    def __init__(self, start: float = 0.0):
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, delta: float) -> float:
        with self._lock:
            self._t += delta
            return self._t

    def stamp_send(self) -> float:
        with self._lock:
            return self._t

    def sync_recv(self, sender_clock: float) -> float:
        with self._lock:
            self._t = max(self._t, sender_clock + NET_LATENCY)
            return self._t

    def sync_recv_local(self, sender_clock: float) -> float:
        with self._lock:
            self._t = max(self._t, sender_clock + P2P_LATENCY)
            return self._t
