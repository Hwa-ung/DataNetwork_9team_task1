import collections
import threading
from common.constants import QUEUE_MAX, QUEUE_WARN_OVER


class ReadyQueue:
    def __init__(self, worker_id, logger, clock):
        self._q = collections.deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._worker_id = worker_id
        self._logger = logger
        self._clock = clock

    def push(self, task) -> bool:
        with self._not_empty:
            if len(self._q) >= QUEUE_MAX:
                return False
            self._q.append(task)
            size = len(self._q)
            if size > QUEUE_WARN_OVER:
                self._logger.warn("QUEUE", f"Queue over 70%: {size}/{QUEUE_MAX} (after push)")
            self._not_empty.notify()
            return True

    def pop(self, timeout=0.05):
        with self._not_empty:
            while not self._q:
                if not self._not_empty.wait(timeout):
                    return None
            task = self._q.popleft()
            size = len(self._q)
            if size > QUEUE_WARN_OVER:
                self._logger.warn("QUEUE", f"Queue over 70%: {size}/{QUEUE_MAX} (after pop)")
            return task

    def force_push(self, task):
        """Push task ignoring QUEUE_MAX (for re-queuing rejected P2P tasks)."""
        with self._not_empty:
            self._q.appendleft(task)
            self._not_empty.notify()

    def pop_tail(self, k):
        with self._not_empty:
            result = []
            for _ in range(min(k, len(self._q))):
                task = self._q.pop()
                result.append(task)
                size = len(self._q)
                if size > QUEUE_WARN_OVER:
                    self._logger.warn("QUEUE", f"Queue over 70%: {size}/{QUEUE_MAX} (after p2p_out)")
            return result

    def __len__(self):
        with self._lock:
            return len(self._q)

    def wakeup(self):
        with self._not_empty:
            self._not_empty.notify_all()
