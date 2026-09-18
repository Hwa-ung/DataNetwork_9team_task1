import heapq
import threading


class PriorityRequeue:
    def __init__(self):
        self._heap = []
        self._lock = threading.Lock()
        self.total_requeues = 0

    def push(self, task):
        task.retry += 1
        task.priority = True
        with self._lock:
            heapq.heappush(self._heap, (-task.retry, task.seq, task))
            self.total_requeues += 1

    def pop(self):
        with self._lock:
            if self._heap:
                return heapq.heappop(self._heap)[2]
            return None

    def __len__(self):
        with self._lock:
            return len(self._heap)
