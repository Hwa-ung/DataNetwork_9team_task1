import random
import threading
from common.protocol import Task
from common.constants import NUM_KV, KEY_SPACE, VALUE_MIN, VALUE_MAX


def generate_kv_pairs(n: int = NUM_KV) -> list:
    keys = random.sample(range(KEY_SPACE), n)
    return [
        Task(task_id=f"{i+1:04d}", key=f"{k:04x}", value=random.randint(VALUE_MIN, VALUE_MAX))
        for i, k in enumerate(keys)
    ]


class KVStore:
    def __init__(self):
        self._store = {}
        self._lock = threading.Lock()

    def commit(self, key: str, value: int, worker_id: int):
        with self._lock:
            self._store[key] = (value, worker_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._store
