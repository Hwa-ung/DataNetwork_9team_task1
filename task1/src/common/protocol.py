import json
import struct
import dataclasses


def send_msg(sock, obj: dict, lock=None) -> None:
    raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    data = struct.pack(">I", len(raw)) + raw
    if lock:
        with lock:
            sock.sendall(data)
    else:
        sock.sendall(data)


def _recv_exact(sock, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def recv_msg(sock):
    head = _recv_exact(sock, 4)
    if head is None:
        return None
    (length,) = struct.unpack(">I", head)
    body = _recv_exact(sock, length)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


_task_seq = 0


def _next_seq():
    global _task_seq
    _task_seq += 1
    return _task_seq


@dataclasses.dataclass
class Task:
    task_id: str
    key: str
    value: int
    priority: bool = False
    retry: int = 0
    enqueue_clock: float = 0.0
    seq: int = dataclasses.field(default_factory=_next_seq)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "key": self.key,
            "value": self.value,
            "priority": self.priority,
            "retry": self.retry,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        return cls(
            task_id=d["task_id"],
            key=d["key"],
            value=d["value"],
            priority=d.get("priority", False),
            retry=d.get("retry", 0),
        )
