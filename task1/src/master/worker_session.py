import threading
from common.protocol import send_msg, recv_msg


class WorkerStat:
    def __init__(self):
        self.dispatched = 0
        self.success = 0
        self.fail = 0
        self.reject = 0
        self.total_wait_time = 0.0
        self.total_proc_time = 0.0


class WorkerSession:
    def __init__(self, worker_id, sock, p2p_port, clock, logger):
        self.worker_id = worker_id
        self.sock = sock
        self.p2p_port = p2p_port
        self.clock = clock
        self.logger = logger
        self.send_lock = threading.Lock()

        self.in_flight = 0
        self.last_known_queue = 0
        self.stats = WorkerStat()

        self.bye_event = threading.Event()
        self._receiver = None

    @property
    def estimated_queue(self):
        return max(self.last_known_queue, self.in_flight)

    def dispatch(self, task):
        msg = {"type": "TASK", "clock": self.clock.stamp_send(), **task.to_dict()}
        send_msg(self.sock, msg, self.send_lock)
        self.in_flight += 1
        self.stats.dispatched += 1

    def on_result(self, queue_size):
        self.in_flight = max(0, self.in_flight - 1)
        self.last_known_queue = queue_size

    def adjust_queue(self, delta):
        self.last_known_queue = max(0, self.last_known_queue + delta)

    def adjust_in_flight(self, delta):
        self.in_flight = max(0, self.in_flight + delta)

    def send(self, msg):
        send_msg(self.sock, msg, self.send_lock)

    def start_receiver(self, result_queue, result_event):
        self._receiver = threading.Thread(
            target=self._recv_loop,
            args=(result_queue, result_event),
            daemon=True,
        )
        self._receiver.start()

    def _recv_loop(self, result_queue, result_event):
        while True:
            try:
                msg = recv_msg(self.sock)
            except Exception:
                break
            if msg is None:
                break
            self.clock.sync_recv(msg["clock"])
            if msg["type"] == "BYE":
                self.bye_event.set()
                break
            result_queue.put(msg)
            result_event.set()

    def wait_bye(self, timeout=10):
        self.bye_event.wait(timeout)

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass
