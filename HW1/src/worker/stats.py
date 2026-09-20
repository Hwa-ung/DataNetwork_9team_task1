class WorkerStats:
    def __init__(self):
        self.received = 0
        self.processed = 0
        self.success = 0
        self.fail = 0
        self.reject = 0
        self.total_wait_time = 0.0
        self.total_proc_time = 0.0
        self.p2p_sent = 0
        self.p2p_received = 0
        self.p2p_queries = 0

    @property
    def avg_wait_time(self):
        return self.total_wait_time / self.processed if self.processed > 0 else 0.0

    def to_dict(self):
        return {
            "received": self.received,
            "processed": self.processed,
            "success": self.success,
            "fail": self.fail,
            "reject": self.reject,
            "avg_wait_time": round(self.avg_wait_time, 2),
            "p2p_sent": self.p2p_sent,
            "p2p_received": self.p2p_received,
        }
