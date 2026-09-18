import collections
import queue
import threading
from common.constants import QUEUE_MAX, NUM_KV


class Scheduler:
    def __init__(self, sessions, tasks, kv_store, fault_handler,
                 result_queue, result_event, clock, logger, num_kv=NUM_KV,
                 lb_demo=False):
        self.sessions = sessions
        self.session_map = {s.worker_id: s for s in sessions}
        self.pending = collections.deque(tasks)
        self.kv_store = kv_store
        self.fault_handler = fault_handler
        self.result_queue = result_queue
        self.result_event = result_event
        self.clock = clock
        self.logger = logger
        self.num_kv = num_kv

        self.in_progress = {}
        self.total_p2p_events = 0
        self._last_progress = 0
        self._demo_remaining = 40 if lb_demo else 0

    def run(self):
        while len(self.kv_store) < self.num_kv:
            self._drain_results()

            if len(self.kv_store) >= self.num_kv:
                break

            task = self.fault_handler.pop()
            if task is None and self.pending:
                task = self.pending.popleft()

            if task is None:
                self._drain_results()
                self.result_event.wait(timeout=0.01)
                self.result_event.clear()
                continue

            target = self._pick_target()
            if target is None:
                if task.priority or task.retry > 0:
                    self.fault_handler.put_back(task)
                else:
                    self.pending.appendleft(task)
                self._drain_results()
                self.result_event.wait(timeout=0.01)
                self.result_event.clear()
                continue

            self.in_progress[task.task_id] = task
            target.dispatch(task)
            self.logger.info(
                "DISTRIB",
                f"Task {task.task_id} (key={task.key}) -> Worker{target.worker_id}"
                f" (queue~{target.estimated_queue}/{QUEUE_MAX})"
                f"{' [RETRY #' + str(task.retry) + ']' if task.retry > 0 else ''}"
            )

            self._log_progress()

        self._drain_results()

    def _pick_target(self):
        # lb-demo: bias initial tasks to Worker1 to trigger P2P
        if self._demo_remaining > 0:
            w1 = self.sessions[0]
            if w1.estimated_queue < QUEUE_MAX:
                self._demo_remaining -= 1
                return w1

        candidates = [s for s in self.sessions if s.estimated_queue < QUEUE_MAX]
        if not candidates:
            return None
        # Fairness cap: don't dispatch to a worker that's too far ahead
        min_dispatched = min(s.stats.dispatched for s in candidates)
        fair = [s for s in candidates if s.stats.dispatched < min_dispatched + 5]
        if not fair:
            return None  # wait for lagging workers to free up
        return min(fair, key=lambda s: (s.stats.dispatched, s.estimated_queue))

    def _drain_results(self):
        while True:
            try:
                msg = self.result_queue.get_nowait()
            except queue.Empty:
                break
            self._process_message(msg)

    def _process_message(self, msg):
        mtype = msg.get("type")
        if mtype == "RESULT":
            self._process_result(msg)
        elif mtype == "P2P_MOVED":
            self._process_p2p_moved(msg)

    def _process_result(self, msg):
        task_id = msg["task_id"]
        status = msg["status"]
        worker_id = msg["worker_id"]
        queue_size = msg.get("queue_size", 0)
        proc_time = msg.get("proc_time", 0)
        wait_time = msg.get("wait_time", 0)

        session = self.session_map.get(worker_id)
        if session:
            session.on_result(queue_size)

        task = self.in_progress.pop(task_id, None)

        if status == "SUCCESS":
            if task:
                self.kv_store.commit(task.key, task.value, worker_id)
            if session:
                session.stats.success += 1
                session.stats.total_wait_time += wait_time
                session.stats.total_proc_time += proc_time
            self.logger.success(
                "RESULT",
                f"Task {task_id} SUCCESS by Worker{worker_id} (queue:{queue_size})"
            )
        else:
            if session:
                if status == "REJECT":
                    session.stats.reject += 1
                else:
                    session.stats.fail += 1
                    session.stats.total_wait_time += wait_time
                    session.stats.total_proc_time += proc_time
            self.logger.fail(
                "RESULT",
                f"Task {task_id} {status} by Worker{worker_id}"
                f" -> Priority Queue (retry #{task.retry + 1 if task else '?'})"
            )
            if task:
                self.fault_handler.push(task)

    def _process_p2p_moved(self, msg):
        task_ids = msg.get("task_ids", [])
        from_w = msg["from_worker"]
        to_w = msg["to_worker"]
        count = len(task_ids)

        from_s = self.session_map.get(from_w)
        to_s = self.session_map.get(to_w)
        if from_s:
            from_s.adjust_queue(-count)
        if to_s:
            to_s.adjust_queue(count)

        self.total_p2p_events += 1
        self.logger.info(
            "LB",
            f"P2P: {count} task(s) Worker{from_w} -> Worker{to_w} {task_ids}"
        )

    def _log_progress(self):
        current = len(self.kv_store)
        if current >= self._last_progress + 100:
            self.logger.info("DISTRIB", f"Progress: {current}/{self.num_kv} completed")
            self._last_progress = (current // 100) * 100
