import hashlib
import random
from common.constants import PROC_TIME_MIN, PROC_TIME_MAX, SUCCESS_RATE


def process(task, clock):
    proc_time = random.uniform(PROC_TIME_MIN, PROC_TIME_MAX)
    clock.advance(proc_time)
    # Simulate actual CPU processing overhead (hash computation)
    # This creates realistic queue buildup for P2P load balancing
    hashlib.sha256(f"{task.task_id}:{task.key}:{task.value}".encode()).hexdigest()
    success = random.random() < SUCCESS_RATE
    return success, proc_time
