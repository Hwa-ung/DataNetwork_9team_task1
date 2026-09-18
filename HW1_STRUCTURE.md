# HW#1 세분화 구조 설계 (Python + AWS EC2)

> [HW1_GUIDE.md](HW1_GUIDE.md) 의 "무엇을 만들어야 하는가"에 이어,
> **"어떤 파일을 어떤 순서로 만들 것인가"** 를 모듈/함수 단위까지 쪼갠 문서.

---

## 0. 실행 토폴로지 (먼저 확정할 것)

```
[AWS EC2  Ubuntu  Public IP: 54.x.x.x]        [조원 1명의 로컬 PC]
+------------------------------+              +--------------------------------+
|  master 프로세스 1개          |              |  worker 프로세스 1개            |
|  - 리스닝 :9000              | <--TCP-----> |   +- Thread: Worker1 (P2P :9101)|
|  - System Clock              |   (인터넷)    |   +- Thread: Worker2 (P2P :9102)|
|  - KV Store 5,000            |              |   +- Thread: Worker3 (P2P :9103)|
|  - Priority Queue            |              |   +- Thread: Worker4 (P2P :9104)|
+------------------------------+              |   P2P는 127.0.0.1 내부통신      |
                                              +--------------------------------+
```

**중요 결정 3가지 (여기서 안 정하면 나중에 다 갈아엎어야 함)**

| 결정 | 선택 | 이유 |
|---|---|---|
| Worker 4개를 한 PC에? | **예. 한 프로세스 안 4 Thread** | 명세가 "4개 Thread"를 요구. 조원 PC 4대에 나누면 P2P가 서로 NAT 뒤에 있어서 직접 연결 불가 |
| P2P 주소 | **127.0.0.1:9101~9104** | 같은 PC라 공인 IP/포트포워딩 불필요. Master만 공인 IP 필요 |
| Master 실행 위치 | **EC2에서 상시 실행 (tmux)** | 필수 조건. SSH 끊겨도 죽지 않게 tmux/screen 필수 |

---

## 1. 디렉터리 구조

```
DataNetwork/
├─ src/
│  ├─ common/                  # Master·Worker 공용 (가장 먼저 완성해야 함)
│  │  ├─ __init__.py
│  │  ├─ protocol.py           # 메시지 정의 + 길이접두 송수신
│  │  ├─ clock.py              # VirtualClock (Lamport 방식)
│  │  ├─ logger.py             # [clock] NODE | EVENT | STATUS | msg
│  │  └─ constants.py          # 모든 매직넘버 한 곳에
│  │
│  ├─ master/
│  │  ├─ __init__.py
│  │  ├─ main.py               # 엔트리포인트 (argparse)
│  │  ├─ kv_store.py           # 5,000 KV 생성 + 저장
│  │  ├─ worker_session.py     # Worker 1개당 연결 담당 Thread
│  │  ├─ scheduler.py          # 동적 분배 알고리즘 (핵심)
│  │  ├─ fault_handler.py      # Priority Queue / 재할당
│  │  └─ stats.py              # STAT 집계
│  │
│  ├─ worker/
│  │  ├─ __init__.py
│  │  ├─ main.py               # 엔트리포인트, Worker Thread 4개 기동
│  │  ├─ node.py               # WorkerNode(threading.Thread) = 명세의 "독립 Thread"
│  │  ├─ ready_queue.py        # 최대 10, 70% 초과 WARN
│  │  ├─ processor.py          # 1~3초 가상처리 + 80/20
│  │  ├─ p2p.py                # P2P 서버/클라이언트
│  │  └─ stats.py
│  │
│  └─ config.json              # master_ip / master_port / num_kv ...
│
├─ logs/                       # 실행 시 생성 (Master.txt, Worker1~4.txt)
├─ scripts/
│  ├─ run_master.sh            # EC2용
│  ├─ run_worker.ps1           # 로컬 윈도우용
│  └─ deploy_ec2.sh            # scp + ssh 배포
│
├─ AllDefinedLogs.txt
├─ Readme.txt
└─ download.txt
```

> 제출 시엔 이 `src/` 전체를 그대로 zip에 넣으면 된다.

---

## 2. 모듈별 상세 명세

### 2-1. `common/constants.py` — 매직넘버 전부 여기로

```python
NUM_KV            = 5000
KEY_SPACE         = 0x10000     # hex 4자리 = 0000~ffff
VALUE_MIN, VALUE_MAX = 1, 100

QUEUE_MAX         = 10
QUEUE_WARN_OVER   = 7           # "70% 초과" = 8개 이상 -> size > 7
SUCCESS_RATE      = 0.8
PROC_TIME_MIN, PROC_TIME_MAX = 1.0, 3.0
NET_LATENCY       = 1.0         # 노드 간 통신 지연 (가상)

AVG_PROC_TIME     = 2.0         # P2P 계산용 평균 처리시간
LB_THRESHOLD      = 15.0        # 예상 대기 임계값(sec)
LB_CHECK_MIN, LB_CHECK_MAX = 1.0, 3.0   # 점검 주기 (가상 clock 기준)

NUM_WORKERS       = 4
P2P_PORT_BASE     = 9100        # Worker N -> 9100+N
```

---

### 2-2. `common/protocol.py` — **여기서 실패하는 조가 제일 많음**

TCP는 스트림이라 `sock.recv(1024)` 한 번이 메시지 1개와 일치하지 않는다.
반드시 **4바이트 길이 접두사 + JSON(UTF-8)** 형식으로 프레이밍한다.

```python
import json, struct, socket

def send_msg(sock, obj: dict) -> None:
    """thread-safe 하게 쓰려면 호출측에서 sock별 send_lock을 잡을 것"""
    raw = json.dumps(obj, ensure_ascii=False).encode('utf-8')
    sock.sendall(struct.pack('>I', len(raw)) + raw)

def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None                      # 상대가 닫음
        buf += chunk
    return buf

def recv_msg(sock):
    head = _recv_exact(sock, 4)
    if head is None: return None
    (length,) = struct.unpack('>I', head)
    body = _recv_exact(sock, length)
    if body is None: return None
    return json.loads(body.decode('utf-8'))
```

**메시지 스키마** — 모든 메시지에 `clock` 필드(송신 시점 System Clock)를 반드시 넣는다.
이게 시계 동기화의 전부다.

| type | 방향 | 필드 |
|---|---|---|
| `HELLO` | W→M | `worker_id`, `p2p_port`, `clock` |
| `PEERS` | M→W | `peers:[{worker_id,host,p2p_port}]`, `clock` |
| `START` | M→W | `clock` |
| `TASK` | M→W | `task_id`, `key`, `value`, `priority`(bool), `retry`(int), `clock` |
| `RESULT` | W→M | `task_id`, `status`(SUCCESS/FAIL/REJECT), `worker_id`, `proc_time`, `wait_time`, `queue_size`, `clock` |
| `P2P_MOVED` | W→M | `task_ids`, `from_worker`, `to_worker`, `clock` |
| `TERMINATE` | M→W | `clock` |
| `BYE` | W→M | `worker_id`, `stats`, `clock` |
| `P2P_QUERY` | W→W | `from_worker`, `clock` |
| `P2P_QUERY_RES` | W→W | `worker_id`, `queue_size`, `clock` |
| `P2P_TRANSFER` | W→W | `from_worker`, `tasks:[...]`, `clock` |
| `P2P_ACK` | W→W | `worker_id`, `accepted:[task_id]`, `rejected:[task_id]`, `clock` |

> **`P2P_MOVED` 를 빼먹지 말 것.** Worker1이 작업을 Worker3에 넘겼는데 Master가 모르면,
> Master의 큐 추정치가 계속 틀어지고 "누가 처리했는지" 통계도 어긋난다.
> `RESULT`의 `queue_size`와 함께 Master의 그림자 상태를 계속 보정해 준다.

---

### 2-3. `common/clock.py` — System Clock (설계 난이도 1순위)

Master와 Worker는 **다른 머신의 다른 프로세스**라 전역 변수를 공유할 수 없다.
→ **Lamport 논리 시계** 방식으로 각자 들고 있되, 메시지를 주고받을 때 맞춘다.

```python
import threading
from .constants import NET_LATENCY

class VirtualClock:
    def __init__(self, start: float = 0.0):
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, delta: float) -> float:
        """가상 처리시간 누적 (실제 sleep 없음)"""
        with self._lock:
            self._t += delta
            return self._t

    def stamp_send(self) -> float:
        """송신 직전 호출 -> 메시지의 clock 필드에 넣을 값"""
        with self._lock:
            return self._t

    def sync_recv(self, sender_clock: float) -> float:
        """수신 직후 호출. 통신지연 1초를 여기서 한 번만 반영한다."""
        with self._lock:
            self._t = max(self._t, sender_clock + NET_LATENCY)
            return self._t
```

**규칙 3줄 요약**

1. 작업 처리 → `clock.advance(random.uniform(1,3))`
2. 메시지 보낼 때 → `msg['clock'] = clock.stamp_send()`
3. 메시지 받을 때 → `clock.sync_recv(msg['clock'])`
   ← 지연 1초는 **수신측에서만** 더한다 (양쪽에서 더하면 2초가 됨)

**예상 결과값 주의**: 명세 예시의 `195.40 sec`는 "예시 값"이라고 명시돼 있다.
5,000건 × 평균 2초 ÷ 4 Worker ≈ 2,500초, 재시도 25% 포함 시 **약 3,000초 내외**가 정상.
당황하지 말고 Readme에 "System Clock 누적 모델 기준"이라고 근거를 적으면 된다.

---

### 2-4. `common/logger.py`

```python
class NodeLogger:
    def __init__(self, node_name: str, clock: VirtualClock, path: str):
        ...   # 파일 핸들 + threading.Lock + stdout 동시 출력

    def log(self, event: str, status: str, message: str, clock: float = None):
        c = clock if clock is not None else self._clock.now()
        line = f"[{c:.2f}] {self._node} | {event:<9} | {status:<7} | {message}"
        with self._lock:
            print(line)              # 시연 영상용 실시간 출력
            self._fp.write(line + "\n")
            self._fp.flush()

    def info(self, event, msg):    self.log(event, "INFO", msg)
    def success(self, event, msg): self.log(event, "SUCCESS", msg)
    def fail(self, event, msg):    self.log(event, "FAIL", msg)
    def warn(self, event, msg):    self.log(event, "WARN", msg)
```

- `STATUS`는 **INFO / SUCCESS / FAIL / WARN 4개만** 사용 (명세 고정)
- `EVENT`는 자유지만 쓴 건 전부 `AllDefinedLogs.txt`에 적어야 하므로,
  **`constants.py`에 EVENT 목록을 상수로 정의**해 두고 그걸로만 쓰면 문서 작성이 자동화된다.

```python
EVENTS = {
    "INIT":      "시스템 초기화 (System Clock 시작, KV 생성, Thread 기동)",
    "CONNECT":   "소켓 연결/해제",
    "DISTRIB":   "Master의 작업 분배 및 재할당 결정",
    "RESULT":    "Master의 처리 결과 수신",
    "RECV":      "Worker의 작업 수신",
    "PROC":      "Worker의 작업 처리 (성공/실패)",
    "QUEUE":     "Ready Queue 상태 변화 (70% 초과 WARN, 포화 FAIL)",
    "LB":        "P2P 부하 분산 (조회/이전/ACK)",
    "STAT":      "최종 통계",
    "TERMINATE": "정상 종료",
}
```

---

### 2-5. `master/` 모듈

#### `kv_store.py`

```python
def generate_kv_pairs(n=NUM_KV) -> list:
    keys = random.sample(range(KEY_SPACE), n)      # 중복 없는 hex4 보장
    return [Task(task_id=f"{i+1:04d}",
                 key=f"{k:04x}",
                 value=random.randint(1, 100)) for i, k in enumerate(keys)]

class KVStore:          # 성공 시 확정 저장
    def commit(self, key, value, worker_id) -> None
    def __len__(self) -> int
    def dump(self, path) -> None      # 최종 5,000쌍 기록
```

#### `worker_session.py` — Worker 1개당 Thread 1개

```python
class WorkerSession(threading.Thread):
    """소켓 수신 전담. 받은 메시지를 Scheduler에 콜백으로 넘긴다."""
    worker_id: int
    sock: socket.socket
    send_lock: threading.Lock      # 여러 스레드가 같은 소켓에 쓰므로 필수
    queue_size: int                # <- Master가 추정하는 그림자 큐 상태
    stats: WorkerStat

    def send_task(self, task) -> None       # queue_size += 1
    def run(self) -> None                   # recv_msg 루프 -> on_result / on_p2p_moved
```

#### `scheduler.py` — **동적 작업 분배 (Readme에 알고리즘 명시 필수)**

알고리즘명: **Least-Queue-First with Priority Preemption (최소 큐 우선 + 우선순위 선점)**

```python
def pick_target(self):
    candidates = [w for w in self.workers if w.queue_size < QUEUE_MAX]
    if not candidates:
        return None                       # 전부 포화 -> 결과 올 때까지 대기
    return min(candidates, key=lambda w: (w.queue_size, w.dispatched))
    #                                       ^ 동률이면 누적 분배 적은 쪽 (공평성)

def dispatch_loop(self):
    while len(self.kv_store) < NUM_KV:
        task = self.priority_q.pop() or self.pending_q.popleft_or_none()
        if task is None:
            self.result_event.wait(); continue      # 분배 끝, 결과만 대기
        target = self.pick_target()
        if target is None:
            self.result_event.wait(); continue      # 전 Worker 포화
        target.send_task(task)
```

| 장점 | 단점 |
|---|---|
| O(N) (N=4)로 매우 가볍고 구현 단순 | Master가 가진 큐 상태가 통신 지연(1초)만큼 낡음 → 순간 과분배 가능 |
| 큐 길이 기준이라 처리 속도 편차에 자동 적응 | 전역 정보 의존 → Master 단일 장애점 |
| 동률 tie-break로 장기적 공평성 확보 | P2P 이전이 겹치면 추정치가 흔들림 (→ `P2P_MOVED`로 보정) |

> 이 표를 그대로 Readme의 "동적 작업 분배 알고리즘 장단점 분석"에 쓰면 된다.

#### `fault_handler.py`

```python
class PriorityRequeue:
    """heapq. retry 횟수가 많을수록 더 우선"""
    def push(self, task, reason: str) -> None:
        task.retry += 1
        heapq.heappush(self._h, (-task.retry, task.seq, task))
    def pop(self):
        return heapq.heappop(self._h)[2] if self._h else None
```

- `RESULT/FAIL` 수신 → `push` → 다음 분배 루프에서 **최우선** 선택
- `RESULT/REJECT`(큐 오버플로) 도 동일하게 재큐
- **성공할 때까지 반복** → `while completed < 5000` 루프가 자연히 보장

---

### 2-6. `worker/` 모듈

#### 스레드 구성 (Worker 1개당 3개, 총 12+1개)

| 스레드 | 역할 | 개수 |
|---|---|---|
| `WorkerNode` | **명세가 요구하는 "독립 Thread"**. 큐에서 꺼내 처리 + LB 점검 | 4 |
| `MasterReceiver` | Master 소켓 blocking recv 전담 → ready_queue에 push | 4 |
| `P2PServer` | 자기 P2P 포트 listen, 조회/이전 수신 처리 | 4 |
| `main` | 위 스레드 기동 + join + 종료 처리 | 1 |

> Readme에는 "각 Worker Node는 `WorkerNode(threading.Thread)` 인스턴스로 독립 실행되며,
> 소켓 I/O 블로킹을 분리하기 위한 보조 스레드를 가진다"고 적으면 된다.
> GIL은 이 과제에선 무관 (I/O 바운드 + 가상시간).

#### `ready_queue.py`

```python
class ReadyQueue:
    def __init__(self, worker_id, logger, clock):
        self._q = collections.deque()
        self._lock = threading.Lock()

    def push(self, task) -> bool:
        with self._lock:
            if len(self._q) >= QUEUE_MAX:
                self._logger.fail("QUEUE", f"Queue full (10/10). {task.tag} rejected.")
                return False                  # -> 호출측이 REJECT 응답
            task.enqueue_clock = self._clock.now()   # 대기시간 측정 시작
            self._q.append(task)
            self._warn_if_over_70(len(self._q), "push")
            return True

    def pop(self):
        with self._lock:
            if not self._q: return None
            t = self._q.popleft()
            self._warn_if_over_70(len(self._q), "pop")   # <- 나갈 때도 검사!
            return t

    def _warn_if_over_70(self, size, op):
        if size > QUEUE_WARN_OVER:            # 8,9,10일 때 매번
            self._logger.warn("QUEUE", f"Queue over 70%: {size}/10 (after {op})")
```

> **함정**: WARN은 "70% 넘는 순간 1회"가 아니라 **70% 초과 상태에서 들고날 때마다 매번**이다
> (명세 0-1 명시). `push`/`pop` 양쪽에 다 걸어야 한다.

#### `processor.py`

```python
def process(task, clock):
    proc = random.uniform(PROC_TIME_MIN, PROC_TIME_MAX)
    clock.advance(proc)                       # 실제 sleep 절대 금지
    return (random.random() < SUCCESS_RATE), proc
```

#### `p2p.py` — **P2P 부하 분산 (Readme에 알고리즘 명시 필수)**

알고리즘명: **Threshold-triggered Greedy Pairwise Balancing (임계 기반 탐욕적 1:1 균형화)**

```python
def maybe_balance(self):
    # 1) 트리거 판정
    expected_wait = len(self.queue) * AVG_PROC_TIME
    if expected_wait <= LB_THRESHOLD:         # 15초
        return
    # 2) 이웃 전원에 큐 상태 조회 (P2P 소켓)
    sizes = {pid: self.query(pid) for pid in self.peers}
    target, tsize = min(sizes.items(), key=lambda kv: kv[1])
    # 3) 이전 개수 = 격차의 절반, 상대 여유분으로 상한
    k = min((len(self.queue) - tsize) // 2, QUEUE_MAX - tsize)
    if k <= 0: return
    # 4) 큐 "뒤쪽"(가장 늦게 처리될) k개를 선택 -> 전송
    tasks = self.queue.peek_tail(k)
    self.send(target, {"type": "P2P_TRANSFER", "tasks": tasks, ...})
    # 5) ACK 받은 것만 자기 큐에서 제거 (유실 방지)
    ack = self.wait_ack(target)
    self.queue.remove_many(ack["accepted"])
    self.notify_master_moved(ack["accepted"], target)   # P2P_MOVED
```

**점검 주기**: 1~3초 랜덤 — 실제 `time.sleep`이 아니라 가상 clock 기준으로
`next_check_clock = clock.now() + random.uniform(1, 3)` 를 잡고 메인 루프에서 비교한다.

| 장점 | 단점 |
|---|---|
| Master 개입 없이 Worker끼리 자율 조정 → Master 병목 완화 | 조회 왕복마다 clock 2초 증가 → 과도 호출 시 오버헤드 |
| 격차의 절반만 옮겨 핑퐁(무한 왕복) 방지 | 전체 최적이 아닌 국소 최적 (pairwise) |
| ACK 후 삭제라 이전 중 유실 없음 | ACK 대기 중 블로킹 → 타임아웃 처리 필요 |

> **핑퐁 방지**가 핵심 포인트다. 격차 전체를 옮기면 A→B→A→B 무한 반복이 생긴다. 반드시 `// 2`.

---

## 3. 단계별 작업 분해 (체크리스트)

### Phase 0 — 환경 (Day 1, 담당 D)

- [ ] EC2 인스턴스 생성: Ubuntu 24.04 LTS, `t3.micro`(프리티어)
- [ ] 보안 그룹 인바운드 추가: **TCP 9000, 소스 0.0.0.0/0**
- [ ] 탄력적 IP(Elastic IP) 할당 → 재부팅해도 IP 고정 (**필수**, 안 하면 매번 IP 바뀜)
- [ ] `.pem` 키 권한: 윈도우는 속성 → 보안 → 상속 해제 후 본인만 남기기
- [ ] SSH 접속 확인: `ssh -i key.pem ubuntu@<EIP>`
- [ ] EC2에 `python3 --version` 확인 (Ubuntu 기본 탑재), `sudo apt install -y tmux`
- [ ] 로컬에서 포트 도달 확인: `Test-NetConnection <EIP> -Port 9000` (PowerShell)
- [ ] GitHub 저장소 생성 + 4명 초대

### Phase 1 — `common/` 완성 (Day 1~2, 담당 A) ※ 다른 모두를 막는 단계

- [ ] `constants.py` 전부 채우기
- [ ] `protocol.py` + **단독 테스트**: 로컬 에코 서버로 1만 건 송수신, 메시지 깨짐 0건 확인
- [ ] `clock.py` + 단위 테스트: `sync_recv` 후 단조증가(monotonic) 보장 확인
- [ ] `logger.py` + 출력 포맷이 명세와 글자 단위로 일치하는지 확인
- **완료 조건**: B, C가 이 4개 파일을 import만 하면 바로 개발 시작 가능

### Phase 2 — 핸드셰이크 (Day 2~3, 담당 B+C 동시)

- [ ] Master: listen → `HELLO` 4개 수신 → `PEERS` 브로드캐스트 → `START`
- [ ] Worker: 4 Thread 기동 → connect → `HELLO` 송신 → `PEERS` 저장 → P2P 서버 listen
- **완료 조건(localhost)**: `Master.txt`에 CONNECT SUCCESS 4줄, `Worker1~4.txt`에 각 1줄

### Phase 3 — 기본 파이프라인 (Day 3~5, B+C) ※ P2P·재시도 없이

- [ ] `--num-kv 50` 옵션으로 소규모 테스트
- [ ] Master: KV 생성 → least-queue 분배 → RESULT 수신 → 진행률 로그
- [ ] Worker: RECV → ReadyQueue → process → RESULT 송신
- **완료 조건**: 50건이 전부 SUCCESS로 끝나고 프로그램이 정상 종료

### Phase 4 — 장애 처리 (Day 5~6, 담당 B)

- [ ] 80/20 성공률 적용
- [ ] FAIL → `PriorityRequeue` 등록 → 다음 루프에서 최우선 재할당
- [ ] 재시도도 80/20 동일 적용, 성공까지 반복
- [ ] 큐 오버플로 REJECT 경로도 동일하게 재큐
- **완료 조건**: `--num-kv 50` 에서 재할당이 10건 내외 발생하고 최종 50/50 성공

### Phase 5 — P2P 부하 분산 (Day 6~8, 담당 C)

- [ ] P2P 서버 스레드 (QUERY / TRANSFER 처리)
- [ ] `maybe_balance()` 트리거 + 격차 절반 이전 + ACK 후 삭제
- [ ] `P2P_MOVED`로 Master 상태 보정
- [ ] 핑퐁 발생 여부 확인 (LB 로그에서 같은 쌍이 반복되면 실패)
- **완료 조건**: LB 이벤트가 로그에 실제로 남고, 이전된 작업이 수신측에서 처리 완료됨

> **TIP**: 큐가 8개 이상 차야 LB가 트리거되는데, 스케줄러가 너무 잘 분산하면 LB가 한 번도 안 일어난다.
> `--num-kv 5000` 으로 돌려 자연 발생시키는 게 정석. 그래도 안 나오면 `--lb-demo` 플래그로
> 특정 Worker에 일시적으로 몰아주는 데모 모드를 만들고 Readme에 명시할 것.

### Phase 6 — 통계·로그 (Day 8~9, 담당 D)

- [ ] 6개 필수 지표 집계 (Worker별 처리량 / 성공·실패 / 평균 대기시간 / P2P 횟수 / 재할당 / 총 수행시간)
- [ ] 평균 대기시간 = `(dequeue_clock - enqueue_clock)` 누적 평균
- [ ] Master STAT / Worker STAT 블록 출력
- [ ] `AllDefinedLogs.txt` 자동 생성 스크립트 (`constants.EVENTS` 순회)
- [ ] Graceful Termination: TERMINATE → BYE → 소켓 close → 파일 flush

### Phase 7 — EC2 통합 (Day 9~10, 전원)

- [ ] `scp -i key.pem -r src ubuntu@<EIP>:~/hw1/`
- [ ] EC2에서 `tmux new -s master` → `python3 -m master.main --port 9000`
- [ ] 로컬에서 `python -m worker.main --master-ip <EIP> --master-port 9000`
- [ ] 5,000건 full run → 로그 5종 확보
- [ ] `scp` 로 `Master.txt` 회수

### Phase 8 — 제출물 (Day 10~11, 담당 D)

- [ ] `Readme.txt` 8개 항목 (특히 두 알고리즘 장단점 표 — 위 2-5, 2-6 표 활용)
- [ ] 시연 영상 5분: 초기화 → 연결 → 분배 → **실패 재할당** → **P2P LB** → STAT
- [ ] `download.txt` 링크 + **권한을 "링크가 있는 모든 사용자"로 변경했는지 재확인**
- [ ] `G?조HW1.zip` 압축 후 **다른 PC에서 풀어서 실행까지** 검증 (압축 오류 0점)

---

## 4. 역할 분담 (파일 충돌 없게 설계)

| 담당 | 파일 | 시작 시점 |
|---|---|---|
| **A** | `common/*` (protocol, clock, constants, logger) | Day 1 — **최우선**, 늦으면 전원 대기 |
| **B** | `master/*` | Day 2 (A의 스텁 나오면) |
| **C** | `worker/*` | Day 2 |
| **D** | EC2 인프라, `scripts/*`, `*/stats.py`, 문서, 영상 | Day 1 |

각자 다른 디렉터리를 건드리므로 git 충돌이 거의 안 난다. `common/`만 A가 단독 소유.

---

## 5. 실행 인터페이스

```bash
# EC2 (Master)
python3 -m master.main --port 9000 --num-kv 5000 --log-dir ./logs

# 로컬 (Worker 4개 = 1 프로세스)
python -m worker.main --master-ip 54.180.12.34 --master-port 9000 \
                      --num-workers 4 --p2p-port-base 9100 --log-dir ./logs
```

`config.json` 대체도 지원 (명세가 "인자 또는 설정파일"을 요구):

```json
{ "master_ip": "54.180.12.34", "master_port": 9000,
  "num_kv": 5000, "num_workers": 4, "p2p_port_base": 9100 }
```

---

## 6. Python 특화 주의사항

| 함정 | 대응 |
|---|---|
| 같은 소켓에 여러 스레드가 `sendall` → 메시지 섞임 | 소켓마다 `send_lock = threading.Lock()` 필수 |
| `recv(1024)` 로 JSON 한 개 받으려다 깨짐 | 4바이트 길이 접두 프레이밍 (2-2) |
| `time.sleep(2)` 로 처리시간 구현 | **절대 금지**. `clock.advance(2)` 만. 5,000건이 몇 초 만에 끝나야 정상 |
| 스레드 예외가 조용히 삼켜짐 | 각 `run()` 최상단을 `try/except` 로 감싸고 `traceback.print_exc()` |
| 종료 시 `recv` 블로킹으로 안 죽음 | `sock.settimeout(1.0)` 또는 TERMINATE 수신 후 `sock.shutdown(SHUT_RDWR)` |
| 로그 파일이 비어 있음 | `flush()` 매번, 또는 `atexit` 로 close 보장 |
| EC2에서 SSH 끊기면 Master 죽음 | `tmux new -s master` 안에서 실행 |
| 5,000건인데 로그가 수십만 줄 | 개별 DISTRIB/RESULT는 남기되, 진행률은 100건마다 1줄로 요약 |
| GIL 때문에 느리다? | 이 과제는 I/O 바운드 + 가상시간이라 **무관**. Readme에 한 줄 적으면 가산점 |

---

## 7. 최소 동작 검증 순서 (각 단계 통과 전 다음으로 안 넘어가기)

```
1) localhost + KV 10개   + P2P off + 실패율 0%    -> 파이프라인 확인
2) localhost + KV 50개   + P2P off + 실패율 20%   -> 재할당 확인
3) localhost + KV 500개  + P2P on  + 실패율 20%   -> LB 확인
4) EC2       + KV 500개                           -> 네트워크 확인
5) EC2       + KV 5000개                          -> 최종 제출본
```
