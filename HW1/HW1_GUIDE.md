# HW#1 완료 가이드 - 분산 Fault-Tolerant 키-값 저장소

> **마감: 2026.9.23(수) 23:59**
> **제출: G조이름HW1.zip (조별 1명)**

---

## 0. 필수 구현 조건 (하나라도 미충족 시 0점)

| 조건 | 설명 |
|---|---|
| 외부 서버 필수 | Master Node는 반드시 AWS/GCP 등 클라우드 서버에 구현 |
| Socket 통신 필수 | Master-Worker, Worker-Worker 간 모든 통신은 TCP Socket |
| 독립 Thread 필수 | 4개 Worker Node는 각각 독립적인 Thread로 동작 |

---

## 1단계: 환경 준비

### 1-1. 프로그래밍 언어 선택

자유이나, 아래 중 하나를 추천:

| 언어 | 장점 | 단점 |
|---|---|---|
| **Java** | Socket/Thread 표준 라이브러리 풍부, 크로스플랫폼 | 보일러플레이트 많음 |
| **Python** | 빠른 개발, socket/threading 모듈 간단 | GIL 제약 (이 과제에선 문제 없음) |
| **C/C++** | 성능 좋음, 소켓 프로그래밍 교과서적 | 개발 속도 느림 |

### 1-2. 클라우드 서버 준비 (Master Node용)

AWS 또는 GCP 중 하나를 선택하여 VM 인스턴스를 생성한다.

#### AWS EC2 사용 시:
1. AWS 계정 생성 (학생 크레딧 또는 프리티어 사용)
2. EC2 인스턴스 생성 (t2.micro 프리티어 가능)
   - OS: Ubuntu 22.04 LTS 추천
   - 보안 그룹에서 사용할 포트(예: 9000) 인바운드 허용 (TCP, 0.0.0.0/0)
3. 키 페어(.pem) 다운로드 후 SSH 접속 확인
4. 탄력적 IP 할당 (선택, IP 고정용)

#### GCP Compute Engine 사용 시:
1. GCP 계정 생성 (학생 크레딧 $300 활용)
2. VM 인스턴스 생성 (e2-micro 무료)
   - OS: Ubuntu 22.04 LTS 추천
   - 방화벽에서 사용할 포트 허용
3. 외부 IP 확인

#### 서버 준비 후 확인사항:
- [ ] 공인(Public) IP 확인
- [ ] 해당 포트 방화벽/보안그룹 오픈
- [ ] 로컬 PC에서 `telnet <공인IP> <포트>`로 연결 테스트
- [ ] 프로그래밍 언어 런타임 설치 (Java JDK / Python 등)

### 1-3. 프로그램 실행 시 IP/포트 설정

Master의 IP와 포트는 **프로그램 인자(argument) 또는 설정파일**로 지정 가능해야 한다.

```
# 예시: 명령행 인자 방식
python worker.py --master-ip 54.180.xx.xx --master-port 9000 --worker-id 1

# 예시: 설정파일 방식 (config.json)
{
  "master_ip": "54.180.xx.xx",
  "master_port": 9000
}
```

---

## 2단계: 시스템 아키텍처 설계

### 2-1. 전체 구조

```
[AWS/GCP 서버]                    [로컬 PC]
+----------------+                +----------+  +----------+
| Master Node    | <--Socket-->   | Worker1  |  | Worker2  |
| - System Clock |                | Thread1  |  | Thread2  |
| - KV Store     |                +----------+  +----------+
| - Scheduler    |                +----------+  +----------+
| - Priority Q   | <--Socket-->   | Worker3  |  | Worker4  |
+----------------+                | Thread3  |  | Thread4  |
                                  +----------+  +----------+
                                  <-- P2P Socket 통신 -->
```

### 2-2. 통신 프로토콜 설계

Master-Worker, Worker-Worker 간 주고받을 메시지 형식을 먼저 정의한다.
JSON 또는 구분자 기반 텍스트 프로토콜 추천.

```
# 메시지 타입 예시 (JSON 방식)

# Master -> Worker: 작업 할당
{"type": "TASK", "task_id": "0001", "key": "a3f7", "value": 47, "priority": false}

# Worker -> Master: 처리 결과 (성공)
{"type": "RESULT", "task_id": "0001", "status": "SUCCESS", "worker_id": 1, "process_time": 2.3}

# Worker -> Master: 처리 결과 (실패)
{"type": "RESULT", "task_id": "0003", "status": "FAIL", "worker_id": 2, "process_time": 1.9}

# Master -> Worker: 큐 상태 요청
{"type": "QUEUE_STATUS_REQ"}

# Worker -> Master: 큐 상태 응답
{"type": "QUEUE_STATUS_RES", "worker_id": 1, "queue_size": 7, "max_queue": 10}

# Master -> Worker: 종료 신호
{"type": "TERMINATE"}

# Worker -> Worker: 큐 상태 조회 (P2P)
{"type": "P2P_QUERY", "from_worker": 1}

# Worker -> Worker: 큐 상태 응답 (P2P)
{"type": "P2P_QUERY_RES", "worker_id": 3, "queue_size": 3}

# Worker -> Worker: 작업 이전 요청 (P2P)
{"type": "P2P_TRANSFER", "tasks": [{"task_id": "0088", "key": "...", "value": ...}, ...]}

# Worker -> Worker: 작업 이전 ACK (P2P)
{"type": "P2P_ACK", "received_count": 3}
```

### 2-3. System Clock 설계

**핵심**: 실제 시간(real time)이 아니라 가상 누적 시간이다. `time.sleep()`을 쓰지 않는다.

```python
# 개념 예시
system_clock = 0.0

# Worker가 작업 처리 시
process_time = random.uniform(1.0, 3.0)  # 1~3초
system_clock += process_time  # 실제 대기 없이 값만 더함

# 노드 간 통신 시
system_clock += 1.0  # 통신 지연 1초 (값만 더함)
```

**주의**: Master와 Worker 각각이 System Clock을 관리하며, 통신 시 동기화 방법을 설계해야 한다.

---

## 3단계: Master Node 구현

### 3-1. Master 핵심 기능 목록

- [ ] **System Clock 초기화** (0초에서 시작)
- [ ] **KV 쌍 5,000개 생성**
  - Key: 고유 16진수 4자리 (예: `a3f7`, `0000`~`ffff` 범위 내 unique)
  - Value: 1~100 랜덤 정수
- [ ] **소켓 서버 시작** (지정 포트에서 Worker 4개 연결 대기)
- [ ] **Worker 4개 연결 수신 및 확인**
- [ ] **동적 작업 분배 (Workload Scheduler)**
  - 각 Worker의 Queue 상태를 모니터링
  - Queue 여유가 가장 많은 Worker에 우선 할당
  - Queue가 꽉 찬(10/10) Worker에는 분배 중단
  - 통신 지연 1초 가정 (System Clock에 누적)
- [ ] **결과 수집**
  - Worker로부터 SUCCESS/FAIL 결과 수신
  - SUCCESS: KV 저장소 업데이트
  - FAIL: Priority Queue에 등록 -> 여유 Worker에 재할당
- [ ] **Priority Queue 운영**
  - 실패한 작업을 최우선으로 재할당
  - 재할당된 작업도 80%/20% 규칙 동일 적용
  - 성공할 때까지 반복 재시도
- [ ] **진행 상황 로그** (주기적으로 x/5000 진행률 기록)
- [ ] **최종 통계 출력** (STAT 로그)
- [ ] **Graceful Termination** (완료 신호 전송 후 종료)
- [ ] **Master.txt 로그 파일 생성**

### 3-2. Master 의사코드

```
시작:
  system_clock = 0
  kv_store = 5000개 KV 쌍 생성
  pending_queue = kv_store 전체 (아직 분배 안 된 작업)
  priority_queue = [] (실패한 작업 재할당용)
  completed = {} (성공한 작업)

  서버 소켓 오픈, 4개 Worker 연결 대기
  모든 Worker 연결 완료 시 -> 작업 분배 시작

분배 루프:
  while len(completed) < 5000:
    # 1. Priority Queue에 작업이 있으면 최우선 처리
    if priority_queue is not empty:
      task = priority_queue에서 꺼냄
    elif pending_queue is not empty:
      task = pending_queue에서 꺼냄
    else:
      # 모든 작업 분배 완료, 결과 대기
      결과 수신 대기
      continue

    # 2. 가장 여유 있는 Worker 선택
    target_worker = queue가 가장 적은 Worker 선택
    if target_worker.queue_size >= 10:
      # 모든 Worker가 꽉 참 -> 결과 수신 후 재시도
      결과 수신 대기
      continue

    # 3. 작업 전송
    target_worker에 task 전송
    system_clock += 1  # 통신 지연

    # 4. 결과 수신 처리 (비동기/이벤트 기반)
    수신된 결과 처리:
      SUCCESS -> completed에 추가
      FAIL -> priority_queue에 추가

종료:
  모든 Worker에 TERMINATE 전송
  최종 통계 출력
  Master.txt 저장
```

---

## 4단계: Worker Node 구현

### 4-1. Worker 핵심 기능 목록

- [ ] **Thread로 독립 실행**
- [ ] **Master에 소켓 연결**
- [ ] **Ready Queue 운영** (최대 10개)
  - 큐가 70% 초과(8개 이상) 상태에서 작업이 들고날 때마다 WARN 로그
  - 큐가 10개 초과 시 즉시 FAIL 처리
- [ ] **작업 처리 시뮬레이션**
  - 처리 시간: 1~3초 랜덤 (System Clock에 누적, 실제 sleep 아님)
  - 성공 확률: 80% (random으로 판정)
  - 성공 시 Master에 SUCCESS 전송
  - 실패 시 Master에 FAIL 전송
- [ ] **P2P 부하 분산**
  - 예상 대기시간 = 큐 잔여 작업 수 x 평균 처리시간(2초)
  - 임계값 15초 초과 시 인접 Worker에 큐 상태 조회
  - 가장 여유 있는 Worker에 작업 이전
  - ACK 수신 후 큐에서 제거
  - 1~3초 랜덤 주기로 반복 점검
- [ ] **P2P 소켓 통신** (Worker 간 직접 연결)
- [ ] **최종 통계 출력**
- [ ] **WorkerX.txt 로그 파일 생성**

### 4-2. Worker 의사코드

```
시작:
  Master에 소켓 연결
  ready_queue = []  # 최대 10개
  P2P 소켓 리스닝 시작 (다른 Worker가 연결할 수 있도록)

메인 루프:
  while not terminated:
    # 1. Master로부터 작업 수신
    if 새 작업 수신:
      if len(ready_queue) >= 10:
        FAIL 응답 (큐 초과)
        WARN 로그
      else:
        ready_queue에 추가
        if len(ready_queue) > 7:  # 70% 초과
          WARN 로그

    # 2. 큐에서 작업 꺼내 처리
    if ready_queue is not empty:
      task = ready_queue에서 꺼냄
      process_time = random(1, 3)
      system_clock += process_time

      if random() < 0.8:  # 80% 성공
        Master에 SUCCESS 전송
      else:  # 20% 실패
        Master에 FAIL 전송

    # 3. P2P 부하 분산 체크 (1~3초 랜덤 주기)
    expected_wait = len(ready_queue) * 2
    if expected_wait > 15:
      인접 Worker들에 큐 상태 조회 (P2P 소켓)
      가장 여유 있는 Worker에 작업 일부 이전
      ACK 수신 후 큐에서 제거

종료:
  최종 통계 출력
  WorkerX.txt 저장
```

### 4-3. P2P 통신을 위한 Worker 간 연결 구조

Worker들은 서로 직접 소켓 통신해야 한다. 두 가지 방법:

**방법 A: 각 Worker가 P2P용 서버 소켓도 열기**
- Worker1은 포트 9001, Worker2는 9002, Worker3은 9003, Worker4는 9004
- 서로의 IP:포트를 설정파일로 공유
- 필요 시 상대 Worker에 연결하여 통신

**방법 B: Master를 통해 Worker 주소 교환**
- Worker가 Master에 접속 시 자신의 P2P 포트를 알려줌
- Master가 모든 Worker의 IP:포트 목록을 전체에 broadcast
- 이후 Worker끼리 직접 연결

> 과제 특성상 **방법 A**가 더 간단하고 직관적이다.

---

## 5단계: 로그 시스템 구현

### 5-1. 공통 로그 형식

```
[clock] NODE | EVENT | STATUS | message
```

- **clock**: System Clock 값 (소수점 2자리)
- **NODE**: MASTER / WORKER1 / WORKER2 / WORKER3 / WORKER4
- **EVENT**: 자유 형식 (INIT, CONNECT, DISTRIB, RESULT, RECV, PROC, LB, QUEUE, STAT, TERMINATE 등)
- **STATUS**: INFO / SUCCESS / FAIL / WARN 중 하나만 사용

### 5-2. 필수 로그 항목

#### Master.txt에 반드시 포함:
- 초기화 (System Clock 시작, KV 생성)
- Worker 연결 (4개 모두)
- 작업 분배 (어떤 KV를 어떤 Worker에)
- 결과 수신 (SUCCESS/FAIL)
- 실패 재할당 (Priority Queue 등록 + 재할당)
- 주기적 진행률 (x/5000)
- **최종 통계 (STAT)**: 총 처리량, 성공/실패, 재할당 횟수, P2P 이벤트, 수행시간, Worker별 통계
- Graceful Termination

#### WorkerX.txt에 반드시 포함:
- 초기화 및 연결
- 작업 수신 (RECV)
- 작업 처리 (PROC) - 성공/실패
- 큐 상태 변화 (QUEUE) - 특히 WARN (70% 초과), FAIL (10개 초과)
- P2P 부하 분산 (LB) - 조회, 이전, ACK
- **최종 통계 (STAT)**: 총 수신, 성공/실패, 평균 대기시간, P2P 전송/수신, 수행시간
- Graceful Termination

### 5-3. AllDefinedLogs.txt 작성

프로그램에서 사용하는 모든 EVENT 종류를 명세한다.

```
# AllDefinedLogs.txt 예시

EVENT: INIT
설명: 시스템 초기화 관련 이벤트 (System Clock 시작, KV 생성, Worker Thread 시작)
사용 노드: MASTER, WORKER1~4
STATUS 사용: INFO, SUCCESS

EVENT: CONNECT
설명: 소켓 연결 관련 이벤트
사용 노드: MASTER, WORKER1~4
STATUS 사용: SUCCESS, FAIL

... (모든 EVENT에 대해 작성)
```

---

## 6단계: 성능 평가 지표 구현

최종 STAT 로그에 반드시 포함해야 하는 6개 필수 지표:

| # | 지표명 | 단위 | 어디서 측정 |
|---|---|---|---|
| 1 | Worker별 작업 처리량 | 건 | Master (전체), Worker (개별) |
| 2 | Worker별 성공/실패 횟수 | 건 | Master (전체), Worker (개별) |
| 3 | 작업 평균 대기시간 | sec | Worker (큐 진입 ~ 처리 시작) |
| 4 | P2P 부하 분산 이벤트 횟수 | 회 | Master (전체), Worker (개별) |
| 5 | 장애 재할당 횟수 | 건 | Master |
| 6 | 전체 수행시간 | sec | Master, Worker (System Clock 최종값) |

---

## 7단계: 통합 테스트

### 7-1. 테스트 순서

1. **로컬 테스트** (localhost로 먼저 테스트)
   - Master와 Worker 모두 로컬에서 실행
   - Master IP를 `127.0.0.1`로 설정
   - 기능 동작 확인

2. **소규모 테스트** (KV 100개로 축소 테스트)
   - 5,000개 전에 소규모로 전체 흐름 검증
   - 로그 형식 확인

3. **외부 서버 테스트**
   - Master를 클라우드 서버에 배포
   - 로컬 PC에서 Worker 4개 실행
   - 공인 IP로 실제 Socket 통신 확인

4. **전체 테스트** (KV 5,000개)
   - 전체 시나리오 실행
   - 로그 파일 5종 생성 확인
   - 통계 수치 검증 (성공 ~80%, 실패 ~20%)

### 7-2. 체크리스트

- [ ] Master가 5,000개 KV 정상 생성
- [ ] Worker 4개 모두 정상 연결
- [ ] 동적 분배가 큐 여유 기준으로 동작
- [ ] 큐 70% 초과 시 매번 WARN 로그
- [ ] 큐 10개 초과 시 FAIL 처리
- [ ] 80%/20% 성공/실패 비율 근사
- [ ] 실패 작업이 Priority Queue를 통해 재할당
- [ ] 재할당 작업도 실패 시 재재할당 (성공까지 반복)
- [ ] P2P 부하 분산이 실제로 발생 (예상 대기 > 15초 시)
- [ ] P2P 통신이 Socket으로 이루어짐
- [ ] 5,000개 모두 성공 처리 후 종료
- [ ] 최종 통계 6개 지표 모두 출력
- [ ] Graceful Termination
- [ ] 로그 파일 5종 정상 생성
- [ ] 로그 형식이 `[clock] NODE | EVENT | STATUS | message` 준수

---

## 8단계: 제출물 준비

### 8-1. 제출 파일 구조

```
G조이름HW1.zip
├── src/                    # 전체 소스 코드
│   ├── master.py (또는 .java, .c)
│   ├── worker.py
│   ├── config.json         # 설정 파일 (선택)
│   └── ...
├── AllDefinedLogs.txt      # 모든 로그 메시지 명세 및 설명
├── Master.txt              # Master 노드 실행 로그
├── Worker1.txt             # Worker1 실행 로그
├── Worker2.txt             # Worker2 실행 로그
├── Worker3.txt             # Worker3 실행 로그
├── Worker4.txt             # Worker4 실행 로그
├── download.txt            # 시연 영상 다운로드 링크
└── Readme.txt              # 아래 항목 모두 포함
```

### 8-2. Readme.txt 필수 포함 항목

1. **조원 이름, 학번, 역할 명시**
2. **프로그램 구성요소 설명**
3. **소스코드 컴파일 및 실행 방법** (컴파일/실행 불가 시 과제 전체 0점)
4. **프로그램 실행 환경 및 실행 방법**
5. **동적 작업 분배 알고리즘 설명** (장단점 분석 포함) - 알고리즘 명시 필수
6. **P2P 부하 분산 알고리즘 설명** (장단점 분석 포함) - 알고리즘 명시 필수
7. **장애 처리(Fault Tolerance) 메커니즘 설명**
8. **추가 구현 사항 및 기타 언급할 내용**

### 8-3. 시연 영상 (download.txt)

- 5분 이내 영상
- 전체 5,000건 처리 완료까지 보여줄 필요 없음
- **반드시 포함할 장면**:
  - 초기화 과정 (Master 시작, KV 생성)
  - Worker 4개 연결
  - 동적 작업 분배 동작
  - 장애(실패) 발생 및 재할당 과정
  - P2P 부하 분산 동작
  - 로그 출력 화면
- Google Drive 등에 업로드 후 링크 공유 (권한 설정 확인!)

---

## 주요 함정 & 주의사항

### System Clock 관련
- **실제 sleep 사용 금지**: 처리시간(1~3초), 통신지연(1초)은 System Clock 값에 더하기만 하고 실제 대기하지 않음
- 프로그램 자체는 실시간 대기 없이 빠르게 실행됨

### Ready Queue WARN 조건
- WARN은 "70% 초과 상태로 전이 시 1회"가 아님
- **큐가 70%를 초과한 상태에서 작업이 들어오거나 나갈 때마다 매번 WARN 기록**

### 재할당 규칙
- 실패한 작업은 Priority Queue에 등록 -> 가장 여유 있는 Worker에 재할당
- 재할당된 작업도 **동일한 80%/20% 규칙** 적용
- 또 실패하면 **성공할 때까지** 최우선 큐에 재등록하여 반복 재시도

### P2P 부하 분산
- 예상 대기시간 = 현재 큐 잔여 작업 수 x 평균 처리시간(2초)
- 임계값: 15초 (즉, 큐에 8개 이상이면 16초 > 15초로 트리거)
- 1~3초 랜덤 주기로 점검
- 부하 분산 알고리즘 자체는 자유 설계 (단, Readme에 명시 필수)

### 통신 지연
- 모든 노드 간 네트워크 지연은 **1초 고정**
- 실제 대기가 아닌 System Clock 값에 누적

---

## 추천 작업 분담 (4인 기준 예시)

| 역할 | 담당 내용 |
|---|---|
| A | Master Node 핵심 로직 (스케줄러, Priority Queue, 결과 수집) |
| B | Worker Node 핵심 로직 (Ready Queue, 작업 처리, 성공/실패 시뮬) |
| C | 소켓 통신 (Master-Worker, P2P Worker-Worker), 프로토콜 설계 |
| D | 로그 시스템, 통계 계산, 테스트, Readme/AllDefinedLogs 작성, 시연 영상 |

---

## 추천 일정

| 기간 | 작업 |
|---|---|
| 9/10~12 (3일) | 아키텍처 설계, 프로토콜 정의, 클라우드 서버 준비, 개발 환경 세팅 |
| 9/13~16 (4일) | Master/Worker 핵심 로직 구현, 로컬 테스트 |
| 9/17~19 (3일) | P2P 부하 분산, 로그 시스템, 통계 구현 |
| 9/20~21 (2일) | 외부 서버 배포, 통합 테스트, 버그 수정 |
| 9/22 (1일) | 시연 영상 촬영, Readme/AllDefinedLogs 작성, 최종 제출물 패키징 |
| 9/23 마감 | 최종 확인 후 제출 |
