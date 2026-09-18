================================================================================
  HW#1 - 분산 Fault-Tolerant 키-값 저장소 (Distributed Fault-Tolerant KV Store)
  9조
================================================================================

1. 조원 정보
--------------------------------------------------------------------------------
  이름        학번            역할
  --------    ------------    --------------------------------------------------
  (이름1)     (학번1)         Master Node 구현 (스케줄러, Priority Queue, 통계)
  (이름2)     (학번2)         Worker Node 구현 (Ready Queue, 처리 시뮬레이션)
  (이름3)     (학번3)         소켓 통신 (Master-Worker, P2P), 프로토콜 설계
  (이름4)     (학번4)         인프라(EC2), 로그 시스템, 문서, 시연 영상


2. 프로그램 구성요소 설명
--------------------------------------------------------------------------------
  본 프로그램은 Python 3.13+ 기반의 분산 키-값 저장소 시뮬레이터이다.
  Master Node는 AWS EC2에서 실행되고, 4개의 Worker Node는 로컬 PC에서
  독립적인 Thread로 동작한다. 모든 통신은 TCP Socket으로 수행된다.

  [구성 모듈]

  src/common/          공용 모듈 (Master/Worker 양쪽에서 사용)
    protocol.py        4바이트 길이접두 + JSON 프레이밍 기반 메시지 송수신
    clock.py           VirtualClock - Lamport 방식 논리 시계 (가상 시간)
    logger.py          [clock] NODE | EVENT | STATUS | msg 형식 로그 출력
    constants.py       모든 상수 정의 (큐 크기, 성공률, 지연 등)

  src/master/          Master Node
    main.py            엔트리포인트 (소켓 서버, Worker 연결, 스케줄러 실행)
    kv_store.py        5,000개 KV 쌍 생성 및 성공 시 확정 저장
    scheduler.py       동적 작업 분배 (Fairness-Capped Least-Dispatched-First)
    worker_session.py  Worker 1개당 소켓 세션 관리 (큐 추정, 결과 수신)
    fault_handler.py   Priority Queue 기반 실패 작업 재할당
    stats.py           최종 통계 출력

  src/worker/          Worker Node
    main.py            엔트리포인트 (4개 WorkerNode Thread 기동)
    node.py            WorkerNode(Thread) - 메인 처리 루프, P2P LB 판단
    ready_queue.py     Ready Queue (최대 10, 70% 초과 WARN)
    processor.py       작업 처리 시뮬레이션 (1~3초 가상, 80/20 성공/실패)
    p2p.py             P2P 서버/클라이언트 (Worker 간 직접 소켓 통신)
    stats.py           Worker별 통계 집계

  src/config.json      설정 파일 (Master IP/포트, KV 개수 등)


3. 소스코드 컴파일 및 실행 방법
--------------------------------------------------------------------------------
  [필수 환경]
  - Python 3.13 이상 (표준 라이브러리만 사용, 별도 패키지 설치 불필요)
  - 컴파일 불필요 (인터프리터 언어)

  [Master 실행 - EC2 서버]
  $ cd src
  $ python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs

  [Worker 실행 - 로컬 PC]
  $ cd src
  $ python -m worker.main --master-ip <EC2_PUBLIC_IP> --master-port 9000 \
                           --log-dir ../logs

  [localhost 테스트 (Master/Worker 모두 로컬)]
  터미널 1: python3 -m master.main --port 9000 --log-dir ../logs
  터미널 2: python -m worker.main --master-ip 127.0.0.1 --master-port 9000 \
                                   --log-dir ../logs

  [명령행 인자]
  Master:
    --port        리스닝 포트 (기본: 9000)
    --num-kv      KV 쌍 개수 (기본: 5000)
    --log-dir     로그 출력 디렉터리 (기본: ./logs)

  Worker:
    --master-ip   Master IP 주소
    --master-port Master 포트 (기본: 9000)
    --num-workers Worker 스레드 수 (기본: 4)
    --p2p-port-base P2P 포트 시작번호 (기본: 9100, Worker N -> 9100+N)
    --log-dir     로그 출력 디렉터리 (기본: ./logs)


4. 프로그램 실행 환경 및 실행 방법
--------------------------------------------------------------------------------
  [실행 환경]
  - Master: AWS EC2 (eu-north-1), Ubuntu, Python 3.14
  - Worker: Windows 11, Python 3.13
  - 네트워크: TCP Socket (포트 9000 - Master, 9101~9104 - P2P)

  [실행 순서]
  1) EC2에 SSH 접속 후 tmux 세션 생성
     $ ssh -i HaEeee.pem ubuntu@<EC2_IP>
     $ tmux new -s master

  2) Master 실행
     $ cd ~/hw1/src
     $ python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs
     (Master가 Worker 4개 연결을 대기)

  3) 로컬 PC에서 Worker 실행
     $ cd src
     $ python -m worker.main --master-ip <EC2_IP> --master-port 9000 \
                              --log-dir ../logs
     (Worker 4개 Thread가 자동 생성되어 Master에 연결)

  4) 5,000건 처리 완료 후 자동 종료
     - logs/ 디렉터리에 Master.txt, Worker1~4.txt 생성

  [EC2 보안 그룹 설정]
  - TCP 9000번 포트: 인바운드 허용 (0.0.0.0/0)
  - TCP 22번 포트: SSH 접속용


5. 동적 작업 분배 알고리즘 설명
--------------------------------------------------------------------------------
  알고리즘명: Fairness-Capped Least-Dispatched-First
              (공평성 제한 최소배정 우선)

  [동작 원리]
  1) 각 Worker의 추정 큐 크기(estimated_queue)를 실시간 모니터링한다.
     - estimated_queue = max(last_known_queue, in_flight)
     - last_known_queue: Worker가 RESULT에 보고한 실제 큐 크기
     - in_flight: Master가 보냈지만 아직 결과를 못 받은 작업 수

  2) 대상 선정 기준 (우선순위 순):
     a) estimated_queue < 10 (큐가 꽉 찬 Worker 제외)
     b) 공평성 제한: dispatched < min_dispatched + 5
        (가장 적게 배정받은 Worker 대비 5건 이상 앞서면 배정 대기)
     c) 1차 정렬: 누적 배정 수(dispatched) 오름차순
     d) 2차 정렬: 추정 큐 크기(estimated_queue) 오름차순

  3) Priority Queue의 실패 작업을 일반 작업보다 최우선 배정한다.

  [장점]
  - 균등 분배 보장: 공평성 제한으로 Worker 간 배정 차이가 최대 5건 이내
  - 실제 네트워크 환경에서도 편중 없음 (피드백 루프 방지)
  - O(N) 복잡도 (N=4)로 매우 가볍고 구현 단순
  - 큐 길이 기준 2차 정렬로 처리 속도 편차에 자동 적응

  [단점]
  - Master가 가진 큐 상태가 통신 지연만큼 낡을 수 있음 (순간 과분배 가능)
  - 공평성 제한으로 인해 빈 Worker가 있어도 잠시 대기할 수 있음
  - 전역 정보 의존으로 Master가 단일 장애점(SPOF)


6. P2P 부하 분산 알고리즘 설명
--------------------------------------------------------------------------------
  알고리즘명: Threshold-triggered Greedy Pairwise Balancing
              (임계 기반 탐욕적 1:1 균형화)

  [동작 원리]
  1) 트리거 판정 (5~10초 가상 시간 랜덤 주기로 점검):
     - 예상 대기시간 = 현재 큐 크기 x 평균 처리시간(2초)
     - 임계값(6초) 초과 시 P2P 부하 분산 시작 (큐 4개 이상)

  2) 이웃 조회:
     - 다른 3개 Worker에 P2P 소켓으로 큐 상태 조회 (P2P_QUERY)
     - 가장 여유 있는 Worker를 이전 대상으로 선택

  3) 작업 이전:
     - 이전 개수 k = min((내 큐 - 상대 큐) / 2, 상대 여유분)
     - 격차의 절반만 이전하여 핑퐁(무한 왕복) 방지
     - 큐의 뒤쪽(가장 늦게 처리될) 작업을 선택

  4) ACK 기반 안전 이전:
     - 상대 Worker가 수용한 작업만 자기 큐에서 제거
     - 거절된 작업은 force_push로 자기 큐에 복원 (유실 방지)
     - P2P_MOVED 메시지로 Master에 이전 사실 통보 (큐 추정치 보정)

  [P2P 통신 구조]
  - Worker N은 포트 9100+N에서 P2P 서버 리스닝
  - 같은 로컬 PC 내 127.0.0.1 통신 (P2P 지연: 0.1초 가상)
  - Master를 거치지 않는 직접 Worker-Worker 소켓 통신

  [장점]
  - Master 개입 없이 Worker끼리 자율 조정 -> Master 병목 완화
  - 격차의 절반만 이전하여 핑퐁(무한 왕복) 방지
  - ACK 후 삭제로 이전 중 작업 유실 없음
  - P2P_MOVED 통보로 Master 큐 추정치 정확도 유지

  [단점]
  - 조회 왕복마다 가상 시계 증가 -> 과도 호출 시 오버헤드
  - 전체 최적이 아닌 국소 최적 (pairwise greedy)
  - ACK 대기 중 해당 Worker 블로킹


7. 장애 처리(Fault Tolerance) 메커니즘 설명
--------------------------------------------------------------------------------
  [실패 작업 재할당]
  - Worker가 작업 처리 실패(20% 확률) 시 Master에 FAIL 보고
  - Master는 해당 작업을 Priority Queue에 등록 (heapq 기반)
  - 재시도 횟수가 많을수록 우선순위 상승 (-retry 기준 min-heap)
  - 다음 분배 루프에서 일반 작업보다 최우선 배정
  - 재할당된 작업도 동일한 80%/20% 성공/실패 규칙 적용
  - 성공할 때까지 무한 반복 재시도

  [큐 오버플로(REJECT) 처리]
  - Worker의 Ready Queue가 10/10 포화 시 REJECT 응답
  - REJECT된 작업도 Priority Queue에 등록되어 재할당
  - 공평성 제한 스케줄러로 REJECT 발생 자체를 최소화 (실측 0건)

  [큐 상태 모니터링]
  - 큐 크기가 70% 초과(8개 이상)인 상태에서 작업 진입/퇴출 시 매번 WARN 로그
  - Master는 각 Worker의 큐 상태를 estimated_queue로 실시간 추정
  - Worker RESULT 메시지의 queue_size로 실제 값 보정

  [P2P 이전 시 안전 보장]
  - ACK 수신 후에만 원본 큐에서 제거 (유실 방지)
  - 거절된 작업은 force_push로 원래 큐에 복원
  - P2P_MOVED로 Master에 통보하여 큐 추정치 동기화

  [Graceful Termination]
  - 5,000건 모두 성공 처리 후 Master가 TERMINATE 전송
  - 각 Worker는 BYE 메시지와 함께 최종 통계 반환
  - 모든 소켓과 로그 파일 정상 close


8. 추가 구현 사항 및 기타
--------------------------------------------------------------------------------
  [System Clock - Lamport 논리 시계]
  - Master/Worker 각각 독립적인 VirtualClock 인스턴스 운영
  - 메시지 송신: clock 필드에 현재 시간 첨부
  - 메시지 수신: max(내 시간, 상대 시간 + 네트워크 지연)으로 동기화
  - 모든 시간은 가상 시간이며 time.sleep()을 사용하지 않음
  - 5,000건 처리가 수 초 내 완료됨 (실시간 대기 없음)

  [프로토콜 설계]
  - 4바이트 빅엔디언 길이접두 + UTF-8 JSON 프레이밍
  - TCP 스트림 특성상 메시지 경계 보장을 위한 필수 설계
  - 메시지 타입: HELLO, PEERS, START, TASK, RESULT, P2P_QUERY,
    P2P_QUERY_RES, P2P_TRANSFER, P2P_ACK, P2P_MOVED, TERMINATE, BYE
  - 소켓별 send_lock으로 멀티스레드 환경 동시쓰기 방지

  [스레드 구성 (Worker 1개당)]
  - WorkerNode: 메인 처리 스레드 (명세의 "독립 Thread")
  - MasterReceiver: Master 소켓 수신 전담 (blocking recv 분리)
  - P2PServer: P2P 포트 리스닝 (조회/이전 요청 수신)
  - GIL은 I/O 바운드 + 가상시간 모델이므로 성능에 무관

  [네트워크 지연 모델]
  - Master-Worker 간: 1.0초 (가상, 수신측에서만 적용)
  - Worker-Worker 간(P2P): 0.1초 (같은 로컬 PC이므로 축소)

  [성능 지표 (EC2 실측)]
  - Worker별 배정: 1558 / 1558 / 1558 / 1557 (균등)
  - 총 SUCCESS: 5,000건, FAIL: 1,231건, REJECT: 0건
  - 평균 대기시간: 0.39초
  - P2P 부하 분산: 5회
  - 장애 재할당: 1,231건
  - 전체 수행시간: 3,261.78초 (System Clock)

================================================================================
