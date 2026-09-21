================================================================================
HW#1 - 분산 Fault-Tolerant 키-값 저장소 (Distributed Fault-Tolerant KV Store)
9조
================================================================================


1. 조원 정보
================================================================================

  김대영 (20223087)
    - Worker Node 구현, 시연 영상, 문서 작성

  김민재 (20223089)
    - 소켓 통신, 프로토콜 설계, 로그 시스템

  황태웅 (20223152)
    - Master Node 구현, 인프라 관리 (EC2)


2. 프로그램 개요
================================================================================

프로그램 설명

  - Python 3.13+ 기반의 분산 키-값 저장소 시뮬레이터
  - Master Node: AWS EC2에서 실행
  - Worker Node: 4개, 로컬 PC의 독립 Thread로 동작
  - 통신: TCP Socket 기반

프로그램 구성요소

  src/common/                    공용 모듈 (Master/Worker 양쪽 사용)
    ├── protocol.py              4바이트 길이접두 + JSON 프레이밍 기반
    ├── clock.py                 VirtualClock - Lamport 논리 시계
    ├── logger.py                [clock] NODE | EVENT | STATUS | msg 형식
    └── constants.py             상수 정의 (큐 크기, 성공률, 지연 등)

  src/master/                    Master Node
    ├── main.py                  엔트리포인트 (소켓 서버, Worker 연결)
    ├── kv_store.py              5,000개 KV 쌍 생성 및 저장
    ├── scheduler.py             동적 작업 분배 (Fairness-Capped LFDT)
    ├── worker_session.py        Worker당 소켓 세션 관리
    ├── fault_handler.py         Priority Queue 기반 실패 작업 재할당
    └── stats.py                 최종 통계 출력

  src/worker/                    Worker Node
    ├── main.py                  엔트리포인트 (4개 WorkerNode Thread 기동)
    ├── node.py                  WorkerNode(Thread) - 메인 처리 루프
    ├── ready_queue.py           Ready Queue (최대 10개)
    ├── processor.py             작업 처리 시뮬레이션 (1~3초, 80/20 성공률)
    ├── p2p.py                   P2P 서버/클라이언트 (Worker 간 통신)
    └── stats.py                 Worker별 통계 집계

  src/config.json                설정 파일


3. 실행 환경 및 필수 준비물
================================================================================

실행 환경

  Master:      AWS EC2 (Ubuntu 22.04+, Python 3.13+)
  Worker:      로컬 PC (Windows/macOS/Linux, Python 3.13+)
  네트워크:    TCP Socket (포트 9000 - Master, 9101~9104 - P2P)

필수 준비물

  로컬 PC
    - Python 3.13 이상 (확인: python --version)
    - OpenSSH 클라이언트 (확인: ssh 명령어 실행)
    - 프로젝트 소스 (HW1/ 폴더)
    - EC2 키 파일 (*.pem 파일)

  AWS
    - EC2 인스턴스 (Ubuntu 22.04+, t2.micro)


4. AWS EC2 Master 노드 준비
================================================================================

4-1. EC2 인스턴스 생성

  1) AWS 콘솔 → EC2 → 인스턴스 시작
  2) AMI: Ubuntu 22.04 LTS 이상
  3) 인스턴스 유형: t2.micro (프리티어 가능)

4-2. 보안 그룹 설정

  인바운드 규칙 추가:

    SSH (TCP 22)
      소스: 내 IP
      용도: SSH 접속

    사용자 지정 TCP (TCP 9000)
      소스: 0.0.0.0/0
      용도: Master ↔ Worker

  주의: P2P 포트(9101~9104)는 로컬 루프백만 사용하므로 개방 불필요

4-3. EC2 인스턴스 시작

  1) AWS 콘솔에서 인스턴스 시작
  2) 상태가 "running"이 될 때까지 대기
  3) 공인 IP (Public IP) 복사

4-4. SSH 키 파일 권한 설정

  Windows PowerShell:

    icacls "$HOME\.ssh\mykey.pem" /inheritance:r
    icacls "$HOME\.ssh\mykey.pem" /grant:r "$($env:USERNAME):(R)"

  macOS/Linux:

    chmod 600 ~/.ssh/mykey.pem

4-5. SSH 접속 및 초기 설정

  로컬 PC 터미널:

    ssh -i ~/.ssh/mykey.pem ubuntu@13.61.180.113
    mkdir -p ~/hw1


5. 소스코드 업로드 (EC2)
================================================================================

로컬 PC에서 EC2로 코드 전송

  scp -i ~/.ssh/mykey.pem -r HW1/src HW1/scripts \
    ubuntu@13.61.180.113:~/hw1/

결과: EC2에 ~/hw1/src와 ~/hw1/scripts가 생성됨


6. 명령행 인자 참고
================================================================================

Master 인자

  --port            리스닝 포트 (기본: 9000)
  --num-kv          KV 쌍 개수 (기본: 5000)
  --log-dir         로그 디렉터리 (기본: ./logs)
  --lb-demo         P2P 부하 분산 시연 모드 (초기 40건을 Worker1에 집중)

Worker 인자

  --master-ip       Master IP 주소 (기본: 127.0.0.1)
  --master-port     Master 포트 (기본: 9000)
  --num-workers     Worker 스레드 수 (기본: 4)
  --p2p-port-base   P2P 포트 시작번호 (기본: 9100)
  --log-dir         로그 디렉터리 (기본: ./logs)

참고: src/config.json에서도 설정 가능 (명령행 인자 우선)


7. 프로그램 실행
================================================================================

7-1. Master 실행 (EC2)

  스크립트 사용:

    bash ~/hw1/scripts/run_master.sh

  또는 직접 실행:

    cd ~/hw1/src
    python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs

  P2P 시연 모드:

    python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs --lb-demo

  정상 시작 메시지:

    INIT | INFO | Listening on 0.0.0.0:9000, waiting for 4 workers...

7-2. 연결 테스트 (로컬 PC)

  Windows PowerShell:

    Test-NetConnection 13.61.180.113 -Port 9000

  Linux/macOS:

    nc -zv 13.61.180.113 9000

  성공 메시지:
    - Windows: "TcpTestSucceeded : True"
    - Linux/macOS: "succeeded"

7-3. Worker 실행 (로컬 PC)

  새 로컬 PC 터미널:

    cd HW1/src
    python -m worker.main --master-ip 13.61.180.113 --master-port 9000 \
      --log-dir ../logs

  또는 스크립트 사용 (Windows):

    cd HW1
    .\scripts\run_worker.ps1 -MasterIP 13.61.180.113

  정상 실행 시:
    - Worker 창: Connected to Master (4번 출력)
    - Master 창: Worker1~4 connected 출력
    - 작업 분배 시작
    - Windows 방화벽 팝업이 뜨면 Python 허용

7-4. 완료 및 종료

  정상 완료:
    - 5,000건 모두 처리 완료
    - Master가 TERMINATE 전송
    - Worker가 BYE 응답
    - 자동 종료

  Worker 창 출력:

    All worker threads finished.

  중간 종료:

    양쪽 모두 Ctrl+C 누르기


8. 로컬 전용 테스트 (EC2 없이)
================================================================================

한 PC에서 전체 동작을 검증

  터미널 1 - Master:

    cd HW1/src
    python -m master.main --port 9000 --num-kv 50 --log-dir ../logs

  터미널 2 - Worker (Master 출력 후):

    cd HW1/src
    python -m worker.main --master-ip 127.0.0.1 --master-port 9000 \
      --log-dir ../logs

  검증 사항:

    ✓ KV store committed: 50/50 확인
    ✓ 재할당 및 P2P 동작 확인
    ✓ 정상 종료 확인


9. 로그 회수
================================================================================

Master 로그 회수 (로컬 PC에서)

  Master 로그는 EC2에만 있으므로 로컬로 복사:

    scp -i ~/.ssh/mykey.pem ubuntu@13.61.180.113:~/hw1/logs/Master.txt \
      ./HW1/logs/

Worker 로그 회수

  Worker 로그는 이미 로컬 HW1/logs/에 위치:
    - Worker1.txt
    - Worker2.txt
    - Worker3.txt
    - Worker4.txt


10. EC2 인스턴스 종료
================================================================================

AWS 콘솔에서:

  1) EC2 → 인스턴스
  2) 해당 인스턴스 선택
  3) 인스턴스 상태 → 중지

AWS CLI 사용:

  aws ec2 describe-instances --instance-ids i-07bed574403dfe75b \
    --region eu-north-1 \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text


11. 문제 해결
================================================================================

Worker가 Master에 연결 실패

  원인: Master 미실행 또는 포트 닫힘
  해결: Master 먼저 실행, 보안그룹 확인

Address already in use

  원인: 이전 실행이 종료 안 됨
  해결: 프로세스 강제 종료 후 재실행

P2P 부하 분산이 안 됨

  원인: 작업이 균등 분배됨
  해결: --lb-demo 플래그 사용

SSH 끊기면 Master 종료

  원인: SSH 연결 유지 필요
  해결: 안정적인 네트워크 사용

SSH 접속 권한 거부

  원인: .pem 파일 권한 문제
  해결: 11 단계 참고


================================================================================

A. 동적 작업 분배 알고리즘
================================================================================

알고리즘명

  Fairness-Capped Least-Dispatched-First
  (공평성 제한 최소배정 우선)

동작 원리

  1단계: 실시간 모니터링
    - estimated_queue = max(last_known_queue, in_flight)
    - last_known_queue: Worker가 보고한 실제 큐 크기
    - in_flight: Master가 보냈지만 결과를 못 받은 작업 수

  2단계: 대상 선정 기준 (우선순위 순)
    a) estimated_queue < 10 (큐 포화 Worker 제외)
    b) 공평성 제한: dispatched < min_dispatched + 5
       (최소 배정 Worker 대비 5건 이상 앞서면 대기)
    c) 1차 정렬: 누적 배정 수(dispatched) 오름차순
    d) 2차 정렬: 추정 큐 크기(estimated_queue) 오름차순

  3단계: Priority Queue 우선 배정
    - 실패 작업을 일반 작업보다 최우선 배정

장점

  ✓ 균등 분배 보장: Worker 간 배정 차이 최대 5건 이내
  ✓ 편중 없음: 실제 네트워크 환경에 적응
  ✓ 가벼운 구현: O(N) 복잡도 (N=4)
  ✓ 자동 적응: 처리 속도 편차 대응

단점

  ✗ 지연된 정보: 통신 지연으로 큐 상태 낡을 가능성
  ✗ 순간 과분배: 공평성 제한으로 빈 Worker도 잠시 대기
  ✗ 단일 장애점: Master의 전역 정보 의존


================================================================================

B. P2P 부하 분산 알고리즘
================================================================================

알고리즘명

  Threshold-triggered Greedy Pairwise Balancing
  (임계 기반 탐욕적 1:1 균형화)

동작 원리

  1단계: 트리거 판정 (1~3초 가상 시간 랜덤 주기)
    - 예상 대기시간 = 큐 크기 × 평균 처리시간(2초)
    - 조건: 대기시간 > 15초 && 큐 ≥ 8개
    - 만족 시 P2P 부하 분산 시작

  2단계: 이웃 조회
    - 다른 3개 Worker에 P2P 소켓으로 큐 상태 조회 (P2P_QUERY)
    - 가장 여유 있는 Worker를 이전 대상으로 선택

  3단계: 작업 이전
    - 이전 개수: k = min((내 큐 - 상대 큐) / 2, 상대 여유분)
    - 격차의 절반만 이전 (핑퐁 방지)
    - 큐의 뒤쪽(늦게 처리될) 작업 선택

  4단계: ACK 기반 안전 이전
    - 수용한 작업만 원본 큐에서 제거
    - 거절된 작업은 force_push로 복원
    - P2P_MOVED로 Master에 통보

P2P 통신 구조

  - 서버: Worker N이 포트 9100+N에서 리스닝
  - 주소: 127.0.0.1 (같은 로컬 PC 내)
  - 지연: 0.1초 (가상)
  - Master 거치지 않음: 직접 Worker-Worker 통신

장점

  ✓ Master 병목 완화: 자율 조정
  ✓ 핑퐁 방지: 격차의 절반만 이전
  ✓ 유실 방지: ACK 후 삭제
  ✓ 정확도 유지: P2P_MOVED 통보

단점

  ✗ 오버헤드: 조회 왕복 시 가상 시계 증가
  ✗ 국소 최적: 전체 최적 아님 (pairwise greedy)
  ✗ 블로킹: ACK 대기 중 해당 Worker 정지


================================================================================

C. 장애 처리 (Fault Tolerance) 메커니즘
================================================================================

실패 작업 재할당

  1) 실패 감지:
     - Worker가 작업 처리 실패(20% 확률) → Master에 FAIL 보고

  2) 우선순위 관리:
     - Master가 Priority Queue에 등록 (heapq 기반)
     - 재시도 횟수 증가 시 우선순위 상승 (-retry 기준)

  3) 재배정:
     - 다음 분배 루프에서 일반 작업보다 최우선
     - 동일한 80%/20% 성공/실패 규칙 적용
     - 성공할 때까지 무한 반복

큐 오버플로 (REJECT) 처리

  - 발생: Ready Queue가 10/10 포화
  - 처리: REJECT 작업도 Priority Queue에 등록 후 재할당
  - 예방: 공평성 제한 스케줄러로 발생 최소화 (실측 0건)

큐 상태 모니터링

  - 임계값: 70% 초과 (8개 이상)
  - 로그: 진입/퇴출 시 WARN 기록
  - 추정: Master의 estimated_queue 실시간 추정
  - 보정: RESULT의 queue_size로 실제 값 보정

P2P 이전 시 안전 보장

  ✓ ACK 수신 후 원본 큐에서 제거 (유실 방지)
  ✓ 거절된 작업은 force_push로 복원
  ✓ P2P_MOVED로 Master에 통보하여 동기화

Graceful Termination

  1) 5,000건 모두 성공 처리
  2) Master가 TERMINATE 전송
  3) 각 Worker는 BYE + 최종 통계 반환
  4) 모든 소켓과 로그 파일 정상 close


================================================================================

D. 추가 구현 사항
================================================================================

System Clock - Lamport 논리 시계

  - Master/Worker: 독립적인 VirtualClock 인스턴스
  - 송신: clock 필드에 현재 시간 첨부
  - 수신: max(내 시간, 상대 시간 + 네트워크 지연) 동기화
  - 특징: 가상 시간 (time.sleep() 미사용)
  - 성능: 5,000건 처리가 수 초 내 완료

프로토콜 설계

  - 형식: 4바이트 빅엔디언 길이접두 + UTF-8 JSON
  - 목적: TCP 스트림 메시지 경계 보장 (필수)
  - 메시지 타입: HELLO, PEERS, START, TASK, RESULT, P2P_QUERY,
                 P2P_QUERY_RES, P2P_TRANSFER, P2P_ACK, P2P_MOVED,
                 TERMINATE, BYE
  - 안전성: 소켓별 send_lock으로 멀티스레드 동시쓰기 방지

스레드 구성 (Worker 1개당)

  - WorkerNode: 메인 처리 스레드 (명세의 "독립 Thread")
  - MasterReceiver: Master 소켓 수신 전담 (blocking recv 분리)
  - P2PServer: P2P 포트 리스닝 (조회/이전 요청 수신)
  - GIL 영향: I/O 바운드 + 가상시간 모델이므로 무관

--lb-demo 모드 (P2P 시연용)

  - 목적: 초기 40개 작업을 Worker1에 집중 배정
  - 효과: Worker1 큐 빠른 축적 → P2P 부하 분산 자연스럽게 트리거
  - 구현: Worker 처리 루프에 5ms yield 삽입
  - 사용: Master에 --lb-demo 플래그 지정

네트워크 지연 모델

  - Master-Worker: 1.0초 (가상, 수신측에서만 적용)
  - Worker-Worker(P2P): 0.1초 (로컬 PC 통신)


================================================================================

E. 성능 지표 (EC2 실측, --lb-demo 모드)
================================================================================

작업 분배

  Worker1: 1,887건
  Worker2: 1,875건
  Worker3: 1,451건
  Worker4: 1,120건

  (lb-demo로 인한 편향 포함)

작업 처리 결과

  SUCCESS: 5,000건
  FAIL: 1,333건
  REJECT: 0건

성능 지표

  평균 대기시간: 3.59초
  P2P 부하 분산: 7회 (21건 이전)
  장애 재할당: 1,333건
  전체 수행시간: 3,839.85초 (System Clock)


================================================================================