==============================================================
HW#1 - 분산 Fault-Tolerant 키-값 저장소
==============================================================

1. 조원 정보
--------------------------------------------------------------
이름:           학번:           역할:
(여기에 작성)

2. 프로그램 구성요소
--------------------------------------------------------------
- Master Node (AWS EC2): 작업 생성/분배, 결과 수집, Priority Queue, KV Store
- Worker Node x4 (로컬 PC Thread): 작업 수신/처리, Ready Queue, P2P 부하 분산
- 통신: TCP Socket (4-byte length-prefixed JSON)
- System Clock: Lamport-style 가상 시계 (real sleep 없음)

3. 컴파일 및 실행 방법
--------------------------------------------------------------
언어: Python 3.13
외부 라이브러리: 없음 (표준 라이브러리만 사용)

[Master 실행 - AWS EC2]
  cd src
  python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs

[Worker 실행 - 로컬 PC]
  cd src
  python -m worker.main --master-ip <EC2-공인IP> --master-port 9000 --log-dir ../logs

4. 실행 환경
--------------------------------------------------------------
- Master: AWS EC2 (Ubuntu 22.04 LTS, t2.micro)
- Worker: Windows 11, Python 3.13
- 통신: TCP Socket (포트 9000)

5. 동적 작업 분배 알고리즘: Least-Queue-First
--------------------------------------------------------------
알고리즘명: Least-Queue-First with Priority Preemption

동작 방식:
  1. Priority Queue에 실패 작업이 있으면 최우선 할당
  2. 4개 Worker 중 estimated_queue가 가장 작은 Worker 선택
  3. estimated_queue = max(last_known_queue, in_flight)
  4. estimated_queue >= QUEUE_MAX(10)이면 해당 Worker 스킵

장점:
  - 구현이 단순하고 직관적
  - Queue 상태 기반으로 자연스러운 부하 분산
  - Priority Queue로 실패 작업 즉시 재할당

단점:
  - Master의 estimated_queue와 실제 Worker Queue 간 시간차 존재
  - 네트워크 지연으로 인해 정확한 실시간 상태 반영 어려움
  - Worker 처리 성능 차이를 고려하지 않음

6. P2P 부하 분산 알고리즘: Threshold-Triggered Greedy Pairwise Balancing
--------------------------------------------------------------
알고리즘명: Threshold-Triggered Greedy Pairwise Balancing

동작 방식:
  1. 각 Worker가 5~10초 랜덤 주기로 자신의 Queue 점검
  2. 예상 대기시간(queue_size x 평균처리시간 2초) > 임계값(6초)이면 트리거
  3. 모든 Peer Worker에 P2P_QUERY로 Queue 크기 조회
  4. 가장 여유 있는 Peer 선택, 차이의 절반만큼 작업 이전
  5. P2P_TRANSFER로 작업 전송, P2P_ACK로 수락/거절 확인
  6. 거절된 작업은 force_push로 자신의 Queue에 재삽입 (유실 방지)
  7. Master에 P2P_MOVED 알림 (estimated_queue 보정)

장점:
  - Worker 간 직접 통신으로 Master 병목 없음
  - 탐욕적(greedy) 접근으로 즉각적 부하 해소
  - ACK 기반으로 작업 유실 방지

단점:
  - P2P 통신 중 메인 처리 루프 블로킹
  - 여러 Worker가 동시에 같은 Peer에 전송 시도 가능
  - Clock 동기화로 인한 가상 시간 점프 발생 가능

7. 장애 처리(Fault Tolerance) 메커니즘
--------------------------------------------------------------
- Worker 작업 실패(20% 확률) 시 Master에 FAIL 전송
- Master는 실패 작업을 Priority Queue(heapq)에 등록
  - 정렬 기준: (-retry_count, sequence) → 많이 실패한 작업 우선
- 다음 분배 시 Priority Queue에서 먼저 꺼내 여유 Worker에 재할당
- 재할당 작업도 동일한 80%/20% 성공/실패 규칙 적용
- 성공할 때까지 무한 재시도
- Worker Queue 포화 시 REJECT → Master가 다시 Priority Queue에 등록

8. 추가 구현 사항
--------------------------------------------------------------
- Python 3.13 호환: threading.Thread._handle 충돌 해결
- P2P 거절 작업 유실 방지: force_push() 메커니즘
- P2P 통신에 별도 P2P_LATENCY(0.1초) 적용 (로컬 통신 특성 반영)
- Graceful Termination: TERMINATE → BYE 핸드셰이크
- 4-byte length-prefixed JSON 프레이밍으로 메시지 경계 보장
