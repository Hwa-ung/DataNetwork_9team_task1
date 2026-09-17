NUM_KV            = 5000
KEY_SPACE         = 0x10000          # hex 4자리: 0000~ffff
VALUE_MIN         = 1
VALUE_MAX         = 100

QUEUE_MAX         = 10
QUEUE_WARN_OVER   = 7                # size > 7 이면 WARN (70% 초과)
SUCCESS_RATE      = 0.8
PROC_TIME_MIN     = 1.0
PROC_TIME_MAX     = 3.0
NET_LATENCY       = 1.0              # Master-Worker 통신 지연 (가상)
P2P_LATENCY       = 0.1              # Worker 간 P2P 지연 (같은 로컬 PC)

AVG_PROC_TIME     = 2.0              # P2P 계산용
LB_THRESHOLD      = 6.0              # 예상 대기 임계값 (queue 4+ triggers P2P)
LB_CHECK_MIN      = 5.0
LB_CHECK_MAX      = 10.0

NUM_WORKERS       = 4
P2P_PORT_BASE     = 9100             # Worker N -> 9100+N

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
