#!/bin/bash
# EC2에서 Master 실행
# 사용법: tmux new -s master 안에서 실행
cd "$(dirname "$0")/../src"
python3 -m master.main --port 9000 --num-kv 5000 --log-dir ../logs
