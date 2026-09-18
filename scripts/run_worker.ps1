# 로컬 PC에서 Worker 실행
# 사용법: PowerShell에서 .\scripts\run_worker.ps1
param(
    [string]$MasterIP = "127.0.0.1",
    [int]$MasterPort = 9000
)

Set-Location "$PSScriptRoot\..\src"
python -m worker.main --master-ip $MasterIP --master-port $MasterPort --log-dir ../logs
