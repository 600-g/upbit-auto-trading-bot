#!/bin/bash
# 안전 시작 스크립트 — 기존 봇 모두 죽이고 단일 인스턴스 보장
cd /Users/600mac/Desktop/업비트자동
# 모든 매매봇 프로세스 종료
pkill -9 -f "upbit_bot_v3_0_complete.py" 2>/dev/null
sleep 1
# 잔존 확인
if ps aux | grep "upbit_bot_v3" | grep -v grep > /dev/null; then
  echo "⚠️ 일부 프로세스 잔존, 강제 종료"
  ps aux | grep "upbit_bot_v3" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
  sleep 1
fi
# 새 봇 시작
nohup ./venv/bin/python3 -u upbit_bot_v3_0_complete.py demo --auto >> logs/bot_output.log 2>&1 &
echo $! > bot.pid
echo "✅ 봇 시작: PID $(cat bot.pid)"
