#!/bin/bash
# ============================================
# watchdog.sh - 업비트 자동거래봇 감시 스크립트
# 봇 프로세스가 죽었으면 자동으로 재시작 + 텔레그램 알림
# ============================================

BOT_DIR="/Users/600mac/Desktop/업비트자동"
PID_FILE="${BOT_DIR}/bot.pid"
LOG_FILE="${BOT_DIR}/watchdog.log"
PYTHON="${BOT_DIR}/venv/bin/python"
BOT_SCRIPT="${BOT_DIR}/upbit_bot_v3_0_complete.py"
BOT_STDOUT="${BOT_DIR}/bot_output.log"

# 텔레그램 설정
TG_TOKEN="8481220569:AAHVSypL0kUN21ehhxG0uKGCz3PpIXkdm28"
TG_CHAT_ID="8440882806"

log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

tg_send() {
    curl -s -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        -d "chat_id=${TG_CHAT_ID}" \
        -d "text=$1" > /dev/null 2>&1
}

# 로그 파일 크기 관리 (10MB 초과 시 로테이션)
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE=$(stat -f%z "$LOG_FILE" 2>/dev/null || stat --format=%s "$LOG_FILE" 2>/dev/null || echo 0)
    if [ "$LOG_SIZE" -gt 10485760 ]; then
        mv "$LOG_FILE" "${LOG_FILE}.old"
        log_msg "로그 로테이션 완료"
    fi
fi

start_bot() {
    cd "$BOT_DIR"
    nohup "$PYTHON" -u "$BOT_SCRIPT" demo --auto >> "$BOT_STDOUT" 2>&1 &
    NEW_PID=$!
    echo "$NEW_PID" > "$PID_FILE"
}

# PID 파일 존재 여부 확인
if [ ! -f "$PID_FILE" ]; then
    log_msg "PID 파일 없음 - 봇 최초 시작"
    start_bot
    log_msg "봇 시작 완료 (PID: $(cat $PID_FILE))"
    tg_send "🔄 봇 최초 시작 (PID: $(cat $PID_FILE))"
    exit 0
fi

# PID 파일에서 PID 읽기
BOT_PID=$(cat "$PID_FILE" 2>/dev/null)

if [ -z "$BOT_PID" ]; then
    log_msg "PID 파일이 비어있음 - 봇 재시작"
    start_bot
    log_msg "봇 재시작 완료 (PID: $(cat $PID_FILE))"
    tg_send "⚠️ PID 파일 비어있음 → 봇 재시작 (PID: $(cat $PID_FILE))"
    exit 0
fi

# 프로세스 생존 확인
if kill -0 "$BOT_PID" 2>/dev/null; then
    # 프로세스가 살아있음 - 정상
    exit 0
else
    # 프로세스가 죽어있음 - 재시작
    log_msg "봇 프로세스(PID: $BOT_PID) 사망 감지 - 재시작"
    tg_send "🚨 봇 프로세스 사망 감지 (PID: $BOT_PID) → 재시작 중..."
    start_bot
    log_msg "봇 재시작 완료 (새 PID: $(cat $PID_FILE), 이전 PID: $BOT_PID)"
    tg_send "✅ 봇 재시작 완료 (새 PID: $(cat $PID_FILE))"
    exit 0
fi
