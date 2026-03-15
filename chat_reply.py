#!/usr/bin/env python3
"""대시보드 채팅 응답 쓰기. 비서봇이나 수동으로 호출.
사용법: python3 chat_reply.py "응답 메시지"
"""
import json, os, sys, subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_PATH = os.path.join(BASE_DIR, 'docs', 'chat.json')

def reply(msg):
    try:
        with open(CHAT_PATH) as f:
            history = json.load(f)
    except:
        history = []

    history.append({
        'type': 'bot',
        'text': msg,
        'time': datetime.now().strftime('%m/%d %H:%M')
    })
    # 최근 50개만 유지
    history = history[-50:]

    with open(CHAT_PATH, 'w') as f:
        json.dump(history, f, ensure_ascii=False)

    # git push
    try:
        os.chdir(BASE_DIR)
        subprocess.run(['git', 'add', 'docs/chat.json'], capture_output=True, timeout=10)
        subprocess.run(['git', 'commit', '-m', f'chat: reply {datetime.now().strftime("%m/%d %H:%M")}'],
                       capture_output=True, timeout=10)
        subprocess.run(['git', 'push'], capture_output=True, timeout=30)
    except:
        pass

    print(f'✓ 응답 전송: {msg}')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        reply(' '.join(sys.argv[1:]))
    else:
        print('사용법: python3 chat_reply.py "응답 메시지"')
