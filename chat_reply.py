#!/usr/bin/env python3
"""대시보드 채팅 응답 (Firebase Firestore 실시간)
사용법: python3 chat_reply.py "응답 메시지"
"""
import json, os, sys
from datetime import datetime

def reply(msg):
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        # 초기화 (중복 방지)
        if not firebase_admin._apps:
            # 서비스 계정 키 없이 프로젝트 ID만으로 초기화
            cred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'firebase-key.json')
            if os.path.exists(cred_path):
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            else:
                # REST API 방식 fallback
                _reply_rest(msg)
                return

        db = firestore.client()
        now = datetime.now()
        db.collection('upbit_chat').add({
            'type': 'bot',
            'text': msg,
            'time': now.strftime('%m/%d %H:%M'),
            'ts': firestore.SERVER_TIMESTAMP
        })
        print(f'✓ 응답 전송 (Firebase): {msg[:50]}...' if len(msg) > 50 else f'✓ 응답 전송 (Firebase): {msg}')

    except Exception as e:
        print(f'Firebase 실패 ({e}), REST 방식 시도...')
        _reply_rest(msg)

def _reply_rest(msg):
    """REST API로 Firestore에 직접 쓰기"""
    import requests
    now = datetime.now()
    url = "https://firestore.googleapis.com/v1/projects/datemap-759bf/databases/(default)/documents/upbit_chat"
    data = {
        "fields": {
            "type": {"stringValue": "bot"},
            "text": {"stringValue": msg},
            "time": {"stringValue": now.strftime('%m/%d %H:%M')},
            "ts": {"timestampValue": now.strftime('%Y-%m-%dT%H:%M:%S.000Z')}
        }
    }
    r = requests.post(url, json=data, timeout=10)
    if r.status_code in (200, 201):
        print(f'✓ 응답 전송 (REST): {msg[:50]}...' if len(msg) > 50 else f'✓ 응답 전송 (REST): {msg}')
    else:
        print(f'✕ 전송 실패: {r.status_code} {r.text[:100]}')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        reply(' '.join(sys.argv[1:]))
    else:
        print('사용법: python3 chat_reply.py "응답 메시지"')
