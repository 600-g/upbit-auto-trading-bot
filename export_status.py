#!/usr/bin/env python3
"""status.json 생성 + git push (5분마다 cron/봇에서 호출)"""

import sqlite3
import json
import os
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'trading_bot_v3.db')
POS_PATH = os.path.join(BASE_DIR, 'positions.json')
OUT_PATH = os.path.join(BASE_DIR, 'docs', 'status.json')
PID_PATH = os.path.join(BASE_DIR, 'bot.pid')

def export():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Total
    c.execute('SELECT COUNT(*), SUM(CASE WHEN profit>0 THEN 1 ELSE 0 END), SUM(CASE WHEN profit<0 THEN 1 ELSE 0 END), SUM(CASE WHEN profit=0 THEN 1 ELSE 0 END), SUM(profit) FROM trades WHERE action="sell"')
    row = c.fetchone()
    total = row[0] or 0
    wins = row[1] or 0
    losses = row[2] or 0
    draws = row[3] or 0
    total_pnl = row[4] or 0
    win_rate = (wins / total * 100) if total > 0 else 0

    # Today
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('SELECT COUNT(*), SUM(CASE WHEN profit>0 THEN 1 ELSE 0 END), SUM(profit) FROM trades WHERE action="sell" AND timestamp LIKE ?', (today + '%',))
    row = c.fetchone()
    today_trades = row[0] or 0
    today_wins = row[1] or 0
    today_pnl = row[2] or 0
    today_wr = (today_wins / today_trades * 100) if today_trades > 0 else 0

    # Daily
    c.execute('SELECT substr(timestamp,1,10) as day, SUM(profit), COUNT(*) FROM trades WHERE action="sell" GROUP BY day ORDER BY day')
    daily = [{'date': r[0], 'pnl': r[1] or 0, 'trades': r[2]} for r in c.fetchall()]

    # Batches
    c.execute('SELECT batch, COUNT(*), SUM(CASE WHEN profit>0 THEN 1 ELSE 0 END), SUM(profit) FROM trades WHERE action="sell" GROUP BY batch')
    batches = []
    for r in c.fetchall():
        wr = (r[2] or 0) / r[1] * 100 if r[1] else 0
        batches.append({'batch': r[0], 'total': r[1], 'win_rate': wr, 'pnl': r[3] or 0})

    # Recent
    c.execute('SELECT timestamp, coin, profit, profit_rate, batch FROM trades WHERE action="sell" ORDER BY id DESC LIMIT 20')
    recent = [{'time': r[0][5:16] if r[0] else '', 'coin': r[1], 'profit': r[2] or 0, 'profit_rate': r[3] or 0, 'batch': r[4]} for r in c.fetchall()]

    # Changelog (AutoTune rules from DB)
    changelog = []
    try:
        c.execute("SELECT created_at, rule_type, reason FROM autotune_rules ORDER BY id DESC LIMIT 10")
        for r in c.fetchall():
            changelog.append({'time': r[0][:16] if r[0] else '', 'type': r[1], 'desc': r[2]})
    except:
        pass

    # Positions
    positions = []
    surge_watchlist = 0
    try:
        if os.path.exists(POS_PATH):
            with open(POS_PATH) as f:
                pd = json.load(f)
            now_ts = datetime.now().timestamp()
            for coin, bs in pd.get('positions', {}).items():
                for bid, p in bs.items():
                    hm = int((now_ts - p.get('timestamp', now_ts)) / 60)
                    positions.append({'coin': coin, 'batch': bid, 'amount': p.get('amount', 0), 'profit_rate': round((p.get('peak_rate', 0) or 0) * 100, 2), 'hold_min': hm})
            for coin, p in pd.get('surge', {}).items():
                hm = int((now_ts - p.get('timestamp', now_ts)) / 60)
                positions.append({'coin': coin, 'batch': 'surge_trade', 'amount': p.get('amount', 0), 'profit_rate': round((p.get('peak_rate', 0) or 0) * 100, 2), 'hold_min': hm})
            surge_watchlist = len(pd.get('surge_watchlist', {}))
    except:
        pass

    # Bot running
    bot_running = False
    try:
        if os.path.exists(PID_PATH):
            with open(PID_PATH) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            bot_running = True
    except:
        pass

    conn.close()

    data = {
        'balance': 10000000 + total_pnl,
        'total_pnl': total_pnl,
        'total_trades': total,
        'wins': wins, 'losses': losses, 'draws': draws,
        'win_rate': round(win_rate, 1),
        'today_pnl': today_pnl,
        'today_trades': today_trades,
        'today_wr': round(today_wr, 1),
        'daily': daily,
        'batches': batches,
        'recent': recent,
        'changelog': changelog,
        'positions': positions,
        'surge_watchlist': surge_watchlist,
        'bot_running': bot_running,
        'updated': datetime.now().strftime('%m/%d %H:%M:%S')
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w') as f:
        json.dump(data, f, ensure_ascii=False)

    return data

def git_push():
    try:
        os.chdir(BASE_DIR)
        subprocess.run(['git', 'add', 'docs/status.json'], capture_output=True, timeout=10)
        result = subprocess.run(
            ['git', 'commit', '-m', f'dashboard: update status {datetime.now().strftime("%m/%d %H:%M")}'],
            capture_output=True, timeout=10
        )
        if result.returncode == 0:
            subprocess.run(['git', 'push'], capture_output=True, timeout=30)
    except:
        pass

if __name__ == '__main__':
    export()
    git_push()
    print(f"✅ status.json 업데이트 완료 ({datetime.now().strftime('%H:%M:%S')})")
