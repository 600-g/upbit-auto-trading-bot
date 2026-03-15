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

    # Positions (DB active_positions 기준 + 현재가 조회)
    positions = []
    surge_watchlist = 0
    try:
        c.execute('SELECT positions_json FROM active_positions LIMIT 1')
        row = c.fetchone()
        if row:
            pd = json.loads(row[0])
            now_ts = datetime.now().timestamp()
            all_coins = []

            # 일반 배치 포지션
            for coin, bs in pd.get('positions', {}).items():
                for bid, p in bs.items():
                    all_coins.append(coin)
                    hm = int((now_ts - p.get('timestamp', now_ts)) / 60)
                    positions.append({'coin': coin, 'batch': bid, 'buy_price': p.get('buy_price', 0), 'amount': p.get('amount', 0), 'profit_rate': 0, 'hold_min': hm})

            # surge 포지션
            for coin, p in pd.get('surge', {}).items():
                all_coins.append(coin)
                hm = int((now_ts - p.get('timestamp', now_ts)) / 60)
                positions.append({'coin': coin, 'batch': 'surge_trade', 'buy_price': p.get('buy_price', 0), 'amount': p.get('amount', 0), 'profit_rate': 0, 'hold_min': hm})

            surge_watchlist = len(pd.get('surge_watchlist', {}))

            # 현재가 조회 (업비트 API)
            if all_coins:
                import requests
                markets = ','.join([f'KRW-{c}' for c in set(all_coins)])
                resp = requests.get(f'https://api.upbit.com/v1/ticker?markets={markets}', timeout=5)
                if resp.status_code == 200:
                    prices = {t['market'].replace('KRW-', ''): t['trade_price'] for t in resp.json()}
                    for pos in positions:
                        cur = prices.get(pos['coin'], 0)
                        bp = pos['buy_price']
                        if bp and cur:
                            pos['cur_price'] = cur
                            pos['profit_rate'] = round((cur - bp) / bp * 100, 2)
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

    # 잔고: DB active_positions 우선, 없으면 누적 계산
    balance = 10000000 + total_pnl
    try:
        c.execute('SELECT balance FROM active_positions LIMIT 1')
        row = c.fetchone()
        if row and row[0]:
            balance = row[0]
    except:
        pass

    # 실전 모드: 업비트 실제 잔고 조회
    CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH) as f:
                cfg = json.load(f)
            if cfg.get('access_key') and cfg.get('secret_key'):
                import pyupbit
                upbit = pyupbit.Upbit(cfg['access_key'], cfg['secret_key'])
                krw = upbit.get_balance('KRW') or 0
                # 보유 코인 평가액 합산
                balances = upbit.get_balances()
                coin_val = 0
                for b in balances:
                    if b['currency'] != 'KRW' and float(b['balance']) > 0:
                        cur_price = pyupbit.get_current_price(f"KRW-{b['currency']}")
                        if cur_price:
                            coin_val += cur_price * float(b['balance'])
                real_balance = krw + coin_val
                if real_balance > 0:
                    balance = real_balance
    except:
        pass

    conn.close()

    data = {
        'balance': balance,
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
