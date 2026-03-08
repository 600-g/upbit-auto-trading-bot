#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 봇 손실 거래 사후 분석 (trade_postmortem.py)

DB에서 손실 거래를 로드하고, 5가지 개선안을 소급 적용해
P&L 영향을 계산합니다.

A. 3분/-0.5% 빠른 컷
B. 1시간 1.5% 상승 필터
C. 고점 근접 차단 (99%)
D. RSI 과매수 차단 (>75)
E. 볼린저밴드 상단 차단 (>80%)

사용법: python trade_postmortem.py
"""

import sqlite3
import pyupbit
import numpy as np
from datetime import datetime, timedelta
import time
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "trading_bot_v3.db")

# ============================================
# 1. DB 로드
# ============================================

def load_loss_trades():
    """DB에서 손실 거래 로드 (buy/sell 페어 매칭)"""
    if not os.path.exists(DB_PATH):
        print(f"❌ DB 파일이 없습니다: {DB_PATH}")
        sys.exit(1)

    db = sqlite3.connect(DB_PATH)
    cursor = db.execute("""
        SELECT coin, batch, action, price, quantity, amount,
               profit, profit_rate, momentum, timestamp
        FROM trades
        ORDER BY timestamp
    """)
    rows = cursor.fetchall()
    db.close()

    # buy/sell 페어 매칭
    buys = {}  # {(coin, batch): row}
    completed = []

    for row in rows:
        coin, batch, action, price, qty, amount, profit, profit_rate, momentum, ts = row
        key = (coin, batch)

        if action == "buy":
            buys[key] = {
                "coin": coin, "batch": batch,
                "buy_price": price, "quantity": qty, "amount": amount,
                "buy_time": ts, "momentum": momentum
            }
        elif action == "sell" and key in buys:
            buy_info = buys[key]
            buy_dt = datetime.fromisoformat(buy_info["buy_time"])
            sell_dt = datetime.fromisoformat(ts)
            hold_min = (sell_dt - buy_dt).total_seconds() / 60
            completed.append({
                **buy_info,
                "sell_price": price,
                "sell_time": ts,
                "profit": profit,
                "profit_rate": profit_rate,
                "hold_minutes": round(hold_min, 1)
            })
            del buys[key]

    losses = [t for t in completed if t["profit_rate"] < 0]
    return losses, completed


# ============================================
# 2. 개선안 시뮬레이션
# ============================================

def _get_ohlcv_from_safe(coin, from_dt, interval, count):
    """pyupbit.get_ohlcv_from 래퍼 (타임아웃 + 재시도)"""
    ticker = f"KRW-{coin}"
    for attempt in range(2):
        try:
            df = pyupbit.get_ohlcv_from(ticker, from_dt, interval=interval, count=count)
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
    return None


def simulate_quick_cut(trade):
    """
    A안: 3분/-0.5% 빠른 컷 소급 적용
    returns: (simulated_sell_price, trigger_minutes) or (sell_price, None)
    """
    coin = trade["coin"]
    buy_time = datetime.fromisoformat(trade["buy_time"])
    buy_price = trade["buy_price"]
    sell_price = trade["sell_price"]

    try:
        # 매수 시점부터 4분치 1분봉
        from_dt = buy_time - timedelta(seconds=30)
        df = _get_ohlcv_from_safe(coin, from_dt, "minute1", 6)
        if df is None:
            return None, None

        # 매수 시점 이후만
        df = df[df.index >= buy_time]
        if len(df) == 0:
            return None, None

        for ts, row in df.iterrows():
            minutes = (ts - buy_time).total_seconds() / 60
            if minutes > 3.5:  # 3분 초과 시 중단
                break
            drop = (row['low'] - buy_price) / buy_price
            if drop <= -0.005:
                return row['low'], round(minutes, 1)

        return sell_price, None  # 빠른 컷 미발동

    except Exception as e:
        return None, None


def simulate_filter_b(trade):
    """
    B안: 1시간 1.5% 상승 필터
    returns: change_1h (float) or None
    """
    coin = trade["coin"]
    buy_time = datetime.fromisoformat(trade["buy_time"])

    try:
        from_dt = buy_time - timedelta(hours=1, minutes=5)
        df = _get_ohlcv_from_safe(coin, from_dt, "minute5", 15)
        if df is None or len(df) < 4:
            return None

        df = df[df.index <= buy_time]
        if len(df) < 2:
            return None

        first_close = float(df['close'].iloc[0])
        last_close = float(df['close'].iloc[-1])
        change_1h = (last_close - first_close) / first_close * 100
        return round(change_1h, 2)

    except Exception:
        return None


def simulate_filter_c(trade):
    """
    C안: 고점 근접 차단 (99%)
    returns: (ratio_pct, high_1h) or (None, None)
    """
    coin = trade["coin"]
    buy_time = datetime.fromisoformat(trade["buy_time"])
    buy_price = trade["buy_price"]

    try:
        from_dt = buy_time - timedelta(hours=1, minutes=5)
        df = _get_ohlcv_from_safe(coin, from_dt, "minute5", 15)
        if df is None or len(df) < 3:
            return None, None

        df = df[df.index <= buy_time]
        high_1h = float(df['high'].max())
        if high_1h <= 0:
            return None, None

        ratio = buy_price / high_1h * 100
        return round(ratio, 1), round(high_1h, 2)

    except Exception:
        return None, None


def simulate_filter_d(trade):
    """
    D안: RSI 과매수 차단 (RSI > 75)
    returns: rsi (float) or None
    """
    coin = trade["coin"]
    buy_time = datetime.fromisoformat(trade["buy_time"])

    try:
        from_dt = buy_time - timedelta(hours=16)
        df = _get_ohlcv_from_safe(coin, from_dt, "minute60", 17)
        if df is None or len(df) < 14:
            return None

        df = df[df.index <= buy_time]
        closes = df['close'].values.astype(float)
        if len(closes) < 14:
            return None

        delta = np.diff(closes[-15:])
        gains = np.where(delta > 0, delta, 0)
        losses = np.where(delta < 0, -delta, 0)

        avg_gain = gains.mean()
        avg_loss = losses.mean()

        if avg_loss == 0:
            rsi = 99.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        return round(rsi, 1)

    except Exception:
        return None


def simulate_filter_e(trade):
    """
    E안: 볼린저밴드 상단 차단 (BB 위치 > 80%)
    returns: bb_position_pct (float) or None
    """
    coin = trade["coin"]
    buy_time = datetime.fromisoformat(trade["buy_time"])
    buy_price = trade["buy_price"]

    try:
        from_dt = buy_time - timedelta(hours=11)
        df = _get_ohlcv_from_safe(coin, from_dt, "minute30", 23)
        if df is None or len(df) < 20:
            return None

        df = df[df.index <= buy_time]
        closes = df['close'].values.astype(float)[-20:]

        ma = np.mean(closes)
        sd = np.std(closes)
        if sd <= 0:
            return None

        lower = ma - 2 * sd
        upper = ma + 2 * sd
        bb_range = upper - lower
        if bb_range <= 0:
            return None

        bb_pos = (buy_price - lower) / bb_range * 100
        return round(bb_pos, 1)

    except Exception:
        return None


# ============================================
# 3. 단일 거래 분석
# ============================================

def analyze_trade(trade):
    """단일 손실 거래에 5가지 개선안 적용"""
    coin = trade["coin"]
    buy_time_str = trade["buy_time"][:16]
    profit_rate = trade["profit_rate"]
    hold_min = trade["hold_minutes"]
    buy_price = trade["buy_price"]
    sell_price = trade["sell_price"]
    amount = trade["amount"]
    actual_pnl = amount * profit_rate / 100

    print(f"\n[ {coin} {trade.get('batch','?')} | {buy_time_str} | {profit_rate:+.2f}% | {hold_min:.0f}분 보유 ]")
    print(f"  매수가 {buy_price:,.2f}원 → 매도가 {sell_price:,.2f}원 | 투자금 {amount:,.0f}원 | 실손익 {actual_pnl:+,.0f}원")
    if trade.get("momentum") == 0.0:
        print(f"  ⚠️  저장된 momentum = 0.0 (진입 근거 추적 불가 - 버그)")

    results = {}

    # A. 빠른컷
    sim_price, cut_min = simulate_quick_cut(trade)
    if sim_price is not None and cut_min is not None:
        sim_rate = (sim_price - buy_price) / buy_price * 100
        sim_pnl = amount * sim_rate / 100
        saved = sim_pnl - actual_pnl
        results['A'] = {
            'triggered': True,
            'sim_rate': sim_rate, 'sim_pnl': sim_pnl, 'saved': saved,
            'desc': f"{cut_min:.0f}분에 -0.5% 도달 → 손실 {sim_rate:.2f}%로 개선 ({abs(saved):,.0f}원 절약)"
        }
        print(f"  A. 빠른컷(-0.5%/3분): {results['A']['desc']}")
    elif sim_price is not None:
        results['A'] = {
            'triggered': False,
            'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
            'desc': "3분 내 -0.5% 미도달 → 빠른 컷 미발동"
        }
        print(f"  A. 빠른컷(-0.5%/3분): {results['A']['desc']}")
    else:
        results['A'] = {'triggered': False, 'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
                        'desc': "데이터 조회 실패"}
        print(f"  A. 빠른컷(-0.5%/3분): 데이터 조회 실패")

    # B. 1시간 상승 필터
    change_1h = simulate_filter_b(trade)
    if change_1h is not None:
        blocked = change_1h < 1.5
        results['B'] = {
            'triggered': blocked,
            'sim_rate': 0.0 if blocked else profit_rate,
            'sim_pnl': 0.0 if blocked else actual_pnl,
            'saved': -actual_pnl if blocked else 0,
            'desc': f"매수 전 1h 상승률 {change_1h:+.1f}% → {'차단 가능 ✅' if blocked else '필터 통과 (차단 불가)'}"
        }
        print(f"  B. 1시간 상승 필터: {results['B']['desc']}")
    else:
        results['B'] = {'triggered': False, 'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
                        'desc': "데이터 조회 실패"}
        print(f"  B. 1시간 상승 필터: 데이터 조회 실패")

    # C. 고점 근접 차단
    ratio, high_1h = simulate_filter_c(trade)
    if ratio is not None:
        blocked = ratio >= 99.0
        results['C'] = {
            'triggered': blocked,
            'sim_rate': 0.0 if blocked else profit_rate,
            'sim_pnl': 0.0 if blocked else actual_pnl,
            'saved': -actual_pnl if blocked else 0,
            'desc': f"매수가 {buy_price:,.2f} / 1h고점 {high_1h:,.2f} = {ratio:.1f}% → {'차단 가능 ✅' if blocked else '통과'}"
        }
        print(f"  C. 고점 근접 차단: {results['C']['desc']}")
    else:
        results['C'] = {'triggered': False, 'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
                        'desc': "데이터 조회 실패"}
        print(f"  C. 고점 근접 차단: 데이터 조회 실패")

    # D. RSI 과매수 차단
    rsi = simulate_filter_d(trade)
    if rsi is not None:
        blocked = rsi > 75
        results['D'] = {
            'triggered': blocked,
            'sim_rate': 0.0 if blocked else profit_rate,
            'sim_pnl': 0.0 if blocked else actual_pnl,
            'saved': -actual_pnl if blocked else 0,
            'desc': f"매수 직전 RSI = {rsi:.0f} → {'차단 가능 ✅' if blocked else '통과'}"
        }
        print(f"  D. RSI 차단: {results['D']['desc']}")
    else:
        results['D'] = {'triggered': False, 'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
                        'desc': "데이터 조회 실패"}
        print(f"  D. RSI 차단: 데이터 조회 실패")

    # E. BB 상단 차단
    bb_pos = simulate_filter_e(trade)
    if bb_pos is not None:
        blocked = bb_pos >= 80
        results['E'] = {
            'triggered': blocked,
            'sim_rate': 0.0 if blocked else profit_rate,
            'sim_pnl': 0.0 if blocked else actual_pnl,
            'saved': -actual_pnl if blocked else 0,
            'desc': f"매수가 BB 상단의 {bb_pos:.0f}% → {'차단 가능 ✅' if blocked else '통과'}"
        }
        print(f"  E. BB 차단: {results['E']['desc']}")
    else:
        results['E'] = {'triggered': False, 'sim_rate': profit_rate, 'sim_pnl': actual_pnl, 'saved': 0,
                        'desc': "데이터 조회 실패"}
        print(f"  E. BB 차단: 데이터 조회 실패")

    return results, actual_pnl


# ============================================
# 4. 메인
# ============================================

def main():
    print("=" * 60)
    print("=== 손실 거래 사후 분석 ===")
    print("=" * 60)

    losses, all_trades = load_loss_trades()
    wins = [t for t in all_trades if t["profit_rate"] > 0]

    if not all_trades:
        print("❌ 완료된 거래가 없습니다.")
        return

    total_trades_pnl = sum(t["amount"] * t["profit_rate"] / 100 for t in all_trades)
    total_loss_pnl = sum(t["amount"] * t["profit_rate"] / 100 for t in losses)

    print(f"\n전체 완료 거래: {len(all_trades)}건 (승: {len(wins)}, 패: {len(losses)})")
    print(f"승률: {len(wins)/len(all_trades)*100:.1f}%" if all_trades else "")
    print(f"총 실현 손익: {total_trades_pnl:+,.0f}원")
    print(f"손실 거래 합계: {total_loss_pnl:,.0f}원")

    if not losses:
        print("\n✅ 손실 거래가 없습니다.")
        return

    print(f"\n분석 대상: {len(losses)}건 손실 거래")

    # 개선안별 집계
    summary = {k: {'triggered': 0, 'total_saved': 0} for k in 'ABCDE'}
    desc_map = {
        'A': '빠른컷(-0.5%/3분)  ',
        'B': '1시간 상승 필터    ',
        'C': '고점 근접 차단     ',
        'D': 'RSI 과매수 차단    ',
        'E': 'BB 상단 차단       '
    }

    for i, trade in enumerate(losses):
        results, actual_pnl = analyze_trade(trade)
        time.sleep(0.5)  # API 레이트 리밋

        for k, r in results.items():
            if r['triggered']:
                summary[k]['triggered'] += 1
                summary[k]['total_saved'] += r['saved']

    print("\n\n" + "=" * 60)
    print("=== 전체 P&L 영향 요약 ===")
    print("=" * 60)
    print(f"  현재 총 손실: {total_loss_pnl:,.0f}원\n")

    ranked = []
    for k in 'ABCDE':
        saved = summary[k]['total_saved']
        triggered = summary[k]['triggered']
        new_loss = total_loss_pnl + saved
        improvement = abs(saved) / abs(total_loss_pnl) * 100 if total_loss_pnl != 0 else 0
        ranked.append((k, desc_map[k], saved, triggered, new_loss, improvement))

    # 개선 효과 큰 순 정렬
    ranked.sort(key=lambda x: x[2], reverse=True)

    for k, desc, saved, triggered, new_loss, improvement in ranked:
        bar = "▓" * int(improvement / 5) + "░" * (20 - int(improvement / 5))
        print(f"  {k}. {desc}: {total_loss_pnl:,.0f}원 → {new_loss:,.0f}원 "
              f"({improvement:.0f}% 개선) [{triggered}건 차단/개선]")
        print(f"     [{bar}] {improvement:.1f}%")

    print("\n")
    best = ranked[0]
    print(f"💡 가장 효과적인 개선안: {best[0]}. {best[1].strip()} ({best[5]:.0f}% 개선)")

    if total_loss_pnl < 0:
        combined_save = sum(r[2] for r in ranked[:2])
        combined_new = total_loss_pnl + combined_save
        combined_pct = abs(combined_save) / abs(total_loss_pnl) * 100
        print(f"💡 상위 2개 조합 시: {total_loss_pnl:,.0f}원 → {combined_new:,.0f}원 ({combined_pct:.0f}% 개선)")


if __name__ == "__main__":
    main()
