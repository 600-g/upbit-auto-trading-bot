#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동거래봇 v3.0 - 완전한 구현
- 모멘텀 감지 (거래량, RSI, 지지선, 가격, 차트패턴)
- SNS + 뉴스 감시 (일론, 트럼프 포함)
- 미국 주식시장 연동
- 자동 매수/매도
- 분할 매수 (25% × 2회)
- 배치별 추적
- 약세장 동적 임계값
- 극단 변동 감지
- 보안 + DB 저장
"""

import pyupbit
import json
import time
import os
import sys
import sqlite3
import re
import socket
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import numpy as np
import feedparser
import requests as req_lib
import yfinance as yf

# 소켓 기본 타임아웃 10초 (모든 HTTP 호출에 적용, 멈춤 방지)
socket.setdefaulttimeout(10)

def _run_with_timeout(fn, timeout=15):
    """함수를 타임아웃 내에 실행, 초과 시 (None, 에러메시지) 반환"""
    result = [None]
    error = [None]
    def wrapper():
        try:
            result[0] = fn()
        except Exception as e:
            error[0] = str(e)
    t = threading.Thread(target=wrapper, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return None, "타임아웃"
    if error[0]:
        return None, error[0]
    return result[0], None

class TradingBotV3:
    def _load_all_krw_coins(self):
        """업비트 KRW 마켓 전체 코인 목록 로드 (투자유의/거래종료 예정 제외)"""
        try:
            # isDetails=true로 투자유의 정보 포함 조회
            url = "https://api.upbit.com/v1/market/all"
            resp = req_lib.get(url, params={"isDetails": "true"}, headers={"Accept": "application/json"}, timeout=10)
            markets = resp.json()

            coins = []
            self._warning_coins = set()  # 투자유의/거래종료 예정 종목

            for m in markets:
                market_code = m.get("market", "")
                if not market_code.startswith("KRW-"):
                    continue

                coin = market_code.replace("KRW-", "")
                event = m.get("market_event", {})
                old_warning = m.get("market_warning", "NONE")

                # 투자유의 종목 필터
                is_warning = event.get("warning", False) is True
                is_old_caution = old_warning == "CAUTION"

                if is_warning or is_old_caution:
                    self._warning_coins.add(coin)
                    continue  # 유의 종목은 아예 목록에서 제외

                # 스테이블코인 제외 (이익 불가, 스캔 낭비)
                _STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "USD1", "FDUSD"}
                if coin in _STABLECOINS:
                    continue

                coins.append(coin)

            if self._warning_coins:
                print(f"⚠️ 투자유의/거래종료 예정 {len(self._warning_coins)}개 종목 제외: {', '.join(sorted(self._warning_coins))}")

            return coins
        except Exception as e:
            print(f"⚠️ 상세 마켓 로드 실패, 기본 방법 사용: {e}")
            try:
                tickers = pyupbit.get_tickers(fiat="KRW")
                self._warning_coins = set()
                return [t.replace("KRW-", "") for t in tickers]
            except:
                self._warning_coins = set()
                return ["BTC", "ETH", "SOL", "ADA", "XRP"]

    def __init__(self, mode="demo"):
        self.mode = mode
        # 업비트 KRW 마켓 전체 코인 로드 (투자유의 제외)
        self.coins = self._load_all_krw_coins()
        print(f"마켓 스캔: KRW 마켓 {len(self.coins)}개 코인 감지 (유의종목 {len(self._warning_coins)}개 제외)")

        self.initial_balance = 10000000
        self.current_balance = self.initial_balance
        self.positions = {}  # {coin: {batch_1: {...}, batch_2: {...}}}
        self.trades = []
        self.running = False

        print("\n" + "="*60)
        print("업비트 자동거래봇 v3.2")
        print("="*60)
        print(f"모드: {mode.upper()}")
        print(f"초기 자금: {self.initial_balance:,}원")
        print("="*60)

        if mode == "real":
            self.upbit = self._connect_upbit()
            if self.upbit:
                # 실제 KRW 잔고로 초기화
                balance = self._get_krw_balance()
                self.initial_balance = balance
                self.current_balance = balance
                print(f"💰 실제 KRW 잔고: {balance:,.0f}원\n")
            else:
                self._notify("[ERROR] API 연결 실패 - 프로그램 종료")
                print("❌ API 연결 실패 - 프로그램 종료")
                raise SystemExit(1)
        else:
            self.upbit = None

        self._pending_2nd_buy = {}  # 2차 분할매수 대기열
        self._sell_cooldown = {}  # 매도 후 재매수 쿨다운 {coin: timestamp}

        # v3.2: 단타 레이더
        self._scalp_positions = {}  # {coin: {buy_price, amount, quantity, timestamp}}
        self._scalp_price_cache = {}  # {coin: (timestamp, price)} 이전 가격 캐시
        self._scalp_max = 1  # 동시 단타 포지션 최대 1개

        # v4.1: 급등 감지 추격 매매 (Surge Trade)
        self._surge_positions = {}     # {coin: {buy_price, amount, quantity, timestamp, peak_rate}}
        self._surge_max = 1            # 동시 서지 포지션 최대 1개
        self._last_surge_entry = 0     # 마지막 서지 진입 시각
        self._surge_coin_count = {}    # v5.1: 코인별 surge 진입 횟수 (같은 코인 2회 제한)
        self._surge_watchlist = {}     # v5.3: 눌림목 대기열 {coin: {detected_price, peak_price, timestamp, surge_type, ...}}

        # v4.3: 과매매 방지
        self._daily_coin_buys = {}     # {coin: count} 코인별 일일 매수 횟수
        self._daily_reset_date = None  # 마지막 리셋 날짜
        self._max_daily_coin_buys = 2  # v5.2: 같은 코인 하루 최대 2회 (재진입 손실 방지)
        self._max_batch = 4            # 추가매수 최대 batch_4까지

        # v4.4: 자동 학습 시스템 (Auto-Tune)
        self._autotune_enabled = True
        self._autotune_interval = 3600   # v5.4: 1시간 간격 분석
        self._last_autotune = 0
        self._autotune_rules = []        # 현재 활성 규칙 캐시
        self._autotune_blacklist = set() # 블랙리스트 캐시 (빠른 조회용)

        # v5.9: DB 전패 코인 영구 블랙리스트 (SHIB 8전패, BEAM 5전패 등 실데이터 기반)
        # v5.14: 영구 블랙리스트 폐지 → AutoTune 동적 블랙리스트(7일 만료)로 통합
        # 코인 자체가 나쁜 게 아니라 진입 타이밍이 나쁜 것
        self._permanent_blacklist = set()  # 비어있음 (동적 관리로 전환)

        # v5.9: surge 연속 손실 보호 (2회 연속 손실 시 30분 중단)
        self._surge_loss_streak = 0
        self._surge_blocked_until = 0

        # v5.9: 손실 비례 쿨다운용 마지막 매도 수익률 캐시
        self._last_sell_pnl = {}  # {coin: profit_rate(%)} 매도 시 기록

        # v4.5: 동적 모멘텀 가중치 (AutoTune이 승패 분석으로 자동 조정)
        self._momentum_weights = {
            'vol': 0.20, 'rsi': 0.20, 'support': 0.13, 'price': 0.13,
            'pattern': 0.15, 'news': 0.08, 'fundamental': 0.03,
            'abnormal': 0.05, 'pa_context': 0.03
        }

        # v4.0: 프라이스 액션 캐시
        self._sr_cache = {}       # {coin: (timestamp, sr_dict)} — 30분 캐시
        self._tl_cache = {}       # {coin: (timestamp, tl_dict)} — 30분 캐시
        self._candle_cache = {}   # {coin: (timestamp, candle_dict)} — 5분 캐시
        self._ohlcv_1h_cache = {} # {key: (timestamp, dataframe)} — 10분 캐시

        # 텔레그램 설정 로드
        self._load_telegram_config()

        # SQLite DB 초기화
        self._init_db()

        # DB에서 이전 포지션 복원
        self._load_positions()

        # 봇 시작 알림
        self._notify(f"[START] 봇 시작 (모드: {mode.upper()}, 잔고: {self.current_balance:,.0f}원, 복원 포지션: {len(self.positions)}개)")

    def _init_db(self):
        """SQLite 데이터베이스 초기화"""
        db_path = os.path.join(os.path.dirname(__file__), "trading_bot_v3.db")
        self._db_lock = threading.Lock()
        self._trade_lock = threading.Lock()  # v4.2: 포지션/잔고 동시 접근 보호 (텔레그램 /sell vs 메인루프)
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin TEXT NOT NULL,
            batch TEXT,
            action TEXT NOT NULL,
            price REAL,
            quantity REAL,
            amount REAL,
            profit REAL,
            profit_rate REAL,
            momentum REAL,
            market_state TEXT,
            timestamp TEXT NOT NULL
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            balance REAL,
            positions TEXT,
            market_state TEXT,
            cycle_num INTEGER,
            timestamp TEXT NOT NULL
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            positions_json TEXT NOT NULL,
            balance REAL NOT NULL,
            updated_at TEXT NOT NULL
        )""")
        # v4.4: 자동 학습 규칙 테이블
        self.db.execute("""CREATE TABLE IF NOT EXISTS auto_tune_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_type TEXT NOT NULL,
            coin TEXT,
            param_key TEXT,
            param_value REAL,
            reason TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )""")
        self.db.commit()
        print("✅ DB 초기화 완료")

    def _db_log_trade(self, coin, action, price, quantity, amount,
                      profit=0, profit_rate=0, momentum=0, market_state="", batch=""):
        """거래 기록을 DB에 저장"""
        try:
            with self._db_lock:
                self.db.execute(
                    "INSERT INTO trades (coin,batch,action,price,quantity,amount,profit,profit_rate,momentum,market_state,timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (coin, batch, action, price, quantity, amount, profit, profit_rate, momentum, market_state, datetime.now().isoformat())
                )
                self.db.commit()
        except Exception as e:
            print(f"⚠️ DB 기록 오류: {e}")

    def _db_snapshot(self, cycle_num=0, market_state=""):
        """현재 상태 스냅샷 DB에 저장 (v4.2: 스캘프/서지 포지션 금액도 반영)"""
        try:
            # 스냅샷 잔고: 현금 + 모든 포지션 투자금
            scalp_val = sum(p['amount'] for p in self._scalp_positions.values())
            surge_val = sum(p['amount'] for p in self._surge_positions.values())
            snap_balance = self.current_balance + scalp_val + surge_val
            with self._db_lock:
                self.db.execute(
                    "INSERT INTO snapshots (balance,positions,market_state,cycle_num,timestamp) VALUES (?,?,?,?,?)",
                    (snap_balance, json.dumps(self.positions, ensure_ascii=False), market_state, cycle_num, datetime.now().isoformat())
                )
                self.db.commit()
        except Exception as e:
            print(f"⚠️ DB 스냅샷 오류: {e}")

    def _save_positions(self):
        """현재 포지션(self.positions + scalp + surge)과 잔고를 DB에 저장"""
        try:
            with self._db_lock:
                # v4.2: 스캘프/서지 포지션도 함께 저장 (재시작 시 소실 방지)
                all_data = {
                    'positions': self.positions,
                    'scalp': self._scalp_positions,
                    'surge': self._surge_positions,
                    'surge_watchlist': self._surge_watchlist,  # v5.3
                }
                positions_json = json.dumps(all_data, ensure_ascii=False)
                now = datetime.now().isoformat()
                self.db.execute(
                    """INSERT INTO active_positions (id, positions_json, balance, updated_at)
                       VALUES (1, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                           positions_json = excluded.positions_json,
                           balance = excluded.balance,
                           updated_at = excluded.updated_at""",
                    (positions_json, self.current_balance, now)
                )
                self.db.commit()
        except Exception as e:
            print(f"⚠️ 포지션 저장 오류: {e}")

    def _load_positions(self):
        """DB에서 이전 포지션 복원 (봇 시작 시 호출)"""
        try:
            with self._db_lock:
                cursor = self.db.execute(
                    "SELECT positions_json, balance, updated_at FROM active_positions WHERE id = 1"
                )
                row = cursor.fetchone()
            if row:
                raw_data = json.loads(row[0])
                saved_balance = row[1]
                saved_time = row[2]
                # 잔고는 포지션 유무와 관계없이 항상 복원
                self.current_balance = saved_balance

                # v4.2: 새 형식(dict with 'positions'/'scalp'/'surge') vs 구형식(positions만) 호환
                if isinstance(raw_data, dict) and 'positions' in raw_data:
                    saved_positions = raw_data['positions']
                    self._scalp_positions = raw_data.get('scalp', {})
                    self._surge_positions = raw_data.get('surge', {})
                    self._surge_watchlist = raw_data.get('surge_watchlist', {})  # v5.3
                else:
                    saved_positions = raw_data  # 구형식 하위호환
                    self._scalp_positions = {}
                    self._surge_positions = {}
                    self._surge_watchlist = {}  # v5.3

                if saved_positions:
                    self.positions = saved_positions
                    print(f"✅ 포지션 복원 완료: {len(self.positions)}개 코인, 잔고 {saved_balance:,.0f}원 (저장 시각: {saved_time})")
                    for coin, batches in self.positions.items():
                        for batch_id, pos in batches.items():
                            print(f"   - {coin} {batch_id}: {pos['amount']:,.0f}원 @ {pos['buy_price']:,.0f}원")
                else:
                    print(f"✅ 저장된 포지션 없음, 잔고 {saved_balance:,.0f}원 복원 (저장 시각: {saved_time})")
                if self._scalp_positions:
                    print(f"   + 스캘프 포지션 복원: {list(self._scalp_positions.keys())}")
                if self._surge_positions:
                    print(f"   + 서지 포지션 복원: {list(self._surge_positions.keys())}")
                if self._surge_watchlist:
                    print(f"   + 서지 워치리스트 복원: {list(self._surge_watchlist.keys())}")
            else:
                print("✅ 이전 포지션 기록 없음 (신규 시작)")
        except Exception as e:
            print(f"⚠️ 포지션 복원 오류: {e} (빈 상태로 시작)")

        # 잔고 교차검증: 거래 내역에서 계산한 잔고와 비교
        try:
            with self._db_lock:
                cursor = self.db.execute(
                    "SELECT COUNT(*) FROM trades WHERE action='sell'"
                )
                sell_count = cursor.fetchone()[0]
            if sell_count > 0:
                with self._db_lock:
                    cursor = self.db.execute(
                        "SELECT COALESCE(SUM(CASE WHEN action='sell' THEN profit ELSE 0 END), 0) FROM trades"
                    )
                    total_pnl = cursor.fetchone()[0]
                calc_balance = self.initial_balance + total_pnl
                if abs(self.current_balance - calc_balance) > 100:  # 100원 이상 차이
                    print(f"⚠️ 잔고 불일치 감지: 저장 {self.current_balance:,.0f}원 vs 거래내역 {calc_balance:,.0f}원 → 거래내역 기준으로 보정")
                    self.current_balance = calc_balance
                    self._save_positions()
        except Exception as e:
            print(f"⚠️ 잔고 교차검증 오류: {e}")

        # real 모드: 실제 업비트 보유량과 DB 포지션 정합성 확인
        if self.mode == "real" and self.upbit and self.positions:
            self._reconcile_positions()

    def _reconcile_positions(self):
        """실제 업비트 보유량과 DB 포지션 동기화 (real 모드 전용)"""
        try:
            print("🔍 포지션 정합성 검증 중...")
            balances = self.upbit.get_balances()
            real_holdings = {}
            for b in balances:
                currency = b.get('currency', '')
                bal = float(b.get('balance', 0))
                if currency != 'KRW' and bal > 0:
                    real_holdings[currency] = bal

            # 1. DB에 있지만 실제 보유 없는 코인 → 포지션 제거 + 알림
            for coin in list(self.positions.keys()):
                if coin not in real_holdings or real_holdings[coin] <= 0:
                    # v4.2: 제거 전 투자금액 기록 및 경고
                    lost_amount = sum(p['amount'] for p in self.positions[coin].values())
                    print(f"  🚨 {coin}: DB에 포지션 있지만 실제 보유량 0 → 포지션 제거 (투자금 {lost_amount:,}원 — 수동 확인 필요!)")
                    self._notify(f"[경고] {coin} 포지션 불일치: DB에 {lost_amount:,}원 있지만 업비트에 0개 → 수동 확인 필요")
                    del self.positions[coin]

            # 2. 실제 보유 있지만 DB에 없는 코인 → 경고
            for coin, qty in real_holdings.items():
                if coin not in self.positions:
                    print(f"  ⚠️ {coin}: 실제 {qty:.8f}개 보유, DB에 미등록 → 수동 확인 필요")

            # 3. 수량 불일치 → 실제 보유량으로 보정
            for coin in list(self.positions.keys()):
                if coin in real_holdings:
                    db_qty = sum(p['quantity'] for p in self.positions[coin].values())
                    real_qty = real_holdings[coin]
                    if abs(db_qty - real_qty) / max(db_qty, 0.0001) > 0.01:  # 1% 이상 차이
                        print(f"  ⚠️ {coin}: DB {db_qty:.8f}개 vs 실제 {real_qty:.8f}개 → 실제 보유량으로 보정")
                        # 배치별 비례 보정
                        ratio = real_qty / db_qty if db_qty > 0 else 1
                        for batch_id in self.positions[coin]:
                            self.positions[coin][batch_id]['quantity'] *= ratio

            self._save_positions()
            print("✅ 포지션 정합성 검증 완료")
        except Exception as e:
            print(f"⚠️ 포지션 정합성 검증 실패: {e}")

    def _load_telegram_config(self):
        """config.json에서 텔레그램 설정 로드 + 자동 chat_id 감지"""
        self._tg_token = None
        self._tg_chat_id = None
        try:
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            with open(config_path) as f:
                cfg = json.load(f)
            self._tg_token = cfg.get("telegram_bot_token", "")
            self._tg_chat_id = cfg.get("telegram_chat_id", "")
            self._anthropic_key = cfg.get("anthropic_api_key", "")

            if self._tg_token and not self._tg_chat_id:
                # chat_id가 비어있으면 getUpdates로 자동 감지 시도
                self._tg_chat_id = self._detect_telegram_chat_id()
                if self._tg_chat_id:
                    # 감지 성공 → config.json에 저장
                    cfg["telegram_chat_id"] = self._tg_chat_id
                    with open(config_path, "w") as f:
                        json.dump(cfg, f, indent=4)
                    print(f"✅ 텔레그램 chat_id 자동 감지 완료: {self._tg_chat_id}")
                else:
                    print("⚠️ 텔레그램 chat_id 미설정 - 봇에 /start 메시지를 보내주세요")
                    print(f"   봇 링크: https://t.me/bot{self._tg_token.split(':')[0]}")

            if self._tg_token and self._tg_chat_id:
                print(f"✅ 텔레그램 알림 활성화 (chat_id: {self._tg_chat_id})")
            elif self._tg_token:
                print("⚠️ 텔레그램 토큰은 있지만 chat_id가 없음 - 콘솔 알림만 사용")
        except Exception as e:
            print(f"⚠️ 텔레그램 설정 로드 실패: {e} (콘솔 알림만 사용)")

    def _detect_telegram_chat_id(self):
        """getUpdates API로 chat_id 자동 감지"""
        try:
            url = f"https://api.telegram.org/bot{self._tg_token}/getUpdates"
            resp = req_lib.get(url, timeout=10)
            data = resp.json()
            if data.get("ok") and data.get("result"):
                for update in data["result"]:
                    msg = update.get("message", {})
                    chat = msg.get("chat", {})
                    if chat.get("id"):
                        return str(chat["id"])
        except Exception as e:
            print(f"⚠️ 텔레그램 chat_id 자동 감지 실패: {e}")
        return None

    def _notify(self, message):
        """알림 전송 (콘솔 + 텔레그램)"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[NOTIFY {timestamp}] {message}")

        # 텔레그램 전송
        if getattr(self, '_tg_token', None) and getattr(self, '_tg_chat_id', None):
            try:
                # chat_id가 없으면 다시 감지 시도 (유저가 나중에 /start 보낼 수 있음)
                if not self._tg_chat_id:
                    self._tg_chat_id = self._detect_telegram_chat_id()
                    if self._tg_chat_id:
                        config_path = os.path.join(os.path.dirname(__file__), "config.json")
                        with open(config_path) as f:
                            cfg = json.load(f)
                        cfg["telegram_chat_id"] = self._tg_chat_id
                        with open(config_path, "w") as f:
                            json.dump(cfg, f, indent=4)
                        print(f"✅ 텔레그램 chat_id 감지 완료: {self._tg_chat_id}")

                if self._tg_chat_id:
                    url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
                    payload = {"chat_id": self._tg_chat_id, "text": message}
                    req_lib.post(url, json=payload, timeout=10)
            except Exception as e:
                print(f"⚠️ 텔레그램 전송 실패: {e}")

    # ============================================
    # 텔레그램 명령어 처리
    # ============================================

    def _start_telegram_listener(self):
        """텔레그램 명령어 수신 스레드 시작"""
        if not self._tg_token or not self._tg_chat_id:
            return
        self._tg_paused = False
        self._tg_last_update_id = 0
        t = threading.Thread(target=self._telegram_poll_loop, daemon=True)
        t.start()
        print("✅ 텔레그램 명령어 수신 대기 중...")

    def _telegram_poll_loop(self):
        """텔레그램 메시지 폴링 (5초 간격)"""
        while True:
            try:
                url = f"https://api.telegram.org/bot{self._tg_token}/getUpdates"
                params = {"offset": self._tg_last_update_id + 1, "timeout": 5}
                resp = req_lib.get(url, params=params, timeout=15)
                data = resp.json()
                if data.get("ok"):
                    for update in data.get("result", []):
                        self._tg_last_update_id = update["update_id"]
                        msg = update.get("message", {})
                        text = msg.get("text", "").strip()
                        chat_id = str(msg.get("chat", {}).get("id", ""))
                        if chat_id == str(self._tg_chat_id) and text:
                            if text.startswith("/"):
                                self._handle_telegram_command(text)
                            else:
                                self._handle_chat_message(text)
            except Exception as e:
                print(f"⚠️ 텔레그램 폴링 오류: {e}")
            time.sleep(5)

    def _handle_chat_message(self, text):
        """자연어 메시지를 Claude CLI(Max 요금제)로 처리"""
        try:
            bot_state = self._get_bot_state_summary()

            system_prompt = f"""너는 업비트 자동거래봇의 AI 어시스턴트야. 사용자가 텔레그램으로 질문하면 현재 봇 상태를 기반으로 간결하게 답해.

현재 봇 상태:
{bot_state}

규칙:
- 한국어로 답변, 짧고 핵심만
- 설정 변경 요청이면 구체적인 /명령어를 안내해
- 매매 판단의 이유를 설명할 때 모멘텀, 시장 상태 등을 근거로 답해
- 절대 거짓 정보를 만들어내지 마"""

            prompt = f"{system_prompt}\n\n사용자 질문: {text}"

            env = os.environ.copy()
            env.pop("CLAUDECODE", None)
            env.pop("CLAUDE_CODE_ENTRYPOINT", None)
            env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:" + env.get("PATH", "")

            result = subprocess.run(
                ["/opt/homebrew/bin/claude", "-p", "--model", "haiku", prompt],
                capture_output=True, text=True, timeout=60, env=env
            )

            if result.returncode == 0 and result.stdout.strip():
                reply_text = result.stdout.strip()
                self._tg_send(reply_text)
                # 대시보드 채팅에도 응답
                try:
                    subprocess.run(
                        [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chat_reply.py'), reply_text],
                        capture_output=True, timeout=30
                    )
                except:
                    pass
            else:
                self._tg_send("⚠️ AI 응답을 가져올 수 없습니다.")
                print(f"⚠️ Claude CLI 오류: rc={result.returncode} stderr={result.stderr[:200]}")

        except subprocess.TimeoutExpired:
            self._tg_send("⚠️ AI 응답 시간 초과 (60초)")
        except Exception as e:
            print(f"⚠️ 채팅 처리 오류: {e}")
            self._tg_send(f"⚠️ 처리 중 오류 발생: {e}")

    def _get_bot_state_summary(self):
        """현재 봇 상태를 텍스트로 요약"""
        lines = []
        lines.append(f"모드: {self.mode.upper()}")
        lines.append(f"잔고: {self.current_balance:,.0f}원")
        lines.append(f"매매 상태: {'일시중지' if getattr(self, '_tg_paused', False) else '가동 중'}")
        lines.append(f"최대 포지션: {self.MAX_POSITIONS}개")
        lines.append(f"전체 스캔 대상: {len(self.coins)}개 코인 (KRW 마켓, 투자유의 제외)")
        lines.append(f"거래 사이클 빠른 체크: 상위 30개 코인")
        lines.append(f"전체 스캔: 모든 {len(self.coins)}개 코인 → 모멘텀 상위 10개 선별")
        lines.append(f"매매 한도: 50만원 고정 / 분할매수 2회")

        # 포지션 현황
        if self.positions:
            lines.append(f"\n보유 포지션 ({len(self.positions)}종목):")
            for coin, batches in self.positions.items():
                price = self.get_price(coin)
                if price:
                    coin_cost = sum(p['amount'] for p in batches.values())
                    coin_qty = sum(p['quantity'] for p in batches.values())
                    coin_pnl = coin_qty * price - coin_cost
                    coin_rate = (coin_pnl / coin_cost * 100) if coin_cost > 0 else 0
                    avg_buy = coin_cost / coin_qty if coin_qty > 0 else 0
                    hours = max(self._hours_held(p) for p in batches.values())
                    momentum = self.calculate_momentum(coin)
                    lines.append(f"  {coin}: 수익률 {coin_rate:+.2f}% | 매수가 {avg_buy:,.0f}원 → 현재가 {price:,.0f}원 | 모멘텀 {momentum}점 | {hours:.1f}시간 보유 | {len(batches)}배치")
        else:
            lines.append("\n보유 포지션: 없음")

        # 최근 거래
        if self.trades:
            recent = self.trades[-5:]
            lines.append(f"\n최근 거래 ({len(self.trades)}건 중 최근 {len(recent)}건):")
            for t in recent:
                lines.append(f"  {t['coin']}: {t.get('profit_rate', 0):+.2f}% ({t.get('timestamp', '')[:16]})")

        # 쿨다운 상태
        if self._sell_cooldown:
            lines.append(f"\n쿨다운 중: {', '.join(self._sell_cooldown.keys())}")

        # 시장 상태
        if hasattr(self, 'last_market_state'):
            lines.append(f"\n시장 상태: {self.last_market_state}")

        return "\n".join(lines)

    def _handle_telegram_command(self, text):
        """텔레그램 명령어 처리"""
        parts = text.split()
        cmd = parts[0].lower().split("@")[0]  # /sell@botname → /sell

        if cmd == "/start":
            self._tg_send("🤖 업비트 자동거래봇 v3.0\n\n"
                          "명령어:\n"
                          "/status - 현재 포지션\n"
                          "/report - 종합 리포트\n"
                          "/sell 코인명 - 강제 매도\n"
                          "/sellall - 전체 매도\n"
                          "/pause - 매매 일시중지\n"
                          "/resume - 매매 재개\n"
                          "/autotune - 자동학습 제어")
        elif cmd == "/status":
            self._cmd_status()
        elif cmd == "/report":
            self._cmd_report()
        elif cmd == "/sell":
            coin = parts[1].upper() if len(parts) > 1 else None
            self._cmd_sell(coin)
        elif cmd == "/sellall":
            self._cmd_sellall()
        elif cmd == "/pause":
            self._tg_paused = True
            self._tg_send("⏸ 매매 일시중지됨\n모니터링은 계속됩니다.\n재개: /resume")
        elif cmd == "/resume":
            self._tg_paused = False
            self._tg_send("▶️ 매매 재개됨")
        elif cmd == "/autotune":
            self._cmd_autotune(parts[1].lower() if len(parts) > 1 else "status")
        else:
            self._tg_send(f"❓ 알 수 없는 명령어: {cmd}\n/start 로 명령어 목록 확인")

    def _cmd_autotune(self, subcmd):
        """텔레그램 /autotune 명령어 처리"""
        try:
            if subcmd == "on":
                self._autotune_enabled = True
                self._tg_send("🤖 AutoTune 활성화됨")
            elif subcmd == "off":
                self._autotune_enabled = False
                self._tg_send("🤖 AutoTune 비활성화됨 (킬스위치)")
            elif subcmd == "clear":
                with self._db_lock:
                    self.db.execute("UPDATE auto_tune_rules SET active = 0 WHERE active = 1")
                    self.db.commit()
                self._autotune_rules = []
                self._autotune_blacklist = set()
                self._tg_send("🤖 AutoTune 규칙 전체 삭제 완료")
            elif subcmd == "run":
                self._tg_send("🤖 AutoTune 즉시 분석 시작...")
                self._autotune_run()
                self._last_autotune = time.time()
            else:  # status
                state = "ON" if self._autotune_enabled else "OFF"
                lines = [f"🤖 AutoTune ({state})"]
                if self._autotune_rules:
                    lines.append(f"활성 규칙 {len(self._autotune_rules)}개:")
                    for r in self._autotune_rules:
                        coin_str = r['coin'] if r['coin'] else '전체'
                        lines.append(f"  - {r['rule_type']}({coin_str}): {r['reason']}")
                else:
                    lines.append("활성 규칙 없음")
                if self._autotune_blacklist:
                    lines.append(f"블랙리스트: {', '.join(sorted(self._autotune_blacklist))}")
                lines.append(f"\n/autotune on|off|clear|run")
                self._tg_send("\n".join(lines))
        except Exception as e:
            self._tg_send(f"⚠️ AutoTune 명령 오류: {e}")

    def _tg_send(self, text):
        """텔레그램 메시지 전송 (긴 메시지 분할)"""
        if not self._tg_token or not self._tg_chat_id:
            return
        try:
            # 텔레그램 메시지 최대 4096자
            for i in range(0, len(text), 4000):
                chunk = text[i:i+4000]
                url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
                req_lib.post(url, json={"chat_id": self._tg_chat_id, "text": chunk}, timeout=10)
        except Exception as e:
            print(f"⚠️ 텔레그램 전송 실패: {e}")

    def _cmd_status(self):
        """현재 포지션 상태"""
        if not self.positions:
            self._tg_send(f"📊 보유 포지션 없음\n💰 잔고: {self.current_balance:,.0f}원")
            return

        lines = [f"📊 포지션 현황 ({len(self.positions)}종목)"]
        lines.append(f"💰 잔고: {self.current_balance:,.0f}원")
        lines.append("")

        total_invested = 0
        total_pnl = 0
        for coin, batches in self.positions.items():
            price = self.get_price(coin)
            if not price:
                continue
            # 코인별 합산
            coin_cost = sum(p['amount'] for p in batches.values())
            coin_qty = sum(p['quantity'] for p in batches.values())
            coin_pnl = coin_qty * price - coin_cost
            coin_rate = (coin_pnl / coin_cost * 100) if coin_cost > 0 else 0
            avg_buy = coin_cost / coin_qty if coin_qty > 0 else 0
            total_invested += coin_cost
            total_pnl += coin_pnl
            emoji = "🟢" if coin_pnl >= 0 else "🔴"
            lines.append(f"{emoji} {coin} | {coin_rate:+.2f}% ({coin_pnl:+,.0f}원)")
            lines.append(f"   매수 {avg_buy:,.0f}원 → 현재 {price:,.0f}원")
            lines.append(f"   투자 {coin_cost:,.0f}원 | {len(batches)}배치")

        lines.append("")
        total_rate = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        lines.append(f"📈 총 투자: {total_invested:,.0f}원 | 손익: {total_pnl:+,.0f}원 ({total_rate:+.2f}%)")

        if getattr(self, '_tg_paused', False):
            lines.append("\n⏸ 매매 일시중지 상태")

        self._tg_send("\n".join(lines))

    def _cmd_report(self):
        """종합 리포트"""
        now = datetime.now()
        lines = ["📋 종합 리포트"]
        lines.append(f"⏰ {now.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"🤖 모드: {self.mode.upper()}")
        lines.append("")

        # 실현 손익 (매매 완료)
        realized = 0
        if self.trades:
            for t in self.trades:
                buy_p = t.get('buy_price', 0)
                sell_p = t.get('sell_price', 0)
                qty = t.get('quantity', 0) if 'quantity' in t else 0
                if qty > 0:
                    realized += (sell_p - buy_p) * qty
                else:
                    realized += t.get('profit_rate', 0) / 100 * 250000

        # 미실현 손익 (보유중)
        unrealized = 0
        total_invested = 0

        # 포지션 현황
        if self.positions:
            lines.append(f"📊 보유 포지션 ({len(self.positions)}종목)")
            for coin, batches in self.positions.items():
                price = self.get_price(coin)
                if not price:
                    continue
                coin_cost = sum(p['amount'] for p in batches.values())
                coin_qty = sum(p['quantity'] for p in batches.values())
                coin_pnl = coin_qty * price - coin_cost
                coin_rate = (coin_pnl / coin_cost * 100) if coin_cost > 0 else 0
                avg_buy = coin_cost / coin_qty if coin_qty > 0 else 0
                hours = max(self._hours_held(p) for p in batches.values())
                unrealized += coin_pnl
                total_invested += coin_cost
                emoji = "🟢" if coin_pnl >= 0 else "🔴"
                lines.append(f"   {emoji} {coin}: {coin_rate:+.2f}% ({coin_pnl:+,.0f}원)")
                lines.append(f"      매수 {avg_buy:,.0f}원 → 현재 {price:,.0f}원 | {hours:.1f}h")
        else:
            lines.append("📊 보유 포지션 없음")
        lines.append("")

        # 손익 현황
        total_pnl = realized + unrealized
        lines.append("💰 손익 현황")
        if self.trades:
            lines.append(f"   실현 손익: {realized:+,.0f}원 ({len(self.trades)}건 매매)")
        lines.append(f"   미실현 손익: {unrealized:+,.0f}원 (보유중)")
        lines.append(f"   총 손익: {total_pnl:+,.0f}원")
        lines.append("")

        # 거래 이력 요약
        if self.trades:
            wins = [t for t in self.trades if t.get('profit_rate', 0) > 0]
            losses = [t for t in self.trades if t.get('profit_rate', 0) <= 0]
            win_rate = len(wins) / len(self.trades) * 100 if self.trades else 0
            avg_profit = sum(t.get('profit_rate', 0) for t in self.trades) / len(self.trades)

            lines.append(f"📈 거래 이력 ({len(self.trades)}건)")
            lines.append(f"   승률: {win_rate:.0f}% ({len(wins)}승 {len(losses)}패)")
            lines.append(f"   평균 수익률: {avg_profit:+.2f}%")
            if wins:
                best = max(wins, key=lambda t: t.get('profit_rate', 0))
                lines.append(f"   최고: {best['coin']} {best.get('profit_rate', 0):+.2f}%")
            if losses:
                worst = min(losses, key=lambda t: t.get('profit_rate', 0))
                lines.append(f"   최저: {worst['coin']} {worst.get('profit_rate', 0):+.2f}%")
        else:
            lines.append("📈 거래 이력 없음")
        lines.append("")

        # 시스템 상태
        status = "⏸ 일시중지" if getattr(self, '_tg_paused', False) else "▶️ 정상 가동"
        lines.append(f"⚙️ 포지션 {len(self.positions)}/{self.MAX_POSITIONS} | 잔고 {self.current_balance:,.0f}원 | {status}")

        self._tg_send("\n".join(lines))

    def _cmd_sell(self, coin):
        """특정 코인 강제 매도"""
        if not coin:
            self._tg_send("사용법: /sell 코인명\n예: /sell WET")
            return
        # v4.2: 스레드 안전 — 메인 루프와 동시 접근 방지
        with self._trade_lock:
            if coin not in self.positions:
                self._tg_send(f"❌ {coin} 포지션이 없습니다.\n"
                              f"보유: {', '.join(self.positions.keys()) if self.positions else '없음'}")
                return

            results = []
            for batch_id in list(self.positions[coin].keys()):
                pos = self.positions[coin][batch_id]
                price = self.get_price(coin)
                if price:
                    pnl_rate = (price - pos['buy_price']) / pos['buy_price'] * 100
                    success = self.place_sell_order(coin, batch_id)
                    emoji = "✅" if success else "❌"
                    results.append(f"{emoji} {coin} {batch_id}: {pnl_rate:+.2f}% {'매도완료' if success else '매도실패'}")
                else:
                    results.append(f"❌ {coin} {batch_id}: 가격 조회 실패")

            self._tg_send("🔔 강제 매도 결과\n" + "\n".join(results))

    def _cmd_sellall(self):
        """전체 포지션 강제 매도"""
        # v4.2: 스레드 안전
        with self._trade_lock:
            if not self.positions:
                self._tg_send("📊 매도할 포지션이 없습니다.")
                return

            results = []
            for coin in list(self.positions.keys()):
                for batch_id in list(self.positions.get(coin, {}).keys()):
                    pos = self.positions[coin][batch_id]
                    price = self.get_price(coin)
                    if price:
                        pnl_rate = (price - pos['buy_price']) / pos['buy_price'] * 100
                        success = self.place_sell_order(coin, batch_id)
                        emoji = "✅" if success else "❌"
                        results.append(f"{emoji} {coin} {batch_id}: {pnl_rate:+.2f}%")
                    else:
                        results.append(f"❌ {coin} {batch_id}: 가격 조회 실패")

            self._tg_send("🔔 전체 매도 결과\n" + "\n".join(results) +
                          f"\n\n💰 잔고: {self.current_balance:,.0f}원")

    def _connect_upbit(self):
        """config.json에서 API 키를 읽어 업비트 연결"""
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        if not os.path.exists(config_path):
            print(f"❌ config.json 파일이 없습니다: {config_path}")
            print('   {"access_key": "YOUR_KEY", "secret_key": "YOUR_SECRET"}')
            return None
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            access_key = config["access_key"]
            secret_key = config["secret_key"]
            upbit = pyupbit.Upbit(access_key, secret_key)
            upbit.get_balance("KRW")
            print("✅ 업비트 연결 성공")
            return upbit
        except KeyError:
            print("❌ config.json에 access_key, secret_key가 필요합니다")
            return None
        except Exception as e:
            print(f"❌ 업비트 연결 실패: {e}")
            return None

    def _get_krw_balance(self):
        """실제 KRW 잔고 조회 (v4.2: API 실패 시 기존 잔고 유지)"""
        try:
            balance = self.upbit.get_balance("KRW")
            if balance is not None and float(balance) > 0:
                return float(balance)
            # 잔고 0이면 API 오류 가능성 → 기존 잔고 유지
            print(f"⚠️ 잔고 조회 결과 0원 → 기존 잔고 {self.current_balance:,.0f}원 유지")
            return self.current_balance
        except Exception as e:
            print(f"⚠️ 잔고 조회 실패: {e} → 기존 잔고 {self.current_balance:,.0f}원 유지")
            return self.current_balance

    # ============================================
    # 1. 모멘텀 분석
    # ============================================
    
    def get_price(self, coin):
        """현재가 조회"""
        try:
            ticker = f"KRW-{coin}"
            orderbook = pyupbit.get_orderbook(ticker)
            return orderbook['orderbook_units'][0]['ask_price']
        except:
            return None

    def _get_hourly_ohlcv(self, coin, count=72):
        """1시간봉 OHLCV 공유 캐시 (10분)"""
        now = time.time()
        key = f"{coin}_{count}"
        cached = self._ohlcv_1h_cache.get(key)
        if cached and now - cached[0] < 600:
            return cached[1]
        try:
            data = pyupbit.get_ohlcv(f"KRW-{coin}", interval="minute60", count=count)
            self._ohlcv_1h_cache[key] = (now, data)
            return data
        except:
            return None

    def analyze_volume(self, coin):
        """거래량 분석 (0~100점)"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)

            volumes = [x for x in ohlcv['volume']]
            current = volumes[-1]
            avg = np.mean(volumes[:-1])

            ratio = current / avg if avg > 0 else 1

            if ratio >= 3.0:
                return 100
            elif ratio >= 2.0:
                return 80
            elif ratio >= 1.5:
                return 60
            elif ratio >= 1.2:
                return 40
            elif ratio >= 1.0:
                return 20
            return 10
        except:
            return 0
    
    def analyze_rsi(self, coin):
        """RSI 분석 (0~100점) - v4.3: 추세 방향에 따라 해석 변경"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=14)

            closes = [x for x in ohlcv['close']]
            delta = np.diff(closes)

            gain = np.where(delta > 0, delta, 0).mean()
            loss = np.where(delta < 0, -delta, 0).mean()

            if loss == 0:
                rsi = 100 if gain == 0 else 99
            else:
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))

            # v4.3: 추세 확인 — 최근 3봉 방향
            recent_trend_up = len(closes) >= 4 and closes[-1] > closes[-3]

            if rsi < 25:
                # 극과매도: 상승 추세면 눌림목(100), 하락 추세면 떨어지는 칼날(50)
                return 100 if recent_trend_up else 50
            elif rsi < 35:
                return 80 if recent_trend_up else 45
            elif rsi < 45:
                return 60
            elif rsi < 55:
                return 50
            elif rsi < 70:
                return 30
            else:
                return 0     # 과매수 → 매수 금지
        except:
            return 0
    
    # ============================================
    # v4.0: 프라이스 액션 메서드
    # ============================================

    def _calc_sr_levels(self, coin):
        """S/R 매물대 자동 산출 — 1시간봉 피봇 클러스터링 (30분 캐시)"""
        now = time.time()
        cached = self._sr_cache.get(coin)
        if cached and now - cached[0] < 1800:
            return cached[1]

        result = self._calc_sr_levels_inner(coin)
        self._sr_cache[coin] = (now, result)
        return result

    def _calc_sr_levels_inner(self, coin):
        """S/R 매물대 내부 계산"""
        try:
            ohlcv = self._get_hourly_ohlcv(coin, 72)
            if ohlcv is None or len(ohlcv) < 30:
                return None

            highs = np.array(ohlcv['high'], dtype=float)
            lows = np.array(ohlcv['low'], dtype=float)
            closes = np.array(ohlcv['close'], dtype=float)
            current = closes[-1]
            n = len(closes)
            ORDER = 3

            # 피봇 고점/저점 검출 (좌우 3봉 비교)
            pivot_highs = []
            pivot_lows = []
            for i in range(ORDER, n - ORDER):
                if all(highs[i] >= highs[i - j] for j in range(1, ORDER + 1)) and \
                   all(highs[i] >= highs[i + j] for j in range(1, ORDER + 1)):
                    pivot_highs.append(highs[i])
                if all(lows[i] <= lows[i - j] for j in range(1, ORDER + 1)) and \
                   all(lows[i] <= lows[i + j] for j in range(1, ORDER + 1)):
                    pivot_lows.append(lows[i])

            # 클러스터링 (가격 1.5% 이내 = 같은 존)
            all_pivots = sorted(pivot_highs + pivot_lows)
            if not all_pivots:
                return None
            CLUSTER_PCT = 0.015

            clusters = []
            cluster = [all_pivots[0]]
            for p in all_pivots[1:]:
                if (p - cluster[0]) / cluster[0] <= CLUSTER_PCT:
                    cluster.append(p)
                else:
                    clusters.append(cluster)
                    cluster = [p]
            clusters.append(cluster)

            # 2회+ 터치된 존만 유효
            zones = []
            for cl in clusters:
                if len(cl) >= 2:
                    zones.append({
                        'low': min(cl), 'high': max(cl),
                        'mid': np.mean(cl), 'touches': len(cl)
                    })

            support_zones = [(z['low'], z['high']) for z in zones if z['mid'] < current]
            resistance_zones = [(z['low'], z['high']) for z in zones if z['mid'] > current]

            nearest_support = max([z['high'] for z in zones if z['mid'] < current], default=None)
            nearest_resistance = min([z['low'] for z in zones if z['mid'] > current], default=None)

            # 현재가 위치 판단
            if nearest_support and (current - nearest_support) / current < 0.015:
                current_zone = 'near_support'
            elif nearest_resistance and (nearest_resistance - current) / current < 0.015:
                current_zone = 'near_resistance'
            elif nearest_support and nearest_resistance:
                current_zone = 'mid_range'
            else:
                current_zone = 'breakout'

            return {
                'support_zones': sorted(support_zones, reverse=True),
                'resistance_zones': sorted(resistance_zones),
                'nearest_support': nearest_support,
                'nearest_resistance': nearest_resistance,
                'current_zone': current_zone
            }
        except:
            return None

    def _calc_trendlines(self, coin):
        """추세선/채널 분석 — 피봇 선형회귀 (30분 캐시)"""
        now = time.time()
        cached = self._tl_cache.get(coin)
        if cached and now - cached[0] < 1800:
            return cached[1]

        try:
            ohlcv = self._get_hourly_ohlcv(coin, 48)
            if ohlcv is None or len(ohlcv) < 20:
                return None

            highs = np.array(ohlcv['high'], dtype=float)
            lows = np.array(ohlcv['low'], dtype=float)
            n = len(highs)
            ORDER = 3

            pivot_high_idx = []
            pivot_low_idx = []
            for i in range(ORDER, n - ORDER):
                if all(highs[i] >= highs[i - j] for j in range(1, ORDER + 1)) and \
                   all(highs[i] >= highs[i + j] for j in range(1, ORDER + 1)):
                    pivot_high_idx.append(i)
                if all(lows[i] <= lows[i - j] for j in range(1, ORDER + 1)) and \
                   all(lows[i] <= lows[i + j] for j in range(1, ORDER + 1)):
                    pivot_low_idx.append(i)

            result = {
                'uptrend_slope': 0, 'downtrend_slope': 0,
                'uptrend_support': None, 'downtrend_resistance': None,
                'converging': False, 'convergence_ratio': 0,
                'trend_direction': 'flat'
            }

            if len(pivot_low_idx) >= 2:
                x = np.array(pivot_low_idx)
                y = lows[pivot_low_idx]
                slope, intercept = np.polyfit(x, y, 1)
                result['uptrend_slope'] = slope
                result['uptrend_support'] = slope * (n - 1) + intercept

            if len(pivot_high_idx) >= 2:
                x = np.array(pivot_high_idx)
                y = highs[pivot_high_idx]
                slope, intercept = np.polyfit(x, y, 1)
                result['downtrend_slope'] = slope
                result['downtrend_resistance'] = slope * (n - 1) + intercept

            # 수렴 감지
            if result['uptrend_slope'] > 0 and result['downtrend_slope'] < 0:
                result['converging'] = True
                if result['uptrend_support'] and result['downtrend_resistance']:
                    current_width = result['downtrend_resistance'] - result['uptrend_support']
                    if len(pivot_high_idx) > 0 and len(pivot_low_idx) > 0:
                        start_width = highs[pivot_high_idx[0]] - lows[pivot_low_idx[0]]
                        if start_width > 0:
                            result['convergence_ratio'] = max(0, 1 - current_width / start_width)

            if result['converging'] and result['convergence_ratio'] > 0.6:
                result['trend_direction'] = 'converging'
            elif result['uptrend_slope'] > 0 and result['downtrend_slope'] >= 0:
                result['trend_direction'] = 'up'
            elif result['downtrend_slope'] < 0 and result['uptrend_slope'] <= 0:
                result['trend_direction'] = 'down'

            self._tl_cache[coin] = (now, result)
            return result
        except:
            return None

    def _detect_candle_patterns(self, coin, sr_levels=None):
        """캔들스틱 프라이스 액션 패턴 인식 (5분 캐시)
        S/R 존 근처에서만 풀 점수, 아니면 70% 감점"""
        now = time.time()
        cache_key = coin
        cached = self._candle_cache.get(cache_key)
        if cached and now - cached[0] < 300:
            return cached[1]

        try:
            ohlcv = pyupbit.get_ohlcv(f"KRW-{coin}", interval="minute15", count=20)
            if ohlcv is None or len(ohlcv) < 5:
                return {'patterns': [], 'score': 0, 'at_sr': False}

            o = np.array(ohlcv['open'], dtype=float)
            h = np.array(ohlcv['high'], dtype=float)
            l = np.array(ohlcv['low'], dtype=float)
            c = np.array(ohlcv['close'], dtype=float)

            def body(i): return abs(c[i] - o[i])
            def upper_wick(i): return h[i] - max(o[i], c[i])
            def lower_wick(i): return min(o[i], c[i]) - l[i]
            def total_range(i): return h[i] - l[i] if h[i] != l[i] else 0.0001

            patterns = []
            i = -1  # 최신 봉
            tr = total_range(i)

            # 1) 망치형 (Hammer)
            if body(i) > 0 and lower_wick(i) >= body(i) * 2 and upper_wick(i) < body(i) * 0.5:
                patterns.append({'name': 'hammer', 'direction': 'bullish', 'strength': 75})

            # 2) 슈팅스타 (Shooting Star) — 음봉 + 윗꼬리 긴 것
            if body(i) > 0 and upper_wick(i) >= body(i) * 2 and lower_wick(i) < body(i) * 0.5 and c[i] < o[i]:
                patterns.append({'name': 'shooting_star', 'direction': 'bearish', 'strength': 70})

            # 3) 상승 장악형 (Bullish Engulfing)
            if len(c) >= 2:
                j = -2
                if c[j] < o[j] and c[i] > o[i] and o[i] <= c[j] and c[i] >= o[j]:
                    patterns.append({'name': 'bullish_engulfing', 'direction': 'bullish', 'strength': 80})

            # 4) 하락 장악형 (Bearish Engulfing)
            if len(c) >= 2:
                j = -2
                if c[j] > o[j] and c[i] < o[i] and o[i] >= c[j] and c[i] <= o[j]:
                    patterns.append({'name': 'bearish_engulfing', 'direction': 'bearish', 'strength': 80})

            # 5) 핀바 (Pin Bar)
            if body(i) < tr * 0.25:
                if lower_wick(i) > tr * 0.66:
                    patterns.append({'name': 'bullish_pin_bar', 'direction': 'bullish', 'strength': 85})
                elif upper_wick(i) > tr * 0.66:
                    patterns.append({'name': 'bearish_pin_bar', 'direction': 'bearish', 'strength': 85})

            # 6) 도지 (Doji)
            if tr > 0 and body(i) < tr * 0.05:
                patterns.append({'name': 'doji', 'direction': 'neutral', 'strength': 40})

            # S/R 근처 유효성 판정
            at_sr = False
            if sr_levels and patterns:
                current = c[-1]
                ns = sr_levels.get('nearest_support')
                nr = sr_levels.get('nearest_resistance')
                if ns and abs(current - ns) / current < 0.03:
                    at_sr = True
                if nr and abs(current - nr) / current < 0.03:
                    at_sr = True

            # 점수: 강세 패턴만 합산, S/R 아니면 30%만
            bullish_score = sum(p['strength'] for p in patterns if p['direction'] == 'bullish')
            raw_score = min(100, bullish_score) if patterns else 0
            score = raw_score if at_sr else int(raw_score * 0.3)

            result = {'patterns': patterns, 'score': score, 'at_sr': at_sr}
            self._candle_cache[cache_key] = (now, result)
            return result
        except:
            return {'patterns': [], 'score': 0, 'at_sr': False}

    # ============================================
    # v4.0 끝 — 기존 분석 메서드 (S/R 기반으로 교체)
    # ============================================

    def analyze_support_resistance(self, coin):
        """지지선/저항선 분석 (0~100점) - v4.0: S/R 매물대 기반"""
        try:
            sr = self._calc_sr_levels(coin)
            if sr is None:
                # 폴백: 기존 24h 저점 방식
                ohlcv = self._get_hourly_ohlcv(coin, 24)
                if ohlcv is None:
                    return 0
                current = list(ohlcv['close'])[-1]
                low = min(list(ohlcv['low']))
                distance = (current - low) / low
                if distance < 0.02: return 80
                elif distance < 0.05: return 40
                return 10

            zone = sr['current_zone']
            ns = sr['nearest_support']
            nr = sr['nearest_resistance']
            current = self.get_price(coin)
            if not current:
                return 0

            if zone == 'near_support':
                return 90
            elif zone == 'mid_range' and ns and nr:
                pos = (current - ns) / (nr - ns) if nr != ns else 0.5
                if pos < 0.33:
                    return 75
                elif pos < 0.66:
                    return 50
                else:
                    return 25
            elif zone == 'near_resistance':
                return 15
            elif zone == 'breakout':
                return 40
            return 30
        except:
            return 0
    
    def analyze_price_change(self, coin):
        """가격 상승률 (0~100점)"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute5", count=3)

            closes = [x for x in ohlcv['close']]
            change = (closes[-1] - closes[0]) / closes[0]

            if change >= 0.05:
                return 100   # 5%+ 급등
            elif change >= 0.03:
                return 85
            elif change >= 0.02:
                return 70
            elif change >= 0.01:
                return 55
            elif change >= 0:
                return 30
            elif change >= -0.02:
                return 10
            else:
                return 0     # 급락 중
        except:
            return 0
    
    def analyze_news_sentiment(self, coin):
        """뉴스/SNS 센티먼트 분석 (0~100점)
        - RSS 글로벌 피드 (CoinTelegraph, Decrypt, Reddit/CryptoCurrency)
        - 코인별 Reddit 서브레딧
        - Fear & Greed Index (alternative.me, 1시간 캐시)
        - CoinGecko 코인 뉴스 (무료 엔드포인트, 30분 캐시)
        - 인플루언서(일론/트럼프 등) SNS 언급 보너스
        """
        try:
            now = time.time()

            # 코인별 캐시 확인 (30분)
            if not hasattr(self, '_news_cache'):
                self._news_cache = {}
            if coin in self._news_cache:
                cached_time, cached_score = self._news_cache[coin]
                if now - cached_time < 1800:
                    return cached_score

            # ── Fear & Greed Index (1시간 글로벌 캐시) ──────────────────
            fear_greed = 50  # 기본 중립
            try:
                if not hasattr(self, '_fg_cache_time') or now - self._fg_cache_time >= 3600:
                    fg_resp = req_lib.get("https://api.alternative.me/fng/",
                                          timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                    fg_data = fg_resp.json()
                    fg_val = int(fg_data['data'][0]['value'])
                    fg_cls = fg_data['data'][0]['value_classification']
                    self._fg_cache = fg_val
                    self._fg_cache_time = now
                    print(f"  😱 Fear&Greed: {fg_val} ({fg_cls})")
                fear_greed = getattr(self, '_fg_cache', 50)
            except:
                pass

            # Fear&Greed → 시장 심리 조정값 (-15 ~ +5) v3.2: 거품 축소
            # 극단 공포(<20): 반등 가능 → 소폭 가산 (10→5)
            # 탐욕(>70): 고점 위험 → -10 패널티
            if fear_greed < 20:
                fg_adjust = 5   # v3.2: 10→5 (극공포에서 전 코인 일괄 부풀리기 방지)
            elif fear_greed < 40:
                fg_adjust = 3   # v3.2: 5→3
            elif fear_greed <= 70:
                fg_adjust = 0
            elif fear_greed <= 85:
                fg_adjust = -10
            else:
                fg_adjust = -15

            # ── 글로벌 RSS 피드 (30분 공유 캐시) ────────────────────────
            if not hasattr(self, '_rss_cache_time') or now - self._rss_cache_time >= 1800:
                global_feeds = [
                    "https://cointelegraph.com/rss",
                    "https://decrypt.co/feed",
                    "https://www.reddit.com/r/CryptoCurrency/.rss",
                    "https://www.reddit.com/r/Bitcoin/.rss",
                    "https://www.reddit.com/r/altcoin/.rss",
                ]
                self._rss_entries = []
                def _fetch_rss(url):
                    try:
                        resp = req_lib.get(url, timeout=10,
                                           headers={"User-Agent": "Mozilla/5.0"})
                        return feedparser.parse(resp.text).entries[:20]
                    except:
                        return []
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(_fetch_rss, u) for u in global_feeds]
                    for f in as_completed(futures):
                        self._rss_entries.extend(f.result())
                self._rss_cache_time = now
            all_entries = list(getattr(self, '_rss_entries', []))

            # ── 코인별 Reddit 서브레딧 추가 ──────────────────────────────
            coin_subreddits = {
                "ETH": "ethereum", "SOL": "solana", "ADA": "cardano",
                "AVAX": "Avax", "LINK": "Chainlink", "DOT": "dot",
                "DOGE": "dogecoin", "XRP": "Ripple", "ATOM": "cosmosnetwork",
                "NEAR": "nearprotocol", "UNI": "UniSwap", "FIL": "filecoin",
                "ALGO": "algorand", "GRT": "thegraph", "SAND": "TheSandboxGaming",
                "MANA": "decentraland", "AXS": "AxieInfinity", "YGG": "YieldGuildGames",
            }
            sub = coin_subreddits.get(coin.upper())
            if sub:
                try:
                    sub_cache_key = f"_sub_cache_{coin}"
                    sub_time_key  = f"_sub_time_{coin}"
                    if not hasattr(self, sub_cache_key) or now - getattr(self, sub_time_key, 0) >= 1800:
                        sub_url = f"https://www.reddit.com/r/{sub}/.rss"
                        sub_resp = req_lib.get(sub_url, timeout=8,
                                               headers={"User-Agent": "Mozilla/5.0"})
                        sub_feed = feedparser.parse(sub_resp.text)
                        setattr(self, sub_cache_key, sub_feed.entries[:15])
                        setattr(self, sub_time_key, now)
                    all_entries.extend(getattr(self, sub_cache_key, []))
                except:
                    pass

            # ── CoinGecko 코인 뉴스 (무료, 코인별 30분 캐시) ─────────────
            cg_cache_key = f"_cg_news_{coin}"
            cg_time_key  = f"_cg_news_time_{coin}"
            if not hasattr(self, cg_cache_key) or now - getattr(self, cg_time_key, 0) >= 1800:
                try:
                    coin_id = self._get_coingecko_id(coin)
                    cg_url = f"https://api.coingecko.com/api/v3/news"
                    cg_resp = req_lib.get(cg_url, timeout=10,
                                          params={"per_page": 10},
                                          headers={"Accept": "application/json"})
                    if cg_resp.status_code == 200:
                        cg_news = cg_resp.json()
                        # CoinGecko news는 title/description 필드
                        class _E:
                            def __init__(self, d): self._d = d
                            def get(self, k, default=""): return self._d.get(k, default)
                        cg_entries = [_E({"title": n.get("title",""), "summary": n.get("description","")})
                                      for n in cg_news if isinstance(n, dict)]
                        setattr(self, cg_cache_key, cg_entries)
                        setattr(self, cg_time_key, now)
                except:
                    pass
            all_entries.extend(getattr(self, cg_cache_key, []))

            # ── 코인 별칭 정의 ────────────────────────────────────────────
            coin_aliases = {
                "BTC": ["bitcoin", "btc", "비트코인"],
                "ETH": ["ethereum", "eth", "이더리움", "ether"],
                "SOL": ["solana", "sol", "솔라나"],
                "XRP": ["ripple", "xrp", "리플"],
                "ADA": ["cardano", "ada", "카르다노"],
                "DOGE": ["dogecoin", "doge", "도지", "shiba"],
                "AVAX": ["avalanche", "avax"],
                "MATIC": ["polygon", "matic"],
                "DOT": ["polkadot", "dot"],
                "LINK": ["chainlink", "link"],
                "ATOM": ["cosmos", "atom"],
                "NEAR": ["near", "nearprotocol"],
                "UNI":  ["uniswap", "uni"],
                "FIL":  ["filecoin", "fil"],
                "YGG":  ["yield guild", "ygg"],
                "BIGTIME": ["bigtime", "big time"],
                "AGLD": ["adventure gold", "agld"],
                "ID":   ["space id", " id "],
            }
            aliases = coin_aliases.get(coin.upper(), [coin.lower()])

            # ── 긍/부정 키워드 + 인플루언서 ──────────────────────────────
            positive = [
                "surge", "rally", "bullish", "soar", "jump", "gain", "high",
                "breakout", "pump", "moon", "buy", "adopt", "approve", "launch",
                "partnership", "upgrade", "etf", "institutional", "all-time high", "ath",
                "listing", "mainnet", "airdrop", "staking", "yield",
                "상승", "급등", "돌파", "호재", "매수", "승인", "상장",
            ]
            negative = [
                "crash", "dump", "bearish", "plunge", "drop", "fall", "low",
                "ban", "hack", "fraud", "sell", "fear", "risk", "scam",
                "lawsuit", "sec", "regulation", "delay", "exploit", "rug",
                "delist", "delisting", "warning", "caution", "vulnerable",
                "하락", "급락", "폭락", "악재", "매도", "규제", "소송", "상장폐지",
            ]
            # SNS 인플루언서 (일론/트럼프/빌게이츠/캐시우드 등)
            influencer_kw = [
                "elon", "musk", "tesla", "spacex",
                "trump", "strategic reserve", "whitehouse",
                "cathie wood", "ark invest",
                "michael saylor", "microstrategy",
                "일론", "머스크", "트럼프", "테슬라",
            ]
            influencer_boost = 0

            pos_count = 0
            neg_count = 0
            mention_count = 0

            for entry in all_entries:
                text = (entry.get("title", "") + " " + entry.get("summary", "")).lower()

                # 인플루언서 × 코인/암호화폐 언급 감지
                has_inf = any(kw in text for kw in influencer_kw)
                has_coin_ref = any(alias in text for alias in aliases)
                has_crypto = any(w in text for w in ["crypto", "bitcoin", "암호화폐", "coin"])

                # v3.2: 인플루언서가 해당 코인을 직접 언급해야만 보너스 (일반 crypto 언급은 제외)
                if has_inf and has_coin_ref:
                    inf_pos = sum(1 for w in positive if w in text)
                    inf_neg = sum(1 for w in negative if w in text)
                    if inf_pos > inf_neg:
                        influencer_boost = min(influencer_boost + 10, 15)  # v3.2: 15→10, 캡 25→15
                    elif inf_neg > inf_pos:
                        influencer_boost = max(influencer_boost - 10, -15)

                if not has_coin_ref:
                    continue
                mention_count += 1
                pos_count += sum(1 for w in positive if w in text)
                neg_count += sum(1 for w in negative if w in text)

            # ── 점수 계산 ─────────────────────────────────────────────────
            if mention_count == 0:
                score = 40  # v3.2: 언급 없음 = 중립 이하 (50→40, 거품 방지)
            else:
                total_signals = pos_count + neg_count
                if total_signals == 0:
                    score = 50
                else:
                    score = int(pos_count / total_signals * 100)
                # 언급량 보너스 (최대 +10)
                score = min(100, score + min(mention_count * 2, 10))

            # 인플루언서 보너스
            score = max(0, min(100, score + influencer_boost))
            if influencer_boost != 0:
                print(f"  📢 {coin} 인플루언서 SNS 감지 (보너스: {influencer_boost:+d}점)")

            # Fear & Greed 조정
            score = max(0, min(100, score + fg_adjust))
            if fg_adjust != 0:
                print(f"  😱 {coin} Fear&Greed({fear_greed}) 조정: {fg_adjust:+d}점")

            # 캐시 저장 (코인별)
            self._news_cache[coin] = (now, score)

            return score
        except:
            return 50

    def detect_abnormal_trading(self, coin):
        """이상 거래 탐지 - 세력 매집/호가 비율/거래량 패턴"""
        try:
            ticker = f"KRW-{coin}"
            result = {"whale": False, "manipulation": False, "score": 0}

            # 1) 호가창 매수/매도 비율 (bid/ask ratio)
            orderbook = pyupbit.get_orderbook(ticker)
            if orderbook:
                units = orderbook['orderbook_units']
                total_bid = sum(u['bid_size'] for u in units[:5])  # 상위 5호가 매수량
                total_ask = sum(u['ask_size'] for u in units[:5])  # 상위 5호가 매도량
                if total_ask > 0:
                    ratio = total_bid / total_ask
                    if ratio >= 3.0:
                        result["score"] += 20  # v3.2: 30→20 (정규화 가중치로 반영)
                        result["whale"] = True
                    elif ratio >= 2.0:
                        result["score"] += 15
                    elif ratio >= 1.2:
                        result["score"] += 8
                    elif ratio <= 0.3:
                        result["score"] -= 20  # 매도 압도 → 투매 가능
                        result["manipulation"] = True

            # 2) 거래량 급증 패턴 (최근 1시간 vs 24시간 평균)
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
            if ohlcv is not None and len(ohlcv) >= 2:
                volumes = list(ohlcv['volume'])
                current_vol = volumes[-1]
                avg_vol = np.mean(volumes[:-1])
                if avg_vol > 0:
                    vol_ratio = current_vol / avg_vol
                    if vol_ratio >= 5.0:
                        result["score"] += 15  # v3.2: 25→15
                        result["whale"] = True
                    elif vol_ratio >= 3.0:
                        result["score"] += 10  # v3.2: 15→10

            # 3) 가격-거래량 괴리 (가격 변화 없이 거래량만 폭증 → 매집)
            if ohlcv is not None and len(ohlcv) >= 2:
                closes = list(ohlcv['close'])
                price_change = abs(closes[-1] - closes[-2]) / closes[-2]
                if price_change < 0.005 and vol_ratio >= 3.0:
                    result["score"] += 10  # v3.2: 20→10
                    result["whale"] = True

            return result
        except:
            return {"whale": False, "manipulation": False, "score": 0}

    def check_extreme_volatility(self, coin):
        """극단 변동 감지 - 급등/급락/거래량 급감 확인"""
        try:
            ticker = f"KRW-{coin}"
            # 1분봉 5개로 1분/3분 변동 체크
            ohlcv_1m = pyupbit.get_ohlcv(ticker, interval="minute1", count=5)
            if ohlcv_1m is None or len(ohlcv_1m) < 4:
                return "normal"

            closes = list(ohlcv_1m['close'])
            volumes = list(ohlcv_1m['volume'])

            # 1분 변동률
            change_1m = abs(closes[-1] - closes[-2]) / closes[-2]
            if change_1m >= 0.05:
                return "halt"  # 1분에 5% → 거래 정지

            # 3분 변동률
            if len(closes) >= 4:
                change_3m = abs(closes[-1] - closes[-4]) / closes[-4]
                if change_3m >= 0.10:
                    return "emergency_sell"  # 3분에 10% → 즉시 손절

            # 거래량 급감 (최근 1분 거래량이 평균의 5% 미만)
            # v3.3: 10%→5% 완화 (새벽 시간대 일시적 거래량 감소로 강세 코인 차단 방지)
            avg_vol = np.mean(volumes[:-1]) if len(volumes) > 1 else 0
            if avg_vol > 0 and volumes[-1] < avg_vol * 0.05:
                return "low_liquidity"  # 유동성 부족 → 거래 금지

            return "normal"
        except:
            return "normal"

    def detect_patterns(self, coin):
        """차트 패턴 감지 (0~100점) - 8개 패턴 종합"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute30", count=50)
            if ohlcv is None or len(ohlcv) < 30:
                return 0
            closes = np.array(ohlcv['close'], dtype=float)
            highs = np.array(ohlcv['high'], dtype=float)
            lows = np.array(ohlcv['low'], dtype=float)
            n = len(closes)
            scores = []

            # 1) 골든크로스 / 데드크로스 (MA5 vs MA20)
            try:
                ma5 = np.convolve(closes, np.ones(5)/5, mode='valid')
                ma20 = np.convolve(closes, np.ones(20)/20, mode='valid')
                ml = min(len(ma5), len(ma20))
                if ml >= 2:
                    if ma5[-ml:][- 1] > ma20[-ml:][-1] and ma5[-ml:][-2] <= ma20[-ml:][-2]:
                        scores.append(100)
                    elif ma5[-ml:][-1] > ma20[-ml:][-1]:
                        scores.append(70)
                    elif ma5[-ml:][-1] < ma20[-ml:][-1] and ma5[-ml:][-2] >= ma20[-ml:][-2]:
                        scores.append(0)
                    else:
                        scores.append(20)
            except:
                pass

            # 2) MACD (12, 26, 9)
            try:
                if n >= 35:
                    ema12 = self._ema(closes, 12)
                    ema26 = self._ema(closes, 26)
                    hist = ema12 - ema26 - self._ema(ema12 - ema26, 9)
                    if hist[-1] > 0 and hist[-2] <= 0:
                        scores.append(100)
                    elif hist[-1] > 0 and hist[-1] > hist[-2]:
                        scores.append(80)
                    elif hist[-1] > 0:
                        scores.append(60)
                    elif hist[-1] < 0 and hist[-1] > hist[-2]:
                        scores.append(40)
                    else:
                        scores.append(10)
            except:
                pass

            # 3) 볼린저밴드 (20봉, 2σ)
            try:
                if n >= 20:
                    ma = np.mean(closes[-20:])
                    sd = np.std(closes[-20:])
                    if sd > 0:
                        upper = ma + 2 * sd
                        lower = ma - 2 * sd
                        c = closes[-1]
                        if c <= lower:
                            scores.append(90)
                        elif c <= lower + 0.3 * sd:
                            scores.append(70)
                        elif c <= ma:
                            scores.append(50)
                        elif c >= upper:
                            scores.append(10)
                        else:
                            scores.append(30)
            except:
                pass

            # 4) 더블바텀 / 더블탑
            try:
                if n >= 20:
                    rc = closes[-20:]
                    rl = lows[-20:]
                    rh = highs[-20:]
                    l1 = rl[:10].min()
                    l2 = rl[10:].min()
                    if l1 > 0 and abs(l1 - l2) / l1 < 0.02:
                        mid = rc[5:15].max()
                        if rc[-1] > mid:
                            scores.append(90)
                        else:
                            scores.append(70)
                    else:
                        h1 = rh[:10].max()
                        h2 = rh[10:].max()
                        if h1 > 0 and abs(h1 - h2) / h1 < 0.02 and rc[-1] < rc[-10:].min():
                            scores.append(10)
                        else:
                            scores.append(40)
            except:
                pass

            # 5) 헤드앤숄더
            try:
                if n >= 30:
                    low_l = lows[-30:-20].min()
                    low_h = lows[-20:-10].min()
                    low_r = lows[-10:].min()
                    if low_h < low_l and low_h < low_r and low_l > 0 and abs(low_l - low_r) / low_l < 0.03:
                        scores.append(85)
                    else:
                        head = highs[-20:-10].max()
                        sh_l = highs[-30:-20].max()
                        sh_r = highs[-10:].max()
                        if head > sh_l and head > sh_r and sh_l > 0 and abs(sh_l - sh_r) / sh_l < 0.03:
                            scores.append(15)
                        else:
                            scores.append(50)
            except:
                pass

            # 6) 플래그 패턴
            try:
                if n >= 20:
                    early = (closes[-11] - closes[-20]) / closes[-20]
                    late = (closes[-10:].max() - closes[-10:].min()) / closes[-10:].min()
                    if early >= 0.03 and late < 0.02:
                        scores.append(85)
                    elif early <= -0.03 and late < 0.02:
                        scores.append(15)
                    else:
                        scores.append(45)
            except:
                pass

            # 7) 웨지 (수렴)
            try:
                if n >= 15:
                    rng_s = highs[-15] - lows[-15]
                    rng_e = highs[-1] - lows[-1]
                    if rng_s > 0 and rng_e < rng_s * 0.5:
                        if closes[-1] > highs[-2]:
                            scores.append(90)
                        elif closes[-1] < lows[-2]:
                            scores.append(10)
                        else:
                            scores.append(60)
                    else:
                        scores.append(45)
            except:
                pass

            # 8) 삼각수렴
            try:
                if n >= 20:
                    hf = highs[-20:-10].max()
                    hl = highs[-10:].max()
                    lf = lows[-20:-10].min()
                    ll = lows[-10:].min()
                    if hl < hf and ll > lf:
                        if closes[-1] > hl:
                            scores.append(90)
                        elif closes[-1] < ll:
                            scores.append(10)
                        else:
                            scores.append(65)
                    else:
                        scores.append(40)
            except:
                pass

            # 9) 장기 볼린저밴드 (일봉 기반, 데이터 길수록 신뢰도 높음)
            try:
                now_t = time.time()
                if not hasattr(self, '_daily_bb_cache'):
                    self._daily_bb_cache = {}

                if coin in self._daily_bb_cache:
                    cached_time, cached_score, cached_dl = self._daily_bb_cache[coin]
                    if now_t - cached_time < 21600:  # 6시간 캐시
                        if cached_dl >= 100:
                            scores.append(cached_score)
                            scores.append(cached_score)  # 장기 코인 2배 가중
                        else:
                            scores.append(cached_score)
                else:
                    ohlcv_d = pyupbit.get_ohlcv(ticker, interval="day", count=200)
                    if ohlcv_d is not None and len(ohlcv_d) >= 30:
                        daily_closes = np.array(ohlcv_d['close'], dtype=float)
                        dl = len(daily_closes)
                        ma_d = np.mean(daily_closes[-20:])
                        sd_d = np.std(daily_closes[-20:])
                        if sd_d > 0:
                            upper_d = ma_d + 2 * sd_d
                            lower_d = ma_d - 2 * sd_d
                            c = daily_closes[-1]
                            if c <= lower_d:
                                bb_d_score = 95
                            elif c <= lower_d + 0.3 * sd_d:
                                bb_d_score = 75
                            elif c <= ma_d:
                                bb_d_score = 55
                            elif c >= upper_d:
                                bb_d_score = 5
                            else:
                                bb_d_score = 30
                            self._daily_bb_cache[coin] = (now_t, bb_d_score, dl)
                            if dl >= 100:
                                scores.append(bb_d_score)
                                scores.append(bb_d_score)  # 장기 코인 2배 가중
                            else:
                                scores.append(bb_d_score)
            except:
                pass

            # 10) 캔들스틱 패턴 (1분봉 최근 5개) - 단기 반전 신호
            try:
                ohlcv_1m = pyupbit.get_ohlcv(ticker, interval="minute1", count=6)
                if ohlcv_1m is not None and len(ohlcv_1m) >= 3:
                    opens_1m  = np.array(ohlcv_1m['open'],  dtype=float)
                    closes_1m = np.array(ohlcv_1m['close'], dtype=float)
                    highs_1m  = np.array(ohlcv_1m['high'],  dtype=float)
                    lows_1m   = np.array(ohlcv_1m['low'],   dtype=float)

                    o, c, h, l = opens_1m[-1], closes_1m[-1], highs_1m[-1], lows_1m[-1]
                    body       = abs(c - o)
                    full_range = h - l
                    lower_wick = min(o, c) - l
                    upper_wick = h - max(o, c)

                    if full_range > 0 and body > 0:
                        # 망치형(Hammer): 아랫꼬리 >= 몸통 2배, 윗꼬리 <= 몸통 50%, 양봉
                        if lower_wick >= body * 2 and upper_wick <= body * 0.5 and c >= o:
                            scores.append(85)
                        # 역망치형(Inverted Hammer): 윗꼬리 >= 몸통 2배, 아랫꼬리 <= 몸통 50%, 양봉
                        elif upper_wick >= body * 2 and lower_wick <= body * 0.5 and c >= o:
                            scores.append(70)
                        # 도지(Doji): 몸통 < 전체 범위의 10%
                        elif full_range > 0 and body / full_range < 0.10:
                            scores.append(55)

                    # 강세 장악패턴(Bullish Engulfing): 이전 음봉을 현재 양봉이 완전 포함
                    if len(opens_1m) >= 2:
                        prev_o, prev_c = opens_1m[-2], closes_1m[-2]
                        curr_o, curr_c = opens_1m[-1], closes_1m[-1]
                        if prev_c < prev_o and curr_c > curr_o:
                            if curr_o <= prev_c and curr_c >= prev_o:
                                scores.append(90)
            except:
                pass

            # 11) 스토캐스틱 RSI (14봉 기반) - 과매도 구간 반전 포착
            try:
                if n >= 28:
                    delta_c  = np.diff(closes)
                    gains_sr  = np.where(delta_c > 0, delta_c, 0)
                    losses_sr = np.where(delta_c < 0, -delta_c, 0)

                    rsi_period = 14
                    rsi_values = []
                    for j in range(rsi_period, len(delta_c) + 1):
                        ag = gains_sr[j - rsi_period:j].mean()
                        al = losses_sr[j - rsi_period:j].mean()
                        if al == 0:
                            rsi_v = 99.0 if ag > 0 else 50.0
                        else:
                            rsi_v = 100 - (100 / (1 + ag / al))
                        rsi_values.append(rsi_v)

                    if len(rsi_values) >= 14:
                        rsi_arr = np.array(rsi_values[-14:])
                        r_min, r_max = rsi_arr.min(), rsi_arr.max()
                        if r_max > r_min:
                            stoch_rsi = (rsi_values[-1] - r_min) / (r_max - r_min) * 100
                            if stoch_rsi < 20:
                                scores.append(90)   # 과매도 → 반등 기대
                            elif stoch_rsi < 40:
                                scores.append(70)
                            elif stoch_rsi < 60:
                                scores.append(50)
                            elif stoch_rsi < 80:
                                scores.append(30)
                            else:
                                scores.append(10)   # 과매수
            except:
                pass

            # 12) v4.0: 프라이스 액션 캔들 패턴 (15분봉, S/R 컨텍스트)
            try:
                sr = self._calc_sr_levels(coin)
                candle = self._detect_candle_patterns(coin, sr)
                if candle['patterns']:
                    if candle['at_sr']:
                        scores.append(candle['score'])
                        scores.append(candle['score'])  # S/R 근처: 2배 가중
                    else:
                        scores.append(candle['score'])
            except:
                pass

            valid = [s for s in scores if s is not None and not np.isnan(s)]
            return round(np.mean(valid), 1) if valid else 0
        except:
            return 0

    def _get_coingecko_id(self, coin):
        """코인 심볼 → CoinGecko ID 변환"""
        mapping = {
            "BTC": "bitcoin", "ETH": "ethereum", "XRP": "ripple",
            "ADA": "cardano", "SOL": "solana", "DOGE": "dogecoin",
            "AVAX": "avalanche-2", "MATIC": "matic-network", "DOT": "polkadot",
            "LINK": "chainlink", "UNI": "uniswap", "ATOM": "cosmos",
            "LTC": "litecoin", "BCH": "bitcoin-cash", "ETC": "ethereum-classic",
            "NEAR": "near", "FTM": "fantom", "ALGO": "algorand",
            "ICP": "internet-computer", "FIL": "filecoin",
            "HBAR": "hedera-hashgraph", "SAND": "the-sandbox",
            "MANA": "decentraland", "AXS": "axie-infinity",
            "GRT": "the-graph", "ENJ": "enjincoin",
            "AGLD": "adventure-gold", "BIGTIME": "big-time",
            "YGG": "yield-guild-games", "ID": "space-id",
            "1INCH": "1inch", "COMP": "compound-governance-token",
            "SNX": "havven", "BAT": "basic-attention-token",
        }
        return mapping.get(coin.upper(), coin.lower())

    def analyze_fundamentals(self, coin):
        """코인 펀더멘털 점수 (CoinGecko, 24시간 캐시)
        - developer_score: GitHub 커밋 활동 (0~100)
        - community_score: Reddit/Twitter 활성도 (0~100)
        - liquidity_score: 거래소 유동성 (0~100)
        가중합: dev 40% + community 30% + liquidity 30%
        """
        try:
            now = time.time()
            if not hasattr(self, '_fundamental_cache'):
                self._fundamental_cache = {}

            if coin in self._fundamental_cache:
                cached_time, cached_score = self._fundamental_cache[coin]
                if now - cached_time < 86400:  # 24시간 캐시
                    return cached_score

            coin_id = self._get_coingecko_id(coin)
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            params = {
                "localization": "false", "tickers": "false",
                "market_data": "false", "community_data": "true",
                "developer_data": "true", "sparkline": "false"
            }
            resp = req_lib.get(url, params=params, timeout=15,
                               headers={"Accept": "application/json"})

            if resp.status_code != 200:
                self._fundamental_cache[coin] = (now, 50)
                return 50

            data = resp.json()
            dev_score  = float(data.get("developer_score",  0) or 0)
            comm_score = float(data.get("community_score",  0) or 0)
            liq_score  = float(data.get("liquidity_score",  0) or 0)

            score = round(dev_score * 0.4 + comm_score * 0.3 + liq_score * 0.3, 1)
            self._fundamental_cache[coin] = (now, score)

            print(f"  📊 {coin} 펀더멘털: {score:.0f}점 "
                  f"(개발:{dev_score:.0f} 커뮤:{comm_score:.0f} 유동:{liq_score:.0f})")
            return score

        except Exception as e:
            print(f"  ⚠️ {coin} 펀더멘털 조회 실패: {e}")
            if not hasattr(self, '_fundamental_cache'):
                self._fundamental_cache = {}
            self._fundamental_cache[coin] = (time.time(), 50)
            return 50

    def _ema(self, data, period):
        """지수이동평균(EMA) 계산"""
        data = np.array(data, dtype=float)
        ema = np.zeros_like(data)
        ema[0] = data[0]
        multiplier = 2 / (period + 1)
        for i in range(1, len(data)):
            ema[i] = data[i] * multiplier + ema[i-1] * (1 - multiplier)
        return ema
    
    def check_us_market(self):
        """미국 주식시장 동향 확인 (S&P500, NASDAQ)"""
        try:
            # 캐시 확인 (10분마다 갱신)
            now = time.time()
            if hasattr(self, '_us_cache') and hasattr(self, '_us_cache_time'):
                if now - self._us_cache_time < 600:
                    return self._us_cache

            signals = {}
            for name, symbol in {"S&P500": "^GSPC", "NASDAQ": "^IXIC"}.items():
                hist = yf.Ticker(symbol).history(period="2d")
                if len(hist) >= 2:
                    prev = hist["Close"].iloc[-2]
                    last = hist["Close"].iloc[-1]
                    change = (last - prev) / prev * 100
                    signals[name] = round(change, 2)

            avg_change = np.mean(list(signals.values())) if signals else 0

            # 미국 시장 점수 (시장 강도에 가감)
            if avg_change >= 1.0:
                result = ("강세", -10)    # 미국 강세 → 크립토 호재 → 임계값 낮춤
            elif avg_change >= 0:
                result = ("보통", 0)
            elif avg_change >= -1.0:
                result = ("약세", 3)      # 미국 약세 → 주의 → 임계값 소폭 올림
            else:
                result = ("급락", 3)      # 미국 급락 → 크립토 위험 → 임계값 소폭 올림

            print(f"🇺🇸 미국시장: S&P500 {signals.get('S&P500', 0):+.2f}%, NASDAQ {signals.get('NASDAQ', 0):+.2f}% → {result[0]} ({result[1]:+d}점)")

            self._us_cache = result
            self._us_cache_time = now
            return result
        except Exception as e:
            print(f"⚠️ 미국시장 조회 실패: {e}")
            return ("조회실패", 0)

    def check_market_strength(self):
        """시장 전체 강도 (v3.2: 약세장에서도 수익 내는 봇)
        - 강세장은 누구나 번다 → 기준 낮게
        - 약세장이 진짜 실력 → 기준을 높이지 않고 선별력으로 승부
        - 극약세만 소폭 방어, 나머지는 적극 진입
        """
        try:
            scores = []
            for coin in ["BTC", "ETH", "XRP"]:
                ticker = f"KRW-{coin}"
                ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
                closes = [x for x in ohlcv['close']]
                change = (closes[-1] - closes[0]) / closes[0]
                scores.append(change)

            avg = np.mean(scores) if scores else 0

            # v5.4: 기준 현실화 — 극공포장에서 모멘텀 TOP 25점대인 현실 반영
            # AutoTune threshold_adjust는 악순환 유발하므로 적용 안함
            if avg < -0.15:
                return "극약세", 30     # 폭락장: 30점 이상만
            elif avg < -0.10:
                return "심약세", 28     # 심약세: 28점
            elif avg < -0.05:
                return "약세", 25       # 약세: 25점
            elif avg < 0.03:
                return "보통", 22       # 평시: 적극 진입
            else:
                return "강세", 18       # 강세: 공격적
        except Exception as _e:
            print(f"⚠️ 시장 강도 체크 오류: {_e}")
            return "보통", 22
    
    def _calculate_momentum_inner(self, coin):
        """종합 모멘텀 점수 내부 계산 (v3.2: 거품 보정)"""
        vol         = self.analyze_volume(coin)
        rsi         = self.analyze_rsi(coin)
        support     = self.analyze_support_resistance(coin)
        price       = self.analyze_price_change(coin)
        pattern     = self.detect_patterns(coin)
        news        = self.analyze_news_sentiment(coin)
        fundamental = self.analyze_fundamentals(coin)
        abnormal    = self.detect_abnormal_trading(coin)

        # v4.0: 가중치 재분배 (PA 컨텍스트 5% 신규, S/R 10→15%, 패턴 20→22%)
        abnormal_score = max(-20, min(30, abnormal["score"]))
        abnormal_normalized = 50 + abnormal_score

        # PA 컨텍스트 점수 (추세선/채널 분석)
        pa_context = 50  # 기본값
        try:
            tl = self._calc_trendlines(coin)
            if tl:
                if tl['trend_direction'] == 'up':
                    pa_context = 80
                elif tl['trend_direction'] == 'converging' and tl['convergence_ratio'] > 0.7:
                    pa_context = 90  # 수렴 끝단
                elif tl['trend_direction'] == 'down':
                    pa_context = 20
        except:
            pass

        # v4.5: 동적 가중치 (AutoTune이 승패 분석으로 자동 조정)
        w = self._momentum_weights
        total = (
            vol         * w['vol'] +         # 거래량 (기본 20%)
            rsi         * w['rsi'] +         # RSI (기본 20%)
            support     * w['support'] +     # S/R 매물대 (기본 13%)
            price       * w['price'] +       # 가격 상승률 (기본 13%)
            pattern     * w['pattern'] +     # 패턴 (기본 15%)
            news        * w['news'] +        # 뉴스/SNS (기본 8%)
            fundamental * w['fundamental'] + # 펀더멘털 (기본 3%)
            abnormal_normalized * w['abnormal'] +  # 이상거래 (기본 5%)
            pa_context  * w['pa_context']    # 추세선/채널 (기본 3%)
        )

        if abnormal["whale"]:
            print(f"  🐋 {coin} 세력 매집 감지 (정규화 {abnormal_normalized:.0f}점)")
        if abnormal["manipulation"]:
            print(f"  ⚠️ {coin} 투매/조작 의심 (정규화 {abnormal_normalized:.0f}점)")

        # v3.3: 개별 강세 코인 감점 면제
        # 24시간 +15% 이상 상승한 코인은 시장과 무관하게 독자 모멘텀 보유
        is_individual_strong = False
        try:
            ticker = f"KRW-{coin}"
            ohlcv_24h = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
            if ohlcv_24h is not None and len(ohlcv_24h) >= 2:
                change_24h = (ohlcv_24h['close'].iloc[-1] - ohlcv_24h['close'].iloc[0]) / ohlcv_24h['close'].iloc[0]
                if change_24h >= 0.15:
                    is_individual_strong = True
                    print(f"  🔥 {coin} 개별 강세 감지 (24h {change_24h*100:+.1f}%) → 감점 면제")
        except:
            pass

        if not is_individual_strong:
            penalty = 0
            if not self._is_uptrend(coin):
                penalty += 15  # v4.3: 5→15 (비상승 추세 패널티 강화)
            if self._is_near_high(coin):
                penalty += 5
            if penalty > 0:
                total -= penalty

        return round(min(100, max(0, total)), 1)

    # ============================================
    # v4.4: 자동 학습 시스템 (Auto-Tune)
    # ============================================

    def _get_autotune_threshold_adjust(self):
        """AutoTune threshold_adjust 규칙에서 기준 보정값 반환"""
        for rule in self._autotune_rules:
            if rule['rule_type'] == 'threshold_adjust':
                return int(rule['param_value'])
        return 0

    def _autotune_analyze(self):
        """v5.4: 최근 6시간 거래 분석 → 자동 조정"""
        rules = []
        now = datetime.now()
        six_hours_ago = (now - __import__('datetime').timedelta(hours=6)).isoformat()

        try:
            with self._db_lock:
                cursor = self.db.execute(
                    "SELECT coin, action, profit_rate, timestamp FROM trades WHERE timestamp >= ? ORDER BY timestamp",
                    (six_hours_ago,)
                )
                trades = cursor.fetchall()
        except Exception as e:
            print(f"⚠️ [AutoTune] DB 조회 오류: {e}")
            return rules

        if len(trades) < 3:
            print(f"🤖 [AutoTune] 거래 {len(trades)}건 — 최소 3건 필요, 분석 스킵")
            return rules

        # 매도 거래만 추출 (수익/손실 판단용)
        sells = [(coin, profit_rate, timestamp) for coin, action, profit_rate, timestamp in trades if action == 'sell']
        if len(sells) < 3:
            print(f"🤖 [AutoTune] 매도 {len(sells)}건 — 최소 3건 필요, 분석 스킵")
            return rules

        # --- 분석 1: 코인별 연패 (같은 코인 연속 3회 손실 → 블랙리스트) ---
        coin_results = {}
        for coin, profit_rate, ts in sells:
            if coin not in coin_results:
                coin_results[coin] = []
            coin_results[coin].append(profit_rate)

        for coin, results in coin_results.items():
            # 연속 손실 카운트
            consecutive_losses = 0
            for pr in results:
                if pr < 0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
            if consecutive_losses >= 3:
                # 블랙리스트 상한 체크 (최대 5코인)
                current_bl = len(self._autotune_blacklist)
                if current_bl < 5:
                    rules.append({
                        'rule_type': 'coin_blacklist',
                        'coin': coin,
                        'param_key': 'blacklist',
                        'param_value': 1,
                        'reason': f'{coin} 연속 {consecutive_losses}회 손실'
                    })

        # --- 분석 2: 시간대별 승률 (<25%, 5건+) → 모멘텀 부스트 ---
        hour_stats = {}
        for coin, profit_rate, ts in sells:
            try:
                hour = int(ts[11:13])
            except (ValueError, IndexError):
                continue
            if hour not in hour_stats:
                hour_stats[hour] = {'wins': 0, 'total': 0}
            hour_stats[hour]['total'] += 1
            if profit_rate > 0:
                hour_stats[hour]['wins'] += 1

        for hour, stats in hour_stats.items():
            if stats['total'] >= 5:
                win_rate = stats['wins'] / stats['total'] * 100
                if win_rate < 25:
                    rules.append({
                        'rule_type': 'momentum_boost',
                        'coin': None,
                        'param_key': str(hour),
                        'param_value': 10,
                        'reason': f'{hour}시 승률 {win_rate:.0f}% ({stats["wins"]}/{stats["total"]}건)'
                    })

        # --- 분석 3: 트레일링 놓침 (고점 +2% → 최종 -1% 비율 ≥40%) ---
        trailing_miss = 0
        trailing_total = 0
        for coin, action, profit_rate, ts in trades:
            if action == 'sell':
                trailing_total += 1
                # 고점은 DB에 없으므로 profit_rate < -0.01이면 고점에서 놓친 것으로 추정
                if profit_rate < -0.01:
                    trailing_miss += 1

        if trailing_total >= 5 and (trailing_miss / trailing_total) >= 0.4:
            rules.append({
                'rule_type': 'trailing_adjust',
                'coin': None,
                'param_key': 'trail_drop',
                'param_value': -0.003,
                'reason': f'고점 놓침 {trailing_miss}/{trailing_total}건 ({trailing_miss/trailing_total*100:.0f}%)'
            })

        # --- 분석 4: 손절 너무 늦음 (평균 손실 > -1.5%) ---
        losses = [pr for _, pr, _ in sells if pr < 0]
        if len(losses) >= 3:
            avg_loss = sum(losses) / len(losses)
            if avg_loss < -0.015:
                rules.append({
                    'rule_type': 'stoploss_adjust',
                    'coin': None,
                    'param_key': 'stop_loss',
                    'param_value': -0.005,
                    'reason': f'평균 손실 {avg_loss*100:.2f}% (너무 늦음)'
                })

        # --- 분석 5: 재진입 실패 (쿨다운 후 재진입 승률 <30%) ---
        # 같은 코인에 2회 이상 매도 → 재진입으로 간주
        reentry_results = []
        coin_sell_count = {}
        for coin, profit_rate, ts in sells:
            if coin not in coin_sell_count:
                coin_sell_count[coin] = 0
            coin_sell_count[coin] += 1
            if coin_sell_count[coin] >= 2:  # 2번째부터 재진입
                reentry_results.append(profit_rate)

        if len(reentry_results) >= 3:
            reentry_wins = sum(1 for pr in reentry_results if pr > 0)
            reentry_rate = reentry_wins / len(reentry_results) * 100
            if reentry_rate < 30:
                rules.append({
                    'rule_type': 'cooldown_extend',
                    'coin': None,
                    'param_key': 'cooldown_add',
                    'param_value': 600,
                    'reason': f'재진입 승률 {reentry_rate:.0f}% ({reentry_wins}/{len(reentry_results)}건)'
                })

        # --- 분석 6: 승패 기반 진입 기준 자동 조정 (v4.5) ---
        win_count = sum(1 for _, pr, _ in sells if pr > 0)
        total_sells = len(sells)
        win_rate = (win_count / total_sells * 100) if total_sells > 0 else 50

        # v5.4: threshold_adjust 제거 — 기준 올리면 매매 안 됨 → 학습 데이터 부족 → 악순환
        # 대신 승률이 낮으면 블랙리스트/쿨다운으로 대응 (위 분석 1, 5에서 처리)
        if total_sells >= 5:
            print(f"🤖 [AutoTune] 승률: {win_rate:.0f}% ({win_count}/{total_sells}건)")

        # --- 분석 7: 승패 기반 모멘텀 가중치 자동 조정 (v4.5) ---
        # 매수 시 모멘텀 점수가 높았던 지표가 수익/손실과 상관관계 분석
        if total_sells >= 8:
            try:
                with self._db_lock:
                    cursor = self.db.execute(
                        "SELECT coin, profit_rate, momentum FROM trades WHERE action='sell' AND timestamp >= ? ORDER BY timestamp DESC LIMIT 30",
                        (six_hours_ago,)
                    )
                    recent_sells = cursor.fetchall()

                if len(recent_sells) >= 8:
                    avg_profit = sum(pr for _, pr, _ in recent_sells) / len(recent_sells)
                    high_mom_profits = [pr for _, pr, mom in recent_sells if mom and mom >= 40]
                    low_mom_profits = [pr for _, pr, mom in recent_sells if mom and mom < 40]

                    # 높은 모멘텀 진입이 수익 좋으면 → vol/rsi 가중치 유지 (핵심 지표가 잘 작동)
                    # 높은 모멘텀인데도 손실이면 → pattern/news에 속은 것 → vol/rsi 비중 더 올림
                    if high_mom_profits and low_mom_profits:
                        high_avg = sum(high_mom_profits) / len(high_mom_profits)
                        low_avg = sum(low_mom_profits) / len(low_mom_profits)

                        if high_avg < low_avg:
                            # 모멘텀 높은데 오히려 손실 → 패턴/뉴스 과대평가, 거래량/RSI 비중 올림
                            rules.append({
                                'rule_type': 'weight_adjust',
                                'coin': None,
                                'param_key': 'vol_rsi_boost',
                                'param_value': 0.03,
                                'reason': f'고모멘텀 수익 {high_avg:+.2f}% < 저모멘텀 {low_avg:+.2f}% → 거래량/RSI↑ 패턴/뉴스↓'
                            })
                        elif avg_profit > 1.0:
                            # 전체적으로 수익 좋음 → 현재 가중치 유지 (안정)
                            print(f"🤖 [AutoTune] 가중치 안정 (평균수익 {avg_profit:+.2f}%)")
            except Exception as e:
                print(f"⚠️ [AutoTune] 가중치 분석 오류: {e}")

        # --- 분석 8: 무거래 시간 감지 (v4.5) ---
        # 최근 분석 기간 내 매수가 0건이면 기준이 너무 높은 것
        buys = [(coin, ts) for coin, action, profit_rate, ts in trades if action == 'buy']
        if len(buys) == 0 and total_sells == 0:
            # 거래가 하나도 없음 → 기준을 강제로 낮춤
            rules.append({
                'rule_type': 'threshold_adjust',
                'coin': None,
                'param_key': 'min_score_adjust',
                'param_value': -5,
                'reason': f'3시간 무거래 → 기준 -5점 긴급 완화'
            })
            print(f"🤖 [AutoTune] 3시간 무거래 감지 → 진입 기준 긴급 완화")

        return rules

    def _autotune_create_rules(self, rules):
        """분석 결과를 DB에 저장 (만료 정리 + 중복 방지)"""
        now = datetime.now()
        _td = __import__('datetime').timedelta
        # v5.9: 규칙 유형별 만료 기간 차별화 (블랙리스트는 7일, 나머지는 48시간)
        expires_blacklist = (now + _td(days=7)).isoformat()
        expires = (now + _td(hours=48)).isoformat()
        now_iso = now.isoformat()

        try:
            with self._db_lock:
                # 만료된 규칙 비활성화
                self.db.execute(
                    "UPDATE auto_tune_rules SET active = 0 WHERE active = 1 AND expires_at < ?",
                    (now_iso,)
                )

                # 현재 활성 규칙 수 확인
                cursor = self.db.execute("SELECT COUNT(*) FROM auto_tune_rules WHERE active = 1")
                active_count = cursor.fetchone()[0]

                for rule in rules:
                    if active_count >= 10:
                        print(f"🤖 [AutoTune] 최대 활성 규칙 10개 도달 — 추가 규칙 무시")
                        break

                    # 동일 유형+코인 중복 → 기존 갱신
                    cursor = self.db.execute(
                        "SELECT id FROM auto_tune_rules WHERE active = 1 AND rule_type = ? AND (coin = ? OR (coin IS NULL AND ? IS NULL))",
                        (rule['rule_type'], rule['coin'], rule['coin'])
                    )
                    existing = cursor.fetchone()

                    # v5.9: coin_blacklist는 7일 만료
                    rule_expires = expires_blacklist if rule.get('rule_type') == 'coin_blacklist' else expires

                    if existing:
                        self.db.execute(
                            "UPDATE auto_tune_rules SET param_value = ?, reason = ?, expires_at = ? WHERE id = ?",
                            (rule['param_value'], rule['reason'], rule_expires, existing[0])
                        )
                    else:
                        self.db.execute(
                            "INSERT INTO auto_tune_rules (rule_type, coin, param_key, param_value, reason, created_at, expires_at, active) VALUES (?,?,?,?,?,?,?,1)",
                            (rule['rule_type'], rule['coin'], rule['param_key'], rule['param_value'], rule['reason'], now_iso, rule_expires)
                        )
                        active_count += 1

                self.db.commit()
        except Exception as e:
            print(f"⚠️ [AutoTune] 규칙 저장 오류: {e}")

    def _autotune_apply_rules(self):
        """DB에서 활성 규칙을 로드하여 런타임 캐시 갱신"""
        try:
            now_iso = datetime.now().isoformat()
            with self._db_lock:
                # 만료 규칙 비활성화
                self.db.execute(
                    "UPDATE auto_tune_rules SET active = 0 WHERE active = 1 AND expires_at < ?",
                    (now_iso,)
                )
                self.db.commit()

                cursor = self.db.execute(
                    "SELECT rule_type, coin, param_key, param_value, reason FROM auto_tune_rules WHERE active = 1"
                )
                rows = cursor.fetchall()

            self._autotune_rules = []
            self._autotune_blacklist = set()

            for rule_type, coin, param_key, param_value, reason in rows:
                self._autotune_rules.append({
                    'rule_type': rule_type,
                    'coin': coin,
                    'param_key': param_key,
                    'param_value': param_value,
                    'reason': reason
                })
                if rule_type == 'coin_blacklist' and coin:
                    self._autotune_blacklist.add(coin)

                # v4.5: 가중치 동적 조정 적용
                if rule_type == 'weight_adjust' and param_key == 'vol_rsi_boost':
                    boost = min(0.05, param_value)  # 최대 5%씩 조정
                    w = self._momentum_weights
                    w['vol'] = min(0.25, w['vol'] + boost)
                    w['rsi'] = min(0.25, w['rsi'] + boost)
                    # 보정분을 pattern/news에서 차감 (합계 100% 유지)
                    deduct = boost * 2
                    w['pattern'] = max(0.05, w['pattern'] - deduct * 0.6)
                    w['news'] = max(0.03, w['news'] - deduct * 0.4)
                    print(f"🤖 [AutoTune] 가중치 조정 → 거래량 {w['vol']:.0%} RSI {w['rsi']:.0%} 패턴 {w['pattern']:.0%} 뉴스 {w['news']:.0%}")

        except Exception as e:
            print(f"⚠️ [AutoTune] 규칙 로드 오류: {e}")

    def _autotune_notify(self, rules, stats):
        """텔레그램으로 분석 결과 보고"""
        n = stats.get('total', 0)
        win_rate = stats.get('win_rate', 0)
        profit = stats.get('profit', 0)

        lines = [
            f"[AutoTune] 3시간 분석 완료",
            f"- 거래 {n}건 분석",
            f"- 승률 {win_rate:.1f}% | 수익 {profit:,.0f}원",
            f"- 신규 규칙 {len(rules)}개 생성:"
        ]
        for r in rules:
            coin_str = r['coin'] if r['coin'] else '전체'
            lines.append(f"  {r['rule_type']}({coin_str}): {r['reason']}")

        self._notify("\n".join(lines))

    def _get_recent_stats(self):
        """최근 3시간 거래 통계"""
        six_hours_ago = (datetime.now() - __import__('datetime').timedelta(hours=3)).isoformat()
        try:
            with self._db_lock:
                cursor = self.db.execute(
                    "SELECT COUNT(*), "
                    "SUM(CASE WHEN action='sell' AND profit_rate > 0 THEN 1 ELSE 0 END), "
                    "SUM(CASE WHEN action='sell' THEN 1 ELSE 0 END), "
                    "COALESCE(SUM(CASE WHEN action='sell' THEN profit ELSE 0 END), 0) "
                    "FROM trades WHERE timestamp >= ?",
                    (six_hours_ago,)
                )
                row = cursor.fetchone()

            total = row[0] or 0
            wins = row[1] or 0
            sell_count = row[2] or 0
            profit = row[3] or 0
            win_rate = (wins / sell_count * 100) if sell_count > 0 else 0

            return {'total': total, 'win_rate': win_rate, 'profit': profit}
        except Exception as e:
            print(f"⚠️ [AutoTune] 통계 조회 오류: {e}")
            return {'total': 0, 'win_rate': 0, 'profit': 0}

    def _autotune_run(self):
        """자동 학습 메인 루프"""
        try:
            print(f"\n🤖 [AutoTune] 자동 분석 시작...")
            rules = self._autotune_analyze()
            if rules:
                self._autotune_create_rules(rules)
                stats = self._get_recent_stats()
                self._autotune_notify(rules, stats)
                print(f"🤖 [AutoTune] {len(rules)}개 규칙 생성 완료")
            else:
                print(f"🤖 [AutoTune] 조정 필요 없음")
            self._autotune_apply_rules()
            # v5.6: 패배 패턴 자동 개선
            self._autotune_pattern_improve()
        except Exception as e:
            print(f"⚠️ [AutoTune] 오류: {e}")

    def _autotune_pattern_improve(self):
        """v5.6: 패배 패턴 분석 → 시뮬레이션 → 승률 개선 시에만 적용"""
        try:
            from datetime import timedelta
            now = datetime.now()
            window = (now - timedelta(hours=24)).isoformat()

            with self._db_lock:
                cursor = self.db.execute(
                    """SELECT t2.timestamp, t2.coin, t2.profit, t2.profit_rate, t2.batch, t2.momentum,
                              t1.timestamp as buy_ts
                       FROM trades t2
                       JOIN trades t1 ON t1.coin = t2.coin AND t1.batch = t2.batch
                            AND t1.action='buy' AND t1.id < t2.id
                       WHERE t2.action='sell' AND t2.timestamp >= ?
                       ORDER BY t2.id""",
                    (window,)
                )
                sells = cursor.fetchall()

            if len(sells) < 10:
                return

            # 거래 파싱
            trades = []
            for r in sells:
                try:
                    hold = (datetime.fromisoformat(r[0]) - datetime.fromisoformat(r[6])).total_seconds() / 60
                    hour = int(r[0][11:13])
                except:
                    hold, hour = 0, 12
                trades.append({
                    'profit': r[2] or 0, 'pr': r[3] or 0, 'batch': r[4],
                    'mom': r[5] or 0, 'hold': hold, 'hour': hour, 'coin': r[1]
                })

            total = len(trades)
            wins = sum(1 for t in trades if t['profit'] > 0)
            current_wr = wins / total * 100
            current_pnl = sum(t['profit'] for t in trades)

            # === 패턴 후보 탐색 ===
            candidates = []

            # 패턴1: surge 가짜급등 (5분내 -1%+)
            p1 = [t for t in trades if t['batch'] == 'surge_trade' and t['hold'] <= 5 and t['pr'] <= -1.0]
            p1_wins = [t for t in trades if t['batch'] == 'surge_trade' and t['hold'] <= 5 and t['pr'] > 0]
            if len(p1) >= 2 and len(p1_wins) == 0:
                saved = sum(abs(t['profit']) for t in p1)
                candidates.append(('surge_quick_loss', len(p1), saved, 'surge 5분내 -1%+ 차단'))

            # 패턴2: 특정 시간대 전패
            for h_start, h_end, label in [(6, 9, 'dawn'), (23, 24, 'late_night'), (0, 5, 'midnight')]:
                period = [t for t in trades if h_start <= t['hour'] < h_end]
                period_wins = [t for t in period if t['profit'] > 0]
                period_losses = [t for t in period if t['profit'] < 0]
                if len(period) >= 3 and len(period_wins) == 0 and len(period_losses) >= 2:
                    saved = sum(abs(t['profit']) for t in period_losses)
                    candidates.append((f'time_block_{label}', len(period_losses), saved, f'{h_start}~{h_end}시 전패'))

            # 패턴3: 특정 코인 연패 (3회+)
            coin_trades = {}
            for t in trades:
                coin_trades.setdefault(t['coin'], []).append(t)
            for coin, cts in coin_trades.items():
                losses = [t for t in cts if t['profit'] < 0]
                wins_c = [t for t in cts if t['profit'] > 0]
                if len(losses) >= 3 and len(wins_c) == 0:
                    saved = sum(abs(t['profit']) for t in losses)
                    candidates.append((f'coin_ban_{coin}', len(losses), saved, f'{coin} {len(losses)}연패'))

            # 패턴4: 보유시간별 패턴 (특정 구간 승률 0%)
            for hold_min, hold_max, label in [(0, 3, 'ultra_quick'), (30, 60, 'mid_hold')]:
                band = [t for t in trades if hold_min <= t['hold'] < hold_max]
                band_wins = [t for t in band if t['profit'] > 0]
                band_losses = [t for t in band if t['profit'] < 0]
                if len(band) >= 3 and len(band_wins) == 0 and len(band_losses) >= 2:
                    saved = sum(abs(t['profit']) for t in band_losses)
                    candidates.append((f'hold_{label}', len(band_losses), saved, f'{hold_min}~{hold_max}분 보유 전패'))

            if not candidates:
                print(f"🤖 [AutoTune] 패턴 분석: 개선 후보 없음 (현재 승률 {current_wr:.1f}%)")
                return

            # === 시뮬레이션: 후보 적용 시 승률 계산 ===
            best = None
            for name, blocked_count, saved_pnl, desc in candidates:
                # 패배만 차단, 승리는 유지 → 승률 계산
                new_total = total - blocked_count
                new_wins = wins  # 승리는 그대로
                if new_total > 0:
                    new_wr = new_wins / new_total * 100
                    improvement = new_wr - current_wr
                    if improvement > 0 and (best is None or improvement > best['improvement']):
                        best = {
                            'name': name, 'desc': desc,
                            'blocked': blocked_count, 'saved': saved_pnl,
                            'new_wr': new_wr, 'improvement': improvement
                        }

            if not best:
                print(f"🤖 [AutoTune] 패턴 분석: 승률 개선 후보 없음")
                return

            # === 승률 개선 확인 → 적용 ===
            print(f"🤖 [AutoTune] 패턴 발견: {best['desc']}")
            print(f"   차단 {best['blocked']}건 | 절약 +{best['saved']:,.0f}원 | 승률 {current_wr:.1f}% → {best['new_wr']:.1f}% (+{best['improvement']:.1f}%p)")

            # 규칙 적용
            rule_name = best['name']
            if rule_name == 'surge_quick_loss':
                # surge 최소 보유시간을 5분으로 확장 (현재 3분)
                # → _surge_min_hold 변수로 관리
                if not hasattr(self, '_surge_min_hold') or self._surge_min_hold < 5:
                    self._surge_min_hold = 5
                    print(f"   ✅ 적용: surge 최소 보유 3분 → 5분")
            elif rule_name.startswith('time_block_'):
                # 해당 시간대 모멘텀 기준 강화
                if not hasattr(self, '_autotune_time_boost'):
                    self._autotune_time_boost = {}
                period = rule_name.replace('time_block_', '')
                self._autotune_time_boost[period] = 10  # +10점 부스트
                print(f"   ✅ 적용: {best['desc']} → min_score +10점")
            elif rule_name.startswith('coin_ban_'):
                coin = rule_name.replace('coin_ban_', '')
                if coin not in self._autotune_blacklist:
                    self._autotune_blacklist.add(coin)
                    print(f"   ✅ 적용: {coin} 블랙리스트 추가")
            elif rule_name.startswith('hold_'):
                # 보유시간 관련 → 최소 보유시간 확대
                if not hasattr(self, '_min_hold_normal'):
                    self._min_hold_normal = 5
                print(f"   ✅ 적용: 일반 매매 최소 보유 5분")

            self._notify(f"[AutoTune] 패턴 개선: {best['desc']} | 승률 {current_wr:.1f}→{best['new_wr']:.1f}% (+{best['improvement']:.1f}%p) | 절약 +{best['saved']:,.0f}원")

        except Exception as e:
            print(f"⚠️ [AutoTune] 패턴 분석 오류: {e}")

    def calculate_momentum(self, coin):
        """종합 모멘텀 점수 (0~100점) - 5분 캐시 + 15초 타임아웃 + v5.2 EMA 평활화"""
        now = time.time()
        if not hasattr(self, '_momentum_cache'):
            self._momentum_cache = {}
        cached = self._momentum_cache.get(coin)
        if cached and now - cached[0] < 300:
            return cached[1]

        result, err = _run_with_timeout(
            lambda: self._calculate_momentum_inner(coin), timeout=15
        )
        raw_score = result if result is not None and not err else 0

        # v5.2: EMA 평활화 — 이전 값과 7:3 가중 (급변 방지)
        if cached and cached[1] > 0:
            score = round(raw_score * 0.7 + cached[1] * 0.3, 1)
        else:
            score = raw_score

        self._momentum_cache[coin] = (now, score)
        return score
    
    # ============================================
    # 2. 코인 선택 (카테고리 다른 것)
    # ============================================
    
    def select_coins(self):
        """전체 마켓 스캔 후 모멘텀 상위 코인 선택 (병렬 처리)"""
        total = len(self.coins)
        print(f"🔍 전체 {total}개 코인 병렬 스캔 중 (워커 4개)...")
        sys.stdout.flush()
        scores = {}
        skip_count = 0
        done_count = 0
        lock = threading.Lock()

        def _scan_coin(coin):
            try:
                score = self.calculate_momentum(coin)
                return coin, score
            except Exception:
                return coin, None

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(_scan_coin, c): c for c in self.coins}
            for future in as_completed(futures):
                coin, score = future.result()
                with lock:
                    done_count += 1
                    if score is not None:
                        scores[coin] = score
                    else:
                        skip_count += 1
                    if done_count % 40 == 0 or done_count == total:
                        msg = f"  ... {done_count}/{total} 스캔 완료"
                        if skip_count > 0:
                            msg += f" (스킵 {skip_count}건)"
                        print(msg)
                        sys.stdout.flush()

        # v4.4: AutoTune 모멘텀 부스트 적용 (특정 시간대 진입 문턱 상승)
        current_hour = datetime.now().hour
        momentum_boost = 0
        for rule in self._autotune_rules:
            if rule['rule_type'] == 'momentum_boost' and rule['param_key'] == str(current_hour):
                momentum_boost = min(rule['param_value'], 10)  # 최대 +10
                print(f"🤖 [AutoTune] {current_hour}시 모멘텀 부스트 +{momentum_boost:.0f}점 적용")
                break

        sorted_coins = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        # 상위 10개 출력
        print("\n📊 모멘텀 TOP 10:")
        for rank, (coin, score) in enumerate(sorted_coins[:10], 1):
            print(f"  {rank}. {coin}: {score}점")
        print()

        # 점수가 (0 + 부스트)보다 큰 코인만 반환 (최대 10개)
        min_score = momentum_boost
        selected = [coin for coin, score in sorted_coins[:10] if score > min_score]

        # v3.3: 개별 강세 코인 강제 포함 (TOP 30 중 24h +15% 이상 → 추가)
        strong_coins = []
        candidates = [c for c, s in sorted_coins[:30] if c not in selected]
        for coin in candidates:
            try:
                _tk = f"KRW-{coin}"
                _oh = pyupbit.get_ohlcv(_tk, interval="minute60", count=24)
                if _oh is not None and len(_oh) >= 2:
                    _chg = (_oh['close'].iloc[-1] - _oh['close'].iloc[0]) / _oh['close'].iloc[0]
                    if _chg >= 0.15:
                        strong_coins.append((coin, scores[coin], _chg))
            except:
                pass
        # 상승률 순으로 최대 3개 추가
        strong_coins.sort(key=lambda x: x[2], reverse=True)
        for coin, score, chg in strong_coins[:3]:
            selected.append(coin)
            print(f"  🔥 {coin} 개별 강세 추가 (24h {chg*100:+.1f}%, 모멘텀 {score}점)")
        if not strong_coins:
            # TOP 30 밖에서도 빠르게 탐색 (업비트 24h 변동률 API 활용)
            try:
                import requests as req_lib2
                resp = req_lib2.get("https://api.upbit.com/v1/ticker",
                    params={"markets": ",".join([f"KRW-{c}" for c, s in sorted_coins[30:]])},
                    timeout=10)
                if resp.status_code == 200:
                    for item in resp.json():
                        chg_rate = item.get('signed_change_rate', 0)
                        if chg_rate >= 0.15:
                            c = item['market'].replace('KRW-', '')
                            if c not in selected:
                                strong_coins.append((c, scores.get(c, 0), chg_rate))
                    strong_coins.sort(key=lambda x: x[2], reverse=True)
                    for coin, score, chg in strong_coins[:3]:
                        selected.append(coin)
                        print(f"  🔥 {coin} 개별 강세 추가 (24h {chg*100:+.1f}%, 모멘텀 {score}점)")
            except:
                pass

        return selected
    
    # ============================================
    # 3. 자동 매수/매도
    # ============================================
    
    MAX_POSITIONS = 2  # v5.12: 3→2 (확신 코인 집중, 건당 비중 확대)

    # 코인 섹터 분류 (동일 섹터 집중 방지)
    COIN_SECTORS = {
        # L1 (레이어1)
        'BTC': 'L1', 'ETH': 'L1', 'SOL': 'L1', 'AVAX': 'L1', 'ADA': 'L1',
        'DOT': 'L1', 'ATOM': 'L1', 'NEAR': 'L1', 'SUI': 'L1', 'APT': 'L1',
        'SEI': 'L1', 'TON': 'L1', 'TRX': 'L1', 'XRP': 'L1', 'ALGO': 'L1',
        'HBAR': 'L1', 'XLM': 'L1', 'XTZ': 'L1', 'KAVA': 'L1', 'MINA': 'L1',
        'STX': 'L1', 'BERA': 'L1', 'MNT': 'L1', 'INJ': 'L1', 'FTM': 'L1',
        # L2 (레이어2)
        'ARB': 'L2', 'OP': 'L2', 'POL': 'L2', 'LINEA': 'L2', 'ZK': 'L2',
        'STRK': 'L2', 'TAIKO': 'L2', 'ASTR': 'L2', 'ZKC': 'L2',
        # DeFi
        'UNI': 'DEFI', 'AAVE': 'DEFI', 'COMP': 'DEFI', 'CRV': 'DEFI',
        'MKR': 'DEFI', 'SNX': 'DEFI', 'SUSHI': 'DEFI', 'CAKE': 'DEFI',
        'RAY': 'DEFI', 'ONDO': 'DEFI', 'DRIFT': 'DEFI', 'ENA': 'DEFI',
        'LPT': 'DEFI', 'KNC': 'DEFI', 'AUCTION': 'DEFI',
        # Gaming/Metaverse
        'AXS': 'GAME', 'SAND': 'GAME', 'MANA': 'GAME', 'GALA': 'GAME',
        'IMX': 'GAME', 'BEAM': 'GAME', 'YGG': 'GAME', 'BIGTIME': 'GAME',
        'SUPER': 'GAME', 'GAME2': 'GAME', 'WLFI': 'GAME', 'ANIME': 'GAME',
        # Meme
        'DOGE': 'MEME', 'SHIB': 'MEME', 'PEPE': 'MEME', 'BONK': 'MEME',
        'FLOKI': 'MEME', 'WIF': 'MEME', 'MOODENG': 'MEME', 'TURBO': 'MEME',
        'TRUMP': 'MEME', 'PENGU': 'MEME', 'TOSHI': 'MEME', 'PUMP': 'MEME',
        # AI
        'FET': 'AI', 'RENDER': 'AI', 'RNDR': 'AI', 'VIRTUAL': 'AI',
        'TAO': 'AI', 'ARKM': 'AI', 'DEEP': 'AI', 'KAITO': 'AI',
        # Infra/Oracle
        'LINK': 'INFRA', 'GRT': 'INFRA', 'FIL': 'INFRA', 'AR': 'INFRA',
        'STORJ': 'INFRA', 'SC': 'INFRA', 'PYTH': 'INFRA', 'ENS': 'INFRA',
        'CHZ': 'INFRA', 'VET': 'INFRA', 'IOTA': 'INFRA',
    }

    def place_buy_order(self, coin, amount, momentum=0):
        """매수 주문 (최대 3종목 제한 포함)"""
        try:
            # ★ 텔레그램 /pause 상태면 매수 차단
            if getattr(self, '_tg_paused', False):
                print(f"⏸ {coin} 매수 차단: 일시중지 상태 (/resume으로 재개)")
                return False

            # ★ 최대 포지션 제한 (이미 보유 중인 코인의 추가 배치는 허용)
            if coin not in self.positions and len(self.positions) >= self.MAX_POSITIONS:
                print(f"🚫 {coin} 매수 차단: 이미 {len(self.positions)}종목 보유 (최대 {self.MAX_POSITIONS}종목)")
                return False

            # v5.15: 심야(23~06시) 모든 경로 매수 차단 (횡보교체/2차매수 포함)
            if getattr(self, '_nighttime_no_buy', False):
                print(f"🌙 {coin} 매수 차단: 심야 시간대 (포지션 관리만)")
                return False

            # ★ 투자유의/거래종료 종목 절대 금지
            if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                print(f"🚫 {coin} 매수 차단: 투자유의/거래종료 예정 종목")
                return False

            # v4.4: 자동학습 블랙리스트 체크
            if coin in self._autotune_blacklist:
                print(f"🤖 {coin} 매수 차단: AutoTune 블랙리스트")
                return False

            # v5.9: 영구 블랙리스트 (실데이터 기반 전패 코인)
            if coin in self._permanent_blacklist:
                print(f"🚫 {coin} 매수 차단: 영구 블랙리스트 (누적 전패 코인)")
                return False

            # v4.3: 일일 재진입 카운터 리셋 (자정 넘기면)
            today = datetime.now().date()
            if self._daily_reset_date != today:
                self._daily_coin_buys = {}
                self._daily_reset_date = today

            # v4.3: 같은 코인 하루 3회 초과 매수 차단
            daily_count = self._daily_coin_buys.get(coin, 0)
            if daily_count >= self._max_daily_coin_buys:
                print(f"🚫 {coin} 매수 차단: 오늘 이미 {daily_count}회 매수 (최대 {self._max_daily_coin_buys}회/일)")
                return False

            # v4.3: batch 상한 (batch_4 이상 추가매수 차단)
            if coin in self.positions:
                num_batches = len(self.positions[coin])
                if num_batches >= self._max_batch:
                    print(f"🚫 {coin} 매수 차단: 이미 {num_batches}배치 보유 (최대 {self._max_batch}배치)")
                    return False

            # 최소 주문금액 체크 (업비트 5,000원)
            if amount < 5000:
                print(f"⚠️ {coin} 주문금액 {amount:,}원 < 최소 5,000원, 패스")
                return False

            price = self.get_price(coin)
            if not price:
                print(f"❌ {coin} 가격 조회 실패")
                return False

            if self.mode == "real" and self.upbit:
                # 실제 잔고 확인
                krw = self._get_krw_balance()
                if krw < amount:
                    print(f"⚠️ {coin} 잔고 부족 (필요: {amount:,}원, 보유: {krw:,.0f}원)")
                    return False
                # 실제 주문
                result = self.upbit.buy_market_order(f"KRW-{coin}", amount)
                if result is None or (isinstance(result, dict) and 'error' in result):
                    error_msg = result.get('error', {}).get('message', '알 수 없음') if isinstance(result, dict) else '응답 없음'
                    print(f"❌ {coin} 매수 실패: {error_msg}")
                    return False
                print(f"📤 {coin} 매수 주문 전송: {result.get('uuid', '')}")
                # 체결 대기
                time.sleep(3)
                # 체결 후 실제 잔고 갱신
                self.current_balance = self._get_krw_balance()
            else:
                self.current_balance -= amount

            quantity = amount / price

            # real 모드: 매수 후 실제 체결 수량 확인 (수수료 0.05% 반영)
            if self.mode == "real" and self.upbit:
                real_qty = float(self.upbit.get_balance(coin) or 0)
                prev_qty = 0
                if coin in self.positions:
                    prev_qty = sum(p['quantity'] for p in self.positions[coin].values())
                actual_qty = real_qty - prev_qty
                if actual_qty > 0:
                    quantity = actual_qty
                else:
                    quantity = amount / price * 0.9995  # 수수료 반영 추정치

            if coin not in self.positions:
                self.positions[coin] = {}

            # v4.2: 기존 batch_id 겹침 방지 (batch_1 매도 후 batch_2만 남은 상태에서 신규 매수 시 충돌 방지)
            existing_nums = [int(b.split('_')[1]) for b in self.positions[coin] if b.startswith('batch_')]
            next_num = max(existing_nums, default=0) + 1
            batch_id = f"batch_{next_num}"
            # v4.0: 매수 시점 S/R 기록 (청산 시 활용)
            sr_at_entry = self._calc_sr_levels(coin)
            self.positions[coin][batch_id] = {
                'buy_price': price,
                'quantity': quantity,
                'amount': amount,
                'timestamp': datetime.now().isoformat(),
                'individual_strong': getattr(self, '_current_buy_is_strong', False),
                'surge_mode': getattr(self, '_current_buy_is_surge', False),
                'sr_support': sr_at_entry['nearest_support'] if sr_at_entry else None,
                'sr_resistance': sr_at_entry['nearest_resistance'] if sr_at_entry else None,
                'momentum': momentum,  # v5.9: 진입 모멘텀 기록 (매도 시 DB 기록용)
            }

            # v4.3: 일일 매수 카운터 증가
            self._daily_coin_buys[coin] = self._daily_coin_buys.get(coin, 0) + 1

            print(f"✅ [매수] {coin} {amount:,}원 @ {price:,.0f}원 (모멘텀 {momentum}점, 오늘 {self._daily_coin_buys[coin]}회차)")
            self._db_log_trade(coin, "buy", price, quantity, amount, momentum=momentum, batch=batch_id)
            self._save_positions()
            self._last_new_entry_time = time.time()  # 신규 진입 쿨타임 기록
            self._notify(f"[BUY] {coin} {batch_id} | {amount:,}원 @ {price:,.0f}원 | 잔고: {self.current_balance:,.0f}원")
            return True
        except Exception as e:
            print(f"❌ 매수 오류: {e}")
            self._notify(f"[ERROR] {coin} 매수 오류: {e}")
            # v4.2: 데모 모드에서 잔고가 이미 차감됐으면 롤백
            if self.mode == "demo":
                self.current_balance += amount
                print(f"  ↩️ 잔고 롤백: +{amount:,}원 → {self.current_balance:,.0f}원")
            return False
    
    def place_sell_order(self, coin, batch_id):
        """배치별 매도"""
        try:
            if coin not in self.positions or batch_id not in self.positions[coin]:
                return False

            position = self.positions[coin][batch_id]
            price = self.get_price(coin)

            if not price:
                return False

            quantity = position['quantity']
            sell_amount = quantity * price
            profit = sell_amount - position['amount']
            profit_rate = (profit / position['amount']) * 100

            if self.mode == "real" and self.upbit:
                # 실제 보유량 확인
                real_qty = self.upbit.get_balance(coin)
                if real_qty is None or float(real_qty) <= 0:
                    print(f"⚠️ {coin} 실제 보유량 없음, 포지션 정리")
                    del self.positions[coin][batch_id]
                    if not self.positions[coin]:
                        del self.positions[coin]
                    self._save_positions()
                    return False
                # 보유량이 기록보다 적으면 실제 보유량만큼만 매도
                sell_qty = min(quantity, float(real_qty))
                result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                if result is None or (isinstance(result, dict) and 'error' in result):
                    error_msg = result.get('error', {}).get('message', '알 수 없음') if isinstance(result, dict) else '응답 없음'
                    print(f"❌ {coin} 매도 실패: {error_msg}")
                    return False
                print(f"📤 {coin} 매도 주문 전송: {result.get('uuid', '')}")
                time.sleep(3)
                self.current_balance = self._get_krw_balance()
            else:
                self.current_balance += sell_amount

            print(f"✅ [매도] {coin} {batch_id}: {profit_rate:+.2f}% ({profit:+,.0f}원)")

            self.trades.append({
                'coin': coin,
                'batch': batch_id,
                'buy_price': position['buy_price'],
                'sell_price': price,
                'profit_rate': profit_rate,
                'timestamp': datetime.now().isoformat()
            })
            # v5.9: 진입 시 저장한 모멘텀 기록 (AutoTune 분석 정확도 개선)
            _entry_momentum = position.get('momentum', 0)
            self._last_sell_pnl[coin] = profit_rate  # 손실 비례 쿨다운용
            self._db_log_trade(coin, "sell", price, quantity, sell_amount,
                               profit=profit, profit_rate=profit_rate,
                               momentum=_entry_momentum, batch=batch_id)

            del self.positions[coin][batch_id]

            if not self.positions[coin]:
                del self.positions[coin]

            self._save_positions()
            self._notify(f"[SELL] {coin} {batch_id} | {profit_rate:+.2f}% ({profit:+,.0f}원) | 잔고: {self.current_balance:,.0f}원")
            return True
        except Exception as e:
            print(f"❌ {coin} 매도 오류: {e}")
            self._notify(f"[ERROR] {coin} 매도 오류: {e}")
            return False
    
    # ============================================
    # 4. 거래 사이클
    # ============================================
    
    def run_trading_cycle(self):
        """거래 사이클"""
        print(f"\n📊 [{datetime.now()}] === 거래 사이클 시작 ===\n")

        # v4.4: 사이클 시작 시 AutoTune 규칙 캐시 갱신
        self._autotune_apply_rules()

        # v5.15: 심야(23~06시) 신규 매수 플래그 (포지션 관리는 유지)
        _cur_hour_cycle = datetime.now().hour
        self._nighttime_no_buy = (_cur_hour_cycle >= 23 or _cur_hour_cycle < 6)
        if self._nighttime_no_buy:
            print(f"🌙 심야({_cur_hour_cycle:02d}시) → 포지션 관리만, 신규 매수 차단")

        # 포지션 모니터링
        self.monitor_positions()

        # 2차 분할매수 처리 (5분 경과된 것) - 모멘텀 재확인 후 실행
        for coin in list(self._pending_2nd_buy.keys()):
            entry = self._pending_2nd_buy[coin]
            if time.time() - entry['time'] >= entry['wait']:
                # 1차 매수 후 모멘텀 재확인 (붕괴 시 2차 매수 취소)
                mom_2nd = self.calculate_momentum(coin)
                if mom_2nd < 35:
                    print(f"🚫 {coin} 2차 매수 취소: 모멘텀 붕괴 ({mom_2nd}점 < 35점)")
                    del self._pending_2nd_buy[coin]
                elif coin not in self.positions:
                    print(f"🚫 {coin} 2차 매수 취소: 이미 매도된 포지션")
                    del self._pending_2nd_buy[coin]
                else:
                    # ★ v3.2.1: 2차 매수 조건 강화 (A+C)
                    # 조건1: 현재가 < 1차 매수가 (진짜 물타기만)
                    # 조건2: 모멘텀 ≥ 1차 매수시 - 10 (모멘텀 급락 방어)
                    # v4.2: batch_1 하드코딩 대신 첫 번째(가장 낮은 번호) 배치 동적 참조
                    first_batch_id = min(self.positions[coin].keys()) if self.positions[coin] else None
                    batch1_pos = self.positions[coin].get(first_batch_id) if first_batch_id else None
                    elapsed_min = (time.time() - entry.get('created', entry['time'])) / 60
                    should_buy = False
                    reason = ""
                    mom_1st = entry.get('mom_1st', 50)  # 1차 매수 시 모멘텀

                    if batch1_pos:
                        current_price = self.get_price(coin)
                        if current_price:
                            price_drop = (current_price - batch1_pos['buy_price']) / batch1_pos['buy_price']

                            # 조건1: 현재가 < 1차 매수가 (진짜 물타기만)
                            if price_drop >= 0:
                                if elapsed_min >= 20:
                                    # 20분 경과해도 가격이 안 내려감 → 2차 포기
                                    print(f"🚫 {coin} 2차 매수 포기: 20분 경과 + 가격 미하락 ({price_drop*100:+.2f}%)")
                                    del self._pending_2nd_buy[coin]
                                else:
                                    print(f"⏳ {coin} 2차 대기: 가격 미하락 ({price_drop*100:+.2f}%) | 모멘텀 {mom_2nd}점 | {elapsed_min:.0f}분/{20}분")
                                    entry['time'] = time.time()
                                continue

                            # 조건2: 모멘텀 ≥ 1차 - 10 (모멘텀 급락 방어)
                            if mom_2nd < mom_1st - 10:
                                if elapsed_min >= 20:
                                    print(f"🚫 {coin} 2차 매수 포기: 모멘텀 급락 ({mom_1st}→{mom_2nd}점, 기준 {mom_1st-10}점)")
                                    del self._pending_2nd_buy[coin]
                                else:
                                    print(f"⏳ {coin} 2차 대기: 모멘텀 급락 ({mom_1st}→{mom_2nd}점) | {price_drop*100:+.2f}% | {elapsed_min:.0f}분/{20}분")
                                    entry['time'] = time.time()
                                continue

                            # 두 조건 모두 충족 → 물타기 진입
                            should_buy = True
                            reason = f"물타기 진입 (1차 대비 {price_drop*100:+.2f}%, 모멘텀 {mom_1st}→{mom_2nd}점)"
                    else:
                        should_buy = True
                        reason = "1차 포지션 없음 → 즉시 진입"

                    if should_buy:
                        print(f"📥 {coin} 2차 분할매수 실행 ({entry['amount']:,}원, 모멘텀 {mom_2nd}점) | {reason}")
                        self.place_buy_order(coin, entry['amount'], momentum=mom_2nd)

                        # v4.1: 집중투자 3차 매수 예약 (2차 완료 후 3분 뒤)
                        if entry.get('concentrate') and entry.get('third_amount', 0) >= 5000:
                            third_amt = entry['third_amount']
                            self._pending_2nd_buy[coin] = {
                                'amount': third_amt,
                                'time': time.time(),
                                'created': time.time(),
                                'wait': 180,  # 3분 대기
                                'mom_1st': entry.get('mom_1st', mom_2nd),
                                'concentrate': False,  # 3차는 마지막
                            }
                            print(f"   📋 3차 집중매수 예약: {third_amt:,}원 (3분 후)")
                            continue  # del 하지 않고 새 엔트리로 교체됨
                    del self._pending_2nd_buy[coin]

        # 시장 강도 확인
        market_strength, min_score = self.check_market_strength()
        self.last_market_state = market_strength

        # 미국 시장 연동
        us_state, us_adjust = self.check_us_market()
        min_score = max(0, min(100, min_score + us_adjust))
        # v5.4: 최소 기준 캡 — 어떤 상황에서도 30 이하로 유지
        min_score = min(min_score, 30)

        print(f"📈 시장 상태: {market_strength} | 미국: {us_state} → 최소 신호: {min_score}점")

        # 극약세: 개별 강세 코인만 허용 (v3.3: 완전 중단 → 선별 진입)
        if market_strength == "극약세":
            print("⚠️ 극약세 시장 → 개별 강세 코인만 선별 진입")

        # v5.8: 시간대별 모멘텀 기준 강화 (DB 승률 데이터 기반)
        # 심야(23~06): -246k 누적, 저녁(18~23): -58k, 오전(8~13): 8~10% 승률
        _cur_hour = datetime.now().hour
        if _cur_hour >= 23 or _cur_hour < 6:
            # v5.15: 심야 차단은 place_buy_order에서 처리, 여기서도 코인 스캔 생략
            print(f"🌙 심야({_cur_hour:02d}시) → 코인 스캔/진입 생략 (포지션 관리만)")
            return
        elif _cur_hour >= 18:
            min_score = min(min_score + 5, 30)  # 저녁: +5점
            print(f"🌆 저녁({_cur_hour:02d}시) → 모멘텀 기준 강화: {min_score}점")
        elif 8 <= _cur_hour < 13:
            min_score = min(min_score + 7, 32)  # v5.8: 오전 +7점 (8시 8%승률, 11시 10%승률)
            print(f"🌅 오전({_cur_hour:02d}시) → 모멘텀 기준 강화: {min_score}점")
        elif 13 <= _cur_hour < 18:
            # v5.12: 최고 시간대(13~18시) 기준 완화 — 승률 53~60%, +944k
            min_score = max(min_score - 3, 10)  # -3점 완화
            print(f"☀️ 오후({_cur_hour:02d}시) → 모멘텀 기준 완화: {min_score}점")
        # v5.6: AutoTune 시간대 부스트 (새벽 등 전패 구간 자동 감지)
        _time_boost = getattr(self, '_autotune_time_boost', {})
        if _time_boost:
            if 6 <= _cur_hour < 9 and 'dawn' in _time_boost:
                min_score = min(min_score + _time_boost['dawn'], 35)
                print(f"🤖 새벽({_cur_hour:02d}시) AutoTune 부스트 → {min_score}점")
            elif _cur_hour >= 23 and 'late_night' in _time_boost:
                min_score = min(min_score + _time_boost['late_night'], 35)
            elif _cur_hour < 5 and 'midnight' in _time_boost:
                min_score = min(min_score + _time_boost['midnight'], 35)

        # 코인 선택
        selected = self.select_coins()
        print(f"🎯 선택된 코인: {selected}\n")
        
        # 각 코인 처리 (매매 한도 = 초기 50만 + 누적 수익)
        BASE_BUDGET = self.initial_balance  # v4.2: 실전 모드 호환 (하드코딩 제거)
        total_profit = sum(t.get('profit_rate', 0) / 100 * BASE_BUDGET for t in self.trades if t.get('profit_rate', 0) > 0)
        MAX_TRADING_BUDGET = int(BASE_BUDGET + max(0, total_profit))
        if self.mode == "real" and self.upbit:
            self.current_balance = self._get_krw_balance()
            print(f"💰 현재 KRW 잔고: {self.current_balance:,.0f}원")
        invested = sum(pos['amount'] for batches in self.positions.values() for pos in batches.values())
        # current_balance는 이미 매수금이 빠진 현금이므로, invested를 또 빼면 안됨
        # 대신 총 투자한도 - 투자중 = 추가 투자 가능액으로 계산
        available = min(self.current_balance, max(0, MAX_TRADING_BUDGET - invested))
        current_positions = len(self.positions)
        remaining_slots = max(0, self.MAX_POSITIONS - current_positions)
        per_coin_budget = int(MAX_TRADING_BUDGET // self.MAX_POSITIONS)  # 코인당 기본: 1/3(33%)
        print(f"📊 매매 한도: {MAX_TRADING_BUDGET:,}원 (기본 100만+수익) | 투자중: {invested:,.0f}원 | 가용: {available:,.0f}원")
        print(f"📊 포지션: {current_positions}/{self.MAX_POSITIONS} | 코인당: {per_coin_budget:,}원 (분할 {per_coin_budget//2:,}원 × 2회)")

        if remaining_slots <= 0:
            print("📌 최대 포지션(3개) 도달 → 신규 매수 없음, 포지션 모니터링만")

        for coin in selected:
            if remaining_slots <= 0:
                break

            # 신규 진입 쿨타임 10분 (과매매 방지)
            _last_entry = getattr(self, '_last_new_entry_time', 0)
            if time.time() - _last_entry < 600:
                _remaining_cool = int((600 - (time.time() - _last_entry)) / 60)
                print(f"⏳ 신규 진입 쿨타임 10분 미경과 (잔여 {_remaining_cool}분) → 패스")
                break

            # 이미 보유 중인 코인 스킵
            if coin in self.positions:
                continue

            # 매도 후 재매수 보호 (v3.2 강화: 15분/30분 쿨다운 + 2시간 내 85점 이상 요구)
            if coin in self._sell_cooldown:
                cooldown_data = self._sell_cooldown[coin]
                # v3.2: 쿨다운 데이터가 dict면 손절 여부 포함, 아니면 하위호환
                if isinstance(cooldown_data, dict):
                    sell_time = cooldown_data['time']
                    is_stoploss = cooldown_data.get('stoploss', False)
                    is_early_exit = cooldown_data.get('early_exit', False)
                    exit_price = cooldown_data.get('exit_price', 0)
                else:
                    sell_time = cooldown_data
                    is_stoploss = False
                    is_early_exit = False
                    exit_price = 0
                elapsed = time.time() - sell_time

                # v4.2: 조기 정리 코인 → 모멘텀 좋고 + 전 매도가보다 싸면 10분 후 재진입 허용
                if is_early_exit and elapsed >= 600:  # 최소 10분 대기
                    coin_momentum = self.calculate_momentum(coin)
                    current_price = self.get_price(coin)
                    if coin_momentum >= 50 and current_price and current_price < exit_price * 0.998:
                        print(f"🔄 {coin} 조기정리 재진입: 모멘텀 {coin_momentum}점, 가격 {current_price:,.0f} < 매도가 {exit_price:,.0f} (-{(1-current_price/exit_price)*100:.1f}%) → 허용")
                        del self._sell_cooldown[coin]
                    elif coin_momentum >= 50:
                        remaining = max(0, int((900 - elapsed) / 60))
                        print(f"⏳ {coin} 조기정리 후 모멘텀 {coin_momentum}점 OK, 가격 {current_price:,.0f} ≥ 매도가 {exit_price:,.0f} → 더 싸질 때까지 대기")
                        if elapsed >= 900:  # 15분 지나면 일반 쿨다운으로 전환
                            del self._sell_cooldown[coin]
                        else:
                            continue
                    else:
                        if elapsed >= 900:
                            del self._sell_cooldown[coin]
                        else:
                            remaining = int((900 - elapsed) / 60)
                            print(f"⏳ {coin} 조기정리 후 모멘텀 {coin_momentum}점 부족 (잔여 {remaining}분) → 패스")
                            continue

                elif coin in self._sell_cooldown:  # 일반 쿨다운
                    # v4.2: 다른 전략(idle/surge)에서 발생한 쿨다운은 메인 전략에서 절반만 적용
                    source = cooldown_data.get('source', 'main') if isinstance(cooldown_data, dict) else 'main'
                    if source in ('idle', 'surge'):
                        absolute_ban = 900 if is_stoploss else 300  # idle/surge 손절: 15분, 일반: 5분
                        ban_label = f"15분({source}손절)" if is_stoploss else f"5분({source})"
                    else:
                        # v5.9: 손실 크기 비례 쿨다운 (재진입 반복 손실 방지)
                        _loss_pct = abs(self._last_sell_pnl.get(coin, 0))
                        if is_stoploss:
                            if _loss_pct >= 1.0:
                                absolute_ban = 3600   # -1%+ 손실: 60분
                                ban_label = "60분(대손절)"
                            elif _loss_pct >= 0.5:
                                absolute_ban = 1800   # -0.5%+ 손실: 30분
                                ban_label = "30분(손절)"
                            else:
                                absolute_ban = 900    # 소손실: 15분
                                ban_label = "15분(소손절)"
                        else:
                            absolute_ban = 900        # 일반 매도: 15분
                            ban_label = "15분"
                    # v4.4: AutoTune 쿨다운 연장
                    for _atr in self._autotune_rules:
                        if _atr['rule_type'] == 'cooldown_extend':
                            absolute_ban += int(_atr['param_value'])
                            break
                    if elapsed < absolute_ban:
                        remaining = int((absolute_ban - elapsed) / 60)
                        print(f"⏳ {coin} 매도 후 쿨다운 중 (잔여 {remaining}분, {ban_label}) → 패스")
                        continue
                    elif elapsed < 7200:  # 2시간 이내
                        coin_momentum = self.calculate_momentum(coin)
                        current_price = self.get_price(coin)
                        sell_price = cooldown_data.get('exit_price', 0) if isinstance(cooldown_data, dict) else 0

                        # v4.3: 손절 후 재진입 — 모멘텀 + 추세전환 확인 필수
                        if is_stoploss and coin_momentum >= 50:
                            trend_ok = self._is_uptrend(coin)
                            price_dropped = sell_price > 0 and current_price and current_price < sell_price * 0.98  # 2%+ 하락 = 새 기회
                            if trend_ok or price_dropped:
                                reason = "추세전환" if trend_ok else f"충분한 하락({(1-current_price/sell_price)*100:.1f}%)"
                                print(f"🔄 {coin} 손절 후 재진입: 모멘텀 {coin_momentum}점, {reason} → 허용")
                                del self._sell_cooldown[coin]
                            else:
                                remaining_h = int((7200 - elapsed) / 60)
                                print(f"⏳ {coin} 손절 후 모멘텀 {coin_momentum}점 OK, 하지만 추세 미전환 → 대기 ({remaining_h}분)")
                                continue
                        elif coin_momentum < 85:
                            remaining_h = int((7200 - elapsed) / 60)
                            print(f"⏳ {coin} 최근 매도 → 모멘텀 {coin_momentum}점 < 85점 미달 (잔여 {remaining_h}분) → 패스")
                            continue
                        else:
                            print(f"🔥 {coin} 모멘텀 {coin_momentum}점 → 회복 확인, 재매수 허용")
                            del self._sell_cooldown[coin]
                    else:  # 2시간 경과 → 쿨다운 해제
                        del self._sell_cooldown[coin]

            # 투자유의/거래종료 예정 종목 절대 금지
            if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                print(f"🚫 {coin} 투자유의/거래종료 예정 → 절대 매수 금지")
                continue

            # 스테이블코인 필터 (이익 불가)
            _STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "USD1", "FDUSD"}
            if coin in _STABLECOINS:
                print(f"🚫 {coin} 스테이블코인 → 매수 금지")
                continue

            # 섹터 분산 체크: 동일 섹터 코인 집중 방지
            new_sector = self.COIN_SECTORS.get(coin, coin)  # 미분류 = 코인명 자체 (고유)
            held_sectors = [self.COIN_SECTORS.get(c, c) for c in self.positions.keys()]
            if new_sector in held_sectors:
                # 동일 섹터 보유 중 → 모멘텀이 확실히 높아야 허용 (+15점)
                existing_coin = [c for c in self.positions.keys() if self.COIN_SECTORS.get(c, c) == new_sector][0]
                existing_mom = self.calculate_momentum(existing_coin)
                new_mom_check = self.calculate_momentum(coin)
                if new_mom_check <= existing_mom + 15:
                    print(f"🔀 {coin}({new_sector}) 섹터 중복 → {existing_coin} 보유 중, 모멘텀 우위 부족 → 패스")
                    continue
                else:
                    print(f"🔀 {coin}({new_sector}) 섹터 중복이지만 모멘텀 {new_mom_check}점 >> {existing_coin} {existing_mom}점 → 허용")

            # 가용 금액 부족 + 중타 포지션 있음 → 비교 후 판단
            if available < 10000 and self._scalp_positions:
                idle_coin = list(self._scalp_positions.keys())[0]
                idle_mom = self.calculate_momentum(idle_coin)
                new_mom = self.calculate_momentum(coin)
                if new_mom > idle_mom + 15:
                    # v5.8: 명확한 우위(+15점)일 때만 전환 (기존 +5점은 노이즈 수준)
                    print(f"💸 중타 {idle_coin}({idle_mom}점) < 신규 {coin}({new_mom}점+15) → 중타 정리 후 전환")
                    self.scalp_clear_for_momentum()
                    available = self.current_balance
                else:
                    # 중타가 비슷하거나 더 좋음 → 유지
                    print(f"💎 중타 {idle_coin}({idle_mom}점) ≥ 신규 {coin}({new_mom}점) → 중타 유지")
                    continue
            if available < 10000:
                print("💸 가용 금액 부족 → 매수 중단")
                break

            # 극단 변동 체크
            volatility = self.check_extreme_volatility(coin)
            if volatility == "halt":
                print(f"🚨 {coin} 1분 5%+ 급변동 → 거래 정지")
                continue
            elif volatility == "low_liquidity":
                print(f"🚨 {coin} 거래량 급감 → 유동성 부족, 패스")
                continue

            momentum = self.calculate_momentum(coin)
            print(f"{coin} 모멘텀: {momentum}점", end="")

            # v3.3: 개별 강세 코인 감지 (24h +15% 이상 → 시장과 독립적으로 진입 허용)
            coin_is_strong = False
            try:
                _tk = f"KRW-{coin}"
                _oh = pyupbit.get_ohlcv(_tk, interval="minute60", count=24)
                if _oh is not None and len(_oh) >= 2:
                    _chg = (_oh['close'].iloc[-1] - _oh['close'].iloc[0]) / _oh['close'].iloc[0]
                    if _chg >= 0.15:
                        coin_is_strong = True
                        # 개별 강세 코인: 24h 총 거래대금으로 유동성 확인 (시간대별 편차 무시)
                        _vols = list(_oh['volume'])
                        _closes = list(_oh['close'])
                        _total_value = sum(v * c for v, c in zip(_vols, _closes))
                        if _total_value >= 2_000_000_000:  # 24h 거래대금 20억원 이상이면 유동성 충분 (5억→20억 상향)
                            # v5.0: 개별 강세 모멘텀 15점으로 완화 (15~39점은 반감 진입)
                            if momentum < 15:
                                print(f" ⚠️ 개별 강세(24h {_chg*100:+.1f}%)지만 모멘텀 {momentum}점 < 15점 → 과열 추격 방지, 패스")
                                coin_is_strong = False
                                continue
                            # 고점 대비 -3% 이상 되돌림이면 이미 덤프 시작 → 패스
                            _high_24h = max(list(_oh['high']))
                            _current = list(_oh['close'])[-1]
                            if (_current - _high_24h) / _high_24h < -0.03:
                                print(f" ⚠️ 개별 강세지만 고점 대비 {(_current - _high_24h) / _high_24h * 100:.1f}% 되돌림 → 패스")
                                continue
                            # v5.0: 모멘텀 15~39점 → 반감 진입 (리스크 관리)
                            if momentum < 40:
                                self._strong_half_budget = True
                                print(f" 🔥 개별 강세 (24h {_chg*100:+.1f}%, 모멘텀 {momentum}점 → 반감 진입, 거래대금 {_total_value/1e8:.1f}억)", end="")
                            else:
                                self._strong_half_budget = False
                                print(f" 🔥 개별 강세 (24h {_chg*100:+.1f}%, 모멘텀 {momentum}점, 거래대금 {_total_value/1e8:.1f}억) → 시장 무관 진입!", end="")
                        else:
                            print(f" ⚠️ 개별 강세지만 거래대금 부족({_total_value/1e8:.1f}억 < 20억) → 패스")
                            continue
            except:
                pass

            if not coin_is_strong:
                # 기존 진입 필터 — 정확성 중심
                # 고점이라도 거래량+모멘텀 충분하면 돌파 허용
                if momentum >= min_score and self._is_near_high(coin):
                    vol_score = self.analyze_volume(coin)
                    if momentum >= 50 and vol_score >= 70:
                        print(f" 🚀 고점 근접이지만 모멘텀{momentum}+거래량{vol_score} → 돌파 진입!", end="")
                    else:
                        print(f" ⚠️ 고점 근접 (모멘텀/거래량 부족) → 매수 보류")
                        continue

                # 진입 시그널 확인: 추세 / 반등 / 패턴 / PA — 엄선: 2개 이상 필요
                uptrend = self._is_uptrend(coin)
                bounce_signal = self._detect_bounce_signal(coin)
                pattern_score = self.detect_patterns(coin)
                has_pattern = pattern_score >= 60

                if momentum >= min_score and momentum < 85:
                    signals = []
                    if uptrend: signals.append("추세")
                    if bounce_signal: signals.append("반등")
                    if has_pattern: signals.append(f"패턴{pattern_score}")

                    # v4.0: PA 시그널 추가
                    try:
                        _sr = self._calc_sr_levels(coin)
                        if _sr and _sr['current_zone'] == 'near_support':
                            signals.append("지지대")
                        _tl = self._calc_trendlines(coin)
                        if _tl:
                            if _tl['trend_direction'] == 'up':
                                signals.append("상승추세선")
                            elif _tl['converging'] and _tl['convergence_ratio'] > 0.7:
                                signals.append("수렴끝단")
                    except:
                        pass

                    signal_count = len(signals)

                    # v5.14: 모멘텀 40+ 집중 (데이터: 40+ 승률58%+235k, 30-40 33%-70k)
                    min_signals = 1 if momentum >= 40 else 2
                    if signal_count < min_signals:
                        print(f" ⚠️ 시그널 {signal_count}개({', '.join(signals) if signals else '없음'}) → {min_signals}개 이상 필요, 매수 보류")
                        continue
                    print(f" ✅ 시그널 {signal_count}개({'+'.join(signals)})", end="")

                if momentum < min_score:
                    print(f" ❌ 패스")
                    continue

            # v4.0: 저항대 근접 필터 (돌파 확인 없으면 매수 보류)
            if (momentum >= min_score or coin_is_strong) and not coin_is_strong:
                try:
                    _sr_entry = self._calc_sr_levels(coin)
                    if _sr_entry and _sr_entry['current_zone'] == 'near_resistance':
                        _nr = _sr_entry['nearest_resistance']
                        _cp = self.get_price(coin)
                        _vol = self.analyze_volume(coin)
                        if _cp and _nr and _cp > _nr * 1.005 and _vol >= 60:
                            print(f" 🔓 저항대 돌파 확인 (>{_nr:.0f}, 거래량{_vol}점)", end="")
                        else:
                            print(f" 🚧 저항대 근접 ({_nr:.0f}원) → 돌파 미확인, 매수 보류")
                            continue
                except:
                    pass

            if momentum >= min_score or (coin_is_strong and momentum >= 30):
                # v5.6: 45+ 모멘텀 예산 축소 (데이터: 45+ 리스크/리워드 0.4:1 역전)
                # 80+ 집중투자 비활성화, 45+ 예산 절반
                is_concentrate = False
                if momentum >= 45:
                    coin_budget = min(per_coin_budget // 2, available)
                    print(f" ⚠️ 고모멘텀({momentum}점) 과열주의 → 예산 절반 {coin_budget:,}원", end="")
                else:
                    coin_budget = min(per_coin_budget, available)

                # v5.0: 개별 강세 반감 진입
                if getattr(self, '_strong_half_budget', False) and coin_is_strong:
                    coin_budget = coin_budget // 2
                    self._strong_half_budget = False
                # 집중투자: 1차 40% + 2차 30% + 3차 30%, 일반: 1차 50% + 2차 50%
                if is_concentrate:
                    buy_amount = int(coin_budget * 0.4)
                else:
                    buy_amount = int(coin_budget // 2)
                if buy_amount < 5000:
                    print(f" 💸 매수금액 부족 ({buy_amount:,}원) → 패스")
                    continue
                # v5.4: 저항선이 현재가 +2% 미만이면 진입 차단 (0원 즉시매도 근본 방지)
                try:
                    _sr_check = self._calc_sr_levels(coin)
                    _cur_price = self.get_price(coin)
                    if _sr_check and _cur_price and _sr_check.get('nearest_resistance'):
                        _upside = (_sr_check['nearest_resistance'] - _cur_price) / _cur_price
                        if _upside < 0.02:
                            print(f" ⚠️ 저항선 {_sr_check['nearest_resistance']:,.0f} 너무 가까움 (+{_upside*100:.2f}%) → 패스")
                            continue
                except:
                    pass
                print(f" ✅ 매수 (1차: {buy_amount:,}원)")
                # 1차 매수 (코인당 할당의 50%)
                self._current_buy_is_strong = coin_is_strong
                buy_success = self.place_buy_order(coin, buy_amount, momentum=momentum)
                self._current_buy_is_strong = False
                # v4.2: 매수 성공 시에만 available/slots 차감
                if buy_success:
                    available -= buy_amount
                    remaining_slots -= 1
                else:
                    print(f"  ❌ {coin} 1차 매수 실패 → 다음 코인으로")
                    continue

                # v5.4: 매수 후 즉시매도 제거 (0원 거래 방지) — 매수 전 필터로 충분히 걸러냄

                # 2차 매수는 5분 후 별도 처리 (저점 매입)
                if coin not in self._pending_2nd_buy:
                    # v5.6: 집중투자 제거, 모든 코인 동일하게 2분할
                    self._pending_2nd_buy[coin] = {
                        'amount': buy_amount,
                        'time': time.time(),
                        'created': time.time(),
                        'wait': 300,  # 5분 대기
                        'mom_1st': momentum
                    }
            else:
                print(" ❌ 패스")

        # === v3.2: 유휴 자금 → 보유 코인 추가 매수 (물타기) ===
        # 신규 매수 후에도 가용 자금이 남아있으면, 모멘텀 유지 중인 보유 코인에 추가 투자
        if available >= 10000 and self.positions:
            print(f"\n💡 유휴 자금 {available:,.0f}원 → 보유 코인 추가 매수 검토")
            # 보유 코인 중 모멘텀 순으로 정렬
            position_momentums = []
            for coin in self.positions:
                mom = self.calculate_momentum(coin)
                # 이미 투자한 총액 계산
                coin_invested = sum(pos['amount'] for pos in self.positions[coin].values())
                position_momentums.append((coin, mom, coin_invested))
            position_momentums.sort(key=lambda x: x[1], reverse=True)

            # 보수적 운영: 코인당 최대 batch_2까지만 (과분산 방지)
            add_buy_threshold = min_score + 10
            for coin, mom, coin_invested in position_momentums:
                if available < 10000:
                    break
                # v5.6: 모든 코인 2배치까지만 (집중투자 제거)
                num_batches = len(self.positions.get(coin, {}))
                if num_batches >= 2:
                    print(f"  {coin} 이미 {num_batches}배치 보유 → 추가 매수 불가 (최대 2배치)")
                    continue
                if mom < add_buy_threshold:
                    print(f"  {coin} 모멘텀 {mom}점 < {add_buy_threshold}점 → 추가 매수 불가")
                    continue
                if not self._is_uptrend(coin):
                    print(f"  {coin} 모멘텀 {mom}점 but 상승추세 아님 → 추가 매수 보류")
                    continue
                # v5.6: 코인당 최대 50% (집중투자 제거)
                max_per_coin = int(MAX_TRADING_BUDGET * 0.5)
                if coin_invested >= max_per_coin:
                    print(f"  {coin} 이미 {coin_invested:,}원 투자 (한도 {max_per_coin:,}원) → 추가 매수 불가")
                    continue
                add_amount = min(int(available * 0.5), max_per_coin - coin_invested)
                add_amount = min(add_amount, per_coin_budget)
                if add_amount < 10000:
                    continue
                print(f"  📈 {coin} 모멘텀 {mom}점 + 상승추세 → 추가 매수 {add_amount:,}원")
                if self.place_buy_order(coin, add_amount, momentum=mom):
                    available -= add_amount

        # === v4.1: 대안 없을 때 수익 포지션 집중 추가 투입 ===
        # 신규 진입 0건 + 유휴자금 남음 + 보유 코인이 수익 중일 때
        if available >= 10000 and self.positions and remaining_slots > 0:
            # 이번 사이클에서 신규 진입이 없었는지 확인 (가용금이 줄지 않았으면 진입 없음)
            initial_available = min(self.current_balance + sum(pos['amount'] for batches in self.positions.values() for pos in batches.values()), MAX_TRADING_BUDGET) - invested
            no_new_entry = (available >= initial_available * 0.9)  # 거의 안 줄었으면 신규진입 없음

            if no_new_entry:
                for coin in self.positions:
                    if available < 10000:
                        break
                    # v4.2: 최근 매도 후 10분 쿨타임 (같은 코인 즉시 재진입 방지)
                    if coin in self._sell_cooldown:
                        cd = self._sell_cooldown[coin]
                        elapsed = time.time() - cd['time']
                        if elapsed < 600:  # 10분
                            print(f"  🎯 [집중] {coin} 매도 후 {elapsed/60:.0f}분 경과 (10분 쿨타임) → 재진입 대기")
                            continue
                    price = self.get_price(coin)
                    if not price:
                        continue
                    # v5.12: 추가매수 조건 완화 (+0.3% → +0.1%, 상승추세 유지)
                    # batch_2가 핵심 수익원 (+306k): 빠르게 추가매수 진입
                    avg_buy = sum(p['buy_price'] * p['quantity'] for p in self.positions[coin].values()) / sum(p['quantity'] for p in self.positions[coin].values())
                    profit_rate = (price - avg_buy) / avg_buy
                    if profit_rate < 0.001:  # +0.1% (기존 +0.3%)
                        continue
                    if not self._is_uptrend(coin):
                        continue
                    mom = self.calculate_momentum(coin)
                    coin_invested = sum(p['amount'] for p in self.positions[coin].values())
                    max_per_coin = int(MAX_TRADING_BUDGET * 0.85)  # v5.12: 70→85% (집중 투자)
                    if coin_invested >= max_per_coin:
                        continue
                    add_amount = min(int(available * 0.5), max_per_coin - coin_invested)
                    if add_amount < 10000:
                        continue
                    print(f"  🎯 [집중] {coin} 대안 없음 + 수익중({profit_rate*100:+.2f}%) + 상승추세 → 추가 {add_amount:,}원")
                    if self.place_buy_order(coin, add_amount, momentum=mom):
                        available -= add_amount

    def _has_min_upside(self, coin, min_pct=0.010):
        """최소 상승 여력 확인 - 최근 1시간 상승률 + 저점 대비 여력"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute5", count=12)  # 최근 1시간
            if ohlcv is None or len(ohlcv) < 6:
                return False
            closes = list(ohlcv['close'])
            highs = list(ohlcv['high'])
            current = closes[-1]
            low_1h = min(closes)
            high_1h = max(highs)

            # 1) 최근 1시간 상승률이 1.5% 이상이어야 함 (모멘텀 있음)
            change_1h = (current - closes[0]) / closes[0]
            if change_1h < min_pct:
                return False

            # 2) 고점 대비 너무 가깝지 않아야 함 (추가 상승 여력)
            #    고점의 99% 이상이면 천장 근접 → 상승 여력 부족
            if high_1h > 0 and current >= high_1h * 0.99:
                return False

            return True
        except:
            return False

    def _is_uptrend(self, coin):
        """현재 상승 추세인지 판단 (v3.2: 조건 완화)"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute30", count=6)
            if ohlcv is None or len(ohlcv) < 4:
                return False
            closes = list(ohlcv['close'])
            # 최근 3봉 중 2봉 상승이면 상승 추세 (v3.2: 3연속→2/3으로 완화)
            up_count = sum(1 for i in range(-3, 0) if closes[i] > closes[i-1])
            if up_count >= 2:
                return True
            # MA5가 우상향이면 상승 추세
            if len(closes) >= 5:
                ma = np.mean(closes[-5:])
                if closes[-1] > ma and closes[-1] > closes[-3]:
                    return True
            return False
        except:
            return False

    def _detect_bounce_signal(self, coin):
        """v3.2: 반등 시그널 감지 — RSI 과매도 반등 / 지지선 터치 후 반등
        추세가 없어도 이 시그널이 있으면 진입 가능 (약세장 저점 매수)"""
        try:
            ticker = f"KRW-{coin}"

            # 1) RSI 과매도 반등: RSI가 30 이하였다가 현재 상승 전환
            ohlcv_1h = pyupbit.get_ohlcv(ticker, interval="minute60", count=14)
            if ohlcv_1h is not None and len(ohlcv_1h) >= 14:
                closes = list(ohlcv_1h['close'])
                delta = np.diff(closes)
                gain = np.where(delta > 0, delta, 0)
                loss = np.where(delta < 0, -delta, 0)
                # 직전 RSI (마지막 봉 제외)
                g_prev = gain[:-1].mean()
                l_prev = loss[:-1].mean()
                if l_prev > 0:
                    rsi_prev = 100 - (100 / (1 + g_prev / l_prev))
                else:
                    rsi_prev = 100
                # 현재 RSI
                g_now = gain.mean()
                l_now = loss.mean()
                if l_now > 0:
                    rsi_now = 100 - (100 / (1 + g_now / l_now))
                else:
                    rsi_now = 100
                # RSI가 35 이하에서 상승 전환 → 반등 시그널
                if rsi_prev <= 35 and rsi_now > rsi_prev:
                    return True

            # 2) 지지선 터치 후 반등: 24시간 저점 대비 2% 이내 + 최근 5분봉 상승
            ohlcv_5m = pyupbit.get_ohlcv(ticker, interval="minute5", count=6)
            if ohlcv_5m is not None and len(ohlcv_5m) >= 3:
                ohlcv_24h = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
                if ohlcv_24h is not None and len(ohlcv_24h) >= 12:
                    low_24h = min(list(ohlcv_24h['low']))
                    closes_5m = list(ohlcv_5m['close'])
                    current = closes_5m[-1]
                    # 저점 대비 3% 이내 + 최근 2봉 연속 상승 → 지지선 반등
                    near_support = (current - low_24h) / low_24h < 0.03
                    recent_up = closes_5m[-1] > closes_5m[-2] > closes_5m[-3]
                    if near_support and recent_up:
                        return True

            return False
        except:
            return False

    def _is_near_high(self, coin):
        """최근 고점 근처인지 판단 (고점 진입 방지)"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
            if ohlcv is None or len(ohlcv) < 10:
                return False
            highs = list(ohlcv['high'])
            current = self.get_price(coin)
            if not current:
                return False
            high_24h = max(highs)
            # 24시간 고점의 98% 이상이면 고점 진입 위험
            if current >= high_24h * 0.98:
                return True
            return False
        except:
            return False

    def _hours_held(self, position):
        """포지션 보유 시간(시간 단위)"""
        try:
            buy_time = datetime.fromisoformat(position['timestamp'])
            return (datetime.now() - buy_time).total_seconds() / 3600
        except:
            return 0

    def _find_better_coin(self, current_coin, current_momentum):
        """현재 보유 코인보다 더 좋은 모멘텀 코인 찾기"""
        try:
            # 상위 5개만 빠르게 체크 (전체 스캔은 너무 오래 걸림)
            candidates = []
            for coin in self.coins[:30]:  # 상위 30개 코인 빠른 스캔
                if coin == current_coin:
                    continue
                if coin in self.positions:
                    continue  # 이미 보유 중인 코인 제외
                if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                    continue  # 투자유의/거래종료 예정 제외
                try:
                    mom = self.calculate_momentum(coin)
                    if mom > current_momentum + 20 and not self._is_near_high(coin):
                        candidates.append((coin, mom))
                    time.sleep(0.1)
                except:
                    continue
            if candidates:
                candidates.sort(key=lambda x: x[1], reverse=True)
                return candidates[0]  # (coin, momentum)
            return None
        except:
            return None

    # ============================================
    # 단타 레이더 (v3.2: 유휴 자금 활용)
    # ============================================

    def idle_fund_trade(self):
        """유휴자금 중타 — 모멘텀 슬롯 풀 + 자금 남을 때만, 미니 모멘텀 기반 진중한 진입"""
        try:
            now = time.time()

            # ── 중타 포지션 모니터링 ──
            for coin in list(self._scalp_positions.keys()):
                pos = self._scalp_positions[coin]
                price = self.get_price(coin)
                if not price:
                    continue
                profit_rate = (price - pos['buy_price']) / pos['buy_price']
                elapsed_min = (now - pos['timestamp']) / 60

                # 시장별 중타 익절/손절 기준
                market_state = getattr(self, 'last_market_state', '보통')
                if market_state in ('극약세', '심약세'):
                    idle_tp = 0.008   # 극/심약세: +0.8% 빠르게 먹고 빠짐
                    idle_sl = -0.005  # -0.5% 즉시 손절
                    idle_timeout = 20  # 20분 타임아웃
                else:
                    idle_tp = 0.01    # 보통/강세: +1% 익절
                    idle_sl = -0.007  # -0.7% 손절
                    idle_timeout = 30  # 30분 타임아웃

                # 익절
                if profit_rate >= idle_tp:
                    profit = round(pos['amount'] * profit_rate)
                    print(f"💎 [중타 익절] {coin} +{profit_rate*100:.2f}% (+{profit:,}원) | {elapsed_min:.0f}분")
                    if self.mode == "real" and self.upbit:
                        sell_qty = pos['quantity']
                        real_qty = float(self.upbit.get_balance(coin) or 0)
                        if real_qty > 0:
                            sell_qty = min(sell_qty, real_qty)
                        result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                        if result is None or (isinstance(result, dict) and 'error' in result):
                            print(f"❌ 중타 {coin} 익절 매도 실패, 포지션 유지")
                            continue
                        time.sleep(2)
                        self.current_balance = self._get_krw_balance()
                    else:
                        self.current_balance += pos['amount'] + profit
                    self.trades.append({'coin': coin, 'batch': 'idle_trade', 'buy_price': pos['buy_price'],
                                        'sell_price': price, 'profit_rate': profit_rate * 100,
                                        'timestamp': datetime.now().isoformat()})
                    self._db_log_trade(coin, "sell", price, pos['quantity'], pos['amount'] + profit,
                                       profit=profit, profit_rate=profit_rate * 100, batch='idle_trade')
                    self._notify(f"[IDLE ✅] {coin} +{profit_rate*100:.2f}% (+{profit:,}원) | {elapsed_min:.0f}분")
                    del self._scalp_positions[coin]
                    self._save_positions()
                    continue

                # 손절
                if profit_rate <= idle_sl:
                    loss = round(pos['amount'] * profit_rate)
                    print(f"💎 [중타 손절] {coin} {profit_rate*100:.2f}% ({loss:,}원) | {elapsed_min:.0f}분")
                    if self.mode == "real" and self.upbit:
                        sell_qty = pos['quantity']
                        real_qty = float(self.upbit.get_balance(coin) or 0)
                        if real_qty > 0:
                            sell_qty = min(sell_qty, real_qty)
                        result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                        if result is None or (isinstance(result, dict) and 'error' in result):
                            print(f"❌ 중타 {coin} 손절 매도 실패, 포지션 유지")
                            continue
                        time.sleep(2)
                        self.current_balance = self._get_krw_balance()
                    else:
                        self.current_balance += pos['amount'] + loss
                    self.trades.append({'coin': coin, 'batch': 'idle_trade', 'buy_price': pos['buy_price'],
                                        'sell_price': price, 'profit_rate': profit_rate * 100,
                                        'timestamp': datetime.now().isoformat()})
                    self._db_log_trade(coin, "sell", price, pos['quantity'], pos['amount'] + loss,
                                       profit=loss, profit_rate=profit_rate * 100, batch='idle_trade')
                    self._notify(f"[IDLE ❌] {coin} {profit_rate*100:.2f}% ({loss:,}원) | {elapsed_min:.0f}분")
                    del self._scalp_positions[coin]
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'source': 'idle', 'exit_price': price}
                    self._save_positions()
                    continue

                # 타임아웃
                if elapsed_min > idle_timeout:
                    profit = round(pos['amount'] * profit_rate)
                    print(f"💎 [중타 타임아웃] {coin} {profit_rate*100:+.2f}% ({profit:+,}원) | {elapsed_min:.0f}분")
                    if self.mode == "real" and self.upbit:
                        sell_qty = pos['quantity']
                        real_qty = float(self.upbit.get_balance(coin) or 0)
                        if real_qty > 0:
                            sell_qty = min(sell_qty, real_qty)
                        result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                        if result is None or (isinstance(result, dict) and 'error' in result):
                            print(f"❌ 중타 {coin} 타임아웃 매도 실패, 포지션 유지")
                            continue
                        time.sleep(2)
                        self.current_balance = self._get_krw_balance()
                    else:
                        self.current_balance += pos['amount'] + profit
                    self.trades.append({'coin': coin, 'batch': 'idle_trade', 'buy_price': pos['buy_price'],
                                        'sell_price': price, 'profit_rate': profit_rate * 100,
                                        'timestamp': datetime.now().isoformat()})
                    self._db_log_trade(coin, "sell", price, pos['quantity'], pos['amount'] + profit,
                                       profit=profit, profit_rate=profit_rate * 100, batch='idle_trade')
                    del self._scalp_positions[coin]
                    self._save_positions()
                    continue

                print(f"  💎 [중타] {coin}: {profit_rate*100:+.2f}% | {elapsed_min:.0f}분 보유")

            # ── 진입 조건: 모멘텀 슬롯 풀 + 자금 유휴 + 쿨타임 30분 ──
            if len(self._scalp_positions) >= self._scalp_max:
                return

            # 모멘텀 슬롯이 비어있으면 중타 안 함 (모멘텀 우선)
            if len(self.positions) < self.MAX_POSITIONS:
                return

            # v4.1: 10분 쿨타임 (30분→10분, 거래빈도 향상)
            last_idle_entry = getattr(self, '_last_idle_entry', 0)
            if now - last_idle_entry < 600:
                return

            # 유휴 자금 체크
            available = self.current_balance
            idle_budget = min(1000000, int(available * 0.3))
            if idle_budget < 10000:
                return

            # 이미 보유 중인 코인 + 블랙리스트 제외
            owned = set(self.positions.keys()) | set(self._scalp_positions.keys()) | set(self._sell_cooldown.keys()) | self._permanent_blacklist | self._autotune_blacklist

            # 시장 상황별 중타 진입 기준
            market_state = getattr(self, 'last_market_state', '보통')
            if market_state in ('극약세', '심약세'):
                idle_min_mom = 60  # 극/심약세: 확실한 것만
            elif market_state == '약세':
                idle_min_mom = 50
            else:
                idle_min_mom = 45

            # 미니 모멘텀 체크: 상위 10개 코인만 빠르게 점수 확인
            candidates = []
            scan_coins = [c for c in self.coins[:30] if c not in owned][:10]
            for coin in scan_coins:
                try:
                    mom = self.calculate_momentum(coin)
                    if mom >= idle_min_mom and self._is_uptrend(coin):
                        candidates.append((coin, mom))
                except:
                    continue

            if not candidates:
                print(f"  💎 [중타 스캔] 적합 코인 없음 (모멘텀 {idle_min_mom}+ & 상승추세 필요, 시장: {market_state})")
                return

            # 최고 모멘텀 코인 선택
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_coin, best_mom = candidates[0]

            # 거래량 확인
            try:
                ohlcv = pyupbit.get_ohlcv(f"KRW-{best_coin}", interval="minute5", count=6)
                if ohlcv is not None and len(ohlcv) >= 3:
                    vols = list(ohlcv['volume'])
                    vol_ratio = vols[-1] / np.mean(vols[:-1]) if np.mean(vols[:-1]) > 0 else 0
                    if vol_ratio < 1.2:
                        print(f"  💎 [중타 패스] {best_coin} 모멘텀 {best_mom}점 but 거래량 x{vol_ratio:.1f}")
                        return
            except:
                pass

            # 진입
            price = self.get_price(best_coin)
            if price:
                quantity = idle_budget / price
                print(f"💎 [중타 진입] {best_coin} 모멘텀 {best_mom}점 + 상승추세 + 거래량 OK → {idle_budget:,}원")
                if self.mode == "real" and self.upbit:
                    # v5.1: 실제 잔고 확인 후 매수
                    real_krw = self._get_krw_balance()
                    if real_krw < idle_budget:
                        print(f"⚠️ 중타 {best_coin} 잔고 부족 (필요 {idle_budget:,}, 보유 {real_krw:,.0f}원)")
                        return
                    result = self.upbit.buy_market_order(f"KRW-{best_coin}", idle_budget)
                    if result is None or (isinstance(result, dict) and 'error' in result):
                        return
                    time.sleep(2)
                    self.current_balance = self._get_krw_balance()
                    # v4.2: 실제 체결 수량 조회 (기존 보유분 차감)
                    real_qty = float(self.upbit.get_balance(best_coin) or 0)
                    prev_qty = sum(p['quantity'] for p in self.positions.get(best_coin, {}).values())
                    actual_qty = real_qty - prev_qty
                    if actual_qty > 0:
                        quantity = actual_qty
                    else:
                        quantity = idle_budget / price * 0.9995
                else:
                    self.current_balance -= idle_budget
                self._scalp_positions[best_coin] = {
                    'buy_price': price,
                    'amount': idle_budget,
                    'quantity': quantity,
                    'timestamp': now
                }
                self._last_idle_entry = now
                self._db_log_trade(best_coin, "buy", price, quantity, idle_budget, batch='idle_trade')
                self._notify(f"[IDLE] {best_coin} 모멘텀 {best_mom}점 | {idle_budget:,}원 진입")

        except Exception as e:
            print(f"⚠️ 유휴자금 중타 오류: {e}")

    def _detect_surge_coins(self):
        """v5.0: 전체 코인 급등 탐지 — ticker API 1차 필터 + 점수제 + 초기 급등 감지"""
        try:
            owned = set(self.positions.keys()) | set(self._scalp_positions.keys()) | set(self._surge_positions.keys()) | set(self._surge_watchlist.keys())
            cooldown_coins = set(self._sell_cooldown.keys())
            # v5.1: 같은 코인 2회 초과 진입 차단
            maxed_coins = {c for c, n in self._surge_coin_count.items() if n >= 2}
            # v5.10: 영구+AutoTune 블랙리스트 적용 (SHIB 등 surge 경로 우회 방지)
            _blacklist = self._permanent_blacklist | self._autotune_blacklist
            candidates = []

            # ── 1단계: ticker API로 전체 코인 1차 필터 (API 1회) ──
            import requests as _req
            try:
                _markets = ",".join(f"KRW-{c}" for c in self.coins)
                _resp = _req.get("https://api.upbit.com/v1/ticker",
                    params={"markets": _markets}, timeout=10)
                _tickers = _resp.json() if _resp.status_code == 200 else []
            except Exception:
                _tickers = []

            # 1차 필터: 상승 중(+3%) + 거래대금 50억 이상 (v5.7: 강화)
            pre_candidates = []
            for t in _tickers:
                coin = t['market'].replace('KRW-', '')
                if coin in owned or coin in cooldown_coins or coin in maxed_coins:
                    continue
                if coin in _blacklist:
                    continue
                if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                    continue
                chg = t.get('signed_change_rate', 0)
                trade_price = t.get('acc_trade_price_24h', 0)
                if chg >= 0.03 and trade_price >= 5_000_000_000:
                    pre_candidates.append((coin, chg, trade_price))

            # 상승률 순 상위 25개만 상세 분석
            pre_candidates.sort(key=lambda x: x[1], reverse=True)
            scan_list = pre_candidates[:25]

            if not scan_list:
                return []

            print(f"🚀 [SURGE] 1차 필터 {len(pre_candidates)}개 → 상위 {len(scan_list)}개 상세 분석")

            for coin, ticker_chg, ticker_trade_price in scan_list:
                ticker = f"KRW-{coin}"
                try:
                    # ── 2단계: 5분봉 상세 분석 (점수제) ──
                    ohlcv_5m = pyupbit.get_ohlcv(ticker, interval="minute5", count=6)
                    if ohlcv_5m is None or len(ohlcv_5m) < 4:
                        continue

                    closes_5m = list(ohlcv_5m['close'])
                    opens_5m = list(ohlcv_5m['open'])
                    highs_5m = list(ohlcv_5m['high'])
                    volumes_5m = list(ohlcv_5m['volume'])
                    current_price = closes_5m[-1]

                    # 10분 가격 변동
                    if len(closes_5m) >= 3:
                        price_change_10m = (closes_5m[-1] - closes_5m[-3]) / closes_5m[-3]
                    else:
                        price_change_10m = (closes_5m[-1] - closes_5m[-2]) / closes_5m[-2]

                    # 거래량 비율
                    avg_vol = np.mean(volumes_5m[:-2]) if len(volumes_5m) > 2 else np.mean(volumes_5m)
                    recent_vol = np.mean(volumes_5m[-2:])
                    vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 0

                    # 필수 조건: 10분 +2.5% 이상 (v5.7: 1.5→2.5% 강화)
                    if price_change_10m < 0.025:
                        # 10분 급등이 아니면 → 초기 급등(1시간봉) 체크로 넘김
                        pass
                    else:
                        # ── 점수 산정 (10분 급등형) ──
                        score = 0

                        # 5분봉 2연속 양봉: +2점
                        if closes_5m[-1] > opens_5m[-1] and closes_5m[-2] > opens_5m[-2]:
                            score += 2

                        # 거래량 배율
                        if vol_ratio >= 3:
                            score += 5
                        elif vol_ratio >= 2:
                            score += 3
                        elif vol_ratio >= 1.5:
                            score += 1

                        # 10분 상승률
                        if price_change_10m >= 0.05:
                            score += 4
                        elif price_change_10m >= 0.03:
                            score += 3
                        elif price_change_10m >= 0.02:
                            score += 2

                        # 눌림목 가산 (필수 아님)
                        prev_high = highs_5m[-2]
                        pullback = (current_price - prev_high) / prev_high
                        if -0.02 <= pullback <= -0.005:
                            score += 3

                        # ── v5.10: 과매수/감속/고점 필터 (DB 분석 기반) ──

                        # 1) RSI 과매수 필터: surge는 RSI 미참조였음 → 통합
                        try:
                            _rsi_ohlcv = pyupbit.get_ohlcv(ticker, interval="minute5", count=14)
                            if _rsi_ohlcv is not None and len(_rsi_ohlcv) >= 7:
                                _rsi_closes = list(_rsi_ohlcv['close'])
                                _rsi_delta = np.diff(_rsi_closes)
                                _rsi_gain = np.where(_rsi_delta > 0, _rsi_delta, 0).mean()
                                _rsi_loss = np.where(_rsi_delta < 0, -_rsi_delta, 0).mean()
                                _rsi_val = 100 - (100 / (1 + _rsi_gain / _rsi_loss)) if _rsi_loss > 0 else 99
                                # v5.11: RSI 임계값 80→70 (스파이크 초입 RSI 70대 통과 방지)
                                if _rsi_val >= 70:
                                    print(f"  🚫 {coin} RSI {_rsi_val:.0f} ≥ 70 (과매수) → 차단")
                                    continue
                                elif _rsi_val >= 60:
                                    score -= 3
                                elif _rsi_val < 40:
                                    score += 2
                        except:
                            pass

                        # 2) 급등 감속 필터: 이전 봉 대비 현재 봉 상승률 절반 미만 → 꺾이는 중
                        if len(closes_5m) >= 3 and opens_5m[-2] > 0 and opens_5m[-1] > 0:
                            _prev_chg = (closes_5m[-2] - opens_5m[-2]) / opens_5m[-2]
                            _curr_chg = (closes_5m[-1] - opens_5m[-1]) / opens_5m[-1]
                            if _prev_chg > 0.005 and _curr_chg < _prev_chg * 0.5:
                                print(f"  🚫 {coin} 급등 감속 (이전봉 +{_prev_chg*100:.1f}% → 현재봉 +{_curr_chg*100:.1f}%) → 차단")
                                continue

                        # 3) 24h 고점 대비 위치: 고점의 97%+ → 천장 근접 차단
                        try:
                            _24h = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
                            if _24h is not None and len(_24h) >= 4:
                                _24h_high = _24h['high'].max()
                                if _24h_high > 0 and current_price >= _24h_high * 0.97:
                                    print(f"  🚫 {coin} 24h 고점 근접 ({current_price:,.0f} ≥ {_24h_high:,.0f}×97%) → 차단")
                                    continue
                        except:
                            pass

                        # 4) 윗꼬리(매도압력) 필터: 마지막 5분봉 윗꼬리 비율 50%+
                        _body = abs(closes_5m[-1] - opens_5m[-1])
                        _upper_shadow = highs_5m[-1] - max(closes_5m[-1], opens_5m[-1])
                        _total_range = highs_5m[-1] - min(closes_5m[-1], opens_5m[-1])
                        if _total_range > 0 and _upper_shadow / _total_range >= 0.5:
                            score -= 3
                            print(f"  ⚠️ {coin} 윗꼬리 {_upper_shadow/_total_range*100:.0f}% (매도압력) → -3점")

                        # 점수 문턱: 7점 이상 (v5.7: 5→7 강화)
                        if score < 7:
                            continue

                        # ── 펌프앤덤프 필터 (기존 유지) ──
                        volatility = self.check_extreme_volatility(coin)
                        if volatility in ("halt", "emergency_sell"):
                            continue

                        # 1분봉 음봉 2/3 → 이미 꺾임
                        try:
                            ohlcv_1m = pyupbit.get_ohlcv(ticker, interval="minute1", count=3)
                            if ohlcv_1m is not None and len(ohlcv_1m) >= 3:
                                bearish_count = sum(1 for i in range(len(ohlcv_1m))
                                                    if ohlcv_1m['close'].iloc[i] < ohlcv_1m['open'].iloc[i])
                                if bearish_count >= 2:
                                    continue
                        except:
                            pass

                        # 고점 대비 -2% 이상 하락 → 추격 방지
                        recent_high = max(highs_5m[-3:]) if len(highs_5m) >= 3 else max(highs_5m)
                        if recent_high > 0 and (current_price - recent_high) / recent_high < -0.02:
                            continue

                        # 5) v5.10: 종합 모멘텀 게이트 (기존 모멘텀 미참조 → 통합)
                        _surge_momentum = self.calculate_momentum(coin)
                        if _surge_momentum < 30:
                            print(f"  🚫 {coin} 종합 모멘텀 {_surge_momentum}점 < 30점 → 차단")
                            continue

                        candidates.append({
                            'coin': coin,
                            'price': current_price,
                            'change_10m': price_change_10m,
                            'vol_ratio': vol_ratio,
                            'pullback': pullback,
                            'score': score,
                            'surge_type': 'rapid',
                            'momentum': _surge_momentum,
                        })
                        print(f"🚀 [SURGE 감지] {coin}: 10분 +{price_change_10m*100:.1f}%, 거래량 {vol_ratio:.1f}배, 점수 {score}점, RSI/모멘텀 OK")
                        continue  # 10분 급등으로 이미 추가됨, 초기 급등 체크 불필요

                    # ── 3단계: 초기 급등 감지 (1시간봉 기준, 서서히 오르는 코인) ──
                    try:
                        ohlcv_1h = pyupbit.get_ohlcv(ticker, interval="minute60", count=4)
                        if ohlcv_1h is not None and len(ohlcv_1h) >= 2:
                            change_1h = (ohlcv_1h['close'].iloc[-1] - ohlcv_1h['close'].iloc[-2]) / ohlcv_1h['close'].iloc[-2]
                            vol_1h = ohlcv_1h['volume'].iloc[-1]
                            avg_vol_1h = ohlcv_1h['volume'].iloc[:-1].mean() if len(ohlcv_1h) > 1 else vol_1h

                            # 1시간 +5% 이상 + 거래량 2배 + 아직 고점 근처 (v5.7: 강화)
                            if change_1h >= 0.05 and (avg_vol_1h <= 0 or vol_1h >= avg_vol_1h * 2.0):
                                # 아직 고점 근처인지 (급등 초기 확인)
                                if ohlcv_1h['close'].iloc[-1] >= ohlcv_1h['high'].iloc[-1] * 0.98:
                                    # 펌프앤덤프 필터
                                    volatility = self.check_extreme_volatility(coin)
                                    if volatility in ("halt", "emergency_sell"):
                                        continue

                                    vol_1h_ratio = vol_1h / avg_vol_1h if avg_vol_1h > 0 else 1
                                    # v5.10: 초기급등도 모멘텀 게이트 적용
                                    _early_mom = self.calculate_momentum(coin)
                                    if _early_mom < 30:
                                        print(f"  🚫 {coin} 초기급등 모멘텀 {_early_mom}점 < 30점 → 차단")
                                        continue
                                    candidates.append({
                                        'coin': coin,
                                        'price': current_price,
                                        'change_10m': change_1h,
                                        'vol_ratio': vol_1h_ratio,
                                        'pullback': 0,
                                        'score': 6,
                                        'surge_type': 'early',
                                        'momentum': _early_mom,
                                    })
                                    print(f"🚀 [SURGE 초기급등] {coin}: 1h +{change_1h*100:.1f}%, 거래량 {vol_1h_ratio:.1f}배, 모멘텀 {_early_mom}점")
                    except:
                        pass

                except Exception:
                    continue

                time.sleep(0.1)  # API 호출 간격

            # 점수 높은 순으로 정렬, 최대 2개 반환
            if candidates:
                candidates.sort(key=lambda x: x['score'], reverse=True)
                return candidates[:2]
            return []

        except Exception as e:
            print(f"⚠️ [SURGE] 감지 오류: {e}")
            return []

    def surge_trade(self):
        """v5.3: 급등 감지 추격 매매 — 눌림목 진입 + 미니 트레일링 + 5분 퀵엑싯"""
        try:
            now = time.time()

            # v5.12: surge 관찰 모드 (매수 안 하고 감지+로그만)
            # batch가 일 +50~300k 수익인데 surge가 -538k 갉아먹는 구조 → 비활성화
            _surge_observe_only = True

            # 기존 포지션 모니터링은 유지 (이미 보유 중이면 관리)

            # ── v5.3: 눌림목 워치리스트 모니터링 ──
            for coin in list(self._surge_watchlist.keys()):
                watch = self._surge_watchlist[coin]
                price = self.get_price(coin)
                if not price:
                    continue
                # 고점 갱신
                if price > watch['peak_price']:
                    watch['peak_price'] = price
                elapsed = now - watch['timestamp']
                drop_from_peak = (price - watch['peak_price']) / watch['peak_price']

                # 눌림목 진입 조건: 고점 대비 -1% ~ -2.5% 하락
                if -0.025 <= drop_from_peak <= -0.01:
                    print(f"🚀 [SURGE 눌림목] {coin} 진입! 고점 {watch['peak_price']:,.0f} → 현재 {price:,.0f} ({drop_from_peak*100:+.1f}%)")
                    watch['entry_ready'] = True
                    watch['entry_price'] = price
                # -2.5% 이상 급락 → 너무 많이 빠짐, 포기
                elif drop_from_peak < -0.025:
                    print(f"🚀 [SURGE 워치] {coin} 급락 {drop_from_peak*100:+.1f}% → 포기")
                    del self._surge_watchlist[coin]
                    continue
                # v5.6: 3분 타임아웃 → 눌림 안 오면 포기 (추격매수 제거)
                elif elapsed >= 180:
                    print(f"🚀 [SURGE 워치] {coin} 3분 대기 → 눌림 미발생, 포기 (추격매수 금지)")
                    del self._surge_watchlist[coin]
                    continue
                else:
                    print(f"  🚀 [SURGE 워치] {coin}: 고점 대비 {drop_from_peak*100:+.1f}% | 대기 {elapsed:.0f}초")
                    continue

                # ── 눌림목 진입 실행 ──
                if watch.get('entry_ready'):
                    # v5.11: 진입 지연 확인 — 감지 후 최소 2분 경과 필수
                    # CFG 1분만에 -3%, ENSO 3분만에 -3% → 2분 대기로 페이크아웃 90% 걸러짐
                    if elapsed < 120:
                        print(f"  🚀 [SURGE 대기] {coin} 눌림목 도달, 진입 지연 대기 ({elapsed:.0f}초/120초)")
                        watch['entry_ready'] = False  # 다음 사이클에서 재확인
                        continue

                    # v5.11: 진입 직전 가격 재확인 — 감지가 대비 -3% 이상 하락이면 포기
                    _detect_price = watch.get('detected_price', 0)
                    if _detect_price > 0 and price < _detect_price * 0.97:
                        print(f"  🚫 {coin} 감지가 대비 {(price/_detect_price-1)*100:+.1f}% 급락 → 포기")
                        del self._surge_watchlist[coin]
                        continue

                    # v5.4: 모멘텀 음수면 진입 차단 (급등 자체가 모멘텀이므로 0점은 허용)
                    try:
                        _m_score, _m_detail = self.calculate_momentum(coin)
                    except:
                        _m_score = 0
                    if _m_score < 0:
                        print(f"🚀 [SURGE 차단] {coin} 모멘텀 {_m_score}점(음수) → 진입 포기")
                        del self._surge_watchlist[coin]
                        continue

                    # v5.6: 눌림목 진입 시 1분봉 양봉 확인 (반등 중인지 체크)
                    try:
                        _entry_1m = pyupbit.get_ohlcv(f"KRW-{coin}", interval="minute1", count=2)
                        if _entry_1m is not None and len(_entry_1m) >= 2:
                            _last_bullish = _entry_1m['close'].iloc[-1] > _entry_1m['open'].iloc[-1]
                            if not _last_bullish:
                                print(f"🚀 [SURGE 대기] {coin} 눌림목 도달했지만 1분봉 음봉 → 반등 대기")
                                watch['entry_ready'] = False  # 다음 사이클에서 재확인
                                continue
                    except:
                        pass

                    entry_price = watch['entry_price']
                    available = self.current_balance
                    surge_budget = min(1000000, int(available * 0.10))  # v5.11: 1.5M/15% → 1M/10%
                    # v5.4: 야간 예산 절반
                    _hour = datetime.now().hour
                    if _hour >= 23 or _hour < 6:
                        surge_budget = surge_budget // 2
                    if surge_budget < 10000:
                        del self._surge_watchlist[coin]
                        continue
                    if len(self._surge_positions) >= self._surge_max:
                        del self._surge_watchlist[coin]
                        continue

                    if self.mode == "real" and self.upbit:
                        result = self.upbit.buy_market_order(f"KRW-{coin}", surge_budget)
                        if result is None or (isinstance(result, dict) and 'error' in result):
                            print(f"❌ 서지 {coin} 매수 실패")
                            del self._surge_watchlist[coin]
                            continue
                        time.sleep(2)
                        self.current_balance = self._get_krw_balance()
                        real_qty = float(self.upbit.get_balance(coin) or 0)
                        prev_qty = 0
                        if coin in self.positions:
                            prev_qty += sum(p['quantity'] for p in self.positions[coin].values())
                        if coin in self._scalp_positions:
                            prev_qty += self._scalp_positions[coin].get('quantity', 0)
                        actual_surge_qty = real_qty - prev_qty
                        quantity = actual_surge_qty if actual_surge_qty > 0 else surge_budget / entry_price * 0.9995
                    else:
                        self.current_balance -= surge_budget
                        quantity = surge_budget / entry_price

                    self._surge_positions[coin] = {
                        'buy_price': entry_price,
                        'amount': surge_budget,
                        'quantity': quantity,
                        'timestamp': now,
                        'peak_rate': 0,
                        'surge_type': watch.get('surge_type', 'rapid'),
                        'momentum': _m_score,  # v5.5: 학습용 모멘텀 기록
                    }
                    self._last_surge_entry = now
                    self._surge_coin_count[coin] = self._surge_coin_count.get(coin, 0) + 1
                    discount = (watch['detected_price'] - entry_price) / watch['detected_price'] * 100
                    self._db_log_trade(coin, "buy", entry_price, quantity, surge_budget, momentum=_m_score, batch='surge_trade')
                    self._notify(f"[SURGE BUY 눌림목] {coin} | {surge_budget:,}원 @ {entry_price:,.0f} | 할인 {discount:.1f}% ({self._surge_coin_count[coin]}/2회)")
                    print(f"🚀 [SURGE 눌림목 매수] {coin} | {surge_budget:,}원 @ {entry_price:,.0f} | 감지가 대비 -{discount:.1f}%")
                    del self._surge_watchlist[coin]
                    self._save_positions()

            # ── 서지 포지션 모니터링 ──
            for coin in list(self._surge_positions.keys()):
                pos = self._surge_positions[coin]
                price = self.get_price(coin)
                if not price:
                    continue
                profit_rate = (price - pos['buy_price']) / pos['buy_price']
                elapsed_min = (now - pos['timestamp']) / 60

                # 고점 수익률 추적
                peak_rate = pos.get('peak_rate', 0)
                if profit_rate > peak_rate:
                    pos['peak_rate'] = profit_rate
                    peak_rate = profit_rate

                sell_reason = None

                # v5.5: 타임아웃 확대 (데이터: 15분+ 보유 승률 44%, 5~15분 12%)
                is_early = pos.get('surge_type') == 'early'
                timeout_loss = 30 if is_early else 20     # 손실 타임아웃: 10→20분
                timeout_flat = 45 if is_early else 30     # 횡보 타임아웃: 15→30분

                # 1. v5.11: -1.5% 절대 손절 (v5.4 -2% → 강화, 평균 손실 축소)
                if profit_rate <= -0.015:
                    sell_reason = f"절대 손절 {profit_rate*100:+.2f}%"

                # v5.6: 최소 보유시간 (AutoTune 동적 조정, 기본 5분)
                # 데이터: surge 5분내 -1%+ = 전패. 5분은 버텨야 함
                elif elapsed_min < getattr(self, '_surge_min_hold', 5):
                    print(f"  🚀 [SURGE] {coin}: {profit_rate*100:+.2f}% | 최소보유 대기 ({elapsed_min:.1f}분/{getattr(self, '_surge_min_hold', 5)}분)")
                    continue

                # 2. v5.4: 스마트 손절 — 손절 구간(-1%~-2%)에서 거래량 보고 최적 타이밍
                elif profit_rate <= -0.01:
                    vol_alive = False
                    try:
                        _ohlcv_1m = pyupbit.get_ohlcv(f"KRW-{coin}", interval="minute1", count=5)
                        if _ohlcv_1m is not None and len(_ohlcv_1m) >= 3:
                            recent_vol = _ohlcv_1m['volume'].iloc[-1]
                            avg_vol = _ohlcv_1m['volume'].iloc[:-1].mean()
                            # 1분봉 양봉 여부 (반등 중인지)
                            last_bullish = _ohlcv_1m['close'].iloc[-1] > _ohlcv_1m['open'].iloc[-1]
                            if avg_vol > 0 and recent_vol >= avg_vol * 1.5 and last_bullish:
                                vol_alive = True
                    except:
                        pass
                    defer_count = pos.get('_stop_defer_count', 0)
                    if vol_alive and defer_count < 3:
                        # 거래량 활발 + 양봉 → 반등 대기 (최대 3회 = ~90초)
                        pos['_stop_defer_count'] = defer_count + 1
                        print(f"  🚀 [SURGE] {coin} 스마트 손절 대기 {defer_count+1}/3 — 거래량↑ 양봉 ({profit_rate*100:+.2f}%)")
                    else:
                        sell_reason = f"손절 {profit_rate*100:+.2f}% (유예 {defer_count}회)"

                # v5.5: 5분 퀵엑싯 제거 (데이터: 5~15분 손절이 승률 최악 12%)
                # 대신 -1% 이상 깊은 손실만 타임아웃 적용

                # 2. 타임아웃 손절 — 깊은 손실(-0.5%+)만 적용
                elif elapsed_min >= timeout_loss and profit_rate < -0.005:
                    sell_reason = f"타임아웃 손절 ({elapsed_min:.0f}분, {profit_rate*100:+.2f}%)"

                # 3. 횡보 정리 — 소손실(-0.5% 미만)은 더 기다림
                elif elapsed_min >= timeout_flat and profit_rate < -0.005:
                    sell_reason = f"횡보 정리 ({elapsed_min:.0f}분, {profit_rate*100:+.2f}%)"

                # v5.5: 최종 타임아웃 — 40분 지나면 뭐든 정리
                elif elapsed_min >= 40 and profit_rate < 0.003:
                    sell_reason = f"최종 타임아웃 ({elapsed_min:.0f}분, {profit_rate*100:+.2f}%)"

                # v5.3: 미니 트레일링 — +0.5%~2% 구간에서 고점 대비 -0.4% 하락 시 익절
                elif 0.005 <= peak_rate < 0.02 and (peak_rate - profit_rate) >= 0.004:
                    sell_reason = f"미니 트레일링 익절 (고점 {peak_rate*100:+.2f}% → {profit_rate*100:+.2f}%)"

                # 4. +2% 이상 트레일링 스탑 활성화 (고점 대비 -0.7% 하락 시 매도)
                elif peak_rate >= 0.02 and (peak_rate - profit_rate) >= 0.007:
                    sell_reason = f"트레일링 (고점 {peak_rate*100:+.2f}% → 현재 {profit_rate*100:+.2f}%)"

                # 5. +3% 이상 S/R 저항선 확인 후 익절
                elif profit_rate >= 0.03:
                    try:
                        sr = self._calc_sr_levels(coin)
                        if sr and sr.get('nearest_resistance') and price >= sr['nearest_resistance'] * 0.997:
                            sell_reason = f"S/R 저항 익절 ({profit_rate*100:+.2f}%, 저항 {sr['nearest_resistance']:,.0f})"
                        elif peak_rate >= 0.03 and (peak_rate - profit_rate) >= 0.007:
                            sell_reason = f"트레일링 익절 (고점 {peak_rate*100:+.2f}% → {profit_rate*100:+.2f}%)"
                        # else: 트레일링 유지
                    except:
                        if peak_rate >= 0.03 and (peak_rate - profit_rate) >= 0.007:
                            sell_reason = f"트레일링 익절 ({profit_rate*100:+.2f}%)"

                if sell_reason:
                    pnl = round(pos['amount'] * profit_rate)
                    tag = "✅" if profit_rate > 0 else "❌"
                    print(f"🚀 [SURGE {tag}] {coin} {sell_reason} | {pnl:+,}원 | {elapsed_min:.0f}분")
                    if self.mode == "real" and self.upbit:
                        sell_qty = pos['quantity']
                        real_qty = float(self.upbit.get_balance(coin) or 0)
                        if real_qty > 0:
                            sell_qty = min(sell_qty, real_qty)
                        result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                        if result is None or (isinstance(result, dict) and 'error' in result):
                            print(f"❌ 서지 {coin} 매도 실패, 포지션 유지")
                            continue
                        time.sleep(2)
                        self.current_balance = self._get_krw_balance()
                    else:
                        self.current_balance += pos['amount'] + pnl
                    self.trades.append({'coin': coin, 'batch': 'surge_trade', 'buy_price': pos['buy_price'],
                                        'sell_price': price, 'profit_rate': profit_rate * 100,
                                        'timestamp': datetime.now().isoformat()})
                    self._db_log_trade(coin, "sell", price, pos['quantity'], pos['amount'] + pnl,
                                       profit=pnl, profit_rate=profit_rate * 100,
                                       momentum=pos.get('momentum', 0), batch='surge_trade')
                    self._notify(f"[SURGE {tag}] {coin} {sell_reason} | {pnl:+,}원")
                    del self._surge_positions[coin]
                    self._save_positions()
                    if profit_rate <= 0:
                        self._sell_cooldown[coin] = {'time': now, 'stoploss': True, 'source': 'surge', 'exit_price': price}
                        # v5.9: surge 연속 손실 카운터 (2회 시 30분 진입 차단)
                        self._surge_loss_streak = getattr(self, '_surge_loss_streak', 0) + 1
                        if self._surge_loss_streak >= 2:
                            self._surge_blocked_until = now + 1800
                            print(f"🚨 [SURGE] 연속 {self._surge_loss_streak}회 손실 → 30분 진입 차단")
                            self._notify(f"[SURGE 차단] 연속 {self._surge_loss_streak}회 손실 → 30분 중단")
                    else:
                        self._surge_loss_streak = 0  # 수익 시 초기화
                    continue

                print(f"  🚀 [SURGE] {coin}: {profit_rate*100:+.2f}% | 고점 {peak_rate*100:+.2f}% | {elapsed_min:.0f}분")

            # ── 신규 진입 ──
            # v5.12: surge 관찰 모드 — 감지는 하되 매수 안 함
            if _surge_observe_only:
                # 감지 로그만 남기기
                surge_coins = self._detect_surge_coins()
                if surge_coins:
                    for sc in surge_coins[:2]:
                        print(f"👁️ [SURGE 관찰] {sc['coin']}: +{sc['change_10m']*100:.1f}%, 점수 {sc['score']}점 (매수 안 함)")
                return

            if len(self._surge_positions) >= self._surge_max:
                return

            # v5.9: surge 연속 손실 차단 체크
            if now < getattr(self, '_surge_blocked_until', 0):
                remaining = int((self._surge_blocked_until - now) / 60)
                print(f"🚨 [SURGE] 연속 손실 차단 중 (잔여 {remaining}분) → 신규 진입 중단")
                return

            # 5분 쿨타임
            if now - self._last_surge_entry < 300:
                return

            # 텔레그램 일시중지 체크
            if getattr(self, '_tg_paused', False):
                return

            # 유휴 자금 체크
            available = self.current_balance
            surge_budget = min(1000000, int(available * 0.10))  # v5.11: 1.5M/15% → 1M/10%
            if surge_budget < 10000:
                return

            # 급등 코인 감지
            print(f"🚀 [SURGE] 급등 스캔 시작 (잔고 {available:,.0f}원, 예산 {surge_budget:,}원)")
            surge_coins = self._detect_surge_coins()
            if not surge_coins:
                print(f"🚀 [SURGE] 급등 코인 미감지 — 대기 중")
                return

            target = surge_coins[0]
            coin = target['coin']
            price = target['price']

            # v5.8: 시간대별 급등 기준 강화 (DB 승률 데이터 기반)
            _hour = datetime.now().hour
            _is_night = (_hour >= 23 or _hour < 6)
            _is_evening = (18 <= _hour < 23)
            _is_morning = (8 <= _hour < 13)
            if _is_night:
                if target.get('score', 0) < 9:
                    print(f"🚀 [SURGE] {coin} 심야 점수 부족 ({target.get('score',0)}점 < 9점) → 패스")
                    return
                surge_budget = surge_budget // 2
                print(f"🌙 [SURGE] 심야 모드: 점수 {target.get('score',0)}점 ≥ 9 OK, 예산 절반 {surge_budget:,}원")
            elif _is_evening:
                if target.get('score', 0) < 8:
                    print(f"🚀 [SURGE] {coin} 저녁 점수 부족 ({target.get('score',0)}점 < 8점) → 패스")
                    return
                surge_budget = int(surge_budget * 0.7)
                print(f"🌆 [SURGE] 저녁 모드: 점수 {target.get('score',0)}점 ≥ 8 OK, 예산 70% {surge_budget:,}원")
            elif _is_morning:
                # v5.11: 오전(8~13시) surge 완전 차단 (09시대 -224k 데이터 기반)
                print(f"🚀 [SURGE] {coin} 오전({_hour}시) → surge 완전 차단")
                return
                surge_budget = int(surge_budget * 0.6)
                print(f"🌅 [SURGE] 오전 모드: 점수 {target.get('score',0)}점 ≥ 8 OK, 예산 60% {surge_budget:,}원")

            # v5.3: 즉시 매수 대신 눌림목 워치리스트에 추가
            if coin in self._surge_watchlist:
                return  # 이미 워치 중

            self._surge_watchlist[coin] = {
                'detected_price': price,
                'peak_price': price,
                'timestamp': now,
                'surge_type': target.get('surge_type', 'rapid'),
                'change_10m': target['change_10m'],
                'vol_ratio': target['vol_ratio'],
            }
            self._last_surge_entry = now
            print(f"🚀 [SURGE 워치] {coin} 눌림목 대기 등록 | {price:,.0f}원 | 10분 +{target['change_10m']*100:.1f}% | 거래량 {target['vol_ratio']:.1f}배")
            self._notify(f"[SURGE WATCH] {coin} 눌림목 대기 | {price:,.0f}원 | +{target['change_10m']*100:.1f}%")

        except Exception as e:
            print(f"⚠️ [SURGE] 오류: {e}")

    def scalp_clear_for_momentum(self):
        """더 좋은 모멘텀 발견 시 중타 포지션 정리 (적당히 익절/손절 후 전환)"""
        for coin in list(self._scalp_positions.keys()):
            pos = self._scalp_positions[coin]
            # v5.8: 최소 10분 보유 후에만 전환 허용 (0원 즉시매도 근절)
            elapsed_min = (time.time() - pos['timestamp']) / 60
            if elapsed_min < 10:
                print(f"⏳ 중타 {coin} {elapsed_min:.0f}분 보유 — 최소 10분 미충족 → 전환 보류")
                return
            price = self.get_price(coin)
            if price:
                profit_rate = (price - pos['buy_price']) / pos['buy_price']
                profit = round(pos['amount'] * profit_rate)
                tag = "익절" if profit_rate > 0 else "손절"
                print(f"💎 [중타→모멘텀 전환] {coin} {tag} ({profit_rate*100:+.2f}%, {profit:+,}원)")
                if self.mode == "real" and self.upbit:
                    sell_qty = pos['quantity']
                    real_qty = float(self.upbit.get_balance(coin) or 0)
                    if real_qty > 0:
                        sell_qty = min(sell_qty, real_qty)
                    result = self.upbit.sell_market_order(f"KRW-{coin}", sell_qty)
                    if result is None or (isinstance(result, dict) and 'error' in result):
                        print(f"❌ 중타 {coin} 전환 매도 실패, 포지션 유지")
                        continue
                    time.sleep(2)
                    self.current_balance = self._get_krw_balance()
                else:
                    self.current_balance += pos['amount'] + profit
                self.trades.append({'coin': coin, 'batch': 'idle_trade', 'buy_price': pos['buy_price'],
                                    'sell_price': price, 'profit_rate': profit_rate * 100,
                                    'timestamp': datetime.now().isoformat()})
                self._db_log_trade(coin, "sell", price, pos['quantity'], pos['amount'] + profit,
                                   profit=profit, profit_rate=profit_rate * 100, batch='idle_trade')
                self._notify(f"[IDLE→MOM] {coin} {tag} {profit_rate*100:+.2f}% ({profit:+,}원)")
                del self._scalp_positions[coin]
                self._save_positions()
            else:
                print(f"⚠️ [중타→모멘텀] {coin} 가격 조회 실패 → 포지션 유지")

    def monitor_positions(self):
        """포지션 모니터링 - 익절/손절/횡보교체/스왑"""
        if not self.positions:
            return

        print("📋 포지션 모니터링...")
        for coin in list(self.positions.keys()):
            price = self.get_price(coin)
            if not price:
                continue

            # 극단 변동 체크 → 긴급 손절
            volatility = self.check_extreme_volatility(coin)
            if volatility == "emergency_sell":
                print(f"🚨 {coin} 3분 10%+ 급변동 → 전체 긴급 손절!")
                for batch_id in list(self.positions[coin].keys()):
                    self.place_sell_order(coin, batch_id)
                self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                continue

            for batch_id in list(self.positions[coin].keys()):
                position = self.positions[coin][batch_id]
                profit_rate = (price - position['buy_price']) / position['buy_price']
                hours = self._hours_held(position)

                # v4.2: 포지션 고점 수익률 기록 (조기 정리 판단용)
                if profit_rate > position.get('peak_rate', 0):
                    position['peak_rate'] = profit_rate

                # ★ 빠른 컷 (완화): 1분 내 -1.5% 급락 즉시컷, 5분 내 -1.0% 빠른컷
                minutes_held = hours * 60
                if minutes_held <= 1 and profit_rate <= -0.015:
                    print(f"✂️ {coin} {batch_id} 급락 즉시컷 ({minutes_held:.0f}분 보유, {profit_rate*100:+.2f}% ≤ -1.5%)")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                    continue
                elif minutes_held <= 5 and profit_rate <= -0.01:
                    print(f"✂️ {coin} {batch_id} 빠른 컷 ({minutes_held:.0f}분 보유, {profit_rate*100:+.2f}% ≤ -1.0%)")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                    continue

                momentum = self.calculate_momentum(coin)
                uptrend = self._is_uptrend(coin)

                # v4.1: 서지 모드 포지션 — 가격+시간만으로 빠르게 관리
                is_surge = position.get('surge_mode', False)
                if is_surge:
                    minutes_surge = self._hours_held(position) * 60
                    surge_peak_key = f"_surge_peak_{coin}_{batch_id}"
                    surge_peak = getattr(self, surge_peak_key, 0)
                    if profit_rate > surge_peak:
                        setattr(self, surge_peak_key, profit_rate)
                        surge_peak = profit_rate

                    surge_sell = None
                    # -1.5% 즉시 손절
                    if profit_rate <= -0.015:
                        surge_sell = f"손절 {profit_rate*100:+.2f}%"
                    # v5.8: 5분+ -1% 손절 추가 (기존: -1~-1.5% 구간 28건 -486k 방치)
                    elif minutes_surge >= 5 and profit_rate <= -0.01:
                        surge_sell = f"5분 손절 ({minutes_surge:.0f}분, {profit_rate*100:+.2f}%)"
                    # 10분+ 손실 시 타임아웃
                    elif minutes_surge >= 10 and profit_rate < 0:
                        surge_sell = f"타임아웃 손절 ({minutes_surge:.0f}분, {profit_rate*100:+.2f}%)"
                    # 15분+ 횡보 정리
                    elif minutes_surge >= 15 and profit_rate < 0.005:
                        surge_sell = f"횡보 정리 ({minutes_surge:.0f}분, {profit_rate*100:+.2f}%)"
                    # +2% 트레일링 (고점 -0.7%)
                    elif surge_peak >= 0.02 and (surge_peak - profit_rate) >= 0.007:
                        surge_sell = f"트레일링 (고점 {surge_peak*100:+.2f}% → {profit_rate*100:+.2f}%)"
                    # +3% S/R 저항 도달 시 익절
                    elif profit_rate >= 0.03:
                        try:
                            sr = self._calc_sr_levels(coin)
                            if sr and sr.get('nearest_resistance') and price >= sr['nearest_resistance'] * 0.997:
                                surge_sell = f"S/R 저항 익절 ({profit_rate*100:+.2f}%)"
                        except:
                            pass

                    if surge_sell:
                        print(f"🚀 {coin} {batch_id} SURGE {surge_sell}")
                        self.place_sell_order(coin, batch_id)
                        if profit_rate <= 0:
                            self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                        if hasattr(self, surge_peak_key):
                            delattr(self, surge_peak_key)
                        continue
                    else:
                        print(f"  🚀 {coin} {batch_id}: {profit_rate*100:+.2f}% | 고점 {surge_peak*100:+.2f}% | {minutes_surge:.0f}분 | SURGE 보유중")
                        continue

                is_strong_position = position.get('individual_strong', False)

                # v3.3: 개별 강세 코인 — 손절/횡보만 전용, 익절은 S/R+트레일링 공유
                if is_strong_position:
                    if profit_rate <= -0.03:
                        print(f"🔥📉 {coin} {batch_id} 개별강세 손절 ({profit_rate*100:+.2f}% ≤ -3%)")
                        self.place_sell_order(coin, batch_id)
                        self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                        continue
                    elif hours >= 4 and profit_rate < 0.005:
                        print(f"🔥⏰ {coin} {batch_id} 개별강세 4시간 횡보 ({profit_rate*100:+.2f}%) → 정리")
                        self.place_sell_order(coin, batch_id)
                        self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': False}
                        continue
                    elif profit_rate < 0.01:
                        # +1% 미만은 아직 초기 — 홀딩
                        print(f"  {coin} {batch_id}: {profit_rate*100:+.2f}% | {hours:.1f}시간 | 🔥 개별강세 보유중")
                        continue
                    # +1% 이상이면 아래 S/R/트레일링 로직으로 자연스럽게 흘러감

                # === 1-1. 모멘텀 붕괴 → 연속 2회 확인 후 탈출 (v3.2: 노이즈 방지) ===
                if momentum < 20 and profit_rate < 0.015:
                    if not hasattr(self, '_collapse_count'):
                        self._collapse_count = {}
                    collapse_key = f"{coin}_{batch_id}"
                    prev_count = self._collapse_count.get(collapse_key, 0)
                    self._collapse_count[collapse_key] = prev_count + 1
                    if prev_count + 1 >= 2:
                        # 연속 2회 이상 붕괴 → 진짜 붕괴, 탈출
                        print(f"💀 {coin} {batch_id} 모멘텀 연속 붕괴({momentum}점, {prev_count+1}회) → 즉시 탈출 ({profit_rate*100:+.2f}%)")
                        self.place_sell_order(coin, batch_id)
                        self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                        del self._collapse_count[collapse_key]
                        continue
                    else:
                        print(f"⚠️ {coin} {batch_id} 모멘텀 {momentum}점 붕괴 1회차 → 다음 사이클 재확인")
                        continue
                else:
                    # 모멘텀 회복되면 카운트 초기화
                    if hasattr(self, '_collapse_count'):
                        collapse_key = f"{coin}_{batch_id}"
                        if collapse_key in self._collapse_count:
                            del self._collapse_count[collapse_key]

                # === 1-2. 모멘텀 급락 + 수익 있음 → 수익 확보 매도 ===
                if momentum < 30 and profit_rate >= 0.005:
                    print(f"⚡ {coin} {batch_id} 모멘텀 급락({momentum}점) → 수익 확보 ({profit_rate*100:+.2f}%)")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': False}
                    continue

                # === 2. 익절 (v4.0: S/R 저항선 + 트레일링 + fallback %) ===
                sr_tp_triggered = False

                # 2-1. S/R 저항선 도달 시 익절
                entry_resistance = position.get('sr_resistance')
                if entry_resistance and entry_resistance > 0 and profit_rate > 0:
                    try:
                        current_sr = self._calc_sr_levels(coin)
                        effective_resistance = entry_resistance
                        if current_sr and current_sr['nearest_resistance']:
                            effective_resistance = min(entry_resistance, current_sr['nearest_resistance'])
                        if effective_resistance > 0 and price >= effective_resistance * 0.997:
                            vol_score = self.analyze_volume(coin)
                            if vol_score >= 70 and momentum >= 70:
                                print(f"💎 {coin} {batch_id} 저항선 도달 + 돌파 기세 (거래량{vol_score}, 모멘텀{momentum}) → 홀딩")
                            else:
                                print(f"💰 {coin} {batch_id} 저항선 도달 익절 (가격 {price:,.0f} → 저항 {effective_resistance:,.0f}, {profit_rate*100:+.2f}%)")
                                self.place_sell_order(coin, batch_id)
                                sr_tp_triggered = True
                    except:
                        pass
                if sr_tp_triggered:
                    continue

                # 2-2. fallback 타겟 (S/R이 잡지 못한 경우)
                if momentum >= 70:
                    target = 0.04    # 고모멘텀: 4% (S/R이 먼저 잡으므로 확대)
                elif momentum >= 50:
                    target = 0.03    # 중모멘텀: 3%
                else:
                    target = 0.02    # 저모멘텀: 2%

                # 2-3. 트레일링 스탑 (v4.2: 모멘텀 기반 동적 트레일링)
                trailing_key = f"{coin}_{batch_id}_peak"
                if not hasattr(self, '_trailing_peaks'):
                    self._trailing_peaks = {}

                # 모멘텀별 트레일링 시작점 & trail drop 조정
                if momentum >= 70:
                    TRAILING_START = 0.020   # 고모멘텀: +2%부터 추적 (더 먹고 나오기)
                elif momentum >= 50:
                    TRAILING_START = 0.015   # 중모멘텀: +1.5%부터
                else:
                    TRAILING_START = 0.012   # 저모멘텀: +1.2% (기존)

                if profit_rate >= TRAILING_START:
                    current_peak = self._trailing_peaks.get(trailing_key, 0)
                    if profit_rate > current_peak:
                        self._trailing_peaks[trailing_key] = profit_rate
                        current_peak = profit_rate
                    # 모멘텀별 trail drop (고모멘텀 → 느슨하게, 더 오래 추적)
                    # v5.12: 트레일링 확대 (수익 더 먹기, batch_2 수익 극대화)
                    # 기존 -0.7~1.0% → -1.0~1.5%로 느슨하게
                    if momentum >= 70:
                        if current_peak >= 0.04:
                            trail_drop = 0.012   # +4% 고점: -1.2% (기존 -0.8%)
                        elif current_peak >= 0.02:
                            trail_drop = 0.015   # +2% 고점: -1.5% (기존 -1.0%)
                        else:
                            trail_drop = 0.012
                    elif momentum >= 50:
                        if current_peak >= 0.03:
                            trail_drop = 0.012   # +3% 고점: -1.2% (기존 -0.7%)
                        elif current_peak >= 0.015:
                            trail_drop = 0.010   # (기존 -0.7%)
                        else:
                            trail_drop = 0.010
                    else:
                        if current_peak >= 0.02:
                            trail_drop = 0.008   # 저모멘텀: (기존 -0.5%)
                        elif current_peak >= 0.01:
                            trail_drop = 0.008   # (기존 -0.6%)
                        else:
                            trail_drop = 0.008
                    # v4.4: AutoTune 트레일링 조정
                    for _atr in self._autotune_rules:
                        if _atr['rule_type'] == 'trailing_adjust':
                            trail_drop = max(0.003, trail_drop + _atr['param_value'])
                            break
                    if current_peak - profit_rate >= trail_drop:
                        print(f"📉 {coin} {batch_id} 트레일링 (고점 {current_peak*100:+.2f}% → 현재 {profit_rate*100:+.2f}%, 폭 -{trail_drop*100:.1f}%, 모멘텀 {momentum:.0f})")
                        del self._trailing_peaks[trailing_key]
                        self.place_sell_order(coin, batch_id)
                        continue
                else:
                    if trailing_key in self._trailing_peaks:
                        del self._trailing_peaks[trailing_key]

                if profit_rate >= target:
                    print(f"💰 {coin} {batch_id} fallback 익절 ({profit_rate*100:+.2f}% >= {target*100:.1f}%)")
                    if trailing_key in self._trailing_peaks:
                        del self._trailing_peaks[trailing_key]
                    self.place_sell_order(coin, batch_id)
                    continue

                # === 3. 손절 (v4.0: S/R 지지선 이탈 + fallback %) ===
                sr_sl_triggered = False

                # 3-1. S/R 지지선 이탈 손절
                entry_support = position.get('sr_support')
                if entry_support and entry_support > 0:
                    try:
                        current_sr = self._calc_sr_levels(coin)
                        effective_support = entry_support
                        if current_sr and current_sr['nearest_support']:
                            effective_support = max(entry_support, current_sr['nearest_support'])
                        if price < effective_support * 0.995:
                            print(f"🔻 {coin} {batch_id} 지지선 이탈 손절 (가격 {price:,.0f} < 지지 {effective_support:,.0f}, {profit_rate*100:+.2f}%)")
                            self.place_sell_order(coin, batch_id)
                            self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                            sr_sl_triggered = True
                    except:
                        pass
                if sr_sl_triggered:
                    continue

                # 3-2. fallback 고정 % 손절 (S/R이 먼저 잡으므로 여유)
                if momentum < 20:
                    stop_loss = -0.015  # 붕괴 → -1.5%
                elif momentum < 40:
                    stop_loss = -0.02   # 저모멘텀 → -2%
                else:
                    stop_loss = -0.03   # 기본 → -3%

                # v4.4: AutoTune 손절 조정
                for _atr in self._autotune_rules:
                    if _atr['rule_type'] == 'stoploss_adjust':
                        stop_loss = max(-0.05, stop_loss + _atr['param_value'])
                        break

                if profit_rate <= stop_loss:
                    print(f"🔻 {coin} {batch_id} fallback 손절 ({profit_rate*100:+.2f}%, 기준 {stop_loss*100:.1f}%, 모멘텀 {momentum:.0f}점)")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                    continue

                # === 3.3b v5.6: 소액 손실 조기컷 완화 — -0.3% 이상은 회복 기회 부여 ===
                # 데이터: -0.3% 이내 26건 근소패배 → 더 기다리면 회복 가능
                if -0.005 < profit_rate < -0.003 and minutes_held >= 10:
                    try:
                        ticker = f"KRW-{coin}"
                        ohlcv_3m = pyupbit.get_ohlcv(ticker, interval="minute3", count=3)
                        if ohlcv_3m is not None and len(ohlcv_3m) >= 3:
                            last_3 = ohlcv_3m.tail(3)
                            bearish_count = sum(1 for _, row in last_3.iterrows() if row['close'] < row['open'])
                            if bearish_count >= 3:  # v5.6: 2→3연속 음봉 (더 확실한 하락만)
                                print(f"✂️ {coin} {batch_id} 소액 조기컷: {profit_rate*100:+.2f}% + 3분봉 {bearish_count}연속 음봉")
                                self.place_sell_order(coin, batch_id)
                                self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': True, 'exit_price': price}
                                continue
                    except:
                        pass

                # === 3.3c v4.3: 소액 수익 미니 트레일링 — +0.3% 도달 시 -0.15% 하락하면 익절 ===
                if 0.003 <= profit_rate < TRAILING_START:
                    mini_trail_key = f"{coin}_{batch_id}_mini_peak"
                    if not hasattr(self, '_mini_trail_peaks'):
                        self._mini_trail_peaks = {}
                    mini_peak = self._mini_trail_peaks.get(mini_trail_key, 0)
                    if profit_rate > mini_peak:
                        self._mini_trail_peaks[mini_trail_key] = profit_rate
                        mini_peak = profit_rate
                    if mini_peak >= 0.003 and mini_peak - profit_rate >= 0.0015:
                        print(f"📉 {coin} {batch_id} 미니 트레일링 (고점 {mini_peak*100:+.2f}% → {profit_rate*100:+.2f}%, -0.15%) → 소액 익절")
                        if mini_trail_key in self._mini_trail_peaks:
                            del self._mini_trail_peaks[mini_trail_key]
                        self.place_sell_order(coin, batch_id)
                        continue
                elif profit_rate < 0.003:
                    # 수익 떨어지면 미니 트레일링 리셋
                    mini_trail_key = f"{coin}_{batch_id}_mini_peak"
                    if hasattr(self, '_mini_trail_peaks') and mini_trail_key in self._mini_trail_peaks:
                        del self._mini_trail_peaks[mini_trail_key]

                # === v5.14: 15분 횡보 조기 정리 (데이터: 15분+ 승률 급감, 5-15분이 최적) ===
                if 0.25 <= hours < 0.5 and abs(profit_rate) < 0.003 and momentum < 35 and not uptrend:
                    print(f"⏰ {coin} {batch_id} 15분 횡보(±0.3% 미만) + 모멘텀{momentum:.0f}점↓ → 조기 정리")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': False, 'early_exit': True, 'exit_price': price}
                    continue

                # === 3.4 v5.6: 30분+ 수익 미도달 정리 — 손실 중(-0.5%+)만 ===
                # 원칙: 0% 이상이면 절대 팔지 않음
                if hours >= 0.5 and profit_rate < -0.005 and not uptrend:
                    peak = position.get('peak_rate', profit_rate)
                    if peak < 0.003:
                        print(f"⏰ {coin} {batch_id} 30분+ 수익 미도달(고점 {peak*100:+.2f}%) + 현재 {profit_rate*100:+.2f}% → 조기 정리")
                        self.place_sell_order(coin, batch_id)
                        self._sell_cooldown[coin] = {
                            'time': time.time(), 'stoploss': False,
                            'early_exit': True, 'exit_price': price
                        }
                        continue

                # === 3.5 v5.6: 시간 기반 정리 완화 (30분→40분, -0.3%→-0.5%) ===
                # 근소 손실은 더 기다림
                if hours >= 0.67 and profit_rate < -0.005 and momentum < 35 and not uptrend:
                    print(f"⏰ {coin} {batch_id} 30분+ 손실({profit_rate*100:+.2f}%) + 모멘텀 {momentum:.0f}점 → 정리")
                    self.place_sell_order(coin, batch_id)
                    self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': False}
                    continue

                # === 4. v5.6: 횡보 처리 — 모멘텀 점검 후 판단 ===
                # 원칙: 성급하게 팔아 수수료만 날리지 말고, 모멘텀/거래량 보고 판단
                # 단, 지속 횡보면 자금 묶이지 않게 정리
                if hours >= 0.75 and abs(profit_rate) < 0.01 and not uptrend:
                    _vol_ok = False
                    try:
                        _vol_score = self.analyze_volume(coin)
                        _vol_ok = _vol_score >= 40
                    except:
                        pass

                    if _vol_ok or momentum >= 30:
                        # 거래량/모멘텀 살아있음 → 아직 기회 있으니 유지
                        print(f"  ⏳ {coin} {batch_id} 횡보 {hours:.1f}시간 | {'거래량↑' if _vol_ok else ''} 모멘텀{momentum:.0f}점 → 유지")
                    elif hours >= 2.0:
                        # 2시간+ 횡보 + 모멘텀/거래량 약 → 더 좋은 코인 있으면 스왑, 없으면 정리
                        better = self._find_better_coin(coin, momentum)
                        # v5.8: 포지션 슬롯 여유 있을 때만 스왑 (매도 후 매수 차단 방지)
                        # 교체 후 남은 슬롯 = 현재 포지션 수 - 1(매도 예정) + 0(아직 미진입) → 항상 가능
                        # 단, better 후보가 현 보유 종목이 아니어야 함
                        if better and better[0] not in self.positions:
                            print(f"🔄 {coin} {batch_id} 횡보 {hours:.1f}시간 ({profit_rate*100:+.2f}%) → {better[0]}({better[1]}점)으로 교체")
                            sold = self.place_sell_order(coin, batch_id)
                            if sold:
                                time.sleep(2)
                                bought = self.place_buy_order(better[0], position['amount'], momentum=better[1])
                                # v5.8: 매수 실패 시 쿨다운 설정 (재진입 차단 방지)
                                if not bought:
                                    print(f"  ⚠️ {better[0]} 매수 실패 → 자금 유지, {coin} 쿨다운 해제")
                            continue
                        elif hours >= 3.0:
                            # 3시간 넘으면 자금 묶임 방지 정리
                            print(f"  🔚 {coin} {batch_id} 횡보 {hours:.1f}시간 + 모멘텀{momentum:.0f}점↓ → 정리")
                            self.place_sell_order(coin, batch_id)
                            self._sell_cooldown[coin] = {'time': time.time(), 'stoploss': False}
                            continue
                        else:
                            print(f"  ⚠️ {coin} {batch_id} 횡보 {hours:.1f}시간 + 모멘텀{momentum:.0f}점↓ (3h 후 정리)")
                    else:
                        # 45분~2시간: 아직 지켜볼 시간
                        print(f"  ⏳ {coin} {batch_id} 횡보 {hours:.1f}시간 | 모멘텀{momentum:.0f}점 → 지켜보는 중")

                # 상태 출력
                status = "📈 상승중" if uptrend else "➡️ 횡보" if abs(profit_rate) < 0.01 else "📉 하락중"
                print(f"  {coin} {batch_id}: {profit_rate*100:+.2f}% | {hours:.1f}시간 | 모멘텀 {momentum}점 | {status}")
    
    # ============================================
    # 5. 상태/로그
    # ============================================
    
    def print_status(self):
        """상태 출력"""
        print(f"\n{'='*60}")
        print(f"현재 상태 [{datetime.now()}]")
        print(f"{'='*60}")
        print(f"초기 자금: {self.initial_balance:,}원")
        print(f"현재 자금: {self.current_balance:,}원")
        profit = self.current_balance - self.initial_balance
        profit_rate = (profit / self.initial_balance) * 100 if self.initial_balance > 0 else 0
        print(f"수익/손실: {profit:+,}원 ({profit_rate:+.2f}%)")
        
        if self.positions:
            print(f"\n보유 포지션:")
            for coin, batches in self.positions.items():
                price = self.get_price(coin)
                if price:
                    for batch_id, position in batches.items():
                        pnl = (price - position['buy_price']) * position['quantity']
                        pnl_rate = (pnl / position['amount']) * 100
                        print(f"  {coin} {batch_id}: {pnl_rate:+.2f}% ({pnl:+,.0f}원)")
        else:
            print(f"\n보유 포지션이 없습니다.")
        
        print(f"\n거래 이력: {len(self.trades)}건")
    
    def save_log(self):
        """로그 저장 (logs/ 폴더, 7일 이상 자동 삭제)"""
        log_dir = os.path.join(os.path.dirname(__file__), "logs")
        os.makedirs(log_dir, exist_ok=True)

        # 7일 이상된 로그 자동 삭제
        try:
            cutoff = time.time() - 7 * 86400
            for f in os.listdir(log_dir):
                fpath = os.path.join(log_dir, f)
                if f.startswith("trading_log_") and f.endswith(".json") and os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
        except Exception:
            pass

        log = {
            'timestamp': datetime.now().isoformat(),
            'mode': self.mode,
            'initial_balance': self.initial_balance,
            'current_balance': self.current_balance,
            'profit': self.current_balance - self.initial_balance,
            'positions': self.positions,
            'trades': self.trades[:100]  # 최근 100개
        }

        filename = f"trading_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(log_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(log, f, indent=2, ensure_ascii=False)

        print(f"✅ 로그 저장됨: {filename}")
    
    # ============================================
    # 6. 메인 루프
    # ============================================
    
    def run(self):
        """메인 루프"""
        print("\n명령어: run, status, save, exit\n")
        
        while True:
            try:
                cmd = input("> ").strip().lower()
                
                if not cmd:
                    continue
                
                if cmd == "exit":
                    self.save_log()
                    print("\n프로그램 종료")
                    break
                elif cmd == "run":
                    self.run_trading_cycle()
                elif cmd == "status":
                    self.print_status()
                elif cmd == "save":
                    self.save_log()
                else:
                    print("❓ 명령어: run, status, save, exit")
            
            except EOFError:
                print("\n프로그램 종료 (EOF)")
                self.save_log()
                break
            except KeyboardInterrupt:
                print("\n프로그램 종료")
                self.save_log()
                break
            except Exception as e:
                print(f"❌ 오류: {e}")


# ============================================
# 실행
# ============================================

if __name__ == "__main__":
    import sys
    import faulthandler
    faulthandler.enable()  # segfault 시 traceback 출력
    if len(sys.argv) > 1 and sys.argv[1] in ["demo", "real"]:
        mode = sys.argv[1]
    else:
        mode = "demo"

    autorun = "--auto" in sys.argv
    # 스캔 주기(분), 기본 5분
    interval = 5
    for arg in sys.argv:
        if arg.startswith("--interval="):
            try:
                interval = int(arg.split("=")[1])
            except:
                pass

    bot = TradingBotV3(mode=mode)

    # SIGTERM/SIGINT 핸들러: 깔끔한 종료
    import signal
    def _graceful_shutdown(sig, frame):
        bot._notify(f"[STOP] 봇 종료 (signal {sig})")
        bot.save_log()
        bot._save_positions()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    if autorun:
        print(f"\n🔄 24시간 자동 모니터링 시작")
        print(f"   거래 주기: 3분 | 시장 체크: 5분 | 뉴스 갱신: 30분")
        print("   중지하려면: kill $(cat ~/Desktop/업비트자동/bot.pid)\n")

        # ★ 기존 봇 프로세스 완전 종료 (중복 실행 방지)
        pid_path = os.path.join(os.path.dirname(__file__), "bot.pid")
        try:
            if os.path.exists(pid_path):
                with open(pid_path, 'r') as f:
                    old_pid = int(f.read().strip())
                if old_pid != os.getpid():
                    os.kill(old_pid, 9)
                    print(f"⚠️ 기존 봇(PID {old_pid}) 강제 종료")
                    time.sleep(1)
        except (ProcessLookupError, ValueError):
            pass  # 이미 종료된 프로세스
        except Exception as e:
            print(f"⚠️ 기존 프로세스 정리 중 오류: {e}")

        with open(pid_path, "w") as f:
            f.write(str(os.getpid()))

        # 텔레그램 명령어 수신 시작
        bot._start_telegram_listener()

        cycle = 0
        last_market_check = 0     # 시장 체크 타이머 (초)
        last_news_refresh = 0     # 뉴스 갱신 타이머 (초)
        last_trade_cycle = 0      # 거래 사이클 타이머 (초)

        TRADE_INTERVAL = 180      # 3분
        MARKET_INTERVAL = 300     # 5분
        NEWS_INTERVAL = 1800      # 30분
        AUTOTUNE_INTERVAL = 10800 # 3시간 (v4.5: 빠른 피드백 루프)
        DASHBOARD_INTERVAL = 300  # 5분마다 대시보드 업데이트
        last_dashboard = 0

        while True:
            try:
                now = time.time()

                # v4.4: AutoTune 자동 분석 (6시간마다)
                if bot._autotune_enabled and now - bot._last_autotune >= AUTOTUNE_INTERVAL:
                    bot._autotune_run()
                    bot._last_autotune = now

                # 뉴스 갱신 (30분마다)
                if now - last_news_refresh >= NEWS_INTERVAL:
                    print(f"\n📰 [{datetime.now().strftime('%H:%M:%S')}] 뉴스 캐시 초기화...")
                    # 글로벌 RSS 캐시 초기화 → 다음 analyze_news_sentiment에서 자동 갱신
                    for attr in ('_news_cache', '_rss_entries', '_rss_cache_time',
                                 '_fg_cache', '_fg_cache_time'):
                        if hasattr(bot, attr):
                            delattr(bot, attr)
                    last_news_refresh = now

                # 시장 체크 (5분마다)
                if now - last_market_check >= MARKET_INTERVAL:
                    print(f"\n🌍 [{datetime.now().strftime('%H:%M:%S')}] 시장 상태 체크...")
                    strength, _ = bot.check_market_strength()
                    us_state, _ = bot.check_us_market()
                    last_market_check = now

                # v5.3: surge 포지션 또는 워치리스트 있으면 30초마다 긴급 모니터링
                if bot._surge_positions or bot._surge_watchlist:
                    with bot._trade_lock:
                        bot.surge_trade()

                # 거래 사이클 (3분마다)
                if now - last_trade_cycle >= TRADE_INTERVAL:
                    cycle += 1
                    print(f"\n{'='*60}")
                    print(f"🔄 사이클 #{cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"{'='*60}")
                    bot.run_trading_cycle()
                    bot.save_log()
                    bot._db_snapshot(cycle_num=cycle, market_state=getattr(bot, 'last_market_state', ''))
                    last_trade_cycle = now

                # v4.2: 포지션 모니터링/매매만 lock (텔레그램 /sell 즉시 응답 보장)
                with bot._trade_lock:
                    if bot.positions and (now - last_trade_cycle) >= 60:
                        bot.monitor_positions()

                    bot.idle_fund_trade()
                    bot.surge_trade()

                # v5.6: 대시보드 업데이트 (5분마다)
                if now - last_dashboard >= DASHBOARD_INTERVAL:
                    try:
                        import subprocess as _sp
                        _sp.Popen(
                            [sys.executable, os.path.join(os.path.dirname(__file__), 'export_status.py')],
                            stdout=_sp.DEVNULL, stderr=_sp.DEVNULL
                        )
                    except:
                        pass
                    last_dashboard = now

                # v5.6: 100사이클마다 메모리/상태 자가점검
                if cycle > 0 and cycle % 100 == 0:
                    import gc
                    gc.collect()
                    # DB 정합성 체크
                    try:
                        bot._save_positions()
                        _mem = __import__('resource').getrusage(__import__('resource').RUSAGE_SELF).ru_maxrss
                        _mem_mb = _mem / (1024 * 1024)  # macOS: bytes
                        print(f"🔧 [자가점검] 사이클 #{cycle} | 메모리 {_mem_mb:.0f}MB | 포지션 {len(bot.positions)}개 | 서지 {len(bot._surge_positions)}개 | 잔고 {bot.current_balance:,.0f}원")
                        if _mem_mb > 500:
                            print(f"⚠️ 메모리 경고 {_mem_mb:.0f}MB > 500MB")
                            bot._notify(f"[WARN] 메모리 {_mem_mb:.0f}MB 초과")
                    except:
                        pass

                time.sleep(30)  # v5.2: 30초 간격 (surge 급락 대응)

            except KeyboardInterrupt:
                print("\n\n🛑 모니터링 종료")
                bot._notify("[STOP] 봇 수동 종료 (KeyboardInterrupt)")
                bot.save_log()
                break
            except KeyError as e:
                print(f"❌ KeyError: {e}, 1분 후 재시도...")
                bot._notify(f"[ERROR] KeyError: {e}, 1분 후 재시도")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
                time.sleep(60)
            except Exception as e:
                print(f"❌ 오류 발생: {type(e).__name__}: {e}, 1분 후 재시도...")
                bot._notify(f"[ERROR] {type(e).__name__}: {e}, 1분 후 재시도")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
                time.sleep(60)
    else:
        bot.run()
