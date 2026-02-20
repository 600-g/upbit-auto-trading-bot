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

        self.initial_balance = 500000
        self.current_balance = self.initial_balance
        self.positions = {}  # {coin: {batch_1: {...}, batch_2: {...}}}
        self.trades = []
        self.running = False

        print("\n" + "="*60)
        print("업비트 자동거래봇 v3.0")
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
        self.db = sqlite3.connect(db_path)
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
        self.db.commit()
        print("✅ DB 초기화 완료")

    def _db_log_trade(self, coin, action, price, quantity, amount,
                      profit=0, profit_rate=0, momentum=0, market_state="", batch=""):
        """거래 기록을 DB에 저장"""
        try:
            self.db.execute(
                "INSERT INTO trades (coin,batch,action,price,quantity,amount,profit,profit_rate,momentum,market_state,timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (coin, batch, action, price, quantity, amount, profit, profit_rate, momentum, market_state, datetime.now().isoformat())
            )
            self.db.commit()
        except Exception as e:
            print(f"⚠️ DB 기록 오류: {e}")

    def _db_snapshot(self, cycle_num=0, market_state=""):
        """현재 상태 스냅샷 DB에 저장"""
        try:
            self.db.execute(
                "INSERT INTO snapshots (balance,positions,market_state,cycle_num,timestamp) VALUES (?,?,?,?,?)",
                (self.current_balance, json.dumps(self.positions, ensure_ascii=False), market_state, cycle_num, datetime.now().isoformat())
            )
            self.db.commit()
        except Exception as e:
            print(f"⚠️ DB 스냅샷 오류: {e}")

    def _save_positions(self):
        """현재 포지션(self.positions)과 잔고를 DB에 저장 (매수/매도 후 호출)"""
        try:
            positions_json = json.dumps(self.positions, ensure_ascii=False)
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
            cursor = self.db.execute(
                "SELECT positions_json, balance, updated_at FROM active_positions WHERE id = 1"
            )
            row = cursor.fetchone()
            if row:
                saved_positions = json.loads(row[0])
                saved_balance = row[1]
                saved_time = row[2]
                if saved_positions:
                    self.positions = saved_positions
                    self.current_balance = saved_balance
                    print(f"✅ 포지션 복원 완료: {len(self.positions)}개 코인, 잔고 {saved_balance:,.0f}원 (저장 시각: {saved_time})")
                    for coin, batches in self.positions.items():
                        for batch_id, pos in batches.items():
                            print(f"   - {coin} {batch_id}: {pos['amount']:,.0f}원 @ {pos['buy_price']:,.0f}원")
                else:
                    print("✅ 저장된 포지션 없음 (빈 상태)")
            else:
                print("✅ 이전 포지션 기록 없음 (신규 시작)")
        except Exception as e:
            print(f"⚠️ 포지션 복원 오류: {e} (빈 상태로 시작)")

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
                        if chat_id == self._tg_chat_id and text.startswith("/"):
                            self._handle_telegram_command(text)
            except Exception as e:
                print(f"⚠️ 텔레그램 폴링 오류: {e}")
            time.sleep(5)

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
                          "/resume - 매매 재개")
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
        else:
            self._tg_send(f"❓ 알 수 없는 명령어: {cmd}\n/start 로 명령어 목록 확인")

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
        """실제 KRW 잔고 조회"""
        try:
            balance = self.upbit.get_balance("KRW")
            return float(balance) if balance else 0
        except Exception as e:
            print(f"⚠️ 잔고 조회 실패: {e}")
            return 0

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
        """RSI 분석 (0~100점) - 과매도일수록 매수 기회"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=14)

            closes = [x for x in ohlcv['close']]
            delta = np.diff(closes)

            gain = np.where(delta > 0, delta, 0).mean()
            loss = np.where(delta < 0, -delta, 0).mean()

            if loss == 0:
                rsi = 100 if gain > 0 else 50
            else:
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))

            if rsi < 25:
                return 100   # 극과매도 → 반등 기대
            elif rsi < 35:
                return 80
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
    
    def analyze_support_resistance(self, coin):
        """지지선/저항선 분석 (0~100점) - 지지선 근접할수록 높은 점수"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)

            current = [x for x in ohlcv['close']][-1]
            low = min([x for x in ohlcv['low']])

            distance = (current - low) / low

            if distance < 0.01:
                return 100   # 지지선 바로 위
            elif distance < 0.02:
                return 80
            elif distance < 0.03:
                return 60
            elif distance < 0.05:
                return 40
            else:
                return 10
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
        """뉴스/SNS 센티먼트 분석 (0~100점) - RSS + Reddit + 인플루언서 감지"""
        try:
            # 캐시 확인 (30분마다 갱신)
            now = time.time()
            if hasattr(self, '_news_cache') and hasattr(self, '_news_cache_time'):
                if now - self._news_cache_time < 1800:
                    return self._news_cache.get(coin, 50)

            # RSS 피드에서 뉴스 + SNS 수집
            feeds = [
                "https://cointelegraph.com/rss",
                "https://decrypt.co/feed",
                "https://www.reddit.com/r/CryptoCurrency/.rss",
                "https://www.reddit.com/r/Bitcoin/.rss",
            ]
            all_entries = []
            for feed_url in feeds:
                try:
                    resp = req_lib.get(feed_url, timeout=10,
                                       headers={"User-Agent": "Mozilla/5.0"})
                    feed = feedparser.parse(resp.text)
                    all_entries.extend(feed.entries[:20])
                except:
                    continue

            # 코인별 별칭
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
            }
            aliases = coin_aliases.get(coin, [coin.lower()])

            # 긍정/부정 키워드
            positive = ["surge", "rally", "bullish", "soar", "jump", "gain", "high",
                        "breakout", "pump", "moon", "buy", "adopt", "approve", "launch",
                        "partnership", "upgrade", "etf", "institutional",
                        "상승", "급등", "돌파", "호재", "매수", "승인"]
            negative = ["crash", "dump", "bearish", "plunge", "drop", "fall", "low",
                        "ban", "hack", "fraud", "sell", "fear", "risk", "scam",
                        "lawsuit", "sec", "regulation", "delay", "exploit",
                        "하락", "급락", "폭락", "악재", "매도", "규제", "소송"]

            # 인플루언서 키워드 (일론 머스크, 트럼프 등)
            influencer_positive = ["elon", "musk", "tesla", "trump", "strategic reserve",
                                   "일론", "머스크", "트럼프", "테슬라"]
            influencer_boost = 0  # 인플루언서 언급 보너스

            pos_count = 0
            neg_count = 0
            mention_count = 0

            for entry in all_entries:
                text = (entry.get("title", "") + " " + entry.get("summary", "")).lower()

                # 인플루언서가 코인 관련 언급했는지 체크 (전체 뉴스 대상)
                has_influencer = any(inf in text for inf in influencer_positive)
                has_coin_ref = any(alias in text for alias in aliases)
                has_crypto_ref = any(w in text for w in ["crypto", "bitcoin", "암호화폐"])

                if has_influencer and (has_coin_ref or has_crypto_ref):
                    # 긍정적 맥락인지 확인
                    inf_pos = sum(1 for w in positive if w in text)
                    inf_neg = sum(1 for w in negative if w in text)
                    if inf_pos > inf_neg:
                        influencer_boost = min(influencer_boost + 15, 25)
                    elif inf_neg > inf_pos:
                        influencer_boost = min(influencer_boost - 10, 0)

                # 코인 관련 뉴스 센티먼트
                if not has_coin_ref:
                    continue
                mention_count += 1
                pos_count += sum(1 for w in positive if w in text)
                neg_count += sum(1 for w in negative if w in text)

            # 점수 계산
            if mention_count == 0:
                score = 50
            else:
                total_signals = pos_count + neg_count
                if total_signals == 0:
                    score = 50
                else:
                    ratio = pos_count / total_signals
                    score = int(ratio * 100)
                # 언급량 보너스
                score = min(100, score + min(mention_count * 2, 10))

            # 인플루언서 보너스 적용
            score = max(0, min(100, score + influencer_boost))

            if influencer_boost != 0:
                print(f"  📢 {coin} 인플루언서 감지 (보너스: {influencer_boost:+d}점)")

            # 캐시 저장
            if not hasattr(self, '_news_cache'):
                self._news_cache = {}
            self._news_cache[coin] = score
            self._news_cache_time = now

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
                        result["score"] += 30  # 매수 압도적 → 세력 매집 가능
                        result["whale"] = True
                    elif ratio >= 2.0:
                        result["score"] += 20
                    elif ratio >= 1.2:
                        result["score"] += 10
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
                        result["score"] += 25  # 5배 이상 → 세력 개입 의심
                        result["whale"] = True
                    elif vol_ratio >= 3.0:
                        result["score"] += 15

            # 3) 가격-거래량 괴리 (가격 변화 없이 거래량만 폭증 → 매집)
            if ohlcv is not None and len(ohlcv) >= 2:
                closes = list(ohlcv['close'])
                price_change = abs(closes[-1] - closes[-2]) / closes[-2]
                if price_change < 0.005 and vol_ratio >= 3.0:
                    result["score"] += 20  # 가격 안 움직이는데 거래량 폭증
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

            # 거래량 급감 (최근 1분 거래량이 평균의 10% 미만)
            avg_vol = np.mean(volumes[:-1]) if len(volumes) > 1 else 0
            if avg_vol > 0 and volumes[-1] < avg_vol * 0.1:
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

            valid = [s for s in scores if s is not None and not np.isnan(s)]
            return round(np.mean(valid), 1) if valid else 0
        except:
            return 0

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
                result = ("강세", 10)     # 미국 강세 → 크립토에 호재
            elif avg_change >= 0:
                result = ("보통", 0)
            elif avg_change >= -1.0:
                result = ("약세", -5)     # 미국 약세 → 주의
            else:
                result = ("급락", -10)    # 미국 급락 → 크립토 위험

            print(f"🇺🇸 미국시장: S&P500 {signals.get('S&P500', 0):+.2f}%, NASDAQ {signals.get('NASDAQ', 0):+.2f}% → {result[0]} ({result[1]:+d}점)")

            self._us_cache = result
            self._us_cache_time = now
            return result
        except Exception as e:
            print(f"⚠️ 미국시장 조회 실패: {e}")
            return ("조회실패", 0)

    def check_market_strength(self):
        """시장 전체 강도"""
        try:
            scores = []
            for coin in ["BTC", "ETH", "XRP"]:
                ticker = f"KRW-{coin}"
                ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=24)
                closes = [x for x in ohlcv['close']]
                change = (closes[-1] - closes[0]) / closes[0]
                scores.append(change)
            
            avg = np.mean(scores) if scores else 0
            
            if avg < -0.15:
                return "극약세", 90
            elif avg < -0.10:
                return "심약세", 85
            elif avg < -0.05:
                return "약세", 80
            elif avg < 0.03:
                return "보통", 70
            else:
                return "강세", 60
        except:
            return "보통", 70
    
    def _calculate_momentum_inner(self, coin):
        """종합 모멘텀 점수 내부 계산"""
        vol = self.analyze_volume(coin)
        rsi = self.analyze_rsi(coin)
        support = self.analyze_support_resistance(coin)
        price = self.analyze_price_change(coin)
        pattern = self.detect_patterns(coin)
        news = self.analyze_news_sentiment(coin)
        abnormal = self.detect_abnormal_trading(coin)

        # 가중치 합산 (GUIDE.md 스펙: 25/25/15/15/10/10)
        total = (
            vol     * 0.25 +   # 거래량 25%
            rsi     * 0.25 +   # RSI 25%
            support * 0.15 +   # 지지선 15%
            price   * 0.15 +   # 가격 상승률 15%
            pattern * 0.10 +   # 패턴 10%
            news    * 0.10     # 뉴스/SNS 10%
        )

        # 이상 거래 보너스/페널티 적용
        total += abnormal["score"]
        if abnormal["whale"]:
            print(f"  🐋 {coin} 세력 매집 감지 (+{abnormal['score']}점)")
        if abnormal["manipulation"]:
            print(f"  ⚠️ {coin} 투매/조작 의심 ({abnormal['score']}점)")

        return round(min(100, max(0, total)), 1)

    def calculate_momentum(self, coin):
        """종합 모멘텀 점수 (0~100점) - 15초 타임아웃 보호"""
        result, err = _run_with_timeout(
            lambda: self._calculate_momentum_inner(coin), timeout=15
        )
        if err or result is None:
            return 0
        return result
    
    # ============================================
    # 2. 코인 선택 (카테고리 다른 것)
    # ============================================
    
    def select_coins(self):
        """전체 마켓 스캔 후 모멘텀 상위 코인 선택"""
        print(f"🔍 전체 {len(self.coins)}개 코인 스캔 중...")
        sys.stdout.flush()
        scores = {}
        skip_count = 0
        for i, coin in enumerate(self.coins):
            try:
                score = self.calculate_momentum(coin)
                if score is not None:
                    scores[coin] = score
            except Exception:
                skip_count += 1
            if (i + 1) % 20 == 0:
                msg = f"  ... {i+1}/{len(self.coins)} 스캔 완료"
                if skip_count > 0:
                    msg += f" (스킵 {skip_count}건)"
                print(msg)
                sys.stdout.flush()
            time.sleep(0.1)

        sorted_coins = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        # 상위 10개 출력
        print("\n📊 모멘텀 TOP 10:")
        for rank, (coin, score) in enumerate(sorted_coins[:10], 1):
            print(f"  {rank}. {coin}: {score}점")
        print()

        # 점수가 0보다 큰 코인만 반환 (최대 10개)
        selected = [coin for coin, score in sorted_coins[:10] if score > 0]
        return selected
    
    # ============================================
    # 3. 자동 매수/매도
    # ============================================
    
    MAX_POSITIONS = 2  # 최대 동시 보유 코인 수

    def place_buy_order(self, coin, amount):
        """매수 주문 (최대 2종목 제한 포함)"""
        try:
            # ★ 텔레그램 /pause 상태면 매수 차단
            if getattr(self, '_tg_paused', False):
                print(f"⏸ {coin} 매수 차단: 일시중지 상태 (/resume으로 재개)")
                return False

            # ★ 최대 포지션 제한 (이미 보유 중인 코인의 추가 배치는 허용)
            if coin not in self.positions and len(self.positions) >= self.MAX_POSITIONS:
                print(f"🚫 {coin} 매수 차단: 이미 {len(self.positions)}종목 보유 (최대 {self.MAX_POSITIONS}종목)")
                return False

            # ★ 투자유의/거래종료 종목 절대 금지
            if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                print(f"🚫 {coin} 매수 차단: 투자유의/거래종료 예정 종목")
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

            if coin not in self.positions:
                self.positions[coin] = {}

            batch_id = f"batch_{len(self.positions[coin]) + 1}"
            self.positions[coin][batch_id] = {
                'buy_price': price,
                'quantity': quantity,
                'amount': amount,
                'timestamp': datetime.now().isoformat()
            }

            print(f"✅ [매수] {coin} {amount:,}원 @ {price:,.0f}원")
            self._db_log_trade(coin, "buy", price, quantity, amount, batch=batch_id)
            self._save_positions()
            self._notify(f"[BUY] {coin} {batch_id} | {amount:,}원 @ {price:,.0f}원 | 잔고: {self.current_balance:,.0f}원")
            return True
        except Exception as e:
            print(f"❌ 매수 오류: {e}")
            self._notify(f"[ERROR] {coin} 매수 오류: {e}")
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
            self._db_log_trade(coin, "sell", price, quantity, sell_amount,
                               profit=profit, profit_rate=profit_rate, batch=batch_id)

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
        
        # 포지션 모니터링
        self.monitor_positions()

        # 2차 분할매수 처리 (5분 경과된 것)
        for coin in list(self._pending_2nd_buy.keys()):
            entry = self._pending_2nd_buy[coin]
            if time.time() - entry['time'] >= entry['wait']:
                print(f"📥 {coin} 2차 분할매수 실행 ({entry['amount']:,}원)")
                self.place_buy_order(coin, entry['amount'])
                del self._pending_2nd_buy[coin]

        # 시장 강도 확인
        market_strength, min_score = self.check_market_strength()
        self.last_market_state = market_strength

        # 미국 시장 연동
        us_state, us_adjust = self.check_us_market()
        min_score = max(0, min(100, min_score + us_adjust))

        print(f"📈 시장 상태: {market_strength} | 미국: {us_state} → 최소 신호: {min_score}점")

        # 극약세: 신규 매수 완전 중단 (기존 포지션 모니터링만)
        if market_strength == "극약세":
            print("🚫 극약세 시장 → 신규 매수 중단, 포지션 모니터링만 수행")
            return

        # 코인 선택
        selected = self.select_coins()
        print(f"🎯 선택된 코인: {selected}\n")
        
        # 각 코인 처리 (매매 한도 = 초기 50만 + 누적 수익)
        BASE_BUDGET = 500000
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
        per_coin_budget = int(MAX_TRADING_BUDGET // self.MAX_POSITIONS)  # 코인당 기본: 반반(50%)
        print(f"📊 매매 한도: {MAX_TRADING_BUDGET:,}원 (기본 50만+수익) | 투자중: {invested:,.0f}원 | 가용: {available:,.0f}원")
        print(f"📊 포지션: {current_positions}/{self.MAX_POSITIONS} | 코인당: {per_coin_budget:,}원 (분할 {per_coin_budget//2:,}원 × 2회)")

        if remaining_slots <= 0:
            print("📌 최대 포지션(2개) 도달 → 신규 매수 없음, 포지션 모니터링만")

        for coin in selected:
            if remaining_slots <= 0:
                break

            # 이미 보유 중인 코인 스킵
            if coin in self.positions:
                continue

            # 투자유의/거래종료 예정 종목 절대 금지
            if hasattr(self, '_warning_coins') and coin in self._warning_coins:
                print(f"🚫 {coin} 투자유의/거래종료 예정 → 절대 매수 금지")
                continue

            # 가용 금액 부족 체크
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

            # 고점 진입 방지
            if momentum >= min_score and self._is_near_high(coin):
                print(f" ⚠️ 고점 근접 → 매수 보류")
                continue

            if momentum >= min_score:
                # 고득점(85+) 과투자: 가용금액의 70%까지, 일반: 반반(50%)
                if momentum >= 85 and remaining_slots >= 1:
                    coin_budget = min(int(available * 0.7), int(MAX_TRADING_BUDGET * 0.7))
                    print(f" 🔥 고점수({momentum}점) 과투자!", end="")
                else:
                    coin_budget = min(per_coin_budget, available)

                buy_amount = int(coin_budget // 2)
                if buy_amount < 5000:
                    print(f" 💸 매수금액 부족 ({buy_amount:,}원) → 패스")
                    continue
                print(f" ✅ 매수 (1차: {buy_amount:,}원)")
                # 1차 매수 (코인당 할당의 50%)
                self.place_buy_order(coin, buy_amount)
                available -= buy_amount
                remaining_slots -= 1
                # 2차 매수는 5분 후 별도 처리 (저점 매입)
                if coin not in self._pending_2nd_buy:
                    self._pending_2nd_buy[coin] = {
                        'amount': buy_amount,
                        'time': time.time(),
                        'wait': 300  # 5분 대기
                    }
            else:
                print(" ❌ 패스")
    
    def _is_uptrend(self, coin):
        """현재 상승 추세인지 판단"""
        try:
            ticker = f"KRW-{coin}"
            ohlcv = pyupbit.get_ohlcv(ticker, interval="minute30", count=6)
            if ohlcv is None or len(ohlcv) < 4:
                return False
            closes = list(ohlcv['close'])
            # 최근 3봉이 연속 상승이면 상승 추세
            if closes[-1] > closes[-2] > closes[-3]:
                return True
            # MA5가 우상향이면 상승 추세
            if len(closes) >= 5:
                ma = np.mean(closes[-5:])
                if closes[-1] > ma and closes[-1] > closes[-3]:
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
                    if mom > current_momentum + 15 and not self._is_near_high(coin):
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
                continue

            for batch_id in list(self.positions[coin].keys()):
                position = self.positions[coin][batch_id]
                profit_rate = (price - position['buy_price']) / position['buy_price']
                hours = self._hours_held(position)
                momentum = self.calculate_momentum(coin)
                uptrend = self._is_uptrend(coin)

                # === 1. 모멘텀 급락 → 즉시 매도 (이익 있으면) ===
                if momentum < 30 and profit_rate > 0:
                    print(f"⚡ {coin} {batch_id} 모멘텀 급락({momentum}점) → 즉시 매도 ({profit_rate*100:+.2f}%)")
                    self.place_sell_order(coin, batch_id)
                    continue

                # === 2. 익절 (모멘텀 기반) ===
                if momentum >= 90:
                    target = 0.10
                elif momentum >= 70:
                    target = 0.08
                elif momentum >= 50:
                    target = 0.06
                else:
                    target = 0.05

                if profit_rate >= target:
                    print(f"💰 {coin} {batch_id} 익절 ({profit_rate*100:+.2f}% >= {target*100}%)")
                    self.place_sell_order(coin, batch_id)
                    continue

                # === 3. 손절 (시간 경과에 따라 기준 강화) ===
                # 1시간 이내: -5% 손절
                # 1~3시간: -3% 손절 (묶임 방지)
                # 3시간+: -2% 손절 (빠른 탈출)
                if hours >= 3:
                    stop_loss = -0.02
                elif hours >= 1:
                    stop_loss = -0.03
                else:
                    stop_loss = -0.05

                if profit_rate <= stop_loss:
                    print(f"🔻 {coin} {batch_id} 손절 ({profit_rate*100:+.2f}%, 기준 {stop_loss*100}%, {hours:.1f}시간 보유)")
                    self.place_sell_order(coin, batch_id)
                    continue

                # === 4. 횡보 감지 → 교체 (2시간 이상 보유 & 수익 ±1% 이내) ===
                if hours >= 2 and abs(profit_rate) < 0.01 and not uptrend:
                    better = self._find_better_coin(coin, momentum)
                    if better:
                        better_coin, better_mom = better
                        print(f"🔄 {coin} {batch_id} 횡보 {hours:.1f}시간 ({profit_rate*100:+.2f}%) → {better_coin}({better_mom}점)으로 교체")
                        if self.place_sell_order(coin, batch_id):
                            time.sleep(2)
                            self.place_buy_order(better_coin, position['amount'])
                        continue
                    else:
                        print(f"  ⏳ {coin} {batch_id} 횡보 {hours:.1f}시간, 더 좋은 대안 없음 → 유지")

                # === 5. 더 좋은 모멘텀 스왑 (보유 3시간+, 소폭 이익, 비상승추세) ===
                if hours >= 3 and 0 < profit_rate < 0.02 and not uptrend:
                    better = self._find_better_coin(coin, momentum)
                    if better:
                        better_coin, better_mom = better
                        print(f"🔀 {coin} {batch_id} 소폭 이익({profit_rate*100:+.2f}%) + 비상승 → {better_coin}({better_mom}점)으로 스왑")
                        if self.place_sell_order(coin, batch_id):
                            time.sleep(2)
                            self.place_buy_order(better_coin, position['amount'])
                        continue

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
        """로그 저장"""
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
        with open(filename, 'w', encoding='utf-8') as f:
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

        while True:
            try:
                now = time.time()

                # 뉴스 갱신 (30분마다)
                if now - last_news_refresh >= NEWS_INTERVAL:
                    print(f"\n📰 [{datetime.now().strftime('%H:%M:%S')}] 뉴스 갱신 중...")
                    # 캐시 초기화 → 다음 calculate_momentum에서 자동 갱신
                    if hasattr(bot, '_news_cache'):
                        del bot._news_cache
                    last_news_refresh = now

                # 시장 체크 (5분마다)
                if now - last_market_check >= MARKET_INTERVAL:
                    print(f"\n🌍 [{datetime.now().strftime('%H:%M:%S')}] 시장 상태 체크...")
                    strength, _ = bot.check_market_strength()
                    us_state, _ = bot.check_us_market()
                    last_market_check = now

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

                # 포지션 있으면 1분마다 모니터링
                if bot.positions:
                    bot.monitor_positions()

                time.sleep(60)  # 1분 간격으로 루프

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
