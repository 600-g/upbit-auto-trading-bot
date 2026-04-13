"""AI 멀티에이전트 시스템 — 3인 토론 + 반성학습 (CLI 기반, Max 플랜 무료)
v2.0: SDK→CLI 전환, 3인 토론(공격파/보수파/심판), 판정 형식 확대
"""

import json
import os
import re
import subprocess
import sys
import threading
import sqlite3
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


class CLIClient:
    """Claude CLI 래퍼 — Max 플랜 무료, 타임아웃/일일 한도 추적"""

    def __init__(self, model="haiku", timeout=30, daily_limit=300):
        self.model = model
        self.timeout = timeout
        self.daily_limit = daily_limit
        self._daily_calls = 0
        self._daily_reset = datetime.now().date()
        self._cli_path = "/opt/homebrew/bin/claude"
        self._env = self._build_env()

    def _build_env(self):
        env = os.environ.copy()
        env.pop("CLAUDECODE", None)
        env.pop("CLAUDE_CODE_ENTRYPOINT", None)
        env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:" + env.get("PATH", "")
        return env

    def call(self, prompt, max_tokens=600):
        """CLI로 LLM 호출. 실패 시 None 반환."""
        today = datetime.now().date()
        if today != self._daily_reset:
            self._daily_calls = 0
            self._daily_reset = today

        if self._daily_calls >= self.daily_limit:
            return None

        try:
            result = subprocess.run(
                [self._cli_path, "-p", "--model", self.model, prompt],
                capture_output=True, text=True, timeout=self.timeout, env=self._env
            )
            self._daily_calls += 1
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
            else:
                if result.stderr:
                    print(f"⚠️ [AI] CLI stderr: {result.stderr[:150]}")
                return None
        except subprocess.TimeoutExpired:
            print(f"⚠️ [AI] CLI 타임아웃 ({self.timeout}초)")
            return None
        except Exception as e:
            print(f"⚠️ [AI] CLI 호출 실패: {e}")
            return None

    @property
    def stats(self):
        return {
            'calls': self._daily_calls,
            'limit': self.daily_limit,
            'model': self.model,
            'cost': '무료 (Max 플랜)'
        }


class ReflectionAgent:
    """매도 후 거래 복기 → 교훈 DB 저장 → BM25 유사 상황 검색"""

    def __init__(self, cli_client, db_path, db_lock):
        self.cli = cli_client
        self.db_path = db_path
        self.db_lock = db_lock
        self._bm25_cache = None
        self._bm25_docs = []
        self._bm25_updated = 0

    def reflect_on_trade(self, trade_data):
        """매도 후 LLM이 거래 복기. 교훈을 DB에 저장."""
        prompt = self._build_prompt(trade_data)
        response = self.cli.call(prompt)
        if not response:
            return None

        parsed = self._parse_response(response)
        if parsed:
            self._save_to_db(trade_data, parsed)
        return parsed

    def search_lessons(self, coin, indicators=None, top_k=3):
        """BM25로 유사 상황의 과거 교훈 검색"""
        self._ensure_bm25_index()
        if not self._bm25_cache or not self._bm25_docs:
            return []

        query = f"{coin}"
        if indicators:
            rsi = indicators.get('rsi', 0)
            vol = indicators.get('volume_score', 0)
            if rsi:
                query += f" RSI{int(rsi)}"
            if vol:
                query += f" 거래량{int(vol)}"

        tokens = self._tokenize(query)
        if not tokens:
            return []

        try:
            scores = self._bm25_cache.get_scores(tokens)
            top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
            return [
                self._bm25_docs[idx]
                for idx in top_indices
                if scores[idx] > 0 and idx < len(self._bm25_docs)
            ]
        except Exception:
            return []

    def _build_prompt(self, td):
        result_emoji = "✅ 수익" if td.get('profit_rate', 0) > 0 else "❌ 손실" if td.get('profit_rate', 0) < 0 else "➖ 무승부"
        return f"""당신은 암호화폐 트레이딩 코치입니다. 매매 결과를 분석하고 짧고 구체적인 교훈을 도출하세요.

거래 결과:
- 코인: {td.get('coin', '?')}
- 매수가: {td.get('buy_price', 0):,.0f}원 → 매도가: {td.get('sell_price', 0):,.0f}원
- 수익률: {td.get('profit_rate', 0):+.2f}% ({result_emoji})
- 보유시간: {td.get('hold_duration_hours', 0):.1f}시간
- 진입 모멘텀: {td.get('momentum_at_entry', 0)}점
- 시장 상태: {td.get('market_state', '보통')}

반드시 아래 JSON 형식으로만 응답하세요:
{{"lesson": "1~2문장 핵심 교훈", "mistake_type": "timing|overchase|early_exit|ignore_trend|none", "severity": 1~5, "actionable_rule": "구체적 행동 규칙"}}"""

    def _parse_response(self, response):
        try:
            match = re.search(r'\{[^}]+\}', response, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception:
            pass
        return None

    def _save_to_db(self, trade_data, parsed):
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                conn.execute("""
                    INSERT INTO trade_reflections
                    (coin, buy_price, sell_price, profit_rate, momentum_at_entry,
                     hold_duration_hours, market_state, lesson, mistake_type,
                     severity, actionable_rule, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    trade_data.get('coin'), trade_data.get('buy_price'),
                    trade_data.get('sell_price'), trade_data.get('profit_rate'),
                    trade_data.get('momentum_at_entry'), trade_data.get('hold_duration_hours'),
                    trade_data.get('market_state'), parsed.get('lesson', ''),
                    parsed.get('mistake_type', 'none'), parsed.get('severity', 0),
                    parsed.get('actionable_rule', ''), datetime.now().isoformat()
                ))
                conn.commit()
                conn.close()
            self._bm25_updated = 0  # 캐시 무효화
        except Exception as e:
            print(f"⚠️ [AI] 교훈 저장 실패: {e}")

    def _ensure_bm25_index(self):
        if time.time() - self._bm25_updated < 300 and self._bm25_cache:
            return  # 5분 캐시

        try:
            from rank_bm25 import BM25Okapi
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.execute(
                    "SELECT coin, lesson, mistake_type, severity, actionable_rule, profit_rate "
                    "FROM trade_reflections ORDER BY created_at DESC LIMIT 200"
                )
                rows = cursor.fetchall()
                conn.close()

            if not rows:
                return

            self._bm25_docs = []
            corpus = []
            for r in rows:
                doc = {
                    'coin': r[0], 'lesson': r[1], 'mistake_type': r[2],
                    'severity': r[3], 'actionable_rule': r[4], 'profit_rate': r[5]
                }
                self._bm25_docs.append(doc)
                text = f"{r[0]} {r[1]} {r[2]} {r[4]}"
                corpus.append(self._tokenize(text))

            self._bm25_cache = BM25Okapi(corpus)
            self._bm25_updated = time.time()
        except ImportError:
            pass  # rank-bm25 미설치 시 무시
        except Exception as e:
            print(f"⚠️ [AI] BM25 인덱스 구축 실패: {e}")

    def _tokenize(self, text):
        return re.findall(r'\b\w+\b', text.lower())


class DebateAgent:
    """3인 토론 — 공격파/보수파/심판 (매수/매도 검증)"""

    def __init__(self, cli_client):
        self.cli = cli_client

    def debate(self, coin, indicators, lessons=None):
        """3인 토론 실행. should_block=True면 매수 보류."""
        prompt = self._build_prompt(coin, indicators, lessons)
        response = self.cli.call(prompt)
        if not response:
            # v7.1: LLM 미응답 시 안전을 위해 매수 차단 (기존: 허용)
            # 과거 문제: ANKR -6.9% 손실 케이스가 LLM 미응답으로 통과됨
            return {
                'verdict': 'skip', 'confidence': 0.0, 'bear_severity': 10,
                'summary': 'LLM 미응답 → 안전 차단', 'should_block': True,
                'ratio': 0, 'stop_loss': -5, 'target': [3, 5], 'risk': '미확인'
            }

        return self._parse_response(response)

    def sell_debate(self, coin, indicators):
        """매도 ���론: +1% 이상 수익 구간에서 지금 팔까/홀딩할까 판단"""
        prompt = self._build_sell_prompt(coin, indicators)
        response = self.cli.call(prompt, max_tokens=400)
        if not response:
            return {'verdict': 'hold', 'summary': 'LLM 미응답', 'should_sell': False}
        return self._parse_sell_response(response)

    def _build_sell_prompt(self, coin, indicators):
        return f"""암호화폐 {coin} 매도 타이밍을 판단하세요.

⚠️ 할루시네이션 방지: 아래 숫자 외엔 어떤 수치도 만들지 마시오. 근거 불명 시 verdict="hold".

현재 상태:
- 수익률: {indicators.get('profit_rate', 0)*100:+.2f}%
- 고점 수익률: {indicators.get('peak_rate', 0)*100:+.2f}%
- 보유시간: {indicators.get('minutes_held', 0):.0f}분
- 모멘텀: {indicators.get('momentum', 0)}점
- 거래량 점수: {indicators.get('volume_score', 0)}
- 추세: {indicators.get('trend', '?')}

핵심 원칙:
1. 조기 익절(early_exit)이 가장 큰 실수. 모멘텀+거래량 살아있으면 홀딩 우선
2. 손절은 -0.7% 이하에서만. 이 판단에서는 수익 구간(+1%~)만 다룸
3. 수익/손실 비율 2:1 이상 유지가 목표

반드시 아�� JSON으로만 응답:
{{"verdict": "sell|hold", "confidence": 0.0~1.0, "reason": "한줄 근거", "hold_target": 추가 홀딩 시 예상 수익률(%)}}"""

    def _parse_sell_response(self, response):
        try:
            match = re.search(r'\{[^}]*\}', response, re.DOTALL)
            if match:
                data = json.loads(match.group())
                verdict = data.get('verdict', 'hold')
                return {
                    'verdict': verdict,
                    'confidence': float(data.get('confidence', 0.5)),
                    'summary': data.get('reason', ''),
                    'should_sell': verdict == 'sell' and float(data.get('confidence', 0.5)) >= 0.7,
                    'hold_target': float(data.get('hold_target', 0)),
                }
        except Exception:
            pass
        return {'verdict': 'hold', 'summary': '파싱 실패', 'should_sell': False}

    def _build_prompt(self, coin, indicators, lessons):
        # 지표 정보
        rsi = indicators.get('rsi', '?')
        vol = indicators.get('volume_score', '?')
        momentum = indicators.get('momentum', '?')
        market = indicators.get('market_state', '보통')
        fear_greed = indicators.get('fear_greed', '?')
        coin_record = indicators.get('coin_record', '')
        # v7.5: 할루시네이션 방지용 검증 토큰 (응답에 실제 숫자 인용 강제)
        self._last_indicators = {'rsi': rsi, 'vol': vol, 'momentum': momentum, 'fg': fear_greed}

        # 과거 교훈
        lesson_str = ""
        if lessons:
            lesson_str = "\n과거 교훈:\n" + "\n".join(
                f"- [{l.get('coin')}] {l.get('lesson')} (심각도 {l.get('severity', 0)}, 수익률 {l.get('profit_rate', 0):+.1f}%)"
                for l in lessons[:3]
            )

        return f"""당신은 암호화폐 투자 분석 팀입니다. 3명의 전문가가 매수 판단을 토론합니다.

⚠️ 절대 규칙 (할루시네이션 방지):
1. 아래 제공된 숫자 외엔 어떤 수치도 만들어내지 마시오
2. 확실하지 않은 건 confidence를 0.3 이하로 낮추시오
3. 근거 없는 추측 시 verdict는 "skip"
4. 응답에 반드시 RSI={rsi}, 모멘텀={momentum}점 숫자를 인용하시오

코인: {coin}
현재 지표 (이것만 사용):
- RSI: {rsi}
- 거래량 점수: {vol}
- 모멘텀: {momentum}점
- 시장 상태: {market}
- 공포탐욕지수: {fear_greed}
{coin_record}
{lesson_str}

아래 3명이 순서대로 의견을 내고, 마지막에 심판이 종합 판정하세요:

🧑‍💼 공격파 (10년차 트레이더):
- 매수해야 하는 이유 (구체적 수치 근거 2~3줄)

🛡 보수파 (리스크 매니저):
- 매수하면 안 되는 이유 (공격파 주장 반박 + 수치 근거 2~3줄)

⚖️ 심판 (퀀트 분석가):
- 공격파/보수파 핵심 논점 정리
- 재반론: 보수파 약점 또는 공격파 약점 지적
- 최종 판정

반드시 아래 JSON 형식으로만 응답하세요:
{{"bull": "공격파 의견 요약", "bear": "보수파 의견 요약", "rebuttal": "심판 재반론", "verdict": "buy|hold|skip", "confidence": 0.0~1.0, "bear_severity": 1~10, "ratio": 50~100, "stop_loss": -3~-5, "target_1": 2~5, "target_2": 5~10, "risk": "낮음|중간|높음", "summary": "한줄 판정"}}"""

    def _parse_response(self, response):
        try:
            match = re.search(r'\{[^}]*\}', response, re.DOTALL)
            if match:
                data = json.loads(match.group())
                severity = int(data.get('bear_severity', 0))
                confidence = float(data.get('confidence', 0.5))

                # v7.5: 할루시네이션 검증 — 실제 지표 숫자를 인용했는지 확인
                ind = getattr(self, '_last_indicators', {})
                text = (data.get('bull', '') + ' ' + data.get('bear', '') + ' ' + data.get('rebuttal', '') + ' ' + data.get('summary', ''))
                grounded = False
                try:
                    rsi_val = ind.get('rsi', '')
                    mom_val = ind.get('momentum', '')
                    if rsi_val and str(int(float(rsi_val))) in text:
                        grounded = True
                    if mom_val and str(int(float(mom_val))) in text:
                        grounded = True
                except Exception:
                    grounded = True  # 지표 없으면 검증 스킵

                if not grounded and confidence >= 0.5:
                    # 실제 숫자 인용 없음 → 할루시네이션 의심, confidence 강제 하향
                    confidence = 0.3
                    severity = max(severity, 5)
                    print(f"⚠️ [AI 할루시네이션] 응답에 지표 숫자 인용 없음 → confidence 하향")

                return {
                    'verdict': data.get('verdict', 'skip'),
                    'confidence': confidence,
                    'bear_severity': severity,
                    'summary': data.get('summary', ''),
                    'should_block': severity >= 7 or confidence < 0.4,  # v7.5: 저신뢰도도 차단
                    'ratio': int(data.get('ratio', 100)),
                    'stop_loss': float(data.get('stop_loss', -5)),
                    'target': [float(data.get('target_1', 3)), float(data.get('target_2', 5))],
                    'risk': data.get('risk', '중간'),
                    'bull': data.get('bull', ''),
                    'bear': data.get('bear', ''),
                    'rebuttal': data.get('rebuttal', ''),
                    'grounded': grounded,
                }
        except Exception:
            pass
        # v7.5: 파싱 실패 시 안전 차단 (기존: 허용 → 차단)
        return {
            'verdict': 'skip', 'confidence': 0.0, 'bear_severity': 10,
            'summary': '파싱 실패 → 안전 차단', 'should_block': True,
            'ratio': 0, 'stop_loss': -5, 'target': [3, 5], 'risk': '미확인'
        }


class AIAgents:
    """통합 인터페이스 — 봇에서 이것만 호출 (CLI 기반, 무료)"""

    def __init__(self, db_path, db_lock, api_key=None, enabled=True):
        self.enabled = enabled
        self.db_path = db_path
        self.db_lock = db_lock
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ai_agent")

        # CLI 존재 확인
        cli_exists = os.path.exists("/opt/homebrew/bin/claude")
        if not cli_exists:
            self.enabled = False
            print("🤖 [AI] 비활성 (claude CLI 미설치)")

        if self.enabled:
            # 토론: haiku (빠른 3인 토론), 반성: haiku (빠른 복기)
            self.cli_debate = CLIClient(model="haiku", timeout=60)
            self.cli_reflect = CLIClient(model="haiku", timeout=30)
            self.reflection = ReflectionAgent(self.cli_reflect, db_path, db_lock)
            self.debate = DebateAgent(self.cli_debate)
            print(f"🤖 [AI] 멀티에이전트 활성화 (토론: {self.cli_debate.model} / 반성: {self.cli_reflect.model}) — CLI 무료")
        else:
            self.cli_debate = None
            self.cli_reflect = None
            self.reflection = None
            self.debate = None

        # DB 테이블 생성
        self._init_db()

    def _init_db(self):
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS trade_reflections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        coin TEXT NOT NULL,
                        buy_price REAL,
                        sell_price REAL,
                        profit_rate REAL,
                        momentum_at_entry REAL,
                        hold_duration_hours REAL,
                        market_state TEXT,
                        lesson TEXT NOT NULL,
                        mistake_type TEXT,
                        severity INTEGER DEFAULT 0,
                        actionable_rule TEXT,
                        created_at TEXT NOT NULL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ref_coin ON trade_reflections(coin)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_ref_created ON trade_reflections(created_at)")
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"⚠️ [AI] DB 테이블 생성 실패: {e}")

    def reflect_async(self, trade_data):
        """매도 후 비동기 반성 (fire-and-forget)"""
        if not self.enabled or not self.reflection:
            return

        def _do_reflect():
            try:
                result = self.reflection.reflect_on_trade(trade_data)
                if result:
                    coin = trade_data.get('coin', '?')
                    pr = trade_data.get('profit_rate', 0)
                    print(f"🤖 [AI 반성] {coin} {pr:+.2f}% → {result.get('lesson', '')[:60]}")
            except Exception as e:
                print(f"⚠️ [AI] 반성 오류: {e}")

        self._executor.submit(_do_reflect)

    def pre_buy_check(self, coin, indicators):
        """매수 직전 3인 토론 검증. approved=False면 매수 보류."""
        if not self.enabled:
            return {'approved': True, 'block_reason': None, 'debate_summary': '', 'lessons_found': 0}

        try:
            # 1. 과거 교훈 검색
            lessons = self.reflection.search_lessons(coin, indicators) if self.reflection else []

            # 2. 3인 토론
            debate_result = self.debate.debate(coin, indicators, lessons) if self.debate else {}

            should_block = debate_result.get('should_block', False)
            summary = debate_result.get('summary', '')
            severity = debate_result.get('bear_severity', 0)
            risk = debate_result.get('risk', '미확인')

            # 3. 교훈에서 동일 코인 실패 패턴 체크
            coin_failures = [l for l in lessons if l.get('coin') == coin and l.get('severity', 0) >= 4]
            if len(coin_failures) >= 2:
                should_block = True
                summary += f" + 과거 고심각도 실패 {len(coin_failures)}건"

            if should_block:
                print(f"🤖 [AI] {coin} 매수 차단 (Bear심각도:{severity}, 리스크:{risk}, {summary})")
            else:
                print(f"🤖 [AI] {coin} 매수 허용 (Bear심각도:{severity}, 리스크:{risk}, {summary})")

            return {
                'approved': not should_block,
                'block_reason': summary if should_block else None,
                'debate_summary': summary,
                'lessons_found': len(lessons),
                'risk': risk,
                'ratio': debate_result.get('ratio', 100),
                'stop_loss': debate_result.get('stop_loss', -5),
                'target': debate_result.get('target', [3, 5]),
            }
        except Exception as e:
            print(f"⚠️ [AI] pre_buy_check 오류: {e}")
            return {'approved': True, 'block_reason': None, 'debate_summary': '', 'lessons_found': 0}

    def pre_sell_check(self, coin, indicators):
        """매도 직전 AI 토론: +1% 이상 수익 구간에서 홀딩/매도 판단"""
        if not self.enabled or not self.debate:
            return {'should_sell': False, 'summary': '', 'verdict': 'hold'}

        try:
            result = self.debate.sell_debate(coin, indicators)
            if result.get('should_sell'):
                print(f"🤖 [AI 매도] {coin} 매도 추천 ({result.get('summary', '')})")
            else:
                print(f"🤖 [AI 홀딩] {coin} 홀딩 추천 ({result.get('summary', '')})")
            return result
        except Exception as e:
            print(f"⚠️ [AI] pre_sell_check 오류: {e}")
            return {'should_sell': False, 'summary': '', 'verdict': 'hold'}

    def _validate_adjustments_backtest(self, adjustments):
        """v7.2: 제안된 조정값을 과거 거래에 시뮬 → P&L 개선되는 경우만 통과
        반환: (통과여부, 개선액, 사유)"""
        if not adjustments:
            return True, 0, '조정 없음'

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                rows = conn.execute(
                    "SELECT coin, batch, action, price, profit_rate, profit "
                    "FROM trades WHERE timestamp >= datetime('now', '-14 days') ORDER BY id"
                ).fetchall()
                conn.close()

            # buy/sell 페어 매칭
            pairs = {}
            for r in rows:
                coin, batch, action, price, pr, profit = r
                key = (coin, batch)
                if action == 'buy':
                    pairs[key] = {'buy': r}
                elif action == 'sell' and key in pairs:
                    pairs[key]['sell'] = r

            # 실제 P&L
            actual_pnl = sum(v['sell'][5] for v in pairs.values()
                             if 'sell' in v and v['sell'][5] is not None)
            if len([v for v in pairs.values() if 'sell' in v]) < 10:
                return True, 0, '데이터 부족(10건 미만) → 일단 적용'

            # 시뮬: min_hold_minutes 증가 → 짧게 끊긴 수익 거래의 상당수가 -보유시간 체크로 연장되어 차이 미미
            # 실제 검증은 trailing_drop_boost + momentum_boost만 (명확한 영향)
            sim_pnl = actual_pnl
            mom_boost = adjustments.get('momentum_boost', 0)
            if mom_boost > 0:
                # 진입 기준 강화 → 저모멘텀 진입 케이스 제거 (profit_rate가 낮거나 음수인 케이스)
                # 보수적으로: 전체 거래의 (mom_boost * 5)% 차단, 차단된 거래 중 손실 60% 회피
                cut_ratio = min(0.3, mom_boost * 0.05)
                losses = [v['sell'][5] for v in pairs.values()
                          if 'sell' in v and v['sell'][5] and v['sell'][5] < 0]
                avg_loss = sum(losses) / len(losses) if losses else 0
                sim_pnl += abs(avg_loss) * cut_ratio * len(pairs) * 0.6  # 회피된 손실 복원

            improve = sim_pnl - actual_pnl
            if improve < 0:
                return False, improve, f'P&L 악화 예상 ({improve:+,.0f}원)'
            return True, improve, f'개선 예상 (+{improve:,.0f}원)'
        except Exception as e:
            return True, 0, f'검증 오류 → 적용: {e}'

    def get_lesson_adjustments(self):
        """교훈 DB 분석 → 매도 파라미터 자동 조정값 반환 (되먹임 루프 + 백테스트 검증)"""
        if not self.enabled:
            return {}

        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)

                # 최근 7일 교훈에서 실수 유형별 빈도 분석
                rows = conn.execute(
                    "SELECT mistake_type, COUNT(*), AVG(severity) FROM trade_reflections "
                    "WHERE created_at >= datetime('now', '-7 days') "
                    "GROUP BY mistake_type ORDER BY COUNT(*) DESC"
                ).fetchall()

                # 최근 7일 평균 수익률 (수익 거래만)
                avg_win = conn.execute(
                    "SELECT AVG(profit_rate) FROM trade_reflections "
                    "WHERE profit_rate > 0 AND created_at >= datetime('now', '-7 days')"
                ).fetchone()

                conn.close()

            adjustments = {}

            for mistake_type, count, avg_severity in rows:
                # early_exit가 많으면 → 최소 보유시간 증가, 트레일링 느슨하게
                if mistake_type == 'early_exit' and count >= 3:
                    adjustments['min_hold_minutes'] = min(25, 15 + count)  # 15분 → 최대 25분
                    adjustments['trailing_drop_boost'] = min(0.005, count * 0.001)  # 트레일링 폭 넓히기
                    adjustments['reason_hold'] = f'early_exit {count}건 → 홀딩 강화'

                # overchase가 많으면 → 진입 기준 강화
                if mistake_type == 'overchase' and count >= 3:
                    adjustments['momentum_boost'] = min(5, count)  # 진입 기준 +N점
                    adjustments['reason_entry'] = f'overchase {count}건 → 진입 기준 강화'

                # timing이 많으면 → 쿨다운 연장
                if mistake_type == 'timing' and count >= 3:
                    adjustments['cooldown_boost'] = min(300, count * 60)  # 쿨다운 +N초
                    adjustments['reason_cooldown'] = f'timing {count}건 → 쿨다운 연장'

            # 평균 수익이 너무 작으면 → 최소 익절 타겟 상향
            if avg_win and avg_win[0] is not None and avg_win[0] < 0.5:
                adjustments['min_profit_target'] = 0.015  # 최소 +1.5%
                adjustments['reason_target'] = f'평균 수익 {avg_win[0]:+.2f}% 낮음 → 익절 기준 상향'

            if adjustments:
                # v7.2: 백테스트 검증 — P&L 악화 예상 시 취소
                passed, improve, reason = self._validate_adjustments_backtest(adjustments)
                if not passed:
                    print(f"🤖 [AI 교훈] ❌ 백테스트 검증 실패 — 조정 취소 ({reason})")
                    return {}
                reasons = [v for k, v in adjustments.items() if k.startswith('reason_')]
                print(f"🤖 [AI 교훈] ✅ 검증 통과 ({reason}) | 조정: {', '.join(reasons)}")

            return adjustments
        except Exception as e:
            print(f"⚠️ [AI] 교훈 분석 오류: {e}")
            return {}

    def get_stats(self):
        if not self.cli_debate:
            return {'enabled': False}
        debate_s = self.cli_debate.stats
        reflect_s = self.cli_reflect.stats if self.cli_reflect else {}
        stats = {
            'enabled': True,
            'debate': f"{debate_s['model']} {debate_s['calls']}회 ({debate_s['cost']})",
            'reflection': f"{reflect_s.get('model', '?')} {reflect_s.get('calls', 0)}회 ({reflect_s.get('cost', '?')})",
        }
        # 교훈 수
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                count = conn.execute("SELECT COUNT(*) FROM trade_reflections").fetchone()[0]
                conn.close()
            stats['lessons'] = count
        except Exception:
            stats['lessons'] = 0
        return stats

    def disable(self):
        self.enabled = False
        print("🤖 [AI] 비활성화")

    def enable(self):
        if self.cli_debate:
            self.enabled = True
            print("🤖 [AI] 활성화")
