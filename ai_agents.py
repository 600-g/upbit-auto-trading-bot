"""AI 멀티에이전트 시스템 — 반성학습 + Bull/Bear 토론 + 지표 해석
v1.0: TradingAgents 참고, 업비트 봇 애드온 구조
"""

import json
import time
import re
import threading
import sqlite3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout


class LLMClient:
    """Claude Haiku API 래퍼 — 타임아웃/비용 추적/일일 한도"""

    def __init__(self, api_key, model="claude-haiku-4-5-20251001", timeout=10, daily_limit=200):
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.daily_limit = daily_limit
        self._daily_calls = 0
        self._daily_tokens = 0
        self._daily_reset = datetime.now().date()
        self._client = None
        self._init_client()

    def _init_client(self):
        try:
            import anthropic
            self._client = anthropic.Anthropic(api_key=self.api_key)
        except ImportError:
            print("⚠️ [AI] anthropic SDK 미설치 — pip install anthropic")
            self._client = None
        except Exception as e:
            print(f"⚠️ [AI] Anthropic 클라이언트 초기화 실패: {e}")
            self._client = None

    def call(self, system_prompt, user_prompt, max_tokens=500):
        """LLM 호출. 실패 시 None 반환."""
        if not self._client:
            return None

        # 일일 리셋
        today = datetime.now().date()
        if today != self._daily_reset:
            self._daily_calls = 0
            self._daily_tokens = 0
            self._daily_reset = today

        if self._daily_calls >= self.daily_limit:
            return None

        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                timeout=self.timeout
            )
            self._daily_calls += 1
            if response.usage:
                self._daily_tokens += response.usage.input_tokens + response.usage.output_tokens
            return response.content[0].text
        except Exception as e:
            print(f"⚠️ [AI] LLM 호출 실패: {e}")
            return None

    @property
    def stats(self):
        return {
            'calls': self._daily_calls,
            'tokens': self._daily_tokens,
            'limit': self.daily_limit,
            'cost_usd': round(self._daily_tokens * 0.000001, 4)  # Haiku 근사
        }


class ReflectionAgent:
    """매도 후 거래 복기 → 교훈 DB 저장 → BM25 유사 상황 검색"""

    def __init__(self, llm_client, db_path, db_lock):
        self.llm = llm_client
        self.db_path = db_path
        self.db_lock = db_lock
        self._bm25_cache = None
        self._bm25_docs = []
        self._bm25_updated = 0

    def reflect_on_trade(self, trade_data):
        """매도 후 LLM이 거래 복기. 교훈을 DB에 저장."""
        prompt = self._build_prompt(trade_data)
        system = "당신은 암호화폐 트레이딩 코치입니다. 매매 결과를 분석하고 짧고 구체적인 교훈을 도출하세요. 반드시 JSON으로만 응답하세요."

        response = self.llm.call(system, prompt)
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
            if rsi: query += f" RSI{int(rsi)}"
            if vol: query += f" 거래량{int(vol)}"

        tokens = self._tokenize(query)
        if not tokens:
            return []

        try:
            scores = self._bm25_cache.get_scores(tokens)
            top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
            results = []
            for idx in top_indices:
                if scores[idx] > 0 and idx < len(self._bm25_docs):
                    results.append(self._bm25_docs[idx])
            return results
        except:
            return []

    def _build_prompt(self, td):
        result_emoji = "✅ 수익" if td.get('profit_rate', 0) > 0 else "❌ 손실" if td.get('profit_rate', 0) < 0 else "➖ 무승부"
        return f"""거래 결과 분석:
- 코인: {td.get('coin', '?')}
- 매수가: {td.get('buy_price', 0):,.0f}원 → 매도가: {td.get('sell_price', 0):,.0f}원
- 수익률: {td.get('profit_rate', 0):+.2f}% ({result_emoji})
- 보유시간: {td.get('hold_duration_hours', 0):.1f}시간
- 진입 모멘텀: {td.get('momentum_at_entry', 0)}점
- 시장 상태: {td.get('market_state', '보통')}
- 배치: {td.get('batch_id', '?')}

다음 JSON 형식으로만 응답:
{{"lesson": "1~2문장 핵심 교훈", "mistake_type": "timing|overchase|early_exit|ignore_trend|none", "severity": 1~5, "actionable_rule": "구체적 행동 규칙"}}"""

    def _parse_response(self, response):
        try:
            # JSON 블록 추출
            match = re.search(r'\{[^}]+\}', response, re.DOTALL)
            if match:
                return json.loads(match.group())
        except:
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
            print("⚠️ [AI] rank-bm25 미설치 — pip install rank-bm25")
        except Exception as e:
            print(f"⚠️ [AI] BM25 인덱스 구축 실패: {e}")

    def _tokenize(self, text):
        return re.findall(r'\b\w+\b', text.lower())


class DebateAgent:
    """Bull/Bear 토론 — 매수 전 반대의견 체크"""

    def __init__(self, llm_client):
        self.llm = llm_client

    def debate(self, coin, indicators, lessons=None):
        """매수 판단 토론. should_block=True면 매수 보류."""
        prompt = self._build_prompt(coin, indicators, lessons)
        system = "당신은 암호화폐 투자 분석가입니다. Bull(매수)과 Bear(매도) 양쪽 관점을 모두 제시하고 판정하세요. 반드시 JSON으로만 응답하세요."

        response = self.llm.call(system, prompt, max_tokens=600)
        if not response:
            return {'verdict': 'buy', 'confidence': 0.5, 'bear_severity': 0,
                    'summary': 'LLM 미응답', 'should_block': False}

        return self._parse_response(response)

    def _build_prompt(self, coin, indicators, lessons):
        ind_str = f"""RSI: {indicators.get('rsi', '?')}
거래량 점수: {indicators.get('volume_score', '?')}
모멘텀: {indicators.get('momentum', '?')}점
시장 상태: {indicators.get('market_state', '보통')}"""

        lesson_str = ""
        if lessons:
            lesson_str = "\n과거 교훈:\n" + "\n".join(
                f"- [{l.get('coin')}] {l.get('lesson')} (심각도 {l.get('severity', 0)})"
                for l in lessons[:3]
            )

        return f"""코인: {coin}
현재 지표:
{ind_str}
{lesson_str}

다음 형식으로 분석:
{{"bull": "매수해야 하는 이유 (2~3줄)", "bear": "매수하면 안 되는 이유 (2~3줄)", "verdict": "buy|hold|skip", "confidence": 0.0~1.0, "bear_severity": 1~10, "summary": "한줄 판정"}}"""

    def _parse_response(self, response):
        try:
            match = re.search(r'\{[^}]*\}', response, re.DOTALL)
            if match:
                data = json.loads(match.group())
                severity = int(data.get('bear_severity', 0))
                return {
                    'verdict': data.get('verdict', 'buy'),
                    'confidence': float(data.get('confidence', 0.5)),
                    'bear_severity': severity,
                    'summary': data.get('summary', ''),
                    'should_block': severity >= 7
                }
        except:
            pass
        return {'verdict': 'buy', 'confidence': 0.5, 'bear_severity': 0,
                'summary': '파싱 실패', 'should_block': False}


class AIAgents:
    """통합 인터페이스 — 봇에서 이것만 호출"""

    def __init__(self, db_path, db_lock, api_key=None, enabled=True):
        self.enabled = enabled and bool(api_key)
        self.db_path = db_path
        self.db_lock = db_lock
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ai_agent")

        if self.enabled:
            # TradingAgents 패턴: deep_think(판단) vs quick_think(복기)
            self.llm_deep = LLMClient(api_key, model="claude-opus-4-6", timeout=30)    # 매수 판단 = 돈
            self.llm_quick = LLMClient(api_key, model="claude-haiku-4-5-20251001", timeout=10)  # 복기 = 양
            self.reflection = ReflectionAgent(self.llm_quick, db_path, db_lock)
            self.debate = DebateAgent(self.llm_deep)
            print(f"🤖 [AI] 멀티에이전트 활성화 (토론: {self.llm_deep.model} / 반성: {self.llm_quick.model})")
        else:
            self.llm_deep = None
            self.llm_quick = None
            self.reflection = None
            self.debate = None
            print("🤖 [AI] 비활성 (API 키 없음)")

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
        """매수 직전 AI 검증. approved=False면 매수 보류."""
        if not self.enabled:
            return {'approved': True, 'block_reason': None, 'debate_summary': '', 'lessons_found': 0}

        try:
            # 1. 과거 교훈 검색
            lessons = self.reflection.search_lessons(coin, indicators) if self.reflection else []

            # 2. Bull/Bear 토론
            debate_result = self.debate.debate(coin, indicators, lessons) if self.debate else {}

            should_block = debate_result.get('should_block', False)
            summary = debate_result.get('summary', '')
            severity = debate_result.get('bear_severity', 0)

            # 3. 교훈에서 동일 코인 실패 패턴 체크
            coin_failures = [l for l in lessons if l.get('coin') == coin and l.get('severity', 0) >= 4]
            if len(coin_failures) >= 2:
                should_block = True
                summary += f" + 과거 고심각도 실패 {len(coin_failures)}건"

            if should_block:
                print(f"🤖 [AI 토론] {coin} 매수 차단 (Bear심각도:{severity}, {summary})")
            else:
                print(f"🤖 [AI 토론] {coin} 매수 허용 (Bear심각도:{severity}, {summary})")

            return {
                'approved': not should_block,
                'block_reason': summary if should_block else None,
                'debate_summary': summary,
                'lessons_found': len(lessons)
            }
        except Exception as e:
            print(f"⚠️ [AI] pre_buy_check 오류: {e}")
            return {'approved': True, 'block_reason': None, 'debate_summary': '', 'lessons_found': 0}

    def get_stats(self):
        if not self.llm_deep:
            return {'enabled': False}
        deep = self.llm_deep.stats
        quick = self.llm_quick.stats if self.llm_quick else {}
        stats = {
            'enabled': True,
            'debate': f"Opus {deep['calls']}회 ${deep['cost_usd']:.3f}",
            'reflection': f"Haiku {quick.get('calls',0)}회 ${quick.get('cost_usd',0):.4f}",
            'total_cost': round(deep['cost_usd'] + quick.get('cost_usd', 0), 4)
        }
        # 교훈 수
        try:
            with self.db_lock:
                conn = sqlite3.connect(self.db_path)
                count = conn.execute("SELECT COUNT(*) FROM trade_reflections").fetchone()[0]
                conn.close()
            stats['lessons'] = count
        except:
            stats['lessons'] = 0
        return stats

    def disable(self):
        self.enabled = False
        print("🤖 [AI] 비활성화")

    def enable(self):
        if self.llm_deep:
            self.enabled = True
            print("🤖 [AI] 활성화")
