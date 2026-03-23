# 업비트 자동매매봇 v5.27 가이드

## 실행

```bash
cd /Users/600mac/Desktop/업비트자동

# 데모 모드 (가상 자금)
nohup ./venv/bin/python -u upbit_bot_v3_0_complete.py demo --auto > bot_output.log 2>&1 &

# 실전 모드
nohup ./venv/bin/python -u upbit_bot_v3_0_complete.py real --auto > bot_output.log 2>&1 &
```

**프로세스 반드시 1개만** — 여러 개 뜨면 중복 매매 발생.

```bash
# 프로세스 확인
ps aux | grep upbit_bot | grep -v grep

# 전부 죽이고 재시작
ps aux | grep upbit_bot | grep -v grep | awk '{print $2}' | xargs kill; sleep 2
nohup ./venv/bin/python -u upbit_bot_v3_0_complete.py demo --auto > bot_output.log 2>&1 &
```

---

## 매매 전략

### 1. Batch (일반 매매)

모멘텀 40점 이상 코인만 진입. 신중하게 고르고 오래 보유.

| 항목 | 설정 |
|------|------|
| 진입 기준 | 모멘텀 40+, 시그널 2개 이상 (50+는 1개) |
| AI 검증 | Bull/Bear 토론 (Opus) + 과거 교훈 (Haiku) |
| 최소 보유 | 15분 |
| 타임아웃 | 30분 (수익 0.3% 미만이면 탈출, 거래량 좋으면 예외) |
| 손절 | S/R 지지선 이탈 or -1.5~-3% |
| 익절 | S/R 저항선 or 트레일링 (+1.2%~+2% 시작) |
| 포지션 | 최대 2개, 분할매수 2회 |
| 재진입 | 당일 1패 코인 재진입 금지 |

**모멘텀 점수 구성 (100점):**
- 거래량 20% + RSI 20% + 지지/저항 13% + 가격변동 13%
- 패턴 15% + 뉴스 8% + 펀더멘털 3% + 이상거래 5% + PA 3%
- 패널티: 비상승추세 -15, 고점근처 -5

### 2. Surge (급등 퀵매매)

급등 감지 → 눌림목 진입 → 5-8분 보유 → 퀵엑싯. 모멘텀 점수 미사용.

| 항목 | 설정 |
|------|------|
| 감지 조건 | **3조건**: 5분봉 양봉 + 거래량 2배+ + 10분 +2%+ |
| 거래대금 | 10억 이상 |
| 진입 | 눌림목 대기 (-1%~-2.5% 하락 시) + 1분봉 양봉 확인 |
| 최소 보유 | **5분** (스위트스팟: 5-8분) |
| 손절 | -0.7% (거래량 살아있으면 유예 2회) / -1.5% 절대 |
| 익절 | +1% 고점-0.3% 퀵엑싯 / +0.5% 음봉 보호 |
| 타임아웃 | 10분 (거래량 좋으면 15분) / 15분 절대 |
| 재진입 | 매도가 -0.5%+ 눌림 + 거래량 1.5배+ 시 (최대 3회) |
| 포지션 | **1개만** |

**핵심 규칙:**
- 첫 진입 손실 → 그 코인 다회 진입 금지 (진짜 급등 아님)
- 첫 진입 수익 → 등락 반복 수익화 (최대 3회)
- 매도가보다 비싸면 재진입 안 함 (떨어질 때만)
- 거래량 빠지면 즉시 컷 (급등 끝남)

### 3. 공통

- 심야(23~06시): 매수 완전 차단, 포지션 관리만
- AI 멀티에이전트: 토론 Claude Opus 4.6 / 반성 Claude Haiku 4.5
- AutoTune: 1시간마다 승패 분석 → 가중치/블랙리스트 자동 조정
- 수수료 구간(0.1% 이하) 매도 방지

---

## 텔레그램 명령어

```
/status    현재 포지션 및 수익률
/report    일간/주간 리포트
/sell 코인  특정 코인 즉시 매도
/sellall   전체 매도
/pause     매수 일시중지
/resume    매수 재개
/autotune  AutoTune 상태/수동 실행
```

---

## 모니터링

```bash
# 실시간 로그
tail -f bot_output.log

# 최근 거래 확인
tail -50 bot_output.log | grep -E "매수|매도|SURGE"

# DB 직접 조회
./venv/bin/python -c "
import sqlite3
conn = sqlite3.connect('trading_bot_v3.db')
for r in conn.execute('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10').fetchall():
    print(r)
"
```

**대시보드:** GitHub Pages 자동 배포 (5분마다 갱신)

---

## 데이터 기반 설계 근거 (14일 748건 분석)

| 발견 | 데이터 | 적용 |
|------|--------|------|
| 모멘텀 40+ = 유일한 엣지 | 92건 +686k vs 전체 543건 +690k | batch 40+ 필터 |
| 1패 반복진입 = 최대 손실원 | CFG -306k 등 64건 | 당일 1패 재진입 금지 |
| surge 5-8분 = 최대 수익 | 43건 +100만원 | 5분 최소보유 |
| surge 0-3분 손절 = 최대 손실 | 34건 -120만원 | 빠른 컷 -0.7% |
| 손절 후 버텨도 75% 추가손실 | 회복 5건 vs 추가손실 15건 | 거래량 없으면 즉시 컷 |
| 수수료 구간 매도 = 실질 손실 | 179건 -38만원 | 0.1% 이하 매도 방지 |

---

## 파일 구조

```
upbit_bot_v3_0_complete.py  # 메인 봇 (5400줄)
ai_agents.py                # AI 멀티에이전트 (반성+토론)
config.json                 # API 키 설정
trading_bot_v3.db           # 거래/스냅샷/교훈 DB
dashboard.py                # GitHub Pages 대시보드 생성
export_status.py            # 상태 JSON 내보내기
watchdog.sh                 # 프로세스 감시
```
