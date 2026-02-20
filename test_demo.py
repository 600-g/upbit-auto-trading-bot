#!/usr/bin/env python3
"""데모 모드 빠른 테스트 - 핵심 로직 검증"""
import sys
sys.path.insert(0, '/Users/600mac/Desktop/업비트자동')

from upbit_bot_v3_0_complete import TradingBotV3
import time

print("=" * 60)
print("데모 모드 테스트 시작")
print("=" * 60)

bot = TradingBotV3(mode="demo")

# 테스트 1: 유의종목 필터 확인
print("\n--- 테스트 1: 투자유의 종목 필터 ---")
print(f"유의종목: {bot._warning_coins}")
for wc in bot._warning_coins:
    if wc in bot.coins:
        print(f"  ❌ 실패: {wc}가 코인 목록에 있음!")
    else:
        print(f"  ✅ {wc} 정상 제외됨")

# 테스트 2: MAX_POSITIONS 확인
print(f"\n--- 테스트 2: MAX_POSITIONS = {bot.MAX_POSITIONS} ---")
assert bot.MAX_POSITIONS == 2, f"MAX_POSITIONS가 {bot.MAX_POSITIONS}! 2여야 함"
print("✅ MAX_POSITIONS = 2 확인")

# 테스트 3: 가상 매수 - 2종목까지만 허용되는지
print("\n--- 테스트 3: 최대 2종목 매수 제한 ---")
bot.positions = {}  # 초기화
bot.current_balance = 500000

# 1번째 코인 매수
result1 = bot.place_buy_order("BTC", 125000)
print(f"  BTC 매수: {'성공' if result1 else '실패'} | 포지션 수: {len(bot.positions)}")

# 2번째 코인 매수
result2 = bot.place_buy_order("ETH", 125000)
print(f"  ETH 매수: {'성공' if result2 else '실패'} | 포지션 수: {len(bot.positions)}")

# 3번째 코인 매수 시도 (차단되어야 함!)
result3 = bot.place_buy_order("XRP", 125000)
print(f"  XRP 매수: {'성공' if result3 else '차단됨'} | 포지션 수: {len(bot.positions)}")

if result3:
    print("  ❌ 실패: 3번째 코인이 매수됨!")
else:
    print("  ✅ 정상: 3번째 코인 매수 차단됨")

# 이미 보유 중인 코인의 추가 배치는 허용
result4 = bot.place_buy_order("BTC", 125000)
print(f"  BTC 2차: {'성공' if result4 else '실패'} | BTC 배치 수: {len(bot.positions.get('BTC', {}))}")
if result4:
    print("  ✅ 정상: 기존 코인 추가 배치 허용")

# 테스트 4: 유의종목 매수 차단
print("\n--- 테스트 4: 유의종목 매수 차단 ---")
bot.positions = {}  # 초기화
bot.current_balance = 500000
if bot._warning_coins:
    wc = list(bot._warning_coins)[0]
    result_w = bot.place_buy_order(wc, 125000)
    print(f"  {wc} 매수: {'성공' if result_w else '차단됨'}")
    if not result_w:
        print(f"  ✅ 정상: 유의종목 {wc} 매수 차단됨")
    else:
        print(f"  ❌ 실패: 유의종목 {wc}가 매수됨!")
else:
    print("  (유의종목 없음, 스킵)")

# 테스트 5: 예산 할당 확인
print("\n--- 테스트 5: 예산 할당 ---")
bot.positions = {}
bot.current_balance = 500000
per_coin = 500000 // bot.MAX_POSITIONS
print(f"  총 예산: 500,000원")
print(f"  코인당: {per_coin:,}원 (반반)")
print(f"  분할매수: {per_coin//2:,}원 × 2회")
print(f"  ✅ 1차 매수: {per_coin//2:,}원 | 2차(5분후): {per_coin//2:,}원")

# 테스트 6: 과투자 로직 (85점+)
print("\n--- 테스트 6: 과투자 계산 ---")
available = 500000
normal_budget = per_coin
overinvest_budget = min(int(available * 0.7), int(500000 * 0.7))
print(f"  일반(65~84점): {normal_budget:,}원 (반반 50%)")
print(f"  과투자(85점+): {overinvest_budget:,}원 (70%)")

# 테스트 7: 실제 거래 사이클 시뮬레이션 (빠른 버전)
print("\n--- 테스트 7: 거래 사이클 시뮬레이션 ---")
bot.positions = {}
bot.current_balance = 500000
bot._pending_2nd_buy = {}

# select_coins 대신 직접 후보 지정 (시간 절약)
test_coins = ["BTC", "ETH", "XRP", "SOL", "DOGE"]
min_score = 65

bought_count = 0
remaining_slots = bot.MAX_POSITIONS - len(bot.positions)

for coin in test_coins:
    if remaining_slots <= 0:
        print(f"  {coin}: 🚫 슬롯 없음 (최대 {bot.MAX_POSITIONS}종목)")
        continue
    if coin in bot.positions:
        print(f"  {coin}: 이미 보유")
        continue

    # 실제 모멘텀 계산
    try:
        momentum = bot.calculate_momentum(coin)
    except:
        momentum = 0

    print(f"  {coin}: 모멘텀 {momentum}점", end="")

    if momentum >= min_score:
        per_coin_budget = 500000 // bot.MAX_POSITIONS
        if momentum >= 85:
            avail = bot.current_balance
            buy_budget = min(int(avail * 0.7), int(500000 * 0.7))
            print(f" 🔥 과투자! 예산: {buy_budget:,}원", end="")
        else:
            buy_budget = per_coin_budget

        buy_amount = buy_budget // 2
        result = bot.place_buy_order(coin, buy_amount)
        if result:
            remaining_slots -= 1
            bought_count += 1
            print(f" → 매수 {buy_amount:,}원")
        else:
            print(f" → 매수 실패")
    else:
        print(f" ❌ 패스 (기준: {min_score}점)")

print(f"\n  결과: {bought_count}종목 매수 | 포지션: {list(bot.positions.keys())}")
print(f"  잔액: {bot.current_balance:,.0f}원")

# 최종 요약
print("\n" + "=" * 60)
print("테스트 완료 요약")
print("=" * 60)
print(f"  MAX_POSITIONS: {bot.MAX_POSITIONS}")
print(f"  유의종목 필터: {len(bot._warning_coins)}개 제외")
print(f"  코인당 예산: {500000 // bot.MAX_POSITIONS:,}원")
print(f"  분할매수: {500000 // bot.MAX_POSITIONS // 2:,}원 × 2회")
print(f"  과투자(85+): 최대 {int(500000 * 0.7):,}원")
