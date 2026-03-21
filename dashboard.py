#!/usr/bin/env python3
"""업비트 자동매매 봇 실시간 대시보드"""

from flask import Flask, jsonify, render_template_string
import sqlite3
import os
import json

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), 'trading_bot_v3.db')
POS_PATH = os.path.join(os.path.dirname(__file__), 'positions.json')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>업비트 봇 대시보드</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 16px; }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.header h1 { font-size: 20px; color: #58a6ff; }
.header .refresh { color: #8b949e; font-size: 12px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 20px; }
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.card .label { font-size: 12px; color: #8b949e; margin-bottom: 4px; }
.card .value { font-size: 24px; font-weight: 600; }
.card .sub { font-size: 12px; color: #8b949e; margin-top: 4px; }
.plus { color: #3fb950; }
.minus { color: #f85149; }
.zero { color: #8b949e; }
.section { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
.section h2 { font-size: 14px; color: #58a6ff; margin-bottom: 12px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; color: #8b949e; font-weight: 500; padding: 6px 8px; border-bottom: 1px solid #30363d; }
td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
tr:hover { background: #1c2128; }
.tag { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 500; }
.tag-win { background: #0d3321; color: #3fb950; }
.tag-loss { background: #3d1419; color: #f85149; }
.tag-draw { background: #272c33; color: #8b949e; }
.tag-surge { background: #1c2541; color: #79c0ff; }
.tag-batch { background: #272c33; color: #d2a8ff; }
.daily-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
.daily-bar .day { width: 70px; font-size: 12px; color: #8b949e; }
.daily-bar .bar-wrap { flex: 1; height: 20px; background: #21262d; border-radius: 4px; position: relative; overflow: hidden; }
.daily-bar .bar { height: 100%; border-radius: 4px; min-width: 2px; }
.daily-bar .bar-plus { background: #238636; }
.daily-bar .bar-minus { background: #da3633; }
.daily-bar .pnl { width: 90px; text-align: right; font-size: 12px; font-weight: 500; }
.pos-card { display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid #21262d; }
.pos-card:last-child { border-bottom: none; }
.pos-coin { font-weight: 600; font-size: 15px; }
.pos-detail { font-size: 12px; color: #8b949e; }
.pos-pnl { font-size: 16px; font-weight: 600; }
.status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.status-on { background: #3fb950; }
.status-off { background: #f85149; }
@media (max-width: 600px) {
  .cards { grid-template-columns: repeat(2, 1fr); }
  .card .value { font-size: 18px; }
}
</style>
</head>
<body>
<div class="header">
  <h1><span class="status-dot" id="statusDot"></span>업비트 봇 대시보드</h1>
  <span class="refresh" id="refreshTime"></span>
</div>
<div class="cards" id="cards"></div>
<div class="section" id="posSection" style="display:none">
  <h2>보유 포지션</h2>
  <div id="positions"></div>
</div>
<div class="section">
  <h2>일별 손익</h2>
  <div id="dailyChart"></div>
</div>
<div class="section">
  <h2>배치별 성적</h2>
  <table id="batchTable"><thead><tr><th>배치</th><th>거래</th><th>승률</th><th>손익</th></tr></thead><tbody></tbody></table>
</div>
<div class="section">
  <h2>시간대별 성과</h2>
  <div id="hourlyChart" style="display:grid; grid-template-columns:repeat(12,1fr); gap:4px; margin-bottom:8px;"></div>
  <div id="hourlyChart2" style="display:grid; grid-template-columns:repeat(12,1fr); gap:4px;"></div>
  <div style="display:flex; justify-content:space-between; margin-top:8px; font-size:11px; color:#8b949e;">
    <span>🟢 승률50%+ &amp; 수익</span><span>🟡 혼합</span><span>🔴 승률40%- 또는 손실</span>
  </div>
</div>
<div class="section">
  <h2>최근 거래</h2>
  <table id="tradeTable"><thead><tr><th>시간</th><th>코인</th><th>손익</th><th>수익률</th><th>배치</th></tr></thead><tbody></tbody></table>
</div>

<script>
function fmt(n) { return n.toLocaleString('ko-KR'); }
function pnlClass(n) { return n > 0 ? 'plus' : n < 0 ? 'minus' : 'zero'; }
function pnlSign(n) { return n > 0 ? '+' + fmt(n) : fmt(n); }

async function load() {
  try {
    const res = await fetch('/api/status');
    const d = await res.json();

    // Status dot
    document.getElementById('statusDot').className = 'status-dot ' + (d.bot_running ? 'status-on' : 'status-off');
    document.getElementById('refreshTime').textContent = d.updated + ' 갱신 (10초 자동)';

    // Cards
    const pnlPct = ((d.balance - 10000000) / 10000000 * 100).toFixed(2);
    document.getElementById('cards').innerHTML = `
      <div class="card">
        <div class="label">잔고</div>
        <div class="value">${fmt(d.balance)}<span style="font-size:14px">원</span></div>
        <div class="sub ${pnlClass(d.total_pnl)}">시작 대비 ${pnlSign(d.total_pnl)}원 (${d.total_pnl >= 0 ? '+' : ''}${pnlPct}%)</div>
      </div>
      <div class="card">
        <div class="label">총 거래</div>
        <div class="value">${d.total_trades}<span style="font-size:14px">건</span></div>
        <div class="sub">승 ${d.wins} / 패 ${d.losses} / 무 ${d.draws}</div>
      </div>
      <div class="card">
        <div class="label">승률</div>
        <div class="value ${d.win_rate >= 40 ? 'plus' : d.win_rate >= 30 ? 'zero' : 'minus'}">${d.win_rate.toFixed(1)}<span style="font-size:14px">%</span></div>
        <div class="sub">목표 40%+</div>
      </div>
      <div class="card">
        <div class="label">오늘 손익</div>
        <div class="value ${pnlClass(d.today_pnl)}">${pnlSign(d.today_pnl)}<span style="font-size:14px">원</span></div>
        <div class="sub">${d.today_trades}건 거래 | 승률 ${d.today_wr.toFixed(0)}%</div>
      </div>
    `;

    // Positions
    const posDiv = document.getElementById('positions');
    const posSection = document.getElementById('posSection');
    if (d.positions && d.positions.length > 0) {
      posSection.style.display = 'block';
      posDiv.innerHTML = d.positions.map(p => `
        <div class="pos-card">
          <div>
            <div class="pos-coin">${p.coin} <span class="tag ${p.batch === 'surge_trade' ? 'tag-surge' : 'tag-batch'}">${p.batch}</span></div>
            <div class="pos-detail">${fmt(p.amount)}원 | ${p.hold_min}분 보유</div>
          </div>
          <div class="pos-pnl ${pnlClass(p.profit_rate)}">${p.profit_rate >= 0 ? '+' : ''}${p.profit_rate.toFixed(2)}%</div>
        </div>
      `).join('');
    } else {
      posSection.style.display = 'none';
    }

    // Daily chart
    const maxAbs = Math.max(...d.daily.map(x => Math.abs(x.pnl)), 1);
    document.getElementById('dailyChart').innerHTML = d.daily.map(x => `
      <div class="daily-bar">
        <span class="day">${x.date.slice(5)}</span>
        <div class="bar-wrap">
          <div class="bar ${x.pnl >= 0 ? 'bar-plus' : 'bar-minus'}" style="width:${Math.abs(x.pnl)/maxAbs*100}%"></div>
        </div>
        <span class="pnl ${pnlClass(x.pnl)}">${pnlSign(x.pnl)}</span>
      </div>
    `).join('');

    // Batch table
    document.querySelector('#batchTable tbody').innerHTML = d.batches.map(b => `
      <tr>
        <td>${b.batch}</td>
        <td>${b.total}건</td>
        <td>${b.win_rate.toFixed(0)}%</td>
        <td class="${pnlClass(b.pnl)}">${pnlSign(b.pnl)}원</td>
      </tr>
    `).join('');

    // Hourly heatmap
    if (d.hourly && d.hourly.length > 0) {
      const allHours = Array.from({length: 24}, (_, i) => {
        const found = d.hourly.find(h => h.hour === i);
        return found || {hour: i, cnt: 0, wins: 0, wr: 0, pnl: 0};
      });
      const maxPnl = Math.max(...allHours.map(h => Math.abs(h.pnl)), 1);
      const row1 = allHours.slice(0, 12);
      const row2 = allHours.slice(12, 24);
      function hourCell(h) {
        let bg, border;
        if (h.cnt === 0) { bg = '#161b22'; border = '#30363d'; }
        else if (h.wr >= 50 && h.pnl > 0) { bg = `rgba(35,134,54,${Math.min(0.8, Math.abs(h.pnl)/maxPnl+0.2)})`; border = '#238636'; }
        else if (h.wr < 40 || h.pnl < -50000) { bg = `rgba(218,54,51,${Math.min(0.8, Math.abs(h.pnl)/maxPnl+0.2)})`; border = '#da3633'; }
        else { bg = `rgba(187,128,9,${Math.min(0.6, Math.abs(h.pnl)/maxPnl+0.15)})`; border = '#bb8009'; }
        const pnlStr = h.pnl >= 0 ? '+' + (h.pnl/1000).toFixed(0) + 'k' : (h.pnl/1000).toFixed(0) + 'k';
        return `<div style="background:${bg}; border:1px solid ${border}; border-radius:6px; padding:6px 2px; text-align:center; min-height:56px;">
          <div style="font-size:11px; color:#8b949e;">${h.hour}시</div>
          <div style="font-size:13px; font-weight:600; color:${h.wr>=50?'#3fb950':h.wr<40?'#f85149':'#e3b341'}">${h.cnt>0?h.wr+'%':'-'}</div>
          <div style="font-size:10px; color:${h.pnl>=0?'#3fb950':'#f85149'}">${h.cnt>0?pnlStr:''}</div>
        </div>`;
      }
      document.getElementById('hourlyChart').innerHTML = row1.map(hourCell).join('');
      document.getElementById('hourlyChart2').innerHTML = row2.map(hourCell).join('');
    }

    // Recent trades
    document.querySelector('#tradeTable tbody').innerHTML = d.recent.map(t => `
      <tr>
        <td>${t.time}</td>
        <td>${t.coin}</td>
        <td class="${pnlClass(t.profit)}">${pnlSign(t.profit)}원</td>
        <td class="${pnlClass(t.profit_rate)}"><span class="tag ${t.profit > 0 ? 'tag-win' : t.profit < 0 ? 'tag-loss' : 'tag-draw'}">${t.profit_rate >= 0 ? '+' : ''}${t.profit_rate.toFixed(2)}%</span></td>
        <td><span class="tag ${t.batch === 'surge_trade' ? 'tag-surge' : 'tag-batch'}">${t.batch}</span></td>
      </tr>
    `).join('');

  } catch(e) {
    document.getElementById('statusDot').className = 'status-dot status-off';
  }
}

load();
setInterval(load, 10000);
</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/status')
def api_status():
    from datetime import datetime
    conn = get_db()
    c = conn.cursor()

    # Total stats
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

    # Daily P&L
    c.execute('SELECT substr(timestamp,1,10) as day, SUM(profit), COUNT(*) FROM trades WHERE action="sell" GROUP BY day ORDER BY day')
    daily = [{'date': r[0], 'pnl': r[1] or 0, 'trades': r[2]} for r in c.fetchall()]

    # Batch stats
    c.execute('SELECT batch, COUNT(*), SUM(CASE WHEN profit>0 THEN 1 ELSE 0 END), SUM(profit) FROM trades WHERE action="sell" GROUP BY batch')
    batches = []
    for r in c.fetchall():
        wr = (r[2] or 0) / r[1] * 100 if r[1] else 0
        batches.append({'batch': r[0], 'total': r[1], 'win_rate': wr, 'pnl': r[3] or 0})

    # Recent trades
    c.execute('SELECT timestamp, coin, profit, profit_rate, batch FROM trades WHERE action="sell" ORDER BY id DESC LIMIT 20')
    recent = []
    for r in c.fetchall():
        recent.append({
            'time': r[0][5:16] if r[0] else '',
            'coin': r[1],
            'profit': r[2] or 0,
            'profit_rate': r[3] or 0,
            'batch': r[4]
        })

    # Positions (from positions.json)
    positions = []
    try:
        if os.path.exists(POS_PATH):
            with open(POS_PATH, 'r') as f:
                pos_data = json.load(f)
            now_ts = datetime.now().timestamp()
            # Normal positions
            for coin, batches_data in pos_data.get('positions', {}).items():
                for batch_id, p in batches_data.items():
                    hold_min = int((now_ts - p.get('timestamp', now_ts)) / 60)
                    positions.append({
                        'coin': coin, 'batch': batch_id,
                        'amount': p.get('amount', 0),
                        'profit_rate': p.get('peak_rate', 0) * 100 if p.get('peak_rate') else 0,
                        'hold_min': hold_min
                    })
            # Surge positions
            for coin, p in pos_data.get('surge', {}).items():
                hold_min = int((now_ts - p.get('timestamp', now_ts)) / 60)
                positions.append({
                    'coin': coin, 'batch': 'surge_trade',
                    'amount': p.get('amount', 0),
                    'profit_rate': p.get('peak_rate', 0) * 100,
                    'hold_min': hold_min
                })
    except:
        pass

    # Hourly stats (시간대별 승률/손익)
    c.execute('''
        SELECT CAST(substr(timestamp,12,2) AS INT) hour,
          COUNT(*) cnt,
          SUM(CASE WHEN profit>0 THEN 1 ELSE 0 END) wins,
          SUM(profit) pnl
        FROM trades WHERE action="sell" AND profit != 0
        GROUP BY hour ORDER BY hour
    ''')
    hourly = []
    for r in c.fetchall():
        wr = (r[2] or 0) / r[1] * 100 if r[1] else 0
        hourly.append({'hour': r[0], 'cnt': r[1], 'wins': r[2] or 0, 'wr': round(wr, 1), 'pnl': round(r[3] or 0)})

    # Bot running check
    bot_running = False
    pid_path = os.path.join(os.path.dirname(__file__), 'bot.pid')
    try:
        if os.path.exists(pid_path):
            with open(pid_path) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
            bot_running = True
    except:
        pass

    conn.close()

    return jsonify({
        'balance': 10000000 + total_pnl,
        'total_pnl': total_pnl,
        'total_trades': total,
        'wins': wins, 'losses': losses, 'draws': draws,
        'win_rate': win_rate,
        'today_pnl': today_pnl,
        'today_trades': today_trades,
        'today_wr': today_wr,
        'daily': daily,
        'batches': batches,
        'recent': recent,
        'positions': positions,
        'hourly': hourly,
        'bot_running': bot_running,
        'updated': datetime.now().strftime('%H:%M:%S')
    })

if __name__ == '__main__':
    print("📊 대시보드: http://localhost:5001")
    app.run(host='0.0.0.0', port=5001, debug=False)
