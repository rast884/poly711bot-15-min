"""
PolyBot v7
Цена: Binance WS btcusdt@kline_15m — тот же источник что rast884/btc-bot-server
      15м свеча Binance = цена которую Polymarket использует для расчёта раундов
Дашборд: дизайн как у rast884, кнопки Старт/Стоп, график обновляется каждую секунду
"""
import asyncio, json, logging, os, math, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from threading import Thread
from flask import Flask, jsonify, render_template_string, request

import aiohttp, websockets
from telegram import Bot, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes

TG_TOKEN   = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]
START_HOUR, END_HOUR = 9, 23
BET_AMOUNT = 5.0
MSK = ZoneInfo("Europe/Moscow")
PORT = int(os.environ.get("PORT", 8080))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

state = {
    "balance": 100.0, "pnl": 0.0, "bets": 0, "wins": 0,
    "active_bet": None,
    "price": 0.0,
    "price_source": "Binance WS",
    "slot_open_price": 0.0,
    "candles_15m": [],
    "ticks": [],            # {ts, p} за последние 20 мин
    "paused": False, "stopped": False,
    "history": [], "last_analysis": {},
    "kline_open": 0.0,      # open текущей 15м свечи
    "kline_high": 0.0,
    "kline_low": 0.0,
}

# ── TIME ──────────────────────────────────────────────────────────────────────
def msk_now(): return datetime.now(MSK)
def utc_now(): return datetime.now(timezone.utc)
def in_hours(): t=msk_now(); return START_HOUR<=t.hour<END_HOUR
def slot15(dt): dt=dt.replace(second=0,microsecond=0); return dt.replace(minute=(dt.minute//15)*15)
def next_slot(dt):
    m=(dt.minute//15+1)*15
    return (dt.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)) if m>=60 else dt.replace(minute=m,second=0,microsecond=0)
def secs_to_next(): return max(0,int((next_slot(utc_now())-utc_now()).total_seconds()))
def fmt(p): return f"${p:,.2f}"
def fmtm(p): return f"+${p:.2f}" if p>=0 else f"-${abs(p):.2f}"

# ── STRATEGY ──────────────────────────────────────────────────────────────────
def ema(prices,p):
    if not prices: return 0
    k=2/(p+1); e=prices[0]
    for v in prices[1:]: e=v*k+e*(1-k)
    return e

def rsi(closes,p=14):
    if len(closes)<p+1: return 50.0
    g=l=0.0
    for i in range(len(closes)-p,len(closes)):
        d=closes[i]-closes[i-1]
        if d>0: g+=d
        else: l+=abs(d)
    return 100-100/(1+(g/(l or 0.001)))

def macd(closes):
    if len(closes)<26: return 0,0,0
    ml=ema(closes[-12:],12)-ema(closes,26)
    sigs=[]
    for i in range(26,len(closes)+1):
        s=closes[max(0,i-26):i]
        sigs.append(ema(s[-12:],12)-ema(s,26))
    sig=ema(sigs[-9:],9) if len(sigs)>=9 else ml
    return ml,sig,ml-sig

def bollinger(closes,p=20,k=2.0):
    if len(closes)<p: v=closes[-1]; return v,v*1.01,v*0.99
    r=closes[-p:]; mid=sum(r)/p
    std=math.sqrt(sum((x-mid)**2 for x in r)/p)
    return mid,mid+k*std,mid-k*std

def atr_val(candles,p=14):
    if len(candles)<2: return 1
    trs=[max(candles[i]["h"]-candles[i]["l"],abs(candles[i]["h"]-candles[i-1]["c"]),abs(candles[i]["l"]-candles[i-1]["c"])) for i in range(1,len(candles))]
    r=trs[-p:]; return sum(r)/len(r) if r else 1

def stoch(candles,k=14):
    if len(candles)<k: return 50
    r=candles[-k:]; lo=min(c["l"] for c in r); hi=max(c["h"] for c in r)
    return (candles[-1]["c"]-lo)/(hi-lo)*100 if hi!=lo else 50

def analyze(candles_15m, price=None):
    if len(candles_15m)<6: return {"dir":"UP","confidence":52,"details":{},"score":0,"rsi":50}
    closes=[c["c"] for c in candles_15m]; p=price or closes[-1]
    score=0; details={}

    e9=ema(closes,9); e21=ema(closes,21); e55=ema(closes,min(55,len(closes)))
    if e9>e21>e55: score+=2; details["EMA Тренд"]="▲ Сильный бычий (9>21>55)"
    elif e9<e21<e55: score-=2; details["EMA Тренд"]="▼ Сильный медвежий"
    elif e9>e21: score+=1; details["EMA Тренд"]="↗ Слабый бычий"
    else: score-=1; details["EMA Тренд"]="↘ Слабый медвежий"

    r=rsi(closes,14)
    if r<35: score+=2; details["RSI"]=f"{r:.1f} ← перепродан ▲"
    elif r<45: score+=1; details["RSI"]=f"{r:.1f} ← слабый ▲"
    elif r>65: score-=2; details["RSI"]=f"{r:.1f} ← перекуплен ▼"
    elif r>55: score-=1; details["RSI"]=f"{r:.1f} ← слабый ▼"
    else: details["RSI"]=f"{r:.1f} нейтрально"

    ml,sl,hist=macd(closes)
    if ml>sl and hist>0: score+=2; details["MACD"]=f"▲ Бычье (hist={hist:+.0f})"
    elif ml<sl and hist<0: score-=2; details["MACD"]=f"▼ Медвежье (hist={hist:+.0f})"
    elif ml>sl: score+=1; details["MACD"]="↗ Выше сигнала"
    else: score-=1; details["MACD"]="↘ Ниже сигнала"

    bb_mid,bb_up,bb_lo=bollinger(closes,20)
    pos=(p-bb_lo)/(bb_up-bb_lo)*100 if bb_up!=bb_lo else 50
    if p<bb_lo: score+=1; details["Bollinger"]=f"▲ Ниже нижней ({pos:.0f}%)"
    elif p>bb_up: score-=1; details["Bollinger"]=f"▼ Выше верхней ({pos:.0f}%)"
    elif pos<35: score+=1; details["Bollinger"]=f"↗ Нижняя зона ({pos:.0f}%)"
    elif pos>65: score-=1; details["Bollinger"]=f"↘ Верхняя зона ({pos:.0f}%)"
    else: details["Bollinger"]=f"Середина ({pos:.0f}%)"

    sk=stoch(candles_15m,14)
    if sk<20: score+=1; details["Stoch"]=f"K={sk:.0f} ← перепродан ▲"
    elif sk>80: score-=1; details["Stoch"]=f"K={sk:.0f} ← перекуплен ▼"
    else: details["Stoch"]=f"K={sk:.0f} нейтрально"

    if len(candles_15m)>=4:
        last=candles_15m[-4:]; bull=sum(1 for i in range(1,4) if last[i]["c"]>last[i-1]["c"])
        move=abs(closes[-1]-closes[-4]); a=atr_val(candles_15m,14); rel=move/a
        if bull>=3 and rel>0.4: score+=2; details["Моментум"]=f"▲ Сильный ({bull}/3, {rel:.1f}x ATR)"
        elif bull==0 and rel>0.4: score-=2; details["Моментум"]=f"▼ Сильный ({bull}/3, {rel:.1f}x ATR)"
        elif bull>=2: score+=1; details["Моментум"]=f"↗ Умеренный ({bull}/3)"
        else: score-=1; details["Моментум"]=f"↘ Умеренный ({bull}/3)"

    conf=min(92,max(52,int(50+abs(score)/10*42)))
    return {"dir":"UP" if score>=0 else "DOWN","confidence":conf,"details":details,"score":score,"rsi":r}

# ── PRICE: Binance 15M kline WS (same as rast884 bot) ────────────────────────
async def binance_kline_ws():
    """
    Подписка на btcusdt@kline_15m — 15-минутные свечи Binance.
    Это ТОЧНО тот же источник что использует rast884/btc-bot-server.
    Цена свечи Binance 15m = цена Polymarket для расчёта раундов.
    """
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_15m/btcusdt@trade"
    while not state["stopped"]:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                state["price_source"] = "Binance WS (15m kline)"
                log.info("Binance 15m kline WS connected")
                async for raw in ws:
                    if state["stopped"]: break
                    d = json.loads(raw)
                    # Trade tick — для обновления цены каждую секунду
                    if d.get("e") == "trade":
                        p = float(d["p"])
                        state["price"] = p
                        ts = int(time.time() * 1000)
                        state["ticks"].append({"ts": ts, "p": p})
                        cutoff = ts - 20 * 60 * 1000
                        state["ticks"] = [t for t in state["ticks"] if t["ts"] > cutoff]

                    # 15m kline — для свечей и расчёта
                    elif d.get("e") == "kline":
                        k = d["k"]
                        state["kline_open"] = float(k["o"])
                        state["kline_high"] = float(k["h"])
                        state["kline_low"]  = float(k["l"])
                        candle = {
                            "t": int(k["t"]), "o": float(k["o"]),
                            "h": float(k["h"]), "l": float(k["l"]), "c": float(k["c"])
                        }
                        c = state["candles_15m"]
                        if c and c[-1]["t"] == candle["t"]:
                            c[-1] = candle
                        else:
                            c.append(candle)
                        if len(c) > 100: state["candles_15m"] = c[-100:]
                        # Если свеча закрылась — обновляем анализ
                        if k["x"]:
                            log.info(f"15m candle closed: {fmt(float(k['c']))}")
        except Exception as e:
            log.warning(f"Binance WS error: {e} — retry 5s")
            await asyncio.sleep(5)

async def load_initial_candles():
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=100",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    state["candles_15m"] = [
                        {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                         "l": float(k[3]), "c": float(k[4])} for k in data
                    ]
                    if state["candles_15m"]:
                        last = state["candles_15m"][-1]
                        state["price"] = last["c"]
                        state["kline_open"] = last["o"]
                        state["kline_high"] = last["h"]
                        state["kline_low"]  = last["l"]
                    log.info(f"Loaded {len(state['candles_15m'])} candles")
    except Exception as e:
        log.warning(f"Initial candles error: {e}")

# ── TRADING ───────────────────────────────────────────────────────────────────
async def open_bet(bot):
    if state["paused"] or state["stopped"] or state["balance"] < BET_AMOUNT or state["active_bet"]: return
    price = state["price"]
    if not price: return
    a = analyze(state["candles_15m"], price)
    state["last_analysis"] = a
    direction = a["dir"]
    now = utc_now(); so = slot15(now); sc = so + timedelta(minutes=15)
    state["balance"] -= BET_AMOUNT; state["bets"] += 1
    state["slot_open_price"] = price
    state["active_bet"] = {"dir": direction, "entry": price, "slot_open": so,
                           "slot_close": sc, "num": state["bets"], "analysis": a}
    om = so.astimezone(MSK).strftime("%H:%M"); cm = sc.astimezone(MSK).strftime("%H:%M")
    sigs = "\n".join(f"  • {k}: {v}" for k,v in a["details"].items())
    await send_msg(bot,
        f"📊 *Ставка #{state['bets']}*\n\n"
        f"{'🟢 ▲ ВВЕРХ' if direction=='UP' else '🔴 ▼ ВНИЗ'} · `{fmt(price)}`\n"
        f"Слот: `{om}→{cm} МСК`\n"
        f"Счёт: `{a['score']:+d}/10` · Уверенность: `{a['confidence']}%`\n\n"
        f"*Индикаторы:*\n{sigs}\n\nБаланс: `${state['balance']:.2f}`"
    )
    log.info(f"BET #{state['bets']}: {direction} @ {fmt(price)} score={a['score']}")

async def close_bet(bot):
    bet = state["active_bet"]
    if not bet: return
    exit_p = state["price"] or bet["entry"]
    up = exit_p > bet["entry"]
    won = (bet["dir"] == "UP" and up) or (bet["dir"] == "DOWN" and not up)
    profit = BET_AMOUNT * 0.88 if won else -BET_AMOUNT
    state["balance"] += BET_AMOUNT + profit; state["pnl"] += profit
    state["active_bet"] = None; state["slot_open_price"] = 0.0
    if won: state["wins"] += 1
    wr = round(state["wins"] / state["bets"] * 100) if state["bets"] else 0
    om = bet["slot_open"].astimezone(MSK).strftime("%H:%M")
    cm = bet["slot_close"].astimezone(MSK).strftime("%H:%M")
    state["history"].append({
        "num": bet["num"], "dir": bet["dir"], "entry": bet["entry"], "exit": exit_p,
        "won": won, "profit": profit, "slot": f"{om}-{cm}",
        "score": bet["analysis"].get("score", 0), "conf": bet["analysis"].get("confidence", 0)
    })
    if len(state["history"]) > 100: state["history"] = state["history"][-100:]
    await send_msg(bot,
        f"{'✅ ВЫИГРЫШ' if won else '❌ ПРОИГРЫШ'} · #{bet['num']}\n\n"
        f"{'▲ ВВЕРХ' if bet['dir']=='UP' else '▼ ВНИЗ'} · `{om}–{cm}`\n"
        f"Вход: `{fmt(bet['entry'])}` → Выход: `{fmt(exit_p)}`\n"
        f"Прибыль: `{fmtm(profit)}`\n"
        f"Баланс: `${state['balance']:.2f}` · P&L: `{fmtm(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']})"
    )
    log.info(f"CLOSED #{bet['num']}: {'WIN' if won else 'LOSS'} {fmtm(profit)} wr={wr}%")

async def trading_loop(bot):
    await asyncio.sleep(10)
    while not state["stopped"]:
        now = utc_now(); nxt = next_slot(now)
        wait = (nxt - now).total_seconds()
        log.info(f"Next slot: {nxt.astimezone(MSK).strftime('%H:%M МСК')} in {wait:.0f}s")
        await asyncio.sleep(max(0, wait - 0.3))
        now = utc_now(); nxt = next_slot(now); p = (nxt - now).total_seconds()
        if p > 0: await asyncio.sleep(p)
        if state["stopped"]: break
        log.info(f"SLOT @ {utc_now().astimezone(MSK).strftime('%H:%M')} BTC={fmt(state['price'])}")
        if state["active_bet"]: await close_bet(bot)
        await asyncio.sleep(1)
        if not state["paused"] and in_hours(): await open_bet(bot)

async def daily_summary(bot):
    while not state["stopped"]:
        now = msk_now(); t = now.replace(hour=END_HOUR, minute=0, second=5, microsecond=0)
        if now >= t: t += timedelta(days=1)
        await asyncio.sleep((t - now).total_seconds())
        if state["stopped"]: break
        wr = round(state["wins"] / state["bets"] * 100) if state["bets"] else 0
        await send_msg(bot, f"🌙 *Итог дня*\n\n💰 `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n🎯 {state['bets']} ставок · Win rate `{wr}%`")

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
async def send_msg(bot, text):
    try: await bot.send_message(chat_id=TG_CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e: log.error(f"TG: {e}")

async def cmd_start(update, ctx):
    global TG_CHAT_ID; TG_CHAT_ID = str(update.effective_chat.id)
    state["paused"] = False; state["stopped"] = False
    await update.message.reply_text("🤖 *PolyBot v7 запущен*\n\nЦена: Binance 15M kline (=Polymarket)\nСтратегия: 6 индикаторов\n\nКоманды в меню 👇", parse_mode="Markdown")

async def cmd_status(update, ctx):
    secs = secs_to_next(); m, sc = divmod(secs, 60)
    wr = round(state["wins"] / state["bets"] * 100) if state["bets"] else 0
    ab = state["active_bet"]; bi = "нет"
    status = "🛑 СТОП" if state["stopped"] else "⏸ ПАУЗА" if state["paused"] else ("✅ АКТИВЕН" if in_hours() else "🌙 ВНЕ ЧАСОВ")
    if ab:
        cur = state["price"]; win = (ab["dir"]=="UP" and cur>ab["entry"]) or (ab["dir"]=="DOWN" and cur<ab["entry"])
        bi = f"{'▲' if ab['dir']=='UP' else '▼'} {fmt(ab['entry'])}→{fmt(cur)} {'✅' if win else '❌'}"
    await update.message.reply_text(
        f"📊 *{msk_now().strftime('%H:%M:%S МСК')}*\n\nСтатус: `{status}`\n"
        f"BTC: `{fmt(state['price'])}`\n\n"
        f"💰 `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n"
        f"🎯 `{state['bets']}` ставок · Win rate `{wr}%`\n"
        f"До слота: `{m:02d}:{sc:02d}`\n\nСтавка: `{bi}`", parse_mode="Markdown")

async def cmd_analysis(update, ctx):
    a = analyze(state["candles_15m"], state["price"])
    sigs = "\n".join(f"• {k}: {v}" for k,v in a["details"].items())
    secs = secs_to_next(); m, s = divmod(secs, 60)
    await update.message.reply_text(
        f"🔍 *Анализ · {fmt(state['price'])}*\n\n"
        f"*{'▲ ВВЕРХ' if a['dir']=='UP' else '▼ ВНИЗ'}* · `{a['score']:+d}/10` · `{a['confidence']}%`\n\n"
        f"{sigs}\n\nДо слота: `{m:02d}:{s:02d}`", parse_mode="Markdown")

async def cmd_bet(update, ctx):
    if not in_hours(): await update.message.reply_text("⛔ 09:00–23:00 МСК"); return
    if state["active_bet"]: await update.message.reply_text("⚠️ Уже открыта ставка"); return
    if state["stopped"]: await update.message.reply_text("🛑 Бот остановлен. /start для запуска"); return
    await open_bet(ctx.bot)

async def cmd_pause(update, ctx):
    state["paused"] = not state["paused"]
    await update.message.reply_text("⏸ Пауза" if state["paused"] else "▶️ Возобновлён")

async def cmd_stop(update, ctx):
    state["stopped"] = True; state["paused"] = True
    if state["active_bet"]: await close_bet(ctx.bot)
    wr = round(state["wins"] / state["bets"] * 100) if state["bets"] else 0
    await update.message.reply_text(
        f"🛑 *Остановлен*\n\n`${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\nWin rate `{wr}%`\n\n/start для перезапуска",
        parse_mode="Markdown")

async def cmd_reset(update, ctx):
    state.update({"balance":100.0,"pnl":0.0,"bets":0,"wins":0,"active_bet":None,
                  "paused":False,"stopped":False,"history":[],"slot_open_price":0.0})
    await update.message.reply_text("↺ Сброс · `$100.00`", parse_mode="Markdown")

# ── WEB API endpoints for dashboard ──────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@flask_app.route("/api/state")
def api_state():
    now = utc_now(); nxt = next_slot(now)
    ab = state["active_bet"]
    a = analyze(state["candles_15m"], state["price"])
    state["last_analysis"] = a
    return jsonify({
        "price": state["price"],
        "price_source": state["price_source"],
        "balance": state["balance"],
        "pnl": state["pnl"],
        "bets": state["bets"],
        "wins": state["wins"],
        "paused": state["paused"],
        "stopped": state["stopped"],
        "in_hours": in_hours(),
        "slot_open_price": state["slot_open_price"],
        "kline_open": state["kline_open"],
        "kline_high": state["kline_high"],
        "kline_low": state["kline_low"],
        "ticks": state["ticks"][-300:],
        "candles_15m": state["candles_15m"][-3:],
        "active_bet": {
            "num": ab["num"], "dir": ab["dir"], "entry": ab["entry"],
            "slot": ab["slot_open"].astimezone(MSK).strftime("%H:%M") + "–" + ab["slot_close"].astimezone(MSK).strftime("%H:%M")
        } if ab else None,
        "last_analysis": a,
        "history": state["history"],
        "next_slot_msk": nxt.astimezone(MSK).strftime("%H:%M"),
        "msk_time": msk_now().strftime("%H:%M:%S"),
    })

@flask_app.route("/api/control", methods=["POST"])
def api_control():
    action = request.json.get("action")
    if action == "start":
        state["paused"] = False; state["stopped"] = False
    elif action == "stop":
        state["paused"] = True; state["stopped"] = True
    elif action == "pause":
        state["paused"] = not state["paused"]
    elif action == "reset":
        state.update({"balance":100.0,"pnl":0.0,"bets":0,"wins":0,
                      "active_bet":None,"paused":False,"stopped":False,
                      "history":[],"slot_open_price":0.0})
    return jsonify({"ok": True, "paused": state["paused"], "stopped": state["stopped"]})

# ── DASHBOARD HTML ─────────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PolyBot Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  :root{
    --bg:#0f1117;--card:#1a1d2e;--card2:#1e2235;--border:#2a2f4a;
    --text:#e2e8f5;--muted:#6b7a9d;
    --green:#00c076;--red:#ff4560;--blue:#3b82f6;--yellow:#f59e0b;
    --purple:#7c3aed;
  }
  body{background:var(--bg);color:var(--text);font-family:'Inter',-apple-system,sans-serif;min-height:100vh}
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  /* ── NAV ── */
  .nav{display:flex;align-items:center;justify-content:space-between;
    padding:0 20px;height:54px;background:var(--card);border-bottom:1px solid var(--border);
    position:sticky;top:0;z-index:99}
  .nav-logo{display:flex;align-items:center;gap:10px;font-size:16px;font-weight:700}
  .nav-logo .icon{width:32px;height:32px;background:linear-gradient(135deg,#3b82f6,#7c3aed);
    border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px}
  .nav-right{display:flex;align-items:center;gap:8px}
  .badge{padding:4px 10px;border-radius:20px;font-size:12px;font-weight:500;font-family:'JetBrains Mono',monospace}
  .badge-green{background:rgba(0,192,118,.15);color:var(--green);border:1px solid rgba(0,192,118,.3)}
  .badge-red{background:rgba(255,69,96,.15);color:var(--red);border:1px solid rgba(255,69,96,.3)}
  .badge-muted{background:rgba(107,122,157,.12);color:var(--muted);border:1px solid var(--border)}
  .live-dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;animation:pulse 1.5s infinite;margin-right:4px}
  @keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(0,192,118,.4)}70%{box-shadow:0 0 0 6px rgba(0,192,118,0)}}

  /* ── LAYOUT ── */
  .wrap{max-width:1080px;margin:0 auto;padding:16px}
  .grid-top{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
  .grid-main{display:grid;grid-template-columns:1fr 300px;gap:12px;margin-bottom:12px}

  /* ── CARDS ── */
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px}
  .card-sm{background:var(--card2);border:1px solid var(--border);border-radius:12px;padding:12px 14px}
  .lbl{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.9px;font-weight:500;margin-bottom:5px}
  .val{font-size:22px;font-weight:700;line-height:1;font-family:'JetBrains Mono',monospace}
  .val-sub{font-size:12px;color:var(--muted);margin-top:4px;font-family:'JetBrains Mono',monospace}
  .g{color:var(--green)}.r{color:var(--red)}.b{color:var(--blue)}.y{color:var(--yellow)}

  /* ── PRICE BLOCK (Polymarket style) ── */
  .price-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px}
  .price-left .lbl{margin-bottom:3px}
  .target-val{font-size:26px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--text)}
  .price-right{text-align:right}
  .current-lbl{font-size:12px;font-weight:500;margin-bottom:3px}
  .current-val{font-size:26px;font-weight:700;font-family:'JetBrains Mono',monospace}
  .timer-wrap{display:flex;gap:6px;justify-content:flex-end;margin-top:8px}
  .timer-box{background:var(--card2);border:1px solid var(--border);border-radius:8px;
    padding:4px 10px;text-align:center;min-width:42px}
  .timer-num{font-size:20px;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1}
  .timer-lbl{font-size:9px;color:var(--muted);text-transform:uppercase;margin-top:2px}

  /* ── CHART ── */
  .chart-area{position:relative;height:170px}
  #mainChart{width:100%;height:170px}
  .ohlc-row{display:flex;gap:16px;margin-top:10px;padding-top:10px;border-top:1px solid var(--border);
    font-size:11px;font-family:'JetBrains Mono',monospace}
  .ohlc-item span{color:var(--muted);display:block;margin-bottom:2px;font-size:10px}

  /* ── RIGHT PANEL ── */
  .right-panel{display:flex;flex-direction:column;gap:10px}

  /* ── ACTIVE BET ── */
  .active-bet{background:rgba(59,130,246,.08);border:1px solid rgba(59,130,246,.25);border-radius:12px;padding:12px}
  .ab-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
  .ab-title{font-size:11px;color:#60a5fa;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
  .ab-dir{font-size:13px;font-weight:700}
  .ab-row{display:flex;justify-content:space-between;font-size:12px;font-family:'JetBrains Mono',monospace;margin-top:3px}
  .ab-key{color:var(--muted)}

  /* ── ANALYSIS ── */
  .analysis-wrap{background:rgba(124,58,237,.07);border:1px solid rgba(124,58,237,.2);border-radius:12px;padding:12px}
  .pred-wrap{display:flex;align-items:center;gap:8px;margin-bottom:8px}
  .pred-badge{padding:5px 14px;border-radius:8px;font-size:14px;font-weight:700}
  .pred-up{background:rgba(0,192,118,.15);color:var(--green);border:1px solid rgba(0,192,118,.3)}
  .pred-dn{background:rgba(255,69,96,.15);color:var(--red);border:1px solid rgba(255,69,96,.3)}
  .score-wrap{margin:6px 0 8px}
  .score-track{height:5px;background:var(--border);border-radius:5px;position:relative;overflow:hidden}
  .score-bar{position:absolute;top:0;height:5px;border-radius:5px;transition:all .4s}
  .sig-row{display:flex;justify-content:space-between;align-items:center;
    font-size:11px;padding:3px 0;border-bottom:1px solid rgba(42,47,74,.6)}
  .sig-row:last-child{border:none}
  .sig-key{color:var(--muted);font-weight:500}
  .sig-val{font-family:'JetBrains Mono',monospace;font-size:10px;text-align:right;max-width:55%}

  /* ── BUTTONS ── */
  .btn-row{display:flex;gap:8px;margin-top:10px}
  .btn{flex:1;padding:10px;border:none;border-radius:10px;font-size:13px;font-weight:600;
    cursor:pointer;transition:all .15s;font-family:'Inter',sans-serif}
  .btn-start{background:var(--green);color:#000}
  .btn-start:hover{opacity:.85}
  .btn-stop{background:rgba(255,69,96,.15);color:var(--red);border:1px solid rgba(255,69,96,.3)}
  .btn-stop:hover{background:rgba(255,69,96,.25)}
  .btn-reset{background:var(--border);color:var(--muted);font-size:12px;flex:0.6}
  .btn-reset:hover{color:var(--text)}

  /* ── HISTORY ── */
  .hist-item{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(42,47,74,.6)}
  .hist-item:last-child{border:none}
  .hist-icon{width:28px;height:28px;border-radius:8px;display:flex;align-items:center;
    justify-content:center;font-size:13px;font-weight:700;flex-shrink:0}
  .hist-body{flex:1;min-width:0}
  .hist-title{font-size:12px;font-weight:500}
  .hist-sub{font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:2px}
  .hist-profit{font-size:12px;font-weight:700;font-family:'JetBrains Mono',monospace}
  .hlist{max-height:220px;overflow-y:auto}
  .hlist::-webkit-scrollbar{width:3px}
  .hlist::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}

  @media(max-width:720px){.grid-top{grid-template-columns:repeat(2,1fr)}.grid-main{grid-template-columns:1fr}}
</style>
</head>
<body>

<nav class="nav">
  <div class="nav-logo">
    <div class="icon">₿</div>
    <span>PolyBot <span style="color:var(--muted);font-weight:400;font-size:13px">/ BTC 15M</span></span>
  </div>
  <div class="nav-right">
    <span id="srcBadge" class="badge badge-muted">Binance WS</span>
    <span id="statusBadge" class="badge badge-green"><span class="live-dot"></span>АКТИВЕН</span>
    <span id="clockBadge" class="badge badge-muted">--:--:--</span>
  </div>
</nav>

<div class="wrap">

  <div class="grid-top">
    <div class="card-sm">
      <div class="lbl">Баланс</div>
      <div class="val" id="balance">$100.00</div>
      <div class="val-sub" id="balSub">— $0.00</div>
    </div>
    <div class="card-sm">
      <div class="lbl">P&L итого</div>
      <div class="val" id="pnl">$0.00</div>
      <div class="val-sub" id="pnlPct">0.00%</div>
    </div>
    <div class="card-sm">
      <div class="lbl">Win rate</div>
      <div class="val" id="wr">—</div>
      <div class="val-sub" id="wrSub">0 / 0 ставок</div>
    </div>
    <div class="card-sm">
      <div class="lbl">Прогноз</div>
      <div class="val" id="quickPred" style="font-size:18px">—</div>
      <div class="val-sub" id="quickConf">уверенность: —</div>
    </div>
  </div>

  <div class="grid-main">

    <!-- LEFT: Chart -->
    <div class="card">
      <div class="price-header">
        <div class="price-left">
          <div class="lbl">Целевая цена</div>
          <div class="target-val" id="targetPrice">$—</div>
        </div>
        <div class="price-right">
          <div class="current-lbl" id="curLbl" style="color:var(--green)">▲ Текущая цена</div>
          <div class="current-val" id="curPrice" style="color:var(--yellow)">$—</div>
          <div class="timer-wrap">
            <div class="timer-box"><div class="timer-num g" id="tMin">--</div><div class="timer-lbl">МИН</div></div>
            <div class="timer-box"><div class="timer-num g" id="tSec">--</div><div class="timer-lbl">СЕК</div></div>
          </div>
        </div>
      </div>
      <div class="chart-area"><canvas id="mainChart"></canvas></div>
      <div class="ohlc-row">
        <div class="ohlc-item"><span>OPEN</span><b id="oO">—</b></div>
        <div class="ohlc-item"><span>HIGH</span><b id="oH" class="g">—</b></div>
        <div class="ohlc-item"><span>LOW</span><b id="oL" class="r">—</b></div>
        <div class="ohlc-item"><span>CLOSE</span><b id="oC">—</b></div>
        <div class="ohlc-item" style="margin-left:auto"><span>ИСТОЧНИК</span><b id="oSrc" style="color:var(--blue);font-size:10px">—</b></div>
      </div>

      <!-- Control buttons -->
      <div class="btn-row">
        <button class="btn btn-start" onclick="control('start')">▶ СТАРТ</button>
        <button class="btn btn-stop" onclick="control('stop')">■ СТОП</button>
        <button class="btn btn-reset" onclick="control('reset')">↺ Сброс</button>
      </div>
    </div>

    <!-- RIGHT panel -->
    <div class="right-panel">

      <!-- Active bet -->
      <div class="card">
        <div class="lbl" style="margin-bottom:8px">Активная ставка</div>
        <div id="activeBet">
          <div style="color:var(--muted);font-size:13px;padding:8px 0">Нет активной ставки</div>
        </div>
      </div>

      <!-- Analysis -->
      <div class="card">
        <div class="lbl" style="margin-bottom:8px">Анализ / Прогноз</div>
        <div class="analysis-wrap">
          <div class="pred-wrap">
            <div class="pred-badge pred-up" id="predBadge">▲ ВВЕРХ</div>
            <div style="font-size:12px;color:var(--muted)" id="predConf">52% уверенность</div>
          </div>
          <div class="score-wrap">
            <div style="font-size:11px;color:var(--muted);margin-bottom:4px">Счёт сигналов: <span id="scoreNum">0</span>/10</div>
            <div class="score-track">
              <div class="score-bar" id="scoreBar"></div>
            </div>
          </div>
          <div id="signals"></div>
        </div>
      </div>

      <!-- P&L mini chart -->
      <div class="card">
        <div class="lbl" style="margin-bottom:6px">P&L кривая</div>
        <div style="height:70px;position:relative"><canvas id="pnlChart"></canvas></div>
      </div>

    </div>
  </div>

  <!-- History -->
  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <div class="lbl" style="margin:0">История ставок</div>
      <div style="display:flex;gap:8px">
        <span id="winBadge" class="badge badge-green">0 побед</span>
        <span id="lossBadge" class="badge badge-red">0 проигр.</span>
      </div>
    </div>
    <div class="hlist" id="histList"><div style="color:var(--muted);text-align:center;padding:20px;font-size:13px">Нет ставок</div></div>
  </div>

</div><!-- /wrap -->

<script>
let mainChart=null, pnlChart=null;
let prevPrice=0;

function initCharts(){
  mainChart=new Chart(document.getElementById('mainChart'),{
    type:'line',
    data:{labels:[],datasets:[{
      data:[],borderColor:'#f59e0b',borderWidth:1.5,fill:true,
      backgroundColor:'rgba(245,158,11,0.06)',pointRadius:0,tension:0.15
    }]},
    options:{responsive:true,maintainAspectRatio:false,animation:{duration:0},
      plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,
        callbacks:{label:c=>'$'+c.raw.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}}},
      scales:{
        x:{display:false},
        y:{position:'right',grid:{color:'rgba(255,255,255,0.03)'},
          ticks:{color:'#6b7a9d',font:{size:10,family:'JetBrains Mono'},
            callback:v=>'$'+Math.round(v).toLocaleString('en-US')}}
      }
    }
  });

  pnlChart=new Chart(document.getElementById('pnlChart'),{
    type:'line',
    data:{labels:['0'],datasets:[{data:[0],
      borderColor:'#00c076',borderWidth:1.5,fill:true,
      backgroundColor:'rgba(0,192,118,0.06)',pointRadius:0,tension:0.3,
      segment:{borderColor:ctx=>ctx.p0.parsed.y<0?'#ff4560':'#00c076'}}]},
    options:{responsive:true,maintainAspectRatio:false,animation:{duration:200},
      plugins:{legend:{display:false}},
      scales:{x:{display:false},y:{display:false}}}
  });
}

async function fetchState(){
  try{
    const d=await fetch('/api/state').then(r=>r.json());
    const p=d.price||0;

    // ── Price display ──────────────────────────────────────────
    const formatted=p?'$'+p.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'$—';
    document.getElementById('curPrice').textContent=formatted;
    document.getElementById('oSrc').textContent=d.price_source||'—';

    const targetP=d.slot_open_price||p;
    document.getElementById('targetPrice').textContent=targetP?'$'+targetP.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'$—';

    const diff=p-targetP; const up=diff>=0;
    document.getElementById('curLbl').textContent=(up?'▲ ':' ▼ ')+'Текущая цена '+(diff>=0?'+':'')+diff.toFixed(2);
    document.getElementById('curLbl').style.color=up?'#00c076':'#ff4560';

    // OHLC
    document.getElementById('oO').textContent=d.kline_open?'$'+Math.round(d.kline_open).toLocaleString():'—';
    document.getElementById('oH').textContent=d.kline_high?'$'+Math.round(d.kline_high).toLocaleString():'—';
    document.getElementById('oL').textContent=d.kline_low?'$'+Math.round(d.kline_low).toLocaleString():'—';
    document.getElementById('oC').textContent=p?'$'+Math.round(p).toLocaleString():'—';

    // ── Live chart from ticks ─────────────────────────────────
    const ticks=d.ticks||[];
    if(ticks.length>1 && mainChart){
      mainChart.data.labels=ticks.map(t=>{
        const dt=new Date(t.ts);
        return dt.toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
      });
      mainChart.data.datasets[0].data=ticks.map(t=>t.p);
      // Color based on direction
      const first=ticks[0].p; const last=ticks[ticks.length-1].p;
      const chartColor=last>=first?'#f59e0b':'#f59e0b';
      mainChart.data.datasets[0].borderColor=chartColor;
      mainChart.update('none');
    }

    // ── Metrics ───────────────────────────────────────────────
    const bal=d.balance, pnl=d.pnl;
    document.getElementById('balance').textContent='$'+bal.toFixed(2);
    document.getElementById('balance').style.color=bal>=100?'#e2e8f5':'#ff4560';
    document.getElementById('balSub').textContent=(pnl>=0?'+ ':'− ')+'$'+Math.abs(pnl).toFixed(2);
    document.getElementById('balSub').style.color=pnl>=0?'#00c076':'#ff4560';
    document.getElementById('pnl').textContent=(pnl>=0?'+':'')+'$'+pnl.toFixed(2);
    document.getElementById('pnl').style.color=pnl>=0?'#00c076':'#ff4560';
    document.getElementById('pnlPct').textContent=(pnl/100*100).toFixed(2)+'%';

    const wr=d.bets?Math.round(d.wins/d.bets*100):0;
    document.getElementById('wr').textContent=wr+'%';
    document.getElementById('wr').style.color=wr>=60?'#00c076':wr>=40?'#f59e0b':'#ff4560';
    document.getElementById('wrSub').textContent=d.wins+' / '+d.bets+' ставок';

    // ── Status badge ──────────────────────────────────────────
    const sb=document.getElementById('statusBadge');
    if(d.stopped){sb.className='badge badge-red';sb.innerHTML='■ СТОП';}
    else if(d.paused){sb.className='badge badge-muted';sb.innerHTML='⏸ ПАУЗА';}
    else if(d.in_hours){sb.className='badge badge-green';sb.innerHTML='<span class="live-dot"></span>АКТИВЕН';}
    else{sb.className='badge badge-muted';sb.innerHTML='🌙 ВНЕ ЧАСОВ';}
    document.getElementById('srcBadge').textContent=d.price_source||'—';

    // ── Analysis ──────────────────────────────────────────────
    const a=d.last_analysis||{};
    if(a.dir){
      const aUp=a.dir==='UP';
      document.getElementById('quickPred').textContent=aUp?'▲ ВВЕРХ':'▼ ВНИЗ';
      document.getElementById('quickPred').style.color=aUp?'#00c076':'#ff4560';
      document.getElementById('quickConf').textContent='уверенность: '+(a.confidence||0)+'%';

      const pb=document.getElementById('predBadge');
      pb.textContent=aUp?'▲ ВВЕРХ':'▼ ВНИЗ';
      pb.className='pred-badge '+(aUp?'pred-up':'pred-dn');
      document.getElementById('predConf').textContent=(a.confidence||0)+'% уверенность';
      document.getElementById('scoreNum').textContent=(a.score>=0?'+':'')+a.score;

      const sc=a.score||0; const pct=Math.abs(sc)/10*50;
      const sbar=document.getElementById('scoreBar');
      sbar.style.width=pct+'%';
      sbar.style.left=sc>=0?'50%':(50-pct)+'%';
      sbar.style.background=sc>=0?'#00c076':'#ff4560';

      const det=a.details||{};
      document.getElementById('signals').innerHTML=Object.entries(det).map(([k,v])=>`
        <div class="sig-row">
          <span class="sig-key">${k}</span>
          <span class="sig-val" style="color:${v.includes('▲')||v.includes('↗')?'#00c076':v.includes('▼')||v.includes('↘')?'#ff4560':'#6b7a9d'}">${v}</span>
        </div>`).join('');
    }

    // ── Active bet ────────────────────────────────────────────
    const ab=d.active_bet;
    const abEl=document.getElementById('activeBet');
    if(ab){
      const abUp=ab.dir==='UP'; const cur=p;
      const abWin=(abUp&&cur>ab.entry)||(!abUp&&cur<ab.entry);
      const abDiff=cur-ab.entry;
      abEl.innerHTML=`<div class="active-bet">
        <div class="ab-header">
          <span class="ab-title">Ставка #${ab.num} · ${ab.slot}</span>
          <span style="font-size:12px;font-weight:600;color:${abWin?'#00c076':'#ff4560'}">${abWin?'✅ Прибыль':'❌ Убыток'}</span>
        </div>
        <div class="ab-dir" style="color:${abUp?'#00c076':'#ff4560'};margin-bottom:6px">${abUp?'▲ ВВЕРХ':'▼ ВНИЗ'} · $5.00</div>
        <div class="ab-row"><span class="ab-key">Вход</span><span>$${ab.entry.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</span></div>
        <div class="ab-row"><span class="ab-key">Сейчас</span><span style="color:${abDiff>=0?'#00c076':'#ff4560'}">$${cur.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})} (${abDiff>=0?'+':''}${abDiff.toFixed(2)})</span></div>
      </div>`;
    } else {
      abEl.innerHTML='<div style="color:var(--muted);font-size:13px;padding:8px 0">Нет активной ставки</div>';
    }

    // ── P&L chart ─────────────────────────────────────────────
    const hist=d.history||[];
    if(pnlChart&&hist.length){
      const pnls=[0,...hist.map((_,i)=>hist.slice(0,i+1).reduce((a,h)=>a+h.profit,0))];
      pnlChart.data.labels=pnls.map((_,i)=>i);
      pnlChart.data.datasets[0].data=pnls;
      pnlChart.update('none');
    }

    // ── History ───────────────────────────────────────────────
    const wins2=hist.filter(h=>h.won).length;
    document.getElementById('winBadge').textContent=wins2+' побед';
    document.getElementById('lossBadge').textContent=(hist.length-wins2)+' проигр.';
    if(hist.length){
      document.getElementById('histList').innerHTML=[...hist].reverse().slice(0,30).map(h=>`
        <div class="hist-item">
          <div class="hist-icon" style="background:${h.won?'rgba(0,192,118,.12)':'rgba(255,69,96,.12)'};color:${h.won?'#00c076':'#ff4560'}">${h.dir==='UP'?'↑':'↓'}</div>
          <div class="hist-body">
            <div class="hist-title" style="color:${h.won?'#00c076':'#ff4560'}">#${h.num} ${h.dir==='UP'?'ВВЕРХ':'ВНИЗ'} · ${h.slot}</div>
            <div class="hist-sub">$${Math.round(h.entry).toLocaleString()}→$${Math.round(h.exit).toLocaleString()} · conf:${h.conf}% score:${h.score>=0?'+':''}${h.score}</div>
          </div>
          <div class="hist-profit" style="color:${h.won?'#00c076':'#ff4560'}">${h.won?'+':''}$${h.profit.toFixed(2)}</div>
        </div>`).join('');
    }

    prevPrice=p;
  }catch(e){console.error(e)}
}

// Clock + countdown
function tick(){
  const now=new Date();
  const msk=new Date(now.toLocaleString('en-US',{timeZone:'Europe/Moscow'}));
  document.getElementById('clockBadge').textContent=msk.toLocaleTimeString('ru-RU');
  const sec=Math.floor(now/1000); const slot=Math.ceil(sec/900)*900; const left=slot-sec;
  document.getElementById('tMin').textContent=String(Math.floor(left/60)).padStart(2,'0');
  document.getElementById('tSec').textContent=String(left%60).padStart(2,'0');
}

async function control(action){
  if(action==='reset'&&!confirm('Сбросить баланс до $100?')) return;
  await fetch('/api/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action})});
  fetchState();
}

initCharts();
fetchState();
setInterval(fetchState,1000);
setInterval(tick,1000);
tick();
</script>
</body>
</html>"""

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

async def main():
    log.info(f"PolyBot v7 | token={'SET' if TG_TOKEN else 'MISSING'} | port={PORT}")
    Thread(target=run_flask, daemon=True).start()

    app = Application.builder().token(TG_TOKEN).concurrent_updates(False).build()
    for cmd, fn in [("start",cmd_start),("status",cmd_status),("analysis",cmd_analysis),
                    ("bet",cmd_bet),("pause",cmd_pause),("stop",cmd_stop),("reset",cmd_reset)]:
        app.add_handler(CommandHandler(cmd, fn))

    async with app:
        await app.start()
        bot = app.bot
        await bot.set_my_commands([
            BotCommand("status","📊 Статус"),BotCommand("analysis","🔍 Анализ"),
            BotCommand("bet","▶️ Ставить сейчас"),BotCommand("pause","⏸ Пауза"),
            BotCommand("stop","🛑 Стоп"),BotCommand("reset","↺ Сброс"),
        ])
        await load_initial_candles()
        await send_msg(bot,
            "🚀 *PolyBot v7*\n\n"
            "Цена: Binance 15M kline (тот же источник что Polymarket)\n"
            "Стратегия: 6 индикаторов · взвешенное голосование\n"
            "Дашборд: доступен по вашему Railway домену\n\n"
            "Команды в меню 👇"
        )
        log.info("Bot started")
        await asyncio.gather(
            app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message"],
                read_timeout=10, write_timeout=10, connect_timeout=10, pool_timeout=10),
            binance_kline_ws(),
            trading_loop(bot),
            daily_summary(bot),
        )
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
