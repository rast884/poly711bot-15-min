"""
PolyBot v6
Цена: Chainlink BTC/USD на Polygon (тот же оракул что Polymarket)
      wss://polygon-bor-rpc.publicnode.com → latestRoundData()
      Fallback: Coinbase WS → Binance WS
Дашборд: полный редизайн в стиле Polymarket, обновление каждую секунду
"""

import asyncio, json, logging, os, math, struct, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from threading import Thread
from flask import Flask, jsonify, render_template_string

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
    "price": 0.0, "price_source": "—", "price_ts": 0,
    "candles_15m": [], "ticks": [],  # ticks для графика реального времени
    "paused": False, "stopped": False,
    "history": [], "last_analysis": {},
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
def ema(prices, p):
    if not prices: return 0
    k=2/(p+1); e=prices[0]
    for v in prices[1:]: e=v*k+e*(1-k)
    return e

def rsi(closes, p=14):
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
    if len(candles)<2: return 0
    trs=[max(candles[i]["h"]-candles[i]["l"],abs(candles[i]["h"]-candles[i-1]["c"]),abs(candles[i]["l"]-candles[i-1]["c"])) for i in range(1,len(candles))]
    r=trs[-p:]; return sum(r)/len(r) if r else 0

def stoch(candles,k=14):
    if len(candles)<k: return 50
    r=candles[-k:]; lo=min(c["l"] for c in r); hi=max(c["h"] for c in r)
    return (candles[-1]["c"]-lo)/(hi-lo)*100 if hi!=lo else 50

def analyze(candles_15m, price=None):
    if len(candles_15m)<6: return {"dir":"UP","confidence":52,"details":{},"score":0,"rsi":50}
    closes=[c["c"] for c in candles_15m]; p=price or closes[-1]
    score=0; details={}

    # 1. EMA trend (w=2)
    e9=ema(closes,9); e21=ema(closes,21); e55=ema(closes,min(55,len(closes)))
    if e9>e21>e55: score+=2; details["EMA Тренд"]="▲ Сильный бычий (9>21>55)"
    elif e9<e21<e55: score-=2; details["EMA Тренд"]="▼ Сильный медвежий (9<21<55)"
    elif e9>e21: score+=1; details["EMA Тренд"]="↗ Слабый бычий"
    else: score-=1; details["EMA Тренд"]="↘ Слабый медвежий"

    # 2. RSI (w=2)
    r=rsi(closes,14)
    if r<35: score+=2; details["RSI"]=f"{r:.1f} ← перепродан ▲"
    elif r<45: score+=1; details["RSI"]=f"{r:.1f} ← слабый ▲"
    elif r>65: score-=2; details["RSI"]=f"{r:.1f} ← перекуплен ▼"
    elif r>55: score-=1; details["RSI"]=f"{r:.1f} ← слабый ▼"
    else: details["RSI"]=f"{r:.1f} нейтрально"

    # 3. MACD (w=2)
    ml,sl,hist=macd(closes)
    if ml>sl and hist>0: score+=2; details["MACD"]=f"▲ Бычье (hist={hist:+.0f})"
    elif ml<sl and hist<0: score-=2; details["MACD"]=f"▼ Медвежье (hist={hist:+.0f})"
    elif ml>sl: score+=1; details["MACD"]=f"↗ Выше сигнала"
    else: score-=1; details["MACD"]=f"↘ Ниже сигнала"

    # 4. Bollinger (w=1)
    bb_mid,bb_up,bb_lo=bollinger(closes,20)
    pos=(p-bb_lo)/(bb_up-bb_lo)*100 if bb_up!=bb_lo else 50
    if p<bb_lo: score+=1; details["Bollinger"]=f"▲ Ниже нижней (pos={pos:.0f}%)"
    elif p>bb_up: score-=1; details["Bollinger"]=f"▼ Выше верхней (pos={pos:.0f}%)"
    else:
        if pos<35: score+=1; details["Bollinger"]=f"↗ Нижняя зона ({pos:.0f}%)"
        elif pos>65: score-=1; details["Bollinger"]=f"↘ Верхняя зона ({pos:.0f}%)"
        else: details["Bollinger"]=f"Середина ({pos:.0f}%)"

    # 5. Stochastic (w=1)
    sk=stoch(candles_15m,14)
    if sk<20: score+=1; details["Stoch"]=f"K={sk:.0f} ← перепродан ▲"
    elif sk>80: score-=1; details["Stoch"]=f"K={sk:.0f} ← перекуплен ▼"
    else: details["Stoch"]=f"K={sk:.0f} нейтрально"

    # 6. Momentum (w=2)
    if len(candles_15m)>=4:
        last=candles_15m[-4:]
        bull=sum(1 for i in range(1,4) if last[i]["c"]>last[i-1]["c"])
        move=abs(closes[-1]-closes[-4]); a=atr_val(candles_15m,14); rel=move/a if a>0 else 0
        if bull>=3 and rel>0.4: score+=2; details["Моментум"]=f"▲ Сильный бычий ({bull}/3, {rel:.1f}x ATR)"
        elif bull==0 and rel>0.4: score-=2; details["Моментум"]=f"▼ Сильный медвежий ({bull}/3, {rel:.1f}x ATR)"
        elif bull>=2: score+=1; details["Моментум"]=f"↗ Умеренный бычий ({bull}/3)"
        else: score-=1; details["Моментум"]=f"↘ Умеренный медвежий ({bull}/3)"

    conf=min(92,max(52,int(50+abs(score)/10*42)))
    return {"dir":"UP" if score>=0 else "DOWN","confidence":conf,"details":details,"score":score,"rsi":r}

# ── PRICE: CHAINLINK ON POLYGON (exact Polymarket oracle) ─────────────────────
CHAINLINK_BTC_USD_POLYGON = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
]

async def fetch_chainlink_price() -> float:
    """Читает BTC/USD прямо из Chainlink контракта на Polygon — точная цена Polymarket"""
    payload = json.dumps({
        "jsonrpc":"2.0","method":"eth_call",
        "params":[{"to":CHAINLINK_BTC_USD_POLYGON,"data":"0xfedb2b40"},"latest"],
        "id":1
    })
    for rpc in POLYGON_RPCS:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(rpc, data=payload,
                    headers={"Content-Type":"application/json"},
                    timeout=aiohttp.ClientTimeout(total=4)) as r:
                    d=await r.json()
                    result=d.get("result","")
                    if result and len(result)>66:
                        answer_hex=result[2+64:2+128]
                        price=int(answer_hex,16)/1e8
                        if 10000<price<1000000:
                            state["price"]=price
                            state["price_source"]="Chainlink ✓ (Polymarket оракул)"
                            state["price_ts"]=int(time.time())
                            log.info(f"Chainlink BTC/USD: {fmt(price)}")
                            return price
        except Exception as e:
            log.debug(f"RPC {rpc}: {e}")
    return 0.0

async def chainlink_price_loop():
    """Опрашивает Chainlink каждую секунду"""
    while not state["stopped"]:
        p = await fetch_chainlink_price()
        if p>0:
            _push_tick(p)
            _update_15m(p)
        await asyncio.sleep(1)

def _push_tick(price):
    ts=int(time.time()*1000)
    t=state["ticks"]
    t.append({"ts":ts,"p":price})
    # Держим тики за последние 20 минут
    cutoff=ts-20*60*1000
    state["ticks"]=[x for x in t if x["ts"]>cutoff]

def _update_15m(price):
    ts_ms=int(time.time()*1000)
    slot_ts=(ts_ms//(15*60*1000))*(15*60*1000)
    c15=state["candles_15m"]
    if c15 and c15[-1]["t"]==slot_ts:
        c15[-1]["h"]=max(c15[-1]["h"],price)
        c15[-1]["l"]=min(c15[-1]["l"],price)
        c15[-1]["c"]=price
    else:
        c15.append({"t":slot_ts,"o":price,"h":price,"l":price,"c":price})
    if len(c15)>100: state["candles_15m"]=c15[-100:]

async def fallback_price_loop():
    """Coinbase WS → Binance WS как резерв если Chainlink недоступен"""
    while not state["stopped"]:
        # Если Chainlink работал последние 5 сек — не нужен fallback
        if time.time()-state["price_ts"]<5 and state["price"]>0:
            await asyncio.sleep(2); continue

        log.warning("Chainlink unavailable, using Coinbase WS fallback")
        try:
            url="wss://advanced-trade-ws.coinbase.com"
            sub={"type":"subscribe","product_ids":["BTC-USD"],"channel":"ticker"}
            async with websockets.connect(url,ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    if state["stopped"]: return
                    if time.time()-state["price_ts"]<3: break  # Chainlink вернулся
                    d=json.loads(raw)
                    if d.get("channel")=="ticker":
                        for ev in d.get("events",[]):
                            for tick in ev.get("tickers",[]):
                                p=float(tick.get("price",0) or 0)
                                if p>0:
                                    state["price"]=p
                                    state["price_source"]="Coinbase WS (fallback)"
                                    state["price_ts"]=int(time.time())
                                    _push_tick(p); _update_15m(p)
        except Exception as e:
            log.warning(f"Coinbase WS: {e}")

        # Try Binance WS as second fallback
        try:
            async with websockets.connect("wss://stream.binance.com:9443/ws/btcusdt@trade",ping_interval=20) as ws:
                async for raw in ws:
                    if state["stopped"]: return
                    if time.time()-state["price_ts"]<3: break
                    d=json.loads(raw)
                    p=float(d.get("p",0))
                    if p>0:
                        state["price"]=p
                        state["price_source"]="Binance WS (fallback)"
                        state["price_ts"]=int(time.time())
                        _push_tick(p); _update_15m(p)
        except Exception as e:
            log.warning(f"Binance WS: {e}")
            await asyncio.sleep(3)

async def load_initial():
    for rpc in POLYGON_RPCS:
        p=await fetch_chainlink_price()
        if p>0: break
    # Load historical 15m candles from Binance for analysis
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=60",
                             timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status==200:
                    data=await r.json()
                    existing_ts={c["t"] for c in state["candles_15m"]}
                    for k in data:
                        slot_ts=int(k[0])
                        if slot_ts not in existing_ts:
                            state["candles_15m"].append({"t":slot_ts,"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4])})
                    state["candles_15m"].sort(key=lambda x:x["t"])
                    log.info(f"Loaded {len(state['candles_15m'])} historical 15m candles")
    except Exception as e:
        log.warning(f"Initial candles: {e}")

# ── TRADING ───────────────────────────────────────────────────────────────────
async def open_bet(bot):
    if state["paused"] or state["stopped"] or state["balance"]<BET_AMOUNT or state["active_bet"]: return
    price=state["price"]
    if not price: return
    a=analyze(state["candles_15m"],price)
    state["last_analysis"]=a
    direction=a["dir"]
    now=utc_now(); so=slot15(now); sc=so+timedelta(minutes=15)
    state["balance"]-=BET_AMOUNT; state["bets"]+=1
    state["active_bet"]={"dir":direction,"entry":price,"slot_open":so,"slot_close":sc,"num":state["bets"],"analysis":a}
    om=so.astimezone(MSK).strftime("%H:%M"); cm=sc.astimezone(MSK).strftime("%H:%M")
    arrow="🟢 ▲ ВВЕРХ" if direction=="UP" else "🔴 ▼ ВНИЗ"
    sigs="\n".join(f"  • {k}: {v}" for k,v in a["details"].items())
    src=state["price_source"]
    await send_msg(bot,
        f"📊 *Ставка #{state['bets']}*\n\n"
        f"{arrow} · `{fmt(price)}`\n"
        f"_{src}_\n"
        f"Слот: `{om}→{cm} МСК`\n"
        f"Счёт: `{a['score']:+d}/10` · Уверенность: `{a['confidence']}%`\n\n"
        f"*Индикаторы:*\n{sigs}\n\n"
        f"Баланс: `${state['balance']:.2f}`"
    )
    log.info(f"BET #{state['bets']}: {direction} @ {fmt(price)} score={a['score']}")

async def close_bet(bot):
    bet=state["active_bet"]
    if not bet: return
    exit_p=state["price"] or bet["entry"]
    up=exit_p>bet["entry"]
    won=(bet["dir"]=="UP" and up) or (bet["dir"]=="DOWN" and not up)
    profit=BET_AMOUNT*0.88 if won else -BET_AMOUNT
    state["balance"]+=BET_AMOUNT+profit; state["pnl"]+=profit
    state["active_bet"]=None
    if won: state["wins"]+=1
    wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
    om=bet["slot_open"].astimezone(MSK).strftime("%H:%M")
    cm=bet["slot_close"].astimezone(MSK).strftime("%H:%M")
    state["history"].append({"num":bet["num"],"dir":bet["dir"],"entry":bet["entry"],"exit":exit_p,
        "won":won,"profit":profit,"slot":f"{om}-{cm}","score":bet["analysis"].get("score",0),
        "conf":bet["analysis"].get("confidence",0)})
    if len(state["history"])>100: state["history"]=state["history"][-100:]
    await send_msg(bot,
        f"{'✅ ВЫИГРЫШ' if won else '❌ ПРОИГРЫШ'} · #{bet['num']}\n\n"
        f"{'▲ ВВЕРХ' if bet['dir']=='UP' else '▼ ВНИЗ'} · `{om}–{cm}`\n"
        f"Вход: `{fmt(bet['entry'])}` → Выход: `{fmt(exit_p)}`\n"
        f"Прибыль: `{fmtm(profit)}`\n"
        f"Баланс: `${state['balance']:.2f}` · P&L: `{fmtm(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']})"
    )

async def trading_loop(bot):
    await asyncio.sleep(10)
    while not state["stopped"]:
        now=utc_now(); nxt=next_slot(now)
        wait=(nxt-now).total_seconds()
        log.info(f"Next slot: {nxt.astimezone(MSK).strftime('%H:%M МСК')} in {wait:.0f}s")
        await asyncio.sleep(max(0,wait-0.3))
        now=utc_now(); nxt=next_slot(now); p=(nxt-now).total_seconds()
        if p>0: await asyncio.sleep(p)
        if state["stopped"]: break
        log.info(f"SLOT @ {utc_now().astimezone(MSK).strftime('%H:%M МСК')} BTC={fmt(state['price'])}")
        if state["active_bet"]: await close_bet(bot)
        await asyncio.sleep(1)
        if not state["paused"] and in_hours(): await open_bet(bot)

async def daily_summary(bot):
    while not state["stopped"]:
        now=msk_now(); t=now.replace(hour=END_HOUR,minute=0,second=5,microsecond=0)
        if now>=t: t+=timedelta(days=1)
        await asyncio.sleep((t-now).total_seconds())
        if state["stopped"]: break
        wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
        await send_msg(bot,f"🌙 *Итог дня*\n\n💰 `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n🎯 {state['bets']} ставок · Win rate `{wr}%`")

# ── TELEGRAM ─────────────────────────────────────────────────────────────────
async def send_msg(bot,text):
    try: await bot.send_message(chat_id=TG_CHAT_ID,text=text,parse_mode="Markdown")
    except Exception as e: log.error(f"TG: {e}")

async def cmd_start(update,ctx):
    global TG_CHAT_ID; TG_CHAT_ID=str(update.effective_chat.id)
    await update.message.reply_text("🤖 *PolyBot v6*\n\nЦена: Chainlink на Polygon (точная цена Polymarket)\nСтратегия: 6 индикаторов\n\nКоманды в меню 👇",parse_mode="Markdown")

async def cmd_status(update,ctx):
    secs=secs_to_next(); m,sc=divmod(secs,60)
    status="🛑 СТОП" if state["stopped"] else "⏸ ПАУЗА" if state["paused"] else ("✅ АКТИВЕН" if in_hours() else "🌙 ВНЕ ЧАСОВ")
    wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
    ab=state["active_bet"]; bi="нет"
    if ab:
        cur=state["price"]; win=(ab["dir"]=="UP" and cur>ab["entry"]) or (ab["dir"]=="DOWN" and cur<ab["entry"])
        bi=f"{'▲' if ab['dir']=='UP' else '▼'} {fmt(ab['entry'])}→{fmt(cur)} {'✅' if win else '❌'}"
    await update.message.reply_text(
        f"📊 *{msk_now().strftime('%H:%M:%S МСК')}*\n\n"
        f"Статус: `{status}`\n"
        f"BTC: `{fmt(state['price'])}` _{state['price_source']}_\n\n"
        f"💰 `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n"
        f"🎯 `{state['bets']}` ставок · Win rate `{wr}%`\n"
        f"До слота: `{m:02d}:{sc:02d}`\n\n"
        f"Ставка: `{bi}`",parse_mode="Markdown")

async def cmd_analysis(update,ctx):
    a=analyze(state["candles_15m"],state["price"])
    sigs="\n".join(f"• {k}: {v}" for k,v in a["details"].items())
    secs=secs_to_next(); m,s=divmod(secs,60)
    await update.message.reply_text(
        f"🔍 *Анализ · {fmt(state['price'])}*\n\n"
        f"*{'▲ ВВЕРХ' if a['dir']=='UP' else '▼ ВНИЗ'}* · Счёт `{a['score']:+d}/10` · `{a['confidence']}%`\n\n"
        f"{sigs}\n\nДо слота: `{m:02d}:{s:02d}`",parse_mode="Markdown")

async def cmd_bet(update,ctx):
    if not in_hours(): await update.message.reply_text("⛔ 09:00–23:00 МСК"); return
    if state["active_bet"]: await update.message.reply_text("⚠️ Уже открыта ставка"); return
    if state["stopped"]: await update.message.reply_text("🛑 Бот остановлен"); return
    await open_bet(ctx.bot)

async def cmd_pause(update,ctx):
    state["paused"]=not state["paused"]
    await update.message.reply_text("⏸ Пауза" if state["paused"] else "▶️ Возобновлён")

async def cmd_stop(update,ctx):
    state["stopped"]=True; state["paused"]=True
    if state["active_bet"]: await close_bet(ctx.bot)
    wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
    await update.message.reply_text(
        f"🛑 *Остановлен*\n\n`${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\nWin rate `{wr}%`",
        parse_mode="Markdown")

async def cmd_reset(update,ctx):
    state.update({"balance":100.0,"pnl":0.0,"bets":0,"wins":0,"active_bet":None,"paused":False,"stopped":False,"history":[]})
    await update.message.reply_text("↺ Сброс · `$100.00`",parse_mode="Markdown")

# ── WEB DASHBOARD ─────────────────────────────────────────────────────────────
DASHBOARD = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PolyBot</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f7f8fa;--white:#fff;--border:#e8eaed;
  --text:#1a1d23;--muted:#6b7280;
  --green:#16a34a;--green-bg:#f0fdf4;--green-border:#bbf7d0;
  --red:#dc2626;--red-bg:#fef2f2;--red-border:#fecaca;
  --blue:#2563eb;--blue-bg:#eff6ff;
  --orange:#f59e0b;
  --poly-purple:#6d28d9;
}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh}
.topnav{background:var(--white);border-bottom:1px solid var(--border);padding:0 20px;display:flex;align-items:center;justify-content:space-between;height:52px;position:sticky;top:0;z-index:100}
.logo{font-size:15px;font-weight:700;display:flex;align-items:center;gap:8px}
.logo-icon{width:28px;height:28px;background:var(--poly-purple);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px}
.nav-right{display:flex;align-items:center;gap:10px}
.pill{display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;font-size:12px;font-weight:500;border:1px solid}
.pill-g{background:var(--green-bg);color:var(--green);border-color:var(--green-border)}
.pill-r{background:var(--red-bg);color:var(--red);border-color:var(--red-border)}
.pill-m{background:#f3f4f6;color:var(--muted);border-color:var(--border)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.dot-g{background:var(--green);animation:pulse 1.5s infinite}
.dot-r{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.wrap{max-width:1100px;margin:0 auto;padding:16px}
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
.grid-main{display:grid;grid-template-columns:1fr 320px;gap:12px;margin-bottom:12px}
.card{background:var(--white);border:1px solid var(--border);border-radius:12px;padding:16px}
.card-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px;font-weight:500}
.card-value{font-size:24px;font-weight:700;color:var(--text);line-height:1}
.card-sub{font-size:12px;color:var(--muted);margin-top:4px;font-family:'DM Mono',monospace}
/* Price card Polymarket style */
.price-block{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:12px}
.target-section{flex:1}
.target-label{font-size:11px;color:var(--muted);font-weight:500;margin-bottom:2px}
.target-price{font-size:20px;font-weight:700;color:var(--text);font-family:'DM Mono',monospace}
.current-section{flex:1;text-align:right}
.current-label{font-size:11px;font-weight:500;margin-bottom:2px}
.current-price{font-size:20px;font-weight:700;font-family:'DM Mono',monospace}
.timer-block{display:flex;align-items:center;gap:6px;margin-top:2px;justify-content:flex-end}
.timer-num{background:#f3f4f6;border-radius:6px;padding:2px 8px;font-size:18px;font-weight:700;font-family:'DM Mono',monospace;min-width:36px;text-align:center}
.timer-lbl{font-size:9px;color:var(--muted);text-transform:uppercase;text-align:center;margin-top:1px}
.chart-wrap{position:relative;height:160px;margin-bottom:8px}
canvas{display:block}
/* Polymarket-style live chart */
#liveChart{width:100%;height:160px}
/* Analysis panel */
.analysis-row{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #f3f4f6;font-size:12px}
.analysis-row:last-child{border:none}
.a-key{color:var(--muted);font-weight:500}
.a-val{font-family:'DM Mono',monospace;font-size:11px;text-align:right;max-width:55%}
.score-wrap{margin:10px 0 6px}
.score-track{height:6px;background:#f3f4f6;border-radius:6px;position:relative;overflow:hidden}
.score-bar{position:absolute;top:0;height:6px;border-radius:6px;transition:all .4s}
.score-mid{position:absolute;top:-3px;left:50%;width:2px;height:12px;background:var(--border)}
.pred-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:8px;font-size:14px;font-weight:600;margin-bottom:10px}
.pred-up{background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)}
.pred-dn{background:var(--red-bg);color:var(--red);border:1px solid var(--red-border)}
/* Active bet */
.active-bet{background:var(--blue-bg);border:1px solid #bfdbfe;border-radius:10px;padding:12px;margin-top:10px}
.ab-row{display:flex;justify-content:space-between;font-size:12px;margin-top:4px;font-family:'DM Mono',monospace}
.ab-key{color:var(--muted)}
/* History */
.hist-row{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)}
.hist-row:last-child{border:none}
.hist-icon{width:28px;height:28px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0}
.hist-main{flex:1;min-width:0}
.hist-title{font-size:13px;font-weight:500;color:var(--text)}
.hist-sub{font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;margin-top:1px}
.hist-profit{font-size:13px;font-weight:600;font-family:'DM Mono',monospace;white-space:nowrap}
.hlist{max-height:240px;overflow-y:auto}
.hlist::-webkit-scrollbar{width:4px}
.hlist::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
.wr-bar{height:4px;background:#f3f4f6;border-radius:4px;overflow:hidden;margin:8px 0 10px}
.wr-fill{height:100%;background:var(--green);border-radius:4px;transition:width .5s}
.src-tag{font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;margin-top:3px;display:flex;align-items:center;gap:4px}
@media(max-width:750px){.grid4{grid-template-columns:repeat(2,1fr)}.grid-main{grid-template-columns:1fr}}
</style>
</head>
<body>
<nav class="topnav">
  <div class="logo">
    <div class="logo-icon">₿</div>
    PolyBot <span style="color:var(--muted);font-weight:400;margin-left:4px">BTC 15M</span>
  </div>
  <div class="nav-right">
    <span id="srcPill" class="pill pill-m">—</span>
    <span id="statusPill" class="pill pill-g"><span class="dot dot-g"></span>АКТИВЕН</span>
    <span id="clockEl" class="pill pill-m">--:--:--</span>
  </div>
</nav>
<div class="wrap">

<div class="grid4">
  <div class="card">
    <div class="card-label">Баланс</div>
    <div class="card-value" id="balance">$100.00</div>
    <div class="card-sub" id="balSub">— $0.00</div>
  </div>
  <div class="card">
    <div class="card-label">P&L итого</div>
    <div class="card-value" id="pnl">$0.00</div>
    <div class="card-sub" id="pnlPct">0.00%</div>
  </div>
  <div class="card">
    <div class="card-label">Win rate</div>
    <div class="card-value" id="wr">—</div>
    <div class="card-sub" id="wrSub">0 побед / 0 ставок</div>
  </div>
  <div class="card">
    <div class="card-label">Серия</div>
    <div class="card-value" id="streak">—</div>
    <div class="card-sub" id="streakSub">нет данных</div>
  </div>
</div>

<div class="grid-main">
  <div class="card">
    <!-- Polymarket-style price header -->
    <div class="price-block">
      <div class="target-section">
        <div class="target-label">Целевая цена</div>
        <div class="target-price" id="targetPrice">$—</div>
      </div>
      <div class="current-section">
        <div class="current-label" id="currentLabel" style="color:var(--green)">▲ Текущая цена</div>
        <div class="current-price" id="currentPrice" style="color:var(--orange)">$—</div>
        <div style="display:flex;align-items:center;gap:8px;justify-content:flex-end;margin-top:6px">
          <div>
            <div class="timer-num" id="timerM">--</div>
            <div class="timer-lbl">МИН</div>
          </div>
          <div>
            <div class="timer-num" id="timerS">--</div>
            <div class="timer-lbl">СЕК</div>
          </div>
        </div>
      </div>
    </div>
    <div class="src-tag"><span id="priceSrc">—</span></div>
    <div class="chart-wrap" style="margin-top:10px">
      <canvas id="liveChart"></canvas>
    </div>
    <!-- OHLC bar -->
    <div style="display:flex;gap:16px;padding-top:8px;border-top:1px solid var(--border);font-size:11px;font-family:'DM Mono',monospace;color:var(--muted)">
      <span>O: <b id="oO" style="color:var(--text)">—</b></span>
      <span>H: <b id="oH" style="color:var(--green)">—</b></span>
      <span>L: <b id="oL" style="color:var(--red)">—</b></span>
      <span>C: <b id="oC" style="color:var(--text)">—</b></span>
    </div>
  </div>

  <div style="display:flex;flex-direction:column;gap:10px">
    <div class="card">
      <div class="card-label" style="margin-bottom:8px">Анализ ставки</div>
      <div id="predBadge" class="pred-badge pred-up">▲ ВВЕРХ</div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:6px">Счёт: <span id="score">0</span>/10 · Уверенность: <span id="conf">0</span>%</div>
      <div class="score-wrap">
        <div class="score-track">
          <div class="score-mid"></div>
          <div class="score-bar" id="scoreBar"></div>
        </div>
      </div>
      <div id="signals"></div>
      <div id="activeBet"></div>
    </div>
    <div class="card">
      <div class="card-label" style="margin-bottom:6px">P&L кривая</div>
      <div style="height:80px;position:relative"><canvas id="pnlChart"></canvas></div>
    </div>
  </div>
</div>

<div class="card">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
    <div class="card-label" style="margin:0">История ставок</div>
    <div style="display:flex;gap:6px">
      <span id="winTag" class="pill pill-g">0 побед</span>
      <span id="lossTag" class="pill pill-r">0 проигр.</span>
    </div>
  </div>
  <div class="wr-bar"><div class="wr-fill" id="wrFill" style="width:50%"></div></div>
  <div class="hlist" id="history"><div style="color:var(--muted);text-align:center;padding:20px;font-size:13px">Нет ставок</div></div>
</div>

</div><!-- /wrap -->

<script>
let liveChartObj=null, pnlChartObj=null;
let prevPrice=0, tickData=[], pnlData=[0];

function initCharts(){
  // Live price chart - Polymarket style (orange line)
  liveChartObj=new Chart(document.getElementById('liveChart'),{
    type:'line',
    data:{labels:[],datasets:[{
      data:[],borderColor:'#f59e0b',borderWidth:2,fill:true,
      backgroundColor:'rgba(245,158,11,0.08)',pointRadius:0,tension:0.1
    }]},
    options:{responsive:true,maintainAspectRatio:false,animation:{duration:0},
      plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,
        callbacks:{label:c=>'$'+Math.round(c.raw).toLocaleString('en-US')}}},
      scales:{
        x:{display:false},
        y:{position:'right',grid:{color:'rgba(0,0,0,0.04)'},
          ticks:{color:'#9ca3af',font:{size:10,family:'DM Mono'},
            callback:v=>'$'+Math.round(v).toLocaleString('en-US')}}
      }
    }
  });

  pnlChartObj=new Chart(document.getElementById('pnlChart'),{
    type:'line',
    data:{labels:['0'],datasets:[{data:[0],
      borderColor:'#16a34a',borderWidth:1.5,fill:true,
      backgroundColor:'rgba(22,163,74,0.06)',pointRadius:0,tension:0.3,
      segment:{borderColor:ctx=>ctx.p0.parsed.y<0?'#dc2626':'#16a34a'}}]},
    options:{responsive:true,maintainAspectRatio:false,animation:{duration:200},
      plugins:{legend:{display:false}},
      scales:{x:{display:false},y:{display:false}}}
  });
}

async function fetchAndUpdate(){
  try{
    const d=await fetch('/api/state').then(r=>r.json());
    const p=d.price||0;

    // ── Price display (Polymarket style) ──
    document.getElementById('currentPrice').textContent=p?'$'+p.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'$—';
    document.getElementById('priceSrc').textContent=d.price_source||'—';

    // Target price = slot open price (active bet entry) or last known
    const ab=d.active_bet;
    const targetP=ab?ab.entry:p;
    document.getElementById('targetPrice').textContent=targetP?'$'+targetP.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}):'$—';

    // Direction indicator
    const diff=p-targetP;
    const up=diff>=0;
    document.getElementById('currentLabel').textContent=(up?'▲ ':'▼ ')+'Текущая цена +$'+Math.abs(diff).toFixed(0);
    document.getElementById('currentLabel').style.color=up?'#16a34a':'#dc2626';
    document.getElementById('currentPrice').style.color='#f59e0b';

    // Live ticks chart
    const ticks=d.ticks||[];
    if(ticks.length>1){
      const labels=ticks.map(t=>new Date(t.ts).toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit',second:'2-digit'}));
      const prices=ticks.map(t=>t.p);
      liveChartObj.data.labels=labels;
      liveChartObj.data.datasets[0].data=prices;
      // Color based on direction vs target
      const isUp=prices[prices.length-1]>=prices[0];
      liveChartObj.data.datasets[0].borderColor=isUp?'#f59e0b':'#f59e0b';
      // Add target line annotation-style as horizontal reference
      liveChartObj.update('none');
    }

    // OHLC of last 15m candle
    const c15=d.candles_15m||[];
    if(c15.length){
      const last=c15[c15.length-1];
      document.getElementById('oO').textContent='$'+Math.round(last.o).toLocaleString('en-US');
      document.getElementById('oH').textContent='$'+Math.round(last.h).toLocaleString('en-US');
      document.getElementById('oL').textContent='$'+Math.round(last.l).toLocaleString('en-US');
      document.getElementById('oC').textContent='$'+Math.round(last.c).toLocaleString('en-US');
    }

    // ── Metrics ──
    const bal=d.balance, pnl=d.pnl;
    document.getElementById('balance').textContent='$'+bal.toFixed(2);
    document.getElementById('balance').style.color=bal>=100?'#1a1d23':'#dc2626';
    document.getElementById('balSub').textContent=(pnl>=0?'+ ':'- ')+'$'+Math.abs(pnl).toFixed(2);
    document.getElementById('balSub').style.color=pnl>=0?'#16a34a':'#dc2626';
    document.getElementById('pnl').textContent=(pnl>=0?'+':'')+'$'+pnl.toFixed(2);
    document.getElementById('pnl').style.color=pnl>=0?'#16a34a':'#dc2626';
    document.getElementById('pnlPct').textContent=(pnl/100*100).toFixed(2)+'%';

    const wr=d.bets?Math.round(d.wins/d.bets*100):0;
    document.getElementById('wr').textContent=wr+'%';
    document.getElementById('wr').style.color=wr>=60?'#16a34a':wr>=40?'#f59e0b':'#dc2626';
    document.getElementById('wrSub').textContent=d.wins+' побед / '+d.bets+' ставок';

    // Streak
    const hist=d.history||[];
    if(hist.length){
      let streak=1;
      for(let i=hist.length-2;i>=0;i--){if(hist[i].won===hist[hist.length-1].won)streak++;else break;}
      const lastWon=hist[hist.length-1].won;
      document.getElementById('streak').textContent=(lastWon?'✅':'❌')+' '+streak;
      document.getElementById('streak').style.color=lastWon?'#16a34a':'#dc2626';
      document.getElementById('streakSub').textContent=lastWon?streak+' побед подряд':streak+' проигрышей подряд';
    }

    // ── Status ──
    const sp=document.getElementById('statusPill');
    if(d.stopped){sp.className='pill pill-r';sp.innerHTML='<span class="dot dot-r"></span>СТОП';}
    else if(d.paused){sp.className='pill pill-m';sp.innerHTML='⏸ ПАУЗА';}
    else if(d.in_hours){sp.className='pill pill-g';sp.innerHTML='<span class="dot dot-g"></span>АКТИВЕН';}
    else{sp.className='pill pill-m';sp.innerHTML='🌙 ВНЕ ЧАСОВ';}
    document.getElementById('srcPill').textContent=d.price_source||'—';

    // ── Analysis ──
    const a=d.last_analysis||{};
    if(a.dir){
      const aUp=a.dir==='UP';
      const pb=document.getElementById('predBadge');
      pb.textContent=(aUp?'▲ ВВЕРХ':'▼ ВНИЗ');
      pb.className='pred-badge '+(aUp?'pred-up':'pred-dn');
      document.getElementById('score').textContent=(a.score>=0?'+':'')+a.score;
      document.getElementById('conf').textContent=a.confidence||0;
      // Score bar
      const sc=a.score||0; const pct=Math.abs(sc)/10*50;
      const sb=document.getElementById('scoreBar');
      sb.style.width=pct+'%';
      sb.style.left=sc>=0?'50%':(50-pct)+'%';
      sb.style.background=sc>=0?'#16a34a':'#dc2626';
      // Signals
      const det=a.details||{};
      document.getElementById('signals').innerHTML=Object.entries(det).map(([k,v])=>`
        <div class="analysis-row">
          <span class="a-key">${k}</span>
          <span class="a-val" style="color:${v.includes('▲')||v.includes('↗')?'#16a34a':v.includes('▼')||v.includes('↘')?'#dc2626':'#6b7280'}">${v}</span>
        </div>`).join('');
    }

    // ── Active bet ──
    const abEl=document.getElementById('activeBet');
    if(ab){
      const cur=p; const abUp=ab.dir==='UP';
      const abWin=(abUp&&cur>ab.entry)||(!abUp&&cur<ab.entry);
      const abDiff=cur-ab.entry;
      abEl.innerHTML=`<div class="active-bet">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
          <span style="font-size:11px;color:#3b82f6;font-weight:600;text-transform:uppercase;letter-spacing:.5px">Открытая ставка #${ab.num}</span>
          <span style="font-size:12px;font-weight:600;color:${abWin?'#16a34a':'#dc2626'}">${abWin?'✅ Выигрываем':'❌ Проигрываем'}</span>
        </div>
        <div class="ab-row"><span class="ab-key">Направление</span><span style="color:${abUp?'#16a34a':'#dc2626'};font-weight:600">${abUp?'▲ ВВЕРХ':'▼ ВНИЗ'}</span></div>
        <div class="ab-row"><span class="ab-key">Вход</span><span>$${Math.round(ab.entry).toLocaleString()}</span></div>
        <div class="ab-row"><span class="ab-key">Сейчас</span><span style="color:${abDiff>=0?'#16a34a':'#dc2626'}">$${Math.round(cur).toLocaleString()} (${abDiff>=0?'+':''}${abDiff.toFixed(0)})</span></div>
      </div>`;
    } else { abEl.innerHTML=''; }

    // ── P&L chart ──
    if(pnlChartObj&&hist.length){
      const pnls=[0,...hist.map((_,i)=>hist.slice(0,i+1).reduce((a,h)=>a+h.profit,0))];
      pnlChartObj.data.labels=pnls.map((_,i)=>i);
      pnlChartObj.data.datasets[0].data=pnls;
      pnlChartObj.update('none');
    }

    // ── History ──
    const wins2=hist.filter(h=>h.won).length; const losses2=hist.length-wins2;
    document.getElementById('winTag').textContent=wins2+' побед';
    document.getElementById('lossTag').textContent=losses2+' проигр.';
    const wrPct=hist.length?wins2/hist.length*100:50;
    document.getElementById('wrFill').style.width=wrPct+'%';
    if(hist.length){
      document.getElementById('history').innerHTML=[...hist].reverse().slice(0,25).map(h=>`
        <div class="hist-row">
          <div class="hist-icon" style="background:${h.won?'#f0fdf4':'#fef2f2'};color:${h.won?'#16a34a':'#dc2626'}">${h.dir==='UP'?'↑':'↓'}</div>
          <div class="hist-main">
            <div class="hist-title">#${h.num} ${h.dir==='UP'?'ВВЕРХ':'ВНИЗ'} · ${h.slot}</div>
            <div class="hist-sub">$${Math.round(h.entry).toLocaleString()}→$${Math.round(h.exit).toLocaleString()} · conf:${h.conf}% score:${h.score>=0?'+':''}${h.score}</div>
          </div>
          <div class="hist-profit" style="color:${h.won?'#16a34a':'#dc2626'}">${h.won?'+':''}$${h.profit.toFixed(2)}</div>
        </div>`).join('');
    }
  }catch(e){console.error(e)}
}

// Clock + countdown
function tick(){
  const now=new Date();
  const msk=new Date(now.toLocaleString('en-US',{timeZone:'Europe/Moscow'}));
  document.getElementById('clockEl').textContent=msk.toLocaleTimeString('ru-RU')+' МСК';
  const sec=Math.floor(now/1000); const slot=Math.ceil(sec/900)*900;
  const left=slot-sec;
  document.getElementById('timerM').textContent=String(Math.floor(left/60)).padStart(2,'0');
  document.getElementById('timerS').textContent=String(left%60).padStart(2,'0');
}

initCharts();
fetchAndUpdate();
setInterval(fetchAndUpdate,1000);
setInterval(tick,1000);
tick();
</script>
</body></html>"""

flask_app=Flask(__name__)

@flask_app.route("/")
def dashboard(): return DASHBOARD

@flask_app.route("/api/state")
def api_state():
    now=utc_now(); nxt=next_slot(now)
    ab=state["active_bet"]
    a=analyze(state["candles_15m"],state["price"])
    state["last_analysis"]=a
    return jsonify({
        "balance":state["balance"],"pnl":state["pnl"],
        "bets":state["bets"],"wins":state["wins"],
        "price":state["price"],"price_source":state["price_source"],
        "paused":state["paused"],"stopped":state["stopped"],"in_hours":in_hours(),
        "ticks":state["ticks"][-120:],
        "candles_15m":state["candles_15m"][-5:],
        "active_bet":{"num":ab["num"],"dir":ab["dir"],"entry":ab["entry"]} if ab else None,
        "last_analysis":a,
        "history":state["history"],
    })

def run_flask():
    flask_app.run(host="0.0.0.0",port=PORT,debug=False,use_reloader=False)

async def main():
    log.info(f"PolyBot v6 | token={'SET' if TG_TOKEN else 'MISSING'} | port={PORT}")
    Thread(target=run_flask,daemon=True).start()
    app=(Application.builder().token(TG_TOKEN).concurrent_updates(False).build())
    for cmd,fn in [("start",cmd_start),("status",cmd_status),("analysis",cmd_analysis),
                   ("bet",cmd_bet),("pause",cmd_pause),("stop",cmd_stop),("reset",cmd_reset)]:
        app.add_handler(CommandHandler(cmd,fn))
    async with app:
        await app.start()
        bot=app.bot
        await bot.set_my_commands([
            BotCommand("status","📊 Статус"),BotCommand("analysis","🔍 Анализ"),
            BotCommand("bet","▶️ Ставить сейчас"),BotCommand("pause","⏸ Пауза"),
            BotCommand("stop","🛑 Стоп"),BotCommand("reset","↺ Сброс"),
        ])
        await load_initial()
        await send_msg(bot,"🚀 *PolyBot v6*\n\nЦена: Chainlink на Polygon (точная цена Polymarket)\nСтратегия: 6 индикаторов · взвешенное голосование\n\nKоманды в меню 👇")
        await asyncio.gather(
            app.updater.start_polling(drop_pending_updates=True,allowed_updates=["message"],
                read_timeout=10,write_timeout=10,connect_timeout=10,pool_timeout=10),
            chainlink_price_loop(),
            fallback_price_loop(),
            trading_loop(bot),
            daily_summary(bot),
        )
        await app.updater.stop()
        await app.stop()

if __name__=="__main__":
    asyncio.run(main())
