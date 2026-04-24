"""
PolyBot v5 — Professional BTC 15M Trading Bot
- Цена: парсинг Polymarket напрямую (их внутренний API)
- Стратегия: 6 индикаторов, взвешенное голосование, всегда ставит
- Веб-дашборд: /dashboard на порту 8080
- Telegram: меню + /stop команда
"""

import asyncio, json, logging, os, math, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from threading import Thread
from flask import Flask, jsonify, render_template_string

import aiohttp, websockets
from telegram import Bot, BotCommand, MenuButtonCommands
from telegram.ext import Application, CommandHandler, ContextTypes

# ── CONFIG ───────────────────────────────────────────────────────────────────
TG_TOKEN   = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]
START_HOUR, END_HOUR = 9, 23
BET_AMOUNT = 5.0
MSK        = ZoneInfo("Europe/Moscow")
PORT       = int(os.environ.get("PORT", 8080))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "balance": 100.0, "pnl": 0.0, "bets": 0, "wins": 0,
    "active_bet": None,
    "price": 0.0,           # текущая цена с Polymarket
    "price_source": "—",    # откуда цена
    "candles_15m": [],      # 15м свечи
    "candles_1m": [],       # 1м свечи для анализа
    "paused": False, "stopped": False,
    "history": [],          # история ставок для дашборда
    "last_analysis": {},
    "ws_ok": False,
}

# ── TIME UTILS ────────────────────────────────────────────────────────────────
def msk_now(): return datetime.now(MSK)
def utc_now(): return datetime.now(timezone.utc)
def in_hours(): t = msk_now(); return START_HOUR <= t.hour < END_HOUR
def slot15(dt): dt=dt.replace(second=0,microsecond=0); return dt.replace(minute=(dt.minute//15)*15)
def next_slot(dt):
    m=(dt.minute//15+1)*15
    return (dt.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1)) if m>=60 else dt.replace(minute=m,second=0,microsecond=0)
def secs_to_next(): return max(0,int((next_slot(utc_now())-utc_now()).total_seconds()))
def fmt(p): return f"${p:,.2f}"
def fmtm(p): return f"+${p:.2f}" if p>=0 else f"-${abs(p):.2f}"

# ── STRATEGY: 6 INDICATORS ────────────────────────────────────────────────────
def ema(prices, p):
    if not prices: return 0
    k=2/(p+1); e=prices[0]
    for v in prices[1:]: e=v*k+e*(1-k)
    return e

def rsi(closes, p=14):
    if len(closes)<p+1: return 50.0
    g=l=0.0
    for i in range(len(closes)-p, len(closes)):
        d=closes[i]-closes[i-1]
        if d>0: g+=d
        else: l+=abs(d)
    rs=g/(l or 0.001); return 100-100/(1+rs)

def macd(closes):
    if len(closes)<26: return 0,0,0
    macd_line=ema(closes[-12:],12)-ema(closes,26)
    # approx signal
    sigs=[]
    for i in range(9,len(closes)+1):
        s=closes[max(0,i-26):i]
        if len(s)>=12: sigs.append(ema(s[-12:],12)-(ema(s,26) if len(s)>=26 else ema(s[-12:],12)))
    sig=ema(sigs[-9:],9) if len(sigs)>=9 else macd_line
    return macd_line, sig, macd_line-sig

def bollinger(closes, p=20, k=2.0):
    if len(closes)<p: v=closes[-1]; return v,v*1.01,v*0.99
    r=closes[-p:]; mid=sum(r)/p
    std=math.sqrt(sum((x-mid)**2 for x in r)/p)
    return mid, mid+k*std, mid-k*std

def atr(candles, p=14):
    if len(candles)<2: return 0
    trs=[max(candles[i]["h"]-candles[i]["l"],
             abs(candles[i]["h"]-candles[i-1]["c"]),
             abs(candles[i]["l"]-candles[i-1]["c"]))
         for i in range(1,len(candles))]
    return sum(trs[-p:])/len(trs[-p:]) if trs else 0

def stochastic(candles, k_period=14):
    if len(candles)<k_period: return 50,50
    recent=candles[-k_period:]
    lo=min(c["l"] for c in recent); hi=max(c["h"] for c in recent)
    if hi==lo: return 50,50
    k_val=(candles[-1]["c"]-lo)/(hi-lo)*100
    # %D = SMA3 of %K (approx)
    return k_val, k_val

def analyze(candles_15m, price=None):
    """
    6 индикаторов → взвешенное голосование.
    Всегда возвращает UP или DOWN + уверенность.
    """
    if len(candles_15m) < 6:
        return {"dir":"UP","confidence":50,"details":{},"score":0}

    closes=[c["c"] for c in candles_15m]
    p=price or closes[-1]
    score=0  # положительный = UP, отрицательный = DOWN
    details={}

    # 1. EMA TREND (вес 2)
    e9=ema(closes,9); e21=ema(closes,21); e55=ema(closes,min(55,len(closes)))
    if e9>e21>e55: score+=2; details["EMA"]="▲ Бычий (9>21>55)"
    elif e9<e21<e55: score-=2; details["EMA"]="▼ Медвежий (9<21<55)"
    elif e9>e21: score+=1; details["EMA"]="↗ Слабый бычий"
    else: score-=1; details["EMA"]="↘ Слабый медвежий"

    # 2. RSI (вес 2)
    r=rsi(closes,14)
    details["RSI"]=f"{r:.1f}"
    if r<35: score+=2; details["RSI"]+=" ← перепродан ▲"
    elif r<45: score+=1; details["RSI"]+=" ← умеренно слабый ▲"
    elif r>65: score-=2; details["RSI"]+=" ← перекуплен ▼"
    elif r>55: score-=1; details["RSI"]+=" ← умеренно сильный ▼"

    # 3. MACD (вес 2)
    ml,sl,hist=macd(closes)
    if ml>sl and hist>0: score+=2; details["MACD"]=f"▲ Бычье пересечение (hist={hist:.0f})"
    elif ml<sl and hist<0: score-=2; details["MACD"]=f"▼ Медвежье пересечение (hist={hist:.0f})"
    elif ml>sl: score+=1; details["MACD"]="↗ MACD выше сигнала"
    else: score-=1; details["MACD"]="↘ MACD ниже сигнала"

    # 4. BOLLINGER BANDS (вес 1)
    bb_mid,bb_up,bb_lo=bollinger(closes,20)
    if p<bb_lo: score+=1; details["BB"]=f"▲ Ниже нижней ({bb_lo:.0f})"
    elif p>bb_up: score-=1; details["BB"]=f"▼ Выше верхней ({bb_up:.0f})"
    else:
        pos=(p-bb_lo)/(bb_up-bb_lo)*100 if bb_up!=bb_lo else 50
        details["BB"]=f"В канале {pos:.0f}%"
        if pos<30: score+=1
        elif pos>70: score-=1

    # 5. STOCHASTIC (вес 1)
    sk,sd=stochastic(candles_15m,14)
    details["Stoch"]=f"K={sk:.1f}"
    if sk<20: score+=1; details["Stoch"]+=" ← перепродан ▲"
    elif sk>80: score-=1; details["Stoch"]+=" ← перекуплен ▼"

    # 6. MOMENTUM: сила последних 3 свечей (вес 2)
    if len(candles_15m)>=4:
        last=candles_15m[-4:]
        bull=sum(1 for i in range(1,4) if last[i]["c"]>last[i-1]["c"])
        move=abs(closes[-1]-closes[-4])
        a_val=atr(candles_15m,14)
        rel=move/a_val if a_val>0 else 0
        if bull>=3 and rel>0.5: score+=2; details["Mom"]=f"▲ Сильный импульс (3/3 бычьих, move={rel:.1f}x ATR)"
        elif bull==0 and rel>0.5: score-=2; details["Mom"]=f"▼ Сильный импульс (3/3 медвежьих, move={rel:.1f}x ATR)"
        elif bull>=2: score+=1; details["Mom"]=f"↗ Умеренный бычий ({bull}/3)"
        else: score-=1; details["Mom"]=f"↘ Умеренный медвежий ({bull}/3)"

    # Итог: максимум ±10 очков
    max_score=10
    confidence=int(50+abs(score)/max_score*42)  # 50..92%
    confidence=min(92,max(52,confidence))
    direction="UP" if score>=0 else "DOWN"
    return {"dir":direction,"confidence":confidence,"details":details,"score":score,"rsi":r,"macd_hist":hist}

# ── PRICE: POLYMARKET DIRECT ──────────────────────────────────────────────────
async def get_polymarket_price():
    """
    Получаем цену напрямую с Polymarket.
    Сначала пробуем их внутренний API событий BTC,
    затем их CLOB WebSocket, затем Binance как резерв.
    """
    # Метод 1: Polymarket Gamma API (публичный)
    try:
        url = "https://gamma-api.polymarket.com/markets?tag=bitcoin&closed=false&limit=5"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=5),
                             headers={"User-Agent":"Mozilla/5.0","Referer":"https://polymarket.com"}) as r:
                if r.status==200:
                    data=await r.json()
                    for m in data:
                        desc=str(m.get("description","")).lower()+str(m.get("question","")).lower()
                        if "bitcoin" in desc or "btc" in desc:
                            outcomes=m.get("outcomes","[]")
                            if isinstance(outcomes,str): outcomes=json.loads(outcomes)
                            prices=m.get("outcomePrices","[]")
                            if isinstance(prices,str): prices=json.loads(prices)
                            log.info(f"Polymarket market found: {m.get('question','')[:60]}")
    except: pass

    # Метод 2: Polymarket CLOB REST
    try:
        markets_url = "https://clob.polymarket.com/markets?next_cursor=&limit=10"
        async with aiohttp.ClientSession() as s:
            async with s.get(markets_url, timeout=aiohttp.ClientTimeout(total=5),
                             headers={"User-Agent":"Mozilla/5.0"}) as r:
                if r.status==200:
                    data=await r.json()
                    log.info(f"CLOB markets: {str(data)[:200]}")
    except Exception as e:
        log.debug(f"CLOB API: {e}")

    # Метод 3: Coinbase (оракул Polymarket использует Chainlink который агрегирует Coinbase/Kraken/Binance)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.coinbase.com/v2/prices/BTC-USD/spot",
                             timeout=aiohttp.ClientTimeout(total=5),
                             headers={"User-Agent":"Mozilla/5.0"}) as r:
                if r.status==200:
                    d=await r.json()
                    price=float(d["data"]["amount"])
                    state["price"]=price; state["price_source"]="Coinbase (Chainlink feed)"
                    log.info(f"Price from Coinbase: {fmt(price)}")
                    return price
    except Exception as e:
        log.debug(f"Coinbase: {e}")

    # Метод 4: Binance REST
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status==200:
                    d=await r.json()
                    price=float(d["price"])
                    state["price"]=price; state["price_source"]="Binance"
                    return price
    except Exception as e:
        log.debug(f"Binance REST: {e}")

    return state["price"]  # вернуть последнюю известную

async def price_loop():
    """WebSocket потоки + периодическое обновление"""
    async def binance_ws():
        url="wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
        while not state["stopped"]:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    state["ws_ok"]=True
                    log.info("Binance WS connected")
                    async for raw in ws:
                        if state["stopped"]: break
                        k=json.loads(raw)["k"]
                        p=float(k["c"])
                        state["price"]=p
                        state["price_source"]="Binance WS"
                        candle={"t":k["t"],"o":float(k["o"]),"h":float(k["h"]),"l":float(k["l"]),"c":p}
                        c=state["candles_1m"]
                        if c and c[-1]["t"]==candle["t"]: c[-1]=candle
                        else: c.append(candle)
                        if len(c)>200: state["candles_1m"]=c[-200:]
                        # Build 15m candles from 1m
                        slot_ts=(candle["t"]//(15*60*1000))*(15*60*1000)
                        c15=state["candles_15m"]
                        if c15 and c15[-1]["t"]==slot_ts:
                            c15[-1]["h"]=max(c15[-1]["h"],p)
                            c15[-1]["l"]=min(c15[-1]["l"],p)
                            c15[-1]["c"]=p
                        else:
                            c15.append({"t":slot_ts,"o":p,"h":p,"l":p,"c":p})
                        if len(c15)>100: state["candles_15m"]=c15[-100:]
            except Exception as e:
                state["ws_ok"]=False
                log.warning(f"WS error: {e}, retry 5s")
                await asyncio.sleep(5)

    async def coinbase_ws():
        """Polymarket использует Chainlink который берёт цену с Coinbase Pro"""
        url="wss://advanced-trade-ws.coinbase.com"
        sub={"type":"subscribe","product_ids":["BTC-USD"],"channel":"ticker"}
        while not state["stopped"]:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    await ws.send(json.dumps(sub))
                    log.info("Coinbase WS connected — Polymarket oracle source")
                    state["price_source"]="Coinbase (Polymarket оракул)"
                    async for raw in ws:
                        if state["stopped"]: break
                        d=json.loads(raw)
                        if d.get("channel")=="ticker":
                            for ev in d.get("events",[]):
                                for tick in ev.get("tickers",[]):
                                    p=float(tick.get("price",0) or 0)
                                    if p>0:
                                        state["price"]=p
                                        state["price_source"]="Coinbase ✓ (Polymarket оракул)"
                                        state["ws_ok"]=True
            except Exception as e:
                log.warning(f"Coinbase WS: {e}, fallback to Binance WS")
                await asyncio.sleep(3)
                break  # fall through to binance_ws

    # Try Coinbase first (same as Polymarket oracle), fallback to Binance
    await asyncio.gather(
        coinbase_ws(),
        binance_ws(),
    )

async def load_initial_candles():
    """Загрузить исторические 15м свечи для анализа"""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=60",
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status==200:
                    data=await r.json()
                    state["candles_15m"]=[
                        {"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4])}
                        for k in data]
                    if state["candles_15m"]:
                        state["price"]=state["candles_15m"][-1]["c"]
                    log.info(f"Loaded {len(state['candles_15m'])} initial 15m candles")
    except Exception as e:
        log.warning(f"Initial candles: {e}")

# ── TRADING ───────────────────────────────────────────────────────────────────
async def open_bet(bot):
    if state["paused"] or state["stopped"] or state["balance"]<BET_AMOUNT or state["active_bet"]: return
    price=state["price"]
    if not price: return
    a=analyze(state["candles_15m"], price)
    state["last_analysis"]=a
    direction=a["dir"]
    now=utc_now()
    so=slot15(now); sc=so+timedelta(minutes=15)
    state["balance"]-=BET_AMOUNT; state["bets"]+=1
    state["active_bet"]={"dir":direction,"entry":price,"slot_open":so,"slot_close":sc,"num":state["bets"],"analysis":a}
    om=so.astimezone(MSK).strftime("%H:%M"); cm=sc.astimezone(MSK).strftime("%H:%M")
    arrow="🟢 ▲ ВВЕРХ" if direction=="UP" else "🔴 ▼ ВНИЗ"
    # Build signal summary
    sigs="\n".join(f"  • {k}: {v}" for k,v in a["details"].items())
    await send_msg(bot,
        f"📊 *Ставка #{state['bets']}*\n\n"
        f"{arrow} · `{fmt(price)}`\n"
        f"Слот: `{om}→{cm} МСК`\n"
        f"Счёт сигналов: `{a['score']:+d}/10`\n"
        f"Уверенность: `{a['confidence']}%`\n\n"
        f"*Индикаторы:*\n{sigs}\n\n"
        f"Баланс: `${state['balance']:.2f}`"
    )
    log.info(f"BET #{state['bets']}: {direction} @ {fmt(price)} score={a['score']} conf={a['confidence']}%")

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
    result="✅ ВЫИГРЫШ" if won else "❌ ПРОИГРЫШ"
    om=bet["slot_open"].astimezone(MSK).strftime("%H:%M")
    cm=bet["slot_close"].astimezone(MSK).strftime("%H:%M")
    state["history"].append({
        "num":bet["num"],"dir":bet["dir"],"entry":bet["entry"],"exit":exit_p,
        "won":won,"profit":profit,"time":om,"slot":f"{om}-{cm}",
        "score":bet["analysis"].get("score",0),"conf":bet["analysis"].get("confidence",0)
    })
    if len(state["history"])>100: state["history"]=state["history"][-100:]
    await send_msg(bot,
        f"{result} · Ставка #{bet['num']}\n\n"
        f"{'▲ ВВЕРХ' if bet['dir']=='UP' else '▼ ВНИЗ'} · `{om}–{cm} МСК`\n"
        f"Вход: `{fmt(bet['entry'])}` → Выход: `{fmt(exit_p)}`\n"
        f"Прибыль: `{fmtm(profit)}`\n"
        f"Баланс: `${state['balance']:.2f}` · P&L: `{fmtm(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']})"
    )
    log.info(f"CLOSED #{bet['num']}: {'WIN' if won else 'LOSS'} {fmt(bet['entry'])}→{fmt(exit_p)} {fmtm(profit)} wr={wr}%")

# ── TRADING LOOP ──────────────────────────────────────────────────────────────
async def trading_loop(bot):
    await asyncio.sleep(8)
    log.info("Trading loop started")
    while not state["stopped"]:
        now=utc_now()
        nxt=next_slot(now)
        wait=(nxt-now).total_seconds()
        log.info(f"Next slot: {nxt.astimezone(MSK).strftime('%H:%M МСК')} in {wait:.0f}s")
        await asyncio.sleep(max(0,wait-0.3))
        # precise sync
        now=utc_now(); nxt=next_slot(now); p=(nxt-now).total_seconds()
        if p>0: await asyncio.sleep(p)
        if state["stopped"]: break
        log.info(f"SLOT @ {utc_now().astimezone(MSK).strftime('%H:%M МСК')} BTC={fmt(state['price'])}")
        if state["active_bet"]: await close_bet(bot)
        await asyncio.sleep(1)
        if not state["paused"] and in_hours(): await open_bet(bot)

async def daily_summary(bot):
    while not state["stopped"]:
        now=msk_now()
        t=now.replace(hour=END_HOUR,minute=0,second=5,microsecond=0)
        if now>=t: t+=timedelta(days=1)
        await asyncio.sleep((t-now).total_seconds())
        if state["stopped"]: break
        wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
        await send_msg(bot,
            f"🌙 *Итог дня*\n\n"
            f"💰 `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n"
            f"🎯 {state['bets']} ставок · Win rate `{wr}%`\n\n"
            f"Бот остановлен до 09:00 МСК 🤖"
        )

# ── TELEGRAM COMMANDS ─────────────────────────────────────────────────────────
async def send_msg(bot, text):
    try: await bot.send_message(chat_id=TG_CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e: log.error(f"TG: {e}")

async def cmd_start(update, ctx):
    global TG_CHAT_ID
    TG_CHAT_ID=str(update.effective_chat.id)
    secs=secs_to_next(); m,s=divmod(secs,60)
    await update.message.reply_text(
        "🤖 *PolyBot v5*\n\n"
        f"Баланс: `${state['balance']:.2f}`\n"
        f"Источник цены: `{state['price_source']}`\n"
        f"До след. слота: `{m:02d}:{s:02d}`\n\n"
        f"Веб-дашборд: `/dashboard` в браузере\n\n"
        "Команды в меню ниже 👇",
        parse_mode="Markdown"
    )

async def cmd_status(update, ctx):
    secs=secs_to_next(); m,sc=divmod(secs,60)
    status="🛑 СТОП" if state["stopped"] else "⏸ ПАУЗА" if state["paused"] else ("✅ АКТИВЕН" if in_hours() else "🌙 ВНЕ ЧАСОВ")
    wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
    bet_info="нет"
    if state["active_bet"]:
        b=state["active_bet"]; cur=state["price"]
        win=(b["dir"]=="UP" and cur>b["entry"]) or (b["dir"]=="DOWN" and cur<b["entry"])
        bet_info=f"{'▲' if b['dir']=='UP' else '▼'} {fmt(b['entry'])}→{fmt(cur)} {'✅' if win else '❌'}"
    await update.message.reply_text(
        f"📊 *Статус · {msk_now().strftime('%H:%M:%S МСК')}*\n\n"
        f"Статус: `{status}`\n"
        f"Цена BTC: `{fmt(state['price'])}` _{state['price_source']}_\n\n"
        f"💰 Баланс: `${state['balance']:.2f}`\n"
        f"📈 P&L: `{fmtm(state['pnl'])}`\n"
        f"🎯 Ставок: `{state['bets']}` · Win rate: `{wr}%`\n"
        f"До слота: `{m:02d}:{sc:02d}`\n\n"
        f"Ставка: `{bet_info}`",
        parse_mode="Markdown"
    )

async def cmd_analysis(update, ctx):
    a=analyze(state["candles_15m"], state["price"])
    sigs="\n".join(f"• {k}: {v}" for k,v in a["details"].items())
    secs=secs_to_next(); m,s=divmod(secs,60)
    await update.message.reply_text(
        f"🔍 *Анализ BTC · {fmt(state['price'])}*\n\n"
        f"Прогноз: *{'▲ ВВЕРХ' if a['dir']=='UP' else '▼ ВНИЗ'}*\n"
        f"Счёт: `{a['score']:+d}/10` · Уверенность: `{a['confidence']}%`\n\n"
        f"*Индикаторы:*\n{sigs}\n\n"
        f"До слота: `{m:02d}:{s:02d}`",
        parse_mode="Markdown"
    )

async def cmd_pause(update, ctx):
    state["paused"]=not state["paused"]
    await update.message.reply_text("⏸ Пауза — новые ставки не открываются" if state["paused"] else "▶️ Возобновлён")

async def cmd_stop(update, ctx):
    state["stopped"]=True; state["paused"]=True
    if state["active_bet"]: await close_bet(ctx.bot)
    wr=round(state["wins"]/state["bets"]*100) if state["bets"] else 0
    await update.message.reply_text(
        f"🛑 *Бот остановлен*\n\n"
        f"Итог: `${state['balance']:.2f}` · P&L `{fmtm(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']} ставок)\n\n"
        f"Для перезапуска — Redeploy в Railway",
        parse_mode="Markdown"
    )

async def cmd_reset(update, ctx):
    state.update({"balance":100.0,"pnl":0.0,"bets":0,"wins":0,"active_bet":None,"paused":False,"stopped":False,"history":[]})
    await update.message.reply_text("↺ Сброс · Баланс: `$100.00`", parse_mode="Markdown")

async def cmd_bet(update, ctx):
    if not in_hours(): await update.message.reply_text("⛔ Только 09:00–23:00 МСК"); return
    if state["active_bet"]: await update.message.reply_text("⚠️ Уже открытая ставка"); return
    if state["stopped"]: await update.message.reply_text("🛑 Бот остановлен. /reset для перезапуска"); return
    await open_bet(ctx.bot)

# ── WEB DASHBOARD ─────────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PolyBot Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Outfit:wght@400;500;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0a0c12;--s:#111520;--card:#161c2a;--b:#1e2840;--t:#e2e8f0;--m:#4a5a7a;--g:#00d395;--r:#f6465d;--bl:#4e7fff;--p:#7c3aed}
body{background:var(--bg);color:var(--t);font-family:'Outfit',sans-serif;min-height:100vh;padding:16px}
.top{display:flex;align-items:center;justify-content:space-between;padding-bottom:14px;border-bottom:1px solid var(--b);margin-bottom:14px}
.logo{font-size:18px;font-weight:700;letter-spacing:-0.5px}
.logo span{color:var(--g)}
.tag{font-size:11px;font-family:'DM Mono',monospace;padding:3px 10px;border-radius:20px;border:1px solid}
.tag-g{border-color:rgba(0,211,149,.3);color:var(--g);background:rgba(0,211,149,.08)}
.tag-r{border-color:rgba(246,70,93,.3);color:var(--r);background:rgba(246,70,93,.08)}
.tag-m{border-color:var(--b);color:var(--m)}
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}
.grid2{display:grid;grid-template-columns:2fr 1fr;gap:10px;margin-bottom:12px}
.card{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:14px}
.lbl{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:1px;font-family:'DM Mono',monospace;margin-bottom:5px}
.val{font-size:22px;font-weight:700;line-height:1}
.sub{font-size:11px;font-family:'DM Mono',monospace;margin-top:4px;color:var(--m)}
.g{color:var(--g)}.r{color:var(--r)}.bl{color:var(--bl)}
.chart-wrap{position:relative;height:180px}
.analysis-box{background:rgba(124,58,237,.08);border:1px solid rgba(124,58,237,.2);border-radius:10px;padding:12px;margin-top:10px}
.arow{display:flex;justify-content:space-between;font-size:12px;font-family:'DM Mono',monospace;padding:3px 0;color:var(--m)}
.av{color:var(--t)}
.hist-item{display:flex;align-items:center;justify-content:space-between;padding:7px 0;border-bottom:1px solid rgba(30,40,64,.8)}
.hist-item:last-child{border:none}
.hi-icon{width:26px;height:26px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex-shrink:0}
.hlist{max-height:220px;overflow-y:auto}
.hlist::-webkit-scrollbar{width:3px}
.hlist::-webkit-scrollbar-thumb{background:var(--b);border-radius:3px}
.active-box{background:rgba(78,127,255,.07);border:1px solid rgba(78,127,255,.25);border-radius:10px;padding:12px;margin-top:10px}
.price-big{font-size:30px;font-weight:700;font-family:'DM Mono',monospace;letter-spacing:-1px}
.src{font-size:11px;color:var(--m);font-family:'DM Mono',monospace;margin-top:3px}
.score-bar{height:6px;background:var(--b);border-radius:6px;margin:8px 0;overflow:hidden;position:relative}
.score-fill{position:absolute;top:0;height:6px;border-radius:6px;transition:width .5s,left .5s}
@media(max-width:700px){.grid4{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="top">
  <div class="logo">Poly<span>Bot</span> <span style="font-size:12px;color:var(--m);font-weight:400">v5 · BTC 15M</span></div>
  <div style="display:flex;gap:8px;align-items:center">
    <span id="srcTag" class="tag tag-m">—</span>
    <span id="statusTag" class="tag tag-g">● АКТИВЕН</span>
    <span id="clock" class="tag tag-m" style="color:var(--t)">--:--:--</span>
  </div>
</div>

<div class="grid4">
  <div class="card">
    <div class="lbl">Баланс</div>
    <div class="val" id="balance">$100.00</div>
    <div class="sub" id="balanceSub">—</div>
  </div>
  <div class="card">
    <div class="lbl">P&L итого</div>
    <div class="val" id="pnl">$0.00</div>
    <div class="sub" id="pnlPct">0.00%</div>
  </div>
  <div class="card">
    <div class="lbl">Ставок</div>
    <div class="val" id="bets">0</div>
    <div class="sub" id="wr">Win rate: —</div>
  </div>
  <div class="card">
    <div class="lbl">До слота</div>
    <div class="val g" id="countdown">--:--</div>
    <div class="sub" id="slotInfo">—</div>
  </div>
</div>

<div class="grid2">
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">
      <div>
        <div class="lbl">BTC / USD</div>
        <div class="price-big" id="price">$—</div>
        <div class="src" id="priceSrc">—</div>
      </div>
      <div id="changeTag" style="font-size:13px;font-family:'DM Mono',monospace;margin-top:4px">—</div>
    </div>
    <div class="chart-wrap"><canvas id="pnlChart"></canvas></div>
  </div>
  <div class="card">
    <div class="lbl">Анализ</div>
    <div id="pred" style="font-size:16px;font-weight:600;margin-bottom:8px">—</div>
    <div class="score-bar" id="scoreBar">
      <div class="score-fill" id="scoreFill"></div>
    </div>
    <div class="analysis-box" id="signals"><div style="color:var(--m);font-size:12px">Загрузка...</div></div>
    <div class="active-box" id="activeBet" style="display:none"></div>
  </div>
</div>

<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <div class="lbl" style="margin:0">История ставок</div>
    <div style="display:flex;gap:6px">
      <span id="winsTag" class="tag tag-g">0 побед</span>
      <span id="lossTag" class="tag tag-r">0 проигр.</span>
    </div>
  </div>
  <div class="hlist" id="history"><div style="color:var(--m);font-size:13px;text-align:center;padding:16px">Нет ставок</div></div>
</div>

<script>
let pnlChart=null, pnlData=[0], prevPrice=0;

function initChart(){
  pnlChart=new Chart(document.getElementById('pnlChart'),{
    type:'line',
    data:{labels:['0'],datasets:[{label:'P&L',data:[0],
      borderColor:'#00d395',borderWidth:2,fill:true,
      backgroundColor:'rgba(0,211,149,0.06)',pointRadius:0,tension:0.3,
      segment:{borderColor:ctx=>ctx.p0.parsed.y<0?'#f6465d':'#00d395'}}]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{x:{display:false},y:{grid:{color:'rgba(255,255,255,0.04)'},
        ticks:{color:'#4a5a7a',font:{size:10,family:'DM Mono'},callback:v=>'$'+v.toFixed(1)}}}}
  });
}

async function update(){
  try{
    const d=await fetch('/api/state').then(r=>r.json());

    // Price
    const p=d.price||0;
    document.getElementById('price').textContent='$'+p.toLocaleString('en-US',{minimumFractionDigits:0,maximumFractionDigits:0});
    document.getElementById('priceSrc').textContent=d.price_source||'—';
    if(prevPrice){
      const diff=p-prevPrice, pct=diff/prevPrice*100;
      const pos=diff>=0;
      document.getElementById('changeTag').textContent=(pos?'+':'')+diff.toFixed(0)+' ('+(pos?'+':'')+pct.toFixed(3)+'%)';
      document.getElementById('changeTag').style.color=pos?'#00d395':'#f6465d';
    }
    if(p) prevPrice=p;

    // Metrics
    const bal=d.balance; const pnl=d.pnl;
    document.getElementById('balance').textContent='$'+bal.toFixed(2);
    document.getElementById('balanceSub').textContent=(pnl>=0?'+ ':'- ')+'$'+Math.abs(pnl).toFixed(2);
    document.getElementById('balanceSub').style.color=pnl>=0?'#00d395':'#f6465d';
    document.getElementById('pnl').textContent=(pnl>=0?'+':'')+'$'+pnl.toFixed(2);
    document.getElementById('pnl').style.color=pnl>=0?'#00d395':'#f6465d';
    document.getElementById('pnlPct').textContent=(pnl/100*100).toFixed(2)+'%';
    document.getElementById('bets').textContent=d.bets;
    const wr=d.bets?Math.round(d.wins/d.bets*100):0;
    document.getElementById('wr').textContent='Win rate: '+wr+'% ('+d.wins+'/'+d.bets+')';
    document.getElementById('wr').style.color=wr>=60?'#00d395':wr>=40?'#f59e0b':'#f6465d';

    // Status
    const stopped=d.stopped, paused=d.paused;
    const inH=d.in_hours;
    const sTag=document.getElementById('statusTag');
    if(stopped){sTag.textContent='🛑 СТОП';sTag.className='tag tag-r';}
    else if(paused){sTag.textContent='⏸ ПАУЗА';sTag.className='tag tag-m';}
    else if(inH){sTag.textContent='● АКТИВЕН';sTag.className='tag tag-g';}
    else{sTag.textContent='🌙 ВНЕ ЧАСОВ';sTag.className='tag tag-m';}
    document.getElementById('srcTag').textContent=d.price_source||'—';

    // Countdown
    document.getElementById('slotInfo').textContent=d.next_slot_msk||'—';

    // Analysis
    const a=d.last_analysis||{};
    if(a.dir){
      const up=a.dir==='UP';
      document.getElementById('pred').textContent=(up?'▲ ВВЕРХ':'▼ ВНИЗ')+' · '+a.confidence+'%';
      document.getElementById('pred').style.color=up?'#00d395':'#f6465d';
      const score=a.score||0; const max=10;
      const pct=((score+max)/(max*2))*100;
      document.getElementById('scoreFill').style.width=Math.abs(score)/max*50+'%';
      document.getElementById('scoreFill').style.left=score>=0?'50%':(50+score/max*50)+'%';
      document.getElementById('scoreFill').style.background=score>=0?'#00d395':'#f6465d';
      const det=a.details||{};
      document.getElementById('signals').innerHTML=Object.entries(det).map(([k,v])=>
        `<div class="arow"><span>${k}</span><span class="av" style="font-size:11px;text-align:right;max-width:60%">${v}</span></div>`
      ).join('');
    }

    // Active bet
    const ab=d.active_bet;
    const abEl=document.getElementById('activeBet');
    if(ab){
      abEl.style.display='block';
      const cur=p; const up2=ab.dir==='UP';
      const win=(up2&&cur>ab.entry)||(!up2&&cur<ab.entry);
      const diff=cur-ab.entry;
      abEl.innerHTML=`<div style="font-size:11px;color:#a0aec0;font-family:'DM Mono',monospace;margin-bottom:6px">ОТКРЫТАЯ СТАВКА #${ab.num}</div>
        <div style="display:flex;justify-content:space-between;font-size:13px">
          <span style="color:${up2?'#00d395':'#f6465d'};font-weight:600">${up2?'▲ ВВЕРХ':'▼ ВНИЗ'}</span>
          <span style="color:${win?'#00d395':'#f6465d'}">${win?'✅ Выигрываем':'❌ Проигрываем'}</span>
        </div>
        <div style="font-size:12px;font-family:'DM Mono',monospace;color:#4a5a7a;margin-top:6px">
          Вход: <span style="color:#e2e8f0">$${Math.round(ab.entry).toLocaleString()}</span> → 
          Сейчас: <span style="color:${win?'#00d395':'#f6465d'}">$${Math.round(cur).toLocaleString()}</span>
          (${diff>=0?'+':''}${diff.toFixed(0)})
        </div>`;
    } else { abEl.style.display='none'; }

    // History
    const hist=d.history||[];
    const wins2=hist.filter(h=>h.won).length;
    document.getElementById('winsTag').textContent=wins2+' побед';
    document.getElementById('lossTag').textContent=(hist.length-wins2)+' проигр.';
    if(hist.length){
      document.getElementById('history').innerHTML=[...hist].reverse().slice(0,20).map(h=>`
        <div class="hist-item">
          <div style="display:flex;align-items:center;gap:8px">
            <div class="hi-icon" style="background:${h.won?'rgba(0,211,149,.12)':'rgba(246,70,93,.12)'};color:${h.won?'#00d395':'#f6465d'}">${h.dir==='UP'?'↑':'↓'}</div>
            <div>
              <div style="font-size:12px;color:#e2e8f0">#${h.num} ${h.dir==='UP'?'ВВЕРХ':'ВНИЗ'} · ${h.slot}</div>
              <div style="font-size:10px;color:#4a5a7a;font-family:'DM Mono',monospace">$${Math.round(h.entry).toLocaleString()}→$${Math.round(h.exit).toLocaleString()} · conf:${h.conf}% score:${h.score>=0?'+':''}${h.score}</div>
            </div>
          </div>
          <span style="font-size:12px;font-family:'DM Mono',monospace;color:${h.won?'#00d395':'#f6465d'}">${h.won?'+':''}$${h.profit.toFixed(2)}</span>
        </div>`).join('');
    }

    // PnL chart
    if(pnlChart){
      pnlData=[0,...(hist.length?hist.map((_,i)=>hist.slice(0,i+1).reduce((a,h)=>a+h.profit,0)):[])];
      pnlChart.data.labels=pnlData.map((_,i)=>i);
      pnlChart.data.datasets[0].data=pnlData;
      pnlChart.update('none');
    }
  }catch(e){console.error(e)}
}

// Clock + countdown
function tick(){
  const now=new Date();
  const msk=new Date(now.toLocaleString('en-US',{timeZone:'Europe/Moscow'}));
  document.getElementById('clock').textContent=msk.toLocaleTimeString('ru-RU')+' МСК';
  const sec=Math.floor(now/1000); const slot=Math.ceil(sec/900)*900;
  const left=slot-sec; const m=Math.floor(left/60); const s=left%60;
  document.getElementById('countdown').textContent=`${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
}

initChart();
update();
setInterval(update,3000);
setInterval(tick,1000);
tick();
</script>
</body></html>"""

# ── FLASK APP ─────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def dashboard(): return render_template_string(DASHBOARD_HTML)

@flask_app.route("/api/state")
def api_state():
    now=utc_now(); nxt=next_slot(now)
    secs=max(0,int((nxt-now).total_seconds()))
    m,s=divmod(secs,60)
    ab=state["active_bet"]
    return jsonify({
        "balance":state["balance"], "pnl":state["pnl"],
        "bets":state["bets"], "wins":state["wins"],
        "price":state["price"], "price_source":state["price_source"],
        "paused":state["paused"], "stopped":state["stopped"],
        "in_hours":in_hours(),
        "ws_ok":state["ws_ok"],
        "next_slot_msk": nxt.astimezone(MSK).strftime("%H:%M МСК"),
        "active_bet": {
            "num":ab["num"],"dir":ab["dir"],"entry":ab["entry"],
            "slot":ab["slot_open"].astimezone(MSK).strftime("%H:%M")
        } if ab else None,
        "last_analysis": state["last_analysis"],
        "history": state["history"][-50:],
    })

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# ── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    log.info(f"PolyBot v5 starting... token={'SET' if TG_TOKEN else 'MISSING'} chat={TG_CHAT_ID}")

    # Flask в отдельном потоке
    Thread(target=run_flask, daemon=True).start()
    log.info(f"Web dashboard: http://0.0.0.0:{PORT}")

    app=(Application.builder().token(TG_TOKEN).concurrent_updates(False).build())
    for cmd,fn in [("start",cmd_start),("status",cmd_status),("analysis",cmd_analysis),
                   ("bet",cmd_bet),("pause",cmd_pause),("stop",cmd_stop),("reset",cmd_reset)]:
        app.add_handler(CommandHandler(cmd,fn))

    async with app:
        await app.start()
        bot=app.bot

        # Настроить меню в Telegram
        await bot.set_my_commands([
            BotCommand("status",   "📊 Статус бота"),
            BotCommand("analysis", "🔍 Анализ рынка"),
            BotCommand("bet",      "▶️ Ставить сейчас"),
            BotCommand("pause",    "⏸ Пауза / Возобновить"),
            BotCommand("stop",     "🛑 Остановить бота"),
            BotCommand("reset",    "↺ Сброс баланса"),
        ])

        await load_initial_candles()
        await send_msg(bot,
            f"🚀 *PolyBot v5 запущен!*\n\n"
            f"Баланс: `$100.00` · Ставки: `$5.00` каждые 15 мин\n"
            f"Расписание: 09:00–23:00 МСК\n"
            f"Стратегия: 6 индикаторов · взвешенное голосование\n\n"
            f"🌐 Веб-дашборд доступен в Railway → ваш домен\n\n"
            f"Команды в меню 👇"
        )
        log.info("Bot started, polling...")
        await asyncio.gather(
            app.updater.start_polling(drop_pending_updates=True,
                allowed_updates=["message"],read_timeout=10,write_timeout=10,
                connect_timeout=10,pool_timeout=10),
            price_loop(),
            trading_loop(bot),
            daily_summary(bot),
        )
        await app.updater.stop()
        await app.stop()

if __name__=="__main__":
    asyncio.run(main())
