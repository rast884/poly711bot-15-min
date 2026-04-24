"""
PolyBot v4 — синхронизация цены напрямую с Polymarket (Chainlink оракул)
Источник цены: wss://ws-subscriptions-clob.polymarket.com/ws/market
topic: crypto_prices_chainlink, symbol: btc/usd
Это та же цена что отображается на Polymarket как "Целевая цена" и "Текущая цена"
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from telegram import Bot
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TG_TOKEN   = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]
START_HOUR = 9
END_HOUR   = 23
BET_AMOUNT = 5.0
MSK        = ZoneInfo("Europe/Moscow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── STATE ───────────────────────────────────────────────────────────────────
state = {
    "balance":        100.0,
    "pnl":            0.0,
    "bets":           0,
    "wins":           0,
    "active_bet":     None,
    "poly_price":     0.0,      # цена с Polymarket Chainlink WS
    "poly_history":   [],       # история цен для анализа {ts, price}
    "slot_open_price": 0.0,     # цена открытия текущего слота (целевая цена)
    "candles_15m":    [],       # 15м свечи из poly_history
    "paused":         False,
    "ws_connected":   False,
}

# ─── TIME ────────────────────────────────────────────────────────────────────
def msk_now(): return datetime.now(MSK)
def utc_now(): return datetime.now(timezone.utc)
def in_trading_hours():
    t = msk_now(); return START_HOUR <= t.hour < END_HOUR

def current_15m_slot(dt):
    dt = dt.replace(second=0, microsecond=0)
    return dt.replace(minute=(dt.minute // 15) * 15)

def next_15m_slot(dt):
    minutes = (dt.minute // 15 + 1) * 15
    if minutes >= 60:
        return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return dt.replace(minute=minutes, second=0, microsecond=0)

def secs_to_next_slot():
    now = utc_now()
    return max(0, int((next_15m_slot(now) - now).total_seconds()))

def fmt_price(p): return f"${p:,.2f}"
def fmt_money(p): return f"+${p:.2f}" if p >= 0 else f"-${abs(p):.2f}"

# ─── POLYMARKET CHAINLINK WEBSOCKET ──────────────────────────────────────────
async def polymarket_price_ws():
    """
    Подключается к Polymarket WebSocket и получает цену BTC/USD от Chainlink.
    Это та же цена что используется для расчёта на Polymarket.
    """
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub_msg = json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "{\"symbol\":\"btc/usd\"}"
            }
        ]
    })

    while True:
        try:
            log.info("Connecting to Polymarket Chainlink WS...")
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=15,
                extra_headers={"User-Agent": "Mozilla/5.0"}
            ) as ws:
                await ws.send(sub_msg)
                state["ws_connected"] = True
                log.info("Polymarket Chainlink WS connected — receiving BTC/USD price")

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        # Формат: {"topic":"crypto_prices_chainlink","type":"update",
                        #          "payload":{"symbol":"btc/usd","value":77765.01,"timestamp":...}}
                        if isinstance(data, list):
                            for item in data:
                                _process_price_msg(item)
                        else:
                            _process_price_msg(data)
                    except Exception as e:
                        log.warning(f"Price parse error: {e}")

        except Exception as e:
            state["ws_connected"] = False
            log.warning(f"Polymarket WS error: {e} — fallback to Binance, retry in 10s")
            # Fallback: если Polymarket WS недоступен — берём с Binance
            await _binance_price_fallback()
            await asyncio.sleep(10)

def _process_price_msg(data):
    """Обработка сообщения с ценой от Polymarket"""
    try:
        topic = data.get("topic", "")
        if "crypto_prices" not in topic:
            return
        payload = data.get("payload", {})
        if not payload:
            return
        symbol = payload.get("symbol", "").lower()
        if "btc" not in symbol:
            return
        price = float(payload.get("value", 0) or payload.get("price", 0))
        if price <= 0:
            return
        ts = payload.get("timestamp", int(utc_now().timestamp() * 1000))
        state["poly_price"] = price
        # Записываем в историю для анализа
        h = state["poly_history"]
        h.append({"ts": ts, "price": price})
        if len(h) > 5000:
            state["poly_history"] = h[-5000:]
        _update_15m_candles(ts, price)
        log.debug(f"Polymarket BTC/USD: {fmt_price(price)}")
    except Exception as e:
        log.warning(f"_process_price_msg error: {e}")

def _update_15m_candles(ts_ms, price):
    """Строим 15м свечи из тик-данных Polymarket"""
    slot_ts = (ts_ms // (15 * 60 * 1000)) * (15 * 60 * 1000)
    c = state["candles_15m"]
    if c and c[-1]["t"] == slot_ts:
        candle = c[-1]
        candle["h"] = max(candle["h"], price)
        candle["l"] = min(candle["l"], price)
        candle["c"] = price
    else:
        c.append({"t": slot_ts, "o": price, "h": price, "l": price, "c": price})
    if len(c) > 50:
        state["candles_15m"] = c[-50:]

async def _binance_price_fallback():
    """Запасной источник цены — Binance WebSocket"""
    try:
        url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
        async with websockets.connect(url, open_timeout=10) as ws:
            for _ in range(30):  # 30 тиков и выходим обратно к Polymarket
                raw = await asyncio.wait_for(ws.recv(), timeout=3)
                data = json.loads(raw)
                price = float(data.get("p", 0))
                if price > 0:
                    state["poly_price"] = price
    except Exception as e:
        log.warning(f"Binance fallback error: {e}")

# ─── FETCH HISTORICAL 15M FROM BINANCE (для начальных данных) ────────────────
async def fetch_initial_candles():
    """Загружаем начальные 15м свечи с Binance чтобы был материал для анализа"""
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=30"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                for k in data:
                    slot_ts = int(k[0])
                    state["candles_15m"].append({
                        "t": slot_ts,
                        "o": float(k[1]), "h": float(k[2]),
                        "l": float(k[3]), "c": float(k[4])
                    })
                if state["candles_15m"] and state["poly_price"] == 0:
                    state["poly_price"] = state["candles_15m"][-1]["c"]
                log.info(f"Initial 15m candles loaded: {len(state['candles_15m'])}")
    except Exception as e:
        log.warning(f"fetch_initial_candles error: {e}")

# ─── ANALYSIS ────────────────────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0: gains += d
        else: losses += abs(d)
    rs = gains / (losses or 0.001)
    return 100 - 100 / (1 + rs)

def calc_macd(closes):
    def ema(arr, p):
        k = 2 / (p + 1); e = arr[0]
        for v in arr[1:]: e = v * k + e * (1 - k)
        return e
    if len(closes) < 26: return 0.0
    return ema(closes[-12:], 12) - ema(closes[-26:], 26)

def analyze():
    c = state["candles_15m"]
    if len(c) < 5:
        return {"dir": "UP", "confidence": 50, "rsi": 50.0, "macd": 0.0, "trend": True}
    closes = [x["c"] for x in c]
    trend = closes[-1] > closes[-5]
    rsi = calc_rsi(closes)
    macd = calc_macd(closes)
    vol = abs(closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] else 0
    score = 0
    if trend:    score += 1
    if rsi < 45: score += 1
    if rsi > 55: score -= 1
    if macd > 0: score += 1
    else:        score -= 1
    return {
        "dir": "UP" if score >= 0 else "DOWN",
        "confidence": min(92, int(50 + abs(score) * 15 + vol * 8)),
        "rsi": round(rsi, 1), "macd": round(macd, 2), "trend": trend
    }

# ─── TRADING ─────────────────────────────────────────────────────────────────
async def open_bet(bot: Bot):
    if state["balance"] < BET_AMOUNT:
        await send_msg(bot, "❌ *Недостаточно средств!*"); return
    if state["active_bet"]: return

    # Цена открытия слота — фиксируем как "целевую цену" (как Polymarket)
    entry_price = state["poly_price"]
    if entry_price == 0:
        log.warning("No price available yet, skipping bet")
        return

    state["slot_open_price"] = entry_price
    analysis = analyze()
    direction = analysis["dir"]
    now_utc = utc_now()
    slot_open  = current_15m_slot(now_utc)
    slot_close = slot_open + timedelta(minutes=15)

    state["balance"]   -= BET_AMOUNT
    state["bets"]      += 1
    state["active_bet"] = {
        "dir": direction, "entry": entry_price,
        "slot_open": slot_open, "slot_close": slot_close,
        "num": state["bets"]
    }

    open_msk  = slot_open.astimezone(MSK).strftime("%H:%M")
    close_msk = slot_close.astimezone(MSK).strftime("%H:%M")
    src = "🔗 Chainlink" if state["ws_connected"] else "📊 Binance"
    arrow = "🟢 ▲ ВВЕРХ" if direction == "UP" else "🔴 ▼ ВНИЗ"

    await send_msg(bot,
        f"📊 *Ставка #{state['bets']} открыта*\n\n"
        f"Направление: {arrow}\n"
        f"Сумма: `$5.00`\n"
        f"Целевая цена: `{fmt_price(entry_price)}` {src}\n"
        f"Слот: `{open_msk} → {close_msk} МСК`\n"
        f"RSI: `{analysis['rsi']}`  MACD: `{analysis['macd']}`\n"
        f"Уверенность: `{analysis['confidence']}%`\n"
        f"Баланс: `${state['balance']:.2f}`\n\n"
        f"⏰ Результат в `{close_msk} МСК`"
    )
    log.info(f"BET #{state['bets']}: {direction} entry={fmt_price(entry_price)} [{open_msk}→{close_msk}] src={'Polymarket' if state['ws_connected'] else 'Binance'}")

async def close_bet(bot: Bot):
    bet = state["active_bet"]
    if not bet: return

    exit_price = state["poly_price"] or bet["entry"]
    up  = exit_price > bet["entry"]
    won = (bet["dir"] == "UP" and up) or (bet["dir"] == "DOWN" and not up)
    profit = BET_AMOUNT * 0.88 if won else -BET_AMOUNT

    state["balance"]   += BET_AMOUNT + profit
    state["pnl"]       += profit
    state["active_bet"] = None
    if won: state["wins"] += 1

    wr     = round(state["wins"] / state["bets"] * 100) if state["bets"] else 0
    result = "✅ ВЫИГРЫШ" if won else "❌ ПРОИГРЫШ"
    open_msk  = bet["slot_open"].astimezone(MSK).strftime("%H:%M")
    close_msk = bet["slot_close"].astimezone(MSK).strftime("%H:%M")

    await send_msg(bot,
        f"{result} · Ставка #{bet['num']}\n\n"
        f"Слот: `{open_msk} → {close_msk} МСК`\n"
        f"{'▲ ВВЕРХ' if bet['dir'] == 'UP' else '▼ ВНИЗ'}\n"
        f"Целевая цена: `{fmt_price(bet['entry'])}`\n"
        f"Итоговая цена: `{fmt_price(exit_price)}`\n"
        f"Разница: `{fmt_price(exit_price - bet['entry'])}` ({'▲' if up else '▼'})\n"
        f"Прибыль: `{fmt_money(profit)}`\n"
        f"Баланс: `${state['balance']:.2f}`\n"
        f"P&L итого: `{fmt_money(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']})"
    )
    log.info(f"CLOSED #{bet['num']}: {'WIN' if won else 'LOSS'} {fmt_price(bet['entry'])}→{fmt_price(exit_price)} {fmt_money(profit)}")

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
async def send_msg(bot, text):
    try:
        await bot.send_message(chat_id=TG_CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        log.error(f"TG error: {e}")

async def cmd_start(update, context):
    global TG_CHAT_ID
    TG_CHAT_ID = str(update.effective_chat.id)
    src = "🔗 Chainlink (Polymarket)" if state["ws_connected"] else "📊 Binance (fallback)"
    await update.message.reply_text(
        "🤖 *PolyBot v4 активен!*\n\n"
        f"Источник цены: {src}\n"
        "Ставки: 9:00, 9:15, 9:30... МСК\n"
        "Целевая цена = цена открытия слота\n\n"
        "/status · /bet · /pause · /reset · /analysis",
        parse_mode="Markdown"
    )

async def cmd_status(update, context):
    s = state
    t = msk_now().strftime("%H:%M:%S МСК")
    secs = secs_to_next_slot()
    m, sc = divmod(secs, 60)
    status = "⏸ ПАУЗА" if s["paused"] else ("✅ АКТИВЕН" if in_trading_hours() else "🌙 ВНЕ ЧАСОВ")
    src = "🔗 Chainlink" if s["ws_connected"] else "📊 Binance"
    bet_info = "нет"
    if s["active_bet"]:
        b = s["active_bet"]
        cur = s["poly_price"]
        diff = cur - b["entry"]
        winning = (b["dir"] == "UP" and diff > 0) or (b["dir"] == "DOWN" and diff < 0)
        open_msk  = b["slot_open"].astimezone(MSK).strftime("%H:%M")
        close_msk = b["slot_close"].astimezone(MSK).strftime("%H:%M")
        bet_info = (
            f"{'▲' if b['dir'] == 'UP' else '▼'} "
            f"цель {fmt_price(b['entry'])} | сейчас {fmt_price(cur)} "
            f"({'✅' if winning else '❌'}) [{open_msk}–{close_msk}]"
        )
    wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
    await update.message.reply_text(
        f"📊 *Статус PolyBot*\n\n"
        f"Время: `{t}`\n"
        f"Статус: `{status}`\n"
        f"Источник цены: {src}\n\n"
        f"💰 Баланс: `${s['balance']:.2f}`\n"
        f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
        f"🎯 Ставок: `{s['bets']}`  Win rate: `{wr}%`\n"
        f"До след. слота: `{m:02d}:{sc:02d}`\n\n"
        f"BTC/USD: `{fmt_price(s['poly_price'])}`\n"
        f"Активная ставка: `{bet_info}`",
        parse_mode="Markdown"
    )

async def cmd_bet(update, context):
    if not in_trading_hours():
        await update.message.reply_text("⛔ Ставки только 09:00–23:00 МСК"); return
    if state["active_bet"]:
        await update.message.reply_text("⚠️ Уже открытая ставка."); return
    await open_bet(context.bot)

async def cmd_pause(update, context):
    state["paused"] = not state["paused"]
    await update.message.reply_text("⏸ Пауза" if state["paused"] else "▶️ Возобновлён")

async def cmd_reset(update, context):
    state.update({"balance": 100.0, "pnl": 0.0, "bets": 0, "wins": 0, "active_bet": None, "paused": False})
    await update.message.reply_text("↺ Сброс. Баланс: `$100.00`", parse_mode="Markdown")

async def cmd_analysis(update, context):
    a = analyze()
    cur = state["poly_price"]
    target = state["slot_open_price"]
    diff = cur - target if target else 0
    secs = secs_to_next_slot()
    m, s2 = divmod(secs, 60)
    src = "🔗 Chainlink (Polymarket)" if state["ws_connected"] else "📊 Binance (fallback)"
    await update.message.reply_text(
        f"🔍 *Анализ BTC — {src}*\n\n"
        f"Целевая цена слота: `{fmt_price(target) if target else 'ждём открытия'}`\n"
        f"Текущая цена: `{fmt_price(cur)}`\n"
        f"Разница: `{fmt_price(diff)}` ({'▲' if diff >= 0 else '▼'})\n\n"
        f"Тренд: `{'▲ Восходящий' if a['trend'] else '▼ Нисходящий'}`\n"
        f"RSI(14): `{a['rsi']}`\n"
        f"MACD: `{a['macd']}`\n\n"
        f"Прогноз: *{'▲ ВВЕРХ' if a['dir'] == 'UP' else '▼ ВНИЗ'}*\n"
        f"Уверенность: `{a['confidence']}%`\n\n"
        f"До слота: `{m:02d}:{s2:02d}`",
        parse_mode="Markdown"
    )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
async def trading_loop(bot):
    log.info("Trading loop started — waiting for first 15m slot")
    await asyncio.sleep(8)

    while True:
        now = utc_now()
        nxt = next_15m_slot(now)
        wait = (nxt - now).total_seconds()
        log.info(f"Next slot: {nxt.astimezone(MSK).strftime('%H:%M МСК')} — wait {wait:.0f}s")
        await asyncio.sleep(max(0, wait - 0.5))

        now = utc_now()
        nxt = next_15m_slot(now)
        precise = (nxt - now).total_seconds()
        if precise > 0:
            await asyncio.sleep(precise)

        slot_msk = utc_now().astimezone(MSK).strftime("%H:%M")
        log.info(f"SLOT TICK at {slot_msk} МСК | BTC={fmt_price(state['poly_price'])}")

        if state["active_bet"]:
            await close_bet(bot)
            await asyncio.sleep(1)

        if not state["paused"] and in_trading_hours():
            await open_bet(bot)

async def daily_report(bot):
    while True:
        now = msk_now()
        target = now.replace(hour=END_HOUR, minute=0, second=5, microsecond=0)
        if now >= target: target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        s = state
        wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
        await send_msg(bot,
            f"🌙 *День завершён*\n\n"
            f"💰 Баланс: `${s['balance']:.2f}`\n"
            f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
            f"🎯 Ставок: `{s['bets']}`  Win rate: `{wr}%`\n\n"
            f"До встречи в 09:00 МСК 🤖"
        )

async def main():
    log.info("Starting PolyBot v4...")
    log.info(f"TG_TOKEN: {'SET (len=' + str(len(TG_TOKEN)) + ')' if TG_TOKEN else 'MISSING'}")
    log.info(f"TG_CHAT_ID: {TG_CHAT_ID}")

    app = (
        Application.builder()
        .token(TG_TOKEN)
        .concurrent_updates(False)
        .build()
    )
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("bet",      cmd_bet))
    app.add_handler(CommandHandler("pause",    cmd_pause))
    app.add_handler(CommandHandler("reset",    cmd_reset))
    app.add_handler(CommandHandler("analysis", cmd_analysis))

    async with app:
        await app.start()
        bot = app.bot

        # Загружаем начальные данные
        await fetch_initial_candles()

        await send_msg(bot,
            f"🚀 *PolyBot v4 запущен!*\n\n"
            f"Баланс: `$100.00`\n"
            f"Источник цены: 🔗 Polymarket Chainlink WS\n"
            f"Ставки: `$5.00` по слотам 9:00, 9:15... МСК\n\n"
            f"/status — текущий статус"
        )

        await asyncio.gather(
            app.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=["message", "callback_query"],
                read_timeout=10,
                write_timeout=10,
                connect_timeout=10,
                pool_timeout=10,
            ),
            polymarket_price_ws(),   # основной источник цены — Polymarket Chainlink
            trading_loop(bot),
            daily_report(bot),
        )
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
