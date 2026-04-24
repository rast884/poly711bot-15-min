"""
PolyBot — BTC 15M Trading Bot
Логика строго как Polymarket:
- Ставки только на закрытых 15м свечах Binance: 9:00, 9:15, 9:30 ...
- Цена входа = close 15м свечи в момент открытия раунда
- Цена выхода = close следующей 15м свечи (через 15 мин)
- Токен из Railway Variables, НЕ вшит в код
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
TG_TOKEN   = os.environ["TG_TOKEN"]        # Railway Variable — обязательно
TG_CHAT_ID = os.environ["TG_CHAT_ID"]      # Railway Variable — обязательно
START_HOUR = 9
END_HOUR   = 23
BET_AMOUNT = 5.0
MSK        = ZoneInfo("Europe/Moscow")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── STATE ───────────────────────────────────────────────────────────────────
state = {
    "balance":     100.0,
    "pnl":         0.0,
    "bets":        0,
    "wins":        0,
    "active_bet":  None,
    "candles_1m":  [],
    "candles_15m": [],
    "last_price":  0.0,
    "paused":      False,
}

# ─── TIME HELPERS ─────────────────────────────────────────────────────────────
def msk_now() -> datetime:
    return datetime.now(MSK)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def in_trading_hours() -> bool:
    t = msk_now()
    return START_HOUR <= t.hour < END_HOUR

def current_15m_slot(dt: datetime) -> datetime:
    dt = dt.replace(second=0, microsecond=0)
    return dt.replace(minute=(dt.minute // 15) * 15)

def next_15m_slot(dt: datetime) -> datetime:
    minutes = (dt.minute // 15 + 1) * 15
    if minutes >= 60:
        return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return dt.replace(minute=minutes, second=0, microsecond=0)

def secs_to_next_slot() -> int:
    now = utc_now()
    return max(0, int((next_15m_slot(now) - now).total_seconds()))

def fmt_price(p: float) -> str:
    return f"${p:,.0f}"

def fmt_money(p: float) -> str:
    return f"+${p:.2f}" if p >= 0 else f"-${abs(p):.2f}"

# ─── ANALYSIS ────────────────────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
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

def analyze() -> dict:
    src = state["candles_1m"] if len(state["candles_1m"]) >= 10 else state["candles_15m"]
    if len(src) < 5:
        return {"dir": "UP", "confidence": 50, "rsi": 50.0, "macd": 0.0, "trend": True}
    closes = [c["c"] for c in src]
    trend = closes[-1] > closes[-5]
    rsi = calc_rsi(closes)
    macd = calc_macd(closes)
    vol = abs(closes[-1] - closes[-2]) / closes[-2] * 100
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

# ─── BINANCE ─────────────────────────────────────────────────────────────────
async def fetch_15m_candles():
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=20"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                state["candles_15m"] = [
                    {"t": k[0], "o": float(k[1]), "h": float(k[2]),
                     "l": float(k[3]), "c": float(k[4])}
                    for k in data
                ]
                if state["candles_15m"]:
                    state["last_price"] = state["candles_15m"][-1]["c"]
                log.info(f"15m candles: {len(state['candles_15m'])}, last close={fmt_price(state['last_price'])}")
    except Exception as e:
        log.warning(f"fetch_15m error: {e}")

async def binance_ws():
    """WebSocket 1м свечи для анализа"""
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("Binance WS 1m connected")
                async for raw in ws:
                    k = json.loads(raw)["k"]
                    candle = {"t": k["t"], "o": float(k["o"]), "h": float(k["h"]),
                              "l": float(k["l"]), "c": float(k["c"])}
                    c = state["candles_1m"]
                    if c and c[-1]["t"] == candle["t"]: c[-1] = candle
                    else: c.append(candle)
                    if len(c) > 100: state["candles_1m"] = c[-100:]
                    state["last_price"] = candle["c"]
        except Exception as e:
            log.warning(f"WS error: {e} — retry 5s")
            await asyncio.sleep(5)

async def get_15m_close_price() -> float:
    """Цена закрытия завершённой 15м свечи (как Polymarket фиксирует цену)"""
    await fetch_15m_candles()
    c = state["candles_15m"]
    if not c: return state["last_price"] or 93000.0
    # Предпоследняя свеча — гарантированно закрыта
    return c[-2]["c"] if len(c) >= 2 else c[-1]["c"]

# ─── TRADING ─────────────────────────────────────────────────────────────────
async def open_bet(bot: Bot):
    if state["balance"] < BET_AMOUNT:
        await send_msg(bot, "❌ *Недостаточно средств!* Баланс < $5.")
        return
    if state["active_bet"]:
        return

    entry_price = await get_15m_close_price()
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
    arrow = "🟢 ▲ ВВЕРХ" if direction == "UP" else "🔴 ▼ ВНИЗ"

    await send_msg(bot,
        f"📊 *Ставка #{state['bets']} открыта*\n\n"
        f"Направление: {arrow}\n"
        f"Сумма: `$5.00`\n"
        f"Цена входа \\(close 15м\\): `{fmt_price(entry_price)}`\n"
        f"Слот: `{open_msk} → {close_msk} МСК`\n"
        f"RSI: `{analysis['rsi']}`  MACD: `{analysis['macd']}`\n"
        f"Уверенность: `{analysis['confidence']}%`\n"
        f"Баланс: `${state['balance']:.2f}`\n\n"
        f"⏰ Результат в `{close_msk} МСК`"
    )
    log.info(f"BET #{state['bets']}: {direction} @ {fmt_price(entry_price)} [{open_msk}→{close_msk}]")

async def close_bet(bot: Bot):
    bet = state["active_bet"]
    if not bet: return

    exit_price = await get_15m_close_price()
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
        f"Вход: `{fmt_price(bet['entry'])}` → Выход: `{fmt_price(exit_price)}`\n"
        f"Прибыль: `{fmt_money(profit)}`\n"
        f"Баланс: `${state['balance']:.2f}`\n"
        f"P&L итого: `{fmt_money(state['pnl'])}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{state['bets']})"
    )
    log.info(f"CLOSED #{bet['num']}: {'WIN' if won else 'LOSS'} {fmt_price(bet['entry'])}→{fmt_price(exit_price)} {fmt_money(profit)}")

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
async def send_msg(bot: Bot, text: str):
    try:
        await bot.send_message(chat_id=TG_CHAT_ID, text=text, parse_mode="Markdown")
    except Exception as e:
        log.error(f"TG error: {e}")

async def cmd_start(update, context: ContextTypes.DEFAULT_TYPE):
    global TG_CHAT_ID
    TG_CHAT_ID = str(update.effective_chat.id)
    await update.message.reply_text(
        "🤖 *PolyBot активен!*\n\n"
        "Торгую BTC/USD строго по слотам Polymarket.\n"
        "Ставки: 9:00, 9:15, 9:30... МСК\n"
        "Цена = close 15м свечи Binance\n\n"
        "/status · /bet · /pause · /reset · /analysis",
        parse_mode="Markdown"
    )

async def cmd_status(update, context: ContextTypes.DEFAULT_TYPE):
    s    = state
    t    = msk_now().strftime("%H:%M:%S МСК")
    secs = secs_to_next_slot()
    m, sc = divmod(secs, 60)
    status = "⏸ ПАУЗА" if s["paused"] else ("✅ АКТИВЕН" if in_trading_hours() else "🌙 ВНЕ ЧАСОВ")
    bet_info = "нет"
    if s["active_bet"]:
        b = s["active_bet"]
        cur = s["last_price"]
        winning = (b["dir"] == "UP" and cur > b["entry"]) or (b["dir"] == "DOWN" and cur < b["entry"])
        open_msk  = b["slot_open"].astimezone(MSK).strftime("%H:%M")
        close_msk = b["slot_close"].astimezone(MSK).strftime("%H:%M")
        bet_info = f"{'▲' if b['dir'] == 'UP' else '▼'} {fmt_price(b['entry'])}→{fmt_price(cur)} {'✅' if winning else '❌'} [{open_msk}-{close_msk}]"
    wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
    await update.message.reply_text(
        f"📊 *Статус PolyBot*\n\n"
        f"Время: `{t}`\n"
        f"Статус: `{status}`\n\n"
        f"💰 Баланс: `${s['balance']:.2f}`\n"
        f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
        f"🎯 Ставок: `{s['bets']}`  Win rate: `{wr}%`\n"
        f"До след. слота: `{m:02d}:{sc:02d}`\n\n"
        f"Активная ставка: `{bet_info}`",
        parse_mode="Markdown"
    )

async def cmd_bet(update, context: ContextTypes.DEFAULT_TYPE):
    if not in_trading_hours():
        await update.message.reply_text("⛔ Ставки только 09:00–23:00 МСК")
        return
    if state["active_bet"]:
        await update.message.reply_text("⚠️ Уже открытая ставка. Дождитесь закрытия слота.")
        return
    await open_bet(context.bot)

async def cmd_pause(update, context: ContextTypes.DEFAULT_TYPE):
    state["paused"] = not state["paused"]
    await update.message.reply_text("⏸ Бот на паузе" if state["paused"] else "▶️ Бот возобновлён")

async def cmd_reset(update, context: ContextTypes.DEFAULT_TYPE):
    state.update({"balance": 100.0, "pnl": 0.0, "bets": 0, "wins": 0, "active_bet": None, "paused": False})
    await update.message.reply_text("↺ Сброс. Баланс: `$100.00`", parse_mode="Markdown")

async def cmd_analysis(update, context: ContextTypes.DEFAULT_TYPE):
    a = analyze()
    c15 = state["candles_15m"]
    next_entry = c15[-1]["c"] if c15 else state["last_price"]
    secs = secs_to_next_slot()
    m, s2 = divmod(secs, 60)
    pred  = "▲ ВВЕРХ" if a["dir"] == "UP" else "▼ ВНИЗ"
    trend = "▲ Восходящий" if a["trend"] else "▼ Нисходящий"
    await update.message.reply_text(
        f"🔍 *Анализ BTC*\n\n"
        f"Цена сейчас: `{fmt_price(state['last_price'])}`\n"
        f"Цена входа след. ставки: `{fmt_price(next_entry)}`\n"
        f"Тренд: `{trend}`\n"
        f"RSI\\(14\\): `{a['rsi']}`\n"
        f"MACD: `{a['macd']}`\n\n"
        f"Прогноз: *{pred}*\n"
        f"Уверенность: `{a['confidence']}%`\n\n"
        f"До слота: `{m:02d}:{s2:02d}`",
        parse_mode="Markdown"
    )

# ─── ГЛАВНЫЙ ЦИКЛ ─────────────────────────────────────────────────────────────
async def trading_loop(bot: Bot):
    """Ждёт точного момента 15м слота, открывает/закрывает ставки как Polymarket"""
    log.info("Trading loop started")
    await asyncio.sleep(5)

    while True:
        now = utc_now()
        nxt = next_15m_slot(now)
        wait = (nxt - now).total_seconds()
        log.info(f"Next slot: {nxt.astimezone(MSK).strftime('%H:%M МСК')} — wait {wait:.0f}s")
        await asyncio.sleep(max(0, wait - 0.5))

        # Финальная точная синхронизация
        now = utc_now()
        nxt = next_15m_slot(now)
        precise = (nxt - now).total_seconds()
        if precise > 0:
            await asyncio.sleep(precise)

        slot_msk = utc_now().astimezone(MSK).strftime("%H:%M")
        log.info(f"SLOT TICK at {slot_msk} МСК")

        # Закрыть предыдущую ставку если есть
        if state["active_bet"]:
            await close_bet(bot)
            await asyncio.sleep(2)

        # Открыть новую если в рабочих часах и не пауза
        if not state["paused"] and in_trading_hours():
            await open_bet(bot)

async def daily_report(bot: Bot):
    while True:
        now = msk_now()
        target = now.replace(hour=END_HOUR, minute=0, second=5, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        s  = state
        wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
        await send_msg(bot,
            f"🌙 *Торговый день завершён*\n\n"
            f"💰 Баланс: `${s['balance']:.2f}`\n"
            f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
            f"🎯 Ставок: `{s['bets']}`\n"
            f"✅ Win rate: `{wr}%`\n\n"
            f"До встречи в 09:00 МСК 🤖"
        )

async def main():
    log.info("Starting PolyBot...")
    # Не логируем токен — он виден в Railway Logs
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
        await fetch_15m_candles()
        await send_msg(bot,
            f"🚀 *PolyBot v2 запущен!*\n\n"
            f"Баланс: `$100.00`\n"
            f"Ставки: `$5.00` по слотам 9:00, 9:15, 9:30... МСК\n"
            f"Цена = close 15м свечи Binance \\(как Polymarket\\)\n\n"
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
            binance_ws(),
            trading_loop(bot),
            daily_report(bot),
        )
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
