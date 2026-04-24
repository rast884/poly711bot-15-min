"""
PolyBot — BTC 15M Trading Bot
Виртуальный счёт $100, ставки $5 каждые 15 минут
Работает 09:00–23:00 МСК, уведомления в Telegram
"""

import asyncio
import json
import logging
import os
import math
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TG_TOKEN   = os.getenv("TG_TOKEN", "YOUR_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "YOUR_CHAT_ID")
START_HOUR = 9
END_HOUR   = 23
BET_AMOUNT = 5.0
INTERVAL   = 15 * 60          # seconds
MSK        = ZoneInfo("Europe/Moscow")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── STATE ───────────────────────────────────────────────────────────────────
state = {
    "balance":    100.0,
    "pnl":        0.0,
    "bets":       0,
    "wins":       0,
    "active_bet": None,   # {dir, entry, time}
    "candles":    [],     # last 60 1m candles from Binance
    "last_price": 0.0,
    "paused":     False,
    "time_left":  INTERVAL,
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def msk_now() -> datetime:
    return datetime.now(MSK)

def in_trading_hours() -> bool:
    t = msk_now()
    return START_HOUR <= t.hour < END_HOUR

def fmt_price(p: float) -> str:
    return f"${p:,.0f}"

def fmt_money(p: float) -> str:
    sign = "+" if p >= 0 else ""
    return f"{sign}${p:.2f}"

def calc_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses += abs(diff)
    rs = gains / (losses or 0.001)
    return 100 - 100 / (1 + rs)

def calc_macd(closes: list[float]) -> float:
    def ema(arr, p):
        k = 2 / (p + 1)
        e = arr[0]
        for v in arr[1:]:
            e = v * k + e * (1 - k)
        return e
    if len(closes) < 26:
        return 0.0
    return ema(closes[-12:], 12) - ema(closes[-26:], 26)

def analyze() -> dict:
    candles = state["candles"]
    if len(candles) < 10:
        return {"dir": "UP", "confidence": 50, "rsi": 50, "macd": 0, "trend": True}
    closes = [c["c"] for c in candles]
    last5  = closes[-5:]
    trend  = last5[-1] > last5[0]
    rsi    = calc_rsi(closes)
    macd   = calc_macd(closes)
    vol    = abs(closes[-1] - closes[-2]) / closes[-2] * 100

    score = 0
    if trend:      score += 1
    if rsi < 45:   score += 1
    if rsi > 55:   score -= 1
    if macd > 0:   score += 1
    else:          score -= 1

    direction  = "UP" if score >= 0 else "DOWN"
    confidence = min(92, int(50 + abs(score) * 15 + vol * 8))
    return {
        "dir":        direction,
        "confidence": confidence,
        "rsi":        round(rsi, 1),
        "macd":       round(macd, 2),
        "trend":      trend,
    }

# ─── BINANCE WS ──────────────────────────────────────────────────────────────
async def binance_ws():
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("Binance WS connected")
                async for raw in ws:
                    data = json.loads(raw)
                    k = data["k"]
                    candle = {
                        "t": k["t"], "o": float(k["o"]), "h": float(k["h"]),
                        "l": float(k["l"]), "c": float(k["c"]), "v": float(k["v"])
                    }
                    c = state["candles"]
                    if c and c[-1]["t"] == candle["t"]:
                        c[-1] = candle
                    else:
                        c.append(candle)
                    if len(c) > 80:
                        state["candles"] = c[-80:]
                    state["last_price"] = candle["c"]
        except Exception as e:
            log.warning(f"WS error: {e} — retry in 5s")
            await asyncio.sleep(5)

# ─── TRADING ─────────────────────────────────────────────────────────────────
async def place_bet(bot: Bot):
    if state["balance"] < BET_AMOUNT:
        await send_msg(bot, "❌ *Недостаточно средств!*\nБаланс меньше $5. Пополните или сбросьте.")
        return
    if state["active_bet"]:
        await settle_bet(bot)

    analysis = analyze()
    direction = analysis["dir"]
    price     = state["last_price"] or 93000
    state["balance"]    -= BET_AMOUNT
    state["active_bet"]  = {"dir": direction, "entry": price, "time": msk_now()}
    state["bets"]       += 1

    arrow = "🟢 ▲ ВВЕРХ" if direction == "UP" else "🔴 ▼ ВНИЗ"
    text = (
        f"📊 *Ставка #{state['bets']} размещена*\n\n"
        f"Направление: {arrow}\n"
        f"Сумма: `$5.00`\n"
        f"Цена входа: `{fmt_price(price)}`\n"
        f"RSI: `{analysis['rsi']}`  MACD: `{analysis['macd']}`\n"
        f"Уверенность: `{analysis['confidence']}%`\n"
        f"Баланс: `${state['balance']:.2f}`\n\n"
        f"⏰ Результат через ~15 минут"
    )
    await send_msg(bot, text)
    log.info(f"Bet #{state['bets']}: {direction} @ {fmt_price(price)}")

async def settle_bet(bot: Bot):
    bet = state["active_bet"]
    if not bet:
        return
    price  = state["last_price"] or bet["entry"]
    up     = price > bet["entry"]
    won    = (bet["dir"] == "UP" and up) or (bet["dir"] == "DOWN" and not up)
    profit = BET_AMOUNT * 0.88 if won else -BET_AMOUNT

    state["balance"]    += BET_AMOUNT + profit
    state["pnl"]        += profit
    state["active_bet"]  = None
    if won:
        state["wins"] += 1

    total    = state["bets"]
    wr       = round(state["wins"] / total * 100) if total else 0
    result   = "✅ ВЫИГРЫШ" if won else "❌ ПРОИГРЫШ"
    profit_s = fmt_money(profit)
    pnl_s    = fmt_money(state["pnl"])
    duration = msk_now() - bet["time"]
    mins     = int(duration.total_seconds() // 60)

    text = (
        f"{result}\n\n"
        f"Направление: {'▲ ВВЕРХ' if bet['dir'] == 'UP' else '▼ ВНИЗ'}\n"
        f"Вход: `{fmt_price(bet['entry'])}`  →  Выход: `{fmt_price(price)}`\n"
        f"Прибыль: `{profit_s}`\n"
        f"Баланс: `${state['balance']:.2f}`\n"
        f"P&L итого: `{pnl_s}`\n"
        f"Win rate: `{wr}%` ({state['wins']}/{total})\n"
        f"Время ставки: `~{mins} мин`"
    )
    await send_msg(bot, text)
    log.info(f"Settled: {'WIN' if won else 'LOSS'} profit={profit_s} balance=${state['balance']:.2f}")

# ─── TELEGRAM ────────────────────────────────────────────────────────────────
async def send_msg(bot: Bot, text: str):
    try:
        await bot.send_message(
            chat_id=TG_CHAT_ID,
            text=text,
            parse_mode="Markdown"
        )
    except Exception as e:
        log.error(f"Telegram send error: {e}")

# ─── COMMANDS ────────────────────────────────────────────────────────────────
async def cmd_start(update, context: ContextTypes.DEFAULT_TYPE):
    global TG_CHAT_ID
    TG_CHAT_ID = str(update.effective_chat.id)
    await update.message.reply_text(
        "🤖 *PolyBot активен!*\n\n"
        "Торгую BTC/USD каждые 15 минут.\n"
        "Работаю с 09:00 до 23:00 МСК.\n\n"
        "Команды:\n"
        "/status — текущий статус\n"
        "/bet — поставить сейчас\n"
        "/pause — пауза/возобновить\n"
        "/reset — сброс бота\n"
        "/analysis — анализ рынка",
        parse_mode="Markdown"
    )

async def cmd_status(update, context: ContextTypes.DEFAULT_TYPE):
    s = state
    t = msk_now().strftime("%H:%M:%S МСК")
    active = s["active_bet"]
    bet_info = "нет"
    if active:
        cur = s["last_price"]
        diff = cur - active["entry"]
        winning = (active["dir"] == "UP" and diff > 0) or (active["dir"] == "DOWN" and diff < 0)
        bet_info = (
            f"{'▲' if active['dir'] == 'UP' else '▼'} {active['dir']} | "
            f"{fmt_price(active['entry'])} → {fmt_price(cur)} "
            f"({'✅' if winning else '❌'})"
        )
    wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
    m, sec = divmod(s["time_left"], 60)
    status = "⏸ ПАУЗА" if s["paused"] else ("✅ АКТИВЕН" if in_trading_hours() else "🌙 ВНЕ ЧАСОВ")
    await update.message.reply_text(
        f"📊 *Статус PolyBot*\n\n"
        f"Время: `{t}`\n"
        f"Статус: `{status}`\n\n"
        f"💰 Баланс: `${s['balance']:.2f}`\n"
        f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
        f"🎯 Ставок: `{s['bets']}`  Win rate: `{wr}%`\n"
        f"Следующая ставка через: `{m:02d}:{sec:02d}`\n\n"
        f"Активная ставка: `{bet_info}`",
        parse_mode="Markdown"
    )

async def cmd_bet(update, context: ContextTypes.DEFAULT_TYPE):
    if not in_trading_hours():
        await update.message.reply_text("⛔ Ставки только 09:00–23:00 МСК")
        return
    await place_bet(context.bot)
    state["time_left"] = INTERVAL

async def cmd_pause(update, context: ContextTypes.DEFAULT_TYPE):
    state["paused"] = not state["paused"]
    status = "⏸ Бот на паузе" if state["paused"] else "▶️ Бот возобновлён"
    await update.message.reply_text(status)

async def cmd_reset(update, context: ContextTypes.DEFAULT_TYPE):
    state.update({"balance": 100.0, "pnl": 0.0, "bets": 0, "wins": 0,
                  "active_bet": None, "time_left": INTERVAL, "paused": False})
    await update.message.reply_text("↺ Бот сброшен. Баланс: `$100.00`", parse_mode="Markdown")

async def cmd_analysis(update, context: ContextTypes.DEFAULT_TYPE):
    a = analyze()
    price = state["last_price"]
    trend = "▲ Восходящий" if a["trend"] else "▼ Нисходящий"
    pred  = "▲ ВВЕРХ" if a["dir"] == "UP" else "▼ ВНИЗ"
    await update.message.reply_text(
        f"🔍 *Анализ рынка BTC*\n\n"
        f"Цена: `{fmt_price(price)}`\n"
        f"Тренд: `{trend}`\n"
        f"RSI(14): `{a['rsi']}`\n"
        f"MACD: `{a['macd']}`\n\n"
        f"Прогноз: *{pred}*\n"
        f"Уверенность: `{a['confidence']}%`",
        parse_mode="Markdown"
    )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
async def trading_loop(bot: Bot):
    log.info("Trading loop started")
    await asyncio.sleep(10)  # wait for WS to load candles
    while True:
        await asyncio.sleep(1)
        if state["paused"] or not in_trading_hours():
            continue
        state["time_left"] -= 1
        if state["time_left"] <= 0:
            await place_bet(bot)
            state["time_left"] = INTERVAL

async def daily_report(bot: Bot):
    """Send daily summary at 23:00 MSK"""
    while True:
        now = msk_now()
        target = now.replace(hour=END_HOUR, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        s = state
        wr = round(s["wins"] / s["bets"] * 100) if s["bets"] else 0
        await send_msg(bot,
            f"🌙 *Торговый день завершён*\n\n"
            f"💰 Итоговый баланс: `${s['balance']:.2f}`\n"
            f"📈 P&L: `{fmt_money(s['pnl'])}`\n"
            f"🎯 Ставок за день: `{s['bets']}`\n"
            f"✅ Win rate: `{wr}%`\n\n"
            f"До встречи в 09:00 МСК! 🤖"
        )

async def main():
    log.info("Starting PolyBot...")
    app = Application.builder().token(TG_TOKEN).build()

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("bet",      cmd_bet))
    app.add_handler(CommandHandler("pause",    cmd_pause))
    app.add_handler(CommandHandler("reset",    cmd_reset))
    app.add_handler(CommandHandler("analysis", cmd_analysis))

    async with app:
        await app.start()
        bot = app.bot

        await send_msg(bot,
            "🚀 *PolyBot запущен!*\n\n"
            f"Баланс: `$100.00`\n"
            f"Ставка: `$5.00` каждые 15 минут\n"
            f"Часы: `09:00–23:00 МСК`\n\n"
            "/status — текущий статус"
        )

        await asyncio.gather(
            app.updater.start_polling(drop_pending_updates=True),
            binance_ws(),
            trading_loop(bot),
            daily_report(bot),
        )

        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
