"""
Стратегия для Polymarket BTC 15M
Принцип: лучше пропустить ставку чем поставить наугад.
Ставим только при confluence 3+ сигналов из 5.
"""
import math

def calc_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

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
    if len(closes) < 26: return 0.0, 0.0, 0.0
    ema12 = calc_ema(closes[-12:], 12)
    ema26 = calc_ema(closes[-26:], 26)
    macd_line = ema12 - ema26
    # Signal line = EMA9 of MACD (approximate)
    macd_vals = []
    for i in range(9, len(closes) + 1):
        sub = closes[max(0, i-26):i]
        if len(sub) >= 12:
            e12 = calc_ema(sub[-12:], 12)
            e26 = calc_ema(sub, 26) if len(sub) >= 26 else e12
            macd_vals.append(e12 - e26)
    signal = calc_ema(macd_vals[-9:], 9) if len(macd_vals) >= 9 else macd_line
    histogram = macd_line - signal
    return macd_line, signal, histogram

def calc_bollinger(closes, period=20, std_mult=2.0):
    if len(closes) < period:
        p = closes[-1]
        return p, p * 1.02, p * 0.98
    recent = closes[-period:]
    mid = sum(recent) / period
    variance = sum((x - mid) ** 2 for x in recent) / period
    std = math.sqrt(variance)
    return mid, mid + std_mult * std, mid - std_mult * std

def calc_atr(candles, period=14):
    if len(candles) < 2: return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    recent = trs[-period:]
    return sum(recent) / len(recent) if recent else 0.0

def analyze_advanced(candles_15m, current_price=None):
    """
    Возвращает:
      dir: 'UP' | 'DOWN' | 'SKIP'  — SKIP = не ставим
      confidence: 0-100
      signals: список сигналов
      reason: описание решения
    """
    if len(candles_15m) < 10:
        return {"dir": "SKIP", "confidence": 0, "signals": [], "reason": "Мало данных"}

    closes = [c["c"] for c in candles_15m]
    price  = current_price or closes[-1]

    signals_up   = []
    signals_down = []

    # ── 1. ТРЕНД: EMA9 vs EMA21 ──────────────────────────────────────────
    ema9  = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema_gap_pct = abs(ema9 - ema21) / ema21 * 100

    if ema9 > ema21 and ema_gap_pct > 0.03:
        signals_up.append(f"EMA9>{ema21:.0f} тренд ▲ ({ema_gap_pct:.3f}%)")
    elif ema9 < ema21 and ema_gap_pct > 0.03:
        signals_down.append(f"EMA9<{ema21:.0f} тренд ▼ ({ema_gap_pct:.3f}%)")
    # else: флэт — нейтрально

    # ── 2. RSI ────────────────────────────────────────────────────────────
    rsi = calc_rsi(closes, 14)
    if rsi < 40:
        signals_up.append(f"RSI={rsi:.1f} перепродан → отскок ▲")
    elif rsi > 60:
        signals_down.append(f"RSI={rsi:.1f} перекуплен → коррекция ▼")
    # 40-60 — нейтрально, не считаем

    # ── 3. MACD CROSSOVER ─────────────────────────────────────────────────
    macd_line, signal_line, histogram = calc_macd(closes)
    if macd_line > signal_line and histogram > 0:
        signals_up.append(f"MACD бычье пересечение ▲ (hist={histogram:.1f})")
    elif macd_line < signal_line and histogram < 0:
        signals_down.append(f"MACD медвежье пересечение ▼ (hist={histogram:.1f})")

    # ── 4. BOLLINGER BANDS ────────────────────────────────────────────────
    bb_mid, bb_upper, bb_lower = calc_bollinger(closes, 20)
    bb_width = (bb_upper - bb_lower) / bb_mid * 100

    if price < bb_lower and bb_width > 0.2:
        signals_up.append(f"Цена ниже нижней BB ({bb_lower:.0f}) → возврат ▲")
    elif price > bb_upper and bb_width > 0.2:
        signals_down.append(f"Цена выше верхней BB ({bb_upper:.0f}) → возврат ▼")
    elif abs(price - bb_mid) / bb_mid < 0.001:
        pass  # в середине — нейтрально

    # ── 5. МОМЕНТУМ: последние 3 свечи ───────────────────────────────────
    if len(closes) >= 4:
        last3 = closes[-4:]
        bull_candles = sum(1 for i in range(1, 4) if last3[i] > last3[i-1])
        atr = calc_atr(candles_15m, 14)
        price_move = abs(closes[-1] - closes[-4])

        if bull_candles >= 3 and price_move > atr * 0.5:
            signals_up.append(f"Моментум: 3/3 бычьих свечи, движение={price_move:.0f}")
        elif bull_candles == 0 and price_move > atr * 0.5:
            signals_down.append(f"Моментум: 3/3 медвежьих свечи, движение={price_move:.0f}")

    # ── 6. ВОЛАТИЛЬНОСТЬ: пропускаем при очень низкой ────────────────────
    atr = calc_atr(candles_15m, 14)
    atr_pct = atr / price * 100 if price else 0
    if atr_pct < 0.05:
        return {
            "dir": "SKIP",
            "confidence": 0,
            "signals": signals_up + signals_down,
            "reason": f"Волатильность слишком низкая (ATR={atr_pct:.3f}%) — рынок в флэте"
        }

    # ── РЕШЕНИЕ ───────────────────────────────────────────────────────────
    up_count   = len(signals_up)
    down_count = len(signals_down)
    total      = up_count + down_count

    # Ставим только при перевесе 3+ сигналов без контр-сигналов (или 2 vs 0)
    MIN_SIGNALS  = 2   # минимум сигналов в одну сторону
    MIN_EDGE     = 2   # минимальный перевес над противоположной стороной

    if up_count >= MIN_SIGNALS and (up_count - down_count) >= MIN_EDGE:
        confidence = min(88, 50 + up_count * 10 - down_count * 5 + int(atr_pct * 50))
        return {
            "dir": "UP",
            "confidence": confidence,
            "signals": signals_up,
            "counter": signals_down,
            "rsi": rsi, "macd": round(macd_line, 1),
            "atr_pct": round(atr_pct, 3),
            "reason": f"{up_count} сигнала ▲ vs {down_count} ▼"
        }
    elif down_count >= MIN_SIGNALS and (down_count - up_count) >= MIN_EDGE:
        confidence = min(88, 50 + down_count * 10 - up_count * 5 + int(atr_pct * 50))
        return {
            "dir": "DOWN",
            "confidence": confidence,
            "signals": signals_down,
            "counter": signals_up,
            "rsi": rsi, "macd": round(macd_line, 1),
            "atr_pct": round(atr_pct, 3),
            "reason": f"{down_count} сигнала ▼ vs {up_count} ▲"
        }
    else:
        return {
            "dir": "SKIP",
            "confidence": 0,
            "signals": signals_up + signals_down,
            "rsi": rsi, "macd": round(macd_line, 1),
            "reason": f"Сигналы противоречат ({up_count}▲ vs {down_count}▼) — пропускаем"
        }
