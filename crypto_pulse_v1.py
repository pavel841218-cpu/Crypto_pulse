import asyncio
import os
import time
import logging

import aiohttp
from aiohttp import web
from aiogram import Bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ====================== CONFIG ======================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE = "https://open-api.bingx.com"
TIMEFRAME = "15m"
KLINE_LIMIT = 100

MIN_24H_VOLUME = 1_000_000
CHECK_INTERVAL = 25
ALERT_COOLDOWN = 4 * 3600

# Параметры паттерна (усилены против шума)
SWING_LOOKBACK = 5
OB_MAX_AGE_CANDLES = 25
BOS_MIN_PCT = 1.8               # Поднят с 0.6% до 1.8% для защиты от микро-сквизов
RETEST_TOLERANCE_PCT = 0.35
MAX_RETEST_BREAK_PCT = 0.25

last_signals = {}
active_setups = {}

# ====================== HELPERS ======================

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def body_high(c):
    return max(c["open"], c["close"])

def body_low(c):
    return min(c["open"], c["close"])

# ====================== API ======================

async def get_tickers(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = str(item.get("symbol", "")).upper()
                if not sym.endswith("-USDT"):
                    continue
                if any(x in sym for x in ["NCSK", "FOOTBALL", "INDEX", "_"]):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Tickers error: {e}")
        return {}

async def get_klines(session, symbol, limit=KLINE_LIMIT):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()
            candles = data.get("data", [])
            if not isinstance(candles, list):
                return []
            parsed = []
            for k in candles:
                if isinstance(k, dict):
                    parsed.append({
                        "ts": int(safe_float(k.get("time") or k.get("timestamp"))),
                        "open": safe_float(k.get("open")),
                        "high": safe_float(k.get("high")),
                        "low": safe_float(k.get("low")),
                        "close": safe_float(k.get("close")),
                        "volume": safe_float(k.get("volume")),
                    })
                elif isinstance(k, (list, tuple)) and len(k) >= 6:
                    parsed.append({
                        "ts": int(safe_float(k[0])),
                        "open": safe_float(k[1]),
                        "high": safe_float(k[2]),
                        "low": safe_float(k[3]),
                        "close": safe_float(k[4]),
                        "volume": safe_float(k[5]),
                    })
            parsed.sort(key=lambda x: x["ts"])
            return parsed
    except Exception:
        return []

# ====================== PATTERN LOGIC ======================

def find_swings(candles, lookback=SWING_LOOKBACK):
    highs, lows = [], []
    for i in range(lookback, len(candles) - lookback):
        h = candles[i]["high"]
        l = candles[i]["low"]
        is_high = all(h >= candles[j]["high"] for j in range(i - lookback, i + lookback + 1) if j != i)
        is_low = all(l <= candles[j]["low"] for j in range(i - lookback, i + lookback + 1) if j != i)
        if is_high:
            highs.append((i, h))
        if is_low:
            lows.append((i, l))
    return highs, lows

def detect_order_block_and_bos(candles):
    if len(candles) < 30:
        return None

    closed = candles[:-1]
    highs, lows = find_swings(closed)

    if len(lows) < 2 or len(highs) < 2:
        return None

    for i in range(len(lows) - 1, 0, -1):
        prev_low_idx, prev_low = lows[i - 1]
        last_low_idx, last_low = lows[i]

        if last_low >= prev_low:
            continue

        impulse_start = last_low_idx
        impulse_candles = closed[impulse_start:impulse_start + 8]
        if len(impulse_candles) < 3:
            continue

        ob_candle = None
        for c in reversed(closed[max(0, last_low_idx - 3):last_low_idx + 1]):
            if c["close"] < c["open"]:
                ob_candle = c
                break
        if ob_candle is None:
            ob_candle = closed[last_low_idx]

        ob_high = body_high(ob_candle)
        ob_low = body_low(ob_candle)

        recent_highs = [h for idx, h in highs if idx < last_low_idx]
        if not recent_highs:
            continue
        structure_high = max(recent_highs[-3:])

        bos_found = False
        bos_idx = None
        for j in range(last_low_idx + 1, len(closed)):
            if closed[j]["close"] > structure_high * (1 + BOS_MIN_PCT / 100):
                bos_found = True
                bos_idx = j
                break

        if not bos_found:
            continue

        age = len(closed) - 1 - bos_idx
        if age > OB_MAX_AGE_CANDLES:
            continue

        return {
            "type": "LONG",
            "ob_high": ob_high,
            "ob_low": ob_low,
            "bos_level": structure_high,
            "bos_idx": bos_idx,
            "created_at": time.time(),
            "age": age
        }

    return None

def check_retest(candles, setup):
    if setup["type"] != "LONG":
        return None

    current = candles[-1]
    ob_high = setup["ob_high"]
    ob_low = setup["ob_low"]

    # 1. Проверка касания зоны
    in_zone = (
        current["low"] <= ob_high * (1 + RETEST_TOLERANCE_PCT / 100)
        and current["high"] >= ob_low * (1 - RETEST_TOLERANCE_PCT / 100)
    )

    if not in_zone:
        return None

    # 2. Если зона пробита слишком глубоко вниз — отменяем сетап
    if current["close"] < ob_low * (1 - MAX_RETEST_BREAK_PCT / 100):
        return None

    # 3. ФИЛЬТР ANTI-MM: Отбраковываем свечи с длинными верхними фитилями (>35%)
    c_range = current["high"] - current["low"]
    if c_range > 0:
        upper_wick = current["high"] - max(current["open"], current["close"])
        if (upper_wick / c_range) > 0.35:
            return None

    # 4. ФИЛЬТР ОБЪЕМА: Требуем RVOL >= 2.0x к средней 20 свечей
    avg_vol = sum(c["volume"] for c in candles[-21:-1]) / 20.0
    rvol = current["volume"] / avg_vol if avg_vol > 0 else 0.0
    if rvol < 2.0:
        return None

    # 5. Проверка бычьего закрытия
    bullish = current["close"] > current["open"]
    close_pos = (current["close"] - current["low"]) / (c_range + 1e-9)

    if bullish and close_pos >= 0.55:
        return {
            "price": current["close"],
            "ob_high": ob_high,
            "ob_low": ob_low,
            "rvol": rvol,
            "reaction": "bullish"
        }

    return None

# ====================== SIGNAL ======================

async def send_signal(bot, symbol, setup, retest, volume):
    coin = symbol.split("-")[0]
    msg = (
        f"🎯 <b>QUASIMODO / OB RETEST</b>\n\n"
        f"Монета: <b>{coin}</b>\n"
        f"Направление: <b>LONG</b>\n\n"
        f"📦 Ордер-блок: <code>{retest['ob_low']:.6f} — {retest['ob_high']:.6f}</code>\n"
        f"💥 Слом структуры: <b>Подтвержден (+{BOS_MIN_PCT}%)</b>\n"
        f"⚡ RVOL отскока: <b>{retest['rvol']:.2f}x</b>\n"
        f"💰 Цена: <code>{retest['price']:.6f}</code>\n"
        f"📊 Объём 24h: ${volume/1_000_000:.2f}M\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График</a>"
    )
    try:
        await bot.send_message(CHAT_ID, msg, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        logging.error(f"TG error: {e}")
        return False

# ====================== MAIN LOOP ======================

async def check_symbol(session, bot, symbol, volume):
    now = time.time()
    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN:
        return

    candles = await get_klines(session, symbol)
    if len(candles) < 40:
        return

    # Проверка имеющегося сетапа
    if symbol in active_setups:
        setup = active_setups[symbol]
        
        # Удаляем просроченный сетап (старше 30 свечей / ~7.5 часов)
        if (now - setup["created_at"]) > (OB_MAX_AGE_CANDLES * 15 * 60):
            del active_setups[symbol]
            return

        retest = check_retest(candles, setup)
        if retest:
            ok = await send_signal(bot, symbol, setup, retest, volume)
            if ok:
                last_signals[symbol] = now
                del active_setups[symbol]
                logging.info(f"SIGNAL {symbol} | OB Retest LONG")
        else:
            current = candles[-1]
            if current["close"] < setup["ob_low"] * (1 - MAX_RETEST_BREAK_PCT / 100):
                del active_setups[symbol]
        return

    # Поиск нового сетапа
    setup = detect_order_block_and_bos(candles)
    if setup:
        active_setups[symbol] = setup
        logging.info(f"SETUP FOUND {symbol} | OB {setup['ob_low']:.6f}-{setup['ob_high']:.6f}")

async def scanner(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                tickers = await get_tickers(session)
                tasks = [
                    check_symbol(session, bot, sym, data["volume"])
                    for sym, data in tickers.items()
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                logging.info(f"Scan done | Active setups: {len(active_setups)}")
            except Exception as e:
                logging.error(f"Scanner error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

async def health(request):
    return web.Response(text=f"Quasimodo OB Bot | Setups: {len(active_setups)}")

async def main():
    bot = Bot(token=BOT_TOKEN)
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    try:
        await bot.send_message(CHAT_ID, "🎯 Quasimodo / Order Block Retest бот запущен (v2 Filtered)")
    except Exception:
        pass

    await scanner(bot)

if __name__ == "__main__":
    asyncio.run(main())
