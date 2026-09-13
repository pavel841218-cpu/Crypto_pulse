import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
MEXC_BASE_URL = "https://contract.mexc.com"


# ============================================================
# КОНТЕКСТ 1H — EMA 20/40/80
# ============================================================

EMA_1H_FAST = 20
EMA_1H_MID = 40
EMA_1H_SLOW = 80
KLINES_1H_LIMIT = 120

SHELF_MIN_HOURS = 6
SHELF_MAX_WIDTH_PCT = 6.0
EMA_COMPRESSION_MAX_PCT = 3.5


# ============================================================
# ИМПУЛЬС 15M
# ============================================================

KLINES_15M_LIMIT = 60

MIN_BREAKOUT_FROM_SHELF_PCT = 1.5
MIN_15M_CHANGE_PCT = 2.0
MIN_15M_VOLUME_MULT = 3.5
MIN_15M_BODY_RATIO = 0.55
MIN_15M_CLOSE_STRENGTH = 0.70

USE_RSI_FILTER = True
MIN_RSI_6 = 55.0


# ============================================================
# OI (MEXC)
# ============================================================

USE_OI_FILTER = True
MIN_OI_GROWTH_PCT = 0.3          # за 15 сек — порог ниже, чем за 5 мин

# TTL кэша OI (если данные старше — считаем устаревшими)
MEXC_OI_CACHE_TTL = 300


# ============================================================
# ФИЛЬТР ЦЕНЫ
# ============================================================

MIN_24H_VOLUME_USDT = 1_000_000
MIN_PRICE_FOR_SCAN = 0.001
MAX_PRICE_FOR_SCAN = 5.0

EXCLUDE_KEYWORDS = [
    "_", "FOOTBALL", "INDEX", "STKFQ", "XAUT", "PAXG",
    "USDC", "USDT_", "GOLD", "OIL", "SILVER"
]


# ============================================================
# СКАНЕР
# ============================================================

CONTEXT_REFRESH_SECONDS = 1800
IMPULSE_SCAN_INTERVAL = 15

ALERT_COOLDOWN_SECONDS = 2 * 3600
MAX_SIGNALS_PER_HOUR = 10

SL_OFFSET = 0.998
TP1_RR = 1.5
TP2_RR = 3.0

last_signals = {}
shelf_cache = {}
signals_this_hour = []
scan_counter = 0
impulse_scan_counter = 0

# Кэш OI: symbol -> {"holdVol": float, "timestamp": float}
_mexc_oi_cache = {}

reject_stats = {
    "no_breakout": 0, "no_change": 0, "no_volume": 0,
    "no_body": 0, "no_close": 0, "no_rsi": 0, "no_oi": 0,
    "oi_no_data": 0,
}


# ============================================================
# HELPERS
# ============================================================

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def format_price(p):
    if p is None or p == 0:
        return "0.00"
    if p >= 1000:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.8f}"


def parse_kline(k):
    try:
        if isinstance(k, dict):
            return {
                "time": int(k.get("time", 0)),
                "open": safe_float(k.get("open")),
                "high": safe_float(k.get("high")),
                "low": safe_float(k.get("low")),
                "close": safe_float(k.get("close")),
                "volume": safe_float(k.get("volume")),
            }
        if isinstance(k, (list, tuple)) and len(k) >= 6:
            return {
                "time": int(k[0]),
                "open": safe_float(k[1]),
                "high": safe_float(k[2]),
                "low": safe_float(k[3]),
                "close": safe_float(k[4]),
                "volume": safe_float(k[5]),
            }
    except Exception:
        pass
    return None


def calculate_ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def candle_range(c):
    return c["high"] - c["low"]


def candle_body(c):
    return abs(c["close"] - c["open"])


def body_ratio(c):
    r = candle_range(c)
    return candle_body(c) / r if r > 0 else 0.0


def close_strength(c):
    r = candle_range(c)
    return (c["close"] - c["low"]) / r if r > 0 else 0.0


def cleanup():
    now = time.time()
    expired = [s for s, t in last_signals.items() if now - t > ALERT_COOLDOWN_SECONDS]
    for s in expired:
        del last_signals[s]
    global signals_this_hour
    signals_this_hour = [t for t in signals_this_hour if now - t < 3600]

    # Чистим старый кэш OI
    expired_oi = [
        sym for sym, data in _mexc_oi_cache.items()
        if now - data.get("timestamp", 0) > MEXC_OI_CACHE_TTL
    ]
    for s in expired_oi:
        del _mexc_oi_cache[s]


# ============================================================
# WEB
# ============================================================

async def health_check(request):
    return web.Response(
        text=f"Shelf Breakout Scanner | oi_cache={len(_mexc_oi_cache)}",
        status=200
    )


# ============================================================
# BINGX API (сигналы + свечи)
# ============================================================

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(url, headers=headers,
                              timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                if any(kw in sym.upper() for kw in EXCLUDE_KEYWORDS):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if price < MIN_PRICE_FOR_SCAN or price > MAX_PRICE_FOR_SCAN:
                    continue
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = {"volume": vol, "price": price}
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров BingX: {e}")
        return {}


async def fetch_klines(session, symbol, interval, limit, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    async with semaphore:
        try:
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=6)
            ) as resp:
                data = await resp.json()
                raw = data.get("data", [])
                if not isinstance(raw, list):
                    return []
                parsed = []
                for k in raw:
                    c = parse_kline(k)
                    if c and c["time"] > 0:
                        parsed.append(c)
                parsed.sort(key=lambda x: x["time"])
                return parsed
        except Exception:
            return []


# ============================================================
# MEXC OI (главный источник)
# ============================================================

def to_mexc_symbol(symbol):
    """
    BingX: SAGA-USDT
    MEXC:  SAGA_USDT
    """
    return symbol.replace("-", "_").upper()


async def get_mexc_oi_delta(session, symbol, semaphore):
    """
    Возвращает (growth_pct, source).
    growth_pct = изменение OI MEXC между двумя последовательными вызовами.
    Первый вызов: сохраняет значение, возвращает (0.0, "MEXC-cold").
    """
    mexc_symbol = to_mexc_symbol(symbol)
    url = f"{MEXC_BASE_URL}/api/v1/contract/ticker"
    params = {"symbol": mexc_symbol}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    async with semaphore:
        try:
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return 0.0, "MEXC-no-http"

                data = await resp.json()
                if not data.get("success"):
                    return 0.0, "MEXC-no-success"

                ticker = data.get("data", {})
                curr_hold = safe_float(ticker.get("holdVol"))

                if curr_hold <= 0:
                    return 0.0, "MEXC-no-oi"

                now = time.time()
                prev = _mexc_oi_cache.get(symbol)

                # Обновляем кэш текущим значением
                _mexc_oi_cache[symbol] = {
                    "holdVol": curr_hold,
                    "timestamp": now,
                }

                # Если есть предыдущее значение — считаем дельту
                if prev and prev.get("holdVol", 0) > 0:
                    time_diff = now - prev.get("timestamp", 0)
                    if time_diff <= MEXC_OI_CACHE_TTL:
                        growth = ((curr_hold - prev["holdVol"]) / prev["holdVol"]) * 100
                        return growth, "MEXC"

                # Первый запрос или данные устарели
                return 0.0, "MEXC-cold"

        except Exception as e:
            logging.debug(f"MEXC OI error {symbol}: {e}")
            return 0.0, "MEXC-err"


# ============================================================
# КОНТЕКСТ 1H
# ============================================================

def analyze_1h_context(candles_1h):
    if len(candles_1h) < EMA_1H_SLOW + 5:
        return None

    closed = candles_1h[:-1]
    shelf = closed[-SHELF_MIN_HOURS:]
    if len(shelf) < SHELF_MIN_HOURS:
        return None

    base_high = max(c["high"] for c in shelf)
    base_low = min(c["low"] for c in shelf)
    if base_low <= 0:
        return None

    shelf_width = ((base_high - base_low) / base_low) * 100
    if shelf_width > SHELF_MAX_WIDTH_PCT:
        return None

    closes = [c["close"] for c in closed]
    ema_fast = calculate_ema(closes, EMA_1H_FAST)
    ema_mid = calculate_ema(closes, EMA_1H_MID)
    ema_slow = calculate_ema(closes, EMA_1H_SLOW)

    if not ema_fast or not ema_mid or not ema_slow:
        return None

    ef, em, es = ema_fast[-1], ema_mid[-1], ema_slow[-1]

    # EMA20 и EMA40 внутри полки
    if not (base_low <= ef <= base_high):
        return None
    if not (base_low <= em <= base_high):
        return None

    # EMA80 не ниже полки более чем на 8%
    if es < base_low * 0.92 or es > base_high:
        return None

    compression = ((ef - es) / es) * 100
    if compression > EMA_COMPRESSION_MAX_PCT:
        return None

    return {
        "shelf_high": base_high,
        "shelf_low": base_low,
        "shelf_width_pct": round(shelf_width, 2),
        "ema_compression": round(compression, 2),
        "updated_at": time.time(),
    }


async def build_shelf_cache(session, symbols, semaphore):
    logging.info(f"🔍 Обновление 1H-контекста для {len(symbols)} пар...")
    start = time.time()

    tasks = [fetch_klines(session, sym, "1h", KLINES_1H_LIMIT, semaphore)
             for sym in symbols]
    all_klines = await asyncio.gather(*tasks, return_exceptions=True)

    new_cache = {}
    for sym, klines in zip(symbols, all_klines):
        if not isinstance(klines, list) or len(klines) < EMA_1H_SLOW + 5:
            continue
        ctx = analyze_1h_context(klines)
        if ctx:
            new_cache[sym] = ctx

    elapsed = time.time() - start
    logging.info(
        f"✅ 1H-контекст: {len(new_cache)} полок из {len(symbols)} за {elapsed:.1f}с"
    )
    return new_cache


# ============================================================
# ДЕТЕКТОР 15M
# ============================================================

def detect_impulse_15m(candles_15m, shelf_ctx):
    if len(candles_15m) < 25:
        return None

    current = candles_15m[-1]
    history = candles_15m[-20:-1]

    if current["open"] <= 0 or current["close"] <= 0:
        return None

    base_high = shelf_ctx["shelf_high"]
    base_low = shelf_ctx["shelf_low"]

    breakout_pct = ((current["close"] - base_high) / base_high) * 100
    if breakout_pct < MIN_BREAKOUT_FROM_SHELF_PCT:
        reject_stats["no_breakout"] += 1
        return None

    change_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if change_pct < MIN_15M_CHANGE_PCT:
        reject_stats["no_change"] += 1
        return None

    avg_vol = sum(c["volume"] for c in history) / len(history)
    if avg_vol <= 0:
        return None
    vol_mult = current["volume"] / avg_vol
    if vol_mult < MIN_15M_VOLUME_MULT:
        reject_stats["no_volume"] += 1
        return None

    if body_ratio(current) < MIN_15M_BODY_RATIO:
        reject_stats["no_body"] += 1
        return None

    if close_strength(current) < MIN_15M_CLOSE_STRENGTH:
        reject_stats["no_close"] += 1
        return None

    rsi6 = 0
    if USE_RSI_FILTER:
        closes = [c["close"] for c in candles_15m]
        rsi6 = calculate_rsi(closes, 6)
        if rsi6 < MIN_RSI_6:
            reject_stats["no_rsi"] += 1
            return None

    stop_loss = base_low * SL_OFFSET
    risk_pct = ((current["close"] - stop_loss) / current["close"]) * 100
    if risk_pct <= 0:
        return None

    tp1 = current["close"] * (1 + risk_pct * TP1_RR / 100)
    tp2 = current["close"] * (1 + risk_pct * TP2_RR / 100)

    return {
        "current_price": current["close"],
        "change_pct": round(change_pct, 2),
        "breakout_pct": round(breakout_pct, 2),
        "volume_mult": round(vol_mult, 2),
        "body_ratio": round(body_ratio(current), 2),
        "close_strength": round(close_strength(current), 2),
        "rsi6": round(rsi6, 1),
        "current_volume_usdt": int(current["volume"] * current["close"]),
        "shelf_high": base_high,
        "shelf_low": base_low,
        "shelf_width_pct": shelf_ctx["shelf_width_pct"],
        "ema_compression": shelf_ctx["ema_compression"],
        "stop_loss": stop_loss,
        "risk_pct": risk_pct,
        "tp1": tp1,
        "tp2": tp2,
    }


# ============================================================
# TELEGRAM ALERT
# ============================================================

async def send_alert(bot, symbol, sig, oi_growth, oi_source):
    try:
        coin = symbol.split("-")[0].upper()

        # Формат строки OI
        oi_line = ""
        if USE_OI_FILTER:
            if oi_source == "MEXC-cold":
                oi_line = f"📊 OI MEXC: <i>накапливается (первый замер)</i>\n"
            elif oi_source.startswith("MEXC-no") or oi_source == "MEXC-err":
                oi_line = f"📊 OI MEXC: <i>нет данных</i>\n"
            else:
                arrow = "📈" if oi_growth > 0 else "📉"
                oi_line = f"{arrow} OI MEXC: <b>{oi_growth:+.2f}%</b>\n"

        message = (
            f"🚀 <b>ПРОБОЙ ПОЛКИ (EMA 20/40/80)</b>\n\n"
            f"🪙 Монета: <code>{coin}</code>\n\n"
            f"💥 Пробой полки: <b>+{sig['breakout_pct']:.2f}%</b>\n"
            f"📈 Свеча 15m: <b>+{sig['change_pct']:.2f}%</b>\n"
            f"🔥 Объём: <b>x{sig['volume_mult']}</b> к среднему\n"
            f"💪 Body: <b>{sig['body_ratio']}</b> | Close: <b>{sig['close_strength']}</b>\n"
            f"📉 RSI6: <b>{sig['rsi6']}</b>\n"
            f"{oi_line}\n"
            f"📦 <b>КОНТЕКСТ 1H:</b>\n"
            f"├ Полка: <b>{sig['shelf_width_pct']}%</b>\n"
            f"├ Верх/низ: <code>{format_price(sig['shelf_high'])}</code> / "
            f"<code>{format_price(sig['shelf_low'])}</code>\n"
            f"└ EMA сжатие: <b>{sig['ema_compression']}%</b>\n\n"
            f"💰 <b>Ориентиры:</b>\n"
            f"├ Текущая цена: <code>{format_price(sig['current_price'])}</code>\n"
            f"├ Возможный стоп: <code>{format_price(sig['stop_loss'])}</code> "
            f"(риск ~{sig['risk_pct']:.2f}%)\n"
            f"├ TP1: <code>{format_price(sig['tp1'])}</code> "
            f"(+{sig['risk_pct'] * TP1_RR:.2f}%)\n"
            f"└ TP2: <code>{format_price(sig['tp2'])}</code> "
            f"(+{sig['risk_pct'] * TP2_RR:.2f}%)\n\n"
            f"💵 Объём свечи: <b>${sig['current_volume_usdt']:,}</b>\n\n"
            f"⚠️ <i>Решение о входе — за вами</i>\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S')}\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть BingX</a>"
        )

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки {symbol}: {e}")
        return False


# ============================================================
# ПРОВЕРКА ОДНОГО СИМВОЛА
# ============================================================

async def check_symbol(session, bot, symbol, shelf_ctx, semaphore):
    now = time.time()
    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN_SECONDS:
        return False
    if len(signals_this_hour) >= MAX_SIGNALS_PER_HOUR:
        return False

    candles = await fetch_klines(session, symbol, "15m", KLINES_15M_LIMIT, semaphore)
    if len(candles) < 25:
        return False

    sig = detect_impulse_15m(candles, shelf_ctx)
    if not sig:
        return False

    # OI проверка через MEXC
    oi_growth = 0.0
    oi_source = "MEXC-off"

    if USE_OI_FILTER:
        oi_growth, oi_source = await get_mexc_oi_delta(session, symbol, semaphore)

        # Если данных нет вообще (монеты нет на MEXC) — пропускаем
        if oi_source == "MEXC-no-success" or oi_source == "MEXC-no-oi":
            reject_stats["oi_no_data"] += 1
            return False

        # Если "cold" (первый замер) — пропускаем этот раз, кэш заполнится
        if oi_source == "MEXC-cold":
            # Не отправляем сигнал, но и не считаем отсевом
            # В следующий скан через 15 сек кэш будет заполнен
            return False

        # Проверка роста OI
        if oi_growth < MIN_OI_GROWTH_PCT:
            reject_stats["no_oi"] += 1
            logging.debug(
                f"{symbol}: OI {oi_growth:+.3f}% < {MIN_OI_GROWTH_PCT}% — пропуск"
            )
            return False

    success = await send_alert(bot, symbol, sig, oi_growth, oi_source)
    if success:
        last_signals[symbol] = now
        signals_this_hour.append(now)
        shelf_cache.pop(symbol, None)
        logging.info(
            f"🚀 АЛЕРТ {symbol} | пробой +{sig['breakout_pct']:.2f}% | "
            f"свеча +{sig['change_pct']:.2f}% | объём x{sig['volume_mult']} | "
            f"RSI6={sig['rsi6']} | OI {oi_growth:+.3f}% ({oi_source})"
        )
    return success


# ============================================================
# СКАНЕР
# ============================================================

async def scanner_loop(bot):
    global scan_counter, impulse_scan_counter, shelf_cache

    semaphore = asyncio.Semaphore(5)
    connector = aiohttp.TCPConnector(limit=15, ttl_dns_cache=300)
    last_context_refresh = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    "🚀 <b>SHELF BREAKOUT SCANNER запущен</b>\n\n"
                    "🎯 Контекст: полки 1H + EMA 20/40/80\n"
                    "⚡️ Детектор: пробой 15m с объёмом x3.5+\n"
                    "📊 OI: <b>MEXC</b> (holdVol delta)\n"
                    "💰 Цена: 0.001 — 5.0 USDT\n\n"
                    "⚠️ <b>Только алерты. Никакой автоторговли.</b>"
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка старта: {e}")

        while True:
            try:
                if time.time() - last_context_refresh > CONTEXT_REFRESH_SECONDS:
                    scan_counter += 1
                    symbols_dict = await fetch_bingx_symbols(session)
                    if not symbols_dict:
                        await asyncio.sleep(30)
                        continue
                    symbols = list(symbols_dict.keys())
                    shelf_cache = await build_shelf_cache(session, symbols, semaphore)
                    last_context_refresh = time.time()
                    logging.info(
                        f"🔄 Контекст #{scan_counter}: {len(shelf_cache)} полок"
                    )

                if not shelf_cache:
                    await asyncio.sleep(IMPULSE_SCAN_INTERVAL)
                    continue

                impulse_scan_counter += 1
                start = time.time()

                tasks = [
                    check_symbol(session, bot, sym, ctx, semaphore)
                    for sym, ctx in list(shelf_cache.items())
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                signals = sum(1 for r in results if r is True)
                elapsed = time.time() - start

                if impulse_scan_counter % 20 == 0 or signals > 0:
                    logging.info(
                        f"⚡️ Скан #{impulse_scan_counter} | {elapsed:.1f}с | "
                        f"Полок: {len(shelf_cache)} | Алертов: {signals} | "
                        f"OI-cache: {len(_mexc_oi_cache)}"
                    )

                if impulse_scan_counter % 40 == 0:
                    logging.info(
                        f"📊 ОТСЕВ: breakout={reject_stats['no_breakout']} | "
                        f"change={reject_stats['no_change']} | "
                        f"volume={reject_stats['no_volume']} | "
                        f"body={reject_stats['no_body']} | "
                        f"close={reject_stats['no_close']} | "
                        f"rsi={reject_stats['no_rsi']} | "
                        f"oi={reject_stats['no_oi']} | "
                        f"oi_no_data={reject_stats['oi_no_data']}"
                    )

                if impulse_scan_counter % 100 == 0:
                    cleanup()

                await asyncio.sleep(IMPULSE_SCAN_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Ошибка сканера: {e}")
                await asyncio.sleep(10)


# ============================================================
# MAIN
# ============================================================

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Установите PUMP_BOT_TOKEN и PUMP_CHAT_ID")
        return

    bot = Bot(token=BOT_TOKEN)

    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Веб-сервер на порту {PORT}")

    try:
        await scanner_loop(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
