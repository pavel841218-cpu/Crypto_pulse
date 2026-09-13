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

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "YOUR_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"
BYBIT_BASE_URL = "https://api.bybit.com"


# ============================================================
# НАСТРОЙКИ: КОНТЕКСТ (1H)
# ============================================================

SHELF_MIN_HOURS = 6
SHELF_MAX_WIDTH_PCT = 3.5
EMA_COMPRESSION_MAX_PCT = 1.8
MAX_PRICE_FOR_SCAN = 50.0
KLINES_1H_LIMIT = 30


# ============================================================
# НАСТРОЙКИ: ИМПУЛЬС (5M)
# ============================================================

KLINES_5M_LIMIT = 40

MIN_5M_CHANGE_PCT = 1.2
MIN_BREAKOUT_FROM_SHELF_PCT = 1.5
MIN_5M_VOLUME_MULT = 3.5
MIN_5M_BODY_RATIO = 0.55
MIN_5M_CLOSE_STRENGTH = 0.70

RSI6_RSI14_SPREAD = 7.0
USE_RSI_FILTER = True


# ============================================================
# НАСТРОЙКИ: OI (МУЛЬТИБИРЖЕВОЙ)
# ============================================================

USE_OI_FILTER = True
MIN_OI_GROWTH_PCT = 1.5

DIVERGENCE_STRONG_VOLUME = 5.0
DIVERGENCE_STRONG_CHANGE = 2.0


# ============================================================
# НАСТРОЙКИ: ФИЛЬТРАЦИЯ ПАР
# ============================================================

MIN_24H_VOLUME_USDT = 1_000_000

EXCLUDE_KEYWORDS = [
    "_", "FOOTBALL", "INDEX", "STKFQ", "XAUT", "PAXG",
    "USDC", "USDT_", "GOLD", "OIL", "SILVER"
]


# ============================================================
# НАСТРОЙКИ: СКАНЕР
# ============================================================

CONTEXT_REFRESH_SECONDS = 1800
IMPULSE_SCAN_INTERVAL = 15

ALERT_COOLDOWN_SECONDS = 2 * 3600
MAX_SIGNALS_PER_HOUR = 15

last_signals = {}
shelf_cache = {}
signals_this_hour = []
scan_counter = 0
impulse_scan_counter = 0


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
    gains = []
    losses = []
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


def format_oi_source(oi_data):
    parts = []
    if oi_data.get("bybit") is not None:
        parts.append(f"Bybit {oi_data['bybit']:+.2f}%")
    if oi_data.get("bingx") is not None:
        parts.append(f"BingX {oi_data['bingx']:+.2f}%")
    return " | ".join(parts) if parts else "None"


# ============================================================
# WEB
# ============================================================

async def health_check(request):
    return web.Response(text="Fast Impulse Hunter is Live!", status=200)


# ============================================================
# BINGX API
# ============================================================

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
                if vol >= MIN_24H_VOLUME_USDT and 0 < price <= MAX_PRICE_FOR_SCAN:
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
                url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=6)
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
# МУЛЬТИБИРЖЕВОЙ OI
# ============================================================

async def get_multi_oi(session, symbol, semaphore):
    results = {}
    bybit_symbol = symbol.replace("-", "").upper()

    async with semaphore:
        # ---------- BYBIT ----------
        try:
            url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
            params = {
                "category": "linear",
                "symbol": bybit_symbol,
                "intervalTime": "5min",
                "limit": 3,
            }
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        lst = data.get("result", {}).get("list", [])
                        if isinstance(lst, list) and len(lst) >= 2:
                            curr = safe_float(lst[0].get("openInterest"))
                            prev = safe_float(lst[1].get("openInterest"))
                            if prev > 0:
                                growth = ((curr - prev) / prev) * 100
                                results["bybit"] = {"growth": growth, "valid": True}
        except Exception:
            pass

        # ---------- BINGX ----------
        try:
            url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/openInterestHistory"
            params = {"symbol": symbol, "interval": "5m", "limit": 3}
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            async with session.get(
                url, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    lst = data.get("data", [])
                    if isinstance(lst, list) and len(lst) >= 2:
                        prev = safe_float(lst[-2].get("openInterest"))
                        curr = safe_float(lst[-1].get("openInterest"))
                        if prev > 0:
                            growth = ((curr - prev) / prev) * 100
                            results["bingx"] = {"growth": growth, "valid": True}
        except Exception:
            pass

    valid = {k: v["growth"] for k, v in results.items() if v.get("valid")}

    if not valid:
        return {
            "bybit": None,
            "bingx": None,
            "consensus": 0.0,
            "divergence": False,
            "spread": 0.0,
            "sources_count": 0,
            "positive_count": 0,
            "negative_count": 0,
        }

    consensus = sum(valid.values()) / len(valid)
    positive = sum(1 for v in valid.values() if v > 0)
    negative = sum(1 for v in valid.values() if v < 0)
    divergence = (positive > 0 and negative > 0)
    spread = (max(valid.values()) - min(valid.values())) if len(valid) >= 2 else 0.0

    return {
        "bybit": results.get("bybit", {}).get("growth"),
        "bingx": results.get("bingx", {}).get("growth"),
        "consensus": consensus,
        "divergence": divergence,
        "spread": spread,
        "sources_count": len(valid),
        "positive_count": positive,
        "negative_count": negative,
    }


# ============================================================
# АНАЛИЗ КОНТЕКСТА (1H) - ИСПРАВЛЕН
# ============================================================

def analyze_1h_context(candles_1h):
    # Требуем минимум 20 свечей под размер нашего лимита (30)
    if len(candles_1h) < 20:
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
    # Используем периоды EMA под доступный объём истории
    ema5_list = calculate_ema(closes, 5)
    ema10_list = calculate_ema(closes, 10)
    ema20_list = calculate_ema(closes, 20)

    if not ema5_list or not ema10_list or not ema20_list:
        return None

    ema5 = ema5_list[-1]
    ema10 = ema10_list[-1]
    ema20 = ema20_list[-1]

    for e in (ema5, ema10, ema20):
        if not (base_low <= e <= base_high):
            return None

    ema_vals = [ema5, ema10, ema20]
    compression = ((max(ema_vals) - min(ema_vals)) / min(ema_vals)) * 100
    if compression > EMA_COMPRESSION_MAX_PCT:
        return None

    avg_shelf_volume = sum(c["volume"] for c in shelf) / len(shelf)

    return {
        "shelf_high": base_high,
        "shelf_low": base_low,
        "shelf_width_pct": round(shelf_width, 2),
        "ema_compression": round(compression, 2),
        "ema20": ema20,
        "avg_shelf_volume": avg_shelf_volume,
        "updated_at": time.time(),
    }


async def build_shelf_cache(session, symbols, semaphore):
    logging.info(f"🔍 Обновление 1H-контекста для {len(symbols)} пар...")
    start = time.time()

    tasks = [
        fetch_klines(session, sym, "1h", KLINES_1H_LIMIT, semaphore)
        for sym in symbols
    ]
    all_klines = await asyncio.gather(*tasks, return_exceptions=True)

    new_cache = {}
    for sym, klines in zip(symbols, all_klines):
        if not isinstance(klines, list) or len(klines) < 20:
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
# АНАЛИЗ ИМПУЛЬСА (5M)
# ============================================================

def detect_impulse_5m(candles_5m, shelf_ctx):
    if len(candles_5m) < 25:
        return None

    current = candles_5m[-1]
    history = candles_5m[-20:-1]

    if current["open"] <= 0 or current["close"] <= 0:
        return None

    base_high = shelf_ctx["shelf_high"]
    breakout_pct = ((current["close"] - base_high) / base_high) * 100
    if breakout_pct < MIN_BREAKOUT_FROM_SHELF_PCT:
        return None

    change_pct = ((current["close"] - current["open"]) / current["open"]) * 100
    if change_pct < MIN_5M_CHANGE_PCT:
        return None

    avg_vol = sum(c["volume"] for c in history) / len(history)
    if avg_vol <= 0:
        return None
    vol_mult = current["volume"] / avg_vol
    if vol_mult < MIN_5M_VOLUME_MULT:
        return None

    if body_ratio(current) < MIN_5M_BODY_RATIO:
        return None

    if close_strength(current) < MIN_5M_CLOSE_STRENGTH:
        return None

    if USE_RSI_FILTER:
        closes = [c["close"] for c in candles_5m]
        rsi6 = calculate_rsi(closes, 6)
        rsi14 = calculate_rsi(closes, 14)
        if rsi6 - rsi14 < RSI6_RSI14_SPREAD:
            return None
    else:
        rsi6 = rsi14 = 0

    return {
        "current_price": current["close"],
        "current_open": current["open"],
        "change_pct": round(change_pct, 2),
        "breakout_pct": round(breakout_pct, 2),
        "volume_mult": round(vol_mult, 2),
        "body_ratio": round(body_ratio(current), 2),
        "close_strength": round(close_strength(current), 2),
        "rsi6": round(rsi6, 1),
        "rsi14": round(rsi14, 1),
        "current_volume_usdt": int(current["volume"] * current["close"]),
        "shelf_high": base_high,
        "shelf_low": shelf_ctx["shelf_low"],
        "shelf_width_pct": shelf_ctx["shelf_width_pct"],
        "ema_compression": shelf_ctx["ema_compression"],
    }


# ============================================================
# TELEGRAM
# ============================================================

async def send_signal(bot, symbol, sig, oi_growth, oi_source, oi_data=None):
    try:
        coin = symbol.split("-")[0].upper()

        oi_lines = ""
        if USE_OI_FILTER and oi_data:
            if oi_data.get("bybit") is not None:
                oi_lines += f"📊 Bybit OI: <b>{oi_data['bybit']:+.2f}%</b>\n"
            if oi_data.get("bingx") is not None:
                oi_lines += f"📊 BingX OI: <b>{oi_data['bingx']:+.2f}%</b>\n"
            if oi_data.get("divergence"):
                oi_lines += "⚠️ <b>ДИВЕРГЕНЦИЯ OI — возможен стоп-хант!</b>\n"

        warning_header = ""
        if sig.get("oi_warning"):
            warning_header = "⚠️ <b>ОСТОРОЖНО: риск стоп-ханта</b>\n\n"

        message = (
            f"⚡️ <b>ИМПУЛЬС ИЗ ПОЛКИ</b>\n\n"
            f"{warning_header}"
            f"🪙 Монета: <code>{coin}</code>\n\n"
            f"💥 Пробой полки: <b>+{sig['breakout_pct']:.2f}%</b>\n"
            f"📈 Свеча 5m: <b>+{sig['change_pct']:.2f}%</b>\n"
            f"🔥 Объём: <b>x{sig['volume_mult']}</b> к среднему\n"
            f"💪 Body: <b>{sig['body_ratio']}</b> | "
            f"Close: <b>{sig['close_strength']}</b>\n"
            f"📉 RSI6/RSI14: <b>{sig['rsi6']}/{sig['rsi14']}</b>\n"
            f"{oi_lines}\n"
            f"📦 <b>КОНТЕКСТ 1H:</b>\n"
            f"├ Полка: <b>{sig['shelf_width_pct']}%</b>\n"
            f"├ Верх/низ: <code>{format_price(sig['shelf_high'])}</code> / "
            f"<code>{format_price(sig['shelf_low'])}</code>\n"
            f"└ EMA compression: <b>{sig['ema_compression']}%</b>\n\n"
            f"💰 Цена: <code>{format_price(sig['current_price'])}</code>\n"
            f"💵 Объём свечи: <b>${sig['current_volume_usdt']:,}</b>\n\n"
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
# ПРОВЕРКА ОДНОГО СИМВОЛА (5M)
# ============================================================

async def check_symbol_5m(session, bot, symbol, shelf_ctx, semaphore):
    now = time.time()

    if symbol in last_signals and now - last_signals[symbol] < ALERT_COOLDOWN_SECONDS:
        return False

    if len(signals_this_hour) >= MAX_SIGNALS_PER_HOUR:
        return False

    candles_5m = await fetch_klines(
        session, symbol, "5m", KLINES_5M_LIMIT, semaphore
    )
    if len(candles_5m) < 25:
        return False

    sig = detect_impulse_5m(candles_5m, shelf_ctx)
    if not sig:
        return False

    oi_data = None
    oi_growth = 0.0
    oi_source = "Disabled"

    if USE_OI_FILTER:
        oi_data = await get_multi_oi(session, symbol, semaphore)

        if oi_data["sources_count"] == 0:
            return False

        bybit_val = oi_data.get("bybit")
        bybit_ok = bybit_val is not None and bybit_val >= MIN_OI_GROWTH_PCT
        consensus_ok = oi_data["consensus"] >= MIN_OI_GROWTH_PCT

        if oi_data["divergence"]:
            if (bybit_ok
                    and sig["volume_mult"] >= DIVERGENCE_STRONG_VOLUME
                    and sig["change_pct"] >= DIVERGENCE_STRONG_CHANGE):
                sig["oi_warning"] = True
            else:
                return False
        else:
            if not (bybit_ok or consensus_ok):
                return False

        oi_growth = oi_data["consensus"]
        oi_source = format_oi_source(oi_data)

    success = await send_signal(bot, symbol, sig, oi_growth, oi_source, oi_data)
    if success:
        last_signals[symbol] = now
        signals_this_hour.append(now)
        shelf_cache.pop(symbol, None)

        warning_tag = " [⚠️DIVERGENCE]" if sig.get("oi_warning") else ""
        logging.info(
            f"🚀 СИГНАЛ {symbol}{warning_tag} | пробой +{sig['breakout_pct']:.2f}% | "
            f"свеча +{sig['change_pct']:.2f}% | объём x{sig['volume_mult']} | "
            f"OI: {oi_source}"
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
                    "⚡️ <b>FAST IMPULSE HUNTER запущен</b>\n\n"
                    "🎯 Ловим импульсы на 5m из 1H-полок\n"
                    "📊 OI: <b>Bybit + BingX</b> с детектором дивергенции\n"
                    "📡 Контекст: каждые 30 мин\n"
                    "⚡️ Сканирование: каждые 15 сек"
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
                        f"🔄 Контекст #{scan_counter}: {len(shelf_cache)} полок в кэше"
                    )

                if not shelf_cache:
                    await asyncio.sleep(IMPULSE_SCAN_INTERVAL)
                    continue

                impulse_scan_counter += 1
                start = time.time()

                tasks = [
                    check_symbol_5m(session, bot, sym, ctx, semaphore)
                    for sym, ctx in list(shelf_cache.items())
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                signals = sum(1 for r in results if r is True)
                elapsed = time.time() - start

                if impulse_scan_counter % 20 == 0 or signals > 0:
                    logging.info(
                        f"⚡️ Скан #{impulse_scan_counter} | {elapsed:.1f}с | "
                        f"В кэше: {len(shelf_cache)} | Сигналов: {signals}"
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
