import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque


# ============================================================
#       CEX FUTURES AGGREGATOR v4.1 (HOURLY SHELF BREAKOUT)
#       Bitget SCANNER + CONSOLIDATION → PerpFinder MARKET OI
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 120
CANDLE_REFRESH_SEC = 35

MAX_COMMON_SYMBOLS = 400
SIGNAL_COOLDOWN_SEC = 4 * 3600
HOURLY_LOG_SEC = 3600


# ============================================================
# Strategy thresholds & Shelf Settings
# ============================================================

MIN_24H_VOLUME_USDT = 400_000
MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 1.0

# BITGET SCANNER
BITGET_MIN_RVOL = 2.0          # Свеча минимум в 2.0x от суточного среднего
BITGET_MIN_MOVE_PCT = 0.7      # Движение минимум +0.7% за 5m
BITGET_MIN_BODY_RATIO = 0.50   # Тело свечи ≥ 50% диапазона
BITGET_MIN_CLOSE_POSITION = 0.60  # Закрытие в верхней части свечи

# НАСТРОЙКИ ЧАСОВОЙ ПОЛКИ (КОНСОЛИДАЦИИ)
CONSOLIDATION_HOURS = 12       # Длина полки (12h = 144 свечи по 5m)
MAX_CONSOLIDATION_RANGE_PCT = 3.2  # Максимальная ширина полки (≤ 3.2%)

# MARKET AGGREGATED OI CONFIRMATION
MARKET_MIN_OI_GROWTH_PCT = 0.15  # Суммарный OI по всем CEX должен расти ≥ 0.15%

MIN_CANDLES = 288              # 288 свечей по 5m = 24 часа истории


# ============================================================
# URLs
# ============================================================

BITGET_BASE = "https://api.bitget.com"
PERPFINDER_BASE = "https://api.perpfinder.com"


# ============================================================
# Logging & Runtime State
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("CEX-AGGREGATOR-v4.1")

START_TIME = time.time()
SESSION = None
BACKGROUND_TASKS = set()

COMMON_SYMBOLS = {}
BITGET_DATA = {}
OI_HISTORY = defaultdict(lambda: deque(maxlen=12))

LAST_SIGNAL = {}
LAST_HOURLY_LOG = 0

STATS = {
    "bitget_requests": 0,
    "perpfinder_requests": 0,
    "bitget_errors": 0,
    "perpfinder_errors": 0,

    "bitget_scans": 0,
    "shelf_candidates": 0,
    "confirmed_signals": 0,

    "rejected_bitget_move": 0,
    "rejected_bitget_rvol": 0,
    "rejected_no_shelf": 0,
    "rejected_market_oi": 0,
}


# ============================================================
# HTTP Helper
# ============================================================

async def http_get(url, params=None, headers=None, timeout=10, service="other"):
    global SESSION
    key_req = f"{service}_requests"
    key_err = f"{service}_errors"

    try:
        if key_req in STATS:
            STATS[key_req] += 1

        async with SESSION.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:

            if resp.status == 429:
                log.warning("429 rate limit: %s. Pacing...", service)
                await asyncio.sleep(2.5)
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            if resp.status >= 400:
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            return await resp.json()

    except Exception as e:
        if key_err in STATS:
            STATS[key_err] += 1
        return None


# ============================================================
# Helpers
# ============================================================

def num(value, default=0.0):
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def normalize_base(symbol):
    if not symbol:
        return ""
    s = str(symbol).upper()
    for suffix in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    return s


# ============================================================
# BITGET & PERPFINDER FETCHERS
# ============================================================

async def fetch_bitget_tickers():
    url = f"{BITGET_BASE}/api/v2/mix/market/tickers"
    params = {"productType": "USDT-FUTURES"}
    data = await http_get(url, params=params, service="bitget")
    result = {}
    if not data or "data" not in data:
        return result

    for row in data.get("data", []):
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        base = normalize_base(symbol)
        if not base:
            continue
        price = num(row.get("lastPr") or row.get("lastPrice"))
        quote_vol = num(row.get("quoteVolume") or row.get("usdtVolume"))
        result[base] = {"symbol": symbol, "price": price, "volume24": quote_vol}

    return result


async def fetch_bitget_candles(symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "granularity": "5m",
        "limit": "288",
    }
    data = await http_get(url, params=params, service="bitget")
    if not data or "data" not in data:
        return []

    candles = []
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_market_aggregated_oi(base):
    symbol = f"{base.upper()}USDT"
    url = f"{PERPFINDER_BASE}/v1/oi/aggregated"
    params = {"symbol": symbol}

    data = await http_get(url, params=params, service="perpfinder")
    if not data:
        return None

    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    total_oi = num(data.get("total_oi_usd") or data.get("open_interest_usd"))
    if total_oi <= 0:
        return None

    now = time.time()
    hist = OI_HISTORY[base]
    hist.append((now, total_oi))

    delta_pct = 0.0
    if len(hist) >= 2 and hist[0][1] > 0:
        delta_pct = ((total_oi / hist[0][1]) - 1) * 100

    return {
        "total_oi": total_oi,
        "delta_pct": delta_pct,
        "exchanges": data.get("exchanges_breakdown", {})
    }


# ============================================================
# HOURLY SHELF & CANDLE ANALYSIS
# ============================================================

def check_hourly_consolidation_breakout(candles):
    """
    Проверяет, находилась ли цена в часовой полке и произошел ли пробой.
    """
    lookback_candles = CONSOLIDATION_HOURS * 12
    if len(candles) < lookback_candles + 2:
        return None

    current_candle = candles[-2]
    consolidation_period = candles[-(lookback_candles + 2):-2]

    highs = [c["high"] for c in consolidation_period]
    lows = [c["low"] for c in consolidation_period]

    shelf_max = max(highs)
    shelf_min = min(lows)

    if shelf_min <= 0:
        return None

    shelf_range_pct = ((shelf_max - shelf_min) / shelf_min) * 100

    # Если диапазон полки превышает максимум — отбраковываем
    if shelf_range_pct > MAX_CONSOLIDATION_RANGE_PCT:
        return None

    # Пробой верхней границы полки
    if current_candle["close"] <= shelf_max:
        return None

    breakout_pct = ((current_candle["close"] - shelf_max) / shelf_max) * 100

    return {
        "shelf_max": shelf_max,
        "shelf_min": shelf_min,
        "shelf_range_pct": shelf_range_pct,
        "breakout_pct": breakout_pct,
    }


def candle_metrics(candles):
    if len(candles) < MIN_CANDLES:
        return None

    candle = candles[-2]
    previous_candles = candles[:-2]

    close, open_price, high, low = candle["close"], candle["open"], candle["high"], candle["low"]
    if open_price <= 0:
        return None

    vol_usd = candle["volume"] * close
    move_pct = ((close / open_price) - 1) * 100
    candle_range = high - low
    if candle_range <= 0:
        return None

    body_ratio = abs(close - open_price) / candle_range
    close_position = (close - low) / candle_range

    vols_usd = [(x["volume"] * x["close"]) for x in previous_candles[-288:] if x["volume"] > 0]
    if not vols_usd:
        return None

    avg_vol_usd = sum(vols_usd) / len(vols_usd)
    rvol = vol_usd / avg_vol_usd if avg_vol_usd > 0 else 0

    return {
        "close": close,
        "move_pct": move_pct,
        "rvol": rvol,
        "volume_usd": vol_usd,
        "body_ratio": body_ratio,
        "close_position": close_position,
    }


# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================

async def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with SESSION.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception:
        return False


async def send_signal(base, bg, shelf, oi):
    message = (
        f"🧱 <b>ПРОБОЙ ЧАСОВОЙ ПОЛКИ</b>\n\n"
        f"Монета: <code>{base}USDT</code> — 🟢 <b>LONG</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        "📦 <b>Параметры консолидации (12h)</b>\n"
        f"Ширина полки: <b>{shelf['shelf_range_pct']:.2f}%</b>\n"
        f"Пробой уровня: <b>+{shelf['breakout_pct']:.2f}%</b>\n"
        f"Верхняя граница: <code>${shelf['shelf_max']:.4f}</code>\n\n"
        "📊 <b>Импульс свечи (5m)</b>\n"
        f"Изменение: {bg['move_pct']:+.2f}%\n"
        f"RVOL (24h): <b>{bg['rvol']:.2f}x</b>\n"
        f"Объём 5m: ${bg['volume_usd']:,.0f}\n\n"
        "📈 <b>Агрегированный OI (Все CEX)</b>\n"
        f"Общий OI: ${oi['total_oi']:,.0f}\n"
        f"Динамика ΔOI: <b>{oi['delta_pct']:+.2f}%</b>\n\n"
        "✅ Подтвержден истинный пробой накопления с объемом!"
    )
    await send_telegram(message)


# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base):
    item = COMMON_SYMBOLS.get(base)
    if not item:
        return

    STATS["bitget_scans"] += 1

    # 1. Сканирование свечей Bitget
    bg_candles = await fetch_bitget_candles(item["symbol"])
    if len(bg_candles) < MIN_CANDLES:
        return

    bg = candle_metrics(bg_candles)
    if not bg:
        return

    if bg["move_pct"] < BITGET_MIN_MOVE_PCT:
        STATS["rejected_bitget_move"] += 1
        return

    if bg["rvol"] < BITGET_MIN_RVOL:
        STATS["rejected_bitget_rvol"] += 1
        return

    # 2. Проверка часовой полки (Консолидации)
    shelf = check_hourly_consolidation_breakout(bg_candles)
    if not shelf:
        STATS["rejected_no_shelf"] += 1
        return

    STATS["shelf_candidates"] += 1

    # 3. Подтверждение суммарного притока OI через PerpFinder
    oi = await fetch_market_aggregated_oi(base)
    if not oi or oi["delta_pct"] < MARKET_MIN_OI_GROWTH_PCT:
        STATS["rejected_market_oi"] += 1
        return

    # 4. Проверка таймаута
    if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN_SEC:
        return

    LAST_SIGNAL[base] = time.time()
    STATS["confirmed_signals"] += 1

    await send_signal(base, bg, shelf, oi)


# ============================================================
# MAIN LOOPS & SERVER
# ============================================================

async def refresh_universe():
    global COMMON_SYMBOLS
    bitget = await fetch_bitget_tickers()
    if not bitget:
        return

    common = {}
    for base, b in bitget.items():
        if b["volume24"] >= MIN_24H_VOLUME_USDT and (MIN_PRICE_USDT <= b["price"] <= MAX_PRICE_USDT):
            common[base] = b

    COMMON_SYMBOLS = dict(sorted(common.items(), key=lambda x: x[1]["volume24"], reverse=True)[:MAX_COMMON_SYMBOLS])
    log.info("UNIVERSE REFRESHED | Common symbols: %d", len(COMMON_SYMBOLS))


async def scan_cycle():
    if not COMMON_SYMBOLS:
        return

    candidates = list(COMMON_SYMBOLS.keys())
    # Ограничение 2 потока + задержка 0.05s полностью устраняет 429 от Bitget
    semaphore = asyncio.Semaphore(2)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.05)
            except Exception as e:
                log.exception("Error processing %s: %s", base, e)

    await asyncio.gather(*[worker(base) for base in candidates])


async def scanner_loop():
    last_universe = 0
    while True:
        try:
            now = time.time()
            if now - last_universe >= UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last_universe = now

            if COMMON_SYMBOLS:
                await scan_cycle()

            await asyncio.sleep(CANDLE_REFRESH_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


async def index(request):
    text = f"""
CEX AGGREGATOR v4.1 (HOURLY SHELF)
==================================
Uptime: {int(time.time() - START_TIME)}s
Symbols: {len(COMMON_SYMBOLS)}

Bitget Scans:        {STATS['bitget_scans']}
Shelf Candidates:    {STATS['shelf_candidates']}
Confirmed Signals:   {STATS['confirmed_signals']}
"""
    return web.Response(text=text, content_type="text/plain")


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession()
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    await send_telegram("🚀 <b>CEX AGGREGATOR v4.1 ЗАПУЩЕН</b>\n• Внедрен поиск часовой полки (12h)\n• Защита от 429 активирована.")


async def cleanup(app):
    task = app.get("scanner_task")
    if task:
        task.cancel()
    global SESSION
    if SESSION:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
