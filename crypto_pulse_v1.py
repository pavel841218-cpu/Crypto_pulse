import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque
import json


# ============================================================
#       CEX FUTURES AGGREGATOR v4.5
#       Bitget SCANNER + HOURLY SHELF → PerpFinder OI
#       FIX: change24h доля → процент
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 120
CANDLE_REFRESH_SEC = 35

MAX_COMMON_SYMBOLS = 150
MAX_CANDLE_CANDIDATES = 60

# Минимальное движение 24h в ПРОЦЕНТАХ
MIN_24H_CHANGE_PCT = 3.0

SIGNAL_COOLDOWN_SEC = 4 * 3600
HOURLY_LOG_SEC = 3600


# ============================================================
# Strategy thresholds & Shelf Settings
# ============================================================

MIN_24H_VOLUME_USDT = 400_000
MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 1.0

BITGET_MIN_RVOL = 2.0
BITGET_MIN_MOVE_PCT = 0.7

CONSOLIDATION_HOURS = 12
MAX_CONSOLIDATION_RANGE_PCT = 3.2

MARKET_MIN_OI_GROWTH_PCT = 0.15

MIN_CANDLES = 288


# ============================================================
# BASE URLs
# ============================================================

BITGET_BASE = "https://api.bitget.com"
PERPFINDER_BASE = "https://perpfinder.com"


# ============================================================
# Logging & Runtime State
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("CEX-AGGREGATOR-v4.5")

START_TIME = time.time()
SESSION = None

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
    "rejected_perpfinder_empty": 0,
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
                log.warning("[%s] 429 rate limit. Pacing...", service)
                await asyncio.sleep(2.0)
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            if resp.status >= 400:
                text = await resp.text()
                log.warning("[%s] HTTP %d: %s", service, resp.status, text[:200])
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            return await resp.json()
    except Exception as e:
        log.error("[%s] request error: %s", service, e)
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
# BITGET
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

        # ⚠️ Bitget возвращает change24h как ДОЛЮ: 0.035 = +3.5%
        change_raw = (
            row.get("change24h")
            or row.get("changeUtc24h")
            or row.get("priceChangePercent")
        )
        change_24 = None
        if change_raw is not None:
            try:
                change_24 = float(change_raw) * 100  # доля → процент
            except Exception:
                change_24 = None

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": quote_vol,
            "change24": change_24,
        }

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
            ts, o, h, l, c, v = (
                int(row[0]), num(row[1]), num(row[2]),
                num(row[3]), num(row[4]), num(row[5])
            )
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


# ============================================================
# PERPFINDER — OI
# ============================================================

async def fetch_perpfinder_oi(base):
    asset = base.upper()
    url = f"{PERPFINDER_BASE}/api/data/open-interest"
    params = {"asset": asset}

    data = await http_get(url, params=params, service="perpfinder")

    if not data:
        log.warning("[PerpFinder][%s] empty response", asset)
        STATS["rejected_perpfinder_empty"] += 1
        return None

    try:
        raw_preview = json.dumps(data)[:500]
        log.info("[PerpFinder][%s] raw: %s", asset, raw_preview)
    except Exception:
        log.info("[PerpFinder][%s] raw type: %s", asset, type(data))

    total_oi_usd = 0.0
    rows = None

    if isinstance(data, dict) and isinstance(data.get("rows"), list):
        rows = data["rows"]
    elif isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = data["data"]
    elif isinstance(data, list):
        rows = data

    if rows is None:
        log.warning(
            "[PerpFinder][%s] no rows field, top keys: %s",
            asset, list(data.keys()) if isinstance(data, dict) else "list"
        )
        STATS["rejected_perpfinder_empty"] += 1
        return None

    for row in rows:
        if not isinstance(row, dict):
            continue

        oi_val = num(
            row.get("open_interest")
            or row.get("openInterest")
            or row.get("open_interest_usd")
            or row.get("openInterestUsd")
            or row.get("oi")
            or row.get("value")
            or row.get("total_oi")
            or row.get("totalOpenInterest")
            or 0.0
        )

        if oi_val > 0:
            price = num(row.get("price") or row.get("mark_price") or 0.0)
            if price > 0 and oi_val < 1_000_000:
                total_oi_usd += oi_val * price
            else:
                total_oi_usd += oi_val

    log.info(
        "[PerpFinder][%s] rows=%d, total_oi_usd=%.2f",
        asset, len(rows), total_oi_usd
    )

    if total_oi_usd <= 0:
        STATS["rejected_perpfinder_empty"] += 1
        return None

    now = time.time()
    hist = OI_HISTORY[base]
    hist.append((now, total_oi_usd))

    delta_pct = 0.0
    if len(hist) >= 2 and hist[0][1] > 0:
        delta_pct = ((total_oi_usd / hist[0][1]) - 1) * 100

    return {
        "total_oi_usd": total_oi_usd,
        "delta_pct": delta_pct,
        "rows_count": len(rows),
    }


# ============================================================
# HOURLY SHELF & CANDLE ANALYSIS
# ============================================================

def check_hourly_consolidation_breakout(candles):
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
    if shelf_range_pct > MAX_CONSOLIDATION_RANGE_PCT:
        return None

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

    close, open_price = candle["close"], candle["open"]
    if open_price <= 0:
        return None

    vol_usd = candle["volume"] * close
    move_pct = ((close / open_price) - 1) * 100

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
    }


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
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
        "📦 <b>Консолидация (12h)</b>\n"
        f"Ширина полки: <b>{shelf['shelf_range_pct']:.2f}%</b>\n"
        f"Пробой: <b>+{shelf['breakout_pct']:.2f}%</b>\n"
        f"Уровень: <code>${shelf['shelf_max']:.4f}</code>\n\n"
        "📊 <b>Импульс свечи (5m)</b>\n"
        f"Рост: {bg['move_pct']:+.2f}%\n"
        f"RVOL: <b>{bg['rvol']:.2f}x</b>\n"
        f"Объём 5m: ${bg['volume_usd']:,.0f}\n\n"
        "📈 <b>Агрегированный OI (PerpFinder)</b>\n"
        f"Суммарный OI: ${oi['total_oi_usd']:,.0f}\n"
        f"Приток ΔOI: <b>{oi['delta_pct']:+.2f}%</b>\n"
        f"Источников: {oi['rows_count']}\n"
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

    shelf = check_hourly_consolidation_breakout(bg_candles)
    if not shelf:
        STATS["rejected_no_shelf"] += 1
        return

    STATS["shelf_candidates"] += 1
    log.info(
        "SHELF candidate: %s | breakout=+%.2f%% | fetching PerpFinder...",
        base, shelf["breakout_pct"]
    )

    oi = await fetch_perpfinder_oi(base)
    if not oi:
        return

    if oi["delta_pct"] < MARKET_MIN_OI_GROWTH_PCT:
        STATS["rejected_market_oi"] += 1
        log.info(
            "REJECT %s: OI delta %.2f%% < %.2f%%",
            base, oi["delta_pct"], MARKET_MIN_OI_GROWTH_PCT
        )
        return

    if time.time() - LAST_SIGNAL.get(base, 0) < SIGNAL_COOLDOWN_SEC:
        return

    LAST_SIGNAL[base] = time.time()
    STATS["confirmed_signals"] += 1

    await send_signal(base, bg, shelf, oi)


# ============================================================
# UNIVERSE
# ============================================================

async def refresh_universe():
    global COMMON_SYMBOLS, BITGET_DATA
    bitget = await fetch_bitget_tickers()
    if not bitget:
        return

    BITGET_DATA = bitget

    passed_vol = 0
    passed_price = 0
    passed_change = 0

    common = {}
    for base, b in bitget.items():
        if b["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        passed_vol += 1

        if not (MIN_PRICE_USDT <= b["price"] <= MAX_PRICE_USDT):
            continue
        passed_price += 1

        change_24 = b.get("change24")
        if change_24 is not None and abs(change_24) < MIN_24H_CHANGE_PCT:
            continue
        passed_change += 1

        common[base] = b

    sorted_common = sorted(common.items(), key=lambda x: x[1]["volume24"], reverse=True)
    COMMON_SYMBOLS = dict(sorted_common[:MAX_COMMON_SYMBOLS])

    log.info(
        "UNIVERSE | Bitget total=%d | vol=%d price=%d change=%d | Common=%d",
        len(bitget), passed_vol, passed_price, passed_change, len(COMMON_SYMBOLS)
    )


def select_candle_candidates():
    items = list(COMMON_SYMBOLS.items())
    items.sort(
        key=lambda x: abs(x[1].get("change24") or 0),
        reverse=True
    )
    return [b for b, _ in items[:MAX_CANDLE_CANDIDATES]]


# ============================================================
# SCAN CYCLE
# ============================================================

async def scan_cycle():
    if not COMMON_SYMBOLS:
        return

    candidates = select_candle_candidates()
    log.info("SCAN cycle | candidates=%d", len(candidates))

    semaphore = asyncio.Semaphore(3)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.1)
            except Exception as e:
                log.exception("Error processing %s: %s", base, e)

    await asyncio.gather(*[worker(base) for base in candidates])


# ============================================================
# HOURLY DIAGNOSTICS
# ============================================================

async def hourly_diagnostics():
    global LAST_HOURLY_LOG
    now = time.time()
    if now - LAST_HOURLY_LOG < HOURLY_LOG_SEC:
        return
    LAST_HOURLY_LOG = now

    log.info(
        "HOURLY | symbols=%d | BG scans=%d shelf=%d signals=%d | "
        "rej: move=%d rvol=%d shelf=%d oi=%d pf_empty=%d | "
        "BG req=%d/%d PF req=%d/%d",
        len(COMMON_SYMBOLS),
        STATS["bitget_scans"], STATS["shelf_candidates"], STATS["confirmed_signals"],
        STATS["rejected_bitget_move"], STATS["rejected_bitget_rvol"],
        STATS["rejected_no_shelf"], STATS["rejected_market_oi"],
        STATS["rejected_perpfinder_empty"],
        STATS["bitget_requests"], STATS["bitget_errors"],
        STATS["perpfinder_requests"], STATS["perpfinder_errors"],
    )

    msg = (
        "🩺 <b>ЧАСОВАЯ ДИАГНОСТИКА v4.5</b>\n\n"
        f"<b>Юниверс</b>\n"
        f"Монет в работе: {len(COMMON_SYMBOLS)}\n"
        f"Кандидатов на цикл: {len(select_candle_candidates())}\n\n"
        f"<b>Воронка</b>\n"
        f"Bitget сканов: {STATS['bitget_scans']}\n"
        f"Найдено полок: {STATS['shelf_candidates']}\n"
        f"Сигналов: <b>{STATS['confirmed_signals']}</b>\n\n"
        f"<b>Отсевы</b>\n"
        f"BG move: {STATS['rejected_bitget_move']}\n"
        f"BG RVOL: {STATS['rejected_bitget_rvol']}\n"
        f"Нет полки: {STATS['rejected_no_shelf']}\n"
        f"OI низкий: {STATS['rejected_market_oi']}\n"
        f"PerpFinder пусто: {STATS['rejected_perpfinder_empty']}\n\n"
        f"<b>HTTP</b>\n"
        f"Bitget: {STATS['bitget_requests']} ({STATS['bitget_errors']} ош)\n"
        f"PerpFinder: {STATS['perpfinder_requests']} ({STATS['perpfinder_errors']} ош)"
    )
    await send_telegram(msg)


# ============================================================
# MAIN LOOP
# ============================================================

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

            await hourly_diagnostics()
            await asyncio.sleep(CANDLE_REFRESH_SEC)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


# ============================================================
# WEB
# ============================================================

async def index(request):
    text = f"""
CEX AGGREGATOR v4.5
===================
Uptime: {int(time.time() - START_TIME)}s
Symbols: {len(COMMON_SYMBOLS)}
Candidates per cycle: {len(select_candle_candidates())}

Bitget Scans:        {STATS['bitget_scans']}
Shelf Candidates:    {STATS['shelf_candidates']}
Confirmed Signals:   {STATS['confirmed_signals']}

Rejected:
  bitget_move:       {STATS['rejected_bitget_move']}
  bitget_rvol:       {STATS['rejected_bitget_rvol']}
  no_shelf:          {STATS['rejected_no_shelf']}
  market_oi:         {STATS['rejected_market_oi']}
  pf_empty:          {STATS['rejected_perpfinder_empty']}

HTTP:
  bitget_requests:   {STATS['bitget_requests']} (errors {STATS['bitget_errors']})
  pf_requests:       {STATS['perpfinder_requests']} (errors {STATS['perpfinder_errors']})
"""
    return web.Response(text=text, content_type="text/plain")


async def health(request):
    return web.Response(text="ok", content_type="text/plain")


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    )
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    await send_telegram(
        "🚀 <b>CEX AGGREGATOR v4.5 ЗАПУЩЕН</b>\n"
        "• FIX: change24h доля → процент\n"
        "• Pre-фильтр: 24h change ≥ 3%\n"
        "• Кандидатов на цикл: до 60\n"
        "• Детальное логирование PerpFinder"
    )


async def cleanup(app):
    task = app.get("scanner_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    global SESSION
    if SESSION:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/stats", index)
app.router.add_get("/health", health)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
