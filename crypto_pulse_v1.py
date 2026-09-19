import asyncio
import aiohttp
import os
import time
import logging
from datetime import datetime, timezone
from collections import defaultdict

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message


# ============================================================
# CEX AGGREGATOR v8.0
# 1H SHELF -> FLOW -> QUARANTINE -> 2ND CEX -> COINGLASS
#
# Bitget = broad universe
# KuCoin = second main CEX
# Binance = CoinGlass final confirmation
#
# IMPORTANT:
# - Shelf uses CLOSED 1H candles only
# - Current live 1H candle is development candle
# - Bitget/KuCoin are NOT treated as permanent leader/follower
# - First abnormal flow => quarantine
# - Second CEX confirmation => candidate
# - CoinGlass only after meaningful candidate
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")

PORT = int(os.getenv("PORT", "10000"))

# ---------- universe ----------
UNIVERSE_REFRESH_SEC = 3600          # full market scan once/hour
MAX_UNIVERSE_SYMBOLS = 450

MIN_24H_VOLUME_USDT = 500_000
MIN_PRICE_USDT = 0.0001
MAX_PRICE_USDT = 100000.0

# ---------- shelf ----------
SHELF_MIN_HOURS = 5
SHELF_MAX_HOURS = 12

SHELF_MAX_WIDTH_PCT = 4.5
SHELF_MAX_CLOSE_DRIFT_PCT = 3.5

MIN_SHELF_CANDLES = 5

# ---------- monitoring ----------
MONITOR_INTERVAL_SEC = 60

MAX_ACTIVE_SHELVES = 80
SHELF_TTL_SEC = 14 * 3600

# ---------- flow ----------
FLOW_MIN_OI_CHANGE_PCT = 0.8
FLOW_STRONG_OI_CHANGE_PCT = 1.5

FLOW_MIN_VOLUME_PACE = 1.8
FLOW_STRONG_VOLUME_PACE = 2.5

# Price should remain close to shelf while flow develops.
NEAR_SHELF_BELOW_PCT = 3.0
NEAR_SHELF_ABOVE_PCT = 5.0

# Don't chase an already vertical move.
MAX_SIGNAL_MOVE_FROM_SHELF_PCT = 5.0

# ---------- confirmation ----------
SECOND_CEX_MIN_OI_CHANGE_PCT = 0.5
SECOND_CEX_MIN_VOLUME_PACE = 1.35

# ---------- quarantine ----------
QUARANTINE_TTL_SEC = 3 * 3600
MAX_QUARANTINE = 60

# ---------- CoinGlass ----------
COINGLASS_MIN_BINANCE_OI_CHANGE_1H = -1.5

# CoinGlass calls only for confirmed candidates.
COINGLASS_COOLDOWN_SEC = 15 * 60

# ---------- signal ----------
SIGNAL_COOLDOWN_SEC = 6 * 3600

# ---------- request ----------
HTTP_TIMEOUT = 12
MAX_CONCURRENT = 12


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("CEX_AGG_V8")


# ============================================================
# GLOBAL STATE
# ============================================================

UNIVERSE = {}
SHELVES = {}
QUARANTINE = {}
LAST_SIGNAL = {}
LAST_COINGLASS = {}

STATS = {
    "universe": 0,
    "shelves": 0,
    "new_shelves": 0,
    "expired_shelves": 0,
    "quarantine": 0,
    "flow_confirmed": 0,
    "signals": 0,
    "late_rejects": 0,
    "cg_calls": 0,
    "errors": 0,
    "cycles": 0,
    "bitget_requests": 0,
    "kucoin_requests": 0,
    "coinglass_requests": 0,
}

HTTP_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT)


# ============================================================
# HTTP
# ============================================================

async def http_get(session, url, params=None, headers=None):
    async with HTTP_SEMAPHORE:
        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            ) as r:

                if r.status != 200:
                    return None

                return await r.json(content_type=None)

        except Exception as e:
            STATS["errors"] += 1
            log.debug("HTTP error %s | %s", url, e)
            return None


# ============================================================
# HELPERS
# ============================================================

def now():
    return time.time()


def norm_symbol(symbol):
    if not symbol:
        return ""

    s = str(symbol).upper()

    replacements = [
        "-USDT",
        "_USDT",
        "/USDT",
        "USDTM",
    ]

    for x in replacements:
        s = s.replace(x, "")

    return s


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def pct_change(a, b):
    if not a:
        return 0.0
    return ((b - a) / a) * 100.0


def format_pct(x):
    return f"{x:+.2f}%"


def fmt_money(x):
    x = safe_float(x)

    if x >= 1_000_000_000:
        return f"{x / 1_000_000_000:.2f}B"

    if x >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"

    if x >= 1_000:
        return f"{x / 1_000:.1f}K"

    return f"{x:.0f}"


# ============================================================
# BITGET
# ============================================================

BITGET_BASE = "https://api.bitget.com"


async def bitget_tickers(session):
    """
    Broad universe.

    Bitget is intentionally NOT intersected with KuCoin.
    """

    url = f"{BITGET_BASE}/api/v2/mix/market/tickers"

    data = await http_get(
        session,
        url,
        params={
            "productType": "USDT-FUTURES"
        }
    )

    STATS["bitget_requests"] += 1

    if not data:
        return {}

    rows = data.get("data", [])

    result = {}

    for row in rows:
        symbol = norm_symbol(
            row.get("symbol")
            or row.get("instId")
        )

        if not symbol:
            continue

        price = safe_float(
            row.get("lastPr")
            or row.get("last")
            or row.get("lastPrice")
        )

        volume = safe_float(
            row.get("quoteVolume")
            or row.get("usdtVolume")
            or row.get("turnover24h")
            or row.get("baseVolume")
        )

        change = safe_float(
            row.get("change24h")
            or row.get("changeUtc24h")
            or row.get("priceChangePercent")
        )

        # Some Bitget responses provide decimal change,
        # others percent.
        if abs(change) < 1:
            change *= 100

        if price <= 0:
            continue

        if price < MIN_PRICE_USDT or price > MAX_PRICE_USDT:
            continue

        if volume < MIN_24H_VOLUME_USDT:
            continue

        result[symbol] = {
            "symbol": row.get("symbol") or row.get("instId"),
            "price": price,
            "volume24h": volume,
            "change24h": change,
            "updated": now(),
        }

    return result


async def bitget_1h_candles(session, symbol):
    """
    Returns candles oldest -> newest.

    Latest candle is potentially LIVE.
    Second-last candle is CLOSED.
    """

    url = f"{BITGET_BASE}/api/v3/market/candles"

    data = await http_get(
        session,
        url,
        params={
            "category": "USDT-FUTURES",
            "symbol": symbol,
            "interval": "1H",
            "limit": 100
        }
    )

    STATS["bitget_requests"] += 1

    if not data:
        return []

    rows = data.get("data", [])

    candles = []

    for r in rows:
        if len(r) < 7:
            continue

        try:
            candles.append({
                "ts": int(r[0]) / 1000,
                "open": safe_float(r[1]),
                "high": safe_float(r[2]),
                "low": safe_float(r[3]),
                "close": safe_float(r[4]),
                "volume": safe_float(r[5]),
                "quote_volume": safe_float(r[6]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])

    return candles


async def bitget_oi(session, symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/open-interest"

    data = await http_get(
        session,
        url,
        params={
            "productType": "USDT-FUTURES",
            "symbol": symbol
        }
    )

    STATS["bitget_requests"] += 1

    if not data:
        return None

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = [rows]

    if not rows:
        return None

    row = rows[0]

    for key in (
        "openInterest",
        "openInterestValue",
        "holding",
        "oi",
        "openInterestAmount"
    ):
        if key in row:
            value = safe_float(row[key])
            if value > 0:
                return value

    return None


# ============================================================
# KUCOIN
# ============================================================

KUCOIN_BASE = "https://api.kucoin.com"


async def kucoin_contracts(session):
    """
    KuCoin is additional data source.
    It does NOT limit the universe.
    """

    url = f"{KUCOIN_BASE}/api/v1/contracts/active"

    data = await http_get(session, url)

    STATS["kucoin_requests"] += 1

    if not data:
        return {}

    rows = data.get("data", [])

    result = {}

    for row in rows:
        symbol = row.get("symbol", "")

        base = norm_symbol(symbol)

        if not base:
            continue

        if not str(symbol).upper().endswith("USDTM"):
            continue

        result[base] = {
            "symbol": symbol,
            "price": safe_float(
                row.get("markPrice")
                or row.get("lastTradePrice")
            ),
            "volume24h": safe_float(
                row.get("turnoverOf24h")
                or row.get("volumeOf24h")
            ),
            "change24h": safe_float(
                row.get("changeRateOf24h")
            ) * 100,
        }

    return result


async def kucoin_1h_candles(session, symbol):
    url = f"{KUCOIN_BASE}/api/v1/kline/query"

    data = await http_get(
        session,
        url,
        params={
            "symbol": symbol,
            "granularity": 60
        }
    )

    STATS["kucoin_requests"] += 1

    if not data:
        return []

    rows = data.get("data", [])

    candles = []

    for r in rows:

        if len(r) < 7:
            continue

        try:
            candles.append({
                "ts": int(r[0]),
                "open": safe_float(r[1]),
                "close": safe_float(r[2]),
                "high": safe_float(r[3]),
                "low": safe_float(r[4]),
                "volume": safe_float(r[5]),
                "quote_volume": safe_float(r[6]),
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])

    return candles


async def kucoin_oi(session, symbol):
    url = f"{KUCOIN_BASE}/api/v1/ticker"

    data = await http_get(
        session,
        url,
        params={
            "symbol": symbol
        }
    )

    STATS["kucoin_requests"] += 1

    if not data:
        return None

    row = data.get("data") or {}

    for key in (
        "openInterest",
        "openInterestValue"
    ):
        if key in row:
            value = safe_float(row[key])

            if value > 0:
                return value

    return None


# ============================================================
# CANDLE METRICS
# ============================================================

def body_high(c):
    return max(c["open"], c["close"])


def body_low(c):
    return min(c["open"], c["close"])


def shelf_width(candles):
    if not candles:
        return 999

    high = max(body_high(x) for x in candles)
    low = min(body_low(x) for x in candles)

    if low <= 0:
        return 999

    return ((high - low) / low) * 100


def close_drift(candles):
    if not candles:
        return 999

    first = candles[0]["close"]
    last = candles[-1]["close"]

    if first <= 0:
        return 999

    return abs((last - first) / first) * 100


def median_volume(candles):
    values = [
        safe_float(x.get("quote_volume"))
        for x in candles
        if safe_float(x.get("quote_volume")) > 0
    ]

    if not values:
        return 0

    values.sort()

    n = len(values)

    if n % 2:
        return values[n // 2]

    return (values[n // 2 - 1] + values[n // 2]) / 2


# ============================================================
# 1H SHELF DETECTOR
# ============================================================

def find_best_shelf(candles):
    """
    IMPORTANT:

    candles[-1] = LIVE
    candles[-2] = LAST CLOSED

    We NEVER use candles[-1] to construct the shelf.

    Search windows:
    5 -> 12 CLOSED 1H candles.
    """

    if len(candles) < SHELF_MIN_HOURS + 2:
        return None

    closed = candles[:-1]

    best = None

    max_len = min(
        SHELF_MAX_HOURS,
        len(closed)
    )

    for length in range(SHELF_MIN_HOURS, max_len + 1):

        window = closed[-length:]

        width = shelf_width(window)
        drift = close_drift(window)

        if width > SHELF_MAX_WIDTH_PCT:
            continue

        if drift > SHELF_MAX_CLOSE_DRIFT_PCT:
            continue

        high = max(body_high(x) for x in window)
        low = min(body_low(x) for x in window)

        if low <= 0:
            continue

        # Require the majority of closes to remain
        # inside the body range.
        inside = 0

        for c in window:
            if low * 0.997 <= c["close"] <= high * 1.003:
                inside += 1

        inside_ratio = inside / len(window)

        if inside_ratio < 0.70:
            continue

        score = (
            (5.0 - min(width, 5.0)) * 10
            + (3.5 - min(drift, 3.5)) * 5
            + inside_ratio * 20
            + length
        )

        candidate = {
            "high": high,
            "low": low,
            "mid": (high + low) / 2,
            "width_pct": width,
            "drift_pct": drift,
            "length": length,
            "inside_ratio": inside_ratio,
            "start_ts": window[0]["ts"],
            "end_ts": window[-1]["ts"],
            "created": now(),
            "score": score,
        }

        if best is None or candidate["score"] > best["score"]:
            best = candidate

    return best


# ============================================================
# LIVE 1H METRICS
# ============================================================

def live_1h_metrics(candles, shelf):
    if len(candles) < 10:
        return None

    live = candles[-1]
    previous = candles[-2]

    price = live["close"]

    shelf_high = shelf["high"]
    shelf_low = shelf["low"]

    if shelf_high <= 0:
        return None

    move_from_top = ((price / shelf_high) - 1) * 100
    move_from_bottom = ((price / shelf_low) - 1) * 100

    # Elapsed fraction of current 1H candle.
    ts = live["ts"]

    elapsed = max(
        60,
        min(
            3600,
            now() - ts
        )
    )

    fraction = elapsed / 3600.0

    # Normalize current live volume to a full-hour pace.
    current_volume = safe_float(
        live.get("quote_volume")
    )

    reference = candles[-min(13, len(candles)):-1]

    ref_volume = median_volume(reference)

    if ref_volume > 0:
        volume_pace = (
            current_volume / fraction
        ) / ref_volume
    else:
        volume_pace = 0

    price_change_live = pct_change(
        previous["close"],
        live["close"]
    )

    range_pct = pct_change(
        live["open"],
        live["high"]
    )

    return {
        "price": price,
        "move_from_top": move_from_top,
        "move_from_bottom": move_from_bottom,
        "volume_pace": volume_pace,
        "price_change_live": price_change_live,
        "range_pct": range_pct,
        "elapsed_min": elapsed / 60,
        "live": live,
    }


# ============================================================
# FLOW ANALYSIS
# ============================================================

def is_near_shelf(metrics):
    if not metrics:
        return False

    move = metrics["move_from_top"]

    return (
        move >= -NEAR_SHELF_BELOW_PCT
        and move <= NEAR_SHELF_ABOVE_PCT
    )


def flow_strength(oi_change, volume_pace):
    score = 0

    if oi_change >= FLOW_MIN_OI_CHANGE_PCT:
        score += 1

    if oi_change >= FLOW_STRONG_OI_CHANGE_PCT:
        score += 1

    if volume_pace >= FLOW_MIN_VOLUME_PACE:
        score += 1

    if volume_pace >= FLOW_STRONG_VOLUME_PACE:
        score += 1

    return score


def detect_flow(oi_change, volume_pace, metrics):
    if not metrics:
        return False

    if not is_near_shelf(metrics):
        return False

    if oi_change < FLOW_MIN_OI_CHANGE_PCT:
        return False

    if volume_pace < FLOW_MIN_VOLUME_PACE:
        return False

    return True


# ============================================================
# UNIVERSE REFRESH
# ============================================================

async def refresh_universe(session):
    global UNIVERSE

    log.info("Refreshing Bitget universe...")

    bitget = await bitget_tickers(session)

    if not bitget:
        log.warning("Bitget universe returned empty")
        return

    # KuCoin is metadata only.
    # It NEVER restricts Bitget universe.
    kucoin = await kucoin_contracts(session)

    result = {}

    for base, item in bitget.items():

        if base in (
            "USDT",
            "USDC",
            "FDUSD",
            "USD1",
            "TUSD",
            "USDE",
        ):
            continue

        result[base] = {
            "base": base,
            "bitget_symbol": item["symbol"],
            "bitget_price": item["price"],
            "volume24h": item["volume24h"],
            "change24h": item["change24h"],
            "kucoin_symbol": (
                kucoin.get(base, {}).get("symbol")
            ),
            "kucoin_available": base in kucoin,
        }

    # Highest liquidity first.
    result = dict(
        sorted(
            result.items(),
            key=lambda x: x[1]["volume24h"],
            reverse=True
        )[:MAX_UNIVERSE_SYMBOLS]
    )

    UNIVERSE = result

    STATS["universe"] = len(UNIVERSE)

    kucoin_count = sum(
        1
        for x in UNIVERSE.values()
        if x["kucoin_available"]
    )

    log.info(
        "UNIVERSE | Bitget=%d | KuCoin available=%d | limit=%d",
        len(UNIVERSE),
        kucoin_count,
        MAX_UNIVERSE_SYMBOLS
    )


# ============================================================
# BUILD 1H SHELVES
# ============================================================

async def build_shelves(session):
    """
    Full hourly 1H structure scan.

    This is deliberately separate from the live monitoring loop.
    """

    global SHELVES

    bases = list(UNIVERSE.keys())

    log.info(
        "1H STRUCTURE SCAN | symbols=%d",
        len(bases)
    )

    semaphore = asyncio.Semaphore(8)

    async def one(base):

        async with semaphore:

            info = UNIVERSE.get(base)

            if not info:
                return None

            symbol = info["bitget_symbol"]

            candles = await bitget_1h_candles(
                session,
                symbol
            )

            if len(candles) < 20:
                return None

            shelf = find_best_shelf(candles)

            if not shelf:
                return None

            shelf["base"] = base
            shelf["bitget_symbol"] = symbol
            shelf["kucoin_symbol"] = info.get(
                "kucoin_symbol"
            )

            # OI baseline when shelf is created.
            oi = await bitget_oi(
                session,
                symbol
            )

            shelf["bitget_oi_base"] = oi

            ku_oi = None

            if shelf["kucoin_symbol"]:
                ku_oi = await kucoin_oi(
                    session,
                    shelf["kucoin_symbol"]
                )

            shelf["kucoin_oi_base"] = ku_oi

            return base, shelf

    tasks = [
        asyncio.create_task(one(base))
        for base in bases
    ]

    found = {}

    for task in asyncio.as_completed(tasks):

        try:
            result = await task

            if result:
                base, shelf = result
                found[base] = shelf

        except Exception as e:
            STATS["errors"] += 1
            log.debug("Shelf error: %s", e)

    # Keep only strongest shelves.
    ranked = sorted(
        found.items(),
        key=lambda x: x[1]["score"],
        reverse=True
    )[:MAX_ACTIVE_SHELVES]

    old_keys = set(SHELVES.keys())

    SHELVES = dict(ranked)

    new_keys = set(SHELVES.keys()) - old_keys

    STATS["shelves"] = len(SHELVES)
    STATS["new_shelves"] += len(new_keys)

    log.info(
        "1H STRUCTURE | shelves=%d | new=%d",
        len(SHELVES),
        len(new_keys)
    )

    for base in new_keys:

        s = SHELVES[base]

        log.info(
            "NEW SHELF | %s | %dH | %.2f%% | %.8f -> %.8f",
            base,
            s["length"],
            s["width_pct"],
            s["low"],
            s["high"]
        )


# ============================================================
# QUARANTINE
# ============================================================

def quarantine_symbol(
    base,
    leader,
    shelf,
    oi_change,
    volume_pace,
    metrics
):

    if base in QUARANTINE:
        return

    if len(QUARANTINE) >= MAX_QUARANTINE:
        return

    QUARANTINE[base] = {
        "base": base,
        "leader": leader,
        "created": now(),
        "expires": now() + QUARANTINE_TTL_SEC,

        "shelf_high": shelf["high"],
        "shelf_low": shelf["low"],

        "leader_oi_change": oi_change,
        "leader_volume_pace": volume_pace,

        "leader_price": metrics["price"],
        "leader_move": metrics["move_from_top"],

        "second_confirmed": False,
        "coinglass_checked": False,
    }

    STATS["quarantine"] += 1

    log.info(
        "QUARANTINE | %s | leader=%s | OI=%+.2f%% | pace=%.2fx",
        base,
        leader,
        oi_change,
        volume_pace
    )


def cleanup_quarantine():
    expired = []

    for base, q in QUARANTINE.items():

        if now() >= q["expires"]:
            expired.append(base)

    for base in expired:
        QUARANTINE.pop(base, None)


def cleanup_shelves():
    expired = []

    for base, shelf in SHELVES.items():

        if now() - shelf["created"] > SHELF_TTL_SEC:
            expired.append(base)

    for base in expired:
        SHELVES.pop(base, None)
        STATS["expired_shelves"] += 1


# ============================================================
# COINGLASS
# ============================================================

COINGLASS_BASE = (
    "https://open-api-v4.coinglass.com"
)


async def coinglass_binance_check(session, base):
    """
    Binance is checked ONLY here.

    We use CoinGlass OI-by-exchange endpoint.
    """

    if not COINGLASS_API_KEY:
        return {
            "available": False,
            "reason": "COINGLASS_API_KEY not set"
        }

    last_call = LAST_COINGLASS.get(base, 0)

    if now() - last_call < COINGLASS_COOLDOWN_SEC:
        return {
            "available": False,
            "reason": "cooldown"
        }

    LAST_COINGLASS[base] = now()

    url = (
        f"{COINGLASS_BASE}"
        "/api/futures/open-interest/exchange-list"
    )

    headers = {
        "CG-API-KEY": COINGLASS_API_KEY
    }

    data = await http_get(
        session,
        url,
        params={
            "symbol": base
        },
        headers=headers
    )

    STATS["coinglass_requests"] += 1
    STATS["cg_calls"] += 1

    if not data:
        return {
            "available": False,
            "reason": "empty response"
        }

    if str(data.get("code")) not in ("0", "200", "None"):
        return {
            "available": False,
            "reason": str(data.get("msg", "API error"))
        }

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = [rows]

    binance = None

    for row in rows:

        if str(
            row.get("exchange", "")
        ).lower() == "binance":

            binance = row
            break

    if not binance:
        return {
            "available": False,
            "reason": "Binance not found"
        }

    oi_1h = safe_float(
        binance.get(
            "open_interest_change_percent_1h"
        )
    )

    oi_5m = safe_float(
        binance.get(
            "open_interest_change_percent_5m"
        )
    )

    oi_15m = safe_float(
        binance.get(
            "open_interest_change_percent_15m"
        )
    )

    oi_usd = safe_float(
        binance.get("open_interest_usd")
    )

    return {
        "available": True,
        "oi_1h": oi_1h,
        "oi_5m": oi_5m,
        "oi_15m": oi_15m,
        "oi_usd": oi_usd,

        # Binance is confirmation, not a mandatory
        # third "vote".
        "positive": oi_1h >= COINGLASS_MIN_BINANCE_OI_CHANGE_1H
    }


# ============================================================
# TELEGRAM
# ============================================================

async def send_message(bot, text):
    if not BOT_TOKEN or not CHAT_ID:
        return

    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text
        )
    except Exception as e:
        log.error("Telegram error: %s", e)


async def send_quarantine_alert(
    bot,
    base,
    leader,
    shelf,
    metrics,
    oi_change,
    volume_pace
):

    text = (
        "🟡 КАРАНТИН — РАННИЙ ПОТОК\n\n"
        f"#{base}\n"
        f"Первый CEX: {leader}\n\n"
        f"1H полка: {shelf['length']}ч\n"
        f"Ширина: {shelf['width_pct']:.2f}%\n"
        f"Верх: {shelf['high']:.8g}\n"
        f"Низ: {shelf['low']:.8g}\n\n"
        f"{leader} OI: {oi_change:+.2f}%\n"
        f"{leader} volume pace: {volume_pace:.2f}x\n"
        f"От верхушки полки: "
        f"{metrics['move_from_top']:+.2f}%\n\n"
        "⏳ Ждём подключение второго CEX."
    )

    await send_message(bot, text)


async def send_signal(
    bot,
    base,
    shelf,
    quarantine,
    second_oi,
    second_pace,
    cg
):

    text = (
        "🚨 CEX FLOW CONFIRMED\n\n"
        f"#{base}\n\n"
        "1H КОНТЕКСТ\n"
        f"Полка: {shelf['length']}ч\n"
        f"Ширина: {shelf['width_pct']:.2f}%\n"
        f"Верх: {shelf['high']:.8g}\n"
        f"Низ: {shelf['low']:.8g}\n\n"
        "ПОТОК\n"
        f"1-й CEX: {quarantine['leader']}\n"
        f"OI: {quarantine['leader_oi_change']:+.2f}%\n"
        f"Volume pace: "
        f"{quarantine['leader_volume_pace']:.2f}x\n\n"
        "2-й CEX\n"
        f"OI: {second_oi:+.2f}%\n"
        f"Volume pace: {second_pace:.2f}x\n\n"
    )

    if cg.get("available"):

        text += (
            "BINANCE / COINGLASS\n"
            f"OI 5m: {cg['oi_5m']:+.2f}%\n"
            f"OI 15m: {cg['oi_15m']:+.2f}%\n"
            f"OI 1H: {cg['oi_1h']:+.2f}%\n"
            f"OI: ${fmt_money(cg['oi_usd'])}\n\n"
        )

    else:

        text += (
            "BINANCE / COINGLASS\n"
            f"Проверка: {cg.get('reason', 'N/A')}\n\n"
        )

    text += (
        "🟢 Раннее подтверждение потока\n"
        "Не сигнал по принципу «2 биржи = вход».\n"
        "Подтверждена последовательность "
        "развития потока."
    )

    await send_message(bot, text)


# ============================================================
# CANDIDATE MONITOR
# ============================================================

async def monitor_shelf(
    session,
    bot,
    base,
    shelf
):

    info = UNIVERSE.get(base)

    if not info:
        return

    symbol = info["bitget_symbol"]

    # --------------------------------------------------------
    # BITGET LIVE
    # --------------------------------------------------------

    bg_candles = await bitget_1h_candles(
        session,
        symbol
    )

    if len(bg_candles) < 20:
        return

    bg_metrics = live_1h_metrics(
        bg_candles,
        shelf
    )

    if not bg_metrics:
        return

    # Current Bitget OI.
    bg_oi = await bitget_oi(
        session,
        symbol
    )

    bg_base_oi = shelf.get(
        "bitget_oi_base"
    )

    bg_oi_change = 0.0

    if bg_oi and bg_base_oi:
        bg_oi_change = pct_change(
            bg_base_oi,
            bg_oi
        )

    # --------------------------------------------------------
    # KUCOIN LIVE
    # --------------------------------------------------------

    ku_metrics = None
    ku_oi_change = 0.0

    ku_symbol = shelf.get(
        "kucoin_symbol"
    )

    if ku_symbol:

        ku_candles = await kucoin_1h_candles(
            session,
            ku_symbol
        )

        if len(ku_candles) >= 10:

            ku_metrics = live_1h_metrics(
                ku_candles,
                shelf
            )

        ku_oi = await kucoin_oi(
            session,
            ku_symbol
        )

        ku_base_oi = shelf.get(
            "kucoin_oi_base"
        )

        if ku_oi and ku_base_oi:

            ku_oi_change = pct_change(
                ku_base_oi,
                ku_oi
            )

    # --------------------------------------------------------
    # FLOW DETECTION
    # --------------------------------------------------------

    bg_flow = detect_flow(
        bg_oi_change,
        bg_metrics["volume_pace"],
        bg_metrics
    )

    ku_flow = False

    if ku_metrics:

        ku_flow = detect_flow(
            ku_oi_change,
            ku_metrics["volume_pace"],
            ku_metrics
        )

    # --------------------------------------------------------
    # EXISTING QUARANTINE
    # --------------------------------------------------------

    q = QUARANTINE.get(base)

    # ========================================================
    # NO QUARANTINE YET
    # ========================================================

    if not q:

        # Bitget is first.
        if bg_flow:

            quarantine_symbol(
                base=base,
                leader="Bitget",
                shelf=shelf,
                oi_change=bg_oi_change,
                volume_pace=bg_metrics["volume_pace"],
                metrics=bg_metrics
            )

            await send_quarantine_alert(
                bot,
                base,
                "Bitget",
                shelf,
                bg_metrics,
                bg_oi_change,
                bg_metrics["volume_pace"]
            )

            return

        # KuCoin is first.
        if ku_flow:

            quarantine_symbol(
                base=base,
                leader="KuCoin",
                shelf=shelf,
                oi_change=ku_oi_change,
                volume_pace=ku_metrics["volume_pace"],
                metrics=ku_metrics
            )

            await send_quarantine_alert(
                bot,
                base,
                "KuCoin",
                shelf,
                ku_metrics,
                ku_oi_change,
                ku_metrics["volume_pace"]
            )

            return

        return

    # ========================================================
    # QUARANTINE ALREADY EXISTS
    # ========================================================

    leader = q["leader"]

    if leader == "Bitget":

        second_flow = (
            ku_metrics is not None
            and ku_oi_change >= SECOND_CEX_MIN_OI_CHANGE_PCT
            and ku_metrics["volume_pace"]
                >= SECOND_CEX_MIN_VOLUME_PACE
            and is_near_shelf(ku_metrics)
        )

        second_oi = ku_oi_change
        second_pace = (
            ku_metrics["volume_pace"]
            if ku_metrics else 0
        )

        current_metrics = (
            ku_metrics
            if ku_metrics
            else bg_metrics
        )

    else:

        second_flow = (
            bg_oi_change >= SECOND_CEX_MIN_OI_CHANGE_PCT
            and bg_metrics["volume_pace"]
                >= SECOND_CEX_MIN_VOLUME_PACE
            and is_near_shelf(bg_metrics)
        )

        second_oi = bg_oi_change
        second_pace = bg_metrics["volume_pace"]

        current_metrics = bg_metrics

    if not second_flow:
        return

    # ========================================================
    # BOTH CEX ARE NOW ACTIVE
    # ========================================================

    if q.get("second_confirmed"):
        return

    q["second_confirmed"] = True

    STATS["flow_confirmed"] += 1

    move_from_top = current_metrics[
        "move_from_top"
    ]

    # ========================================================
    # DON'T CHASE
    # ========================================================

    if move_from_top > MAX_SIGNAL_MOVE_FROM_SHELF_PCT:

        STATS["late_rejects"] += 1

        log.info(
            "LATE REJECT | %s | move=%.2f%%",
            base,
            move_from_top
        )

        return

    # ========================================================
    # COINGLASS
    # ========================================================

    cg = await coinglass_binance_check(
        session,
        base
    )

    # ========================================================
    # SIGNAL COOLDOWN
    # ========================================================

    last = LAST_SIGNAL.get(base, 0)

    if now() - last < SIGNAL_COOLDOWN_SEC:
        return

    LAST_SIGNAL[base] = now()

    STATS["signals"] += 1

    await send_signal(
        bot=bot,
        base=base,
        shelf=shelf,
        quarantine=q,
        second_oi=second_oi,
        second_pace=second_pace,
        cg=cg
    )

    log.info(
        "SIGNAL | %s | leader=%s | second OI=%.2f%%",
        base,
        leader,
        second_oi
    )


# ============================================================
# MONITOR ALL SHELVES
# ============================================================

async def monitor_shelves(session, bot):

    cleanup_shelves()
    cleanup_quarantine()

    if not SHELVES:
        return

    bases = list(SHELVES.keys())

    # Limit simultaneous live monitoring.
    semaphore = asyncio.Semaphore(8)

    async def one(base):

        async with semaphore:

            try:
                await monitor_shelf(
                    session,
                    bot,
                    base,
                    SHELVES[base]
                )

            except Exception as e:

                STATS["errors"] += 1

                log.debug(
                    "Monitor error %s | %s",
                    base,
                    e
                )

    await asyncio.gather(
        *[
            asyncio.create_task(one(base))
            for base in bases
        ],
        return_exceptions=True
    )


# ============================================================
# HOURLY AUDIT
# ============================================================

async def hourly_audit():

    while True:

        await asyncio.sleep(3600)

        try:

            q_count = len(QUARANTINE)

            log.info(
                "DATA AUDIT | "
                "universe=%d | shelves=%d | "
                "quarantine=%d | confirmed=%d | "
                "signals=%d | CG=%d | errors=%d",
                len(UNIVERSE),
                len(SHELVES),
                q_count,
                STATS["flow_confirmed"],
                STATS["signals"],
                STATS["cg_calls"],
                STATS["errors"]
            )

        except Exception as e:

            log.error(
                "Audit error: %s",
                e
            )


# ============================================================
# SCANNER
# ============================================================

async def scanner_loop(bot):

    global STATS

    async with aiohttp.ClientSession() as session:

        last_universe_refresh = 0
        last_structure_scan = 0

        while True:

            try:

                current = now()

                # ------------------------------------------------
                # FULL UNIVERSE + 1H STRUCTURE
                # ------------------------------------------------

                if (
                    current
                    - last_universe_refresh
                    >= UNIVERSE_REFRESH_SEC
                ):

                    await refresh_universe(
                        session
                    )

                    last_universe_refresh = current

                    await build_shelves(
                        session
                    )

                    last_structure_scan = current

                # ------------------------------------------------
                # If startup / empty universe.
                # ------------------------------------------------

                if not UNIVERSE:

                    await refresh_universe(
                        session
                    )

                    await build_shelves(
                        session
                    )

                    last_universe_refresh = now()
                    last_structure_scan = now()

                # ------------------------------------------------
                # LIVE MONITORING
                # ------------------------------------------------

                await monitor_shelves(
                    session,
                    bot
                )

                STATS["cycles"] += 1

                if STATS["cycles"] % 10 == 0:

                    log.info(
                        "SCAN | universe=%d | "
                        "shelves=%d | quarantine=%d | "
                        "signals=%d",
                        len(UNIVERSE),
                        len(SHELVES),
                        len(QUARANTINE),
                        STATS["signals"]
                    )

                await asyncio.sleep(
                    MONITOR_INTERVAL_SEC
                )

            except Exception as e:

                STATS["errors"] += 1

                log.exception(
                    "Scanner loop error: %s",
                    e
                )

                await asyncio.sleep(10)


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def cmd_start(message: Message):

    await message.answer(
        "🛰 CEX AGGREGATOR v8.0\n\n"
        "1H SHELF → FLOW → QUARANTINE → "
        "2nd CEX → COINGLASS\n\n"
        "Bitget + KuCoin\n"
        "Binance = CoinGlass only"
    )


async def cmd_stats(message: Message):

    text = (
        "📊 CEX AGGREGATOR v8\n\n"
        f"Universe: {len(UNIVERSE)}\n"
        f"1H shelves: {len(SHELVES)}\n"
        f"Quarantine: {len(QUARANTINE)}\n\n"
        f"New shelves: {STATS['new_shelves']}\n"
        f"Confirmed flow: {STATS['flow_confirmed']}\n"
        f"Signals: {STATS['signals']}\n"
        f"Late rejects: {STATS['late_rejects']}\n"
        f"CoinGlass calls: {STATS['cg_calls']}\n"
        f"Errors: {STATS['errors']}\n\n"
        f"Bitget req: {STATS['bitget_requests']}\n"
        f"KuCoin req: {STATS['kucoin_requests']}\n"
    )

    await message.answer(text)


async def cmd_wl(message: Message):

    if not QUARANTINE:

        await message.answer(
            "🟢 Карантин пуст."
        )

        return

    rows = []

    for base, q in QUARANTINE.items():

        age = (
            now() - q["created"]
        ) / 60

        rows.append(
            f"#{base} | "
            f"{q['leader']} | "
            f"OI {q['leader_oi_change']:+.2f}% | "
            f"pace {q['leader_volume_pace']:.2f}x | "
            f"{age:.0f}m"
        )

    await message.answer(
        "🟡 QUARANTINE\n\n"
        + "\n".join(rows[:60])
    )


async def cmd_check(message: Message):

    if not SHELVES:

        await message.answer(
            "Пока 1H полок не найдено."
        )

        return

    rows = []

    for base, s in sorted(
        SHELVES.items(),
        key=lambda x: x[1]["score"],
        reverse=True
    )[:30]:

        rows.append(
            f"#{base} | "
            f"{s['length']}H | "
            f"{s['width_pct']:.2f}% | "
            f"{s['low']:.8g}-{s['high']:.8g}"
        )

    await message.answer(
        "🧱 ACTIVE 1H SHELVES\n\n"
        + "\n".join(rows)
    )


async def cmd_test(message: Message):

    await message.answer(
        "✅ CEX Aggregator v8 работает.\n"
        f"Universe: {len(UNIVERSE)}\n"
        f"Shelves: {len(SHELVES)}\n"
        f"Quarantine: {len(QUARANTINE)}"
    )


# ============================================================
# HEALTH SERVER
# ============================================================

async def health(request):

    return web.Response(
        text=(
            "CEX AGGREGATOR v8 ACTIVE | "
            f"Universe={len(UNIVERSE)} | "
            f"Shelves={len(SHELVES)} | "
            f"Quarantine={len(QUARANTINE)} | "
            f"Signals={STATS['signals']}"
        )
    )


async def start_web():

    app = web.Application()

    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    log.info(
        "HTTP server started on port %d",
        PORT
    )

    while True:
        await asyncio.sleep(3600)


# ============================================================
# MAIN
# ============================================================

async def main():

    if not BOT_TOKEN:

        log.warning(
            "BOT_TOKEN is not configured"
        )

    if not CHAT_ID:

        log.warning(
            "CHAT_ID is not configured"
        )

    bot = Bot(
        token=BOT_TOKEN
    )

    dp = Dispatcher()

    dp.message.register(
        cmd_start,
        Command("start")
    )

    dp.message.register(
        cmd_stats,
        Command("stats")
    )

    dp.message.register(
        cmd_wl,
        Command("wl")
    )

    dp.message.register(
        cmd_check,
        Command("check")
    )

    dp.message.register(
        cmd_test,
        Command("test")
    )

    await asyncio.gather(
        scanner_loop(bot),
        hourly_audit(),
        start_web(),
        dp.start_polling(bot)
    )


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        log.info(
            "CEX Aggregator stopped."
        )
