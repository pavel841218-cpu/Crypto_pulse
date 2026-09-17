import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque


# ============================================================
#       CEX FUTURES AGGREGATOR v3.0
#       Bitget + Binance Futures (KuCoin removed)
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 120
CANDLE_REFRESH_SEC = 60
OI_REFRESH_SEC = 60

MAX_COMMON_SYMBOLS = 400
MAX_CANDLE_CANDIDATES = 400

SIGNAL_COOLDOWN_SEC = 4 * 3600
FLOW_COOLDOWN_SEC = 90 * 60

RECENT_PUMP_LOOKBACK_SEC = 30 * 60
RECENT_PUMP_THRESHOLD_PCT = 5.0

HOURLY_LOG_SEC = 3600


# ============================================================
# Strategy thresholds
# ============================================================

MIN_24H_VOLUME_USDT = 400_000

MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 1.0

IGNITION_MIN_MOVE_PCT = 0.50
IGNITION_MIN_FORMING_RVOL = 1.50
IGNITION_MAX_MOVE_PCT = 3.0

MIN_5M_MOVE_PCT = 0.65
STRONG_5M_MOVE_PCT = 1.20

MIN_RVOL = 2.0
STRONG_RVOL = 3.0

MIN_AGG_RVOL = 1.80
STRONG_AGG_RVOL = 2.50

MIN_OI_GROWTH_PCT = 0.20
STRONG_OI_GROWTH_PCT = 0.60

MAX_5M_MOVE_FOR_EARLY = 6.0
MAX_BREAKOUT_DISTANCE_PCT = 4.0


# ============================================================
# URLs
# ============================================================

BITGET_BASE = "https://api.bitget.com"
BINANCE_FUTURES_BASE = "https://fapi.binance.com"


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("CEX-AGGREGATOR")


# ============================================================
# Runtime state
# ============================================================

START_TIME = time.time()
SESSION = None
BACKGROUND_TASKS = set()

COMMON_SYMBOLS = {}

BITGET_DATA = {}
BINANCE_DATA = {}

# NEW: список активных фьючерсных символов Binance
BINANCE_FUTURES_SYMBOLS = set()

CANDLE_CACHE = {}

OI_HISTORY = defaultdict(lambda: {
    "bitget": deque(maxlen=12),
    "binance": deque(maxlen=12),
})

LAST_SIGNAL = {}
LAST_FLOW = {}
LAST_HOURLY_LOG = 0

STATS = {
    "bitget_requests": 0,
    "binance_requests": 0,

    "bitget_errors": 0,
    "binance_errors": 0,

    "universe_refresh": 0,
    "candle_checks": 0,
    "oi_checks": 0,

    "common_symbols": 0,
    "candle_candidates": 0,
    "oi_candidates": 0,

    "price_in_range": 0,

    "ignition_signals": 0,
    "flow_confirmed": 0,
    "binance_confirmed": 0,

    "early_signals": 0,
    "distribution_signals": 0,

    "rejected_no_sync": 0,
    "rejected_volume": 0,
    "rejected_structure": 0,
    "rejected_too_late": 0,
    "rejected_distribution": 0,
    "rejected_recent_pump": 0,
    "rejected_oi": 0,

    "bg_oi_from_api": 0,
    "bg_oi_from_ticker": 0,
    "bg_oi_missing": 0,

    "bn_oi_ok": 0,
    "bn_oi_fail": 0,
}


# ============================================================
# HTTP helper
# ============================================================

async def http_get(url, params=None, headers=None, timeout=10, exchange="other"):
    global SESSION

    key_req = f"{exchange}_requests"
    key_err = f"{exchange}_errors"

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
                log.warning("429 rate limit: %s", exchange)
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            if resp.status >= 500:
                log.warning("%s server error %s", exchange, resp.status)
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            if resp.status != 200:
                text = await resp.text()
                log.warning("%s HTTP %s: %s", exchange, resp.status, text[:200])
                if key_err in STATS:
                    STATS[key_err] += 1
                return None

            return await resp.json()

    except asyncio.TimeoutError:
        log.warning("%s timeout", exchange)
        if key_err in STATS:
            STATS[key_err] += 1
        return None

    except Exception as e:
        log.warning("%s request error: %s", exchange, e)
        if key_err in STATS:
            STATS[key_err] += 1
        return None


# ============================================================
# Helpers
# ============================================================

def num(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def normalize_base(symbol):
    if not symbol:
        return ""

    s = str(symbol).upper()

    if s.startswith("XBT"):
        return "BTC"

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

    data = await http_get(url, params=params, exchange="bitget")
    result = {}
    if not data:
        return result

    rows = data.get("data", [])
    if not isinstance(rows, list):
        return result

    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue

        base = normalize_base(symbol)
        if not base:
            continue

        price = num(row.get("lastPr") or row.get("lastPrice") or row.get("last"))
        quote_volume = num(
            row.get("quoteVolume")
            or row.get("usdtVolume")
            or row.get("quoteVol")
            or row.get("turnover")
        )
        change = num(
            row.get("change24h")
            or row.get("changeUtc24h")
            or row.get("priceChangePercent")
        )

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": quote_volume,
            "change24": change,
            "raw": row,
        }

    return result


async def fetch_bitget_candles(symbol):
    url = f"{BITGET_BASE}/api/v2/mix/market/candles"
    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "granularity": "5m",
        "limit": "24",
    }

    data = await http_get(url, params=params, exchange="bitget")
    if not data:
        return []

    rows = data.get("data", [])
    candles = []

    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts = int(row[0])
            o = num(row[1])
            h = num(row[2])
            l = num(row[3])
            c = num(row[4])
            v = num(row[5])
            if o <= 0 or c <= 0:
                continue
            candles.append({
                "ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_oi(symbol):
    if not symbol.endswith("USDT"):
        symbol = f"{symbol}USDT"

    url_oi = f"{BITGET_BASE}/api/v2/mix/market/open-interest"
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}

    data = await http_get(url_oi, params=params, exchange="bitget")

    if data and data.get("code") == "00000":
        raw_data = data.get("data", {})
        row = {}

        if isinstance(raw_data, dict):
            list_data = raw_data.get("list", [])
            if isinstance(list_data, list) and list_data:
                row = list_data[0]
            else:
                row = raw_data
        elif isinstance(raw_data, list) and raw_data:
            row = raw_data[0]

        val = num(
            row.get("amount")
            or row.get("openInterest")
            or row.get("size")
            or row.get("openInterestUsd")
            or row.get("oi")
            or row.get("holdingAmount")
        )
        if val > 0:
            STATS["bg_oi_from_api"] += 1
            return val

    base = normalize_base(symbol)
    if base in BITGET_DATA:
        raw_t = BITGET_DATA[base].get("raw", {}) or {}
        fallback_val = num(
            raw_t.get("openInterest")
            or raw_t.get("holdAcm")
            or raw_t.get("openInterestSize")
            or raw_t.get("openInterestUsd")
            or raw_t.get("holdingAmount")
            or raw_t.get("oi")
        )
        if fallback_val > 0:
            STATS["bg_oi_from_ticker"] += 1
            return fallback_val

    STATS["bg_oi_missing"] += 1
    return 0.0


# ============================================================
# BINANCE FUTURES
# ============================================================

async def fetch_binance_futures_symbols():
    """
    Один запрос к exchangeInfo, кэшируем список символов.
    Позволяет отсекать монеты, которых нет на Binance Futures.
    """
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/exchangeInfo"
    data = await http_get(url, exchange="binance")

    result = set()
    if not data or not isinstance(data, dict):
        return result

    symbols = data.get("symbols", [])
    if not isinstance(symbols, list):
        return result

    for row in symbols:
        if str(row.get("status", "")).upper() != "TRADING":
            continue
        if str(row.get("contractType", "")).upper() != "PERPETUAL":
            continue
        if str(row.get("quoteAsset", "")).upper() != "USDT":
            continue

        symbol = str(row.get("symbol", "")).upper()
        base = normalize_base(symbol)
        if base:
            result.add(base)

    return result


async def fetch_binance_tickers():
    """
    Тикеры Binance Futures через fapi/v1/ticker/24hr.
    Работает из Франкфурта (проверено).
    """
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/ticker/24hr"
    data = await http_get(url, exchange="binance")

    result = {}
    if not data or not isinstance(data, list):
        return result

    for row in data:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue

        base = normalize_base(symbol)
        if not base:
            continue

        price = num(row.get("lastPrice"))
        volume24 = num(row.get("quoteVolume"))
        change24 = num(row.get("priceChangePercent"))

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": volume24,
            "change24": change24,
        }

    return result


async def fetch_binance_candles(symbol):
    """
    Klines Binance Futures (5m).
    Формат: [openTime, open, high, low, close, volume, ...]
    """
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "5m",
        "limit": 24,
    }

    data = await http_get(url, params=params, exchange="binance")
    if not data or not isinstance(data, list):
        return []

    candles = []
    for row in data:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts = int(row[0])
            o = num(row[1])
            h = num(row[2])
            l = num(row[3])
            c = num(row[4])
            v = num(row[5])
            if o <= 0 or c <= 0:
                continue
            candles.append({
                "ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_binance_oi_direct(base):
    symbol = f"{base.upper()}USDT"
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest"
    params = {"symbol": symbol}

    data = await http_get(url, params=params, exchange="binance")

    if not data or not isinstance(data, dict):
        STATS["bn_oi_fail"] += 1
        return 0.0

    oi = num(data.get("openInterest"))
    if oi > 0:
        STATS["bn_oi_ok"] += 1
        return oi

    STATS["bn_oi_fail"] += 1
    return 0.0


# ============================================================
# CANDLE SYNC / METRICS
# ============================================================

def sync_candles_by_time(candles_a, candles_b, tolerance_ms=60000):
    if not candles_a or not candles_b:
        return None, None

    target_a = candles_a[-2] if len(candles_a) >= 2 else candles_a[-1]
    target_ts = target_a["ts"]

    best = None
    best_diff = None

    for c in candles_b:
        diff = abs(c["ts"] - target_ts)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = c

    if best_diff is not None and best_diff <= tolerance_ms:
        return target_a, best

    return target_a, None


def candle_metrics_for(candle, previous_candles):
    if not candle or not previous_candles:
        return None

    close = candle["close"]
    open_price = candle["open"]
    high = candle["high"]
    low = candle["low"]

    if open_price <= 0:
        return None

    vol_usd = candle["volume"] * close

    move_pct = ((close / open_price) - 1) * 100
    candle_range = high - low
    if candle_range <= 0:
        return None

    body = abs(close - open_price)
    body_ratio = body / candle_range
    close_position = (close - low) / candle_range

    vols_usd = [(x["volume"] * x["close"]) for x in previous_candles[-12:] if x["volume"] > 0]
    if not vols_usd:
        return None

    avg_vol_usd = sum(vols_usd) / len(vols_usd)
    if avg_vol_usd <= 0:
        return None

    rvol = vol_usd / avg_vol_usd

    previous_high = max(x["high"] for x in previous_candles[-6:])
    previous_low = min(x["low"] for x in previous_candles[-6:])

    upper_wick = high - max(open_price, close)
    lower_wick = min(open_price, close) - low

    return {
        "ts": candle["ts"],
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": candle["volume"],
        "volume_usd": vol_usd,
        "move_pct": move_pct,
        "rvol": rvol,
        "body_ratio": body_ratio,
        "close_position": close_position,
        "previous_high": previous_high,
        "previous_low": previous_low,
        "breakout_up_pct": ((close / previous_high) - 1) * 100,
        "breakout_down_pct": ((previous_low / close) - 1) * 100,
        "upper_wick_ratio": upper_wick / candle_range,
        "lower_wick_ratio": lower_wick / candle_range,
    }


def candle_metrics(candles):
    if len(candles) < 8:
        return None
    return candle_metrics_for(candles[-2], candles[:-2])


# ============================================================
# UNIVERSE (Bitget ∩ Binance Futures, price filter)
# ============================================================

def build_common_universe(bitget, binance):
    common_bases = set(bitget.keys()) & set(binance.keys()) & BINANCE_FUTURES_SYMBOLS
    common = {}

    for base in common_bases:
        b = bitget[base]
        bn = binance[base]

        volume_b = b["volume24"]
        volume_bn = bn["volume24"]

        if volume_b < MIN_24H_VOLUME_USDT or volume_bn < MIN_24H_VOLUME_USDT:
            continue

        price_b = num(b.get("price"))
        price_bn = num(bn.get("price"))
        if price_b <= 0 or price_bn <= 0:
            continue

        avg_price = (price_b + price_bn) / 2
        if avg_price < MIN_PRICE_USDT or avg_price > MAX_PRICE_USDT:
            continue

        common[base] = {
            "bitget": b,
            "binance": bn,
            "aggregate_volume": volume_b + volume_bn,
            "price": avg_price,
        }

    return dict(
        sorted(
            common.items(),
            key=lambda x: x[1]["aggregate_volume"],
            reverse=True
        )[:MAX_COMMON_SYMBOLS]
    )


async def get_candles(base):
    now = time.time()
    cached = CANDLE_CACHE.get(base)
    if cached and now - cached["time"] < CANDLE_REFRESH_SEC:
        return cached["data"]

    item = COMMON_SYMBOLS.get(base)
    if not item:
        return None

    bitget_candles, binance_candles = await asyncio.gather(
        fetch_bitget_candles(item["bitget"]["symbol"]),
        fetch_binance_candles(item["binance"]["symbol"]),
    )

    result = {"bitget": bitget_candles, "binance": binance_candles}
    CANDLE_CACHE[base] = {"time": now, "data": result}
    return result


# ============================================================
# RECENT DUMP
# ============================================================

def has_recent_dump(candles_5m):
    if len(candles_5m) < 8:
        return False

    lookback_candles = RECENT_PUMP_LOOKBACK_SEC // (5 * 60)
    recent = candles_5m[-lookback_candles - 2:-2]
    if len(recent) < 3:
        return False

    first_open = recent[0]["open"]
    min_low = min(c["low"] for c in recent)
    if first_open <= 0:
        return False

    move = ((min_low / first_open) - 1) * 100
    return move <= -RECENT_PUMP_THRESHOLD_PCT


# ============================================================
# IGNITION
# ============================================================

def analyze_ignition(base, candles):
    bg = candles.get("bitget", [])
    bn = candles.get("binance", [])
    if len(bg) < 8 or len(bn) < 8:
        return None

    fb, fbn = bg[-1], bn[-1]
    if abs(fb["ts"] - fbn["ts"]) > 120_000:
        return None

    def forming_metrics(c, closed):
        o = c["open"]
        if o <= 0:
            return None
        move = ((c["close"] / o) - 1) * 100
        vols_usd = [(x["volume"] * x["close"]) for x in closed[-12:] if x["volume"] > 0]
        if not vols_usd:
            return None
        avg_vol_usd = sum(vols_usd) / len(vols_usd)
        if avg_vol_usd <= 0:
            return None
        return move, (c["volume"] * c["close"]) / avg_vol_usd

    mb = forming_metrics(fb, bg[:-1])
    mbn = forming_metrics(fbn, bn[:-1])
    if not mb or not mbn:
        return None

    avg_move = (mb[0] + mbn[0]) / 2
    up = avg_move >= IGNITION_MIN_MOVE_PCT and mb[0] > 0.1 and mbn[0] > 0.1
    down = avg_move <= -IGNITION_MIN_MOVE_PCT and mb[0] < -0.1 and mbn[0] < -0.1
    if not (up or down):
        return None

    if abs(avg_move) > IGNITION_MAX_MOVE_PCT:
        return None

    if not (mb[1] >= IGNITION_MIN_FORMING_RVOL or mbn[1] >= IGNITION_MIN_FORMING_RVOL):
        return None

    return {
        "type": "IGNITION",
        "direction": "LONG" if up else "SHORT",
        "bitget_move": mb[0],
        "binance_move": mbn[0],
        "bitget_forming_rvol": mb[1],
        "binance_forming_rvol": mbn[1],
    }


# ============================================================
# FLOW
# ============================================================

def analyze_flow(base, candles):
    if not candles:
        return None

    bitget_candles = candles.get("bitget", [])
    binance_candles = candles.get("binance", [])

    b_candle, bn_candle = sync_candles_by_time(bitget_candles, binance_candles)
    if not b_candle or not bn_candle:
        return {"confirmed": False, "reason": "no_sync_time"}

    b_idx = bitget_candles.index(b_candle)
    bn_idx = binance_candles.index(bn_candle)

    b = candle_metrics_for(b_candle, bitget_candles[:b_idx])
    bn = candle_metrics_for(bn_candle, binance_candles[:bn_idx])
    if not b or not bn:
        return {"confirmed": False, "reason": "no_metrics"}

    avg_move = (b["move_pct"] + bn["move_pct"]) / 2

    same_up = avg_move >= MIN_5M_MOVE_PCT and b["move_pct"] > 0.15 and bn["move_pct"] > 0.15
    same_down = avg_move <= -MIN_5M_MOVE_PCT and b["move_pct"] < -0.15 and bn["move_pct"] < -0.15

    if not same_up and not same_down:
        STATS["rejected_no_sync"] += 1
        return {"confirmed": False, "reason": "no_sync", "bitget": b, "binance": bn}

    direction = "LONG" if same_up else "SHORT"

    if direction == "SHORT" and has_recent_dump(bitget_candles):
        STATS["rejected_recent_pump"] += 1
        return {"confirmed": False, "reason": "recent_dump", "bitget": b, "binance": bn}

    agg_rvol = (b["rvol"] + bn["rvol"]) / 2
    price_move = (abs(b["move_pct"]) + abs(bn["move_pct"])) / 2

    volume_ok = (b["rvol"] >= MIN_RVOL or bn["rvol"] >= MIN_RVOL) and agg_rvol >= MIN_AGG_RVOL
    if not volume_ok:
        STATS["rejected_volume"] += 1
        return {"confirmed": False, "reason": "volume", "bitget": b, "binance": bn}

    if direction == "LONG":
        structure_ok = (
            b["close_position"] >= 0.55 and bn["close_position"] >= 0.55
            and b["body_ratio"] >= 0.40 and bn["body_ratio"] >= 0.40
        )
    else:
        structure_ok = (
            b["close_position"] <= 0.45 and bn["close_position"] <= 0.45
            and b["body_ratio"] >= 0.40 and bn["body_ratio"] >= 0.40
        )

    if not structure_ok:
        STATS["rejected_structure"] += 1
        return {"confirmed": False, "reason": "structure", "bitget": b, "binance": bn}

    too_late = (
        abs(b["move_pct"]) > MAX_5M_MOVE_FOR_EARLY
        or abs(bn["move_pct"]) > MAX_5M_MOVE_FOR_EARLY
    )
    if too_late:
        STATS["rejected_too_late"] += 1
        return {"confirmed": False, "reason": "too_late", "bitget": b, "binance": bn}

    distribution = False
    if direction == "LONG":
        if (
            b["rvol"] >= STRONG_RVOL and bn["rvol"] >= STRONG_RVOL
            and (b["upper_wick_ratio"] >= 0.35 or bn["upper_wick_ratio"] >= 0.35)
            and (b["close_position"] < 0.72 or bn["close_position"] < 0.72)
        ):
            distribution = True

    if distribution:
        STATS["rejected_distribution"] += 1
        return {
            "confirmed": False,
            "distribution": True,
            "direction": direction,
            "bitget": b,
            "binance": bn,
            "aggregate_rvol": agg_rvol,
            "average_move": price_move,
        }

    strong = agg_rvol >= STRONG_AGG_RVOL and price_move >= STRONG_5M_MOVE_PCT

    return {
        "confirmed": True,
        "strong": strong,
        "distribution": False,
        "direction": direction,
        "bitget": b,
        "binance": bn,
        "aggregate_rvol": agg_rvol,
        "average_move": price_move,
    }


# ============================================================
# OI (Bitget + Binance)
# ============================================================

async def update_oi(base):
    item = COMMON_SYMBOLS.get(base)
    if not item:
        return None

    now = time.time()

    bitget_oi, binance_oi = await asyncio.gather(
        fetch_bitget_oi(item["bitget"]["symbol"]),
        fetch_binance_oi_direct(base),
    )

    if bitget_oi <= 0 and binance_oi <= 0:
        return None

    hist = OI_HISTORY[base]
    if bitget_oi > 0:
        hist["bitget"].append((now, bitget_oi))
    if binance_oi > 0:
        hist["binance"].append((now, binance_oi))

    def calc_delta(history):
        if len(history) < 2:
            return 0.0
        old = history[0][1]
        new = history[-1][1]
        if old <= 0:
            return 0.0
        return ((new / old) - 1) * 100

    bitget_delta = calc_delta(hist["bitget"])
    binance_delta = calc_delta(hist["binance"])

    valid = []
    if bitget_oi > 0:
        valid.append(bitget_delta)
    if binance_oi > 0:
        valid.append(binance_delta)
    if not valid:
        return None

    avg_delta = sum(valid) / len(valid)

    return {
        "bitget_oi": bitget_oi,
        "binance_oi": binance_oi,
        "bitget_delta": bitget_delta,
        "binance_delta": binance_delta,
        "avg_delta": avg_delta,
        "sources": len(valid),
        "positive_count": sum(1 for x in valid if x >= MIN_OI_GROWTH_PCT),
        "negative_count": sum(1 for x in valid if x <= -MIN_OI_GROWTH_PCT),
    }


def cleanup_oi_history():
    active = set(COMMON_SYMBOLS.keys())
    for b in list(OI_HISTORY.keys()):
        if b not in active:
            del OI_HISTORY[b]


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text, retries=2):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("BOT_TOKEN / CHAT_ID not configured")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for attempt in range(retries):
        try:
            async with SESSION.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    return True
                log.warning("Telegram HTTP %s (attempt %d)", resp.status, attempt + 1)
        except Exception as e:
            log.warning("Telegram error: %s (attempt %d)", e, attempt + 1)

        if attempt < retries - 1:
            await asyncio.sleep(2)
    return False


def signal_allowed(base):
    return time.time() - LAST_SIGNAL.get(base, 0) >= SIGNAL_COOLDOWN_SEC


def flow_allowed(base):
    return time.time() - LAST_FLOW.get(base, 0) >= FLOW_COOLDOWN_SEC


# ============================================================
# MESSAGES
# ============================================================

async def send_ignition_signal(base, ig):
    dir_emoji = "🟢" if ig['direction'] == "LONG" else "🔴"
    message = (
        f"🔥 <b>ИМПУЛЬС ИНИЦИАЦИИ (IGNITION)</b>\n\n"
        f"Монета: <code>{base}USDT</code> — {dir_emoji} <b>{ig['direction']}</b>\n"
        f"Изм. цены (текущая 5m): Bitget {ig['bitget_move']:+.2f}% / Binance {ig['binance_move']:+.2f}%\n"
        f"Текущий RVOL: Bitget {ig['bitget_forming_rvol']:.2f}x / Binance {ig['binance_forming_rvol']:.2f}x\n\n"
        "⚠️ Зафиксировано зажигание импульса. Ожидается подтверждение по OI."
    )
    await send_telegram(message)


async def send_flow_signal(base, flow, oi, binance_price=None):
    direction = flow["direction"]
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    b = flow["bitget"]
    bn = flow["binance"]

    message = (
        f"🚨 <b>РАННИЙ ПОТОК (CEX EARLY FLOW)</b>\n\n"
        f"Монета: <code>{base}USDT</code> — {dir_emoji} <b>{direction}</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        "📊 <b>Bitget</b>\n"
        f"Изменение 5m: {b['move_pct']:+.2f}%\n"
        f"RVOL: {b['rvol']:.2f}x\n"
        f"Объём 5m ($): ${b['volume_usd']:,.0f}\n"
        f"Сила закрытия: {b['close_position']:.2f}\n\n"
        "📊 <b>Binance</b>\n"
        f"Изменение 5m: {bn['move_pct']:+.2f}%\n"
        f"RVOL: {bn['rvol']:.2f}x\n"
        f"Объём 5m ($): ${bn['volume_usd']:,.0f}\n"
        f"Сила закрытия: {bn['close_position']:.2f}\n\n"
        "━━━━━━━━━━━━━━\n"
        "🌐 <b>АГРЕГИРОВАННЫЙ ПОТОК ФЬЮЧЕРСОВ</b>\n"
        f"Суммарный объём 5m ($): ${(b['volume_usd'] + bn['volume_usd']):,.0f}\n"
        f"Средний RVOL: {flow['aggregate_rvol']:.2f}x\n"
        f"Средний ход: {flow['average_move']:+.2f}%\n\n"
        "📈 <b>ОТКРЫТЫЙ ИНТЕРЕС (OI)</b>\n"
        f"Bitget ΔOI: {oi['bitget_delta']:+.2f}%\n"
        f"Binance ΔOI: {oi['binance_delta']:+.2f}%\n"
        f"Средний ΔOI: {oi['avg_delta']:+.2f}% ({oi['sources']} ист.)\n\n"
    )

    if binance_price:
        message += f"💵 Цена Binance: {binance_price:.6f}\n\n"

    message += "⚠️ Сигнал подтверждает наличие аномального потока. Не входить без сетапа на ретесте!"
    await send_telegram(message)


async def send_distribution(base, flow):
    b = flow["bitget"]
    bn = flow["binance"]
    message = (
        "⚠️ <b>ДИСТРИБУЦИЯ / РАЗДАЧА (CEX DISTRIBUTION)</b>\n\n"
        f"Монета: <code>{base}USDT</code>\n\n"
        "Обнаружены признаки фиксации:\n"
        "• Экстремальный объем на фьючерсах\n"
        "• Слабое продолжение движения\n"
        "• Длинная верхняя тень / плохое закрытие свечи\n\n"
        f"Bitget RVOL: {b['rvol']:.2f}x (закрытие: {b['close_position']:.2f})\n"
        f"Binance RVOL: {bn['rvol']:.2f}x (закрытие: {bn['close_position']:.2f})\n\n"
        "🛑 ЛОНГ ПОТОК ВРЕМЕННО БЛОКИРОВАН.\n"
        "Высокая вероятность разворота вниз (SHORT setup)."
    )
    await send_telegram(message)


# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base):
    candles = await get_candles(base)
    if not candles:
        return

    STATS["candle_checks"] += 1

    try:
        ignition = analyze_ignition(base, candles)
    except Exception as e:
        log.exception("Ignition %s error: %s", base, e)
        ignition = None

    if ignition and signal_allowed(base):
        LAST_SIGNAL[base] = time.time()
        STATS["ignition_signals"] += 1
        await send_ignition_signal(base, ignition)
        return

    flow = analyze_flow(base, candles)
    if not flow:
        return

    if flow.get("distribution"):
        if flow_allowed(base):
            LAST_FLOW[base] = time.time()
            STATS["distribution_signals"] += 1
            await send_distribution(base, flow)
        return

    if not flow.get("confirmed"):
        return

    oi = await update_oi(base)
    STATS["oi_checks"] += 1
    if not oi:
        return

    STATS["oi_candidates"] += 1

    # OI фильтр: средняя дельта по всем источникам
    if flow["direction"] == "LONG":
        if oi["avg_delta"] < MIN_OI_GROWTH_PCT:
            STATS["rejected_oi"] += 1
            return
    else:
        if oi["avg_delta"] < MIN_OI_GROWTH_PCT:
            STATS["rejected_oi"] += 1
            return

    STATS["flow_confirmed"] += 1

    binance_price = None
    item = COMMON_SYMBOLS.get(base)
    if item and item.get("binance"):
        binance_price = item["binance"]["price"]

    if not flow_allowed(base) or not signal_allowed(base):
        return

    LAST_FLOW[base] = time.time()
    LAST_SIGNAL[base] = time.time()
    STATS["early_signals"] += 1

    await send_flow_signal(base, flow, oi, binance_price)


# ============================================================
# UNIVERSE REFRESH
# ============================================================

async def refresh_universe():
    global BITGET_DATA, BINANCE_DATA, BINANCE_FUTURES_SYMBOLS, COMMON_SYMBOLS

    bitget_task = fetch_bitget_tickers()
    binance_task = fetch_binance_tickers()
    futures_task = fetch_binance_futures_symbols()

    bitget, binance, futures = await asyncio.gather(
        bitget_task, binance_task, futures_task
    )

    if not bitget or not binance:
        log.warning(
            "Universe refresh failed: Bitget=%d Binance=%d Futures=%d",
            len(bitget), len(binance), len(futures)
        )
        return

    BITGET_DATA = bitget
    BINANCE_DATA = binance
    BINANCE_FUTURES_SYMBOLS = futures

    in_range = 0
    for base in set(bitget.keys()) & set(binance.keys()) & futures:
        pb = num(bitget[base].get("price"))
        pbn = num(binance[base].get("price"))
        if pb > 0 and pbn > 0:
            avg = (pb + pbn) / 2
            if MIN_PRICE_USDT <= avg <= MAX_PRICE_USDT:
                in_range += 1

    COMMON_SYMBOLS = build_common_universe(bitget, binance)

    for b in [x for x in CANDLE_CACHE if x not in COMMON_SYMBOLS]:
        del CANDLE_CACHE[b]

    cleanup_oi_history()

    STATS["universe_refresh"] += 1
    STATS["common_symbols"] = len(COMMON_SYMBOLS)
    STATS["price_in_range"] = in_range

    log.info(
        "UNIVERSE | Bitget=%d | Binance=%d | BN Futures=%d | "
        "In price range [%.4f-%.4f]: %d | Common: %d",
        len(bitget), len(binance), len(futures),
        MIN_PRICE_USDT, MAX_PRICE_USDT,
        in_range, len(COMMON_SYMBOLS)
    )


def select_candle_candidates():
    selected = list(COMMON_SYMBOLS.keys())[:MAX_CANDLE_CANDIDATES]
    STATS["candle_candidates"] = len(selected)
    return selected


async def scan_cycle():
    if not COMMON_SYMBOLS:
        return

    candidates = select_candle_candidates()
    semaphore = asyncio.Semaphore(3)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.15)
            except Exception as e:
                log.exception("Symbol %s error: %s", base, e)

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

    bitget, binance = await asyncio.gather(
        fetch_bitget_tickers(),
        fetch_binance_tickers(),
    )

    def vol_sum(data):
        return sum(num(v.get("volume24")) for v in data.values())

    sample_base = None
    sample_bitget_symbol = None
    sample_binance_symbol = None

    if COMMON_SYMBOLS:
        sample_base = max(
            COMMON_SYMBOLS.items(),
            key=lambda x: x[1]["aggregate_volume"]
        )[0]
        item = COMMON_SYMBOLS[sample_base]
        sample_bitget_symbol = item["bitget"]["symbol"]
        sample_binance_symbol = item["binance"]["symbol"]

    bg_candles, bn_candles, bg_oi = [], [], 0.0
    if sample_bitget_symbol and sample_binance_symbol:
        bg_candles, bn_candles, bg_oi = await asyncio.gather(
            fetch_bitget_candles(sample_bitget_symbol),
            fetch_binance_candles(sample_binance_symbol),
            fetch_bitget_oi(sample_bitget_symbol),
        )

    def rvol_of(candles):
        m = candle_metrics(candles)
        return m["rvol"] if m else 0.0

    bg_rvol = rvol_of(bg_candles)
    bn_rvol = rvol_of(bn_candles)

    log.info(
        "HOURLY | BG=%d BN=%d common=%d sample=%s "
        "bg_rvol=%.2f bn_rvol=%.2f bg_oi=%.0f | "
        "OI: api=%d ticker=%d miss=%d | BN OI: ok=%d fail=%d",
        len(bitget), len(binance),
        len(COMMON_SYMBOLS), sample_base or "-",
        bg_rvol, bn_rvol, bg_oi,
        STATS["bg_oi_from_api"],
        STATS["bg_oi_from_ticker"],
        STATS["bg_oi_missing"],
        STATS["bn_oi_ok"], STATS["bn_oi_fail"],
    )

    msg = (
        "🩺 <b>ЧАСОВАЯ ДИАГНОСТИКА СИСТЕМЫ</b>\n\n"
        f"<b>Тикеры с бирж</b>\n"
        f"Bitget: {len(bitget)} | 24h vol≈${vol_sum(bitget)/1e6:.1f}M\n"
        f"Binance Futures: {len(binance)} | 24h vol≈${vol_sum(binance)/1e6:.1f}M\n"
        f"Binance Futures symbols: {len(BINANCE_FUTURES_SYMBOLS)}\n\n"
        f"<b>Общие монеты (Common)</b>: {len(COMMON_SYMBOLS)}\n"
        f"<b>Тестовый sample</b>: <code>{sample_base or 'n/a'}USDT</code>\n"
        f"Bitget свечи: {len(bg_candles)} | RVOL≈{bg_rvol:.2f}x | OI={bg_oi:,.0f}\n"
        f"Binance свечи: {len(bn_candles)} | RVOL≈{bn_rvol:.2f}x\n\n"
        f"<b>OI Bitget источники</b>\n"
        f"API: {STATS['bg_oi_from_api']} | ticker: {STATS['bg_oi_from_ticker']} | нет: {STATS['bg_oi_missing']}\n\n"
        f"<b>Binance OI (direct)</b>\n"
        f"Успешно: {STATS['bn_oi_ok']} | Отказов: {STATS['bn_oi_fail']}\n\n"
        f"<b>Статистика HTTP запросов</b>\n"
        f"BG {STATS['bitget_requests']}/{STATS['bitget_errors']} ош | "
        f"BN {STATS['binance_requests']}/{STATS['binance_errors']} ош\n\n"
        f"Сигналы: ign={STATS['ignition_signals']} "
        f"early={STATS['early_signals']} dist={STATS['distribution_signals']}"
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
            log.exception("Scanner loop error: %s", e)
            await asyncio.sleep(15)


# ============================================================
# STATS PAGE
# ============================================================

def format_uptime():
    seconds = int(time.time() - START_TIME)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def stats_text():
    return f"""
CEX FUTURES AGGREGATOR v3.0
===========================

Uptime: {format_uptime()}

PRICE FILTER
------------
Range: {MIN_PRICE_USDT} — {MAX_PRICE_USDT} USDT
Coins in range: {STATS.get("price_in_range", 0)}

MARKET
------
Common symbols: {STATS["common_symbols"]}
Candle candidates: {STATS["candle_candidates"]}
OI candidates: {STATS["oi_candidates"]}
Binance Futures symbols: {len(BINANCE_FUTURES_SYMBOLS)}

OI BITGET SOURCES
-----------------
From API:    {STATS["bg_oi_from_api"]}
From ticker: {STATS["bg_oi_from_ticker"]}
Missing:     {STATS["bg_oi_missing"]}

BINANCE OI (DIRECT)
-------------------
OK:    {STATS["bn_oi_ok"]}
Fail:  {STATS["bn_oi_fail"]}
Binance confirmations: {STATS["binance_confirmed"]}

FLOW
----
Ignition signals: {STATS["ignition_signals"]}
Confirmed CEX flow: {STATS["flow_confirmed"]}
Early signals: {STATS["early_signals"]}
Distribution: {STATS["distribution_signals"]}

REJECTED
--------
No sync: {STATS["rejected_no_sync"]}
Volume: {STATS["rejected_volume"]}
Structure: {STATS["rejected_structure"]}
Too late: {STATS["rejected_too_late"]}
Distribution: {STATS["rejected_distribution"]}
Recent dump (SHORT): {STATS["rejected_recent_pump"]}
OI: {STATS["rejected_oi"]}

HTTP REQUESTS
-------------
Bitget: {STATS["bitget_requests"]}
Binance: {STATS["binance_requests"]}

ERRORS
------
Bitget: {STATS["bitget_errors"]}
Binance: {STATS["binance_errors"]}
"""


# ============================================================
# WEB
# ============================================================

async def index(request):
    return web.Response(text=stats_text(), content_type="text/plain")


async def health(request):
    return web.Response(text="CEX FUTURES AGGREGATOR ACTIVE", content_type="text/plain")


# ============================================================
# STARTUP
# ============================================================

async def start_background(app):
    global SESSION

    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    )
    scanner_task = asyncio.create_task(scanner_loop())
    app["scanner_task"] = scanner_task

    # --- ТЕСТ BINANCE FUTURES ---
    bn_status = "⚠️ не проверено"

    log.info("=" * 60)
    log.info("BINANCE FUTURES TEST START")
    log.info("-" * 60)

    try:
        # OI
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/openInterest"
        params = {"symbol": "BTCUSDT"}

        async with SESSION.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            log.info("OI HTTP status: %d", resp.status)
            if resp.status == 200:
                data = await resp.json()
                oi = num(data.get("openInterest"))
                bn_status = f"✅ OI OK (BTC: {oi:,.0f})" if oi > 0 else "⚠️ OI=0"
            else:
                text = await resp.text()
                log.warning("OI Response: %s", text[:300])
                bn_status = f"❌ OI HTTP {resp.status}"

        # Candles
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/klines"
        params = {"symbol": "BTCUSDT", "interval": "5m", "limit": 3}

        async with SESSION.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            log.info("Klines HTTP status: %d", resp.status)
            if resp.status == 200:
                data = await resp.json()
                log.info("Klines rows: %d", len(data) if isinstance(data, list) else 0)
            else:
                text = await resp.text()
                log.warning("Klines Response: %s", text[:300])

    except Exception as e:
        bn_status = f"❌ {e}"
        log.error("Binance test error: %s", e)

    log.info("BINANCE RESULT: %s", bn_status)
    log.info("=" * 60)

    startup_msg = (
        "🚀 <b>CEX FUTURES AGGREGATOR v3.0 ЗАПУЩЕН</b>\n\n"
        "• Биржи: <b>Bitget + Binance Futures</b>\n"
        "• KuCoin: <b>удалён</b>\n"
        f"• Binance: {bn_status}\n"
        f"• Ценовой фильтр: {MIN_PRICE_USDT}–{MAX_PRICE_USDT} USDT\n"
        "• OI: Bitget + Binance (2 источника)\n"
        "• Статус: поиск аномалий запущен."
    )

    tg_task = asyncio.create_task(send_telegram(startup_msg))
    BACKGROUND_TASKS.add(tg_task)
    tg_task.add_done_callback(BACKGROUND_TASKS.discard)

    log.info("CEX FUTURES AGGREGATOR v3.0 STARTED")


async def cleanup(app):
    task = app.get("scanner_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    for t in BACKGROUND_TASKS:
        t.cancel()

    global SESSION
    if SESSION:
        await SESSION.close()
        SESSION = None


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/stats", index)
app.router.add_get("/health", health)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
