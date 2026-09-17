import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque


# ============================================================
#       CEX FUTURES AGGREGATOR v2.1 (FIXED OI + FALLBACK)
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 120
CANDLE_REFRESH_SEC = 60
OI_REFRESH_SEC = 60

MAX_COMMON_SYMBOLS = 350
MAX_CANDLE_CANDIDATES = 350

MAX_COINGLASS_PER_HOUR = 20

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

# IGNITION
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
KUCOIN_BASE = "https://api-futures.kucoin.com"
BINANCE_VISION_BASE = "https://data-api.binance.vision"
COINGLASS_BASE = "https://open-api-v4.coinglass.com"


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

COMMON_SYMBOLS = {}

BITGET_DATA = {}
KUCOIN_DATA = {}
BINANCE_DATA = {}

CANDLE_CACHE = {}

OI_HISTORY = defaultdict(lambda: {
    "bitget": deque(maxlen=12),
    "kucoin": deque(maxlen=12),
})

LAST_SIGNAL = {}
LAST_FLOW = {}
LAST_HOURLY_LOG = 0

COINGLASS_REQUESTS = deque()

STATS = {
    "bitget_requests": 0,
    "kucoin_requests": 0,
    "binance_requests": 0,
    "coinglass_requests": 0,

    "bitget_errors": 0,
    "kucoin_errors": 0,
    "binance_errors": 0,
    "coinglass_errors": 0,

    "universe_refresh": 0,
    "candle_checks": 0,
    "oi_checks": 0,

    "common_symbols": 0,
    "candle_candidates": 0,
    "oi_candidates": 0,

    "price_in_range": 0,

    "ignition_signals": 0,
    "flow_confirmed": 0,
    "coinglass_confirmed": 0,
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

    # NEW: счётчики OI fallback
    "bg_oi_from_api": 0,
    "bg_oi_from_ticker": 0,
    "bg_oi_missing": 0,
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
            # NEW: сохраняем сырой ответ тикера для фоллбэка OI
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
    # 1. Пробуем прямой запрос OI
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
        )
        if val > 0:
            STATS["bg_oi_from_api"] += 1
            return val

    # 2. ФОЛЛБЭК: если 0, берём OI из сохранённых данных тикера
    base = normalize_base(symbol)
    if base in BITGET_DATA:
        raw_t = BITGET_DATA[base].get("raw", {}) or {}
        fallback_val = num(
            raw_t.get("openInterest")
            or raw_t.get("holdAcm")
            or raw_t.get("openInterestSize")
            or raw_t.get("openInterestUsd")
            or raw_t.get("oi")
        )
        if fallback_val > 0:
            STATS["bg_oi_from_ticker"] += 1
            return fallback_val

    STATS["bg_oi_missing"] += 1
    return 0.0


# ============================================================
# KUCOIN
# ============================================================

async def fetch_kucoin_contracts():
    url = f"{KUCOIN_BASE}/api/v1/contracts/active"
    data = await http_get(url, exchange="kucoin")

    result = {}
    if not data:
        return result

    rows = data.get("data", [])
    if not isinstance(rows, list):
        return result

    for row in rows:
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue

        symbol = str(row.get("symbol", "")).upper()
        base = normalize_base(row.get("baseCurrency") or symbol)
        if not base:
            continue

        price = num(row.get("lastTradePrice") or row.get("markPrice"))
        turnover = num(row.get("turnoverOf24h"))
        oi = num(row.get("openInterest"))
        change = num(row.get("priceChgPct")) * 100

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": turnover,
            "change24": change,
            "oi": oi,
        }

    return result


async def fetch_kucoin_candles(symbol):
    url = f"{KUCOIN_BASE}/api/v1/kline/query"
    params = {"symbol": symbol, "granularity": "5"}

    data = await http_get(url, params=params, exchange="kucoin")
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
                "ts": ts * 1000,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            })
        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])
    return candles[-24:]


# ============================================================
# BINANCE (vision)
# ============================================================

async def fetch_binance_tickers():
    url = f"{BINANCE_VISION_BASE}/api/v3/ticker/24hr"
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

        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastPrice")),
            "volume24": num(row.get("quoteVolume")),
            "change24": num(row.get("priceChangePercent")),
        }

    return result


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

    move_pct = ((close / open_price) - 1) * 100
    candle_range = high - low
    if candle_range <= 0:
        return None

    body = abs(close - open_price)
    body_ratio = body / candle_range
    close_position = (close - low) / candle_range

    volumes = [x["volume"] for x in previous_candles[-12:] if x["volume"] > 0]
    if not volumes:
        return None

    avg_volume = sum(volumes) / len(volumes)
    if avg_volume <= 0:
        return None

    rvol = candle["volume"] / avg_volume

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
# UNIVERSE
# ============================================================

def build_common_universe(bitget, kucoin, binance):
    common_bases = set(bitget.keys()) & set(kucoin.keys())
    common = {}

    for base in common_bases:
        b = bitget[base]
        k = kucoin[base]

        volume_b = b["volume24"]
        volume_k = k["volume24"]

        if volume_b < MIN_24H_VOLUME_USDT or volume_k < MIN_24H_VOLUME_USDT:
            continue

        price_b = num(b.get("price"))
        price_k = num(k.get("price"))
        if price_b <= 0 or price_k <= 0:
            continue

        avg_price = (price_b + price_k) / 2
        if avg_price < MIN_PRICE_USDT or avg_price > MAX_PRICE_USDT:
            continue

        common[base] = {
            "bitget": b,
            "kucoin": k,
            "aggregate_volume": volume_b + volume_k,
            "binance": binance.get(base),
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

    bitget_candles, kucoin_candles = await asyncio.gather(
        fetch_bitget_candles(item["bitget"]["symbol"]),
        fetch_kucoin_candles(item["kucoin"]["symbol"]),
    )

    result = {"bitget": bitget_candles, "kucoin": kucoin_candles}
    CANDLE_CACHE[base] = {"time": now, "data": result}
    return result


# ============================================================
# RECENT DUMP (SHORT only)
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
    kc = candles.get("kucoin", [])
    if len(bg) < 8 or len(kc) < 8:
        return None

    fb, fk = bg[-1], kc[-1]
    if abs(fb["ts"] - fk["ts"]) > 120_000:
        return None

    def forming_metrics(c, closed):
        o = c["open"]
        if o <= 0:
            return None
        move = ((c["close"] / o) - 1) * 100
        vols = [x["volume"] for x in closed[-12:] if x["volume"] > 0]
        if not vols:
            return None
        avg_vol = sum(vols) / len(vols)
        if avg_vol <= 0:
            return None
        return move, c["volume"] / avg_vol

    mb = forming_metrics(fb, bg[:-1])
    mk = forming_metrics(fk, kc[:-1])
    if not mb or not mk:
        return None

    avg_move = (mb[0] + mk[0]) / 2
    up = avg_move >= IGNITION_MIN_MOVE_PCT and mb[0] > 0.1 and mk[0] > 0.1
    down = avg_move <= -IGNITION_MIN_MOVE_PCT and mb[0] < -0.1 and mk[0] < -0.1
    if not (up or down):
        return None

    if abs(avg_move) > IGNITION_MAX_MOVE_PCT:
        return None

    if not (mb[1] >= IGNITION_MIN_FORMING_RVOL or mk[1] >= IGNITION_MIN_FORMING_RVOL):
        return None

    return {
        "type": "IGNITION",
        "direction": "LONG" if up else "SHORT",
        "bitget_move": mb[0],
        "kucoin_move": mk[0],
        "bitget_forming_rvol": mb[1],
        "kucoin_forming_rvol": mk[1],
    }


# ============================================================
# FLOW
# ============================================================

def analyze_flow(base, candles):
    if not candles:
        return None

    bitget_candles = candles.get("bitget", [])
    kucoin_candles = candles.get("kucoin", [])

    b_candle, k_candle = sync_candles_by_time(bitget_candles, kucoin_candles)
    if not b_candle or not k_candle:
        return {"confirmed": False, "reason": "no_sync_time"}

    b_idx = bitget_candles.index(b_candle)
    k_idx = kucoin_candles.index(k_candle)

    b = candle_metrics_for(b_candle, bitget_candles[:b_idx])
    k = candle_metrics_for(k_candle, kucoin_candles[:k_idx])
    if not b or not k:
        return {"confirmed": False, "reason": "no_metrics"}

    avg_move = (b["move_pct"] + k["move_pct"]) / 2

    same_up = avg_move >= MIN_5M_MOVE_PCT and b["move_pct"] > 0.15 and k["move_pct"] > 0.15
    same_down = avg_move <= -MIN_5M_MOVE_PCT and b["move_pct"] < -0.15 and k["move_pct"] < -0.15

    if not same_up and not same_down:
        STATS["rejected_no_sync"] += 1
        return {"confirmed": False, "reason": "no_sync", "bitget": b, "kucoin": k}

    direction = "LONG" if same_up else "SHORT"

    if direction == "SHORT" and has_recent_dump(bitget_candles):
        STATS["rejected_recent_pump"] += 1
        return {"confirmed": False, "reason": "recent_dump", "bitget": b, "kucoin": k}

    agg_rvol = (b["rvol"] + k["rvol"]) / 2
    price_move = (abs(b["move_pct"]) + abs(k["move_pct"])) / 2

    volume_ok = (b["rvol"] >= MIN_RVOL or k["rvol"] >= MIN_RVOL) and agg_rvol >= MIN_AGG_RVOL
    if not volume_ok:
        STATS["rejected_volume"] += 1
        return {"confirmed": False, "reason": "volume", "bitget": b, "kucoin": k}

    if direction == "LONG":
        structure_ok = (
            b["close_position"] >= 0.55 and k["close_position"] >= 0.55
            and b["body_ratio"] >= 0.40 and k["body_ratio"] >= 0.40
        )
    else:
        structure_ok = (
            b["close_position"] <= 0.45 and k["close_position"] <= 0.45
            and b["body_ratio"] >= 0.40 and k["body_ratio"] >= 0.40
        )

    if not structure_ok:
        STATS["rejected_structure"] += 1
        return {"confirmed": False, "reason": "structure", "bitget": b, "kucoin": k}

    too_late = (
        abs(b["move_pct"]) > MAX_5M_MOVE_FOR_EARLY
        or abs(k["move_pct"]) > MAX_5M_MOVE_FOR_EARLY
    )
    if too_late:
        STATS["rejected_too_late"] += 1
        return {"confirmed": False, "reason": "too_late", "bitget": b, "kucoin": k}

    distribution = False
    if direction == "LONG":
        if (
            b["rvol"] >= STRONG_RVOL and k["rvol"] >= STRONG_RVOL
            and (b["upper_wick_ratio"] >= 0.35 or k["upper_wick_ratio"] >= 0.35)
            and (b["close_position"] < 0.72 or k["close_position"] < 0.72)
        ):
            distribution = True

    if distribution:
        STATS["rejected_distribution"] += 1
        return {
            "confirmed": False,
            "distribution": True,
            "direction": direction,
            "bitget": b,
            "kucoin": k,
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
        "kucoin": k,
        "aggregate_rvol": agg_rvol,
        "average_move": price_move,
    }


# ============================================================
# OI
# ============================================================

async def update_oi(base):
    item = COMMON_SYMBOLS.get(base)
    if not item:
        return None

    now = time.time()
    kucoin_oi = num(item["kucoin"].get("oi"))
    bitget_oi = await fetch_bitget_oi(item["bitget"]["symbol"])

    if kucoin_oi <= 0 and bitget_oi <= 0:
        return None

    hist = OI_HISTORY[base]
    if bitget_oi > 0:
        hist["bitget"].append((now, bitget_oi))
    if kucoin_oi > 0:
        hist["kucoin"].append((now, kucoin_oi))

    def calc_delta(history):
        if len(history) < 2:
            return 0.0
        old = history[0][1]
        new = history[-1][1]
        if old <= 0:
            return 0.0
        return ((new / old) - 1) * 100

    bitget_delta = calc_delta(hist["bitget"])
    kucoin_delta = calc_delta(hist["kucoin"])

    valid = []
    if bitget_oi > 0:
        valid.append(bitget_delta)
    if kucoin_oi > 0:
        valid.append(kucoin_delta)
    if not valid:
        return None

    avg_delta = sum(valid) / len(valid)

    return {
        "bitget_oi": bitget_oi,
        "kucoin_oi": kucoin_oi,
        "bitget_delta": bitget_delta,
        "kucoin_delta": kucoin_delta,
        "avg_delta": avg_delta,
        "positive_count": sum(1 for x in valid if x >= MIN_OI_GROWTH_PCT),
        "negative_count": sum(1 for x in valid if x <= -MIN_OI_GROWTH_PCT),
    }


def cleanup_oi_history():
    active = set(COMMON_SYMBOLS.keys())
    for b in list(OI_HISTORY.keys()):
        if b not in active:
            del OI_HISTORY[b]


# ============================================================
# COINGLASS
# ============================================================

def coinglass_allowed():
    now = time.time()
    while COINGLASS_REQUESTS and COINGLASS_REQUESTS[0] < now - 3600:
        COINGLASS_REQUESTS.popleft()
    return len(COINGLASS_REQUESTS) < MAX_COINGLASS_PER_HOUR


async def fetch_binance_coinglass(base):
    if not COINGLASS_API_KEY:
        return None
    if not coinglass_allowed():
        log.warning("CoinGlass hourly limit reached")
        return None

    url = f"{COINGLASS_BASE}/api/futures/open-interest/exchange-list"
    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    params = {"symbol": base}

    COINGLASS_REQUESTS.append(time.time())
    data = await http_get(url, params=params, headers=headers, exchange="coinglass")
    if not data:
        return None

    rows = data.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return None

    for row in rows:
        exchange = str(row.get("exchange") or row.get("exchangeName") or "").lower()
        if exchange != "binance":
            continue
        oi = num(row.get("openInterest") or row.get("oi") or row.get("open_interest"))
        if oi > 0:
            return {"exchange": "Binance", "oi": oi}
    return None


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
# MESSAGES (РУСИФИЦИРОВАНЫ)
# ============================================================

async def send_ignition_signal(base, ig):
    dir_emoji = "🟢" if ig['direction'] == "LONG" else "🔴"
    message = (
        f"🔥 <b>ИМПУЛЬС ИНИЦИАЦИИ (IGNITION)</b>\n\n"
        f"Монета: <code>{base}USDT</code> — {dir_emoji} <b>{ig['direction']}</b>\n"
        f"Изм. цены (текущая 5m): Bitget {ig['bitget_move']:+.2f}% / KuCoin {ig['kucoin_move']:+.2f}%\n"
        f"Текущий RVOL: Bitget {ig['bitget_forming_rvol']:.2f}x / KuCoin {ig['kucoin_forming_rvol']:.2f}x\n\n"
        "⚠️ Зафиксировано зажигание импульса. Ожидается подтверждение по OI."
    )
    await send_telegram(message)


async def send_flow_signal(base, flow, oi, binance_oi, binance_price=None):
    direction = flow["direction"]
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    b = flow["bitget"]
    k = flow["kucoin"]

    message = (
        f"🚨 <b>РАННИЙ ПОТОК (CEX EARLY FLOW)</b>\n\n"
        f"Монета: <code>{base}USDT</code> — {dir_emoji} <b>{direction}</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        "📊 <b>Bitget</b>\n"
        f"Изменение 5m: {b['move_pct']:+.2f}%\n"
        f"RVOL: {b['rvol']:.2f}x\n"
        f"Сила закрытия: {b['close_position']:.2f}\n\n"
        "📊 <b>KuCoin</b>\n"
        f"Изменение 5m: {k['move_pct']:+.2f}%\n"
        f"RVOL: {k['rvol']:.2f}x\n"
        f"Сила закрытия: {k['close_position']:.2f}\n\n"
        "━━━━━━━━━━━━━━\n"
        "🌐 <b>АГРЕГИРОВАННЫЙ ПОТОК ФЬЮЧЕРСОВ</b>\n"
        f"Средний RVOL: {flow['aggregate_rvol']:.2f}x\n"
        f"Средний ход: {flow['average_move']:+.2f}%\n\n"
        "📈 <b>ОТКРЫТЫЙ ИНТЕРЕС (OI)</b>\n"
        f"Bitget ΔOI: {oi['bitget_delta']:+.2f}%\n"
        f"KuCoin ΔOI: {oi['kucoin_delta']:+.2f}%\n"
        f"Средний ΔOI: {oi['avg_delta']:+.2f}%\n\n"
    )

    if binance_oi:
        message += f"🟡 <b>CoinGlass → Binance</b>\nOI: {binance_oi['oi']:,.0f}\n\n"
    if binance_price:
        message += f"💵 Цена Binance: {binance_price:.6f}\n\n"

    message += "⚠️ Сигнал подтверждает наличие аномального потока. Не входить без сетапа на ретесте!"
    await send_telegram(message)


async def send_distribution(base, flow):
    b = flow["bitget"]
    k = flow["kucoin"]
    message = (
        "⚠️ <b>ДИСТРИБУЦИЯ / РАЗДАЧА (CEX DISTRIBUTION)</b>\n\n"
        f"Монета: <code>{base}USDT</code>\n\n"
        "Обнаружены признаки фиксации:\n"
        "• Экстремальный объем на фьючерсах\n"
        "• Слабое продолжение движения\n"
        "• Длинная верхняя тень / плохое закрытие свечи\n\n"
        f"Bitget RVOL: {b['rvol']:.2f}x (закрытие: {b['close_position']:.2f})\n"
        f"KuCoin RVOL: {k['rvol']:.2f}x (закрытие: {k['close_position']:.2f})\n\n"
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

    if flow["direction"] == "LONG":
        if oi["avg_delta"] < MIN_OI_GROWTH_PCT:
            STATS["rejected_oi"] += 1
            return
    else:
        if oi["avg_delta"] < MIN_OI_GROWTH_PCT:
            STATS["rejected_oi"] += 1
            return

    STATS["flow_confirmed"] += 1

    binance_oi = await fetch_binance_coinglass(base)
    if binance_oi:
        STATS["coinglass_confirmed"] += 1

    binance_price = None
    item = COMMON_SYMBOLS.get(base)
    if item and item.get("binance"):
        binance_price = item["binance"]["price"]
        STATS["binance_confirmed"] += 1

    if not flow_allowed(base) or not signal_allowed(base):
        return

    LAST_FLOW[base] = time.time()
    LAST_SIGNAL[base] = time.time()
    STATS["early_signals"] += 1

    await send_flow_signal(base, flow, oi, binance_oi, binance_price)


# ============================================================
# UNIVERSE REFRESH
# ============================================================

async def refresh_universe():
    global BITGET_DATA, KUCOIN_DATA, BINANCE_DATA, COMMON_SYMBOLS

    bitget, kucoin, binance = await asyncio.gather(
        fetch_bitget_tickers(),
        fetch_kucoin_contracts(),
        fetch_binance_tickers(),
    )

    if not bitget or not kucoin:
        log.warning(
            "Universe refresh failed: Bitget=%d KuCoin=%d Binance=%d",
            len(bitget), len(kucoin), len(binance)
        )
        return

    BITGET_DATA = bitget
    KUCOIN_DATA = kucoin
    BINANCE_DATA = binance

    in_range = 0
    for base in set(bitget.keys()) & set(kucoin.keys()):
        pb = num(bitget[base].get("price"))
        pk = num(kucoin[base].get("price"))
        if pb > 0 and pk > 0:
            avg = (pb + pk) / 2
            if MIN_PRICE_USDT <= avg <= MAX_PRICE_USDT:
                in_range += 1

    COMMON_SYMBOLS = build_common_universe(bitget, kucoin, binance)

    for b in [x for x in CANDLE_CACHE if x not in COMMON_SYMBOLS]:
        del CANDLE_CACHE[b]

    cleanup_oi_history()

    STATS["universe_refresh"] += 1
    STATS["common_symbols"] = len(COMMON_SYMBOLS)
    STATS["price_in_range"] = in_range

    log.info(
        "UNIVERSE | Bitget=%d | KuCoin=%d | Binance=%d | "
        "In price range [%.4f-%.4f]: %d | Common: %d",
        len(bitget), len(kucoin), len(binance),
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

    bitget, kucoin, binance = await asyncio.gather(
        fetch_bitget_tickers(),
        fetch_kucoin_contracts(),
        fetch_binance_tickers(),
    )

    def vol_sum(data):
        return sum(num(v.get("volume24")) for v in data.values())

    def oi_sum_kucoin(data):
        return sum(num(v.get("oi")) for v in data.values())

    sample_base = None
    sample_bitget_symbol = None
    sample_kucoin_symbol = None

    if COMMON_SYMBOLS:
        sample_base = max(
            COMMON_SYMBOLS.items(),
            key=lambda x: x[1]["aggregate_volume"]
        )[0]
        item = COMMON_SYMBOLS[sample_base]
        sample_bitget_symbol = item["bitget"]["symbol"]
        sample_kucoin_symbol = item["kucoin"]["symbol"]

    bg_candles, kc_candles, bg_oi = [], [], 0.0
    if sample_bitget_symbol and sample_kucoin_symbol:
        bg_candles, kc_candles, bg_oi = await asyncio.gather(
            fetch_bitget_candles(sample_bitget_symbol),
            fetch_kucoin_candles(sample_kucoin_symbol),
            fetch_bitget_oi(sample_bitget_symbol),
        )

    def rvol_of(candles):
        m = candle_metrics(candles)
        return m["rvol"] if m else 0.0

    bg_rvol = rvol_of(bg_candles)
    kc_rvol = rvol_of(kc_candles)

    kc_oi = 0.0
    if sample_base and sample_base in kucoin:
        kc_oi = num(kucoin[sample_base].get("oi"))

    log.info(
        "HOURLY | BG=%d KC=%d BN=%d common=%d sample=%s "
        "bg_rvol=%.2f kc_rvol=%.2f bg_oi=%.0f kc_oi=%.0f | "
        "OI: api=%d ticker=%d miss=%d",
        len(bitget), len(kucoin), len(binance),
        len(COMMON_SYMBOLS), sample_base or "-",
        bg_rvol, kc_rvol, bg_oi, kc_oi,
        STATS["bg_oi_from_api"],
        STATS["bg_oi_from_ticker"],
        STATS["bg_oi_missing"],
    )

    msg = (
        "🩺 <b>ЧАСОВАЯ ДИАГНОСТИКА СИСТЕМЫ</b>\n\n"
        f"<b>Тикеры с бирж</b>\n"
        f"Bitget: {len(bitget)} | 24h vol≈${vol_sum(bitget)/1e6:.1f}M\n"
        f"KuCoin: {len(kucoin)} | 24h vol≈${vol_sum(kucoin)/1e6:.1f}M | Сумма OI≈{oi_sum_kucoin(kucoin):,.0f}\n"
        f"Binance vision: {len(binance)} | 24h vol≈${vol_sum(binance)/1e6:.1f}M\n\n"
        f"<b>Общие монеты (Common)</b>: {len(COMMON_SYMBOLS)}\n"
        f"<b>Тестовый sample</b>: <code>{sample_base or 'n/a'}USDT</code>\n"
        f"Bitget свечи: {len(bg_candles)} | RVOL≈{bg_rvol:.2f}x | OI={bg_oi:,.0f}\n"
        f"KuCoin свечи: {len(kc_candles)} | RVOL≈{kc_rvol:.2f}x | OI={kc_oi:,.0f}\n\n"
        f"<b>OI Bitget источники</b>\n"
        f"API: {STATS['bg_oi_from_api']} | ticker: {STATS['bg_oi_from_ticker']} | нет: {STATS['bg_oi_missing']}\n\n"
        f"<b>Статистика HTTP запросов</b>\n"
        f"BG {STATS['bitget_requests']}/{STATS['bitget_errors']} ош | "
        f"KC {STATS['kucoin_requests']}/{STATS['kucoin_errors']} ош | "
        f"BN {STATS['binance_requests']}/{STATS['binance_errors']} ош | "
        f"CG {STATS['coinglass_requests']}/{STATS['coinglass_errors']} ош\n\n"
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
    now = time.time()
    while COINGLASS_REQUESTS and COINGLASS_REQUESTS[0] < now - 3600:
        COINGLASS_REQUESTS.popleft()

    return f"""
CEX FUTURES AGGREGATOR v2.1
===========================

Uptime:
{format_uptime()}

PRICE FILTER
------------
Range: {MIN_PRICE_USDT} — {MAX_PRICE_USDT} USDT
Coins in range: {STATS.get("price_in_range", 0)}

MARKET
------
Common symbols: {STATS["common_symbols"]}
Candle candidates: {STATS["candle_candidates"]}
OI candidates: {STATS["oi_candidates"]}

OI BITGET SOURCES
-----------------
From API:    {STATS["bg_oi_from_api"]}
From ticker: {STATS["bg_oi_from_ticker"]}
Missing:     {STATS["bg_oi_missing"]}

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

COINGLASS
---------
Requests/hour: {len(COINGLASS_REQUESTS)} / {MAX_COINGLASS_PER_HOUR}
Binance confirmations: {STATS["coinglass_confirmed"]}

BINANCE (vision)
----------------
Confirmations: {STATS["binance_confirmed"]}

HTTP REQUESTS
-------------
Bitget: {STATS["bitget_requests"]}
KuCoin: {STATS["kucoin_requests"]}
Binance: {STATS["binance_requests"]}
CoinGlass: {STATS["coinglass_requests"]}

ERRORS
------
Bitget: {STATS["bitget_errors"]}
KuCoin: {STATS["kucoin_errors"]}
Binance: {STATS["binance_errors"]}
CoinGlass: {STATS["coinglass_errors"]}

REFRESHES
---------
Universe: {STATS["universe_refresh"]}
Candle checks: {STATS["candle_checks"]}
OI checks: {STATS["oi_checks"]}
"""


# ============================================================
# WEB
# ============================================================

async def index(request):
    return web.Response(text=stats_text(), content_type="text/plain")


async def health(request):
    return web.Response(text="CEX FUTURES AGGREGATOR ACTIVE", content_type="text/plain")


async def start_background(app):
    global SESSION

    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    )
    app["scanner_task"] = asyncio.create_task(scanner_loop())

    startup_msg = (
        "🚀 <b>CEX FUTURES AGGREGATOR v2.1 ЗАПУЩЕН</b>\n\n"
        "• Биржи: Bitget + KuCoin + Binance(vision)\n"
        f"• Ценовой фильтр: {MIN_PRICE_USDT}–{MAX_PRICE_USDT} USDT\n"
        "• IGNITION: детект по формирующейся свече\n"
        "• OI Bitget: с фоллбэком на тикер\n"
        "• Статус: поиск аномалий запущен."
    )
    asyncio.create_task(send_telegram(startup_msg))

    log.info("CEX FUTURES AGGREGATOR v2.1 STARTED")
    log.info("Exchanges: Bitget + KuCoin + Binance(vision)")
    log.info("Price filter: %.4f — %.4f USDT", MIN_PRICE_USDT, MAX_PRICE_USDT)


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
        SESSION = None


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/stats", index)
app.router.add_get("/health", health)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
