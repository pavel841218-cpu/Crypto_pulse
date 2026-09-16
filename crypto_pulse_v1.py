import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone


# ============================================================
#       CEX FUTURES AGGREGATOR
#
#       Bitget + KuCoin
#              ↓
#       Futures Volume Flow
#              ↓
#       OI confirmation
#              ↓
#       CoinGlass → Binance
#              ↓
#       EARLY FLOW / DISTRIBUTION
#
#       BingX НЕ используется
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")

PORT = int(os.environ.get("PORT", "10000"))

# ------------------------------------------------------------
# Traffic protection
# ------------------------------------------------------------

UNIVERSE_REFRESH_SEC = 120

# Свечи не гоняем постоянно.
CANDLE_REFRESH_SEC = 60

# После подтверждения CEX OI.
OI_REFRESH_SEC = 60

# Максимум монет после дешёвого фильтра.
MAX_COMMON_SYMBOLS = 40

# Максимум монет, которым разрешено получать 5m candles.
MAX_CANDLE_CANDIDATES = 20

# Максимум кандидатов, которым разрешено проверять OI.
MAX_OI_CANDIDATES = 5

# CoinGlass — только после двух CEX.
MAX_COINGLASS_PER_HOUR = 20

# Не сигналить одну и ту же монету слишком часто.
SIGNAL_COOLDOWN_SEC = 4 * 3600

# Повторный early-flow только через это время.
FLOW_COOLDOWN_SEC = 90 * 60


# ============================================================
# Strategy thresholds
# ============================================================

MIN_24H_VOLUME_USDT = 1_000_000

# Минимальное изменение закрытой 5m свечи.
MIN_5M_MOVE_PCT = 0.65

# Сильный импульс.
STRONG_5M_MOVE_PCT = 1.20

# RVOL.
MIN_RVOL = 2.0
STRONG_RVOL = 3.0

# Совокупный объём двух CEX.
MIN_AGG_RVOL = 1.80
STRONG_AGG_RVOL = 2.50

# OI.
MIN_OI_GROWTH_PCT = 0.20
STRONG_OI_GROWTH_PCT = 0.60

# Не считаем свечу нормальной, если цена уже улетела.
MAX_5M_MOVE_FOR_EARLY = 3.5

# Расстояние от предыдущего high для раннего сигнала.
MAX_BREAKOUT_DISTANCE_PCT = 4.0


# ============================================================
# URLs
# ============================================================

BITGET_BASE = "https://api.bitget.com"

KUCOIN_BASE = "https://api-futures.kucoin.com"

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

CANDLE_CACHE = {}

OI_HISTORY = defaultdict(lambda: {
    "bitget": deque(maxlen=12),
    "kucoin": deque(maxlen=12),
})

LAST_SIGNAL = {}
LAST_FLOW = {}

COINGLASS_REQUESTS = deque()

STATS = {
    "bitget_requests": 0,
    "kucoin_requests": 0,
    "coinglass_requests": 0,

    "bitget_errors": 0,
    "kucoin_errors": 0,
    "coinglass_errors": 0,

    "universe_refresh": 0,
    "candle_checks": 0,
    "oi_checks": 0,

    "common_symbols": 0,
    "candle_candidates": 0,
    "oi_candidates": 0,

    "flow_confirmed": 0,
    "coinglass_confirmed": 0,

    "early_signals": 0,
    "distribution_signals": 0,
}


# ============================================================
# HTTP helper
# ============================================================

async def http_get(
    url,
    params=None,
    headers=None,
    timeout=10,
    exchange="other"
):
    global SESSION

    try:
        if exchange == "bitget":
            STATS["bitget_requests"] += 1

        elif exchange == "kucoin":
            STATS["kucoin_requests"] += 1

        elif exchange == "coinglass":
            STATS["coinglass_requests"] += 1

        async with SESSION.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:

            if resp.status == 429:
                log.warning(
                    "429 rate limit: %s",
                    exchange
                )
                return None

            if resp.status >= 500:
                log.warning(
                    "%s server error %s",
                    exchange,
                    resp.status
                )
                return None

            if resp.status != 200:
                text = await resp.text()

                log.warning(
                    "%s HTTP %s: %s",
                    exchange,
                    resp.status,
                    text[:200]
                )

                return None

            return await resp.json()

    except asyncio.TimeoutError:
        log.warning(
            "%s timeout",
            exchange
        )

        if exchange == "bitget":
            STATS["bitget_errors"] += 1

        elif exchange == "kucoin":
            STATS["kucoin_errors"] += 1

        elif exchange == "coinglass":
            STATS["coinglass_errors"] += 1

        return None

    except Exception as e:
        log.warning(
            "%s request error: %s",
            exchange,
            e
        )

        if exchange == "bitget":
            STATS["bitget_errors"] += 1

        elif exchange == "kucoin":
            STATS["kucoin_errors"] += 1

        elif exchange == "coinglass":
            STATS["coinglass_errors"] += 1

        return None


# ============================================================
# Numeric helper
# ============================================================

def num(value, default=0.0):
    try:
        if value is None:
            return default

        return float(value)

    except Exception:
        return default


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def normalize_base(symbol):
    """
    BTCUSDT -> BTC
    BTCUSDTM -> BTC
    XBTUSDTM -> BTC
    """

    if not symbol:
        return ""

    s = str(symbol).upper()

    if s.startswith("XBT"):
        return "BTC"

    for suffix in (
        "USDTM",
        "USDT",
        "-USDT",
        "_USDT",
    ):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break

    return s


# ============================================================
# BITGET
# ============================================================

async def fetch_bitget_tickers():
    """
    Один запрос получает рынок Bitget.
    Это дешёвый этап.
    """

    url = (
        BITGET_BASE
        "/api/v2/mix/market/tickers"
    )

    params = {
        "productType": "USDT-FUTURES"
    }

    data = await http_get(
        url,
        params=params,
        exchange="bitget"
    )

    result = {}

    if not data:
        return result

    rows = data.get("data", [])

    for row in rows:

        symbol = str(
            row.get("symbol", "")
        ).upper()

        if not symbol.endswith("USDT"):
            continue

        base = normalize_base(symbol)

        if not base:
            continue

        price = num(
            row.get("lastPr")
            or row.get("lastPrice")
            or row.get("last")
        )

        quote_volume = num(
            row.get("quoteVolume")
            or row.get("usdtVolume")
            or row.get("quoteVol")
            or row.get("turnover")
        )

        change = num(
            row.get("change24h")
            or row.get("changeUtc24h")
        )

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": quote_volume,
            "change24": change,
        }

    return result


# ============================================================
# BITGET 5M CANDLES
# ============================================================

async def fetch_bitget_candles(symbol):
    url = (
        BITGET_BASE
        "/api/v2/mix/market/candles"
    )

    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "granularity": "5m",
        "limit": "24",
    }

    data = await http_get(
        url,
        params=params,
        exchange="bitget"
    )

    if not data:
        return []

    rows = data.get("data", [])

    candles = []

    for row in rows:

        if not isinstance(row, list):
            continue

        if len(row) < 6:
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
                "ts": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            })

        except Exception:
            continue

    candles.sort(
        key=lambda x: x["ts"]
    )

    return candles


# ============================================================
# BITGET OI
# ============================================================

async def fetch_bitget_oi(symbol):
    url = (
        BITGET_BASE
        "/api/v2/mix/market/open-interest"
    )

    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
    }

    data = await http_get(
        url,
        params=params,
        exchange="bitget"
    )

    if not data:
        return 0.0

    rows = data.get("data", [])

    if isinstance(rows, dict):
        rows = [rows]

    if not rows:
        return 0.0

    row = rows[0]

    return num(
        row.get("openInterest")
        or row.get("oi")
        or row.get("openInterestUsd")
    )


# ============================================================
# KUCOIN ACTIVE CONTRACTS
# ============================================================

async def fetch_kucoin_contracts():

    url = (
        KUCOIN_BASE
        "/api/v1/contracts/active"
    )

    data = await http_get(
        url,
        exchange="kucoin"
    )

    result = {}

    if not data:
        return result

    rows = data.get("data", [])

    for row in rows:

        if str(
            row.get("status", "")
        ).lower() != "open":
            continue

        if str(
            row.get("settleCurrency", "")
        ).upper() != "USDT":
            continue

        symbol = str(
            row.get("symbol", "")
        ).upper()

        base = normalize_base(
            row.get("baseCurrency")
            or symbol
        )

        if not base:
            continue

        price = num(
            row.get("lastTradePrice")
            or row.get("markPrice")
        )

        turnover = num(
            row.get("turnoverOf24h")
        )

        oi = num(
            row.get("openInterest")
        )

        change = num(
            row.get("priceChgPct")
        ) * 100

        result[base] = {
            "symbol": symbol,
            "price": price,
            "volume24": turnover,
            "change24": change,
            "oi": oi,
        }

    return result


# ============================================================
# KUCOIN 5M CANDLES
# ============================================================

async def fetch_kucoin_candles(symbol):

    url = (
        KUCOIN_BASE
        "/api/v1/kline/query"
    )

    params = {
        "symbol": symbol,
        "granularity": "5",
    }

    data = await http_get(
        url,
        params=params,
        exchange="kucoin"
    )

    if not data:
        return []

    rows = data.get("data", [])

    candles = []

    for row in rows:

        if not isinstance(row, list):
            continue

        if len(row) < 6:
            continue

        try:

            ts = int(row[0])

            o = num(row[1])
            c = num(row[2])
            h = num(row[3])
            l = num(row[4])
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

    candles.sort(
        key=lambda x: x["ts"]
    )

    return candles[-24:]


# ============================================================
# CANDLE ANALYSIS
# ============================================================

def candle_metrics(candles):

    if len(candles) < 8:
        return None

    # Последняя свеча может быть ещё открыта.
    # Поэтому анализируем предпоследнюю.
    c = candles[-2]

    previous = candles[:-2]

    if not previous:
        return None

    close = c["close"]
    open_price = c["open"]
    high = c["high"]
    low = c["low"]

    if open_price <= 0:
        return None

    move_pct = (
        (close / open_price) - 1
    ) * 100

    candle_range = high - low

    if candle_range <= 0:
        return None

    body = abs(close - open_price)

    body_ratio = body / candle_range

    close_position = (
        (close - low) / candle_range
    )

    volumes = [
        x["volume"]
        for x in previous[-12:]
        if x["volume"] > 0
    ]

    if not volumes:
        return None

    avg_volume = (
        sum(volumes) / len(volumes)
    )

    if avg_volume <= 0:
        return None

    rvol = c["volume"] / avg_volume

    previous_high = max(
        x["high"]
        for x in previous[-6:]
    )

    previous_low = min(
        x["low"]
        for x in previous[-6:]
    )

    breakout_up_pct = (
        (close / previous_high) - 1
    ) * 100

    breakout_down_pct = (
        (previous_low / close) - 1
    ) * 100

    upper_wick = high - max(
        open_price,
        close
    )

    lower_wick = min(
        open_price,
        close
    ) - low

    upper_wick_ratio = (
        upper_wick / candle_range
    )

    lower_wick_ratio = (
        lower_wick / candle_range
    )

    return {
        "ts": c["ts"],
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": c["volume"],

        "move_pct": move_pct,
        "rvol": rvol,
        "body_ratio": body_ratio,
        "close_position": close_position,

        "previous_high": previous_high,
        "previous_low": previous_low,

        "breakout_up_pct": breakout_up_pct,
        "breakout_down_pct": breakout_down_pct,

        "upper_wick_ratio": upper_wick_ratio,
        "lower_wick_ratio": lower_wick_ratio,
    }


# ============================================================
# CHEAP UNIVERSE
# ============================================================

def build_common_universe(
    bitget,
    kucoin
):

    common = {}

    common_bases = (
        set(bitget.keys())
        &
        set(kucoin.keys())
    )

    for base in common_bases:

        b = bitget[base]
        k = kucoin[base]

        volume_b = b["volume24"]
        volume_k = k["volume24"]

        if volume_b < MIN_24H_VOLUME_USDT:
            continue

        if volume_k < MIN_24H_VOLUME_USDT:
            continue

        aggregate_volume = (
            volume_b + volume_k
        )

        common[base] = {
            "bitget": b,
            "kucoin": k,
            "aggregate_volume": aggregate_volume,
        }

    # Берём самые ликвидные.
    common = dict(
        sorted(
            common.items(),
            key=lambda x:
                x[1]["aggregate_volume"],
            reverse=True
        )[
            :MAX_COMMON_SYMBOLS
        ]
    )

    return common


# ============================================================
# CANDLE CACHE
# ============================================================

async def get_candles(base):

    now = time.time()

    cached = CANDLE_CACHE.get(base)

    if cached:

        if now - cached["time"] < CANDLE_REFRESH_SEC:

            return cached["data"]

    item = COMMON_SYMBOLS.get(base)

    if not item:
        return None

    bitget_symbol = (
        item["bitget"]["symbol"]
    )

    kucoin_symbol = (
        item["kucoin"]["symbol"]
    )

    # Два запроса параллельно.
    bitget_task = fetch_bitget_candles(
        bitget_symbol
    )

    kucoin_task = fetch_kucoin_candles(
        kucoin_symbol
    )

    bitget_candles, kucoin_candles = (
        await asyncio.gather(
            bitget_task,
            kucoin_task
        )
    )

    result = {
        "bitget": bitget_candles,
        "kucoin": kucoin_candles,
    }

    CANDLE_CACHE[base] = {
        "time": now,
        "data": result,
    }

    return result


# ============================================================
# FLOW ANALYSIS
# ============================================================

def analyze_flow(base, candles):

    if not candles:
        return None

    b = candle_metrics(
        candles.get("bitget", [])
    )

    k = candle_metrics(
        candles.get("kucoin", [])
    )

    if not b or not k:
        return None

    # Направление должно совпадать.
    same_up = (
        b["move_pct"] >= MIN_5M_MOVE_PCT
        and
        k["move_pct"] >= MIN_5M_MOVE_PCT
    )

    same_down = (
        b["move_pct"] <= -MIN_5M_MOVE_PCT
        and
        k["move_pct"] <= -MIN_5M_MOVE_PCT
    )

    if not same_up and not same_down:
        return {
            "confirmed": False,
            "reason": "no_sync",
            "bitget": b,
            "kucoin": k,
        }

    direction = (
        "LONG"
        if same_up
        else
        "SHORT"
    )

    # --------------------------------------------------------
    # Aggregate futures volume
    # --------------------------------------------------------

    agg_rvol = (
        b["rvol"] + k["rvol"]
    ) / 2

    # --------------------------------------------------------
    # Price synchronization
    # --------------------------------------------------------

    price_move = (
        abs(b["move_pct"])
        +
        abs(k["move_pct"])
    ) / 2

    # --------------------------------------------------------
    # Strong volume
    # --------------------------------------------------------

    volume_ok = (
        b["rvol"] >= MIN_RVOL
        and
        k["rvol"] >= MIN_RVOL
        and
        agg_rvol >= MIN_AGG_RVOL
    )

    # --------------------------------------------------------
    # Body / close
    # --------------------------------------------------------

    if direction == "LONG":

        structure_ok = (
            b["close_position"] >= 0.65
            and
            k["close_position"] >= 0.65
            and
            b["body_ratio"] >= 0.45
            and
            k["body_ratio"] >= 0.45
        )

    else:

        structure_ok = (
            b["close_position"] <= 0.35
            and
            k["close_position"] <= 0.35
            and
            b["body_ratio"] >= 0.45
            and
            k["body_ratio"] >= 0.45
        )

    # --------------------------------------------------------
    # Too late?
    # --------------------------------------------------------

    too_late = (
        abs(b["move_pct"])
        > MAX_5M_MOVE_FOR_EARLY
        or
        abs(k["move_pct"])
        > MAX_5M_MOVE_FOR_EARLY
    )

    # --------------------------------------------------------
    # Distribution detection
    # --------------------------------------------------------

    distribution = False

    if direction == "LONG":

        if (
            b["rvol"] >= STRONG_RVOL
            and
            k["rvol"] >= STRONG_RVOL
            and
            (
                b["upper_wick_ratio"] >= 0.35
                or
                k["upper_wick_ratio"] >= 0.35
            )
            and
            (
                b["close_position"] < 0.72
                or
                k["close_position"] < 0.72
            )
        ):
            distribution = True

    # --------------------------------------------------------
    # Early Flow
    # --------------------------------------------------------

    confirmed = (
        volume_ok
        and
        structure_ok
        and
        not too_late
        and
        not distribution
    )

    strong = (
        confirmed
        and
        agg_rvol >= STRONG_AGG_RVOL
        and
        price_move >= STRONG_5M_MOVE_PCT
    )

    return {
        "confirmed": confirmed,
        "strong": strong,
        "distribution": distribution,

        "direction": direction,

        "bitget": b,
        "kucoin": k,

        "aggregate_rvol": agg_rvol,
        "average_move": price_move,

        "too_late": too_late,
    }


# ============================================================
# OI
# ============================================================

async def update_oi(base):

    item = COMMON_SYMBOLS.get(base)

    if not item:
        return None

    now = time.time()

    # --------------------------------------------------------
    # KuCoin OI уже есть в contracts/active.
    # Никакого отдельного запроса.
    # --------------------------------------------------------

    kucoin_oi = num(
        item["kucoin"].get("oi")
    )

    # --------------------------------------------------------
    # Bitget OI — только для кандидата.
    # --------------------------------------------------------

    bitget_symbol = (
        item["bitget"]["symbol"]
    )

    bitget_oi = await fetch_bitget_oi(
        bitget_symbol
    )

    if kucoin_oi <= 0 and bitget_oi <= 0:
        return None

    hist = OI_HISTORY[base]

    if bitget_oi > 0:
        hist["bitget"].append(
            (
                now,
                bitget_oi
            )
        )

    if kucoin_oi > 0:
        hist["kucoin"].append(
            (
                now,
                kucoin_oi
            )
        )

    def calc_delta(history):

        if len(history) < 2:
            return 0.0

        old = history[0][1]
        new = history[-1][1]

        if old <= 0:
            return 0.0

        return (
            (new / old) - 1
        ) * 100

    bitget_delta = calc_delta(
        hist["bitget"]
    )

    kucoin_delta = calc_delta(
        hist["kucoin"]
    )

    valid = []

    if bitget_oi > 0:
        valid.append(
            bitget_delta
        )

    if kucoin_oi > 0:
        valid.append(
            kucoin_delta
        )

    if not valid:
        return None

    avg_delta = (
        sum(valid) / len(valid)
    )

    return {
        "bitget_oi": bitget_oi,
        "kucoin_oi": kucoin_oi,

        "bitget_delta": bitget_delta,
        "kucoin_delta": kucoin_delta,

        "avg_delta": avg_delta,

        "positive_count": sum(
            1 for x in valid
            if x >= MIN_OI_GROWTH_PCT
        ),

        "negative_count": sum(
            1 for x in valid
            if x <= -MIN_OI_GROWTH_PCT
        ),
    }


# ============================================================
# COINGLASS LIMIT
# ============================================================

def coinglass_allowed():

    now = time.time()

    one_hour_ago = (
        now - 3600
    )

    while (
        COINGLASS_REQUESTS
        and
        COINGLASS_REQUESTS[0]
        < one_hour_ago
    ):
        COINGLASS_REQUESTS.popleft()

    return (
        len(COINGLASS_REQUESTS)
        <
        MAX_COINGLASS_PER_HOUR
    )


# ============================================================
# COINGLASS BINANCE
# ============================================================

async def fetch_binance_coinglass(
    base
):

    if not COINGLASS_API_KEY:
        return None

    if not coinglass_allowed():
        log.warning(
            "CoinGlass hourly limit reached"
        )
        return None

    url = (
        COINGLASS_BASE
        "/api/futures/open-interest/"
        "exchange-list"
    )

    headers = {
        "CG-API-KEY": COINGLASS_API_KEY,
        "accept": "application/json",
    }

    params = {
        "symbol": base
    }

    COINGLASS_REQUESTS.append(
        time.time()
    )

    data = await http_get(
        url,
        params=params,
        headers=headers,
        exchange="coinglass"
    )

    if not data:
        return None

    rows = data.get("data")

    if isinstance(rows, dict):
        rows = [rows]

    if not isinstance(rows, list):
        return None

    for row in rows:

        exchange = str(
            row.get("exchange")
            or row.get("exchangeName")
            or ""
        ).lower()

        if exchange != "binance":
            continue

        oi = num(
            row.get("openInterest")
            or row.get("oi")
            or row.get("open_interest")
        )

        if oi <= 0:
            continue

        return {
            "exchange": "Binance",
            "oi": oi,
        }

    return None


# ============================================================
# TELEGRAM
# ============================================================

async def send_telegram(text):

    if not BOT_TOKEN or not CHAT_ID:
        log.warning(
            "BOT_TOKEN / CHAT_ID not configured"
        )
        return False

    url = (
        f"https://api.telegram.org/bot"
        f"{BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:

        async with SESSION.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(
                total=10
            )
        ) as resp:

            if resp.status != 200:
                log.warning(
                    "Telegram HTTP %s",
                    resp.status
                )
                return False

            return True

    except Exception as e:

        log.warning(
            "Telegram error: %s",
            e
        )

        return False


# ============================================================
# SIGNAL COOLDOWN
# ============================================================

def signal_allowed(base):

    last = LAST_SIGNAL.get(base, 0)

    return (
        time.time() - last
        >= SIGNAL_COOLDOWN_SEC
    )


def flow_allowed(base):

    last = LAST_FLOW.get(base, 0)

    return (
        time.time() - last
        >= FLOW_COOLDOWN_SEC
    )


# ============================================================
# EARLY FLOW MESSAGE
# ============================================================

async def send_flow_signal(
    base,
    flow,
    oi,
    binance_oi,
):

    direction = flow["direction"]

    b = flow["bitget"]
    k = flow["kucoin"]

    message = (
        "🚨 <b>CEX EARLY FLOW</b>\n\n"

        f"<b>{base}USDT</b> — "
        f"<b>{direction}</b>\n\n"

        "━━━━━━━━━━━━━━\n"
        "📊 <b>Bitget</b>\n"
        f"Цена 5m: {b['move_pct']:+.2f}%\n"
        f"RVOL: {b['rvol']:.2f}x\n"
        f"Close strength: "
        f"{b['close_position']:.2f}\n\n"

        "📊 <b>KuCoin</b>\n"
        f"Цена 5m: {k['move_pct']:+.2f}%\n"
        f"RVOL: {k['rvol']:.2f}x\n"
        f"Close strength: "
        f"{k['close_position']:.2f}\n\n"

        "━━━━━━━━━━━━━━\n"
        "🌐 <b>AGGREGATED FUTURES FLOW</b>\n"
        f"Avg RVOL: "
        f"{flow['aggregate_rvol']:.2f}x\n"
        f"Avg move: "
        f"{flow['average_move']:+.2f}%\n\n"

        "📈 <b>OI</b>\n"
        f"Bitget ΔOI: "
        f"{oi['bitget_delta']:+.2f}%\n"
        f"KuCoin ΔOI: "
        f"{oi['kucoin_delta']:+.2f}%\n"
        f"Avg ΔOI: "
        f"{oi['avg_delta']:+.2f}%\n\n"
    )

    if binance_oi:

        message += (
            "🟡 <b>CoinGlass → Binance</b>\n"
            f"OI: {binance_oi['oi']:,.0f}\n\n"
        )

    message += (
        "⚠️ Это ранний поток, "
        "а не сигнал входа.\n"
        "Ожидаем продолжение / ретест."
    )

    await send_telegram(message)


# ============================================================
# DISTRIBUTION MESSAGE
# ============================================================

async def send_distribution(
    base,
    flow
):

    b = flow["bitget"]
    k = flow["kucoin"]

    message = (
        "⚠️ <b>CEX DISTRIBUTION</b>\n\n"

        f"<b>{base}USDT</b>\n\n"

        "Обнаружено:\n"
        "• экстремальный futures volume\n"
        "• новый локальный high\n"
        "• слабое продолжение\n"
        "• верхняя тень / плохое закрытие\n\n"

        f"Bitget RVOL: {b['rvol']:.2f}x\n"
        f"KuCoin RVOL: {k['rvol']:.2f}x\n\n"

        "🛑 LONG FLOW временно заблокирован."
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

    flow = analyze_flow(
        base,
        candles
    )

    if not flow:
        return

    # --------------------------------------------------------
    # DISTRIBUTION
    # --------------------------------------------------------

    if flow.get("distribution"):

        if flow_allowed(base):

            LAST_FLOW[base] = time.time()

            STATS[
                "distribution_signals"
            ] += 1

            await send_distribution(
                base,
                flow
            )

        return

    if not flow.get("confirmed"):
        return

    STATS["flow_confirmed"] += 1

    # --------------------------------------------------------
    # OI
    # --------------------------------------------------------

    oi = await update_oi(base)

    STATS["oi_checks"] += 1

    if not oi:
        return

    STATS["oi_candidates"] += 1

    # --------------------------------------------------------
    # OI must support price.
    # --------------------------------------------------------

    if flow["direction"] == "LONG":

        if oi["avg_delta"] < MIN_OI_GROWTH_PCT:
            return

    else:

        if oi["avg_delta"] > -MIN_OI_GROWTH_PCT:
            return

    # --------------------------------------------------------
    # CoinGlass Binance
    #
    # ONLY NOW.
    # --------------------------------------------------------

    binance_oi = await fetch_binance_coinglass(
        base
    )

    if binance_oi:
        STATS[
            "coinglass_confirmed"
        ] += 1

    # --------------------------------------------------------
    # Final signal
    # --------------------------------------------------------

    if not flow_allowed(base):
        return

    if not signal_allowed(base):
        return

    LAST_FLOW[base] = time.time()
    LAST_SIGNAL[base] = time.time()

    STATS["early_signals"] += 1

    await send_flow_signal(
        base,
        flow,
        oi,
        binance_oi
    )


# ============================================================
# UNIVERSE REFRESH
# ============================================================

async def refresh_universe():

    global BITGET_DATA
    global KUCOIN_DATA
    global COMMON_SYMBOLS

    bitget_task = fetch_bitget_tickers()

    kucoin_task = fetch_kucoin_contracts()

    bitget, kucoin = await asyncio.gather(
        bitget_task,
        kucoin_task
    )

    if not bitget or not kucoin:

        log.warning(
            "Universe refresh failed: "
            "Bitget=%d KuCoin=%d",
            len(bitget),
            len(kucoin)
        )

        return

    BITGET_DATA = bitget
    KUCOIN_DATA = kucoin

    COMMON_SYMBOLS = build_common_universe(
        bitget,
        kucoin
    )

    STATS["universe_refresh"] += 1

    STATS["common_symbols"] = len(
        COMMON_SYMBOLS
    )

    log.info(
        "UNIVERSE | Bitget=%d | "
        "KuCoin=%d | Common=%d",
        len(bitget),
        len(kucoin),
        len(COMMON_SYMBOLS)
    )


# ============================================================
# SELECT CANDLE CANDIDATES
# ============================================================

def select_candle_candidates():

    candidates = []

    for base, item in COMMON_SYMBOLS.items():

        b = item["bitget"]
        k = item["kucoin"]

        # ----------------------------------------------------
        # Не тянем свечи у всех.
        # Смотрим 24h change как дешёвый pre-filter.
        # ----------------------------------------------------

        move_b = abs(
            num(b.get("change24"))
        )

        move_k = abs(
            num(k.get("change24"))
        )

        # Небольшое движение тоже оставляем,
        # потому что нам нужен PRE-PUMP.
        score = (
            move_b
            +
            move_k
        )

        candidates.append(
            (
                score,
                base
            )
        )

    candidates.sort(
        reverse=True
    )

    selected = [
        base
        for _, base
        in candidates[
            :MAX_CANDLE_CANDIDATES
        ]
    ]

    STATS["candle_candidates"] = len(
        selected
    )

    return selected


# ============================================================
# MAIN SCAN
# ============================================================

async def scan_cycle():

    if not COMMON_SYMBOLS:
        return

    candidates = (
        select_candle_candidates()
    )

    # --------------------------------------------------------
    # Ограничиваем одновременно работающие запросы.
    # --------------------------------------------------------

    semaphore = asyncio.Semaphore(5)

    async def worker(base):

        async with semaphore:

            try:
                await process_symbol(base)

            except Exception as e:

                log.exception(
                    "Symbol %s error: %s",
                    base,
                    e
                )

    await asyncio.gather(
        *[
            worker(base)
            for base in candidates
        ]
    )


# ============================================================
# BACKGROUND LOOP
# ============================================================

async def scanner_loop():

    last_universe = 0

    while True:

        try:

            now = time.time()

            # ------------------------------------------------
            # Universe refresh.
            # Только раз в 120 сек.
            # ------------------------------------------------

            if (
                now - last_universe
                >= UNIVERSE_REFRESH_SEC
            ):

                await refresh_universe()

                last_universe = now

            # ------------------------------------------------
            # Main scan.
            # ------------------------------------------------

            if COMMON_SYMBOLS:

                await scan_cycle()

            # ------------------------------------------------
            # Не долбим API.
            # ------------------------------------------------

            await asyncio.sleep(
                CANDLE_REFRESH_SEC
            )

        except asyncio.CancelledError:
            raise

        except Exception as e:

            log.exception(
                "Scanner loop error: %s",
                e
            )

            await asyncio.sleep(15)


# ============================================================
# STATS PAGE
# ============================================================

def format_uptime():

    seconds = int(
        time.time() - START_TIME
    )

    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    return (
        f"{h:02d}:{m:02d}:{s:02d}"
    )


def stats_text():

    now = time.time()

    while (
        COINGLASS_REQUESTS
        and
        COINGLASS_REQUESTS[0]
        < now - 3600
    ):
        COINGLASS_REQUESTS.popleft()

    return f"""
CEX FUTURES AGGREGATOR
======================

Uptime:
{format_uptime()}

MARKET
------
Common symbols: {STATS["common_symbols"]}
Candle candidates: {STATS["candle_candidates"]}
OI candidates: {STATS["oi_candidates"]}

FLOW
----
Confirmed CEX flow: {STATS["flow_confirmed"]}
Early signals: {STATS["early_signals"]}
Distribution: {STATS["distribution_signals"]}

COINGLASS
---------
Requests/hour:
{len(COINGLASS_REQUESTS)} / {MAX_COINGLASS_PER_HOUR}

Binance confirmations:
{STATS["coinglass_confirmed"]}

HTTP REQUESTS
-------------
Bitget:
{STATS["bitget_requests"]}

KuCoin:
{STATS["kucoin_requests"]}

CoinGlass:
{STATS["coinglass_requests"]}

ERRORS
------
Bitget:
{STATS["bitget_errors"]}

KuCoin:
{STATS["kucoin_errors"]}

CoinGlass:
{STATS["coinglass_errors"]}

REFRESHES
---------
Universe:
{STATS["universe_refresh"]}

Candle checks:
{STATS["candle_checks"]}

OI checks:
{STATS["oi_checks"]}
"""


# ============================================================
# WEB
# ============================================================

async def index(request):

    return web.Response(
        text=stats_text(),
        content_type="text/plain"
    )


async def health(request):

    return web.Response(
        text="CEX FUTURES AGGREGATOR ACTIVE",
        content_type="text/plain"
    )


# ============================================================
# STARTUP
# ============================================================

async def start_background(app):

    global SESSION

    SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300
        )
    )

    app["scanner_task"] = asyncio.create_task(
        scanner_loop()
    )

    log.info(
        "CEX FUTURES AGGREGATOR STARTED"
    )

    log.info(
        "Exchanges: Bitget + KuCoin"
    )

    log.info(
        "Binance: CoinGlass only"
    )

    log.info(
        "BingX: DISABLED"
    )

    log.info(
        "Max common symbols: %d",
        MAX_COMMON_SYMBOLS
    )

    log.info(
        "Max candle candidates: %d",
        MAX_CANDLE_CANDIDATES
    )


async def cleanup(app):

    task = app.get(
        "scanner_task"
    )

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


# ============================================================
# APP
# ============================================================

app = web.Application()

app.router.add_get(
    "/",
    index
)

app.router.add_get(
    "/stats",
    index
)

app.router.add_get(
    "/health",
    health
)

app.on_startup.append(
    start_background
)

app.on_cleanup.append(
    cleanup
)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    web.run_app(
        app,
        host="0.0.0.0",
        port=PORT
    )
