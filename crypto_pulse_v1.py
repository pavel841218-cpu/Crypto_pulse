import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque


# ============================================================
#       CEX FUTURES AGGREGATOR v3.3 (24H RVOL FIXED)
#       Bitget SCANNER → Binance CONFIRMATION
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

PORT = int(os.environ.get("PORT", "10000"))

UNIVERSE_REFRESH_SEC = 120
CANDLE_REFRESH_SEC = 30
OI_REFRESH_SEC = 60

MAX_COMMON_SYMBOLS = 400
MAX_CANDLE_CANDIDATES = 400

SIGNAL_COOLDOWN_SEC = 4 * 3600
FLOW_COOLDOWN_SEC = 90 * 60

HOURLY_LOG_SEC = 3600


# ============================================================
# Strategy thresholds
# ============================================================

MIN_24H_VOLUME_USDT = 400_000

MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 1.0

# ------------------------------------------------------------
# BITGET SCANNER (лидер)
# ------------------------------------------------------------
BITGET_MIN_RVOL = 2.5          # Bitget: свеча минимум в 2.5x от суточного среднего
BITGET_MIN_MOVE_PCT = 0.8      # Bitget: движение минимум +0.8% за 5m
BITGET_MIN_BODY_RATIO = 0.50   # Bitget: тело свечи ≥ 50% диапазона
BITGET_MIN_CLOSE_POSITION = 0.60  # Bitget: закрытие в верхней части свечи

# ------------------------------------------------------------
# BINANCE CONFIRMATION (подтверждающий)
# ------------------------------------------------------------
BINANCE_MIN_RVOL = 2.0         # Binance: свеча минимум в 2.0x от суточного среднего
BINANCE_MIN_MOVE_PCT = 0.5     # Binance: движение минимум +0.5% за 5m
BINANCE_MIN_OI_GROWTH_PCT = 0.15  # Binance: OI должен расти ≥0.15%

# Диапазон спреда Binance от Bitget
BINANCE_MIN_LEAD_PCT = -0.2    # Binance не должен отставать (отсекаем "холостые" прострелы Bitget)
BINANCE_MAX_LEAD_PCT = 3.0     # Binance не должен уходить слишком далеко (отсекаем "уходящий поезд")

MIN_CANDLES = 288              # 288 свечей по 5m = 24 часа истории


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
BINANCE_FUTURES_DATA = {}

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
    "bitget_scans": 0,
    "binance_confirmations": 0,
    "oi_checks": 0,

    "common_symbols": 0,
    "bitget_candidates": 0,
    "binance_confirmed": 0,

    "price_in_range": 0,

    "signals": 0,
    "distribution_signals": 0,

    "rejected_bitget_rvol": 0,
    "rejected_bitget_move": 0,
    "rejected_bitget_structure": 0,
    "rejected_binance_rvol": 0,
    "rejected_binance_move": 0,
    "rejected_binance_lead": 0,
    "rejected_oi": 0,

    "bg_oi_from_api": 0,
    "bg_oi_from_ticker": 0,
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

            if resp.status == 418:
                text = await resp.text()
                log.warning("418 BANNED: %s → %s", exchange, text[:200])
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
        "limit": "288",  # Увеличено до 288 свечей (24 часа)
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

    return 0.0


# ============================================================
# BINANCE FUTURES
# ============================================================

async def fetch_binance_premium_index():
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex"
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

        mark_price = num(row.get("markPrice"))

        result[base] = {
            "symbol": symbol,
            "price": mark_price,
        }

    return result


async def fetch_binance_candles(symbol):
    url = f"{BINANCE_FUTURES_BASE}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "5m",
        "limit": 288,  # Увеличено до 288 свечей (24 часа)
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
# CANDLE METRICS
# ============================================================

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

    # Расчёт среднего объёма по всей истории (до 288 прошлых 5-минуток)
    vols_usd = [(x["volume"] * x["close"]) for x in previous_candles[-288:] if x["volume"] > 0]
    if not vols_usd:
        return None

    avg_vol_usd = sum(vols_usd) / len(vols_usd)
    if avg_vol_usd <= 0:
        return None

    rvol = vol_usd / avg_vol_usd

    return {
        "ts": candle["ts"],
        "close": close,
        "move_pct": move_pct,
        "rvol": rvol,
        "volume_usd": vol_usd,
        "body_ratio": body_ratio,
        "close_position": close_position,
    }


def candle_metrics(candles):
    """Берём предпоследнюю свечу (последняя может быть не сформирована)."""
    if len(candles) < MIN_CANDLES:
        return None
    return candle_metrics_for(candles[-2], candles[:-2])


# ============================================================
# UNIVERSE
# ============================================================

def build_common_universe(bitget, binance_futures):
    common_bases = set(bitget.keys()) & set(binance_futures.keys())
    common = {}

    for base in common_bases:
        b = bitget[base]
        bnf = binance_futures[base]

        if b["volume24"] < MIN_24H_VOLUME_USDT:
            continue

        price_b = num(b.get("price"))
        price_bnf = num(bnf.get("price"))
        if price_b <= 0 or price_bnf <= 0:
            continue

        avg_price = (price_b + price_bnf) / 2
        if avg_price < MIN_PRICE_USDT or avg_price > MAX_PRICE_USDT:
            continue

        common[base] = {
            "bitget": b,
            "binance": bnf,
            "aggregate_volume": b["volume24"],
            "price": avg_price,
        }

    return dict(
        sorted(
            common.items(),
            key=lambda x: x[1]["aggregate_volume"],
            reverse=True
        )[:MAX_COMMON_SYMBOLS]
    )


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

    return {
        "bitget_oi": bitget_oi,
        "binance_oi": binance_oi,
        "bitget_delta": calc_delta(hist["bitget"]),
        "binance_delta": calc_delta(hist["binance"]),
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


# ============================================================
# SIGNAL MESSAGE
# ============================================================

async def send_signal(base, bg, bn, oi, lead_pct):
    message = (
        f"🚨 <b>BITGET → BINANCE CONFIRMED</b>\n\n"
        f"Монета: <code>{base}USDT</code> — 🟢 <b>LONG</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        "🎯 <b>Bitget (сканирование)</b>\n"
        f"Изменение 5m: {bg['move_pct']:+.2f}%\n"
        f"RVOL (24h): {bg['rvol']:.2f}x\n"
        f"Объём 5m: ${bg['volume_usd']:,.0f}\n"
        f"Тело свечи: {bg['body_ratio']:.2f}\n"
        f"Сила закрытия: {bg['close_position']:.2f}\n\n"
        "✅ <b>Binance (подтверждение)</b>\n"
        f"Изменение 5m: {bn['move_pct']:+.2f}%\n"
        f"RVOL (24h): {bn['rvol']:.2f}x\n"
        f"Объём 5m: ${bn['volume_usd']:,.0f}\n\n"
        "📈 <b>ОТКРЫТЫЙ ИНТЕРЕС</b>\n"
        f"Bitget ΔOI: {oi['bitget_delta']:+.2f}%\n"
        f"Binance ΔOI: {oi['binance_delta']:+.2f}%\n\n"
        f"📊 Отрыв Binance от Bitget: <b>{lead_pct:+.2f}%</b>\n\n"
        "⚠️ Двойное подтверждение — сигнал находится в окне первой волны."
    )
    await send_telegram(message)


# ============================================================
# PROCESS SYMBOL — BITGET SCANNER → BINANCE CONFIRM
# ============================================================

async def process_symbol(base):
    item = COMMON_SYMBOLS.get(base)
    if not item:
        return

    STATS["bitget_scans"] += 1

    # =========================================================
    # ШАГ 1: Bitget сканирует — ищем импульс
    # =========================================================
    bg_candles = await fetch_bitget_candles(item["bitget"]["symbol"])
    if len(bg_candles) < MIN_CANDLES:
        return

    bg = candle_metrics(bg_candles)
    if not bg:
        return

    # Bitget: только LONG-импульс
    if bg["move_pct"] < BITGET_MIN_MOVE_PCT:
        STATS["rejected_bitget_move"] += 1
        return

    if bg["rvol"] < BITGET_MIN_RVOL:
        STATS["rejected_bitget_rvol"] += 1
        return

    if bg["body_ratio"] < BITGET_MIN_BODY_RATIO:
        STATS["rejected_bitget_structure"] += 1
        return

    if bg["close_position"] < BITGET_MIN_CLOSE_POSITION:
        STATS["rejected_bitget_structure"] += 1
        return

    # Bitget нашёл импульс — это кандидат
    STATS["bitget_candidates"] += 1

    # =========================================================
    # ШАГ 2: Binance подтверждает
    # =========================================================
    bn_candles = await fetch_binance_candles(item["binance"]["symbol"])
    if len(bn_candles) < MIN_CANDLES:
        return

    bn = candle_metrics(bn_candles)
    if not bn:
        return

    STATS["binance_confirmations"] += 1

    # Binance: RVOL
    if bn["rvol"] < BINANCE_MIN_RVOL:
        STATS["rejected_binance_rvol"] += 1
        return

    # Binance: движение
    if bn["move_pct"] < BINANCE_MIN_MOVE_PCT:
        STATS["rejected_binance_move"] += 1
        return

    # ---------------------------------------------------------
    # Валидация спреда (отрыв Binance от Bitget)
    # ---------------------------------------------------------
    bg_price = num(item["bitget"].get("price"))
    bn_price = num(item["binance"].get("price"))
    if bg_price <= 0:
        return

    lead_pct = ((bn_price - bg_price) / bg_price) * 100

    # 1. Отсекаем фейки: Binance не должен отставать от Bitget (залив объемов на Binance обязателен)
    if lead_pct < BINANCE_MIN_LEAD_PCT:
        STATS["rejected_binance_lead"] += 1
        return

    # 2. Отсекаем "уходящий поезд": Binance ушел слишком далеко — заходить поздно
    if lead_pct > BINANCE_MAX_LEAD_PCT:
        STATS["rejected_binance_lead"] += 1
        return

    # =========================================================
    # ШАГ 3: OI — финальная валидация
    # =========================================================
    oi = await update_oi(base)
    STATS["oi_checks"] += 1
    if not oi:
        return

    # Binance OI должен расти
    if oi["binance_oi"] > 0 and oi["binance_delta"] < BINANCE_MIN_OI_GROWTH_PCT:
        STATS["rejected_oi"] += 1
        return

    # =========================================================
    # ШАГ 4: Сигнал
    # =========================================================
    if not signal_allowed(base):
        return

    LAST_SIGNAL[base] = time.time()
    STATS["signals"] += 1
    STATS["binance_confirmed"] += 1

    await send_signal(base, bg, bn, oi, lead_pct)


# ============================================================
# UNIVERSE REFRESH
# ============================================================

async def refresh_universe():
    global BITGET_DATA, BINANCE_FUTURES_DATA, BINANCE_FUTURES_SYMBOLS, COMMON_SYMBOLS

    bitget, binance_futures = await asyncio.gather(
        fetch_bitget_tickers(),
        fetch_binance_premium_index(),
    )

    if not bitget:
        log.warning("Bitget tickers failed")
        return

    if not binance_futures:
        log.warning("Binance premiumIndex failed")
        return

    BITGET_DATA = bitget
    BINANCE_FUTURES_DATA = binance_futures
    BINANCE_FUTURES_SYMBOLS = set(binance_futures.keys())

    in_range = 0
    for base in set(bitget.keys()) & set(binance_futures.keys()):
        pb = num(bitget[base].get("price"))
        pbn = num(binance_futures[base].get("price"))
        if pb > 0 and pbn > 0:
            avg = (pb + pbn) / 2
            if MIN_PRICE_USDT <= avg <= MAX_PRICE_USDT:
                in_range += 1

    COMMON_SYMBOLS = build_common_universe(bitget, binance_futures)

    for b in [x for x in CANDLE_CACHE if x not in COMMON_SYMBOLS]:
        del CANDLE_CACHE[b]

    cleanup_oi_history()

    STATS["universe_refresh"] += 1
    STATS["common_symbols"] = len(COMMON_SYMBOLS)
    STATS["price_in_range"] = in_range

    log.info(
        "UNIVERSE | Bitget=%d | BN Futures=%d | "
        "In price range [%.4f-%.4f]: %d | Common: %d",
        len(bitget), len(binance_futures),
        MIN_PRICE_USDT, MAX_PRICE_USDT,
        in_range, len(COMMON_SYMBOLS)
    )


def select_candle_candidates():
    selected = list(COMMON_SYMBOLS.keys())[:MAX_CANDLE_CANDIDATES]
    STATS["bitget_candidates"] = 0  # обнуляем на каждом цикле
    return selected


async def scan_cycle():
    if not COMMON_SYMBOLS:
        return

    candidates = select_candle_candidates()
    semaphore = asyncio.Semaphore(3)  # Снижен семафор для плавности сетевых запросов

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.1)
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

    bitget = BITGET_DATA
    binance_futures = BINANCE_FUTURES_DATA

    def vol_sum_bitget(data):
        return sum(num(v.get("volume24")) for v in data.values())

    log.info(
        "HOURLY | BG=%d BNF=%d common=%d | BG scans=%d BG cand=%d BN conf=%d | "
        "signals=%d | BG req=%d/%d BN req=%d/%d",
        len(bitget), len(binance_futures), len(COMMON_SYMBOLS),
        STATS["bitget_scans"], STATS["bitget_candidates"],
        STATS["binance_confirmations"], STATS["signals"],
        STATS["bitget_requests"], STATS["bitget_errors"],
        STATS["binance_requests"], STATS["binance_errors"],
    )

    msg = (
        "🩺 <b>ЧАСОВАЯ ДИАГНОСТИКА v3.3 (24h RVOL)</b>\n\n"
        f"<b>Тикеры</b>\n"
        f"Bitget: {len(bitget)} | 24h vol≈${vol_sum_bitget(bitget)/1e6:.1f}M\n"
        f"Binance Futures: {len(binance_futures)}\n"
        f"Common: <b>{len(COMMON_SYMBOLS)}</b>\n\n"
        f"<b>Воронка сигналов</b>\n"
        f"Bitget сканирований: {STATS['bitget_scans']}\n"
        f"Bitget кандидатов: {STATS['bitget_candidates']}\n"
        f"Binance подтверждений: {STATS['binance_confirmations']}\n"
        f"Сигналов: <b>{STATS['signals']}</b>\n\n"
        f"<b>Отсевы</b>\n"
        f"BG RVOL: {STATS['rejected_bitget_rvol']}\n"
        f"BG move: {STATS['rejected_bitget_move']}\n"
        f"BG structure: {STATS['rejected_bitget_structure']}\n"
        f"BN RVOL: {STATS['rejected_binance_rvol']}\n"
        f"BN move: {STATS['rejected_binance_move']}\n"
        f"BN lead: {STATS['rejected_binance_lead']}\n"
        f"OI: {STATS['rejected_oi']}\n\n"
        f"<b>HTTP</b>\n"
        f"Bitget: {STATS['bitget_requests']} ({STATS['bitget_errors']} ош)\n"
        f"Binance: {STATS['binance_requests']} ({STATS['binance_errors']} ош)"
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
CEX FUTURES AGGREGATOR v3.3 (24h RVOL)
=====================================

Uptime: {format_uptime()}

PRICE FILTER
------------
Range: {MIN_PRICE_USDT} — {MAX_PRICE_USDT} USDT
Coins in range: {STATS.get("price_in_range", 0)}

MARKET
------
Common symbols: {STATS["common_symbols"]}
Binance Futures symbols: {len(BINANCE_FUTURES_SYMBOLS)}

SCANNER FUNNEL
--------------
Bitget scans:        {STATS["bitget_scans"]}
Bitget candidates:   {STATS["bitget_candidates"]}
Binance confirms:    {STATS["binance_confirmations"]}
Signals:             {STATS["signals"]}
Distribution:        {STATS["distribution_signals"]}

REJECTED
--------
BG RVOL:      {STATS["rejected_bitget_rvol"]}
BG move:      {STATS["rejected_bitget_move"]}
BG structure: {STATS["rejected_bitget_structure"]}
BN RVOL:      {STATS["rejected_binance_rvol"]}
BN move:      {STATS["rejected_binance_move"]}
BN lead:      {STATS["rejected_binance_lead"]}
OI:           {STATS["rejected_oi"]}

OI
--
BG from API:    {STATS["bg_oi_from_api"]}
BG from ticker: {STATS["bg_oi_from_ticker"]}
BN OK:          {STATS["bn_oi_ok"]}
BN Fail:        {STATS["bn_oi_fail"]}

HTTP
----
Bitget:  {STATS["bitget_requests"]} (errors {STATS["bitget_errors"]})
Binance: {STATS["binance_requests"]} (errors {STATS["binance_errors"]})
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

    bn_status = "⚠️ не проверено"

    log.info("=" * 60)
    log.info("BINANCE FUTURES TEST (premiumIndex)")
    log.info("-" * 60)

    try:
        url = f"{BINANCE_FUTURES_BASE}/fapi/v1/premiumIndex"

        async with SESSION.get(
            url, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            log.info("premiumIndex HTTP status: %d", resp.status)

            if resp.status == 200:
                data = await resp.json()
                count = len(data) if isinstance(data, list) else 0
                bn_status = f"✅ OK ({count} символов)"
                log.info("premiumIndex rows: %d", count)
            elif resp.status == 418:
                bn_status = "❌ 418: временный бан"
            else:
                bn_status = f"❌ HTTP {resp.status}"

    except Exception as e:
        bn_status = f"❌ {e}"
        log.error("Binance test error: %s", e)

    log.info("BINANCE RESULT: %s", bn_status)
    log.info("=" * 60)

    startup_msg = (
        "🚀 <b>CEX FUTURES AGGREGATOR v3.3 ЗАПУЩЕН</b>\n\n"
        "• <b>Bitget = сканер</b> (лидер)\n"
        "• <b>Binance = подтверждение</b>\n"
        f"• Binance API: {bn_status}\n"
        f"• База RVOL: <b>24 часа (288 свечей)</b>\n"
        f"• Ценовой фильтр: {MIN_PRICE_USDT}–{MAX_PRICE_USDT} USDT\n\n"
        "📊 Логика:\n"
        "1. Bitget ищет импульс (24h RVOL + движение)\n"
        "2. Binance подтверждает (24h RVOL + OI + спред [-0.2%..+3.0%])\n"
        "3. Сигнал подается исключительно при двойном подтверждении.\n\n"
        "Статус: поиск аномалий запущен."
    )

    tg_task = asyncio.create_task(send_telegram(startup_msg))
    BACKGROUND_TASKS.add(tg_task)
    tg_task.add_done_callback(BACKGROUND_TASKS.discard)

    log.info("CEX FUTURES AGGREGATOR v3.3 STARTED")


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
