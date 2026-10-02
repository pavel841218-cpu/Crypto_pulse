import asyncio
import aiohttp
from aiohttp import web
import os
import time
import json
import gc
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v11.0 — Score Edition (Fixed)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "90"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "900"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))
PERSIST_INTERVAL_SEC = int(os.environ.get("PERSIST_INTERVAL_SEC", "300"))
PERSIST_PATH = os.environ.get("PERSIST_PATH", "/tmp/pump_hunter_state.json")

# --- Юниверс ---
MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "250"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "250"))
MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "80000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# --- Таймфреймы для детекции пампа: 15м ("impulse") и 1ч ("grind") ---
TIMEFRAMES = {
    "impulse": {
        "granularity_min": 15,
        "lookback_candles": int(os.environ.get("IMPULSE_LOOKBACK_CANDLES", "2")),    # 30 мин
        "base_candles": int(os.environ.get("IMPULSE_BASE_CANDLES", "96")),           # 24ч база
        "min_pct": float(os.environ.get("IMPULSE_MIN_PCT", "4.0")),
        "min_rvol": float(os.environ.get("IMPULSE_MIN_RVOL", "2.0")),
        "oi_window_hours": float(os.environ.get("IMPULSE_OI_WINDOW_HOURS", "1.5")),
        "fetch_limit": 120,
        "emoji": "⚡",
        "label": "ИМПУЛЬС 15м",
    },
    "grind": {
        "granularity_min": 60,
        "lookback_candles": int(os.environ.get("GRIND_LOOKBACK_CANDLES", "12")),     # 12ч
        "base_candles": int(os.environ.get("GRIND_BASE_CANDLES", "72")),             # 72ч база
        "min_pct": float(os.environ.get("GRIND_MIN_PCT", "14.0")),
        "min_rvol": float(os.environ.get("GRIND_MIN_RVOL", "1.8")),
        "oi_window_hours": float(os.environ.get("GRIND_OI_WINDOW_HOURS", "12.0")),
        "fetch_limit": 100,
        "emoji": "📈",
        "label": "РАЗГОН 1ч",
    },
}
BREAKOUT_TOLERANCE = float(os.environ.get("BREAKOUT_TOLERANCE", "0.985"))
CONFIRM_PCT_RATIO = float(os.environ.get("CONFIRM_PCT_RATIO", "0.5"))
MIN_CONFIRMATIONS = int(os.environ.get("MIN_CONFIRMATIONS", "1"))

# --- Score ---
SCORE_WATCH_THRESHOLD = float(os.environ.get("SCORE_WATCH_THRESHOLD", "35"))
SCORE_SIGNAL_THRESHOLD = float(os.environ.get("SCORE_SIGNAL_THRESHOLD", "55"))
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "8.0"))
FUNDING_EXTREME_NEG = float(os.environ.get("FUNDING_EXTREME_NEG", "-0.0005"))   # -0.05% за интервал
FUNDING_MILD_NEG = float(os.environ.get("FUNDING_MILD_NEG", "-0.0002"))

# --- Накопление ---
ACCUM_ENABLED = os.environ.get("ACCUM_ENABLED", "true").lower() == "true"
ACCUM_WINDOW_HOURS = int(os.environ.get("ACCUM_WINDOW_HOURS", "3"))
ACCUM_MAX_PRICE_MOVE_PCT = float(os.environ.get("ACCUM_MAX_PRICE_MOVE_PCT", "6.0"))
ACCUM_MIN_OI_GROWTH_PCT = float(os.environ.get("ACCUM_MIN_OI_GROWTH_PCT", "15.0"))
ACCUM_MIN_SOURCES = int(os.environ.get("ACCUM_MIN_SOURCES", "2"))
ACCUM_MIN_EFFICIENCY_INVERSE = float(os.environ.get("ACCUM_MAX_EFFICIENCY", "0.35"))
ACCUM_COOLDOWN_SEC = int(os.environ.get("ACCUM_COOLDOWN_SEC", str(2 * 3600)))

# --- Кулдауны ---
COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(6 * 3600)))
REJECT_COOLDOWN_SEC = int(os.environ.get("REJECT_COOLDOWN_SEC", str(20 * 60)))

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v11")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
LAST_REJECT = {}
ACCUM_LAST_SIGNAL = {}

OI_HIST_MAXLEN = int(14 * 3600 / OI_SAMPLE_INTERVAL_SEC) + 10
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0,
    "oi_samples": 0,
    "triggers": 0,
    "signals": 0,
    "watch_signals": 0,
    "accum_alerts": 0,
    "rejected_no_breakout": 0,
    "rejected_no_confirm": 0,
    "rejected_low_score": 0,
    "state_loaded": False,
    "state_saved_count": 0,
}

# ============================================================
# HTTP UTILS
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None
    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status == 429:
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.4)
        except Exception:
            break
    return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def norm(s):
    if not s:
        return ""
    s = str(s).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

# ============================================================
# ПЕРСИСТЕНТНОСТЬ
# ============================================================

def _sync_save(data, path):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f)
    os.replace(tmp_path, path)


async def persist_loop():
    while True:
        try:
            await asyncio.sleep(PERSIST_INTERVAL_SEC)
            data = {
                "oi_history": {
                    base: {exch: list(dq) for exch, dq in exch_map.items()}
                    for base, exch_map in OI_HISTORY.items()
                },
                "last_signal": LAST_SIGNAL,
                "accum_last_signal": ACCUM_LAST_SIGNAL,
                "saved_at": time.time(),
            }
            await asyncio.to_thread(_sync_save, data, PERSIST_PATH)
            STATS["state_saved_count"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("persist_loop error")


def load_state():
    if not os.path.exists(PERSIST_PATH):
        return
    try:
        with open(PERSIST_PATH, "r") as f:
            data = json.load(f)
        for base, exch_map in data.get("oi_history", {}).items():
            for exch, points in exch_map.items():
                dq = OI_HISTORY[base][exch]
                for item in points:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        dq.append((float(item[0]), float(item[1])))
        LAST_SIGNAL.update(data.get("last_signal", {}))
        ACCUM_LAST_SIGNAL.update(data.get("accum_last_signal", {}))
        age_min = (time.time() - data.get("saved_at", time.time())) / 60
        log.info("💾 Состояние загружено (сохранено %.0f мин назад): %d монет с историей OI",
                 age_min, len(data.get("oi_history", {})))
        STATS["state_loaded"] = True
    except Exception:
        log.exception("Не удалось загрузить состояние — стартуем с чистого листа")

# ============================================================
# FETCHERS — Universe
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base:
            continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
            "change24": num(row.get("priceChgPct")) * 100,
            "first_open_ms": num(row.get("firstOpenDate")),
        }
    return result


async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base:
                result[base] = {"symbol": symbol}
    return result

# ============================================================
# FETCHERS — candles
# ============================================================

def _parse_list_candles(rows, ts_ms=True):
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                if ts_ms and ts < 10 ** 12:
                    ts *= 1000
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_candles(symbol, granularity_min, limit):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - limit * granularity_min * 60 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": str(granularity_min), "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])


BITGET_GRANULARITY = {15: "15m", 60: "1H"}


async def fetch_bitget_candles(symbol, granularity_min, limit):
    gran = BITGET_GRANULARITY.get(granularity_min, "1H")
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES", "granularity": gran, "limit": str(limit),
    })
    if not data or data.get("code") != "00000":
        return []
    return _parse_list_candles(data.get("data", []), ts_ms=False)


async def fetch_bybit_candles(base, granularity_min, limit):
    interval = "60" if granularity_min == 60 else str(granularity_min)
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": interval, "limit": limit,
    })
    if not data or data.get("retCode") != 0:
        return []
    result = data.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("list"), list):
        return []
    return _parse_list_candles(result["list"], ts_ms=False)

# ============================================================
# FETCHERS — OI
# ============================================================

async def fetch_kucoin_oi(symbol):
    if not symbol:
        return 0.0
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict):
        return num(data["data"].get("openInterest"))
    return 0.0


async def fetch_bitget_oi(symbol):
    if not symbol:
        return 0.0
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "USDT-FUTURES",
    })
    if not data or data.get("code") != "00000":
        return 0.0
    raw = data.get("data")
    if not isinstance(raw, (dict, list)):
        return 0.0
    row = {}
    if isinstance(raw, dict):
        items = raw.get("openList") or raw.get("openInterestList") or raw.get("list")
        row = items[0] if isinstance(items, list) and items else raw
    elif isinstance(raw, list) and raw:
        row = raw[0]
    return num(row.get("openInterest") or row.get("amount") or row.get("size") or row.get("openInterestUsd"))


async def fetch_bybit_oi(base):
    # Используем эндпоинт tickers, так как он более стабилен и отдаёт свежий OI
    data = await http_get(f"{BYBIT_BASE}/v5/market/tickers", {
        "category": "linear", "symbol": f"{base}USDT"
    })
    if not data or data.get("retCode") != 0:
        return 0.0
    result = data.get("result")
    if not isinstance(result, dict):
        return 0.0
    items = result.get("list")
    if not isinstance(items, list) or not items:
        return 0.0
    return num(items[0].get("openInterest"))

# ============================================================
# FETCHERS — Funding rate
# ============================================================

async def fetch_kucoin_funding(symbol):
    if not symbol:
        return None
    data = await http_get(f"{KUCOIN_BASE}/api/v1/funding-rate/{symbol}/current")
    if data and isinstance(data.get("data"), dict):
        val = data["data"].get("value")
        return num(val, None) if val is not None else None
    return None


async def fetch_bybit_funding(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/tickers", {"category": "linear", "symbol": f"{base}USDT"})
    if not data or data.get("retCode") != 0:
        return None
    result = data.get("result")
    if not isinstance(result, dict):
        return None
    items = result.get("list")
    if not isinstance(items, list) or not items:
        return None
    val = items[0].get("fundingRate")
    return num(val, None) if val is not None else None


async def fetch_best_funding(base, kucoin_symbol):
    kc = await fetch_kucoin_funding(kucoin_symbol)
    if kc is not None:
        return kc, "kucoin"
    bb = await fetch_bybit_funding(base)
    if bb is not None:
        return bb, "bybit"
    return None, None

# ============================================================
# МЕТРИКИ И SCORE
# ============================================================

def efficiency_ratio(candles, idx_now, window):
    """Коэффициент Кауфмана: чистый вектор перемещения / полная длина движения (High-Low)."""
    start = max(0, idx_now - window + 1)
    slice_c = candles[start:idx_now + 1]
    if len(slice_c) < 3:
        return 0.0
    net = abs(slice_c[-1]["close"] - slice_c[0]["close"])
    path = sum(c["high"] - c["low"] for c in slice_c)
    return net / path if path > 0 else 0.0


def calc_tf_metrics(candles, tf_cfg):
    lookback = tf_cfg["lookback_candles"]
    base_n = tf_cfg["base_candles"]
    needed = lookback + base_n + 2
    if len(candles) < needed:
        return None

    idx_now = len(candles) - 2
    idx_past = idx_now - lookback
    if idx_past < 0:
        return None

    close_now = candles[idx_now]["close"]
    close_past = candles[idx_past]["close"]
    if close_past <= 0:
        return None
    pct = ((close_now / close_past) - 1) * 100

    base_start = idx_past - base_n
    base_slice = candles[max(0, base_start):idx_past]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high * BREAKOUT_TOLERANCE

    vol_now_usd = candles[idx_now]["volume"] * candles[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = vol_now_usd / avg_vol if avg_vol > 0 else 0

    eff = efficiency_ratio(candles, idx_now, lookback)

    return {
        "close": close_now,
        "pct": pct,
        "breakout": breakout,
        "base_high": base_high,
        "rvol": rvol,
        "volume_usd": vol_now_usd,
        "efficiency": eff,
    }


def oi_growth_pct(base, exchange, window_hours):
    hist = OI_HISTORY[base][exchange]
    if len(hist) < 2:
        return None
    now_ts, now_oi = hist[-1]
    min_span = max(OI_SAMPLE_INTERVAL_SEC * 1.5, min(window_hours * 3600 * 0.3, OI_SAMPLE_INTERVAL_SEC * 6))
    if now_ts - hist[0][0] < min_span:
        return None
    target_ts = time.time() - window_hours * 3600
    past = None
    for ts, val in hist:
        if ts <= target_ts:
            past = (ts, val)
        else:
            break
    if past is None:
        past = hist[0]
    past_ts, past_oi = past
    if past_oi <= 0:
        return None
    return ((now_oi / past_oi) - 1) * 100


def score_signal(tf_key, kc, oi_deltas, funding_rate):
    tf_cfg = TIMEFRAMES[tf_key]
    momentum_score = min(40.0, (kc["pct"] / tf_cfg["min_pct"]) * 20.0) if tf_cfg["min_pct"] > 0 else 0.0
    volume_score = min(25.0, (kc["rvol"] / tf_cfg["min_rvol"]) * 12.0) if tf_cfg["min_rvol"] > 0 else 0.0

    best_oi_delta = 0.0
    is_squeeze = False
    for d in oi_deltas.values():
        if d is None:
            continue
        if abs(d) > abs(best_oi_delta):
            best_oi_delta = d
    oi_score = min(20.0, (abs(best_oi_delta) / OI_MIN_GROWTH_PCT) * 10.0) if OI_MIN_GROWTH_PCT > 0 else 0.0
    if best_oi_delta < 0 and abs(best_oi_delta) >= OI_MIN_GROWTH_PCT:
        is_squeeze = True

    funding_bonus = 0.0
    if funding_rate is not None:
        if funding_rate <= FUNDING_EXTREME_NEG:
            funding_bonus = 10.0
        elif funding_rate <= FUNDING_MILD_NEG:
            funding_bonus = 5.0

    chop_penalty = (1.0 - kc["efficiency"]) * 15.0

    score = momentum_score + volume_score + oi_score + funding_bonus - chop_penalty
    breakdown = {
        "momentum": momentum_score,
        "volume": volume_score,
        "oi": oi_score,
        "funding": funding_bonus,
        "chop_penalty": -chop_penalty,
    }
    return score, breakdown, best_oi_delta, is_squeeze


def calc_accumulation(base, candles_1h):
    window = ACCUM_WINDOW_HOURS
    needed = window + 2
    if len(candles_1h) < needed:
        return None
    idx_now = len(candles_1h) - 2
    idx_past = idx_now - window
    if idx_past < 0:
        return None

    start_close = candles_1h[idx_past]["close"]
    now_close = candles_1h[idx_now]["close"]
    if start_close <= 0:
        return None
    price_change_pct = abs((now_close - start_close) / start_close) * 100
    if price_change_pct > ACCUM_MAX_PRICE_MOVE_PCT:
        return None

    eff = efficiency_ratio(candles_1h, idx_now, window)
    if eff > ACCUM_MIN_EFFICIENCY_INVERSE:
        return None

    oi_deltas = {}
    growing_sources = 0
    max_delta = 0.0
    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch, window)
        oi_deltas[exch] = d if d is not None else 0.0
        if d is not None and d >= ACCUM_MIN_OI_GROWTH_PCT:
            growing_sources += 1
            max_delta = max(max_delta, d)

    if growing_sources < ACCUM_MIN_SOURCES:
        return None

    return {
        "price_change_pct": price_change_pct,
        "efficiency": eff,
        "oi_deltas": oi_deltas,
        "max_delta": max_delta,
        "close": now_close,
    }

# ============================================================
# TELEGRAM
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return False
            payload = await r.json(content_type=None)
            return payload.get("ok", False)
    except Exception:
        return False


async def send_accumulation_alert(base, acc):
    ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"🎯 <b>НАКОПЛЕНИЕ: {ticker}</b>\n\n"
        f"Цена изменилась на {acc['price_change_pct']:.1f}% за {ACCUM_WINDOW_HOURS}ч "
        f"(efficiency ratio {acc['efficiency']:.2f} — чистый шум, не тренд),\n"
        f"но OI вырос до +{acc['max_delta']:.1f}% на части бирж.\n\n"
        f"<b>📊 OI Δ ({ACCUM_WINDOW_HOURS}ч)</b>\n"
        f"  KuCoin: {acc['oi_deltas'].get('kucoin', 0):+.1f}%\n"
        f"  Bitget: {acc['oi_deltas'].get('bitget', 0):+.1f}%\n"
        f"  Bybit:  {acc['oi_deltas'].get('bybit', 0):+.1f}%\n\n"
        f"Текущая цена: <b>{acc['close']:.6g}</b>\n\n"
        f"🔍 Единственная стадия, которая приходит ДО движения цены — это и есть точка входа."
    )
    sent = await send_tg(msg)
    if sent:
        STATS["accum_alerts"] += 1
    return sent


async def send_pump_signal(base, tf_key, kc, bg, bb, oi_deltas, change_24h, score, breakdown, is_squeeze, funding_rate, is_watch):
    tf_cfg = TIMEFRAMES[tf_key]
    ticker = f"<code>{escape(base)}USDT</code>"
    tier_emoji = "👀" if is_watch else tf_cfg["emoji"]
    tier_label = "НАБЛЮДЕНИЕ" if is_watch else tf_cfg["label"]

    conf_lines = []
    if bg:
        conf_lines.append(f"  Bitget: +{bg['pct']:.1f}% | RVOL {bg['rvol']:.1f}x")
    if bb:
        conf_lines.append(f"  Bybit:  +{bb['pct']:.1f}% | RVOL {bb['rvol']:.1f}x")

    invalidation = kc["base_high"] * BREAKOUT_TOLERANCE

    def _oi_line(name, delta):
        if delta is None:
            return f"  {name}: нет данных (разогрев истории)"
        if delta <= -OI_MIN_GROWTH_PCT:
            return f"  {name}: {delta:+.1f}% (шорт-сквиз)"
        return f"  {name}: {delta:+.1f}%"

    funding_line = ""
    if funding_rate is not None:
        funding_line = f"\nFunding: {funding_rate * 100:+.3f}%" + (" ⚠️ экстремальный" if funding_rate <= FUNDING_EXTREME_NEG else "")

    msg = (
        f"{tier_emoji} <b>{tier_label}: {ticker}</b>  [score {score:.0f}]\n\n"
        f"<b>🎯 KuCoin</b>\n"
        f"  Рост: <b>+{kc['pct']:.1f}%</b>\n"
        f"  Пробой базы: <b>{kc['base_high']:.6g} → {kc['close']:.6g}</b>\n"
        f"  RVOL: <b>{kc['rvol']:.2f}x</b> | Efficiency: {kc['efficiency']:.2f}\n\n"
        f"<b>✅ Подтверждение</b>\n" + ("\n".join(conf_lines) if conf_lines else "  нет данных") + "\n\n"
        f"<b>📊 OI Δ</b>\n"
        f"{_oi_line('KuCoin', oi_deltas.get('kucoin', 0))}\n"
        f"{_oi_line('Bitget', oi_deltas.get('bitget', 0))}\n"
        f"{_oi_line('Bybit', oi_deltas.get('bybit', 0))}"
        f"{funding_line}\n\n"
        f"<b>⛔ Инвалидация:</b> закрытие ниже {invalidation:.6g}\n"
        f"<b>24h change:</b> {change_24h:+.1f}%\n\n"
        f"<i>Score: моментум {breakdown['momentum']:.0f} + объём {breakdown['volume']:.0f} + "
        f"OI {breakdown['oi']:.0f} + funding {breakdown['funding']:.0f} {breakdown['chop_penalty']:+.0f} (шум)</i>"
    )
    if is_squeeze:
        msg += "\n⚠️ Часть роста OI отрицательна — похоже на шорт-сквиз, а не только новые лонги."
    if kc["rvol"] >= 8.0:
        msg += "\n⚠️ Аномально высокий RVOL — риск резкого разворота (памп-и-дамп)."

    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1
        if is_watch:
            STATS["watch_signals"] += 1
    return sent


async def send_status_message():
    uptime_hours = (time.time() - START_TIME) / 3600
    active_cooldowns = sum(1 for ts in LAST_SIGNAL.values() if time.time() - ts < COOLDOWN_SEC)
    active_accum = sum(1 for ts in ACCUM_LAST_SIGNAL.values() if time.time() - ts < ACCUM_COOLDOWN_SEC)
    msg = (
        f"📊 <b>Статус PUMP-HUNTER v11 (Score Edition)</b>\n\n"
        f"⏱ Аптайм: {uptime_hours:.1f}ч | Состояние с диска: {'да' if STATS['state_loaded'] else 'нет (чистый старт)'}\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар\n"
        f"🔍 Сканов: {STATS['scans']} | OI-сэмплов: {STATS['oi_samples']}\n"
        f"🚨 Триггеров (прошли гейт пробоя+подтверждения): {STATS['triggers']}\n"
        f"✅ Сигналов: {STATS['signals']} (из них наблюдение: {STATS['watch_signals']})\n"
        f"🎯 Алертов накопления: {STATS['accum_alerts']}\n"
        f"🧊 На кулдауне: памп {active_cooldowns} | накопление {active_accum}\n\n"
        f"<b>Отклонено:</b>\n"
        f"  без пробоя: {STATS['rejected_no_breakout']}\n"
        f"  нет подтверждения: {STATS['rejected_no_confirm']}\n"
        f"  низкий score: {STATS['rejected_low_score']}\n"
        f"  сохранений состояния: {STATS['state_saved_count']}"
    )
    await send_tg(msg)


async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if CHAT_ID and chat_id != str(CHAT_ID):
        return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/start"):
        await send_tg("👋 <b>PUMP-HUNTER v11 (Score Edition) запущен.</b> /status — отчёт.")
    elif text.startswith("/status"):
        await send_status_message()


async def telegram_get_updates(offset):
    if SESSION is None or SESSION.closed or not BOT_TOKEN:
        return None
    try:
        params = {"timeout": 25}
        if offset is not None:
            params["offset"] = offset
        async with SESSION.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params=params, timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                return None
            return await r.json(content_type=None)
    except Exception:
        return None


async def telegram_poll_loop():
    if not BOT_TOKEN:
        return
    offset = None
    while True:
        try:
            data = await telegram_get_updates(offset)
            if data and data.get("ok"):
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await handle_telegram_update(update)
                    except Exception:
                        log.exception("Ошибка обработки команды TG")
            else:
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Telegram poll error")
            await asyncio.sleep(5)

# ============================================================
# PROCESS SYMBOL
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return
    now = time.time()
    if now - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
        return
    if now - LAST_REJECT.get(base, 0) < REJECT_COOLDOWN_SEC:
        return

    best = None  # (score, tf_key, kc, oi_deltas, bg, bb, is_squeeze, breakdown, funding_rate)

    for tf_key, tf_cfg in TIMEFRAMES.items():
        candles = await fetch_kucoin_candles(item["kucoin_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"])

        if tf_key == "grind" and ACCUM_ENABLED and now - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
            acc = calc_accumulation(base, candles)
            if acc:
                ACCUM_LAST_SIGNAL[base] = time.time()
                await send_accumulation_alert(base, acc)
                log.info("🎯 НАКОПЛЕНИЕ: %s | Δцена %.1f%% eff=%.2f", base, acc["price_change_pct"], acc["efficiency"])

        kc = calc_tf_metrics(candles, tf_cfg)
        if not kc:
            continue
        if kc["pct"] < tf_cfg["min_pct"] * 0.5:
            continue
        if not kc["breakout"]:
            STATS["rejected_no_breakout"] += 1
            continue

        bg_candles, bb_candles = await asyncio.gather(
            fetch_bitget_candles(item["bitget_symbol"], tf_cfg["granularity_min"], tf_cfg["fetch_limit"]),
            fetch_bybit_candles(base, tf_cfg["granularity_min"], tf_cfg["fetch_limit"]),
        )
        bg = calc_tf_metrics(bg_candles, tf_cfg)
        bb = calc_tf_metrics(bb_candles, tf_cfg)
        confirm_threshold = tf_cfg["min_pct"] * CONFIRM_PCT_RATIO
        confirmations = sum(1 for m in (bg, bb) if m and m["pct"] >= confirm_threshold)
        if confirmations < MIN_CONFIRMATIONS:
            STATS["rejected_no_confirm"] += 1
            LAST_REJECT[base] = time.time()
            continue

        STATS["triggers"] += 1

        oi_deltas = {exch: oi_growth_pct(base, exch, tf_cfg["oi_window_hours"]) for exch in ("kucoin", "bitget", "bybit")}
        funding_rate, _ = await fetch_best_funding(base, item["kucoin_symbol"])

        curr_score, breakdown, best_oi_delta, is_squeeze = score_signal(tf_key, kc, oi_deltas, funding_rate)

        if best is None or curr_score > best[0]:
            best = (curr_score, tf_key, kc, oi_deltas, bg, bb, is_squeeze, breakdown, funding_rate)

    if best is None:
        return

    score, tf_key, kc, oi_deltas, bg, bb, is_squeeze, breakdown, funding_rate = best

    if score < SCORE_WATCH_THRESHOLD:
        STATS["rejected_low_score"] += 1
        LAST_REJECT[base] = time.time()
        return

    is_watch = score < SCORE_SIGNAL_THRESHOLD
    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, tf_key, kc, bg, bb, oi_deltas, item.get("change24", 0),
                            score, breakdown, is_squeeze, funding_rate, is_watch)

# ============================================================
# BACKGROUND WORKERS
# ============================================================

async def sample_oi_for_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return
    now = time.time()
    kc_oi, bg_oi, bb_oi = await asyncio.gather(
        fetch_kucoin_oi(item["kucoin_symbol"]),
        fetch_bitget_oi(item["bitget_symbol"]),
        fetch_bybit_oi(base),
    )
    if kc_oi > 0:
        OI_HISTORY[base]["kucoin"].append((now, kc_oi))
    if bg_oi > 0:
        OI_HISTORY[base]["bitget"].append((now, bg_oi))
    if bb_oi > 0:
        OI_HISTORY[base]["bybit"].append((now, bb_oi))
    STATS["oi_samples"] += 1


async def oi_sample_cycle():
    if not UNIVERSE:
        return
    bases = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(10)

    async def worker(base):
        async with semaphore:
            try:
                await sample_oi_for_symbol(base)
            except Exception:
                pass

    await asyncio.gather(*[worker(b) for b in bases])
    gc.collect()


async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return
    bg_set = set(bg.keys())
    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        first_open = info.get("first_open_ms", 0)
        if first_open > 0 and (now_ms - first_open) < min_age_ms:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }
    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d пар", len(UNIVERSE))
    gc.collect()


async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(10)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(b) for b in cands])
    gc.collect()


async def universe_loop():
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            await refresh_universe()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(10)


async def oi_loop():
    while True:
        try:
            if UNIVERSE:
                await oi_sample_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(OI_SAMPLE_INTERVAL_SEC)


async def scan_loop():
    while True:
        try:
            if UNIVERSE:
                await scan_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)

# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=(f"PUMP-HUNTER v11 (Score) | Uni: {len(UNIVERSE)} | "
              f"Triggers: {STATS['triggers']} | Signals: {STATS['signals']} | Scans: {STATS['scans']}"),
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(10)

    load_state()

    if BOT_TOKEN:
        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "true"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                log.info("deleteWebhook: HTTP %s", r.status)
        except Exception as e:
            log.warning("deleteWebhook error: %s", e)

    await refresh_universe()

    app["universe_task"] = asyncio.create_task(universe_loop())
    app["oi_task"] = asyncio.create_task(oi_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())
    app["persist_task"] = asyncio.create_task(persist_loop())

    try:
        await send_tg(
            "🚀 <b>PUMP-HUNTER v11.0 (Score Edition) запущен.</b>\n\n"
            f"Score-пороги: watch ≥{SCORE_WATCH_THRESHOLD:.0f}, signal ≥{SCORE_SIGNAL_THRESHOLD:.0f}\n"
            f"Состояние с диска: {'загружено' if STATS['state_loaded'] else 'чистый старт'}"
        )
    except Exception as e:
        log.error("Ошибка при отправке приветствия в TG: %s", e)


async def stop(app):
    for key in ("universe_task", "oi_task", "scan_task", "poll_task", "persist_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    try:
        data = {
            "oi_history": {
                base: {exch: list(dq) for exch, dq in exch_map.items()}
                for base, exch_map in OI_HISTORY.items()
            },
            "last_signal": LAST_SIGNAL,
            "accum_last_signal": ACCUM_LAST_SIGNAL,
            "saved_at": time.time(),
        }
        _sync_save(data, PERSIST_PATH)
    except Exception:
        log.exception("Ошибка при финальном сохранении состояния")

    if SESSION and not SESSION.closed:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
