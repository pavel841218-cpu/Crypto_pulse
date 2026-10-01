import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque

# ============================================================
# QUIET INFLOW SCREENER v12.0
# Тихие свечи + рост OI (подготовка к пампу)
# Биржи: KuCoin (приоритет), Bitget, Bybit
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Основные настройки ---
UNIVERSE_REFRESH_SEC = 300
SCAN_INTERVAL_SEC = 60

MAX_UNIVERSE_SYMBOLS = 180
MAX_SCAN_CANDIDATES = 60

MIN_24H_VOLUME_USDT = 400_000
MIN_PRICE_USDT = 0.0005
MAX_PRICE_USDT = 5.0

# --- Тихое влитие ---
OI_LOOKBACK_SNAPSHOTS = 60          # \~60 минут
QUIET_MAX_PRICE_MOVE_PCT = 2.0      # цена почти не выросла
QUIET_MIN_OI_GROWTH_PCT = 5.5       # мин. рост OI на KuCoin
QUIET_MIN_LEAD_PCT = 4.0            # KuCoin обгоняет среднее
QUIET_COOLDOWN_SEC = 4 * 3600

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("QuietInflow")

SESSION = None
UNIVERSE = {}
LAST_SIGNAL = {}
OI_SNAPSHOTS = defaultdict(lambda: deque(maxlen=200))

STATS = {
    "scans": 0,
    "quiet_signals": 0,
}


async def http_get(url, params=None, timeout=8):
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 429:
                await asyncio.sleep(1.5)
                return None
            if resp.status >= 400:
                return None
            return await resp.json(content_type=None)
    except Exception:
        return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def norm(symbol):
    if not symbol:
        return ""
    s = str(symbol).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
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
        }
    return result


async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if data and data.get("code") == "00000" and isinstance(data.get("data"), list):
        for row in data["data"]:
            symbol = str(row.get("symbol", ""))
            base = norm(symbol)
            if base:
                result[base] = True
    return result


async def fetch_kucoin_candles(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": symbol, "granularity": "5"})
    if not data or not isinstance(data.get("data"), list):
        return []
    candles = []
    for row in data["data"]:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts = int(row[0]) * 1000
            o, h, l, c, v = num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_oi(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if not data or not isinstance(data.get("data"), dict):
        return 0.0
    return num(data["data"].get("openInterest"))


async def fetch_bitget_oi(base):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": f"{base}USDT", "productType": "USDT-FUTURES"
    })
    if data and data.get("code") == "00000":
        raw = data.get("data", {})
        if isinstance(raw, dict) and "list" in raw and raw["list"]:
            row = raw["list"][0]
        else:
            row = raw if isinstance(raw, dict) else {}
        return num(row.get("amount") or row.get("openInterest"))
    return 0.0


async def fetch_bybit_oi(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/open-interest", {
        "category": "linear",
        "symbol": f"{base}USDT",
        "intervalTime": "5min",
        "limit": 1
    })
    if data and data.get("retCode") == 0:
        items = data.get("result", {}).get("list", [])
        if items:
            return num(items[0].get("openInterest"))
    return 0.0


async def collect_all_oi(base):
    item = UNIVERSE.get(base)
    kc_sym = item["kucoin_symbol"] if item else f"{base}USDTM"

    results = await asyncio.gather(
        fetch_kucoin_oi(kc_sym),
        fetch_bitget_oi(base),
        fetch_bybit_oi(base),
    )
    snap = {
        "kucoin": results[0],
        "bitget": results[1],
        "bybit": results[2],
    }
    OI_SNAPSHOTS[base].append((time.time(), snap.copy()))
    return snap


def calculate_oi_deltas(base):
    snaps = OI_SNAPSHOTS[base]
    if len(snaps) < 3:
        return {"kucoin": 0.0, "bitget": 0.0, "bybit": 0.0}

    lookback = min(OI_LOOKBACK_SNAPSHOTS, len(snaps))
    old_ts, old_snap = snaps[-lookback]
    curr_ts, curr_snap = snaps[-1]

    def pct(curr, old):
        if old <= 0:
            return 0.0
        return ((curr - old) / old) * 100.0

    return {
        "kucoin": pct(curr_snap["kucoin"], old_snap["kucoin"]),
        "bitget": pct(curr_snap["bitget"], old_snap["bitget"]),
        "bybit": pct(curr_snap["bybit"], old_snap["bybit"]),
    }


def detect_quiet_inflow(base, price_move_pct):
    deltas = calculate_oi_deltas(base)

    kc_delta = deltas.get("kucoin", 0.0)
    bg_delta = deltas.get("bitget", 0.0)
    bb_delta = deltas.get("bybit", 0.0)

    others = []
    if abs(bg_delta) > 0.01:
        others.append(bg_delta)
    if abs(bb_delta) > 0.01:
        others.append(bb_delta)

    avg_others = sum(others) / len(others) if others else 0.0
    lead = kc_delta - avg_others

    is_quiet = (
        abs(price_move_pct) <= QUIET_MAX_PRICE_MOVE_PCT and
        kc_delta >= QUIET_MIN_OI_GROWTH_PCT and
        lead >= QUIET_MIN_LEAD_PCT
    )

    return {
        "is_quiet": is_quiet,
        "kucoin_oi_delta": round(kc_delta, 2),
        "bitget_oi_delta": round(bg_delta, 2),
        "bybit_oi_delta": round(bb_delta, 2),
        "avg_others": round(avg_others, 2),
        "lead": round(lead, 2),
        "price_move": round(price_move_pct, 2),
    }


async def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with SESSION.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception:
        return False


async def send_quiet_signal(base, quiet_info):
    msg = (
        f"🟡 <b>ТИХОЕ ВЛИТИЕ — {base}USDT</b>\n\n"
        f"Цена почти не изменилась: <b>{quiet_info['price_move']:+.2f}%</b>\n"
        f"KuCoin OI: <b>+{quiet_info['kucoin_oi_delta']}%</b>\n"
        f"Bitget OI: {quiet_info['bitget_oi_delta']:+.2f}%\n"
        f"Bybit OI: {quiet_info['bybit_oi_delta']:+.2f}%\n"
        f"Преимущество KuCoin: <b>+{quiet_info['lead']}%</b>\n\n"
        f"💡 Подготовка к пампу (накопление без движения цены)."
    )
    await send_telegram(msg)
    STATS["quiet_signals"] = STATS.get("quiet_signals", 0) + 1


async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bitget = await fetch_bitget_tickers()
    if not kc or not bitget:
        return

    bitget_set = set(bitget.keys())
    universe = {}

    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bitget_set:
            continue

        universe[base] = {
            "kucoin_symbol": info["symbol"],
            "price": info["price"],
            "volume24": info["volume24"],
        }

    sorted_u = sorted(universe.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс обновлён: %d пар", len(UNIVERSE))


async def scan_cycle():
    if not UNIVERSE:
        return

    STATS["scans"] += 1
    candidates = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(4)

    async def scan_one(base):
        async with semaphore:
            item = UNIVERSE.get(base)
            if not item:
                return

            candles = await fetch_kucoin_candles(item["kucoin_symbol"])
            if len(candles) < 8:
                return

            # Изменение цены за последние \~30 минут
            price_move = ((candles[-1]["close"] / candles[-7]["close"]) - 1) * 100

            await collect_all_oi(base)
            quiet = detect_quiet_inflow(base, price_move)

            if quiet["is_quiet"]:
                if time.time() - LAST_SIGNAL.get(base, 0) > QUIET_COOLDOWN_SEC:
                    LAST_SIGNAL[base] = time.time()
                    await send_quiet_signal(base, quiet)

    await asyncio.gather(*[scan_one(b) for b in candidates])


async def scanner_loop():
    last_universe = 0
    while True:
        try:
            now = time.time()
            if now - last_universe >= UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last_universe = now

            if UNIVERSE:
                await scan_cycle()

            await asyncio.sleep(SCAN_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(15)


async def index(request):
    return web.Response(
        text=f"Quiet Inflow v12.0 OK | Universe: {len(UNIVERSE)} | Signals: {STATS.get('quiet_signals', 0)}"
    )


async def start_background(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    app["scanner_task"] = asyncio.create_task(scanner_loop())
    await send_telegram("🟡 <b>Quiet Inflow Screener v12.0</b>\nРежим: только тихое влитие")


async def cleanup(app):
    t = app.get("scanner_task")
    if t:
        t.cancel()
        await t
    if SESSION:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start_background)
app.on_cleanup.append(cleanup)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
