import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque

# ============================================================
# CEX SCREENER v8.0 — Тихое влитие (Multi-Exchange + Long Volume)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

CANDLE_REFRESH_SEC = 30
UNIVERSE_REFRESH_SEC = 300

MAX_UNIVERSE_SYMBOLS = 300
MAX_SCAN_CANDIDATES = 100

MIN_24H_VOLUME_USDT = 450_000
MIN_PRICE_USDT = 0.001
MAX_PRICE_USDT = 2.0

# === Тихое влитие ===
OI_LOOKBACK = 40                  # \~20 минут
QUIET_MAX_PRICE_MOVE = 1.9
QUIET_MIN_OI_GROWTH = 5.5
QUIET_MIN_LEAD = 4.0

QUIET_COOLDOWN_SEC = 3 * 3600
SIGNAL_COOLDOWN_SEC = 4 * 3600

# === Импульс ===
IMPULSE_RVOL = 2.0
IMPULSE_MOVE = 0.7

MIN_CANDLES = 18

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("QUIET-v8")

SESSION = None
UNIVERSE = {}
WATCHLIST = {}
LAST_QUIET = {}
LAST_SIGNAL = {}
OI_SNAPSHOTS = defaultdict(lambda: deque(maxlen=90))

STATS = {"scans": 0, "quiet": 0, "impulse": 0}

# ============================================================
# HTTP & Utils
# ============================================================

async def http_get(url, params=None, timeout=7):
    try:
        async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 429:
                await asyncio.sleep(1.0)
                return None
            if r.status >= 400:
                return None
            return await r.json()
    except Exception:
        return None

def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except:
        return default

def norm(s):
    if not s: return ""
    s = str(s).upper()
    if s.startswith("XBT"): s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s

# ============================================================
# FETCHERS
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if str(row.get("status", "")).lower() != "open": continue
        if str(row.get("settleCurrency", "")).upper() != "USDT": continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base: continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
        }
    return result

async def fetch_kucoin_candles(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {"symbol": symbol, "granularity": "5"})
    if not data or not isinstance(data.get("data"), list):
        return []
    candles = []
    for row in data["data"]:
        if not isinstance(row, list) or len(row) < 6: continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                # KuCoin не всегда отдаёт taker buy, используем направление свечи как proxy
                buy_ratio = 0.62 if c > o else 0.38
                candles.append({"ts": ts*1000, "open": o, "high": h, "low": l, "close": c, "volume": v, "buy_ratio": buy_ratio})
        except: continue
    candles.sort(key=lambda x: x["ts"])
    return candles

async def fetch_kucoin_oi(symbol):
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict):
        return num(data["data"].get("openInterest"))
    return 0.0

async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if not data or data.get("code") != "00000": return result
    for row in data.get("data", []):
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base: result[base] = {"symbol": symbol}
    return result

async def fetch_bitget_candles(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES", "granularity": "5m", "limit": "80"
    })
    if not data or data.get("code") != "00000": return []
    candles = []
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) < 6: continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                # Bitget иногда отдаёт base volume, используем направление
                buy_ratio = 0.60 if c > o else 0.40
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "buy_ratio": buy_ratio})
        except: continue
    candles.sort(key=lambda x: x["ts"])
    return candles

async def fetch_bitget_oi(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "USDT-FUTURES"
    })
    if data and data.get("code") == "00000":
        raw = data.get("data", {})
        row = raw.get("list", [{}])[0] if isinstance(raw, dict) and raw.get("list") else raw
        return num(row.get("amount") or row.get("openInterest") or row.get("openInterestUsd"))
    return 0.0

async def fetch_bybit_oi(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/open-interest", {
        "category": "linear", "symbol": f"{base}USDT", "intervalTime": "5min", "limit": 1
    })
    if data and data.get("retCode") == 0:
        items = data.get("result", {}).get("list", [])
        return num(items[0].get("openInterest")) if items else 0.0
    return 0.0

async def fetch_bybit_candles(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": "5", "limit": 80
    })
    if not data or data.get("retCode") != 0: return []
    candles = []
    for row in data.get("result", {}).get("list", []):
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                buy_ratio = 0.61 if c > o else 0.39
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "buy_ratio": buy_ratio})
        except: continue
    candles.sort(key=lambda x: x["ts"])
    return candles

# ============================================================
# METRICS
# ============================================================

def calc_metrics(candles):
    if len(candles) < MIN_CANDLES: return None
    c = candles[-2]
    prev = candles[:-2]
    if c["open"] <= 0: return None

    vol_usd = c["volume"] * c["close"]
    move = ((c["close"] / c["open"]) - 1) * 100
    buy_ratio = c.get("buy_ratio", 0.5)

    vols = [x["volume"] * x["close"] for x in prev[-18:] if x["volume"] > 0]
    if not vols: return None
    rvol = vol_usd / (sum(vols)/len(vols)) if vols else 0

    return {
        "move_pct": move,
        "rvol": rvol,
        "buy_ratio": buy_ratio,
        "volume_usd": vol_usd,
        "close": c["close"]
    }

async def collect_oi(base):
    item = UNIVERSE.get(base, {})
    kc_sym = item.get("kucoin_symbol", f"{base}USDTM")

    res = await asyncio.gather(
        fetch_kucoin_oi(kc_sym),
        fetch_bitget_oi(f"{base}USDT"),
        fetch_bybit_oi(base),
    )
    return {"kucoin": res[0], "bitget": res[1], "bybit": res[2]}

def calc_oi_deltas(base, current):
    now = time.time()
    snaps = OI_SNAPSHOTS[base]
    snaps.append((now, current.copy()))

    if len(snaps) < OI_LOOKBACK:
        return {k: 0.0 for k in current}

    _, old = snaps[-OI_LOOKBACK]
    deltas = {}
    for ex, cur in current.items():
        old_v = old.get(ex, 0.0)
        if cur > 0 and old_v > 0:
            deltas[ex] = ((cur / old_v) - 1) * 100
        else:
            deltas[ex] = 0.0
    return deltas

def detect_quiet(base, oi_now, price_move, buy_ratio):
    deltas = calc_oi_deltas(base, oi_now)

    # Ищем лидера по росту OI
    leader = max(deltas.items(), key=lambda x: x[1])
    leader_name, leader_delta = leader

    others = [v for k, v in deltas.items() if k != leader_name]
    avg_others = sum(others) / len(others) if others else 0.0
    lead = leader_delta - avg_others

    # Лонговый объём (лёгкий фильтр)
    long_ok = buy_ratio >= 0.55

    is_quiet = (
        abs(price_move) <= QUIET_MAX_PRICE_MOVE and
        leader_delta >= QUIET_MIN_OI_GROWTH and
        lead >= QUIET_MIN_LEAD and
        long_ok
    )

    return {
        "is_quiet": is_quiet,
        "leader": leader_name,
        "leader_oi": round(leader_delta, 2),
        "avg_others": round(avg_others, 2),
        "lead": round(lead, 2),
        "price_move": round(price_move, 2),
        "buy_ratio": round(buy_ratio, 2),
        "deltas": {k: round(v, 2) for k, v in deltas.items()}
    }

# ============================================================
# SIGNALS
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID: return
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            return r.status == 200
    except:
        return False

async def send_quiet_signal(base, quiet, hold_min):
    d = quiet["deltas"]
    msg = (
        f"🟡 <b>ТИХОЕ ВЛИТИЕ</b>\n"
        f"<b>{base}USDT</b>\n\n"
        f"🏆 Лидер: <b>{quiet['leader'].upper()}</b>\n"
        f"⏱ В отслеживании: {hold_min} мин\n\n"
        f"📊 Цена: <b>{quiet['price_move']:+.2f}%</b>\n"
        f"📈 {quiet['leader'].upper()} OI: <b>+{quiet['leader_oi']}%</b>\n"
        f"📉 Остальные: <b>{quiet['avg_others']:+.2f}%</b>\n"
        f"⚡ Преимущество: <b>+{quiet['lead']}%</b>\n"
        f"🟢 Лонг объём: <b>{quiet['buy_ratio']*100:.0f}%</b>\n\n"
        f"OI: KC {d.get('kucoin',0):+.1f}% | BG {d.get('bitget',0):+.1f}% | BB {d.get('bybit',0):+.1f}%"
    )
    await send_tg(msg)
    STATS["quiet"] += 1

async def send_impulse_signal(base, kc, hold_min):
    msg = (
        f"🚀 <b>ИМПУЛЬС</b>\n"
        f"<b>{base}USDT</b>\n\n"
        f"⏱ {hold_min} мин\n"
        f"Move: <b>+{kc['move_pct']:.2f}%</b>\n"
        f"RVOL: <b>{kc['rvol']:.2f}x</b>"
    )
    await send_tg(msg)
    STATS["impulse"] += 1

# ============================================================
# CORE
# ============================================================

def add_watch(base, source, metrics):
    now = time.time()
    if base in WATCHLIST:
        WATCHLIST[base]["exp"] = now + 10*3600
        return
    if len(WATCHLIST) >= 35:
        oldest = min(WATCHLIST.items(), key=lambda x: x[1]["added"])[0]
        WATCHLIST.pop(oldest, None)
    WATCHLIST[base] = {"added": now, "exp": now + 10*3600, "source": source}
    log.info("Кандидат %s | %s | RVOL %.2f", base, source, metrics["rvol"])

async def process(base):
    item = UNIVERSE.get(base)
    if not item: return

    # Свечи
    kc_c, bg_c, bb_c = await asyncio.gather(
        fetch_kucoin_candles(item["kucoin_symbol"]),
        fetch_bitget_candles(item["bitget_symbol"]),
        fetch_bybit_candles(base)
    )

    kc = calc_metrics(kc_c)
    bg = calc_metrics(bg_c)
    bb = calc_metrics(bb_c)
    if not kc: return

    # Берём лучший buy_ratio из доступных
    buy_ratio = max(
        kc.get("buy_ratio", 0.5),
        bg.get("buy_ratio", 0.5) if bg else 0.5,
        bb.get("buy_ratio", 0.5) if bb else 0.5
    )

    oi = await collect_oi(base)
    quiet = detect_quiet(base, oi, kc["move_pct"], buy_ratio)
    hold = int((time.time() - WATCHLIST.get(base, {}).get("added", time.time())) / 60)

    # Тихое влитие
    if quiet["is_quiet"]:
        if time.time() - LAST_QUIET.get(base, 0) > QUIET_COOLDOWN_SEC:
            LAST_QUIET[base] = time.time()
            await send_quiet_signal(base, quiet, hold)

    # Импульс
    if kc["rvol"] >= IMPULSE_RVOL and kc["move_pct"] >= IMPULSE_MOVE:
        if time.time() - LAST_SIGNAL.get(base, 0) > SIGNAL_COOLDOWN_SEC:
            LAST_SIGNAL[base] = time.time()
            await send_impulse_signal(base, kc, hold)
            WATCHLIST.pop(base, None)

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg: return

    bg_set = set(bg.keys())
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT: continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT): continue
        if base not in bg_set: continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"]
        }
    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("Юниверс: %d пар", len(UNIVERSE))

async def scan_cycle():
    if not UNIVERSE: return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    sem = asyncio.Semaphore(6)

    async def one(base):
        async with sem:
            if base in WATCHLIST: return
            item = UNIVERSE.get(base)
            if not item: return
            kc_c = await fetch_kucoin_candles(item["kucoin_symbol"])
            kc = calc_metrics(kc_c)
            if kc and kc["rvol"] >= 1.7:
                add_watch(base, "kucoin", kc)

    await asyncio.gather(*[one(b) for b in cands])

    if WATCHLIST:
        await asyncio.gather(*[process(b) for b in list(WATCHLIST.keys())])

    now = time.time()
    for b in [b for b, w in WATCHLIST.items() if w["exp"] < now]:
        WATCHLIST.pop(b, None)

async def main_loop():
    last = 0
    while True:
        try:
            now = time.time()
            if now - last > UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last = now
            if UNIVERSE:
                await scan_cycle()
            await asyncio.sleep(CANDLE_REFRESH_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Ошибка: %s", e)
            await asyncio.sleep(10)

# ============================================================
# SERVER
# ============================================================

async def index(req):
    return web.Response(text=f"Quiet v8.0 | Uni: {len(UNIVERSE)} | WL: {len(WATCHLIST)} | Quiet: {STATS['quiet']} | Impulse: {STATS['impulse']}")

async def start(app):
    global SESSION
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=40, ttl_dns_cache=300))
    app["task"] = asyncio.create_task(main_loop())
    await send_tg("🟡 <b>Quiet Inflow v8.0 запущен</b>\nМульти-биржа + лонговый объём")

async def stop(app):
    t = app.get("task")
    if t:
        t.cancel()
        await t
    if SESSION:
        await SESSION.close()

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
