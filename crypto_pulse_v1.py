import os
import time
import asyncio
import logging
import threading
import aiohttp
from flask import Flask

# ============================================================
#                    CONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()

# Оптимизированные фильтры накопительной базы
MIN_24H_VOLUME_USDT = 8_000_000   # Порог $8M+ расширяет выборку
MAX_BASE_RANGE_PCT = 6.0          # Ширина базы до 6% (реалистично для 4H)
ACCUMULATION_CANDLES = 5          # 20 часов накопления (5 свечей по 4H)
MIN_RVOL_4H = 1.35                # Всплеск объема от 1.35x (акула начинает входить)
MAX_RSI_4H = 62.0                 # RSI до 62 (зона активного накопления)
MAX_PRICE_DISTANCE_FROM_BASE = 2.5 # Макс. отрыв от хая базы (%)

CHECK_INTERVAL_SECONDS = 300      
ALERT_COOLDOWN_SECONDS = 14400    

EXCLUDE_KEYWORDS = [
    "XAUT", "GOLD", "PAXG", "USDC", "BTCDOM", "ETHDAI",
    "OIL", "WTI", "BRENT", "XAU", "XAG", "NCCO", "SILVER"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
sent_alerts = {}

# ============================================================
#                     FLASK (ДЛЯ RENDER)
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "MacroPulse 4H | Active", 200

def run_flask():
    cli = logging.getLogger('werkzeug')
    cli.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=PORT)

# ============================================================
#                 HELPERS & PARSERS
# ============================================================

def to_ticker_format(symbol: str) -> str:
    symbol = symbol.upper().strip()
    if "-" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return symbol

def to_api_format(symbol: str) -> str:
    return symbol.replace("-", "").upper().strip()

def is_crypto_symbol(symbol: str) -> bool:
    sym_upper = symbol.upper()
    coin = sym_upper.split("-")[0] if "-" in sym_upper else sym_upper.replace("USDT", "")
    for kw in EXCLUDE_KEYWORDS:
        if kw in coin:
            return False
    return len(coin) <= 10

def format_price(price):
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    return f"{price:.8f}"

def parse_kline(k):
    try:
        if isinstance(k, dict):
            return int(k.get("time", 0)), float(k.get("open", 0)), float(k.get("high", 0)), float(k.get("low", 0)), float(k.get("close", 0)), float(k.get("volume", 0))
        if isinstance(k, (list, tuple)):
            return int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
    except Exception:
        pass
    return 0, 0.0, 0.0, 0.0, 0.0, 0.0

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        gains.append(max(0, change))
        losses.append(max(0, -change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

# ============================================================
#                 API BINGX & ANALYSIS
# ============================================================

async def send_telegram_alert(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.info("[TG MOCK]\n%s", text)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        await session.post(url, json=payload, timeout=10)
    except Exception as e:
        logging.error("Telegram error: %s", e)

async def get_top_tickers(session):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return []
            res = []
            for item in data.get("data", []):
                sym = to_ticker_format(item.get("symbol", ""))
                if not sym.endswith("-USDT") or not is_crypto_symbol(sym):
                    continue
                vol = float(item.get("quoteVolume", 0))
                price = float(item.get("lastPrice", 0))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    res.append((sym, price, vol))
            return res
    except Exception as e:
        logging.error(f"Error fetching tickers: {e}")
        return []

async def analyze_4h_setup(session, symbol, current_price):
    url = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
    symbol_api = to_api_format(symbol)
    params = {"symbol": symbol_api, "interval": "4h", "limit": 30}
    try:
        async with session.get(url, params=params, timeout=8) as resp:
            data = await resp.json()
            candles = data.get("data", [])
            if len(candles) < 20:
                return None

            parsed = [parse_kline(c) for c in candles if parse_kline(c)[0] > 0]
            parsed.sort(key=lambda x: x[0])

            # Исключаем текущую открытую свечу
            closed_candles = parsed[:-1]
            if len(closed_candles) < ACCUMULATION_CANDLES + 10:
                return None

            closes = [c[4] for c in closed_candles]
            volumes = [c[5] * c[4] for c in closed_candles] # USDT Объём

            # Диапазон накопительной базы (берутся свечи ДО текущего движения)
            base_candles = closed_candles[-(ACCUMULATION_CANDLES + 1):-1]
            base_highs = [c[2] for c in base_candles]
            base_lows = [c[3] for c in base_candles]

            max_p = max(base_highs)
            min_p = min(base_lows)

            if min_p <= 0:
                return None

            range_pct = ((max_p - min_p) / min_p) * 100.0
            if range_pct > MAX_BASE_RANGE_PCT:
                return None

            # Защита от упущенного входа (цена не должна уйти выше 2.5% от базы)
            if current_price > max_p * (1 + MAX_PRICE_DISTANCE_FROM_BASE / 100):
                return None

            # Расчет RVOL (Сравнение объема последней закрытой 4H с исторической медианой)
            current_vol = volumes[-1]
            hist_vol = volumes[-11:-1]
            avg_hist_vol = sum(hist_vol) / len(hist_vol) if hist_vol else 1
            rvol_4h = current_vol / avg_hist_vol if avg_hist_vol > 0 else 1.0

            if rvol_4h < MIN_RVOL_4H:
                return None

            rsi = calculate_rsi(closes)
            if rsi > MAX_RSI_4H:
                return None

            return {
                "range_pct": range_pct,
                "rvol": rvol_4h,
                "rsi": rsi,
                "base_high": max_p,
                "base_low": min_p,
                "duration_hours": ACCUMULATION_CANDLES * 4
            }
    except Exception:
        return None

# ============================================================
#                     MAIN LOOP
# ============================================================

async def self_ping():
    if not SELF_URL:
        return
    await asyncio.sleep(30)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(SELF_URL, timeout=5):
                    pass
            except Exception:
                pass
            await asyncio.sleep(600)

async def process_ticker(session, ticker, semaphore):
    symbol, price, vol = ticker
    now = time.time()

    if symbol in sent_alerts and (now - sent_alerts[symbol]) < ALERT_COOLDOWN_SECONDS:
        return

    async with semaphore:
        setup = await analyze_4h_setup(session, symbol, price)

    if setup:
        coin = symbol.split("-")[0]
        msg = (
            f"💣 <b>ПОДГОТОВКА К ПАМПУ (4H)</b> | <code>{coin}</code>\n\n"
            f"📦 <b>Накопление:</b> {setup['duration_hours']} часов в пределах {setup['range_pct']:.2f}%\n"
            f"📊 <b>RVOL 4H:</b> {setup['rvol']:.2f}x\n"
            f"📈 <b>RSI 4H:</b> {setup['rsi']:.1f}\n\n"
            f"🎯 <b>Верхняя граница базы:</b> {format_price(setup['base_high'])}\n"
            f"🛡 <b>Нижняя граница (Стоп):</b> {format_price(setup['base_low'])}\n"
            f"💰 <b>Текущая цена:</b> {format_price(price)}"
        )
        await send_telegram_alert(session, msg)
        sent_alerts[symbol] = now

async def main():
    async with aiohttp.ClientSession() as session:
        logging.info("🎯 MACRO_PULSE (4H Pump Hunter) Запущен")
        asyncio.create_task(self_ping())

        while True:
            tickers = await get_top_tickers(session)
            logging.info(f"🔍 Сканирование {len(tickers)} ликвидных монет...")

            semaphore = asyncio.Semaphore(10)
            tasks = [process_ticker(session, ticker, semaphore) for ticker in tickers]
            await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main())
