import os
import time
import asyncio
import logging
import threading
import aiohttp
import numpy as np
from flask import Flask

# ============================================================
#  СКАНЕР ИМПУЛЬСА С ПОЛКИ С ПОЛНЫМ ИНФО-ПАСПОРТОМ (BINGX)
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE = "https://open-api.bingx.com"

# --- Единственные базовые триггеры для фиксации импульса ---
MIN_24H_VOLUME_USDT = 100_000    # Минимальная ликвидность ($100k)
SHELF_LOOKBACK_CANDLES = 4      # Полка из 4 закрытых 4H свечей (16 часов)
MAX_SHELF_WIDTH_PCT = 6.0       # Максимальная ширина полки (до 6%)
MIN_BREAKOUT_PCT = 2.5          # Старт импульса (от +2.5% от верха полки)
CHECK_INTERVAL_SECONDS = 30    # Проверка каждые 30 секунд
ALERT_COOLDOWN_SECONDS = 4 * 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

@app.route("/")
def home():
    return "Pump Bot Analytics Active", 200

def run_flask():
    cli = logging.getLogger('werkzeug')
    cli.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=PORT)

# ============================================================
#  РАСЧЕТ МЕТРИК И ИНДИКАТОРОВ
# ============================================================

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def parse_kline(k):
    return {
        "timestamp": int(k.get("time", 0)),
        "open": safe_float(k.get("open")),
        "high": safe_float(k.get("high")),
        "low": safe_float(k.get("low")),
        "close": safe_float(k.get("close")),
        "volume": safe_float(k.get("volume")),
    }

def format_price(price):
    if price >= 1000:
        return f"{price:.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    elif price >= 0.01:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

# ============================================================
#  ПОЛУЧЕНИЕ ДАННЫХ С BINGX
# ============================================================

async def get_tradable_symbols(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров: {e}")
        return {}

async def get_klines(session, symbol, semaphore):
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": "4h", "limit": 30}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=8) as resp:
                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list):
                    return []
                return [parse_kline(k) for k in candles]
        except:
            return []

async def get_open_interest(session, symbol, semaphore):
    """Запрос Открытого Интереса (OI) с BingX"""
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/openInterest"
    params = {"symbol": symbol}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == 0:
                    return safe_float(data.get("data", {}).get("openInterest"))
        except:
            pass
    return None

async def send_telegram(session, text):
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or not BOT_TOKEN:
        logging.info(f"[MOCK TELEGRAM]:\n{text}")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with session.post(url, json=payload, timeout=8) as resp:
            if resp.status != 200:
                logging.error(f"Telegram error: {await resp.text()}")
    except Exception as e:
        logging.error(f"Telegram exception: {e}")

# ============================================================
#  АНАЛИЗ И ОТПРАВКА КАРТОЧКИ
# ============================================================

async def check_symbol(session, symbol, vol_24h, sent_alerts, semaphore):
    now = time.time()
    if symbol in sent_alerts and (now - sent_alerts[symbol]) < ALERT_COOLDOWN_SECONDS:
        return

    candles = await get_klines(session, symbol, semaphore)
    if len(candles) < SHELF_LOOKBACK_CANDLES + 15:
        return

    closed = candles[:-1]
    current = candles[-1]

    # Анализ полки
    recent_closed = closed[-SHELF_LOOKBACK_CANDLES:]
    highs = [c["high"] for c in recent_closed]
    lows = [c["low"] for c in recent_closed]
    shelf_high = max(highs)
    shelf_low = min(lows)
    
    if shelf_low <= 0:
        return
        
    shelf_width_pct = ((shelf_high - shelf_low) / shelf_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return

    current_price = current["close"]
    if current_price <= 0:
        return

    breakout_pct = ((current_price - shelf_high) / shelf_high) * 100
    if breakout_pct < MIN_BREAKOUT_PCT:
        return

    # --- СБОР ИНФОРМАЦИОННЫХ МЕТРИК ---
    
    # 1. Расчет RVOL (Всплеск объема)
    base_vols = [c["volume"] for c in recent_closed]
    avg_base_vol = np.mean(base_vols) if base_vols else 0
    rvol = (current["volume"] / avg_base_vol) if avg_base_vol > 0 else 0.0

    # 2. Расчет RSI
    closes = [c["close"] for c in candles]
    rsi_val = calculate_rsi(closes)

    # 3. Запрос Открытого Интереса (OI)
    oi_val = await get_open_interest(session, symbol, semaphore)
    oi_str = f"{oi_val:,.0f}" if oi_val is not None else "Н/Д"

    # --- ОЦЕНКА КАЧЕСТВА ДЛЯ БЫСТРОГО ВЗГЛЯДА ---
    rvol_tag = "🔥 Всплеск!" if rvol >= 2.0 else ("✅ Высокий" if rvol >= 1.2 else "💤 Сухой")
    rsi_tag = "⚠️ Перегрет (>75)" if rsi_val >= 75 else "✅ Норма"
    vol_tag = "🟢 Ликвидная" if vol_24h >= 500_000 else "🟡 Щиткоин"

    coin = symbol.split("-")[0]
    
    message = (
        f"⚡ <b>ИМПУЛЬС С ПОЛКИ: {coin}</b>\n\n"
        f"🎯 <b>ПРОБОЙ И ЦЕНА:</b>\n"
        f"├ Рост от полки: <b>+{breakout_pct:.2f}%</b>\n"
        f"├ Верх полки: <code>{format_price(shelf_high)}</code>\n"
        f"└ Текущая цена: <code>{format_price(current_price)}</code>\n\n"
        
        f"📊 <b>ИНФОРМАЦИОННОЕ ПОЛЕ:</b>\n"
        f"├ <b>RVOL (Всплеск):</b> {rvol:.2f}x ({rvol_tag})\n"
        f"├ <b>Объем 24h:</b> ${vol_24h/1000:.1f}k ({vol_tag})\n"
        f"├ <b>Открытый интерес (OI):</b> {oi_str}\n"
        f"├ <b>RSI (14):</b> {rsi_val:.1f} ({rsi_tag})\n"
        f"├ <b>Ширина полки:</b> {shelf_width_pct:.2f}%\n"
        f"└ <b>База:</b> {SHELF_LOOKBACK_CANDLES * 4}ч ({SHELF_LOOKBACK_CANDLES} свечей 4H)\n\n"
        
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть график BingX</a>"
    )

    await send_telegram(session, message)
    sent_alerts[symbol] = now
    logging.info(f"СИГНАЛ: {symbol} +{breakout_pct:.2f}% | RVOL: {rvol:.2f}x")

# ============================================================
#  ГЛАВНЫЙ ЦИКЛ
# ============================================================

async def main():
    sent_alerts = {}
    semaphore = asyncio.Semaphore(15)
    
    async with aiohttp.ClientSession() as session:
        logging.info("Сканер с информационным полем запущен")
        await send_telegram(session, "🤖 <b>Бот запущен!</b> Ищу импульсы с полки и собираю аналитику (RVOL, OI, RSI).")

        while True:
            try:
                symbols_dict = await get_tradable_symbols(session)
                logging.info(f"Сканирую {len(symbols_dict)} монет...")
                
                tasks = [
                    check_symbol(session, sym, vol, sent_alerts, semaphore) 
                    for sym, vol in symbols_dict.items()
                ]
                await asyncio.gather(*tasks)
                
            except Exception as e:
                logging.error(f"Ошибка главного цикла: {e}")
                
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановлено")
