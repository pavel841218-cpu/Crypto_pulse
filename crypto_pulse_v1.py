import os
import time
import asyncio
import logging
import threading
import aiohttp
from flask import Flask

# ============================================================
#  БОТ НА ЛОНГ ОТ БАЗЫ (ЧИСТЫЙ ПРОБОЙ ВВЕРХ) — 4H ВЕРСИЯ
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE = "https://open-api.bingx.com"

# --- Настройки стратегии (адаптированы под 4-часовые свечи) ---
BASE_CANDLES_COUNT = 4          # База из 4 закрытых 4H свечей (16 часов)
MAX_SHELF_WIDTH_PCT = 8.0       # Макс. ширина полки (для 4H более реалистично)
MIN_BREAKOUT_PCT = 1.5          # Пробой верха базы на +1.5% и выше
MIN_24H_VOLUME_USDT = 500_000   # Мин. ликвидность (можно менять под себя)
CHECK_INTERVAL_SECONDS = 60     # Проверка раз в минуту (достаточно для 4H)
ALERT_COOLDOWN_SECONDS = 3 * 3600 # 3 часа кулдаун на монету

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

@app.route("/")
def home():
    return "Base Long Breakout Bot (4H) Active", 200

def run_flask():
    cli = logging.getLogger('werkzeug')
    cli.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=PORT)

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def format_price(price):
    if price >= 1000: return f"{price:.2f}"
    elif price >= 1: return f"{price:.4f}"
    elif price >= 0.01: return f"{price:.6f}"
    else: return f"{price:.8f}"

async def get_tradable_symbols(session):
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            if data.get("code") != 0: return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"): continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка получения тикеров: {e}")
        return {}

async def get_klines(session, symbol, semaphore):
    # Запрос 4-часовых свечей
    url = f"{BINGX_BASE}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": "4h", "limit": 10}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=8) as resp:
                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list): return []

                parsed = [{
                    "high": safe_float(k.get("high")),
                    "low": safe_float(k.get("low")),
                    "close": safe_float(k.get("close"))
                } for k in candles]

                # РЕВЕРС: приводим порядок от старых свечей к новым
                parsed.reverse()
                return parsed
        except:
            return []

async def send_telegram(session, text):
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logging.info(f"[MOCK]: {text}")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=8): pass
    except Exception as e:
        logging.error(f"Ошибка Telegram: {e}")

async def check_symbol(session, symbol, vol_24h, sent_alerts, semaphore):
    now = time.time()
    if symbol in sent_alerts and (now - sent_alerts[symbol]) < ALERT_COOLDOWN_SECONDS:
        return

    candles = await get_klines(session, symbol, semaphore)
    if len(candles) < BASE_CANDLES_COUNT + 1:
        return

    # Последние 4 закрытые свечи и текущая открытая
    base_candles = candles[-(BASE_CANDLES_COUNT + 1):-1]
    current_candle = candles[-1]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)

    if base_low <= 0: return

    # Проверка на ширину полки (отсекаем высокую волатильность)
    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return

    current_price = current_candle["close"]

    # Пробой верхнего уровня базы
    breakout_pct = ((current_price - base_high) / base_high) * 100

    if breakout_pct < MIN_BREAKOUT_PCT:
        return

    coin = symbol.split("-")[0]
    message = (
        f"🟢 <b>ЛОНГ ОТ БАЗЫ: {coin}</b>\n\n"
        f"🚀 Пробой верха базы: <b>+{breakout_pct:.2f}%</b>\n"
        f"├ Верх базы (4H): <code>{format_price(base_high)}</code>\n"
        f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
        f"├ Ширина полки: <code>{shelf_width_pct:.2f}%</code>\n"
        f"└ Объем 24h: ${vol_24h/1000:.1f}k\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График BingX</a>"
    )

    await send_telegram(session, message)
    sent_alerts[symbol] = now
    logging.info(f"ЛОНГ СИГНАЛ: {symbol} +{breakout_pct:.2f}%")

async def main():
    sent_alerts = {}
    semaphore = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        logging.info("Лонг-бот от 4H базы запущен")
        await send_telegram(session, "🟢 <b>Лонг-бот (4H) запущен!</b> Сканирую четырёхчасовые базы и ловлю чистый пробой вверх.")

        while True:
            try:
                symbols_dict = await get_tradable_symbols(session)
                tasks = [check_symbol(session, sym, vol, sent_alerts, semaphore) for sym, vol in symbols_dict.items()]
                await asyncio.gather(*tasks)
            except Exception as e:
                logging.error(f"Ошибка главного цикла: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
