import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN") or os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID") or os.environ.get("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

BINGX_BASE_URL = "https://open-api.bingx.com"

# ===== НАСТРОЙКИ ФИЛЬТРАЦИИ =====
BASE_CANDLES_COUNT = 4          # 4 свечи для формирования базы
MAX_SHELF_WIDTH_PCT = 5.0       # Максимальная ширина базы (5%)
MIN_24H_VOLUME_USDT = 1_000_000 # Мин. объем $1M

CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 3600   # Кулдаун на одну монету (1 час)
SESSION_MAX_AGE = 1800

last_signals = {}
scan_counter = 0

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def format_price(price: float) -> str:
    if price is None or price == 0:
        return "0.00"
    if price >= 1000:
        return f"{price:.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    elif price >= 0.01:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"

def cleanup_storage():
    current_time = time.time()
    expired = [sym for sym, t in last_signals.items() if current_time - t > ALERT_COOLDOWN_SECONDS]
    for sym in expired:
        del last_signals[sym]

async def health_check(request):
    return web.Response(text="False Breakout Bot Active", status=200)

async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("code") != 0:
                return {}
            result = {}
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT") or any(x in sym for x in ["_", "FOOTBALL", "INDEX"]):
                    continue
                vol = safe_float(item.get("quoteVolume"))
                price = safe_float(item.get("lastPrice"))
                if vol >= MIN_24H_VOLUME_USDT and price > 0:
                    result[sym] = vol
            return result
    except Exception as e:
        logging.error(f"Ошибка тикеров: {e}")
        return {}

async def fetch_klines(session, symbol, semaphore):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    # Работаем по 1H таймфрейму
    params = {"symbol": symbol, "interval": "1h", "limit": 30}
    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
                candles = data.get("data", [])
                if not isinstance(candles, list):
                    return []
                parsed = []
                for k in candles:
                    if isinstance(k, dict):
                        parsed.append({
                            "open": safe_float(k.get("open")),
                            "high": safe_float(k.get("high")),
                            "low": safe_float(k.get("low")),
                            "close": safe_float(k.get("close"))
                        })
                    elif isinstance(k, list) and len(k) >= 5:
                        parsed.append({
                            "open": safe_float(k[1]),
                            "high": safe_float(k[2]),
                            "low": safe_float(k[3]),
                            "close": safe_float(k[4])
                        })
                return parsed
        except Exception:
            return []

def detect_false_breakout(base_candles, current_candle):
    """
    Детектор ложного пробоя с перекрытием:
    1. Ложный пробой ВВЕРХ (Шорт-сигнал): Тень пробила High базы, но свеча закрылась с перекрытием вниз.
    2. Ложный пробой ВНИЗ (Лонг-сигнал): Тень пробила Low базы, но свеча закрылась с перекрытием вверх.
    """
    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)

    c_open = current_candle["open"]
    c_close = current_candle["close"]
    c_high = current_candle["high"]
    c_low = current_candle["low"]

    # 1. ЛОЖНЫЙ ПРОБОЙ ВВЕРХ (Захват ликвидности сверху -> ШОРТ)
    if c_high > base_high and c_close < base_high:
        wick = c_high - max(c_open, c_close)
        body = abs(c_close - c_open)
        # Условие перекрытия: глубокий возврат внутрь базы или длинный фитиль
        if c_close <= c_open or wick > body:
            return "SHORT_FALSE_BREAK", base_high, base_low

    # 2. ЛОЖНЫЙ ПРОБОЙ ВНИЗ (Захват ликвидности снизу -> ЛОНГ)
    if c_low < base_low and c_close > base_low:
        wick = min(c_open, c_close) - c_low
        body = abs(c_close - c_open)
        # Условие перекрытия: бычье закрытие выше уровня базы
        if c_close >= c_open or wick > body:
            return "LONG_FALSE_BREAK", base_high, base_low

    return None, base_high, base_low

async def send_signal(bot, symbol, signal_type, base_high, base_low, current_price, vol_24h):
    try:
        clean_coin = symbol.split("-")[0].upper()
        
        if signal_type == "LONG_FALSE_BREAK":
            title = f"🟢 <b>ЛОЖНЫЙ ПРОБОЙ ВНИЗ (ЛОНГ): {clean_coin}</b>"
            desc = "Снята ликвидность под базой, произошло бычье перекрытие!"
            level_info = f"├ Нижняя граница базы: <code>{format_price(base_low)}</code>"
        else:
            title = f"🔴 <b>ЛОЖНЫЙ ПРОБОЙ ВВЕРХ (ШОРТ): {clean_coin}</b>"
            desc = "Снята ликвидность над базой, произошло медвежье перекрытие!"
            level_info = f"├ Верхняя граница базы: <code>{format_price(base_high)}</code>"

        message = (
            f"{title}\n\n"
            f"⚠️ <i>{desc}</i>\n"
            f"{level_info}\n"
            f"├ Текущая цена: <code>{format_price(current_price)}</code>\n"
            f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
            f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График BingX</a>"
        )
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки Telegram: {e}")
        return False

async def check_symbol(session, bot, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    if len(candles) < (BASE_CANDLES_COUNT + 1):
        return False

    base_candles = candles[-(BASE_CANDLES_COUNT + 1):-1]
    current_candle = candles[-1]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)
    if base_low <= 0:
        return False

    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return False

    signal_type, b_high, b_low = detect_false_breakout(base_candles, current_candle)
    if not signal_type:
        return False

    last_signals[symbol] = now
    return await send_signal(bot, symbol, signal_type, b_high, b_low, current_candle["close"], vol_24h)

async def scanner_loop(bot):
    global scan_counter
    semaphore = asyncio.Semaphore(15)
    
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300, force_close=False)
            
            async with aiohttp.ClientSession(connector=connector) as session:
                while True:
                    scan_counter += 1
                    start_time = time.time()
                    
                    if scan_counter % 30 == 0:
                        cleanup_storage()
                    
                    if time.time() - session_start_time > SESSION_MAX_AGE:
                        break
                    
                    symbols_dict = await fetch_bingx_symbols(session)
                    if not symbols_dict:
                        await asyncio.sleep(30)
                        break
                    
                    tasks = [check_symbol(session, bot, sym, vol, semaphore) for sym, vol in symbols_dict.items()]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    signals_count = sum(1 for r in results if r is True)
                    elapsed = time.time() - start_time
                    logging.info(f"Скан #{scan_counter} | {elapsed:.1f}с | Пар: {len(symbols_dict)} | Сигналов: {signals_count}")
                    
                    await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Ошибка цикла сканера: {e}")
            await asyncio.sleep(10)

async def main():
    bot = Bot(token=BOT_TOKEN)
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"🌐 Сервер запущен на порту {PORT}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «Ложный пробой + Перекрытие» успешно запущен!")
    except Exception as e:
        logging.error(f"❌ Стартовая ошибка: {e}")

    try:
        await scanner_loop(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
