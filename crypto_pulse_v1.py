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
BASE_CANDLES_COUNT = 4          # Количество свечей базы
MAX_SHELF_WIDTH_PCT = 4.0       # Максимальная ширина полки (4%)
MIN_24H_VOLUME_USDT = 1_500_000 # Мин. объем $1.5M

# Фильтры качества закола
MIN_SWEEP_PCT = 0.2             # Мин. глубина закола (0.2%)
MAX_SWEEP_PCT = 2.5             # Макс. глубина закола (2.5% - отсекаем проливы)
RECOVERY_RATIO = 0.65           # Закрытие в верхней/нижней 35% части свечи (выкуп)

CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 3600   # Кулдаун 1 час на монету
SESSION_MAX_AGE = 1800

last_signals = {}
scan_counter = 0

# Очередь сообщений для Telegram, чтобы избежать бана за флуд
message_queue = asyncio.Queue()

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

async def telegram_worker(bot):
    """
    Фоновый воркер: гарантирует отправку не более 20 сообщений в секунду (Защита от Too Many Requests)
    """
    while True:
        text, chat_id = await message_queue.get()
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
            await asyncio.sleep(0.05)  # Задержка 50мс между сообщениями
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения: {e}")
            await asyncio.sleep(1)
        finally:
            message_queue.task_done()

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
    params = {"symbol": symbol, "interval": "1h", "limit": 20}
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

def detect_false_breakout(base_candles, signal_candle):
    """
    Строгий математический детектор ложного пробоя
    """
    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)

    c_close = signal_candle["close"]
    c_high = signal_candle["high"]
    c_low = signal_candle["low"]
    candle_range = c_high - c_low

    if candle_range <= 0:
        return None, base_high, base_low

    # 1. ЛОЖНЫЙ ПРОБОЙ ВНИЗ (ЛОНГ)
    if c_low < base_low and c_close > base_low:
        sweep_pct = ((base_low - c_low) / base_low) * 100
        if MIN_SWEEP_PCT <= sweep_pct <= MAX_SWEEP_PCT:
            # Закрытие в верхней трети свечи (сильный бычий выкуп)
            if (c_close - c_low) / candle_range >= RECOVERY_RATIO:
                return "LONG_FALSE_BREAK", base_high, base_low

    # 2. ЛОЖНЫЙ ПРОБОЙ ВВЕРХ (ШОРТ)
    if c_high > base_high and c_close < base_high:
        sweep_pct = ((c_high - base_high) / base_high) * 100
        if MIN_SWEEP_PCT <= sweep_pct <= MAX_SWEEP_PCT:
            # Закрытие в нижней трети свечи (сильный продавец)
            if (c_high - c_close) / candle_range >= RECOVERY_RATIO:
                return "SHORT_FALSE_BREAK", base_high, base_low

    return None, base_high, base_low

async def check_symbol(session, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    # Нужно минимум (BASE_CANDLES_COUNT + 2) свечей
    if len(candles) < (BASE_CANDLES_COUNT + 2):
        return False

    # Берем ЗАКРЫТЫЕ свечи! 
    # candles[-1] — живая неоконченная свеча (игнорируем!)
    # candles[-2] — последняя СФОРМИРОВАННАЯ сигнальная свеча
    signal_candle = candles[-2]
    base_candles = candles[-(BASE_CANDLES_COUNT + 2):-2]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)
    if base_low <= 0:
        return False

    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return False

    signal_type, b_high, b_low = detect_false_breakout(base_candles, signal_candle)
    if not signal_type:
        return False

    last_signals[symbol] = now

    clean_coin = symbol.split("-")[0].upper()
    if signal_type == "LONG_FALSE_BREAK":
        title = f"🟢 <b>ЛОЖНЫЙ ПРОБОЙ ВНИЗ (ЛОНГ): {clean_coin}</b>"
        desc = "Закол уровня + Сильный бычий выкуп!"
        level_info = f"├ Уровень базы: <code>{format_price(b_low)}</code>"
    else:
        title = f"🔴 <b>ЛОЖНЫЙ ПРОБОЙ ВВЕРХ (ШОРТ): {clean_coin}</b>"
        desc = "Закол уровня + Сильный медвежий отвал!"
        level_info = f"├ Уровень базы: <code>{format_price(b_high)}</code>"

    message = (
        f"{title}\n\n"
        f"⚠️ <i>{desc}</i>\n"
        f"{level_info}\n"
        f"├ Закрытие свечи: <code>{format_price(signal_candle['close'])}</code>\n"
        f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>График BingX</a>"
    )

    # Кладем сообщение в очередь воркера
    await message_queue.put((message, CHAT_ID))
    return True

async def scanner_loop():
    global scan_counter
    semaphore = asyncio.Semaphore(12)
    
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=25, ttl_dns_cache=300, force_close=False)
            
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
                    
                    tasks = [check_symbol(session, sym, vol, semaphore) for sym, vol in symbols_dict.items()]
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
    
    # Запускаем фоновый обработчик очереди Telegram
    asyncio.create_task(telegram_worker(bot))

    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"🌐 Сервер запущен на порту {PORT}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text="🤖 Бот «Ложный пробой» запущен со строгой фильтрацией!")
    except Exception as e:
        logging.error(f"❌ Стартовая ошибка: {e}")

    try:
        await scanner_loop()
    finally:
        await runner.cleanup()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
