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

# ===== НАСТРОЙКИ ФИЛЬТРАЦИИ И ПОГЛОЩЕНИЯ =====
BASE_CANDLES_COUNT = 4          # 4 свечи базы
MAX_SHELF_WIDTH_PCT = 4.0       # Максимальная ширина полки (4%)
MIN_24H_VOLUME_USDT = 1_500_000 # Мин. объем $1.5M

# Фильтры закола и поглощения
MIN_SWEEP_PCT = 0.15            # Мин. закол лоу/хая (от 0.15%)
MAX_SWEEP_PCT = 2.0             # Макс. закол (до 2.0%, чтобы не брать сильные проливы)
RECOVERY_RATIO = 0.65           # Закрытие в верхних/нижних 35% свечи (выкуп/продавец)

CHECK_INTERVAL_SECONDS = 30     
ALERT_COOLDOWN_SECONDS = 3600   # Кулдаун 1 час
SESSION_MAX_AGE = 1800

last_signals = {}
scan_counter = 0

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
    while True:
        text, chat_id = await message_queue.get()
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
            await asyncio.sleep(0.05)
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
                
                # Исключаем акции, премаркеты и не-крипто инструменты (NCSK, AMD, NET и т.д.)
                if not sym.endswith("-USDT") or any(x in sym for x in ["_", "FOOTBALL", "INDEX", "NCSK"]):
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
    # Запрашиваем 1H таймфрейм для поиска крупных наборов позиции
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

def detect_accumulation_pattern(base_candles, signal_candle):
    """
    Паттерн: Закол ликвидности + Бычье/Медвежье Поглощение (Добор позиции)
    """
    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)

    c_open = signal_candle["open"]
    c_close = signal_candle["close"]
    c_high = signal_candle["high"]
    c_low = signal_candle["low"]
    candle_range = c_high - c_low

    if candle_range <= 0:
        return None, base_high, base_low

    prev_candle = base_candles[-1]

    # === 1. ЛОНГ: БЫЧЬЕ ПОГЛОЩЕНИЕ И ДОБОР (как на вашем скриншоте ARX) ===
    # Свеча сняла лоу базы, но закрылась выше открытия И перекрыла тело предыдущей свечи
    if c_low < base_low and c_close > base_low and c_close > c_open:
        sweep_pct = ((base_low - c_low) / base_low) * 100
        if MIN_SWEEP_PCT <= sweep_pct <= MAX_SWEEP_PCT:
            # Бычье поглощение: закрытие сигнальной свечи выше открытия предыдущей
            is_engulfing = c_close >= prev_candle["open"]
            # Закрытие в верхней части свечи (сильный бычий выкуп)
            is_strong_close = (c_close - c_low) / candle_range >= RECOVERY_RATIO
            
            if is_engulfing or is_strong_close:
                return "LONG_ACCUMULATION", base_high, base_low

    # === 2. ШОРТ: МЕДВЕЖЬЕ ПОГЛОЩЕНИЕ (Сброс позиции наверх) ===
    if c_high > base_high and c_close < base_high and c_close < c_open:
        sweep_pct = ((c_high - base_high) / base_high) * 100
        if MIN_SWEEP_PCT <= sweep_pct <= MAX_SWEEP_PCT:
            is_engulfing = c_close <= prev_candle["open"]
            is_strong_close = (c_high - c_close) / candle_range >= RECOVERY_RATIO
            
            if is_engulfing or is_strong_close:
                return "SHORT_DISTRIBUTION", base_high, base_low

    return None, base_high, base_low

async def check_symbol(session, symbol, vol_24h, semaphore):
    now = time.time()
    if symbol in last_signals and (now - last_signals[symbol]) < ALERT_COOLDOWN_SECONDS:
        return False

    candles = await fetch_klines(session, symbol, semaphore)
    if len(candles) < (BASE_CANDLES_COUNT + 2):
        return False

    # ВАЖНО: Анализируем только полностью ЗАКРЫТУЮ 1H свечу!
    signal_candle = candles[-2]
    base_candles = candles[-(BASE_CANDLES_COUNT + 2):-2]

    base_high = max(c["high"] for c in base_candles)
    base_low = min(c["low"] for c in base_candles)
    if base_low <= 0:
        return False

    shelf_width_pct = ((base_high - base_low) / base_low) * 100
    if shelf_width_pct > MAX_SHELF_WIDTH_PCT:
        return False

    signal_type, b_high, b_low = detect_accumulation_pattern(base_candles, signal_candle)
    if not signal_type:
        return False

    last_signals[symbol] = now

    clean_coin = symbol.split("-")[0].upper()
    if signal_type == "LONG_ACCUMULATION":
        title = f"🚀 <b>ДОБОР ПОЗИЦИИ / ПОГЛОЩЕНИЕ (ЛОНГ): {clean_coin}</b>"
        desc = "Снята ликвидность под базой + Бычье перекрытие/поглощение!"
        level_info = f"├ Уровень поддержки (база): <code>{format_price(b_low)}</code>"
    else:
        title = f"🔻 <b>СБРОС ПОЗИЦИИ / ПОГЛОЩЕНИЕ (ШОРТ): {clean_coin}</b>"
        desc = "Снята ликвидность над базой + Медвежье поглощение!"
        level_info = f"├ Уровень сопротивления (база): <code>{format_price(b_high)}</code>"

    message = (
        f"{title}\n\n"
        f"⚠️ <i>{desc}</i>\n"
        f"{level_info}\n"
        f"├ Закрытие 1H свечи: <code>{format_price(signal_candle['close'])}</code>\n"
        f"└ Объем 24h: <b>${vol_24h/1_000_000:.2f}M</b>\n\n"
        f"🔗 <a href='https://bingx.com/ru-ru/futures/forward/{symbol}'>Открыть график BingX</a>"
    )

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
    
    asyncio.create_task(telegram_worker(bot))

    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"🌐 Сервер запущен на порту {PORT}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text="⚡ Бот настроен на поиск бычьего/медвежьего поглощения (добор позиции)!")
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
