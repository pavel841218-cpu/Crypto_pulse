import os
import time
import threading
import requests
import numpy as np
from flask import Flask

# --- МИКРО ВЕБ-СЕРВЕР ДЛЯ RENDER ---
app = Flask('')

@app.route('/')
def home():
    return "Bot Quasimodo is running!", 200

def run_http_server():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# Запуск HTTP сервера в фоновом потоке
threading.Thread(target=run_http_server, daemon=True).start()

# --- НАСТРОЙКИ БОТА ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

sent_signals_cache = {}
CACHE_COOLDOWN = 900  # 15 минут пауза


def send_telegram_message(message: str):
    """Отправка сообщения в Telegram."""
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print(f"[TG MOCK]: {message}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки Telegram: {e}")


def calc_ema(candles, period=80):
    """Расчет EMA 80."""
    if len(candles) < period:
        return None
    closes = [c["close"] for c in candles]
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    ema = np.convolve(closes, weights, mode='full')[:len(closes)]
    return float(ema[-1])


def fetch_bingx_candles(symbol: str, interval: str = "15m", limit: int = 100):
    """Получение свечей BingX."""
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        res = requests.get(url, params=params, timeout=5).json()
        if res.get("code") == 0 and "data" in res:
            raw_data = res["data"]
            parsed = []
            for item in raw_data:
                parsed.append({
                    "time": int(item["time"]),
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item["volume"])
                })
            parsed.sort(key=lambda x: x["time"])
            return parsed
    except Exception as e:
        print(f"Ошибка загрузки свечей {symbol}: {e}")
    return []


def fetch_oi_deltas(symbol: str) -> dict:
    """Заглушка дельты Open Interest."""
    return {
        "KuCoin": 0.0,
        "Bitget": 0.0,
        "Bybit": 0.0
    }


def analyze_market_conditions(candles_15m, candles_1m, oi_deltas):
    """Анализ условий."""
    if len(candles_15m) < 40 or len(candles_1m) < 5:
        return None, {}

    curr = candles_15m[-1]
    close_now = curr["close"]
    open_now = curr["open"]

    base_slice = candles_15m[-98:-3] if len(candles_15m) >= 98 else candles_15m[:-3]
    if base_slice:
        base_high = max(c["high"] for c in base_slice)
        base_low = min(c["low"] for c in base_slice)
        base_width_pct = ((base_high - base_low) / base_low) * 100.0 if base_low > 0 else 999.0
    else:
        base_high, base_low, base_width_pct = close_now, close_now, 999.0

    vol_now = curr["volume"] * close_now
    prev_vols = [c["volume"] * c["close"] for c in candles_15m[-21:-1] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1.0
    rvol = vol_now / avg_vol if avg_vol > 0 else 0.0

    recent_5m = candles_1m[-5:]
    pct_5m = ((close_now - recent_5m[0]["open"]) / recent_5m[0]["open"]) * 100.0 if recent_5m[0]["open"] > 0 else 0.0

    valid_ois = [v for v in oi_deltas.values() if v is not None]
    max_oi = max(valid_ois) if valid_ois else 0.0
    min_oi = min(valid_ois) if valid_ois else 0.0

    ema80 = calc_ema(candles_15m, 80)

    # ДЕТЕКТОР 1: СБРОС (Только на КРАСНОЙ свече)
    high_5m = max(c["high"] for c in recent_5m)
    drop_from_5m_high = ((high_5m - close_now) / high_5m) * 100.0 if high_5m > 0 else 0.0

    is_red_candle = close_now < open_now
    if is_red_candle and drop_from_5m_high >= 3.0 and (rvol >= 2.5 or min_oi <= -3.0):
        return "DUMP", {
            "title": "🔴 НАЧАЛСЯ СБРОС ПОЗИЦИЙ",
            "drop_pct": drop_from_5m_high,
            "rvol": rvol,
            "close": close_now,
            "oi_deltas": oi_deltas
        }

    # ДЕТЕКТОР 2: РАННИЙ ВЫХОД ИЗ ПОЛКИ
    is_base_breakout = (close_now > base_high) and (base_width_pct <= 6.0)
    if is_base_breakout:
        return "EARLY_BASE_BREAKOUT", {
            "title": "🚀 ИМПУЛЬС / ПАМП",
            "base_high": base_high,
            "base_width": base_width_pct,
            "close": close_now,
            "pct_5m": pct_5m,
            "rvol": rvol,
            "oi_deltas": oi_deltas
        }

    # ДЕТЕКТОР 3: БЫСТРЫЙ ИМПУЛЬС
    if pct_5m >= 3.0 and (max_oi >= 8.0 or rvol >= 3.0):
        return "FAST_IMPULSE", {
            "title": "🚀 ИМПУЛЬС / ПАМП",
            "base_high": base_high,
            "close": close_now,
            "pct_5m": pct_5m,
            "rvol": rvol,
            "oi_deltas": oi_deltas
        }

    # ДЕТЕКТОР 4: ПРОДОЛЖЕНИЕ ТРЕНДА
    if ema80 and close_now > ema80 and max_oi >= 12.0:
        return "TREND_CONTINUATION", {
            "title": "📈 ПРОДОЛЖЕНИЕ ТРЕНДА",
            "base_high": base_high,
            "close": close_now,
            "pct_5m": pct_5m,
            "rvol": rvol,
            "oi_deltas": oi_deltas
        }

    return None, {}


def format_telegram_alert(symbol: str, signal_type: str, data: dict) -> str:
    """Форматирование алерта."""
    title = data.get("title", "⚠️ СИГНАЛ")
    close = data.get("close", 0.0)
    rvol = data.get("rvol", 0.0)
    oi_deltas = data.get("oi_deltas", {})

    clean_symbol = symbol.replace("-", "").replace(".P", "")
    oi_str = "\n".join([f"  {k}: +{v:.1f}%" if v >= 0 else f"  {k}: {v:.1f}%" for k, v in oi_deltas.items()])

    if signal_type == "DUMP":
        return (
            f"<b>{title}: {clean_symbol}</b>\n\n"
            f"⚠️ <b>Слив об маркет на 1m/5m таймфрейме!</b>\n\n"
            f"📉 <b>Откат от пика:</b> -{data.get('drop_pct', 0.0):.1f}%\n"
            f"🔥 <b>RVOL:</b> {rvol:.1f}x\n"
            f"💰 <b>Текущая цена:</b> {close}\n\n"
            f"📊 <b>Отток OI (Фиксация):</b>\n{oi_str}\n\n"
            f"🔴 <b>Статус: Выход / Фиксация прибыли</b>"
        )
    else:
        base_high = data.get("base_high", 0.0)
        base_info = f"{base_high:.4f} ➔ {close:.4f}" if base_high > 0 else f"{close:.4f}"
        return (
            f"<b>{title}: {clean_symbol}</b>\n\n"
            f"🟢 <b>Рост:</b> +{data.get('pct_5m', 0.0):.1f}%\n"
            f"💥 <b>RVOL:</b> {rvol:.1f}x\n"
            f"📈 <b>Пробой базы:</b> {base_info}\n\n"
            f"📊 <b>Прирост OI:</b>\n{oi_str}\n\n"
            f"🎯 <b>Статус: Вход / Набор позиции</b>"
        )


def process_symbol(symbol: str):
    """Обработка монеты."""
    now = time.time()
    if symbol in sent_signals_cache and (now - sent_signals_cache[symbol] < CACHE_COOLDOWN):
        return

    candles_15m = fetch_bingx_candles(symbol, interval="15m", limit=100)
    candles_1m = fetch_bingx_candles(symbol, interval="1m", limit=15)

    if not candles_15m or not candles_1m:
        return

    oi_deltas = fetch_oi_deltas(symbol)
    signal_type, data = analyze_market_conditions(candles_15m, candles_1m, oi_deltas)

    if signal_type:
        text = format_telegram_alert(symbol, signal_type, data)
        send_telegram_message(text)
        sent_signals_cache[symbol] = now


def main():
    """Основной цикл."""
    print("🤖 Бот Quasimodo запущен...")
    symbols = [
        "NBIS-USDT", "SPCX-USDT", "LUNR-USDT", "APE-USDT",
        "BTC-USDT", "ETH-USDT", "SOL-USDT"
    ]

    while True:
        try:
            for symbol in symbols:
                process_symbol(symbol)
                time.sleep(0.4)
        except Exception as e:
            print(f"Ошибка цикла: {e}")

        time.sleep(10)


if __name__ == "__main__":
    main()
