import os
import time
import requests
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# ==========================================
# 1. НАСТРОЙКИ И КОНСТАНТЫ
# ==========================================
BREAKOUT_TARGET_PCT = 4.0
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==========================================
# 2. ФИЛЬТРЫ И ЛОГИКА СИГНАЛОВ
# ==========================================

def check_anti_mm_breakout(candle):
    """
    Фильтр бычьего пробоя (Лонг):
    Проверяет, что у пробойной свечи нет гигантского верхнего фитиля (сброса).
    """
    candle_range = candle['high'] - candle['low']
    if candle_range == 0:
        return True
        
    body_top = max(candle['open'], candle['close'])
    upper_wick = candle['high'] - body_top
    wick_ratio = upper_wick / candle_range

    # Если верхняя тень занимает > 35% свечи — пробой сливают, пропускаем
    if wick_ratio > 0.35:
        return False
    return True


def check_mm_distribution_short(candle, rvol, oi_change_pct):
    """
    Детектор разгрузки Маркет-Мейкера (Шорт):
    Ловит момент, когда ММ обгружает позицию об толпу на пике импульса.
    """
    candle_range = candle['high'] - candle['low']
    if candle_range == 0:
        return False
        
    body_top = max(candle['open'], candle['close'])
    upper_wick = candle['high'] - body_top
    wick_ratio = upper_wick / candle_range

    # Условия сброса ММ:
    # 1. Длинный верхний фитиль (>= 40% от всей свечи)
    # 2. Аномальный объем (RVOL >= 5.0x)
    # 3. Падение Открытого Интереса (OI обвалился более чем на 8% за свечу)
    is_wick_rejection = wick_ratio >= 0.40
    is_volume_climax = rvol >= 5.0
    is_oi_unloading = (oi_change_pct is not None) and (oi_change_pct <= -8.0)

    if is_wick_rejection and is_volume_climax and is_oi_unloading:
        return True
        
    return False


# ==========================================
# 3. ФОРМАТИРОВАНИЕ И ОТПРАВКА В TELEGRAM
# ==========================================

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки Telegram: {e}")


def build_signal_message(symbol, ticker, candidate, rvol, oi=None, oi_growth=None, signal_type="LONG"):
    shelf = candidate["shelf"]
    ema = candidate["ema"]
    impulse = candidate["impulse"]
    price = ticker.get("price", 0.0)
    clean_symbol = symbol.replace("-USDT", "")

    oi_str = f"${oi:,.0f}" if oi is not None else "Н/Д"
    oi_growth_str = f"{oi_growth:+.2f}%" if oi_growth is not None else "Н/Д"

    if signal_type == "SHORT_MM":
        header = "🎯 <b>ПАРТИЗАН — СБРОС ММ (ШОРТ / ОТСКОК)</b>"
        action_note = "⚠️ <i>ММ разгружается об толпу! Вход в шорт на откат к полке.</i>"
    else:
        header = "🏹 <b>ПАРТИЗАН v6.5 — БЫЧИЙ ПРОБОЙ</b>"
        action_note = f"🚀 <b>Цель:</b> +{BREAKOUT_TARGET_PCT:.1f}%"

    return (
        f"{header}\n\n"
        f"🪙 <b>Монета:</b> <code>{symbol}</code> (<code>{clean_symbol}</code>)\n"
        f"💰 <b>Цена:</b> {price:.8g}\n\n"
        f"📦 <b>ПОЛКА</b>\n"
        f"   Нижняя: {shelf['bottom']:.8g}\n"
        f"   Верхняя: {shelf['top']:.8g}\n"
        f"   Ширина: {shelf['width_pct']:.2f}%\n"
        f"   Свечей: {shelf['length']}\n\n"
        f"📈 <b>EMA ВЕЕР</b>\n"
        f"   EMA20 внутри: {ema['inside20_pct']:.0f}%\n"
        f"   EMA40 внутри: {ema['inside40_pct']:.0f}%\n"
        f"   EMA80 дистанция: {ema['ema80_distance_pct']:.2f}%\n\n"
        f"⚡ <b>ИМПУЛЬС И ОБЪЕМ</b>\n"
        f"   Свеча пробоя: {impulse['candles']}\n"
        f"   RVOL: <b>{rvol:.2f}x</b>\n"
        f"   24h объём: ${ticker.get('volume24h', 0):,.0f}\n"
        f"   Открытый интерес (OI): <b>{oi_str}</b> ({oi_growth_str})\n\n"
        f"{action_note}"
    )


def process_candidate(symbol, ticker, candidate, rvol, oi, oi_growth, last_candle):
    # 1. Проверяем на разгрузку ММ (Шорт)
    if check_mm_distribution_short(last_candle, rvol, oi_growth):
        msg = build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth, signal_type="SHORT_MM")
        send_telegram(msg)
        return

    # 2. Проверяем на чистый бычий пробой (Лонг)
    if check_anti_mm_breakout(last_candle):
        msg = build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth, signal_type="LONG")
        send_telegram(msg)


# ==========================================
# 4. ОБРАБОТКА И СКАНЕР РЫНКА
# ==========================================

def run_market_scan():
    """
    Здесь находится твоя существующая функция получения пар с биржи,
    расчета полок, EMA и запуска process_candidate().
    """
    # Пример вызова процесса внутри твоего цикла по парам:
    # process_candidate(symbol, ticker, candidate, rvol, oi, oi_growth, last_candle)
    pass


# ==========================================
# 5. СЕРВЕР ДЛЯ RENDER (HEALTH CHECK) И MAIN
# ==========================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        return  # Глушим технические логи HealthCheck в консоли


def run_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    print(f"🌐 Health-check сервер запущен на порту {port}")
    server.serve_forever()


def main_loop():
    print("🚀 Старт основного цикла сканирования...")
    scan_count = 0
    while True:
        try:
            scan_count += 1
            print(f"🔍 Скан #{scan_count} запущен")
            run_market_scan()
        except Exception as e:
            print(f"⚠️ Ошибка во время сканирования (перехвачена): {e}")
        
        # Пауза между сканами (60 секунд)
        time.sleep(60)


if __name__ == "__main__":
    # Запускаем лёгкий веб-сервер в фоновом потоке для Render
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # Запускаем сканер в основном потоке
    main_loop()
