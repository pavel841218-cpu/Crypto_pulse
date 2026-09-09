import os
import requests

# Константы настроек
BREAKOUT_TARGET_PCT = 4.0

def check_anti_mm_breakout(candle):
    """
    Фильтр бычьего пробоя: проверяет, что свечу не начали сливать (нет гигантской верхней тени)
    """
    candle_range = candle['high'] - candle['low']
    if candle_range == 0:
        return True
        
    body_top = max(candle['open'], candle['close'])
    upper_wick = candle['high'] - body_top
    wick_ratio = upper_wick / candle_range

    # Если верхняя тень занимает больше 35% свечи — пробой сдувается
    if wick_ratio > 0.35:
        return False
    return True


def check_mm_distribution_short(candle, rvol, oi_change_pct):
    """
    Детектор разгрузки Маркет-Мейкера (Сигнал на ШОРТ)
    Ловит момент, когда ММ сбрасывает позицию об толпу.
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
    # 3. Обвал Открытого Интереса (OI упал более чем на 8% за свечу)
    is_wick_rejection = wick_ratio >= 0.40
    is_volume_climax = rvol >= 5.0
    is_oi_unloading = (oi_change_pct is not None) and (oi_change_pct <= -8.0)

    if is_wick_rejection and is_volume_climax and is_oi_unloading:
        return True
        
    return False


def build_signal_message(symbol, ticker, candidate, rvol, oi=None, oi_growth=None, signal_type="LONG"):
    """
    Генератор красиво оформленного сообщения для Telegram
    """
    shelf = candidate["shelf"]
    ema = candidate["ema"]
    impulse = candidate["impulse"]
    price = ticker.get("price", 0.0)
    clean_symbol = symbol.replace("-USDT", "")

    # Форматирование OI и его прироста
    oi_str = f"${oi:,.0f}" if oi is not None else "Н/Д"
    if oi_growth is not None:
        oi_growth_str = f"{oi_growth:+.2f}%"
    else:
        oi_growth_str = "Н/Д"

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
    """
    Главный конвейер обработки кандидата
    """
    # 1. Проверка на сброс ММ (Шорт)
    if check_mm_distribution_short(last_candle, rvol, oi_growth):
        msg = build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth, signal_type="SHORT_MM")
        send_telegram(msg)
        return

    # 2. Проверка на чистый бычий пробой (Лонг)
    if check_anti_mm_breakout(last_candle):
        msg = build_signal_message(symbol, ticker, candidate, rvol, oi, oi_growth, signal_type="LONG")
        send_telegram(msg)


def send_telegram(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Ошибка отправки Telegram: {e}")
