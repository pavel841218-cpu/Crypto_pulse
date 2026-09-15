import asyncio
import logging
import time
from aiohttp import web, ClientSession

# --- НАСТРОЙКИ ---
BOT_TOKEN = "ТВОЙ_TELEGRAM_BOT_TOKEN"
CHAT_ID = "ТВОЙ_CHAT_ID"
COINGLASS_API_KEY = "ТВОЙ_COINGLASS_KEY"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class DEXCEXAggregatorBot:
    def __init__(self):
        # Буфер рассинхрона: { "CAPUSDT": {"bitget": timestamp, "gate": timestamp} }
        self.trigger_buffer = {}
        # Кэш OI для расчета дельты: { "CAPUSDT": last_oi }
        self.oi_cache = {}
        # Защита от флуда (cooldown 15 минут): { "CAPUSDT": timestamp_alert }
        self.cooldown_cache = {}

    # --- 1. ПРИВЕТСТВИЕ ПРИ СТАРТЕ ---
    async def send_startup_message(self, session: ClientSession):
        text = (
            "🚀 <b>Агрегатор DEX/CEX запущен и готов к работе!</b>\n\n"
            "• Мониторинг: Bitget, Gate, KuCoin + Coinglass (Binance)\n"
            "• Алгоритм: Детекция «Полки» ➔ Всплеск Vol/OI ➔ Каскадный фильтр (2+ биржи)\n"
            "• Окно рассинхрона: 3 минуты\n\n"
            "<i>Ожидаю аномалии на рынке...</i>"
        )
        await self.send_telegram(session, text)

    async def send_telegram(self, session: ClientSession, text: str):
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            await session.post(url, json=payload, timeout=5)
        except Exception as e:
            logging.error(f"Ошибка отправки в Telegram: {e}")

    # --- 2. НОРМАЛИЗАЦИЯ И МЭТЧИНГ ТИКЕРОВ ---
    def normalize_ticker(self, raw_symbol: str) -> str:
        """Приводит тикер к единому стандарту CEX (например: CAPUSDT)"""
        clean = raw_symbol.replace("$", "").upper().replace("-", "").replace("_", "")
        if not clean.endswith("USDT"):
            clean += "USDT"
        return clean

    # --- 3. ПРОВЕРКА ПОЛКИ И ВСБЛЕСКА НА CEX ---
    async def check_bitget(self, session: ClientSession, symbol: str) -> bool:
        url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&granularity=5m&limit=7&productType=USDT-FUTURES"
        try:
            async with session.get(url, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    klines = data.get("data", [])
                    if len(klines) < 7:
                        return False
                    
                    closes = [float(k[4]) for k in klines]
                    vols = [float(k[6]) for k in klines]
                    
                    # Проверка полки (за последние 30 мин волатильность < 2%)
                    range_pct = (max(closes[1:]) - min(closes[1:])) / min(closes[1:]) * 100
                    avg_vol = sum(vols[1:]) / len(vols[1:]) if len(vols[1:]) > 0 else 1.0
                    curr_vol = vols[0]

                    # Условие: полка <= 2% И спайк объема >= x4
                    if range_pct <= 2.0 and curr_vol / avg_vol >= 4.0:
                        return True
        except Exception:
            pass
        return False

    async def check_gate(self, session: ClientSession, symbol: str) -> bool:
        # Аналогичная легкая проверка для Gate.io
        url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval=5m&limit=7"
        try:
            async with session.get(url, timeout=3) as resp:
                if resp.status == 200:
                    klines = await resp.json()
                    if len(klines) < 7:
                        return False
                    vols = [float(k["v"]) for k in klines]
                    avg_vol = sum(vols[:-1]) / len(vols[:-1]) if len(vols[:-1]) > 0 else 1.0
                    return (vols[-1] / avg_vol) >= 4.0
        except Exception:
            pass
        return False

    # --- 4. ЗАПРОС К COINGLASS (ФИНАЛЬНЫЙ ВАЛИДАТОР) ---
    async def fetch_coinglass_data(self, session: ClientSession, symbol: str) -> dict:
        """Вызывается ТОЛЬКО при совпадении 2+ бирж"""
        clean_base = symbol.replace("USDT", "")
        url = f"https://open-api.coinglass.com/public/v2/indicator/open_interest?symbol={clean_base}&currency=USDT"
        headers = {"coinglassSecret": COINGLASS_API_KEY}
        
        try:
            async with session.get(url, headers=headers, timeout=4) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    data = res.get("data", [])
                    if data:
                        return {
                            "agg_oi": float(data[-1].get("openInterest", 0)),
                            "agg_vol": float(data[-1].get("volUsd", 0))
                        }
        except Exception as e:
            logging.error(f"Coinglass API error: {e}")
        return {"agg_oi": 0.0, "agg_vol": 0.0}

    # --- 5. ОБРАБОТКА СИГНАЛОВ И БУФЕР РАССИНХРОНА ---
    async def process_symbol(self, session: ClientSession, raw_symbol: str):
        symbol = self.normalize_ticker(raw_symbol)
        now = time.time()

        # Cooldown: пропускаем, если недавно уже отправляли алерт
        if now - self.cooldown_cache.get(symbol, 0) < 900: # 15 минут
            return

        # Проверяем спайк на биржах
        bg_spike, gate_spike = await asyncio.gather(
            self.check_bitget(session, symbol),
            self.check_gate(session, symbol)
        )

        if symbol not in self.trigger_buffer:
            self.trigger_buffer[symbol] = {}

        if bg_spike:
            self.trigger_buffer[symbol]["bitget"] = now
        if gate_spike:
            self.trigger_buffer[symbol]["gate"] = now

        # Очистка устаревших меток (старше 3 минут / 180 секунд)
        self.trigger_buffer[symbol] = {
            ex: ts for ex, ts in self.trigger_buffer[symbol].items() 
            if now - ts <= 180
        }

        # Каскад сработал: минимум 2 биржи подтвердили аномалию в пределах 3 минут!
        if len(self.trigger_buffer[symbol]) >= 2:
            self.cooldown_cache[symbol] = now
            del self.trigger_buffer[symbol] # Сброс буфера

            # Точечный запрос к Coinglass
            cg_data = await self.fetch_coinglass_data(session, symbol)
            
            # Расчет дельты OI
            last_oi = self.oi_cache.get(symbol, cg_data["agg_oi"])
            oi_delta_pct = ((cg_data["agg_oi"] - last_oi) / last_oi * 100) if last_oi > 0 else 0.0
            self.oi_cache[symbol] = cg_data["agg_oi"]

            # ФОРМИРОВАНИЕ АЛЕРТА С КОПИРУЕМЫМ ТИКЕРОМ
            base_coin = symbol.replace("USDT", "")
            alert_text = (
                f"🚨 <b>АНОМАЛИЯ ИМПУЛЬСА (CEX Cascade)</b>\n\n"
                f"Монета: <code>{symbol}</code> (Нажми для копирования)\n"
                f"├ Агрегированный Vol (5m): <b>${cg_data['agg_vol']:,.0f}</b>\n"
                f"├ Агрегированный OI: <b>${cg_data['agg_oi']:,.0f}</b> ({oi_delta_pct:+.2f}%)\n"
                f"└ Паттерн: <b>Выход из полки + Каскадный занос</b>\n\n"
                f"🔗 <a href='https://www.coinglass.com/tv/Binance_{symbol}'>Coinglass Chart</a> | "
                f"<a href='https://dexscreener.com/search?q={base_coin}'>DexScreener</a>"
            )
            await self.send_telegram(session, alert_text)

# --- 6. ПИНГ-СЕРВЕР ДЛЯ БЕСПЛАТНОГО RENDER ---
async def handle_ping(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/check', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 10000)
    await site.start()

# --- MAIN LOOP ---
async def main():
    bot = DEXCEXAggregatorBot()
    
    # Запускаем веб-сервер для UptimeRobot
    await start_web_server()
    
    symbols_to_watch = ["CAPUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    async with ClientSession() as session:
        # Отправляем приветствие в Telegram при запуске
        await bot.send_startup_message(session)
        
        while True:
            tasks = [bot.process_symbol(session, sym) for sym in symbols_to_watch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(15) # Цикл сканирования 15 сек

if __name__ == "__main__":
    asyncio.run(main())
