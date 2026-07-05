# ИСПОЛЬЗУЕМ ОФИЦИАЛЬНЫЙ СТАБИЛЬНЫЙ ОБРАЗ PYTHON
FROM python:3.11-slim

# УСТАНАВЛИВАЕМ РАБОЧУЮ ДИРЕКТОРИЮ ВНУТРИ КОНТЕЙНЕРА
WORKDIR /app

# КОПИРУЕМ ФАЙЛ ЗАВИСИМОСТЕЙ И УСТАНАВЛИВАЕМ ИХ
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# КОПИРУЕМ ВСЕ ОСТАЛЬНЫЕ ФАЙЛЫ ПРОЕКТА
COPY . .

# ОТКРЫВАЕМ ПОРТ, КОТОРЫЙ ИСПОЛЬЗУЕТ WEB-СЕРВЕР AIOHTTP (ПО УМОЛЧАНИЮ НА RENDER)
EXPOSE 7860

# КОМАНДА ДЛЯ ЗАПУСКА БОТА
CMD ["python", "crypto_pulse_v1.py"]
