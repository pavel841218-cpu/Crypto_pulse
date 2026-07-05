FROM python:3.11-slim
WORKDIR /code
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY . .
CMD ["python", "crypto_pulse_v1.py"]
