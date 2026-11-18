FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bot.py /app/bot.py
COPY bot_logging.py /app/bot_logging.py

RUN chmod 0755 /app \
    && chmod 0644 /app/bot.py \
    && chmod 0644 /app/bot_logging.py \
    && mkdir -p /data \
    && chown 10001:10001 /data \
    && chmod 0700 /data

USER 10001:10001

CMD ["python", "/app/bot.py"]