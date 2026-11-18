import logging
import os
import re
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo


def clean(value):
    text = str(value)
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if token:
        text = text.replace(token, "[TOKEN]")
    text = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}", "[TOKEN]", text)

    def redact_url(match):
        # conserva host e ultimo segmento del path (es. metodo API), scarta query e token
        host, path = match.group(1), (match.group(2) or "").split("?")[0].rstrip("/")
        last = path.rsplit("/", 1)[-1]
        return f"[URL {host}/{last}]" if last else f"[URL {host}]"

    text = re.sub(r"https?://([^\s/<>\"']+)([^\s<>\"']*)", redact_url, text)
    return text.replace("\r", r"\r").replace("\n", r"\n").replace("\t", r"\t")


def atom(value):
    text = clean(value)
    if not text or re.search(r'\s|[="\\]', text):
        return '"' + text.replace('\\', '\\\\').replace('"', '\\"') + '"'
    return text


class TextFormatter(logging.Formatter):
    def __init__(self, timezone="Europe/Rome"):
        super().__init__()
        self.tz = ZoneInfo(timezone)

    def format(self, record):
        stamp = datetime.fromtimestamp(record.created, self.tz).isoformat(timespec="seconds")
        level = "WARN" if record.levelname == "WARNING" else record.levelname
        fields = " ".join(f"{k}={atom(v)}" for k, v in getattr(record, "fields", {}).items() if v is not None)
        line = f"{stamp} {level:<5} {clean(record.getMessage())}"
        if fields:
            line += " " + fields
        if record.exc_info:
            line += " traceback=" + atom(self.formatException(record.exc_info))
        return line


def setup(timezone="Europe/Rome"):
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), None)
    if not isinstance(level, int):
        raise ValueError("LOG_LEVEL non valido")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(TextFormatter(timezone))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # il polling getUpdates logga una riga INFO per ogni richiesta: solo rumore
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))


def event(level, name, **fields):
    logging.getLogger("treni_bot").log(level, name, extra={"fields": fields})


def unexpected(name, exc, **fields):
    event(logging.ERROR, name, error=type(exc).__name__, detail=clean(exc), **fields)
    logging.getLogger("treni_bot").debug("exception.traceback", extra={"fields": fields}, exc_info=(type(exc), exc, exc.__traceback__))


class FailureTracker:
    def __init__(self, interval=300):
        self.interval = interval
        self.items = {}
        self.lock = threading.Lock()

    def failed(self, key, exc, **fields):
        now = time.monotonic()
        with self.lock:
            item = self.items.get(key)
            if item is None:
                item = self.items[key] = {"start": now, "last": now, "count": 1}
                reason = getattr(exc, "reason", exc)
                event(logging.WARNING, "service.failed", service=key, error=type(reason).__name__, detail=clean(reason), **fields)
            else:
                item["count"] += 1
                if now - item["last"] >= self.interval:
                    event(logging.WARNING, "service.failure_summary", service=key, failures=item["count"], outage_s=round(now-item["start"]), **fields)
                    item["last"] = now

    def recovered(self, key):
        with self.lock:
            item = self.items.pop(key, None)
            if item:
                event(logging.INFO, "service.recovered", service=key, failures=item["count"], outage_s=round(time.monotonic()-item["start"]))
