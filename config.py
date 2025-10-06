# config.py
import os
from dotenv import load_dotenv

# Загрузим .env из текущей директории (если есть)
load_dotenv()

def _get_env(name, default=None, cast=str):
    v = os.getenv(name, default)
    if v is None:
        return None
    if cast is bool:
        return str(v).lower() in ("1", "true", "yes", "on")
    try:
        return cast(v)
    except Exception:
        return default

# --- Секреты и ключи ---
TELEGRAM_TOKEN = _get_env("TELEGRAM_TOKEN", None, str)
BINANCE_API_KEY = _get_env("BINANCE_API_KEY", None, str)
BINANCE_API_SECRET = _get_env("BINANCE_API_SECRET", None, str)
MEXC_API_KEY = _get_env("MEXC_API_KEY", None, str)
MEXC_API_SECRET = _get_env("MEXC_API_SECRET", None, str)

# --- Настройки ---
PRICE_CHECK_INTERVAL = _get_env("PRICE_CHECK_INTERVAL", 60, int)
SPREAD_THRESHOLD_PERCENT = _get_env("SPREAD_THRESHOLD_PERCENT", 5.0, float)
ALERT_COOLDOWN_MINUTES = _get_env("ALERT_COOLDOWN_MINUTES", 1, int)
SPREAD_CHANGE_THRESHOLD = _get_env("SPREAD_CHANGE_THRESHOLD", 0.2, float)

# ADMINS — строка с id через запятую
_admins_raw = os.getenv("ADMINS", "")
ADMINS = []
if _admins_raw:
    for p in _admins_raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            ADMINS.append(int(p))
        except Exception:
            # игнорируем нечисловые значения
            pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Примитивная валидация, чтобы не запускать бот без токена ---
if not TELEGRAM_TOKEN:
    raise RuntimeError(
        "TELEGRAM_TOKEN не задан. Создай файл .env в корне проекта или установи переменную окружения TELEGRAM_TOKEN."
)

def debug_summary():
    """Короткая безопасная сводка для проверки загрузки (НЕ выводит секреты)."""
    return {
        "has_telegram_token": bool(TELEGRAM_TOKEN),
        "admins": ADMINS,
        "price_check_interval": PRICE_CHECK_INTERVAL,
        "spread_threshold_percent": SPREAD_THRESHOLD_PERCENT,
        "alert_cooldown_minutes": ALERT_COOLDOWN_MINUTES,
        "log_level": LOG_LEVEL,
    }