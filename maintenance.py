"""
maintenance.py
Добавляет фоновые задачи:
- ежедневное резервное копирование bot.db -> backups/bot_YYYY-MM-DD.db
- ежедневная очистка неактивных пользователей (добавляет last_active, is_active при необходимости)
- ежедневная сводка статистики администраторам (ADMINS берутся из config.py)
- helper safe_request с retries и экспоненциальной задержкой

Как подключить: в main.py импортируй maintenance и запусти
    asyncio.create_task(maintenance.start_background_tasks(bot))
"""

import asyncio
import os
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
import aiohttp
import logging
import math

logger = logging.getLogger("bot")

DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")
BACKUP_DIR = os.path.join(os.path.dirname(__file__), "backups")

# --- Параметры ---
INACTIVITY_DAYS = 30  # считать неактивными пользователей, неактивных больше этого
BACKUP_HOUR_UTC = 0    # час в UTC, когда делать бэкап (по умолчанию полночь UTC)
SUMMARY_HOUR_UTC = 1   # час в UTC, когда отправлять сводку
RETRIES = 3
BASE_DELAY = 1  # секунды

# Простая статистика (инкрементируй в коде бота, или используй эти поля)
stats = {
    "coins_checked": 0,
    "notifications_sent": 0,
    "errors": 0
}

async def safe_request(method, url, session=None, retries=RETRIES, base_delay=BASE_DELAY, **kwargs):
    """
    aiohttp request wrapper с retries и экспоненциальной задержкой.
    Пример: await safe_request('GET', url, params={'k':1})
    Возвращает объект response.json() или None при неудаче.
    """
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        for attempt in range(retries):
            try:
                async with session.request(method, url, **kwargs) as resp:
                    if resp.status == 200:
                        try:
                            return await resp.json()
                        except Exception:
                            return await resp.text()
                    else:
                        logger.warning(f"[safe_request] {method} {url} -> status {resp.status}")
            except Exception as e:
                logger.warning(f"[safe_request] attempt {attempt+1}/{retries} error: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
        return None
    finally:
        if close_session:
            await session.close()

def ensure_backup_dir():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR, exist_ok=True)

async def daily_backup_task():
    """Создаёт бэкап базы каждый день (UTC)"""
    ensure_backup_dir()
    while True:
        now = datetime.now(timezone.utc)
        # next run at BACKUP_HOUR_UTC
        next_run = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
        next_run = next_run.replace(hour=BACKUP_HOUR_UTC, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        logger.info(f"[maintenance] daily_backup_task sleeping {wait:.1f}s until {next_run.isoformat()}")
        await asyncio.sleep(wait)
        # perform backup
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            src = DB_PATH
            dst = os.path.join(BACKUP_DIR, f"bot_{date_str}.db")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                logger.info(f"[maintenance] backup created: {dst}")
            else:
                logger.warning(f"[maintenance] db file not found: {src}")
        except Exception as e:
            logger.exception(f"[maintenance] backup failed: {e}")
        # small sleep to avoid tight loop in case of time skew
        await asyncio.sleep(5)

def add_missing_columns(conn):
    """Добавляет поля last_active и is_active, если их нет"""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cur.fetchall()]
    if "last_active" not in cols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN last_active TIMESTAMP")
            logger.info("[maintenance] Добавлен столбец last_active")
        except Exception as e:
            logger.warning(f"[maintenance] could not add last_active: {e}")
    if "is_active" not in cols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")
            logger.info("[maintenance] Добавлен столбец is_active")
        except Exception as e:
            logger.warning(f"[maintenance] could not add is_active: {e}")
    conn.commit()

async def daily_cleanup_task(inactivity_days=INACTIVITY_DAYS):
    """Отключает уведомления у неактивных пользователей и у кого закончилась подписка"""
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            add_missing_columns(conn)
            cur = conn.cursor()
            # посчитать порог
            threshold = datetime.now(timezone.utc) - timedelta(days=inactivity_days)
            threshold_str = threshold.isoformat(sep=' ', timespec='seconds')
            # Для пользователей, у которых last_active < threshold и subscription == 0, выставим notify=0 и is_active=0
            cur.execute("""
                UPDATE users
                SET notify = 0, is_active = 0
                WHERE (subscription = 0 OR subscription IS NULL)
                  AND (trial_end IS NULL OR trial_end < ?)
                  AND (last_active IS NOT NULL AND last_active < ?)
            """, (datetime.now(timezone.utc).isoformat(), threshold_str))
            changed = cur.rowcount
            if changed:
                logger.info(f"[maintenance] disabled notifications for {changed} inactive users")
            conn.commit()
            conn.close()
        except Exception as e:
            logger.exception(f"[maintenance] cleanup failed: {e}")
        # Sleep 24 hours
        await asyncio.sleep(86400)

async def daily_summary_task(bot, admins, hour_utc=SUMMARY_HOUR_UTC):
    """Отправляет администраторам краткую статистику раз в день (UTC)"""
    while True:
        now = datetime.now(timezone.utc)
        next_run = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
        next_run = next_run.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        logger.info(f"[maintenance] daily_summary_task sleeping {wait:.1f}s until {next_run.isoformat()}")
        await asyncio.sleep(wait)
        try:
            # prepare summary from stats and db
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = cur.fetchone()[0]
            conn.close()
            text = ("📊 Ежедневная сводка по боту\n\n"
                    f"Пользователей в базе: {total_users}\n"
                    f"Проверено монет: {stats.get('coins_checked',0)}\n"
                    f"Отправлено уведомлений: {stats.get('notifications_sent',0)}\n"
                    f"Ошибок: {stats.get('errors',0)}\n")
            for admin in admins:
                try:
                    await bot.send_message(admin, text)
                except Exception as e:
                    logger.warning(f"[maintenance] could not send summary to {admin}: {e}")
            # reset counters
            stats['coins_checked'] = 0
            stats['notifications_sent'] = 0
            stats['errors'] = 0
        except Exception as e:
            logger.exception(f"[maintenance] daily_summary failed: {e}")
        await asyncio.sleep(5)

async def start_background_tasks(bot, admins=None):
    """Запускать при старте бота: создаёт фоновые задачи"""
    if admins is None:
        from config import ADMINS as admins
    # запустить задачи параллельно
    asyncio.create_task(daily_backup_task())
    asyncio.create_task(daily_cleanup_task())
    asyncio.create_task(daily_summary_task(bot, admins))
    logger.info("[maintenance] background tasks started")