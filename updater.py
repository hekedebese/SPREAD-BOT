# updater.py — безопасная версия
import os
import subprocess
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updater")

REPO_PATH = os.path.dirname(os.path.abspath(__file__))

def safe_update():
    print("⚠️ Автоматическое обновление выключено ради безопасности.")
    print("Чтобы обновить вручную, сделай на сервере:")
    print(f"cd {REPO_PATH} && git pull && sudo systemctl restart spread-bot")
    print()
    print("Или просто перезапусти бота вручную:")
    print("sudo systemctl restart spread-bot")
    sys.exit(0)

if __name__ == "__main__":
    safe_update()
