# updater.py
# Simple script to pull latest code and restart the process (systemd recommended).
import os, sys, subprocess, time, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updater")

def git_pull(repo_dir="."):
    try:
        res = subprocess.run(["git","pull"], cwd=repo_dir, capture_output=True, text=True, timeout=60)
        logger.info("git pull stdout:\n" + res.stdout)
        logger.info("git pull stderr:\n" + res.stderr)
        return res.returncode == 0
    except Exception as e:
        logger.exception("git pull failed: %s", e)
        return False

if __name__ == "__main__":
    repo = os.path.dirname(os.path.abspath(__file__))
    ok = git_pull(repo)
    if ok:
        logger.info("Update successful. If you're running under systemd, run: sudo systemctl restart spread-bot")
    else:
        logger.error("Update failed.")