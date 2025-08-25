# watcher.py
import os
import time
import logging
import requests
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import PatternMatchingEventHandler

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [watcher] %(message)s",
)
log = logging.getLogger("watcher")

WATCH_ROOT = os.getenv("WATCH_ROOT", "/watch").rstrip("/")
# Prefer explicit ADD_JOB_URL, otherwise derive from manager port on localhost
MANAGER_HTTP_PORT = os.getenv("MANAGER_HTTP_PORT", "5005")
ADD_JOB_URL = os.getenv("ADD_JOB_URL", f"http://127.0.0.1:{MANAGER_HTTP_PORT}/add_job")

STABILIZE_CHECKS = int(os.getenv("STABILIZE_CHECKS", "5"))   # how many consecutive same-size checks
STABILIZE_DELAY  = int(os.getenv("STABILIZE_DELAY", "10"))   # seconds between checks
REQUEST_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT", "5"))  # POST timeout

class VideoFileHandler(PatternMatchingEventHandler):
    def __init__(self):
        super().__init__(
            patterns=['*.mkv', '*.mp4'],
            ignore_directories=True,
            case_sensitive=False
        )

    def on_created(self, event):
        path = event.src_path
        if not path:
            return
        if not path.startswith(WATCH_ROOT):
            log.debug("Ignoring create outside WATCH_ROOT: %s", path)
            return

        rel = os.path.relpath(path, WATCH_ROOT).lstrip("./")
        log.info("File created: %s (rel=%s) — waiting for stabilization…", path, rel)

        if self.wait_until_stable(path):
            log.info("Stable file: %s — submitting job…", rel)
            try:
                r = requests.post(
                    ADD_JOB_URL,
                    json={"filename": rel},
                    timeout=REQUEST_TIMEOUT,
                )
                if r.ok:
                    log.info("Job submitted OK (%s): %s", r.status_code, r.text[:200])
                else:
                    log.warning("Manager returned %s for %s: %s", r.status_code, ADD_JOB_URL, r.text[:200])
            except Exception as e:
                log.error("Failed to submit job for %s: %s", rel, e)
        else:
            log.warning("File never stabilized: %s — skipping", rel)

    def wait_until_stable(self, path, checks=STABILIZE_CHECKS, delay=STABILIZE_DELAY):
        last_size = -1
        stable = 0
        while True:
            try:
                cur = os.path.getsize(path)
            except FileNotFoundError:
                return False
            if cur == last_size:
                stable += 1
                if stable >= checks:
                    return True
            else:
                stable = 0
                last_size = cur
            time.sleep(delay)

def start_watcher():
    log.info("Watching %s for *.mkv/*.mp4 (polling observer for NFS)", WATCH_ROOT)
    handler = VideoFileHandler()
    observer = Observer()
    observer.schedule(handler, WATCH_ROOT, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()
