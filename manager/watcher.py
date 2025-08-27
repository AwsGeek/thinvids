#!/usr/bin/env python3
import os
import time
import threading
import logging
import hashlib
import requests
import fcntl
from concurrent.futures import ThreadPoolExecutor

# --- Optional watchdog (can be disabled by env) ---
USE_WATCHDOG = os.getenv("USE_WATCHDOG", "1").strip().lower() in ("1", "true", "yes", "on")
if USE_WATCHDOG:
    try:
        from watchdog.observers.polling import PollingObserver as Observer
        from watchdog.events import PatternMatchingEventHandler
    except Exception:
        USE_WATCHDOG = False

# ----------------- Config -----------------
WATCH_ROOT         = os.getenv("WATCH_ROOT", "/watch")
SUBMIT_URL         = os.getenv("SUBMIT_URL", "http://localhost:5005/add_job")

USE_SCANNER        = os.getenv("USE_SCANNER", "1").strip().lower() in ("1", "true", "yes", "on")
SCAN_INTERVAL_SEC  = int(os.getenv("SCAN_INTERVAL_SEC", "60"))

STABLE_CHECKS      = int(os.getenv("STABLE_CHECKS", "5"))
STABLE_DELAY_SEC   = int(os.getenv("STABLE_DELAY_SEC", "10"))

MAX_WORKERS        = int(os.getenv("WORKERS", "4"))

# Processed log on shared NFS
PROCESSED_FILE     = os.getenv("PROCESSED_FILE", "/config/processed.log")

VIDEO_EXTS = {".mkv", ".mp4"}  # case-insensitive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [watcher] %(message)s")
logger = logging.getLogger("watcher")

# ------------------ HTTP session ------------------
_session = requests.Session()

# ------------------ In-memory state ----------------
_index_lock = threading.Lock()
_seen = {}          # path -> (size, mtime)
_submitted = set()  # paths successfully submitted in THIS runtime
_pending = set()    # paths currently stabilizing/submitting in THIS runtime


# ===================================================
#                   PROCESSED STORE (file-only)
# ===================================================
class FileProcessedStore:
    """
    File-backed processed ledger at PROCESSED_FILE.
    - Keeps an in-memory set for fast membership checks.
    - Uses fcntl.flock to serialize appends across multiple workers.
    - Detects external updates by reloading when file mtime grows.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._set = set()
        self._lock = threading.Lock()
        self._mtime = 0.0
        self._ensure_dir()
        self._load_full(initial=True)

    def _ensure_dir(self):
        d = os.path.dirname(self.file_path) or "."
        os.makedirs(d, exist_ok=True)

    def _load_full(self, initial=False):
        try:
            st = os.stat(self.file_path)
            mtime = st.st_mtime
        except FileNotFoundError:
            if initial:
                logger.info(f"Processed store (file): {self.file_path} not found; will create on bootstrap")
            return

        with open(self.file_path, "r", encoding="utf-8") as f:
            # shared lock for read is optional; NFS usually serializes writes with LOCK_EX below
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            except Exception:
                pass
            lines = [line.strip() for line in f if line.strip()]
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass

        with self._lock:
            self._set = set(lines)
            self._mtime = mtime

        if initial:
            logger.info(f"Processed store (file): loaded {len(self._set)} entries from {self.file_path}")
        else:
            logger.debug(f"Processed store reloaded ({len(self._set)} entries)")

    def _maybe_reload(self):
        try:
            st = os.stat(self.file_path)
            mtime = st.st_mtime
        except FileNotFoundError:
            return
        if mtime > self._mtime:
            self._load_full()

    def contains(self, rel_path: str) -> bool:
        self._maybe_reload()
        with self._lock:
            return rel_path in self._set

    def add(self, rel_path: str) -> None:
        # Fast path: already in memory -> done
        self._maybe_reload()
        with self._lock:
            if rel_path in self._set:
                return

        # Append with exclusive lock to avoid races cross-host
        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        with open(self.file_path, "a+", encoding="utf-8") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass

            # Double-check after lock in case another process added it
            f.seek(0)
            existing = {line.strip() for line in f if line.strip()}
            if rel_path not in existing:
                f.write(rel_path + "\n")
                f.flush()
                os.fsync(f.fileno())
                existing.add(rel_path)

            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass

        with self._lock:
            self._set = existing
            try:
                self._mtime = os.stat(self.file_path).st_mtime
            except Exception:
                pass

    def add_many(self, rel_paths) -> None:
        to_add = set(p for p in rel_paths if p)
        if not to_add:
            return

        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        with open(self.file_path, "a+", encoding="utf-8") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass

            f.seek(0)
            existing = {line.strip() for line in f if line.strip()}
            new_lines = [p for p in to_add if p not in existing]
            if new_lines:
                f.write("\n".join(new_lines) + "\n")
                f.flush()
                os.fsync(f.fileno())
                existing.update(new_lines)

            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass

        with self._lock:
            self._set = existing
            try:
                self._mtime = os.stat(self.file_path).st_mtime
            except Exception:
                pass

    def count(self) -> int:
        self._maybe_reload()
        with self._lock:
            return len(self._set)


# ===================================================
#                    HELPERS
# ===================================================
def is_video_file(path: str) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() in VIDEO_EXTS

def rel_from_watch(path: str) -> str:
    try:
        return os.path.relpath(path, WATCH_ROOT).replace("\\", "/")
    except ValueError:
        return path


# ===================================================
#           STABILIZE & SUBMIT PIPELINE
# ===================================================
def submit_job_if_stable(path: str, store: FileProcessedStore):
    """
    Stabilize (size constant for STABLE_CHECKS*STABLE_DELAY_SEC)
    then POST to SUBMIT_URL once. On success, mark as processed in the store.
    """
    rel = rel_from_watch(path)

    # Skip if already processed (durable)
    if store.contains(rel):
        with _index_lock:
            _pending.discard(path)
        return

    filename = os.path.basename(path)
    logger.info(f"Stabilizing: {filename}")

    last_size = -1
    stable_count = 0
    while True:
        try:
            cur_size = os.path.getsize(path)
        except FileNotFoundError:
            logger.warning(f"File vanished: {path}")
            with _index_lock:
                _pending.discard(path)
            return

        if cur_size == last_size and cur_size > 0:
            stable_count += 1
            if stable_count >= STABLE_CHECKS:
                break
        else:
            stable_count = 0
            last_size = cur_size
        time.sleep(STABLE_DELAY_SEC)

    # Submit
    try:
        payload = {"filename": rel}
        logger.info(f"Submitting job for {rel}")
        r = _session.post(SUBMIT_URL, json=payload, timeout=20)
        if r.ok:
            logger.info(f"Job submitted ({r.status_code}) for {rel}")
            store.add(rel)
            with _index_lock:
                _submitted.add(path)
        else:
            logger.error(f"Submit failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.exception(f"Submit error for {rel}: {e}")
    finally:
        with _index_lock:
            _pending.discard(path)

def schedule_submit(executor: ThreadPoolExecutor, path: str, store: FileProcessedStore):
    rel = rel_from_watch(path)
    # Skip early if already processed (persisted)
    if store.contains(rel):
        return
    with _index_lock:
        if path in _submitted or path in _pending:
            return
        _pending.add(path)
    executor.submit(submit_job_if_stable, path, store)


# ===================================================
#                 BOOTSTRAP & SCANNER
# ===================================================
def initial_index_scan():
    """
    Build the initial _seen index so we don't instantly re-process
    a huge directory on startup. We do NOT submit here.
    """
    count = 0
    t0 = time.time()
    for root, dirs, files in os.walk(WATCH_ROOT):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            p = os.path.join(root, name)
            if not is_video_file(p):
                continue
            try:
                st = os.stat(p)
            except FileNotFoundError:
                continue
            with _index_lock:
                _seen[p] = (st.st_size, st.st_mtime)
            count += 1
    logger.info(f"Initial index: {count} files indexed in {time.time()-t0:.2f}s")

def bootstrap_processed_if_first_run(store: FileProcessedStore):
    """
    If processed store is empty, assume this is the first run:
    add ALL current video files as 'processed' WITHOUT submitting.
    """
    if store.count() > 0:
        return

    logger.info("Processed store is empty -> first run. Bootstrapping...")
    rels = []
    for root, dirs, files in os.walk(WATCH_ROOT):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            p = os.path.join(root, name)
            if is_video_file(p):
                rels.append(rel_from_watch(p))
    store.add_many(rels)
    logger.info(f"Bootstrap complete: marked {len(rels)} existing files as processed (no submissions)")

def periodic_scanner(executor: ThreadPoolExecutor, interval: int, store: FileProcessedStore):
    """
    Every `interval` seconds, scan recursively:
      - New video files not in processed store -> schedule submit
      - Files that changed (size/mtime) and not in processed store -> schedule submit
    """
    logger.info(f"Scanner loop running every {interval}s (root={WATCH_ROOT})")
    while True:
        t0 = time.time()
        current = {}  # path -> (size, mtime)
        candidates = []

        for root, dirs, files in os.walk(WATCH_ROOT):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for name in files:
                if name.startswith("."):
                    continue
                p = os.path.join(root, name)
                if not is_video_file(p):
                    continue
                try:
                    st = os.stat(p)
                except FileNotFoundError:
                    continue
                cur = (st.st_size, st.st_mtime)
                current[p] = cur

                with _index_lock:
                    prev = _seen.get(p)

                rel = rel_from_watch(p)
                if store.contains(rel):
                    # already processed; skip
                    continue

                if prev is None:
                    # new file discovered
                    candidates.append(p)
                else:
                    # changed if size or mtime moved (and not processed)
                    if cur != prev:
                        candidates.append(p)

        # Update index
        with _index_lock:
            _seen.clear()
            _seen.update(current)

        # Schedule stabilization/submission for candidates
        for p in candidates:
            schedule_submit(executor, p, store)

        dt = time.time() - t0
        time.sleep(max(0.0, interval - dt))


# ===================================================
#                 WATCHDOG HANDLER
# ===================================================
if USE_WATCHDOG:
    class VideoFileHandler(PatternMatchingEventHandler):
        def __init__(self, executor: ThreadPoolExecutor, store: FileProcessedStore):
            super().__init__(patterns=['*.mkv', '*.mp4'], ignore_directories=True, case_sensitive=False)
            self.executor = executor
            self.store = store

        def on_created(self, event):
            path = event.src_path
            if not is_video_file(path):
                return
            logger.info(f"Watchdog created: {path}")
            schedule_submit(self.executor, path, self.store)

        def on_modified(self, event):
            path = event.src_path
            if not is_video_file(path):
                return
            logger.info(f"Watchdog modified: {path}")
            schedule_submit(self.executor, path, self.store)


# ===================================================
#                     ENTRYPOINT
# ===================================================
def start_watcher():
    logger.info(f"Watching root: {WATCH_ROOT}")
    os.makedirs(WATCH_ROOT, exist_ok=True)
    # Make sure /config exists (NFS mount managed by Ansible)
    try:
        os.makedirs(os.path.dirname(PROCESSED_FILE) or "/config", exist_ok=True)
    except Exception:
        pass

    # Build processed store (file only)
    store = FileProcessedStore(PROCESSED_FILE)

    # Thread pool for stabilize/submit tasks
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    # Build baseline size/mtime index (no submissions)
    # initial_index_scan()

    # First-run bootstrap: mark all existing files as processed (no submits)
    bootstrap_processed_if_first_run(store)

    # Start periodic scanner (optional)
    if USE_SCANNER:
        th = threading.Thread(target=periodic_scanner, args=(executor, SCAN_INTERVAL_SEC, store), daemon=True)
        th.start()

    # Start watchdog (optional)
    observer = None
    if USE_WATCHDOG:
        try:
            handler = VideoFileHandler(executor, store)
            observer = Observer()
            observer.schedule(handler, WATCH_ROOT, recursive=True)
            observer.start()
            logger.info("Watchdog PollingObserver started.")
        except Exception as e:
            logger.error(f"Failed to start watchdog: {e}")
            observer = None

    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down watcher...")
    finally:
        if observer:
            observer.stop()
            observer.join(timeout=5)
        executor.shutdown(wait=False)

if __name__ == "__main__":
    start_watcher()
