#!/usr/bin/env python3
import os
import json
import time
import threading
import logging
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
ADOPT_EXISTING_PROCESSED_ON_STARTUP = os.getenv(
    "ADOPT_EXISTING_PROCESSED_ON_STARTUP", "0"
).strip().lower() in ("1", "true", "yes", "on")
ADOPT_MARKER_FILE = os.getenv("ADOPT_EXISTING_PROCESSED_MARKER", "").strip()
PROCESSED_PATH_ALIASES = os.getenv("PROCESSED_PATH_ALIASES", "").strip()

VIDEO_EXTS = {".mkv", ".mp4"}  # case-insensitive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [watcher] %(message)s")
logger = logging.getLogger("watcher")

# ------------------ HTTP session ------------------
_session = requests.Session()

# ------------------ In-memory state ----------------
_index_lock = threading.Lock()
_seen = {}          # path -> (size, mtime)
_submitted = {}     # path -> signature successfully submitted in THIS runtime
_pending = set()    # paths currently stabilizing/submitting in THIS runtime


# ===================================================
#               FILE SIGNATURE HELPERS
# ===================================================
def signature_from_stat(st: os.stat_result) -> str:
    mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
    return f"{int(st.st_size)}:{int(mtime_ns)}"


def signature_for_path(path: str):
    try:
        return signature_from_stat(os.stat(path))
    except FileNotFoundError:
        return None


# ===================================================
#                   PROCESSED STORE (file-only)
# ===================================================
class FileProcessedStore:
    """
    File-backed processed ledger at PROCESSED_FILE.
    - Keeps an in-memory map of relative path -> signature.
    - Uses fcntl.flock to serialize appends across multiple workers.
    - Detects external updates by reloading when file mtime grows.
    - Accepts both legacy path-only lines and newer JSON path+signature lines.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._entries = {}
        self._adopted = {}
        self._lock = threading.Lock()
        self._mtime = 0.0
        self._ensure_dir()
        self._load_full(initial=True)

    def _ensure_dir(self):
        d = os.path.dirname(self.file_path) or "."
        os.makedirs(d, exist_ok=True)

    def _parse_line(self, line: str):
        line = (line or "").strip()
        if not line:
            return (None, None)
        if line.startswith("{"):
            try:
                payload = json.loads(line)
            except Exception:
                logger.warning("Processed store: skipping unreadable JSON line in %s", self.file_path)
                return (None, None)
            rel_path = str(payload.get("path") or "").strip()
            raw_sig = payload.get("sig")
            signature = str(raw_sig).strip() if raw_sig not in (None, "") else None
            return (rel_path or None, signature)
        return (line, None)

    def _append_record(self, rel_path: str, signature: str):
        record = json.dumps({"path": rel_path, "sig": signature}, separators=(",", ":"))
        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        with open(self.file_path, "a+", encoding="utf-8") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass
            f.write(record + "\n")
            f.flush()
            os.fsync(f.fileno())
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            self._mtime = os.stat(self.file_path).st_mtime
        except Exception:
            pass

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

        entries = {}
        for line in lines:
            rel_path, signature = self._parse_line(line)
            if not rel_path:
                continue
            entries[rel_path] = signature

        with self._lock:
            for rel_path, signature in self._adopted.items():
                if entries.get(rel_path) is None:
                    entries[rel_path] = signature
            self._entries = entries
            self._mtime = mtime
            legacy_count = sum(1 for sig in entries.values() if sig is None)
            concrete_count = len(entries) - legacy_count

        if initial:
            logger.info(
                "Processed store (file): loaded %d entries from %s (%d legacy, %d signed)",
                len(entries),
                self.file_path,
                legacy_count,
                concrete_count,
            )
        else:
            logger.debug("Processed store reloaded (%d entries)", len(entries))

    def _maybe_reload(self):
        try:
            st = os.stat(self.file_path)
            mtime = st.st_mtime
        except FileNotFoundError:
            return
        if mtime > self._mtime:
            self._load_full()

    def state_for(self, rel_path: str, signature: str) -> str:
        self._maybe_reload()
        with self._lock:
            if rel_path not in self._entries:
                return "missing"
            stored = self._entries.get(rel_path)
            if stored is None:
                return "legacy"
            if signature and stored == signature:
                return "matched"
            return "changed"

    def adopt_legacy(self, rel_path: str, signature: str) -> bool:
        if not rel_path or not signature:
            return False
        self._maybe_reload()
        with self._lock:
            current = self._entries.get(rel_path)
            if current is not None:
                return False
            self._entries[rel_path] = signature
            self._adopted[rel_path] = signature
            return True

    def add(self, rel_path: str, signature: str) -> None:
        if not rel_path or not signature:
            return

        changed = False
        self._maybe_reload()
        with self._lock:
            if self._entries.get(rel_path) != signature:
                self._entries[rel_path] = signature
                self._adopted.pop(rel_path, None)
                changed = True
        if changed:
            self._append_record(rel_path, signature)

    def add_many(self, rel_entries) -> None:
        normalized = []
        seen = set()
        for rel_path, signature in rel_entries:
            rel_path = str(rel_path or "").strip()
            signature = str(signature or "").strip()
            if not rel_path or not signature or rel_path in seen:
                continue
            normalized.append((rel_path, signature))
            seen.add(rel_path)
        if not normalized:
            return

        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        with open(self.file_path, "a+", encoding="utf-8") as f:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass
            for rel_path, signature in normalized:
                record = json.dumps({"path": rel_path, "sig": signature}, separators=(",", ":"))
                f.write(record + "\n")
            f.flush()
            os.fsync(f.fileno())
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass

        with self._lock:
            for rel_path, signature in normalized:
                self._entries[rel_path] = signature
                self._adopted.pop(rel_path, None)
        try:
            self._mtime = os.stat(self.file_path).st_mtime
        except Exception:
            pass

    def count(self) -> int:
        self._maybe_reload()
        with self._lock:
            return len(self._entries)


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


def _normalize_rel_path(path: str) -> str:
    return str(path or "").strip().replace("\\", "/").strip("/")


def processed_path_aliases():
    """
    Return configured current-prefix -> legacy-prefix aliases.

    Format: PROCESSED_PATH_ALIASES="tv=television,series=television"
    """
    aliases = []
    for raw in PROCESSED_PATH_ALIASES.split(","):
        raw = raw.strip()
        if not raw or "=" not in raw:
            continue
        current, legacy = raw.split("=", 1)
        current = _normalize_rel_path(current)
        legacy = _normalize_rel_path(legacy)
        if current and legacy:
            aliases.append((current, legacy))
    return aliases


def processed_path_candidates(rel_path: str):
    rel_path = _normalize_rel_path(rel_path)
    yielded = set()

    def emit(candidate):
        candidate = _normalize_rel_path(candidate)
        if candidate and candidate not in yielded:
            yielded.add(candidate)
            return candidate
        return None

    first = emit(rel_path)
    if first:
        yield first

    for current, legacy in processed_path_aliases():
        if rel_path == current:
            candidate = legacy
        elif rel_path.startswith(current + "/"):
            candidate = legacy + rel_path[len(current):]
        else:
            continue
        candidate = emit(candidate)
        if candidate:
            yield candidate


def processed_state_for_rel(store: FileProcessedStore, rel_path: str, signature: str):
    for candidate in processed_path_candidates(rel_path):
        state = store.state_for(candidate, signature)
        if state != "missing":
            return (state, candidate)
    return ("missing", _normalize_rel_path(rel_path))


def adopt_marker_file() -> str:
    if ADOPT_MARKER_FILE:
        return ADOPT_MARKER_FILE
    safe_root = WATCH_ROOT.strip(os.sep).replace(os.sep, "_") or "root"
    return f"{PROCESSED_FILE}.{safe_root}.adopted"


# ===================================================
#           STABILIZE & SUBMIT PIPELINE
# ===================================================
def submit_job_if_stable(path: str, store: FileProcessedStore):
    """
    Stabilize (size constant for STABLE_CHECKS*STABLE_DELAY_SEC)
    then POST to SUBMIT_URL once. On success, mark as processed in the store.
    """
    rel = rel_from_watch(path)
    initial_sig = signature_for_path(path)
    state, ledger_rel = processed_state_for_rel(store, rel, initial_sig)

    # Skip if already processed (durable), or lazily adopt legacy entries.
    if state == "matched":
        if ledger_rel != rel and initial_sig:
            store.add(rel, initial_sig)
        with _index_lock:
            _pending.discard(path)
        return
    if state == "legacy":
        if initial_sig:
            store.add(rel, initial_sig)
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
        final_sig = signature_for_path(path)
        if not final_sig:
            logger.warning(f"File vanished before submit: {path}")
            return
        final_state, final_ledger_rel = processed_state_for_rel(store, rel, final_sig)
        if final_state == "matched":
            if final_ledger_rel != rel:
                store.add(rel, final_sig)
            logger.info(f"Skipping unchanged processed file: {rel}")
            return
        if final_state == "legacy":
            store.add(rel, final_sig)
            logger.info(f"Skipping legacy processed file during migration: {rel}")
            return
        payload = {"filename": rel}
        logger.info(f"Submitting job for {rel}")
        r = _session.post(SUBMIT_URL, json=payload, timeout=20)
        if r.ok:
            logger.info(f"Job submitted ({r.status_code}) for {rel}")
            if final_sig:
                store.add(rel, final_sig)
            with _index_lock:
                _submitted[path] = final_sig or ""
        else:
            logger.error(f"Submit failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.exception(f"Submit error for {rel}: {e}")
    finally:
        with _index_lock:
            _pending.discard(path)

def schedule_submit(executor: ThreadPoolExecutor, path: str, store: FileProcessedStore):
    rel = rel_from_watch(path)
    sig = signature_for_path(path)
    if not sig:
        return

    state, ledger_rel = processed_state_for_rel(store, rel, sig)
    # Skip early if already processed (persisted)
    if state == "matched":
        if ledger_rel != rel:
            store.add(rel, sig)
        return
    if state == "legacy":
        store.add(rel, sig)
        return

    with _index_lock:
        if path in _pending:
            return
        if _submitted.get(path) == sig:
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
                sig = signature_for_path(p)
                if sig:
                    rels.append((rel_from_watch(p), sig))
    store.add_many(rels)
    logger.info(f"Bootstrap complete: marked {len(rels)} existing files as processed (no submissions)")


def adopt_existing_processed_once(store: FileProcessedStore):
    """
    One-time migration helper for watch-root moves.

    If the same relative path was already processed under an old root, adopting
    the current signature prevents a root migration from looking like every file
    changed. Missing ledger entries are intentionally left alone so genuinely
    new files are still queued.
    """
    if not ADOPT_EXISTING_PROCESSED_ON_STARTUP:
        return
    if store.count() == 0:
        return

    marker = adopt_marker_file()
    if os.path.exists(marker):
        logger.info("Processed adoption already completed; marker exists at %s", marker)
        return

    logger.info(
        "Processed adoption enabled: scanning %s and adopting existing ledger paths",
        WATCH_ROOT,
    )
    t0 = time.time()
    checked = 0
    matched = 0
    adopted = 0
    missing = 0
    rels_to_add = []

    for root, dirs, files in os.walk(WATCH_ROOT):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith("."):
                continue
            p = os.path.join(root, name)
            if not is_video_file(p):
                continue
            sig = signature_for_path(p)
            if not sig:
                continue
            rel = rel_from_watch(p)
            state, _ledger_rel = processed_state_for_rel(store, rel, sig)
            checked += 1
            if state == "matched":
                matched += 1
            elif state in ("legacy", "changed"):
                rels_to_add.append((rel, sig))
                adopted += 1
            else:
                missing += 1

    store.add_many(rels_to_add)
    os.makedirs(os.path.dirname(marker) or ".", exist_ok=True)
    with open(marker, "w", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "watch_root": WATCH_ROOT,
                    "processed_file": PROCESSED_FILE,
                    "checked": checked,
                    "matched": matched,
                    "adopted": adopted,
                    "missing": missing,
                    "created_at": int(time.time()),
                },
                sort_keys=True,
            )
            + "\n"
        )
    logger.info(
        "Processed adoption complete: checked=%d matched=%d adopted=%d missing=%d in %.2fs; marker=%s",
        checked,
        matched,
        adopted,
        missing,
        time.time() - t0,
        marker,
    )

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
                sig = signature_from_stat(st)
                state, ledger_rel = processed_state_for_rel(store, rel, sig)
                if state == "matched":
                    if ledger_rel != rel:
                        store.add(rel, sig)
                    # already processed; skip
                    continue
                if state == "legacy":
                    # Lazy migration: keep existing processed files skipped without
                    # doing a second startup diff or mass rewrite of the ledger.
                    store.add(rel, sig)
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

    # One-time watch-root migration: adopt current signatures for paths that
    # were already in processed.log before enabling the scanner/watchdog.
    adopt_existing_processed_once(store)

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
