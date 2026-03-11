from flask import Flask, render_template, render_template_string, request, jsonify, abort, send_file
import uuid
import time
import os
import json
import logging
import humanize
import shutil
from math import ceil
import socket
import re
import struct
from typing import List
import subprocess
import threading

# import backend task planner
from tasks import transcode, stamp

app = Flask(__name__)
app.secret_key = os.urandom(24).hex()

from common import (
    Status,
    get_huey,
    get_redis,
    get_logging,
    get_settings as _get_settings,
    emit_activity,
    fetch_activity,
    fetch_job_activity,
)
huey = get_huey()
redis_client = get_redis()
logger = get_logging("manager")

# ------------------------ Node discovery (fast path) ------------------------

# Nodes are considered "active" if their last metrics timestamp is within this window
ACTIVE_WINDOW_SEC = int(os.getenv("ACTIVE_WINDOW_SEC", "5"))

def get_all_nodes():
    """
    Return ALL known nodes from the persistent mapping published by agents.
    Format: [{'hostname': 'thinman01', 'mac': 'aa:bb:cc:dd:ee:ff'}, ...]
    Source: HGETALL nodes:mac
    """
    out = []
    try:
        mapping = redis_client.hgetall("nodes:mac") or {}
        for host, mac in mapping.items():
            host = (host or "").strip()
            mac  = (mac or "").strip()
            if host and mac:
                out.append({"hostname": host, "mac": mac})
    except Exception as e:
        logger.warning(f"get_all_nodes: failed to read nodes:mac: {e}")
    return out

def _natural_key(host: str):
    m = re.search(r'(\d+)', host or '')
    return (int(m.group(1)) if m else 0, host or '')

def get_active_nodes():
    """
    Return ONLY nodes that are active based on recent metrics within ACTIVE_WINDOW_SEC.
    Uses nodes:mac for the universe, then pipelines HGET(ts) on metrics:node:<host>.
    Format: [{'hostname': 'thinman01', 'mac': 'aa:bb:cc:dd:ee:ff'}, ...]
    """
    mac_map = {n["hostname"]: n["mac"] for n in get_all_nodes()}
    if not mac_map:
        return []

    cutoff = int(time.time()) - ACTIVE_WINDOW_SEC
    hosts = list(mac_map.keys())

    pipe = redis_client.pipeline()
    for h in hosts:
        pipe.hget(f"metrics:node:{h}", "ts")
    ts_vals = pipe.execute()

    result = []
    for h, ts in zip(hosts, ts_vals):
        try:
            t = int(float(ts or 0))
        except Exception:
            t = 0
        if t >= cutoff:
            result.append({"hostname": h, "mac": mac_map[h]})
    return result


def _ensure_one_worker_awake():
    active = get_active_nodes()
    if active:
        return
    # Wake just one known node if possible; else best-effort wake all
    all_nodes = get_all_nodes()
    if all_nodes:
        try:
            nodes_wake_one(all_nodes[0]["hostname"])  # best-effort
        except Exception:
            pass
    else:
        try:
            nodes_wake_all()
        except Exception:
            pass

# ---------- Warm-up & launch helpers (wait for heartbeats/metrics) ----------

# Tunables (can be overridden via env)
CLUSTER_WARMUP_SEC  = int(os.getenv("CLUSTER_WARMUP_SEC", "60"))  # max wait for nodes to appear
MIN_WARMUP_WORKERS  = int(os.getenv("MIN_WARMUP_WORKERS", "3"))   # desired active workers before start
PARTS_PER_WORKER    = int(os.getenv("PARTS_PER_WORKER", "1"))     # hint only
MIN_PARTS           = int(os.getenv("MIN_PARTS", "20"))            # hint only
MAX_PARTS           = int(os.getenv("MAX_PARTS", "20"))          # hint only
ALLOWED_TARGET_HEIGHTS = (720, 1080)
DEFAULT_TARGET_HEIGHT = 1080
LOCAL_PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/projects")
NFS_PROJECT_ROOT = os.getenv("NFS_PROJECT_ROOT", "/library/.thinvids-projects")
PIPELINE_QUEUE_ACTION_TRANSCODE = "TRANSCODE"
PIPELINE_QUEUE_ACTION_STAMP = "STAMP"
PIPELINE_ACTIVE_JOB_KEY = "pipeline:active_job"
PIPELINE_SCHED_LOCK_KEY = "pipeline:scheduler:lock"
PIPELINE_SCHED_LOCK_TTL_SEC = max(5, int(os.getenv("PIPELINE_SCHED_LOCK_TTL_SEC", "30")))
PIPELINE_SCHED_POLL_SEC = max(0.5, float(os.getenv("PIPELINE_SCHED_POLL_SEC", "2")))
_PIPELINE_SCHED_STARTED = False
_PIPELINE_SCHED_GUARD = threading.Lock()

def normalize_target_height(value, default: int = DEFAULT_TARGET_HEIGHT) -> int:
    try:
        h = int(value)
    except Exception:
        h = default
    return h if h in ALLOWED_TARGET_HEIGHTS else default

def get_default_target_height() -> int:
    """
    Read global default target height from controller settings,
    with a legacy fallback key for older deployments.
    """
    settings = {}
    legacy = {}
    try:
        settings = _get_settings() or {}
    except Exception:
        settings = {}
    try:
        legacy = redis_client.hgetall('settings:global') or {}
    except Exception:
        legacy = {}

    return normalize_target_height(
        settings.get("default_target_height", legacy.get("default_target_height")),
        DEFAULT_TARGET_HEIGHT
    )

def _current_active_hostnames() -> List[str]:
    nodes = get_active_nodes()
    hosts = sorted([n["hostname"] for n in nodes], key=_natural_key)
    return hosts

def _wait_for_workers(min_count: int, timeout_sec: int) -> List[str]:
    deadline = time.time() + max(0, int(timeout_sec))
    best: List[str] = []
    while time.time() < deadline:
        cur = _current_active_hostnames()
        if len(cur) >= min_count:
            return cur
        if len(cur) > len(best):
            best = cur
        time.sleep(1)
    return best

def _launch_after_warmup(job_key: str, job_id: str, filename: str):
    """
    Wake the cluster, wait for heartbeats when needed, store audit + parts_hint, then start transcode.
    """
    try:
        # Best-effort WOL
        try:
            nodes_wake_all()
        except Exception as e:
            logger.warning("nodes_wake_all() raised: %s", e)

        # If workers are already active, don't wait the full warmup window.
        existing = _current_active_hostnames()
        if existing:
            seen = existing
        else:
            # Target at most what's realistically available
            total_known = max(1, len(get_all_nodes()))
            wanted = max(1, min(MIN_WARMUP_WORKERS, total_known))
            seen = _wait_for_workers(wanted, CLUSTER_WARMUP_SEC)

        # Compute parts hint
        parts_hint = max(MIN_PARTS, min(MAX_PARTS, max(0, len(seen)) * PARTS_PER_WORKER))

        # Stash info for UI/debug + hint for tasks.transcode
        redis_client.hset(job_key, mapping={
            'warmup_workers_json': json.dumps(seen),
            'warmup_worker_count': len(seen),
            'warmup_wait_s': CLUSTER_WARMUP_SEC,
            'parts_hint': parts_hint,
        })

        # Kick the pipeline
        redis_client.hset(job_key, mapping={'status': Status.WAITING.value, 'waiting_at': time.time()})
        transcode(job_id, f'/watch/{filename}')
    except Exception as e:
        logger.exception("[%s] launch_after_warmup failed", job_id)
        try:
            redis_client.hset(job_key, mapping={'status': Status.FAILED.value, 'error': str(e)})
        except Exception:
            pass

def _float_or(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default

def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in ("1", "true", "yes", "on", "y", "t")

def _as_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

def _as_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def _load_global_settings():
    settings = {}
    try:
        settings.update(redis_client.hgetall("settings:global") or {})
    except Exception:
        pass
    try:
        settings.update(_get_settings() or {})
    except Exception:
        pass
    settings.setdefault("max_source_file_size_gb", "15")
    settings.setdefault("av1_check_enabled", "1")
    settings.setdefault("use_nfs_for_all_files", "0")
    settings.setdefault("large_file_behavior", "reject")
    return settings

def _display_title(filename):
    base = os.path.basename((filename or "").strip())
    if not base:
        return "Unknown"
    return os.path.splitext(base)[0] or base

def _evaluate_job_policy(video_details, settings):
    max_source_file_size_gb = _as_float(settings.get("max_source_file_size_gb", 15), 15.0)
    av1_check_enabled = _as_bool(settings.get("av1_check_enabled", "1"), True)
    use_nfs_for_all_files = _as_bool(settings.get("use_nfs_for_all_files", "0"), False)
    large_file_behavior = str(settings.get("large_file_behavior", "reject") or "reject").strip().lower()
    if large_file_behavior not in ("reject", "nfs"):
        large_file_behavior = "reject"

    source_size = _as_int(video_details.get("source_file_size", 0), 0)
    source_codec = str(video_details.get("source_codec") or "").strip().lower()

    if av1_check_enabled and source_codec in ("av1", "av01"):
        return (
            "av1_rejected",
            "AV1 source rejected by global setting (av1_check_enabled).",
            "local",
            LOCAL_PROJECT_ROOT,
        )

    if use_nfs_for_all_files:
        return (None, None, "nfs", NFS_PROJECT_ROOT)

    max_source_bytes = int(max_source_file_size_gb * 1024 * 1024 * 1024)
    is_large = max_source_bytes > 0 and source_size > max_source_bytes
    if is_large:
        if large_file_behavior == "nfs":
            return (None, None, "nfs", NFS_PROJECT_ROOT)
        return (
            "size_limit",
            f"Source file too large: {humanize.naturalsize(source_size, binary=True)} > {max_source_file_size_gb:g} GiB limit",
            "local",
            LOCAL_PROJECT_ROOT,
        )

    return (None, None, "local", LOCAL_PROJECT_ROOT)

def _job_index_keys() -> List[str]:
    keys = list(redis_client.smembers("jobs:all") or [])
    if keys:
        return keys

    keys = [k for k in redis_client.scan_iter("job:*", count=1000)]
    if keys:
        try:
            redis_client.sadd("jobs:all", *keys)
        except Exception:
            pass
    return keys

def _is_terminal_pipeline_status(status_raw: str) -> bool:
    raw = (status_raw or "").strip().upper()
    if raw == "COMPLETED":
        return True
    try:
        status = Status.parse(raw)
    except Exception:
        return False
    return status in {Status.READY, Status.STOPPED, Status.FAILED, Status.REJECTED, Status.DONE}

def _queue_job_for_dispatch(job_key: str, action: str, started_at: float):
    redis_client.hset(job_key, mapping={
        'status': Status.WAITING.value,
        'queue_action': action,
        'waiting_at': str(time.time()),
        'started_at': str(started_at),
    })
    try:
        job = redis_client.hgetall(job_key) or {}
        job_id = (job.get('job_id') or (job_key.split(':', 1)[1] if ':' in job_key else '')).strip()
        filename = job.get('filename') or ''
        emit_activity(
            f'Queued "{_display_title(filename)}"',
            job_id=job_id,
            filename=filename,
            stage='queue',
            source='manager',
        )
    except Exception:
        pass

def _acquire_pipeline_sched_lock():
    token = f"{uuid.uuid4().hex}:{time.time()}"
    ok = redis_client.set(PIPELINE_SCHED_LOCK_KEY, token, nx=True, ex=PIPELINE_SCHED_LOCK_TTL_SEC)
    return token if ok else None

def _release_pipeline_sched_lock(token: str):
    try:
        current = redis_client.get(PIPELINE_SCHED_LOCK_KEY)
        if current == token:
            redis_client.delete(PIPELINE_SCHED_LOCK_KEY)
    except Exception:
        pass

def _reserve_next_waiting_job_locked():
    active_job_id = (redis_client.get(PIPELINE_ACTIVE_JOB_KEY) or "").strip()
    if active_job_id:
        active_key = f"job:{active_job_id}"
        if (not redis_client.exists(active_key)) or _is_terminal_pipeline_status(redis_client.hget(active_key, "status") or ""):
            redis_client.delete(PIPELINE_ACTIVE_JOB_KEY)
        else:
            return None

    keys = _job_index_keys()
    if not keys:
        return None

    pipe = redis_client.pipeline()
    for k in keys:
        pipe.hgetall(k)
    raw_jobs = pipe.execute()

    candidates = []
    active_candidates = []
    for job_key, job in zip(keys, raw_jobs):
        if not job:
            continue

        try:
            status = Status.parse(job.get('status'))
        except Exception:
            if (job.get('status') or '').strip().upper() == "COMPLETED":
                status = Status.DONE
            else:
                continue

        filename = (job.get('filename') or '').strip()
        job_id = (job.get('job_id') or (job_key.split(':', 1)[1] if ':' in job_key else '')).strip()
        if not filename or not job_id:
            continue

        if status in {Status.STARTING, Status.RUNNING, Status.STAMPING}:
            started_at = _float_or(job.get('started_at'), _float_or(job.get('created_at'), 0.0))
            created_at = _float_or(job.get('created_at'), 0.0)
            active_candidates.append((started_at, created_at, job_id))
            continue

        if status != Status.WAITING:
            continue

        action = (job.get('queue_action') or PIPELINE_QUEUE_ACTION_TRANSCODE).strip().upper()
        if action not in {PIPELINE_QUEUE_ACTION_TRANSCODE, PIPELINE_QUEUE_ACTION_STAMP}:
            action = PIPELINE_QUEUE_ACTION_TRANSCODE

        waiting_at = _float_or(job.get('waiting_at'), _float_or(job.get('started_at'), _float_or(job.get('created_at'), 0.0)))
        created_at = _float_or(job.get('created_at'), 0.0)
        candidates.append((waiting_at, created_at, job_id, job_key, filename, action, job))

    if active_candidates:
        active_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        redis_client.set(PIPELINE_ACTIVE_JOB_KEY, active_candidates[0][2])
        return None

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    _, _, job_id, job_key, filename, action, job = candidates[0]

    started_at = _float_or(job.get('started_at'), 0.0)
    mapping = {
        'status': Status.STAMPING.value if action == PIPELINE_QUEUE_ACTION_STAMP else Status.STARTING.value,
    }
    if started_at <= 0:
        mapping['started_at'] = str(time.time())
    redis_client.hset(job_key, mapping=mapping)
    redis_client.set(PIPELINE_ACTIVE_JOB_KEY, job_id)

    return {
        'job_id': job_id,
        'job_key': job_key,
        'filename': filename,
        'action': action,
    }

def _launch_reserved_job(reserved):
    if not reserved:
        return

    job_id = reserved['job_id']
    job_key = reserved['job_key']
    filename = reserved['filename']
    action = reserved['action']

    try:
        if action == PIPELINE_QUEUE_ACTION_STAMP:
            _ensure_one_worker_awake()
            stamp(job_id)
        else:
            emit_activity(
                f'Started "{_display_title(filename)}"',
                job_id=job_id,
                filename=filename,
                stage='start',
                source='manager',
            )
            _launch_after_warmup(job_key, job_id, filename)
    except Exception as e:
        logger.exception("[%s] reserved launch failed", job_id)
        try:
            redis_client.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': str(e),
                'ended_at': time.time(),
            })
        except Exception:
            pass

def dispatch_next_waiting_job() -> bool:
    token = _acquire_pipeline_sched_lock()
    if not token:
        return False

    try:
        reserved = _reserve_next_waiting_job_locked()
    finally:
        _release_pipeline_sched_lock(token)

    if not reserved:
        return False

    _launch_reserved_job(reserved)
    return True

def _pipeline_scheduler_loop():
    while True:
        try:
            dispatch_next_waiting_job()
        except Exception:
            logger.exception("pipeline scheduler tick failed")
        time.sleep(PIPELINE_SCHED_POLL_SEC)

def _start_pipeline_scheduler():
    global _PIPELINE_SCHED_STARTED
    with _PIPELINE_SCHED_GUARD:
        if _PIPELINE_SCHED_STARTED:
            return
        thread = threading.Thread(
            target=_pipeline_scheduler_loop,
            name="pipeline-scheduler",
            daemon=True
        )
        thread.start()
        _PIPELINE_SCHED_STARTED = True
        logger.info("Pipeline scheduler started (poll=%.1fs)", PIPELINE_SCHED_POLL_SEC)


# -------------------------- Views --------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.get('/metrics')
def metrics_page():
    return render_template('metrics.html')

# -------------------- tiny in-process caches --------------------
# (shield front-end polling from re-hitting Redis every time)
_metrics_cache = (0.0, None)  # (ts, payload dict)
_jobs_cache    = (0.0, None)  # (ts, list of job dicts)

# --- Nodes page & data ---
def _resolve_ip(hostname: str) -> str:
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return ""

@app.get('/nodes_data')
def nodes_data():
    """
    Returns list of nodes with hostname, ip, mac, last_seen_ts, active.
    Uses only get_all_nodes() and get_active_nodes() for consistency.
    """
    all_nodes = get_all_nodes()
    active_hosts = set(n["hostname"] for n in get_active_nodes())

    def last_seen(host):
        try:
            md = redis_client.hgetall(f"metrics:node:{host}") or {}
            return int(float(md.get('ts', 0) or 0))
        except Exception:
            return 0

    items = []
    for n in all_nodes:
        host = n["hostname"]
        mac  = n["mac"]
        items.append({
            "hostname": host,
            "ip": _resolve_ip(host),
            "mac": mac,
            "last_seen_ts": last_seen(host),
            "active": host in active_hosts,
        })
    items.sort(key=lambda x: _natural_key(x["hostname"]))
    return jsonify({"nodes": items})

@app.get('/nodes')
def nodes_page():
    return render_template('nodes.html')

# --- metrics snapshot (batched + cached) ---
@app.get('/metrics_snapshot')
def metrics_snapshot():
    global _metrics_cache
    now = time.time()
    ts, cached = _metrics_cache
    # serve cached snapshot if it's fresh (<0.5s)
    if cached and (now - ts) < 0.5:
        return jsonify(cached)

    # derive host list from nodes:mac (no keyspace SCAN)
    hosts = [n["hostname"] for n in get_all_nodes()]
    if not hosts:
        payload = {"nodes": []}
        _metrics_cache = (now, payload)
        return jsonify(payload)

    # batch HGETALL for each metrics hash
    pipe = redis_client.pipeline()
    for h in hosts:
        pipe.hgetall(f"metrics:node:{h}")
    raw_list = pipe.execute()

    nodes = []
    for host, data in zip(hosts, raw_list):
        if not data:
            continue
        try:
            nodes.append({
                "key": f"metrics:node:{host}",
                "hostname": data.get("hostname") or host,
                "ts": int(float(data.get("ts", 0) or 0)),
                "cpu": float(data.get("cpu", 0.0) or 0.0),
                "gpu": float(data.get("gpu", -1.0) or -1.0),
                "mem": float(data.get("mem", 0.0) or 0.0),
                "mem_used": int(float(data.get("mem_used", 0) or 0)),
                "mem_total": int(float(data.get("mem_total", 0) or 0)),
                "rx_bps": int(float(data.get("rx_bps", 0) or 0)),
                "tx_bps": int(float(data.get("tx_bps", 0) or 0)),
                "disk": int(float(data.get("disk", 0) or 0)),
            })
        except Exception:
            continue

    nodes.sort(key=lambda n: _natural_key(n["hostname"]))
    payload = {"nodes": nodes}
    _metrics_cache = (now, payload)
    return jsonify(payload)

GLOBAL_SETTINGS_KEY = "global:settings"

@app.get("/settings")
def get_settings():
    settings = _get_settings() or {}
    try:
        idle_sec = max(30, int(settings.get("suspend_idle_sec", 300) or 300))
    except Exception:
        idle_sec = 300
    out = dict(settings)
    out["suspend_enabled"] = str(settings.get("suspend_enabled", "0")).strip().lower() in ("1", "true", "yes", "on")
    out["suspend_idle_sec"] = idle_sec
    out["suspend_gc_enabled"] = str(settings.get("suspend_gc_enabled", "0")).strip().lower() in ("1", "true", "yes", "on")
    out["max_source_file_size_gb"] = _as_float(settings.get("max_source_file_size_gb", 15), 15.0)
    out["av1_check_enabled"] = _as_bool(settings.get("av1_check_enabled", "1"), True)
    out["use_nfs_for_all_files"] = _as_bool(settings.get("use_nfs_for_all_files", "0"), False)
    out["large_file_behavior"] = str(settings.get("large_file_behavior", "reject") or "reject").strip().lower()
    out["default_target_height"] = normalize_target_height(
        settings.get("default_target_height", DEFAULT_TARGET_HEIGHT),
        DEFAULT_TARGET_HEIGHT
    )
    return jsonify(out)

@app.post("/settings")
def post_global_settings():
    """
    Accepts JSON:
    {
        "suspend_enabled": bool,
        "suspend_idle_sec": int (>=30),
        "suspend_gc_enabled": bool,
        "max_source_file_size_gb": number (>0),
        "av1_check_enabled": bool,
        "use_nfs_for_all_files": bool,
        "large_file_behavior": "reject"|"nfs",
        "default_target_height": 720|1080
    }
    """
    payload = request.get_json(silent=True) or {}
    try:
        suspend_enabled = _as_bool(payload.get("suspend_enabled", False), False)
        suspend_gc_enabled = _as_bool(payload.get("suspend_gc_enabled", False), False)
        idle = int(payload.get("suspend_idle_sec", 300))
        if idle < 30:
            idle = 30  # sane floor
        max_size_gb = _as_float(payload.get("max_source_file_size_gb", 15), 15.0)
        if max_size_gb <= 0:
            max_size_gb = 15.0
        av1_check_enabled = _as_bool(payload.get("av1_check_enabled", True), True)
        use_nfs_for_all_files = _as_bool(payload.get("use_nfs_for_all_files", False), False)
        large_file_behavior = str(payload.get("large_file_behavior", "reject") or "reject").strip().lower()
        if large_file_behavior not in ("reject", "nfs"):
            large_file_behavior = "reject"
        default_target_height = normalize_target_height(
            payload.get("default_target_height", DEFAULT_TARGET_HEIGHT),
            DEFAULT_TARGET_HEIGHT
        )
    except Exception:
        return jsonify({"error": "invalid payload"}), 400

    redis_client.hset(GLOBAL_SETTINGS_KEY, mapping={
        "suspend_enabled": "1" if suspend_enabled else "0",
        "suspend_idle_sec": str(idle),
        "suspend_gc_enabled": "1" if suspend_gc_enabled else "0",
        "max_source_file_size_gb": str(max_size_gb),
        "av1_check_enabled": "1" if av1_check_enabled else "0",
        "use_nfs_for_all_files": "1" if use_nfs_for_all_files else "0",
        "large_file_behavior": large_file_behavior,
        "default_target_height": str(default_target_height),
    })
    # Backward-compatible mirror for legacy readers.
    redis_client.hset("settings:global", mapping={
        "suspend_enabled": "1" if suspend_enabled else "0",
        "suspend_idle_sec": str(idle),
        "suspend_gc_enabled": "1" if suspend_gc_enabled else "0",
        "max_source_file_size_gb": str(max_size_gb),
        "av1_check_enabled": "1" if av1_check_enabled else "0",
        "use_nfs_for_all_files": "1" if use_nfs_for_all_files else "0",
        "large_file_behavior": large_file_behavior,
        "default_target_height": str(default_target_height),
    })

    return jsonify({
        "suspend_enabled": suspend_enabled,
        "suspend_idle_sec": idle,
        "suspend_gc_enabled": suspend_gc_enabled,
        "max_source_file_size_gb": max_size_gb,
        "av1_check_enabled": av1_check_enabled,
        "use_nfs_for_all_files": use_nfs_for_all_files,
        "large_file_behavior": large_file_behavior,
        "default_target_height": default_target_height,
    })

# ------------------------ Jobs API -------------------------
@app.get('/jobs')
def list_jobs():
    """
    Fast jobs listing:
      - Maintains `jobs:all` set as index of job keys.
      - Auto-seeds from keyspace once if the set is empty (to include existing jobs).
      - Pipelines HGETALL for all jobs, with a 0.5s in-process cache.
    """
    global _jobs_cache

    now = time.time()
    ts, cached_list = _jobs_cache

    # refresh cache if stale
    if cached_list is None or (now - ts) >= 0.5:
        keys = list(redis_client.smembers("jobs:all") or [])

        # Fallback seeding if index missing (first run / migration)
        if not keys:
            # One-time SCAN (kept off hot path by cache + subsequent index usage)
            keys = [k for k in redis_client.scan_iter("job:*", count=1000)]
            if keys:
                try:
                    redis_client.sadd("jobs:all", *keys)
                except Exception:
                    pass

        # batch HGETALL
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.hgetall(k)
        raws = pipe.execute()

        jobs = []
        for k, job_data in zip(keys, raws):
            if not job_data:
                # clean up dead membership if needed
                try:
                    redis_client.srem("jobs:all", k)
                except Exception:
                    pass
                continue

            job_id = (k.split(':', 1)[1]) if ':' in k else k
            started = float(job_data.get('started_at', '0') or 0)
            ended   = float(job_data.get('ended_at', '0') or 0)
            created = float(job_data.get('created_at', '0') or 0)
            nowf    = time.time()
            elapsed = 0

            raw_status = str(job_data.get('status') or '').strip().upper()
            if raw_status == 'COMPLETED':
                status = Status.DONE
            else:
                try:
                    status = Status.parse(raw_status)
                except Exception:
                    status = None

            if status in [Status.RUNNING, Status.WAITING, Status.STARTING]:
                elapsed = nowf - started
            elif ended:
                elapsed = ended - started
            


            jobs.append({
                'job_id': job_id,
                **job_data,
                'segment_progress': int(job_data.get('segment_progress', '0') or 0),
                'encode_progress':  int(job_data.get('encode_progress',  '0') or 0),
                'combine_progress': int(job_data.get('combine_progress', '0') or 0),
                'elapsed': elapsed,
                'started': started,
                'created': created,
                'stitched_chunks': int(job_data.get('stitched_chunks', '0') or 0),
            })

        _jobs_cache = (now, jobs)
        jobs_list = jobs
    else:
        jobs_list = cached_list

    # ---- sorting/paging (retain your existing behavior) ----
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get('page_size', 10))
        page_size = min(max(1, page_size), 50)
    except Exception:
        page_size = 10

    sort_by = (request.args.get('sort_by') or 'date').lower()
    sort_dir = (request.args.get('sort_dir') or 'desc').lower()
    status_filter = (request.args.get('status') or '').strip().upper()
    name_query = (request.args.get('q') or '').strip().lower()
    reverse = (sort_dir != 'asc')

    status_order = {
        'READY': 0,
        'STARTING': 1,
        'WAITING': 2,
        'RUNNING': 3,
        'STAMPING': 4,
        'STOPPED': 5,
        'FAILED': 6,
        'REJECTED': 7,
        'DONE': 8,
        'COMPLETED': 8,
    }

    filtered_jobs = jobs_list
    if status_filter and status_filter != 'ALL':
        if status_filter == 'DONE':
            filtered_jobs = [
                j for j in filtered_jobs
                if (j.get('status') or '').upper() in {'DONE', 'COMPLETED'}
            ]
        else:
            filtered_jobs = [
                j for j in filtered_jobs
                if (j.get('status') or '').upper() == status_filter
            ]

    if name_query:
        def _matches_name(job):
            filename = (job.get('filename') or '')
            base_name = os.path.basename(filename).lower()
            display_name = os.path.splitext(base_name)[0]
            return (
                name_query in display_name
                or name_query in base_name
                or name_query in filename.lower()
            )
        filtered_jobs = [j for j in filtered_jobs if _matches_name(j)]

    def sort_key(job):
        s = (job.get('status') or '').upper()
        filename = os.path.basename(job.get('filename') or '').lower()
        started = float(job.get('started') or 0)
        created = float(job.get('created') or 0)
        encode = int(job.get('encode_progress') or 0)

        if sort_by == 'filename':
            return (filename,)
        elif sort_by == 'status':
            return (status_order.get(s, 99), filename)
        elif sort_by == 'encode':
            return (encode, started, filename)
        else:  # 'date'
            return max(started, created)

    jobs_sorted = sorted(filtered_jobs, key=sort_key, reverse=reverse)

    total = len(jobs_sorted)
    total_pages = max(1, ceil(total / page_size))
    page = min(max(1, page), total_pages)
    start = (page - 1) * page_size
    items = jobs_sorted[start:start + page_size]

    return jsonify({
        'page': page,
        'page_size': page_size,
        'total': total,
        'total_pages': total_pages,
        'items': items
    })

@app.get('/activity')
def list_activity():
    try:
        limit = int(request.args.get('limit', 120))
    except Exception:
        limit = 120
    return jsonify(fetch_activity(limit))

@app.get('/job_activity/<job_id>')
def list_job_activity(job_id):
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        return jsonify({'error': 'Job not found'}), 404
    try:
        limit = request.args.get('limit')
        if limit is None:
            return jsonify(fetch_job_activity(job_id))
        return jsonify(fetch_job_activity(job_id, limit=int(limit)))
    except Exception:
        return jsonify(fetch_job_activity(job_id))


def get_video_details(job_id: str, file_path: str):
    job_key = f"job:{job_id}"

    real_input = file_path
    if not os.path.exists(real_input):
        job = redis_client.hgetall(job_key) or {}
        filename = job.get('filename') or ''
        alt = os.path.join(WATCH_ROOT, filename.lstrip('/'))
        if os.path.exists(alt):
            real_input = alt

    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration,size,bit_rate:stream=index,codec_type,codec_name,width,height,avg_frame_rate,nb_frames,channels,channel_layout,disposition:stream_tags=language,title',
        '-of', 'json', real_input
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise subprocess.CalledProcessError(res.returncode, cmd, res.stdout, res.stderr)

    info = json.loads(res.stdout or '{}')
    fmt = info.get('format', {}) or {}
    streams = info.get('streams', []) or []

    # format metrics
    try: duration_s = float(fmt.get('duration', 0) or 0)
    except Exception: duration_s = 0.0
    size_b = int(fmt.get('size', 0) or 0)
    try: fmt_bps = int(fmt.get('bit_rate', 0) or 0)
    except Exception: fmt_bps = 0
    kbps = (fmt_bps/1000.0) if fmt_bps>0 else (((size_b*8)/duration_s/1000.0) if (size_b and duration_s) else 0.0)

    video_streams, audio_streams = [], []
    for s in streams:
        base = {
            'index': int(s.get('index', 0)),
            'codec': s.get('codec_name') or '',
            'disposition_default': bool((s.get('disposition') or {}).get('default', 0)),
            'title': ((s.get('tags') or {}).get('title') or ''),
            'language': ((s.get('tags') or {}).get('language') or '')
        }
        if s.get('codec_type') == 'video':
            afr = s.get('avg_frame_rate') or '0'
            try:
                fps = (float(afr.split('/')[0]) / float(afr.split('/')[1])) if '/' in afr else float(afr)
            except Exception:
                fps = 0.0
            video_streams.append({**base,
                'width': int(s.get('width') or 0),
                'height': int(s.get('height') or 0),
                'fps': fps,
                'nb_frames': int(s.get('nb_frames', 0) or 0)
            })
        elif s.get('codec_type') == 'audio':
            audio_streams.append({**base,
                'channels': int(s.get('channels') or 0),
                'channel_layout': s.get('channel_layout') or ''
            })

    # Default to the first video stream
    v_sel = 0

    def _is_english(lang: str) -> bool:
        if not lang:
            return False
        L = lang.strip().lower()
        return (
            L == 'eng' or
            L == 'en' or
            L == 'english' or
            L.startswith('en-')  # e.g., en-us, en-gb
        )

    a_sel = 0
    # Prefer first English audio stream; fallback to 0
    for i, a in enumerate(audio_streams):
        if _is_english(a.get('language', '')):
            a_sel = i
            break

    pv = video_streams[0] if video_streams else {}
    fps = float(pv.get('fps', 0) or 0)
    total_frames = int(pv.get('nb_frames') or 0)
    if total_frames == 0 and duration_s and fps:
        total_frames = int(duration_s * fps)

    mapping = {
        'source_file_size': size_b,
        'source_duration': f"{duration_s:.2f}" if duration_s else '0',
        'source_codec': pv.get('codec',''),
        'source_resolution': f"{pv.get('width',0)}x{pv.get('height',0)}" if pv else '',
        'source_fps': f"{fps:.2f}" if fps else '0',
        'streams_json': json.dumps({'video': video_streams, 'audio': audio_streams}),
        'source_bitrate_kbps': f"{kbps:.0f}" if kbps>0 else '0',
        'selected_v_stream': v_sel,
        'selected_a_stream': a_sel,
    }
    if total_frames:
        mapping['total_frames'] = total_frames

    return mapping

@app.post('/add_job')
def add_job():
    data = request.get_json(silent=True) or {}
    filename = data.get('filename')
    force_paused = bool(data.get('force_paused', False))  # copies set this true

    if not filename or not filename.endswith(('.mkv', '.mp4')):
        return jsonify({'status': 'error', 'message': 'Invalid file format'}), 400

    full_path = os.path.join("/watch", filename.lstrip('/'))
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"

    now = str(time.time())
    global_settings = _load_global_settings()
    default_target_height = get_default_target_height()
    target_height = normalize_target_height(
        data.get('target_height', default_target_height),
        default_target_height
    )

    auto_start_global = _as_bool(global_settings.get('auto_start', '1'), True)
    auto_start_effective = (auto_start_global and not force_paused)
    serialize_global  = _as_bool(global_settings.get('serialize_pipeline', '0'), False)

    segment_duration = _as_int(global_settings.get('segment_duration', 10), 10)
    number_parts = _as_int(global_settings.get('number_parts', 2), 2)

    status = Status.WAITING if auto_start_effective else Status.READY
    scratch_mode = 'local'
    scratch_root = LOCAL_PROJECT_ROOT
    job_settings = {
        'job_id': job_id,
        'filename': filename,
        'status': status.value,
        'created_at': now,
        'started_at': now if auto_start_effective else '0',
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if serialize_global else '0',
        'software_encode': '0',  # per-job default: disabled
        'target_height': target_height,
        'queue_action': PIPELINE_QUEUE_ACTION_TRANSCODE if auto_start_effective else '',
        'waiting_at': now if auto_start_effective else '0',
        # placeholders (worker fills in)
        'source_codec': '',
        'source_resolution': '',
        'source_duration': '0',
        'source_fps': '0',
        'source_file_size': 0,
        'total_frames': 0,
        'scratch_mode': scratch_mode,
        'scratch_root': scratch_root,
    }
    emit_activity(
        f'Received "{_display_title(filename)}"',
        job_id=job_id,
        filename=filename,
        stage='received',
        source='manager',
    )
    # Kick off async probe on a worker
    video_details = get_video_details(job_id, full_path)
    job_settings = {**job_settings, **video_details}

    rejection_reason, rejection_message, scratch_mode, scratch_root = _evaluate_job_policy(video_details, global_settings)
    job_settings.update({
        'scratch_mode': scratch_mode,
        'scratch_root': scratch_root,
    })
    if rejection_reason:
        job_settings.update({
            'status': Status.REJECTED.value,
            'started_at': '0',
            'queue_action': '',
            'waiting_at': '0',
            'error': rejection_message,
            'rejected_reason': rejection_reason,
            'rejected_at': now,
        })
        emit_activity(
            f'Rejected "{_display_title(filename)}": {rejection_message}',
            job_id=job_id,
            filename=filename,
            stage='rejected',
            source='manager',
        )

    redis_client.hset(job_key, mapping=job_settings)

    # index the job for fast listing
    redis_client.sadd("jobs:all", job_key)

    if auto_start_effective and not rejection_reason:
        _queue_job_for_dispatch(job_key, PIPELINE_QUEUE_ACTION_TRANSCODE, _float_or(now, time.time()))
        dispatch_next_waiting_job()

    if rejection_reason:
        return jsonify({
            'status': 'rejected',
            'job_id': job_id,
            'message': rejection_message,
            'reason': rejection_reason,
        }), 201

    return jsonify({'status': 'success', 'job_id': job_id}), 201

@app.route('/copy_job', methods=['POST'])
def copy_job():
    data = request.get_json(silent=True) or {}
    source_job_id = data.get('job_id')
    if not source_job_id:
        return jsonify({'status': 'error', 'message': 'job_id is required'}), 400

    source_key = f"job:{source_job_id}"
    if not redis_client.exists(source_key):
        return jsonify({'status': 'error', 'message': 'Source job not found'}), 404

    src = redis_client.hgetall(source_key)

    globals_map = redis_client.hgetall('settings:global')
    def _int(v, default):
        try:
            return int(v)
        except Exception:
            return default

    filename = src.get('filename')
    if not filename:
        return jsonify({'status': 'error', 'message': 'Source job missing filename'}), 400

    segment_duration = _int(
        src.get('segment_duration', globals_map.get('segment_duration')),
        10
    )
    number_parts = _int(
        src.get('number_parts', globals_map.get('number_parts')),
        2
    )
    serialize_src = src.get('serialize_pipeline', globals_map.get('serialize_pipeline', '0'))
    frame_src = src.get('software_encode', '0')
    default_target_height = get_default_target_height()
    target_height = normalize_target_height(src.get('target_height', default_target_height), default_target_height)

    new_job_id = str(uuid.uuid4())
    now = time.time()
    selected_v_stream = src.get('selected_v_stream',0)
    selected_a_stream = src.get('selected_a_stream',0)

    new_job = {
        'job_id': new_job_id,
        'filename': filename,
        'status': Status.READY.value,
        'created_at': str(now),
        'started_at': '0',
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if str(serialize_src) in ('1','true','True') else '0',
        'software_encode': '1' if str(frame_src) in ('1','true','True') else '0',
        'target_height': target_height,
        'selected_v_stream': selected_v_stream,
        'selected_a_stream': selected_a_stream,
        'scratch_mode': src.get('scratch_mode', 'local'),
        'scratch_root': src.get('scratch_root', LOCAL_PROJECT_ROOT),
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        'streams_json': src.get('streams_json')
    }
    new_key = f"job:{new_job_id}"
    redis_client.hset(new_key, mapping=new_job)
    # index the job for fast listing
    try:
        redis_client.sadd("jobs:all", new_key)
    except Exception:
        pass

    logger.info(f"[{new_job_id}] Copied from {source_job_id} with filename={filename}, "
                f"segment_duration={segment_duration}, number_parts={number_parts} (PAUSED)")

    return jsonify({'status': 'success', 'job_id': new_job_id}), 201

@app.post('/start_job/<job_id>')
def start_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if Status.parse(job.get('status')) != Status.READY:
        return jsonify({'status': 'invalid', 'message': 'Job is not in READY state'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

    now = time.time()
    _queue_job_for_dispatch(job_key, PIPELINE_QUEUE_ACTION_TRANSCODE, now)
    dispatch_next_waiting_job()
    return jsonify({'status': 'queued'}), 200

@app.post('/restart_job/<job_id>')
def restart_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if Status.parse(job.get('status')) not in [Status.STOPPED, Status.FAILED, Status.REJECTED, Status.DONE]:
        return jsonify({'status': 'invalid', 'message': 'Job is not in STOPPED/FAILED/REJECTED/DONE state.'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

    # Remove working directory (best-effort)
    job_dir = job.get('job_dir', '')
    logger.info(f"[{job_id}] Cleaning job dir {job_dir!r}")
    if job_dir and os.path.exists(job_dir):
        try:
            shutil.rmtree(job_dir)
        except Exception as e:
            logger.warning(f"[{job_id}] Failed to delete job directory {job_dir}: {e}")

    # Remove ONLY per-job subkeys (chunks, progress shards, etc), keep the base hash
    for key in redis_client.scan_iter(f"{job_key}:*"):
        try:
            redis_client.delete(key)
        except Exception:
            pass

    # Carry over settings (with sane fallbacks)
    globals_map = _load_global_settings()
    def _int(v, default):
        try:
            return int(v)
        except Exception:
            return default

    segment_duration = _int(job.get('segment_duration', globals_map.get('segment_duration', 10)), 10)
    number_parts     = _int(job.get('number_parts',     globals_map.get('number_parts', 2)), 2)
    serialize_val    = job.get('serialize_pipeline', globals_map.get('serialize_pipeline', '0'))
    frame_val        = job.get('software_encode', '0')
    default_target_height = get_default_target_height()
    target_height    = normalize_target_height(job.get('target_height', default_target_height), default_target_height)
    selected_v_stream= job.get('selected_v_stream', 0)
    selected_a_stream= job.get('selected_a_stream', 0)
    source_updates = {}
    global_settings = _load_global_settings()
    full_path = os.path.join("/watch", filename.lstrip('/'))
    try:
        video_details = get_video_details(job_id, full_path)
        source_updates = {
            'source_file_size': video_details.get('source_file_size', 0),
            'source_duration': video_details.get('source_duration', '0'),
            'source_codec': video_details.get('source_codec', ''),
            'source_resolution': video_details.get('source_resolution', ''),
            'source_fps': video_details.get('source_fps', '0'),
            'source_bitrate_kbps': video_details.get('source_bitrate_kbps', '0'),
            'total_frames': video_details.get('total_frames', 0),
        }
    except Exception as e:
        logger.warning(f"[{job_id}] Probe failed during restart validation: {e}")
        source_updates = {
            'source_file_size': job.get('source_file_size', 0),
            'source_duration': job.get('source_duration', '0'),
            'source_codec': job.get('source_codec', ''),
            'source_resolution': job.get('source_resolution', ''),
            'source_fps': job.get('source_fps', '0'),
            'source_bitrate_kbps': job.get('source_bitrate_kbps', '0'),
            'total_frames': job.get('total_frames', 0),
        }

    rejection_reason, rejection_message, scratch_mode, scratch_root = _evaluate_job_policy(source_updates, global_settings)

    now = time.time()
    new_fields = {
        'job_id': job_id,
        'filename': filename,
        'status': Status.WAITING.value,
        'started_at': str(now),
        'ended_at': 0,
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if str(serialize_val) in ('1', 'true', 'True') else '0',
        'software_encode': '1' if str(frame_val) in ('1', 'true', 'True') else '0',
        'target_height': target_height,
        'selected_v_stream': selected_v_stream,
        'selected_a_stream': selected_a_stream,
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        'segment_started': 0,
        'segment_elapsed': 0,
        'segment_progress': 0,
        'encode_started': 0,
        'encode_elapsed': 0,
        'encode_progress': 0,
        'combine_started': 0,
        'combine_elapsed': 0,
        'combine_progress': 0,
        'elapsed': 0,
        'streams_json': job.get('streams_json'),
        'scratch_mode': scratch_mode,
        'scratch_root': scratch_root,
        'queue_action': PIPELINE_QUEUE_ACTION_TRANSCODE,
        'waiting_at': str(now),

    }
    new_fields.update(source_updates)
    if rejection_reason:
        new_fields.update({
            'status': Status.REJECTED.value,
            'started_at': '0',
            'queue_action': '',
            'waiting_at': '0',
            'error': rejection_message,
            'rejected_reason': rejection_reason,
            'rejected_at': str(now),
        })
        emit_activity(
            f'Rejected "{_display_title(filename)}": {rejection_message}',
            job_id=job_id,
            filename=filename,
            stage='rejected',
            source='manager',
        )

    # Overwrite base hash in one go (no delete -> no set race)
    redis_client.hset(job_key, mapping=new_fields)

    # Ensure membership in the UI index (fixes the disappearing job)
    try:
        redis_client.sadd("jobs:all", job_key)
    except Exception:
        pass

    if rejection_reason:
        return jsonify({'status': 'rejected', 'reason': rejection_reason, 'message': rejection_message}), 200

    dispatch_next_waiting_job()
    return jsonify({'status': 'queued'}), 200


@app.get('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.post('/stop_job/<job_id>')
def stop_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    redis_client.hset(job_key, mapping={'status': Status.STOPPED.value, 'ended_at': time.time()})
    huey.revoke_by_id(job_id)
    if (redis_client.get(PIPELINE_ACTIVE_JOB_KEY) or '').strip() == job_id:
        redis_client.delete(PIPELINE_ACTIVE_JOB_KEY)
    dispatch_next_waiting_job()
    return jsonify({'status': 'stopped'}), 200

@app.delete('/delete_job/<job_id>')
def delete_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job_data = redis_client.hgetall(job_key) or {}
    job_dir = job_data.get('job_dir', '')

    logger.info(f"[{job_id}] Deleting job key and dir {job_dir!r}")

    for key in redis_client.scan_iter(f"{job_key}*"):
        redis_client.delete(key)
    try:
        redis_client.delete(f"joblog:{job_id}")
    except Exception:
        pass

    # remove from the jobs index
    try:
        redis_client.srem("jobs:all", job_key)
    except Exception:
        pass

    if (redis_client.get(PIPELINE_ACTIVE_JOB_KEY) or '').strip() == job_id:
        redis_client.delete(PIPELINE_ACTIVE_JOB_KEY)
    dispatch_next_waiting_job()

    if job_dir and os.path.exists(job_dir):
        try:
            shutil.rmtree(job_dir)
        except Exception as e:
            logger.warning(f"[{job_id}] Failed to delete job directory {job_dir}: {e}")

    return jsonify({'status': 'deleted'}), 200

@app.get('/preview/<job_id>')
def preview_video(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        logger.warning(f"[{job_id}] Key not found '{job_key}'")
        return jsonify({'error': 'Job not found'}), 404

    job = redis_client.hgetall(job_key) or {}
    output_path = job.get('output_path')
    if not output_path or not os.path.isfile(output_path):
        logger.warning(f"[{job_id}] 'Output not found '{output_path}'")
        return jsonify({'error': 'Output not found'}), 404

    return send_file(output_path, mimetype='video/mp4', conditional=True)

# ------------------ Job info / utilities -------------------
@app.get('/job_properties/<job_id>')
def job_properties(job_id):
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        return jsonify({'error': 'Job not found'}), 404
    job_data = {k: v for k, v in redis_client.hgetall(key).items()}
    job_data["activity_log"] = fetch_job_activity(job_id)
    return jsonify(job_data)

# ---------------- Per-job settings (PAUSED only) ------------
@app.route('/job_settings/<job_id>', methods=['GET', 'POST'])
def job_settings(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'error': 'Job not found'}), 404

    if request.method == 'GET':
        job = redis_client.hgetall(job_key)
        default_target_height = get_default_target_height()
        streams_json = job.get('streams_json') or '{"video":[],"audio":[]}'
        try:
            streams = json.loads(streams_json)
        except Exception:
            streams = {'video': [], 'audio': []}
        return jsonify({
            'job_id': job_id,
            'filename': job.get('filename'),
            'status': job.get('status'),
            'segment_duration': int(job.get('segment_duration', '10')),
            'number_parts': int(job.get('number_parts', '2')),
            'serialize_pipeline': job.get('serialize_pipeline', '0'),
            'software_encode': job.get('software_encode', '0'),
            'target_height': normalize_target_height(job.get('target_height', default_target_height), default_target_height),
            'streams': streams_json,
            'selected_v_stream': job.get('selected_v_stream', '0'),
            'selected_a_stream': job.get('selected_a_stream', '0'),
        })

    # POST
    job = redis_client.hgetall(job_key)
    if job.get('status') in ['RUNNING']:
        return jsonify({'error': 'Job is RUNNING; stop it or copy/restart to change settings.'}), 400

    data = request.get_json(silent=True) or {}
    try:
        default_target_height = get_default_target_height()
        seg   = int(data.get('segment_duration', job.get('segment_duration', 10)))
        parts = int(data.get('number_parts', job.get('number_parts', 2)))
        serialize_pipeline = bool(data.get('serialize_pipeline',
                                   job.get('serialize_pipeline', '0') in ('1','true','True')))
        software_encode = bool(data.get('software_encode',
                              job.get('software_encode', '0') in ('1','true','True')))
        target_height = normalize_target_height(
            data.get('target_height', job.get('target_height', default_target_height)),
            default_target_height
        )
        v_sel = data.get('selected_v_stream', job.get('selected_v_stream', 0))
        a_sel = data.get('selected_a_stream', job.get('selected_a_stream', 0))

        v_sel = int(v_sel)
        a_sel = int(a_sel)

        mapping = {
            'segment_duration': seg,
            'number_parts': parts,
            'selected_v_stream': v_sel,
            'selected_a_stream': a_sel,
            'serialize_pipeline': '1' if serialize_pipeline else '0',
            'software_encode': '1' if software_encode else '0',
            'target_height': target_height,
        }
        redis_client.hset(job_key, mapping=mapping)

        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.exception(f"[{job_id}] Failed to update job settings: {e}")
        return jsonify({'error': 'Failed to update settings'}), 500

# ---------------- Legacy shim (optional) --------------------
@app.get('/tasks')
def legacy_list_tasks():
    return list_jobs()

@app.post('/add_task')
def legacy_add_task():
    return add_job()

@app.post('/start_task/<job_id>')
def legacy_start_task(job_id):
    return start_job(job_id)

@app.post('/stop_task/<job_id>')
def legacy_stop_task(job_id):
    return stop_job(job_id)

@app.delete('/delete_task/<job_id>')
def legacy_delete_task(job_id):
    return delete_job(job_id)

# -------------------- Node utilities (delete) --------------------
@app.delete('/nodes/delete/<hostname>')
def nodes_delete(hostname):
    """
    Remove a node's MAC mapping and associated metrics.
    """
    host = (hostname or '').strip()
    if not host:
        return jsonify({'error': 'Hostname required'}), 400
    try:
        removed = 0
        if redis_client.hexists("nodes:mac", host):
            redis_client.hdel("nodes:mac", host)
            removed += 1
        # Best-effort: remove metrics key too
        redis_client.delete(f"metrics:node:{host}")
        return jsonify({'status': 'ok', 'removed': removed})
    except Exception as e:
        logger.exception("nodes_delete failed")
        return jsonify({'error': str(e)}), 500

# -------------------- Node Wake-on-LAN (direct UDP from host) --------------------

# Env tunables for WOL on the host:
# - WOL_BROADCASTS: comma-separated list of broadcast IPs (default "255.255.255.255")
# - WOL_PORT: UDP port (default 9)
# - WOL_REPEATS: number of times to send packet to each broadcast (default 3)
WOL_BROADCASTS = [x.strip() for x in os.getenv("WOL_BROADCASTS", "255.255.255.255").split(",") if x.strip()]
WOL_PORT = int(os.getenv("WOL_PORT", "9"))
WOL_REPEATS = max(1, int(os.getenv("WOL_REPEATS", "3")))

def _mac_bytes(mac: str) -> bytes:
    """
    Normalize MAC string and return 6 raw bytes.
    Accepts 'aa:bb:cc:dd:ee:ff', 'aa-bb-cc-dd-ee-ff', 'aabbccddeeff'.
    """
    s = re.sub(r'[^0-9a-fA-F]', '', mac or '')
    if len(s) != 12:
        raise ValueError(f"Invalid MAC: {mac!r}")
    return bytes.fromhex(s)

def _build_magic_packet(mac: str) -> bytes:
    mb = _mac_bytes(mac)
    return b'\xff' * 6 + mb * 16

def _send_magic_udp(mac: str, broadcasts=None, port: int = WOL_PORT, repeats: int = WOL_REPEATS):
    """
    Send a WOL magic packet to one or more broadcast addresses.
    Returns (sent_count, errors_list).
    """
    if not broadcasts:
        broadcasts = WOL_BROADCASTS
    pkt = _build_magic_packet(mac)
    sent = 0
    errors = []
    for bcast in broadcasts:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.settimeout(0.5)
                for _ in range(max(1, int(repeats))):
                    s.sendto(pkt, (bcast, int(port)))
                    sent += 1
        except Exception as e:
            errors.append(f"{bcast}:{port} -> {e}")
    return sent, errors

@app.post('/nodes/wake/<hostname>')
def nodes_wake_one(hostname):
    """
    Wake a single node by hostname using its MAC stored in Redis at nodes:mac.
    """
    nodes = {n["hostname"]: n["mac"] for n in get_all_nodes()}
    host = (hostname or '').strip()
    mac = nodes.get(host)
    if not mac:
        return jsonify({'error': f'Unknown node {host} or missing MAC in nodes:mac'}), 404
    try:
        sent, errors = _send_magic_udp(mac)
        if errors:
            logger.warning("WOL partial errors for %s (%s): %s", host, mac, "; ".join(errors))
        logger.info("WOL sent to %s (%s), packets=%d", host, mac, sent)
        return jsonify({'status': 'ok', 'host': host, 'mac': mac, 'packets': sent, 'errors': errors})
    except Exception as e:
        logger.exception("WOL failed for %s", host)
        return jsonify({'error': str(e)}), 500

@app.post('/nodes/wake_all')
def nodes_wake_all():
    """
    Wake all known nodes from nodes:mac.
    """
    try:
        nodes = get_all_nodes()
        total_hosts = len(nodes)
        total_packets = 0
        errors = {}
        for n in nodes:
            sent, errs = _send_magic_udp(n["mac"])
            total_packets += sent
            if errs:
                errors[n["hostname"]] = errs
        logger.info("WOL sent to %d host(s), total packets=%d", total_hosts, total_packets)
        # 'sent' matches what the front-end toast expects (number of hosts)
        return jsonify({'status': 'ok', 'sent': total_hosts, 'total_packets': total_packets, 'errors': errors})
    except Exception as e:
        logger.exception("Wake All failed")
        return jsonify({'error': str(e)}), 500

@app.post('/stamp_job/<job_id>')
def stamp_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status':'not found'}), 404

    job = redis_client.hgetall(job_key) or {}
    s = Status.parse(job.get('status'))
    if s in [Status.STARTING, Status.WAITING, Status.RUNNING, Status.STAMPING]:
        return jsonify({'status':'invalid', 'message':'Job is busy; stop it first.'}), 400

    # verify source exists
    filename = job.get('filename') or ''
    full_path = os.path.join("/watch", filename.lstrip('/'))
    if (not filename or not os.path.exists(full_path)) and job.get('input_path'):
        full_path = job.get('input_path')
    if not os.path.exists(full_path):
        return jsonify({'status':'error','message':f'Input not found: {full_path}'}), 400

    now = time.time()
    started_at = _float_or(job.get('started_at'), 0.0)
    if started_at <= 0:
        started_at = now

    _queue_job_for_dispatch(job_key, PIPELINE_QUEUE_ACTION_STAMP, started_at)
    redis_client.hset(job_key, mapping={
        'encode_progress': 0,
        'encode_elapsed': 0,
    })
    dispatch_next_waiting_job()
    return jsonify({'status':'queued'}), 202


_start_pipeline_scheduler()
