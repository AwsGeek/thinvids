from flask import Flask, render_template, render_template_string, request, jsonify, abort, send_file
from huey import RedisHuey
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

import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

# import backend task planner
from tasks import transcode_video, probe_source

app = Flask(__name__)
app.secret_key = os.urandom(24).hex()

# NOTE: container/service hostnames; adjust if needed
huey = RedisHuey(
    'tasks',
    host='redis',
    port=6379,
    db=0,
    # Huey/redis-py connection hardening:
    blocking=True,
    read_timeout=15,                 # block-pop read
    socket_timeout=5,                # per-op timeout
    socket_connect_timeout=5,        # connect timeout
    socket_keepalive=True,           # keep sockets alive through NAT/overlay resets
    health_check_interval=30,        # auto PING on idle sockets
    retry_on_timeout=True,           # retry timeouts
    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),  # 1s,2s,4s... capped
)

redis_client = redis.Redis(
    host='redis',
    port=6379,
    db=1,
    decode_responses=True,
    socket_keepalive=True,
    socket_timeout=5,
    socket_connect_timeout=5,
    health_check_interval=30,
    retry_on_timeout=True,
    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATUS_READY = 'READY'
STATUS_STARTING = 'STARTING'
STATUS_RUNNING = 'RUNNING'
STATUS_STOPPED = 'STOPPED'
STATUS_FAILED = 'FAILED'
STATUS_DONE    = 'DONE'

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

# ---------- Warm-up & launch helpers (wait for heartbeats/metrics) ----------

# Tunables (can be overridden via env)
CLUSTER_WARMUP_SEC  = int(os.getenv("CLUSTER_WARMUP_SEC", "60"))  # max wait for nodes to appear
MIN_WARMUP_WORKERS  = int(os.getenv("MIN_WARMUP_WORKERS", "20"))   # desired active workers before start
PARTS_PER_WORKER    = int(os.getenv("PARTS_PER_WORKER", "1"))     # hint only
MIN_PARTS           = int(os.getenv("MIN_PARTS", "20"))            # hint only
MAX_PARTS           = int(os.getenv("MAX_PARTS", "20"))          # hint only

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
    Wake the cluster, wait for heartbeats, store audit + parts_hint, then start transcode.
    """
    try:
        # Fire WOL for all known nodes (reuse existing route function)
        try:
            nodes_wake_all()  # ignore JSON response
        except Exception as e:
            logger.warning("nodes_wake_all() raised: %s", e)

        wanted = max(1, MIN_WARMUP_WORKERS)
        seen = _wait_for_workers(wanted, CLUSTER_WARMUP_SEC)

        # Compute parts hint
        parts_hint = max(MIN_PARTS, min(MAX_PARTS, max(0, len(seen)) * PARTS_PER_WORKER))

        # Stash info for UI/debug + hint for tasks.transcode_video
        redis_client.hset(job_key, mapping={
            'warmup_workers_json': json.dumps(seen),
            'warmup_worker_count': len(seen),
            'warmup_wait_s': CLUSTER_WARMUP_SEC,
            'parts_hint': parts_hint,
        })

        # Kick the pipeline
        transcode_video(job_id, f'/watch/{filename}')
    except Exception as e:
        logger.exception("[%s] launch_after_warmup failed", job_id)
        # Mark FAILED so UI surfaces the problem
        try:
            redis_client.hset(job_key, mapping={'status': STATUS_FAILED, 'error': str(e)})
        except Exception:
            pass

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
            })
        except Exception:
            continue

    nodes.sort(key=lambda n: _natural_key(n["hostname"]))
    payload = {"nodes": nodes}
    _metrics_cache = (now, payload)
    return jsonify(payload)

# ------------------- Global settings API -------------------
@app.get('/global_settings')
def get_global_settings():
    g = redis_client.hgetall('settings:global') or {}
    segment_duration = int(g.get('segment_duration', 10))
    number_parts = int(g.get('number_parts', 2))
    auto_start = g.get('auto_start', '1') == '1'
    serialize_pipeline = g.get('serialize_pipeline', '0') == '1'
    return jsonify({
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'auto_start': auto_start,
        'serialize_pipeline': serialize_pipeline,
    })

@app.post('/global_settings')
def post_global_settings():
    data = request.get_json(silent=True) or {}
    try:
        segment_duration = int(data.get('segment_duration', 10))
        number_parts = int(data.get('number_parts', 2))
        auto_start = bool(data.get('auto_start', True))
        serialize_pipeline = bool(data.get('serialize_pipeline', False))

        if not (1 <= segment_duration <= 300):
            return "Segment length must be between 1 and 300 seconds.", 400
        if not (1 <= number_parts <= 8):
            return "Number of parts must be between 1 and 8.", 400

        redis_client.hset('settings:global', mapping={
            'segment_duration': segment_duration,
            'number_parts': number_parts,
            'auto_start': '1' if auto_start else '0',
            'serialize_pipeline': '1' if serialize_pipeline else '0',
        })
        return jsonify({'status': 'ok'})
    except Exception:
        logger.exception("Failed to save global settings")
        return "Failed to save global settings.", 500

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
            elapsed = int((ended if ended > 0 else nowf) - started) if started > 0 else 0

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
    reverse = (sort_dir != 'asc')

    status_order = {'READY': 0, 'RUNNING': 1, 'STOPPED': 2, 'FAILED': 3, 'DONE': 4}

    def sort_key(job):
        s = (job.get('status') or '').upper()
        filename = (job.get('filename') or '').lower()
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
            paused_bucket = 0 if started == 0 else 1
            secondary = created if started == 0 else started
            return (paused_bucket, secondary)

    if sort_by == 'date':
        jobs_sorted = sorted(
            jobs_list,
            key=lambda j: (0 if float(j.get('started') or 0) == 0 else 1,
                           float(j.get('created') or 0) if float(j.get('started') or 0) == 0
                           else float(j.get('started') or 0)),
            reverse=False
        )
        paused = [j for j in jobs_sorted if float(j.get('started') or 0) == 0]
        running = [j for j in jobs_sorted if float(j.get('started') or 0) != 0]
        paused.sort(key=lambda j: float(j.get('created') or 0), reverse=reverse)
        running.sort(key=lambda j: float(j.get('started') or 0), reverse=reverse)
        jobs_sorted = paused + running
    else:
        jobs_sorted = sorted(jobs_list, key=sort_key, reverse=reverse)

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
    global_settings = redis_client.hgetall('settings:global') or {}

    auto_start_global = global_settings.get('auto_start', '1') == '1'
    auto_start_effective = (auto_start_global and not force_paused)
    serialize_global  = global_settings.get('serialize_pipeline', '0') == '1'

    segment_duration = int(global_settings.get('segment_duration', 10))
    number_parts = int(global_settings.get('number_parts', 2))

    status = STATUS_STARTING if auto_start_effective else STATUS_READY
    job_settings = {
        'job_id': job_id,
        'filename': filename,
        'status': status,
        'created_at': now,
        'started_at': now if auto_start_effective else '0',
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if serialize_global else '0',
        # placeholders (worker fills in)
        'source_codec': '',
        'source_resolution': '',
        'source_duration': '0',
        'source_fps': '0',
        'source_file_size': 0,
        'total_frames': 0
    }
    redis_client.hset(job_key, mapping=job_settings)
    # index the job for fast listing
    try:
        redis_client.sadd("jobs:all", job_key)
    except Exception:
        pass

    # Kick off async probe on a worker
    probe_source(job_id, full_path)

    if auto_start_effective:
        # Confirm STARTING + warm-up launch
        redis_client.hset(job_key, mapping={'status': STATUS_STARTING, 'started_at': now})
        _launch_after_warmup(job_key, job_id, filename)

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

    new_job_id = str(uuid.uuid4())
    now = time.time()
    selected_v_stream = src.get('selected_v_stream',0)
    selected_a_stream = src.get('selected_a_stream',0)

    new_job = {
        'job_id': new_job_id,
        'filename': filename,
        'status': STATUS_READY,
        'created_at': str(now),
        'started_at': '0',
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if str(serialize_src) in ('1','true','True') else '0',
        'selected_v_stream': selected_v_stream,
        'selected_a_stream': selected_a_stream,
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0
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

    try:
        full_path = os.path.join("/watch", filename.lstrip('/'))
        probe_source(new_job_id, full_path)   # async
        logger.info(f"[{new_job_id}] Launched probe_source (source had no streams_json)")
    except Exception as e:
        logger.warning(f"[{new_job_id}] probe_source dispatch failed: {e}")

    return jsonify({'status': 'success', 'job_id': new_job_id}), 201

@app.post('/start_job/<job_id>')
def start_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if job.get('status') != STATUS_READY:
        return jsonify({'status': 'invalid', 'message': 'Job is not in READY state'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

    now = str(time.time())
    redis_client.hset(job_key, mapping={'status': STATUS_STARTING, 'started_at': now})

    # Wake, wait for heartbeats, set hint, then launch
    _launch_after_warmup(job_key, job_id, filename)
    return jsonify({'status': 'started'}), 200

@app.post('/restart_job/<job_id>')
def restart_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if job.get('status') not in [STATUS_STOPPED, STATUS_FAILED, STATUS_DONE]:
        return jsonify({'status': 'invalid', 'message': 'Job is not in STOPPED/FAILED/DONE state.'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

    # ------ Reset job state and start fresh ------
    job_dir = job.get('job_dir', '')

    logger.info(f"[{job_id}] Deleting job dir {job_dir!r}")
    if job_dir and os.path.exists(job_dir):
        try:
            shutil.rmtree(job_dir)
        except Exception as e:
            logger.warning(f"[{job_id}] Failed to delete job directory {job_dir}: {e}")

    globals_map = redis_client.hgetall('settings:global')
    def _int(v, default):
        try:
            return int(v)
        except Exception:
            return default

    segment_duration = _int(
        job.get('segment_duration', globals_map.get('segment_duration')),
        10
    )
    number_parts = _int(
        job.get('number_parts', globals_map.get('number_parts')),
        2
    )
    serialize_existing = job.get('serialize_pipeline', (globals_map.get('serialize_pipeline', '0')))

    for key in redis_client.scan_iter(f"{job_key}*"):
        redis_client.delete(key)

    now = time.time()
    selected_v_stream = job.get('selected_v_stream', 0)
    selected_a_stream = job.get('selected_a_stream', 0)

    new_job = {
        'job_id': job_id,
        'filename': filename,
        'status': STATUS_STARTING,
        'created_at': str(now),
        'started_at': str(now),
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'serialize_pipeline': '1' if str(serialize_existing) in ('1','true','True') else '0',
        'selected_v_stream': selected_v_stream,
        'selected_a_stream': selected_a_stream,
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
    }
    redis_client.hset(f"job:{job_id}", mapping=new_job)

    # Wake, wait for heartbeats, set hint, then launch
    _launch_after_warmup(f"job:{job_id}", job_id, filename)
    return jsonify({'status': 'started'}), 200

@app.get('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.post('/stop_job/<job_id>')
def stop_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    redis_client.hset(job_key, 'status', STATUS_STOPPED)
    huey.revoke_by_id(job_id)
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

    # remove from the jobs index
    try:
        redis_client.srem("jobs:all", job_key)
    except Exception:
        pass

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
    return jsonify(job_data)

# ---------------- Per-job settings (PAUSED only) ------------
@app.route('/job_settings/<job_id>', methods=['GET', 'POST'])
def job_settings(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'error': 'Job not found'}), 404

    if request.method == 'GET':
        job = redis_client.hgetall(job_key)
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
        seg   = int(data.get('segment_duration', job.get('segment_duration', 10)))
        parts = int(data.get('number_parts', job.get('number_parts', 2)))
        serialize_pipeline = bool(data.get('serialize_pipeline',
                                   job.get('serialize_pipeline', '0') in ('1','true','True')))
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
