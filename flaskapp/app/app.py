from flask import Flask, render_template, render_template_string, request, jsonify, abort, send_file
import redis
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

# import backend task planner
from tasks import segment_video, probe_source

app = Flask(__name__)
app.secret_key = os.urandom(24).hex()

# NOTE: container/service hostnames; adjust if needed
huey = RedisHuey('tasks', host='redis', port=6379, db=0)
redis_client = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATUS_READY = 'READY'
STATUS_RUNNING = 'RUNNING'
STATUS_STOPPED = 'STOPPED'
STATUS_FAILED = 'FAILED'
STATUS_DONE    = 'DONE'

# ------------------------ Wake-on-LAN helpers ------------------------

# TTL used to decide if a node is "active" (matches agent expiry window)
WOL_TTL_SEC = int(os.getenv("TTL_SEC", "15"))
WOL_TTL_GRACE_SEC = int(os.getenv("TTL_GRACE_SEC", "5"))
WOL_DEFAULT_BCAST = os.getenv("WOL_BROADCAST", "255.255.255.255")
WOL_WAKE_ALL = os.getenv("WOL_WAKE_ALL", "0") in ("1", "true", "True")
WOL_USE_PROXY = os.getenv("WOL_USE_PROXY", "0") in ("1","true","True")
WOL_CHANNEL   = os.getenv("WOL_CHANNEL", "wol:wake")

def _active_nodes():
    """Return a set of hostnames with fresh metrics heartbeat."""
    now_cutoff = int(time.time()) - (WOL_TTL_SEC + WOL_TTL_GRACE_SEC)
    active = set()
    for key in redis_client.scan_iter("metrics:node:*"):
        try:
            d = redis_client.hgetall(key) or {}
            ts = int(float(d.get("ts", 0)))
            host = d.get("hostname") or key.split(":")[-1]
            if ts >= now_cutoff:
                active.add(host)
        except Exception:
            continue
    return active

def _normalize_mac(mac: str) -> str:
    """Return MAC as 12 hex chars or raise ValueError."""
    s = re.sub(r'[^0-9A-Fa-f]', '', mac or '')
    if len(s) != 12:
        raise ValueError(f"Invalid MAC: {mac!r}")
    return s.lower()

def _send_magic(mac: str, bcast: str, port: int = 9, repeats: int = 3):
    """Send magic packet; if WOL_USE_PROXY=1 publish to host-side wol-proxy."""
    try:
        payload = {"mac": mac, "bcast": bcast, "port": int(port), "repeats": int(repeats)}
        redis_client.publish(WOL_CHANNEL, json.dumps(payload))
        logger.info(f"WOL(proxy): queued {mac} via {bcast}")
        return
    except Exception as e:
        logger.warning(f"WOL(proxy) publish failed; falling back to UDP: {e}")

    # direct UDP (works if app is on host network; otherwise proxy recommended)
    # mac_hex = _normalize_mac(mac)
    # packet = b'\xff' * 6 + bytes.fromhex(mac_hex) * 16
    # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
    #     s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    #     for _ in range(max(1, repeats)):
    #         s.sendto(packet, (bcast, port))

def _load_wol_nodes():
    """
    Load WOL targets from persistent Redis hash published by agent.py.

    Reads:  HGETALL nodes:mac  -> {hostname: "aa:bb:cc:dd:ee:ff", ...}

    Returns:
        {hostname: {"mac": "<mac>", "bcast": None}}
    """
    nodes = {}
    try:
        mapping = redis_client.hgetall("nodes:mac") or {}
        for host, mac in mapping.items():
            host = (host or "").strip()
            mac  = (mac or "").strip()
            if host and mac:
                nodes[host] = {"mac": mac, "bcast": None}
    except Exception as e:
        logger.warning(f"WOL: failed to read nodes:mac: {e}")
    return nodes

def wake_all_nodes():
    """
    Wake ALL nodes discovered in nodes:mac (persistent), regardless of recent activity.
    Returns number of packets sent (unique hosts).
    """
    targets = _load_wol_nodes()
    if not targets:
        logger.warning("WOL: no nodes found in Redis hash 'nodes:mac'.")
        return 0

    sent = 0
    for host, cfg in targets.items():
        mac = cfg.get("mac")
        bcast = cfg.get("bcast") or WOL_DEFAULT_BCAST
        if not mac:
            continue

        try:
            _send_magic(mac, bcast)
            sent += 1
            logger.info(f"WOL: sent to {host} ({mac}) via {bcast}")
        except Exception as e:
            logger.warning(f"WOL: failed for {host} ({mac}): {e}")

    logger.info(f"WOL: packets sent for {sent} host(s)")
    return sent

# NEW: wake a single node by hostname
@app.post('/nodes/wake/<hostname>')
def nodes_wake_one(hostname):
    nodes = _load_wol_nodes()
    cfg = nodes.get(hostname)
    if not cfg:
        return jsonify({'error': f'Unknown node {hostname} or missing MAC in nodes:mac'}), 404
    mac = cfg.get('mac')
    bcast = cfg.get('bcast') or WOL_DEFAULT_BCAST
    try:
        _send_magic(mac, bcast)
        logger.info(f"WOL: sent to {hostname} ({mac}) via {bcast}")
        return jsonify({'status': 'ok', 'host': hostname, 'mac': mac})
    except Exception as e:
        logger.warning(f"WOL: failed for {hostname} ({mac}): {e}")
        return jsonify({'error': str(e)}), 500

# NEW: wake all nodes
@app.post('/nodes/wake_all')
def nodes_wake_all():
    try:
        sent = wake_all_nodes()
        return jsonify({'status': 'ok', 'sent': sent})
    except Exception as e:
        logger.exception("Wake All failed")
        return jsonify({'error': str(e)}), 500

# -------------------------- Views --------------------------
@app.route('/')
def index():
    return render_template('index.html')

# --- Nodes page & data ---
def _resolve_ip(hostname: str) -> str:
    try:
        return socket.gethostbyname(hostname)
    except Exception:
        return ""

def _natural_key(host: str):
    m = re.search(r'(\d+)', host or '')
    return (int(m.group(1)) if m else 0, host or '')

@app.get('/nodes_data')
def nodes_data():
    """
    Returns list of nodes with hostname, ip, mac, last_seen_ts, active.
    MACs come from persistent 'nodes:mac'; last_seen from metrics:node:* (if present).
    """
    mapping = redis_client.hgetall('nodes:mac') or {}
    cutoff = int(time.time()) - (WOL_TTL_SEC + WOL_TTL_GRACE_SEC)
    items = []
    for host, mac in mapping.items():
        host = (host or '').strip()
        mac  = (mac or '').strip()
        if not host or not mac:
            continue
        ts = 0
        try:
            md = redis_client.hgetall(f"metrics:node:{host}") or {}
            ts = int(float(md.get('ts', 0) or 0))
        except Exception:
            ts = 0
        items.append({
            "hostname": host,
            "ip": _resolve_ip(host),
            "mac": mac,
            "last_seen_ts": ts,
            "active": bool(ts and ts >= cutoff),
        })
    items.sort(key=lambda x: _natural_key(x["hostname"]))
    return jsonify({"nodes": items})

# NEW: real nodes page
@app.get('/nodes')
def nodes_page():
    return render_template('nodes.html')

# --- new route: metrics snapshot (all nodes) ---
@app.get('/metrics_snapshot')
def metrics_snapshot():
    nodes = []
    for key in redis_client.scan_iter("metrics:node:*"):
        try:
            data = redis_client.hgetall(key) or {}
            # coerce numeric fields
            out = {
                "key": key,
                "hostname": data.get("hostname") or key.split(":")[-1],
                "ts": int(float(data.get("ts", 0))),
                "cpu": float(data.get("cpu", 0.0)),
                "gpu": float(data.get("gpu", -1.0)),
                "mem": float(data.get("mem", 0.0)),
                "mem_used": int(float(data.get("mem_used", 0))),
                "mem_total": int(float(data.get("mem_total", 0))),
                "rx_bps": int(float(data.get("rx_bps", 0))),
                "tx_bps": int(float(data.get("tx_bps", 0)))
            }
            nodes.append(out)
        except Exception:
            pass
    # sort by hostname for stable bar order
    nodes.sort(key=lambda n: n["hostname"])
    return jsonify({"nodes": nodes})

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
    # --- gather all jobs ---
    jobs = []
    for key in redis_client.scan_iter("job:*"):
        try:
            job_id = key.split(':', 1)[1]
            raw = redis_client.hgetall(key)
            job_data = {k: v for k, v in raw.items()}

            started = float(job_data.get('started_at', '0') or 0)
            ended = float(job_data.get('ended_at', '0') or 0)
            created = float(job_data.get('created_at', '0') or 0)
            now = time.time()
            elapsed = int((ended if ended > 0 else now) - started) if started > 0 else 0

            total = int(job_data.get('total_chunks', '0') or 0)
            done = int(job_data.get('completed_chunks', '0') or 0)
            stitched = int(job_data.get('stitched_chunks', '0') or 0)

            segment_progress = int(job_data.get('segment_progress', '0') or 0)
            encode_progress = int((done / total) * 100) if total > 0 else 0
            combine_progress = int(job_data.get('combine_progress', '0') or 0)

            jobs.append({
                'job_id': job_id,
                **job_data,
                'segment_progress': segment_progress,
                'encode_progress': encode_progress,
                'combine_progress': combine_progress,
                'elapsed': elapsed,
                'started': started,
                'created': created,
                'stitched_chunks': stitched
            })
        except Exception:
            pass

    # --- if no pagination params, return full list (back-compat) ---
    if 'page' not in request.args:
        return jsonify(jobs)

    # --- pagination + sorting ---
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get('page_size', 10))
        page_size = min(max(1, page_size), 50)
    except Exception:
        page_size = 10

    sort_by = (request.args.get('sort_by') or 'date').lower()   # 'date'|'filename'|'status'|'encode'
    sort_dir = (request.args.get('sort_dir') or 'desc').lower() # 'asc' or 'desc'

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

    reverse = (sort_dir != 'asc')

    if sort_by == 'date':
        jobs.sort(key=lambda j: (0 if float(j.get('started') or 0) == 0 else 1,
                                 float(j.get('created') or 0) if float(j.get('started') or 0) == 0
                                 else float(j.get('started') or 0)),
                  reverse=False)
        paused = [j for j in jobs if float(j.get('started') or 0) == 0]
        running = [j for j in jobs if float(j.get('started') or 0) != 0]
        paused.sort(key=lambda j: float(j.get('created') or 0), reverse=True if reverse else False)
        running.sort(key=lambda j: float(j.get('started') or 0), reverse=True if reverse else False)
        jobs_sorted = paused + running
    else:
        jobs_sorted = sorted(jobs, key=sort_key, reverse=reverse)

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
    now = str(time.time())
    global_settings = redis_client.hgetall('settings:global') or {}

    auto_start_global = global_settings.get('auto_start', '1') == '1'
    auto_start_effective = (auto_start_global and not force_paused)
    serialize_global  = global_settings.get('serialize_pipeline', '0') == '1'

    segment_duration = int(global_settings.get('segment_duration', 10))
    number_parts = int(global_settings.get('number_parts', 2))

    status = STATUS_RUNNING if auto_start_effective else STATUS_READY
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
    redis_client.hset(f"job:{job_id}", mapping=job_settings)

    # Kick off async probe on a worker
    probe_source(job_id, full_path)

    # Optionally auto-start segmentation
    if auto_start_effective:
        try:
            wake_all_nodes()  # wake thinman nodes
        except Exception as e:
            logger.warning(f"WOL during add_job failed: {e}")
        segment_video(job_id, f'/watch/{filename}', filename)

    return jsonify({'status': 'success', 'job_id': job_id}), 201

@app.route('/copy_job', methods=['POST'])
def copy_job():
    """
    Create a new PAUSED job by copying per-job settings (e.g., filename,
    number_parts, segment_duration) from an existing job. Do NOT copy
    stats/metadata (progress, timings, dirs, etc).
    """
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
    redis_client.hset(f"job:{new_job_id}", mapping=new_job)
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
    redis_client.hset(job_key, mapping={'status': STATUS_RUNNING, 'started_at': now})

    try:
        wake_all_nodes()  # wake thinman nodes
    except Exception as e:
        logger.warning(f"WOL during start_job failed: {e}")

    segment_video(job_id, f'/watch/{filename}', filename)

    return jsonify({'status': 'started'}), 200

@app.post('/restart_job/<job_id>')
def restart_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if job.get('status') not in [STATUS_STOPPED, STATUS_FAILED]:
        return jsonify({'status': 'invalid', 'message': 'Job is not in STOPPED/FAILED state.'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

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
        'status': STATUS_RUNNING,
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

    try:
        wake_all_nodes()  # wake thinman nodes
    except Exception as e:
        logger.warning(f"WOL during restart_job failed: {e}")

    segment_video(job_id, f'/watch/{filename}', filename)
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

    # Let the browser seek; Werkz eug/Flask will handle range requests for local files.
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
    # Allow changing when the job is not actively RUNNING
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
