from flask import Flask, render_template, request, jsonify, abort
import redis
from huey import RedisHuey
import uuid
import time
import os
import logging
import humanize
import shutil

# import backend task planner
from tasks import segment_video, probe_source

app = Flask(__name__)
app.secret_key = os.urandom(24).hex()

# NOTE: container/service hostnames; adjust if needed
huey = RedisHuey('tasks', host='redis', port=6379, db=0)
redis_client = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------- Views --------------------------
@app.route('/')
def index():
    return render_template('index.html')


# ------------------- Global settings API -------------------
@app.get('/global_settings')
def get_global_settings():
    g = redis_client.hgetall('settings:global') or {}
    segment_duration = int(g.get('segment_duration', 10))
    number_parts = int(g.get('number_parts', 2))
    auto_start = g.get('auto_start', '1') == '1'
    return jsonify({
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'auto_start': auto_start
    })

@app.post('/global_settings')
def post_global_settings():
    data = request.get_json(silent=True) or {}
    try:
        segment_duration = int(data.get('segment_duration', 10))
        number_parts = int(data.get('number_parts', 2))
        auto_start = bool(data.get('auto_start', True))

        if not (1 <= segment_duration <= 300):
            return "Segment length must be between 1 and 300 seconds.", 400
        if not (1 <= number_parts <= 8):
            return "Number of parts must be between 1 and 8.", 400

        redis_client.hset('settings:global', mapping={
            'segment_duration': segment_duration,
            'number_parts': number_parts,
            'auto_start': '1' if auto_start else '0'
        })
        logger.info(f"[GLOBAL] Saved settings: segment_duration={segment_duration}, number_parts={number_parts}, auto_start={auto_start}")
        return jsonify({'status': 'ok'})
    except Exception as e:
        logger.exception(f"[GLOBAL] Failed to save settings: {e}")
        return "Failed to save global settings.", 500


# ------------------------ Jobs API -------------------------
@app.get('/jobs')
def list_jobs():
    jobs = []
    for key in redis_client.scan_iter("job:*"):
        try:
            job_id = key.split(':', 1)[1]
            raw = redis_client.hgetall(key)
            job_data = {k: v for k, v in raw.items()}

            started = float(job_data.get('started_at', '0') or 0)
            ended = float(job_data.get('ended_at', '0') or 0)
            created = float(job_data.get('created_at', '0') or 0)  # <-- NEW
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
                'created': created,            # <-- NEW
                'stitched_chunks': stitched
            })
        except Exception as e:            
            #logger.warning(f"[{key}] list_jobs error, skipping: {e}")
            pass

    # REMOVED backend sort; frontend handles it:
    # jobs.sort(key=lambda x: x['started'], reverse=True)

    return jsonify(jobs)

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

    segment_duration = int(global_settings.get('segment_duration', 10))
    number_parts = int(global_settings.get('number_parts', 2))

    # Placeholders until worker probes the source
    job_settings = {
        'job_id': job_id,
        'filename': filename,
        'status': 'QUEUED' if auto_start_effective else 'PAUSED',
        'created_at': now,
        'started_at': now if auto_start_effective else '0',
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        'segment_duration': segment_duration,
        'number_parts': number_parts,
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

    src = redis_client.hgetall(source_key)  # decode_responses=True -> str keys/values

    # Pull the settings we want to inherit. Fallback to globals if needed.
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

    # New job skeleton (no stats/meta)
    new_job_id = str(uuid.uuid4())
    now = time.time()
    new_job = {
        'job_id': new_job_id,
        'filename': filename,
        'status': 'PAUSED',              # always paused when copied
        'created_at': str(now),
        'started_at': '0',               # not started yet
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        # leave out: job_dir, encode times, source_* fields, etc.
    }

    redis_client.hset(f"job:{new_job_id}", mapping=new_job)
    logger.info(f"[{new_job_id}] Copied from {source_job_id} with filename={filename}, "
                f"segment_duration={segment_duration}, number_parts={number_parts} (PAUSED)")

    return jsonify({'status': 'success', 'job_id': new_job_id}), 201

@app.post('/start_job/<job_id>')
def start_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if job.get('status') != 'PAUSED':
        return jsonify({'status': 'invalid', 'message': 'Job is not in PAUSED state'}), 400

    filename = job.get('filename')
    if not filename:
        return jsonify({'status': 'invalid', 'message': 'Missing filename'}), 400

    now = str(time.time())
    redis_client.hset(job_key, mapping={'status': 'QUEUED', 'started_at': now})
    segment_video(job_id, f'/watch/{filename}', filename)
    return jsonify({'status': 'started'}), 200

@app.post('/restart_job/<job_id>')
def restart_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    job = redis_client.hgetall(job_key)
    if not job.get('status') in ['COMPLETED', 'FAILED', 'ABORTED']:
        return jsonify({'status': 'invalid', 'message': 'Job is not in terminal state.'}), 400

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

   # Pull the settings we want to inherit. Fallback to globals if needed.
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

    for key in redis_client.scan_iter(f"{job_key}*"):
        redis_client.delete(key)

    # New job skeleton (no stats/meta), but use the same job ID
    now = time.time()
    new_job = {
        'job_id': job_id,
        'filename': filename,
        'status': 'QUEUED',
        'created_at': str(now),
        'started_at': str(now),
        'segment_duration': segment_duration,
        'number_parts': number_parts,
        'total_chunks': 0,
        'completed_chunks': 0,
        'stitched_chunks': 0,
        # leave out: job_dir, encode times, source_* fields, etc.
    }

    redis_client.hset(f"job:{job_id}", mapping=new_job)

    segment_video(job_id, f'/watch/{filename}', filename)
    return jsonify({'status': 'started'}), 200

@app.post('/stop_job/<job_id>')
def stop_job(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'status': 'not found'}), 404

    redis_client.hset(job_key, 'status', 'ABORTED')
    # best-effort revoke; background task ids differ, but keep parity with prior behavior
    huey.revoke_by_id(job_id)
    logger.info(f"[{job_id}] Stopped job and attempted to revoke tasks")
    return jsonify({'status': 'aborted'}), 200


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


# ------------------ Job info / utilities -------------------
@app.get('/job_properties/<job_id>')
def job_properties(job_id):
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        return jsonify({'error': 'Job not found'}), 404
    job_data = {k: v for k, v in redis_client.hgetall(key).items()}
    return jsonify(job_data)


@app.get('/job_files/<job_id>')
def job_files(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'error': 'Job not found'}), 404

    job_data = redis_client.hgetall(job_key) or {}
    job_dir = job_data.get('job_dir', '')
    if not job_dir or not os.path.exists(job_dir):
        return jsonify({'error': 'Job directory not found'}), 404

    files = []
    for entry in os.scandir(job_dir):
        if entry.is_file():
            stat = entry.stat()
            files.append({
                'name': entry.name,
                'size': humanize.naturalsize(stat.st_size),
                'mtime': stat.st_mtime,
                'mtime_display': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat.st_mtime))
            })

    return render_template('job_files.html', job_id=job_id, files=files)


@app.get('/job_file_content/<job_id>/<path:filename>')
def job_file_content(job_id, filename):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'error': 'Job not found'}), 404

    job_data = redis_client.hgetall(job_key) or {}
    job_dir = job_data.get('job_dir', '')
    if not job_dir or not os.path.exists(job_dir):
        return jsonify({'error': 'Job directory not found'}), 404

    file_path = os.path.join(job_dir, filename)
    if not file_path.endswith('.txt') or not os.path.isfile(file_path):
        return jsonify({'error': 'File not found or not a text file'}), 404

    try:
        with open(file_path, 'r') as f:
            content = f.read()
        return jsonify({'content': content})
    except Exception as e:
        logger.error(f"[{job_id}] Failed to read file {file_path}: {e}")
        return jsonify({'error': 'Failed to read file'}), 500


@app.get("/log/<job_id>")
def serve_log(job_id):
    job_key = f"job:{job_id}"
    log_path = redis_client.hget(job_key, 'log_path')
    if not log_path or not os.path.isfile(log_path):
        abort(404)
    with open(log_path) as f:
        content = f.read()
    return f"<pre style='white-space: pre-wrap;'>{content}</pre>"


# ---------------- Per-job settings (PAUSED only) ------------
@app.route('/job_settings/<job_id>', methods=['GET', 'POST'])
def job_settings(job_id):
    job_key = f"job:{job_id}"
    if not redis_client.exists(job_key):
        return jsonify({'error': 'Job not found'}), 404

    if request.method == 'GET':
        job = redis_client.hgetall(job_key)
        return jsonify({
            'job_id': job_id,
            'filename': job.get('filename'),
            'status': job.get('status'),
            'segment_duration': int(job.get('segment_duration', 10)),
            'number_parts': int(job.get('number_parts', 2))
        })

    # POST
    job = redis_client.hgetall(job_key)
    if job.get('status') != 'PAUSED':
        return jsonify({'error': 'Job is not in PAUSED state'}), 400

    data = request.get_json(silent=True) or {}
    try:
        seg = int(data.get('segment_duration', job.get('segment_duration', 10)))
        parts = int(data.get('number_parts', job.get('number_parts', 2)))

        if not (1 <= seg <= 300):
            return jsonify({'error': 'segment_duration must be between 1 and 300'}), 400
        if not (1 <= parts <= 8):
            return jsonify({'error': 'number_parts must be between 1 and 8'}), 400

        redis_client.hset(job_key, mapping={'segment_duration': seg, 'number_parts': parts})
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.exception(f"[{job_id}] Failed to update job settings: {e}")
        return jsonify({'error': 'Failed to update settings'}), 500


# ---------------- Legacy shim (optional) --------------------
# Keep old /tasks routes working temporarily if you want a soft rollout:
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
