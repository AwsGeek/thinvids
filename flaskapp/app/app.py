from flask import Flask, render_template, request, jsonify
import redis
from celery import Celery
from celery.utils import uuid
import time
import os
import logging
import humanize
import shutil


app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://redis:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://redis:6379/1'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

redis_client = redis.Redis(host='redis', port=6379, db=1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/tasks', methods=['GET'])
def get_jobs():
    jobs = []
    for key in redis_client.scan_iter("job:*"):
        try:
            job_id = key.decode().split(':', 1)[1]
            raw_data = redis_client.hgetall(key)
            job_data = {k.decode(): v.decode() for k, v in raw_data.items()}

            started = float(job_data.get('started_at', '0') or 0)
            ended = float(job_data.get('ended_at', '0') or 0)
            now = time.time()
            elapsed = int((ended if ended > 0 else now) - started) if started > 0 else 0

            total = int(job_data.get('total_chunks', '0') or 0)
            done = int(job_data.get('completed_chunks', '0') or 0)

            segment_progress = int(job_data.get('segment_progress', '0') or 0)
            encode_progress = int((done / total) * 100) if total > 0 else 0
            combine_progress = int(job_data.get('combine_progress', '0') or 0)

            jobs.append({
                'job_id': job_id,
                **job_data,  # include all fields from Redis
                'segment_progress': segment_progress,
                'encode_progress': encode_progress,
                'combine_progress': combine_progress,
                'elapsed': elapsed,
                'started': started
            })
        except Exception as e:
            logger.warning(f"[{key}] Job error, skipping: {e}")
    
    jobs.sort(key=lambda x: x['started'], reverse=True)
    return jsonify(jobs)

import json
import humanize

@app.route('/job_properties/<job_id>')
def job_properties(job_id):
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        return jsonify({'error': 'Job not found'}), 404

    job_data = {k.decode(): v.decode() for k, v in redis_client.hgetall(key).items()}
    return jsonify(job_data)


@app.route('/add_task', methods=['POST'])
def add_task():
    data = request.json
    filename = data.get('filename')
    
    if filename and filename.endswith(('.mkv', '.mp4')):
        job_id = uuid()
        now = str(time.time())
        redis_client.hset(f"job:{job_id}", mapping={
            'job_id': job_id,
            'filename': filename,
            'status': 'QUEUED',
            'created_at': now,
            'started_at': now,
            'total_chunks': 0,
            'completed_chunks': 0
        })
        celery.send_task('tasks.segment_video', args=[job_id, f'/watch/{filename}', filename])
        return jsonify({'status': 'success', 'job_id': job_id}), 201
    return jsonify({'status': 'error', 'message': 'Invalid file format'}), 400

@app.route('/delete_task/<job_id>', methods=['DELETE'])
def delete_task(job_id):
    job_key = f"job:{job_id}"
    if redis_client.exists(job_key):
        job_data = redis_client.hgetall(job_key)
        job_dir = job_data.get(b'job_dir', b'').decode() if b'job_dir' in job_data else None

        app.logger.info (f"[{job_id}] Deleting {job_dir}")

        # Delete job record from Redis
        redis_client.delete(job_key)

        # Delete job directory and all contents if present
        if job_dir and os.path.exists(job_dir):
            try:
                import shutil
                shutil.rmtree(job_dir)
            except Exception as e:
                app.logger.warning(f"[{job_id}] Failed to delete job directory {job_dir}: {e}")

        return jsonify({'status': 'deleted'}), 200
    return jsonify({'status': 'not found'}), 404

@app.route("/log/<job_id>")
def serve_log(job_id):
    job_key = f"job:{job_id}"
    log_path = redis_client.hget(job_key, 'log_path')
    if not log_path:
        abort(404)
    log_path = log_path.decode()
    if not os.path.isfile(log_path):
        abort(404)
    with open(log_path) as f:
        content = f.read()
    return f"<pre style='white-space: pre-wrap;'>{content}</pre>"    