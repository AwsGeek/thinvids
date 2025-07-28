from flask import Flask, render_template, request, jsonify
import redis
from celery import Celery
from celery.utils import uuid
import time
import os
import logging
import humanize

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
            job_id = key.decode().split(':')[1]
            data = redis_client.hgetall(key)
            started = float(data.get(b'started_at', b'0').decode() or 0)
            ended = float(data.get(b'ended_at', b'0').decode() or 0)
            now = time.time()
            elapsed = int((ended if ended > 0 else now) - started) if started > 0 else 0

            total = int(data.get(b'total_chunks', b'0').decode() or 0)
            done = int(data.get(b'completed_chunks', b'0').decode() or 0)

            segment_progress = int(data.get(b'segment_progress', b'0').decode() or 0)
            encode_progress = int((done / total) * 100) if total > 0 else 0
            combine_progress = int(data.get(b'combine_progress', b'0').decode() or '0')

            jobs.append({
                'job_id': job_id,
                'filename': data.get(b'filename', b'UNKNOWN').decode(),
                'status': data.get(b'status', b'UNKNOWN').decode(),
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

    job_data = redis_client.hgetall(key)
    filename = job_data.get(b'filename', b'UNKNOWN').decode()
    job_dir = job_data.get(b'job_dir', b'').decode()
    segment_duration = int(job_data.get(b'segment_duration', b'10').decode() or 10)

    def get_media_info(path):
        try:
            out = subprocess.check_output([
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration',
                '-of', 'json', path
            ], text=True)
            result = json.loads(out)['streams'][0]
            return {
                'file': os.path.basename(path),
                'size': humanize.naturalsize(os.path.getsize(path)),
                'width': result.get('width'),
                'height': result.get('height'),
                'duration': f"{float(result.get('duration', 0)):.1f} sec"
            }
        except Exception as e:
            return {'error': str(e)}

    input_info = get_media_info(f"/watch/{filename}")
    output_info = get_media_info(os.path.join(job_dir, 'output.mp4'))

    return jsonify({
        'Input File': input_info,
        'Output File': output_info,
        'Segment Length (sec)': segment_duration,
        'Encoder': 'Intel VAAPI h264_vaapi',
        'Quality': 'CQP QP=30',
        'Audio': 'AAC 2ch 192k'
    })


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