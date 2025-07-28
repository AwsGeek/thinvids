from celery import Celery
from celery.exceptions import Retry
import uuid
import redis
import time
import math
import os
import subprocess
import glob
import logging
import re

app = Celery('tasks', broker='redis://192.168.0.120:6379/0', backend='redis://192.168.0.120:6379/1')
redis_client = redis.Redis(host='192.168.0.120', port=6379, db=1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEGMENT_DURATION = 10  # seconds
CHUNKS_PER_GROUP = 5
IDEAL_SEGMENT_COUNT = 300

@app.task(bind=True)
def segment_video(self, job_id, file_path, filename):
    try:
        if not redis_client.exists(f"job:{job_id}"):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}        
            
        logger.info(f"VEM Segmenting {file_path}")

        job_dir = os.path.join('/chunks', f"job_{job_id}")
        os.makedirs(job_dir, exist_ok=True)
        
        chunk_prefix = os.path.join(job_dir, f"chunk_%03d.ts")

        redis_client.hset(f"job:{job_id}", mapping={
            'status': 'SEGMENTING',
            'segment_progress': 0,
            'encode_progress': 0,
            'combine_progress': 0,
            'job_dir': job_dir,
            'started_at': time.time()
        })

        # Log stream info
        try:
            info = subprocess.check_output([
                'ffprobe', '-v', 'error',
                '-show_entries', 'stream=index,codec_type,codec_name:stream_tags=language,title',
                '-of', 'json', file_path
            ], stderr=subprocess.STDOUT, text=True)
            logger.info(f"VEM [{job_id}] Streams:\n{info}")

        except Exception as e:
            logger.warning(f"VEM [{job_id}] ffprobe failed: {e}")

        # Estimate number of segments
        segment_duration = SEGMENT_DURATION
        try:
            duration_output = subprocess.check_output([
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1',
                file_path
            ], text=True).strip()
            duration = float(duration_output)
            segment_duration = max(10, int(duration / IDEAL_SEGMENT_COUNT))
            logger.info(f"VEM [{job_id}] Calculated segment duration: {segment_duration}")
            estimated_chunks = math.ceil(duration / segment_duration)
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Duration estimate failed: {e}")
            estimated_chunks = 0

        redis_client.hset(f"job:{job_id}", mapping={
            'total_chunks': estimated_chunks,
            'completed_chunks': 0
        })

        cmd = [
            'ffmpeg', '-i', file_path,
            '-map', '0',  # all streams
            '-c', 'copy',
            '-f', 'segment',
            '-segment_time', str(segment_duration),
            '-reset_timestamps', '1',
            chunk_prefix
        ]
        logger.info(f"VEM [{job_id}] Running: {' '.join(cmd)}")

        segment_re = re.compile(r"Opening '(.*?)chunk_(\d+)\.ts' for writing")
        previous_chunk_path = None
        previous_chunk_index = None

        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        queued_chunks = set()
        while True:
            if not redis_client.exists(f"job:{job_id}"):
                logger.warning(f"VEM [{job_id}] Job deleted. Terminating segmentation.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stderr.readline()
            if not line:
                break
            match = segment_re.search(line)
            if match:
                chunk_path = match.group(1) + f"chunk_{match.group(2)}.ts"
                chunk_index = int(match.group(2))

                redis_client.hset(f"job:{job_id}", mapping={
                    'status': f"SEG {chunk_index + 1}/{estimated_chunks}",
                    'segment_progress': int(((chunk_index + 1) / estimated_chunks) * 100)
                })

                if previous_chunk_path is not None:
                    encode_chunk.apply_async(args=[job_id, previous_chunk_path, previous_chunk_index])
                    queued_chunks.add(previous_chunk_index)
                
                logger.info(f"VEM [{job_id}] Segmented chunk {previous_chunk_index}")

                previous_chunk_path = chunk_path
                previous_chunk_index = chunk_index

        process.wait()

        if process.returncode != 0:
            logger.error(f"VEM [{job_id}] FFmpeg failed with code {process.returncode}")
            redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
            return {'status': 'FAILED'}

        # Encode final chunk
        if previous_chunk_path:
            encode_chunk.apply_async(args=[job_id, previous_chunk_path, previous_chunk_index])
            queued_chunks.add(previous_chunk_index)

        total_chunks = previous_chunk_index + 1

        # check for missing chunks
        missing_chunks = set(range(total_chunks)) - queued_chunks
        for index in sorted(missing_chunks):
            chunk_path = os.path.join(job_dir, f"chunk_{index:04d}.ts")
            logger.warning(f"VEM [{job_id}] Missing encode job for {chunk_path}.")
            if os.path.exists(chunk_path):
                encode_chunk.apply_async(args=[job_id, chunk_path, index])

        redis_client.hset(f"job:{job_id}", mapping={
            'status': 'SEGMENTED',
            'total_chunks': total_chunks,
            'segment_progress': 100
        })

        monitor_encoding.apply_async(args=[job_id])
        return {'status': 'SEGMENTED', 'total_chunks': total_chunks}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Segmenting failed")
        logger.exception(f"VEM {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        return {'status': 'FAILED'}


@app.task(bind=True, max_retries=3, default_retry_delay=5)
def encode_chunk(self, job_id, chunk_path, chunk_index):
    try:

        if not redis_client.exists(f"job:{job_id}"):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}        

        # Check if chunk file exists
        if not os.path.exists(chunk_path):
            logger.warning(f"VEM [{job_id}] Missing chunk file: {chunk_path}. Aborting encode.")
            return {'status': 'ABORTED'}

        encoded_path = chunk_path.replace('chunk_', 'encoded_chunk_').replace('.ts', '.mp4')

        cmd = [
            'ffmpeg', '-vaapi_device', '/dev/dri/renderD128',
            '-i', chunk_path,
            '-vf', 'scale=-1:720,format=nv12,hwupload',
            '-map', '0:v:0',
            '-map', '0:a:0?',
            '-c:v', 'h264_vaapi',
            '-rc_mode', 'CQP',
            '-qp', '30',
            '-c:a', 'aac',
            '-ac', '2',
            '-b:a', '192k',
            encoded_path
        ]
        logger.info(f"VEM [{job_id}] Encoding chunk {chunk_index}")
        subprocess.run(cmd, check=True)

        redis_client.hincrby(f"job:{job_id}", 'completed_chunks', 1)

        total_chunks = int(redis_client.hget(f"job:{job_id}", 'total_chunks').decode())
        completed = int(redis_client.hget(f"job:{job_id}", 'completed_chunks').decode())
        redis_client.hset(
            f"job:{job_id}", mapping={
                'encode_progress': int((completed / total_chunks) * 100), 
                'status': f"ENC {completed}/{total_chunks}"
            })

        return {'status': 'COMPLETED', 'chunk_index': chunk_index}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] Encode failed: {e}")
        try:
            self.retry(exc=e)
        except Retry:
            redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
            raise
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Unexpected encode error")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        raise


@app.task(bind=True)
def monitor_encoding(self, job_id):
    job_key = f"job:{job_id}"
    try:

        if not redis_client.exists(f"job:{job_id}"):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}        

        data = redis_client.hgetall(job_key)
        total = int(data.get(b'total_chunks', b'0').decode())
        completed = int(data.get(b'completed_chunks', b'0').decode())

        if completed >= total:
            logger.info(f"VEM [{job_id}] Encoding complete. Starting combine.")
            stitch_video.apply_async(args=[job_id])
            return {'status': 'DONE'}

        monitor_encoding.apply_async(args=[job_id], countdown=5)
        return {'status': 'WAITING', 'completed': completed, 'total': total}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Monitor failed")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}


@app.task(bind=True)
def stitch_video(self, job_id):
    try:

        if not redis_client.exists(f"job:{job_id}"):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}        

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM No job found with id: {job_id}")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get(b'job_dir', b'').decode()
        output_path = os.path.join(job_dir, "output.mp4")
        total_chunks = int(job_data[b'total_chunks'].decode())
        concat_file = os.path.join(job_dir, "concat_list.txt")

        # Wait up to 10 seconds for all encoded chunks to appear
        retries = 10
        for attempt in range(retries):
            missing = []
            for i in range(total_chunks):
                path = os.path.join(job_dir, f"encoded_chunk_{i:03d}.mp4")
                if not os.path.exists(path):
                    missing.append(path)
            if not missing:
                break
            logger.warning(f"VEM [{job_id}] Waiting for {len(missing)} chunks to appear: {missing[:3]}...")
            time.sleep(10)
        else:
            logger.error(f"VEM [{job_id}] Still missing chunks after waiting: {missing}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED', 'reason': f'Missing {missing}'}

        # Write concat list
        with open(concat_file, 'w') as f:
            for i in range(total_chunks):
                f.write(f"file 'encoded_chunk_{i:03d}.mp4'\n")

        redis_client.hset(job_key, 'status', 'STITCHING')

        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0',
            '-i', concat_file,
            '-c', 'copy',
            '-progress', 'pipe:1',
            output_path
        ]
        logger.info(f"VEM [{job_id}] Stitching command: {' '.join(cmd)}")

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        current_out_time = 0
        while True:
            if not redis_client.exists(job_key):
                logger.warning(f"VEM [{job_id}] Job deleted. Terminating stitching.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stdout.readline()
            if not line:
                break
            line = line.strip()
            if '=' not in line:
                continue
            key, value = line.split('=', 1)
            if key == 'out_time_ms':
                current_out_time = int(value) / 1_000_000
                progress = int((current_out_time / (total_chunks * 10)) * 100)
                redis_client.hset(job_key, 'combine_progress', min(progress, 100))
            elif key == 'progress' and value == 'end':
                break

        process.wait()

        if process.returncode != 0:
            logger.error(f"VEM [{job_id}] FFmpeg stitching failed")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED'}

        # Cleanup
        for pattern in ('chunk_*.ts', 'encoded_chunk_*.mp4'):
            for file in glob.glob(os.path.join(job_dir, pattern)):
                try:
                    os.remove(file)
                except Exception as e:
                    logger.warning(f"VEM [{job_id}] Failed to remove {file}: {e}")

        redis_client.hset(job_key, mapping={
            'status': 'COMPLETED',
            'output_path': output_path,
            'ended_at': time.time(),
            'combine_progress': 100
        })

        return {'status': 'COMPLETED', 'output': output_path}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] FFmpeg error: {e.stderr}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Stitching error")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}
