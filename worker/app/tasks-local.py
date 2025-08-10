from huey import RedisHuey
from huey.exceptions import RetryTask
import uuid
import redis
import time
import math
import json
import os
import subprocess
import glob
import logging
import re
import shutil
import requests
import socket
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading

huey = RedisHuey('tasks', host='192.168.0.120', port=6379, db=0)
redis_client = redis.Redis(host='192.168.0.120', port=6379, db=1, decode_responses=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SEGMENT_LENGTH = 10

# Start HTTP server to serve chunks and handle cleanup
def start_http_server(port=8000):
    """
    Initialize an HTTP server on each worker to serve files from /chunks and handle cleanup.
    """
    class LocalFileHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory='/chunks', **kwargs)

        def do_POST(self):
            """
            Handle POST requests for cleanup of local job directories.
            """
            if self.path.endswith('/cleanup'):
                job_id = self.path.split('/')[-2]
                job_dir = os.path.join('/chunks', job_id)
                try:
                    if os.path.exists(job_dir):
                        shutil.rmtree(job_dir)
                        logger.info(f"VEM [{job_id}] Removed local job directory {job_dir} via HTTP cleanup")
                    self.send_response(200)
                    self.end_headers()
                except Exception as e:
                    logger.warning(f"VEM [{job_id}] Failed to remove local job dir {job_dir}: {e}")
                    self.send_response(500)
                    self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()

    try:
        server = HTTPServer(('0.0.0.0', port), LocalFileHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(f"VEM Started HTTP server on port {port}")
    except OSError as e:
        if "Address already in use" in str(e):
            logger.info(f"VEM HTTP server already running on port {port}")
        else:
            raise

# Start HTTP server when module is imported
start_http_server()

def get_hostname():
    """
    Retrieve the hostname of the worker.
    """
    return socket.gethostname()

def is_job_halted(job_id):
    """
    Check if a job is halted based on its status in Redis.
    """
    status = redis_client.hget(f"job:{job_id}", "status")
    if not status:
        return True
    return status in ("FAILED", "ABORTED")

@huey.task()
def segment_video(job_id, file_path, filename):
    """
    Segment an input video into chunks, store them locally, and enqueue encoding tasks in batch.
    """
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        segment_start_time = time.time()
        logger.info(f"VEM [{job_id}] Segmenting {file_path}")

        job_key = f"job:{job_id}"
        job_settings = redis_client.hgetall(job_key)
        logger.info(f"VEM [{job_id}] Job settings: {job_settings}")

        job_dir = os.path.join('/chunks', f"job_{job_id}")
        os.makedirs(job_dir, exist_ok=True)
        chunk_prefix = os.path.join(job_dir, f"chunk_%03d.ts")

        hostname = get_hostname()
        redis_client.hset(f"job:{job_id}", mapping={
            'worker_hostname': hostname
        })

        # Fetch global segment length from Redis
        segment_duration = int(job_settings.get('segment_duration', DEFAULT_SEGMENT_LENGTH))
        logger.info(f"VEM [{job_id}] Using segment duration: {segment_duration} seconds")

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
        try:
            duration_output = subprocess.check_output([
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', file_path
            ], text=True).strip()
            duration = float(duration_output)
            logger.info(f"VEM [{job_id}] Calculated segment duration: {segment_duration}")
            estimated_chunks = math.ceil(duration / segment_duration)
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Duration estimate failed: {e}")
            estimated_chunks = 0

        redis_client.hset(f"job:{job_id}", 'segment_duration', segment_duration)

        # Log video resolution and duration
        try:
            video_info = subprocess.check_output([
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height', '-of', 'csv=p=0:s=x', file_path
            ], text=True).strip()
            logger.info(f"VEM [{job_id}] Resolution: {video_info}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to retrieve resolution/duration: {e}")

        # Get stream info
        probe = subprocess.check_output([
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=codec_name,width,height,avg_frame_rate',
            '-show_entries', 'format=duration,size', '-of', 'json', file_path
        ], text=True)
        probe_data = json.loads(probe)
        video = probe_data['streams'][0]
        fmt = probe_data['format']

        codec = video.get('codec_name', '')
        width = video.get('width', '')
        height = video.get('height', '')
        fps = eval(video.get('avg_frame_rate', '0'))
        duration = float(fmt.get('duration', 0))
        file_size = int(fmt.get('size', 0))

        frame_count = int(fps * duration)
        redis_client.hset(f"job:{job_id}", 'total_frames', frame_count)

        redis_client.hset(f"job:{job_id}", mapping={
            'status': 'SEGMENTING',
            'segment_progress': 0,
            'encode_progress': 0,
            'combine_progress': 0,
            'encode_start_time': 0,
            'encode_end_time': 0,
            'job_dir': job_dir,
            'started_at': time.time(),
            'source_codec': codec,
            'source_resolution': f"{width}x{height}",
            'source_duration': f"{duration:.2f}",
            'source_fps': f"{fps:.2f}",
            'source_file_size': file_size,
            'segment_duration': segment_duration,
            'total_chunks': estimated_chunks,
            'completed_chunks': 0
        })
        logger.info(f"VEM [{job_id}] Detected codec: {codec}")

        # Use copy method for supported codecs
        bsf = 'hevc_mp4toannexb' if codec == 'hevc' else 'h264_mp4toannexb'
        cmd = [
            'ffmpeg', '-i', file_path, '-map', '0', '-c', 'copy',
            '-bsf:v', bsf, '-f', 'segment', '-segment_time', str(segment_duration),
            '-force_key_frames', f"expr:gte(t,n_forced*{segment_duration})",
            '-reset_timestamps', '1', chunk_prefix
        ]
        logger.info(f"VEM [{job_id}] Segment command: {' '.join(cmd)}")

        segment_re = re.compile(r"Opening '(.*?)chunk_(\d+)\.ts' for writing")
        chunk_paths = []
        chunk_indices = []

        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        while True:
            if is_job_halted(job_id):
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
                chunk_paths.append(chunk_path)
                chunk_indices.append(chunk_index)
                chunk_url = f"http://{hostname}:8000/job_{job_id}/chunk_{chunk_index:03d}.ts"
                redis_client.hset(f"job:{job_id}:chunks", f"chunk_{chunk_index}", chunk_url)
                logger.info(f"VEM [{job_id}] Detected chunk {chunk_index} at {chunk_path}")

                segment_end_time = time.time()
                redis_client.hset(f"job:{job_id}", mapping={
                    'status': f"SEG {chunk_index + 1}/{estimated_chunks}",
                    'segment_progress': min(100, int(((chunk_index + 1) / estimated_chunks) * 100)),
                    'segment_elapsed': round(segment_end_time - segment_start_time, 2)
                })

        process.wait()

        if process.returncode != 0:
            logger.error(f"VEM [{job_id}] FFmpeg failed with code {process.returncode}")
            redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
            return {'status': 'FAILED'}

        # Queue all chunks at once
        for chunk_path, chunk_index in zip(chunk_paths, chunk_indices):
            encode_chunk(job_id, chunk_path, chunk_index)
            logger.info(f"VEM [{job_id}] Queued chunk {chunk_index} for encoding")

        total_chunks = len(chunk_paths)
        redis_client.hset(f"job:{job_id}", mapping={
            'status': 'SEGMENTED',
            'total_chunks': total_chunks,
            'segment_progress': 100
        })

        segment_end_time = time.time()
        redis_client.hset(f"job:{job_id}", 'segment_end_time', segment_end_time)
        redis_client.hset(f"job:{job_id}", 'segment_elapsed', round(segment_end_time - segment_start_time, 2))

        if frame_count > 0:
            segment_fps = frame_count / (segment_end_time - segment_start_time)
            redis_client.hset(f"job:{job_id}", 'segmentation_fps', round(segment_fps, 2))

        monitor_encoding(job_id)
        return {'status': 'SEGMENTED', 'total_chunks': total_chunks}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Segmenting failed: {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        try:
            shutil.rmtree(job_dir)
            logger.info(f"VEM [{job_id}] Removed job directory {job_dir}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to remove job dir {job_dir}: {e}")
        return {'status': 'FAILED'}

@huey.task()
def encode_chunk(job_id, chunk_path, chunk_index):
    """
    Encode a video chunk, fetching it via HTTP and storing the result locally.
    """
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job marked ABORTED before encoding chunk {chunk_index}. Exiting.")
            return {'status': 'ABORTED'}

        # Fetch chunk URL from Redis
        chunk_url = redis_client.hget(f"job:{job_id}:chunks", f"chunk_{chunk_index}")
        if not chunk_url:
            logger.warning(f"VEM [{job_id}] Missing chunk URL for chunk {chunk_index}. Aborting encode.")
            return {'status': 'ABORTED'}

        # Download chunk locally with retry
        job_dir = os.path.join('/chunks', f"job_{job_id}")
        os.makedirs(job_dir, exist_ok=True)
        chunk_path = os.path.join(job_dir, f"chunk_{chunk_index:03d}.ts")
        encoded_path = os.path.join(job_dir, f"encoded_chunk_{chunk_index:03d}.mp4")

        logger.info(f"VEM [{job_id}] Downloading chunk {chunk_index} from {chunk_url}")
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = requests.get(chunk_url, timeout=15)
                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}")
                with open(chunk_path, 'wb') as f:
                    f.write(response.content)
                break
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(f"VEM [{job_id}] Failed to download chunk {chunk_index} after {max_attempts} attempts: {e}")
                    redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
                    return {'status': 'FAILED', 'error': str(e)}
                logger.warning(f"VEM [{job_id}] Download attempt {attempt + 1} failed for chunk {chunk_index}: {e}")
                time.sleep(2)

        cmd = [
            'ffmpeg', '-vaapi_device', '/dev/dri/renderD128',
            '-i', chunk_path, '-vf', 'scale=-1:720,format=nv12,hwupload',
            '-map', '0:v:0', '-map', '0:a:0?',
            '-c:v', 'h264_vaapi', '-rc_mode', 'CQP', '-qp', '27',
            '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
            encoded_path
        ]
        logger.info(f"VEM [{job_id}] Encoding chunk {chunk_index}")

        encode_start_time = float(redis_client.hget(f"job:{job_id}", "encode_start_time") or 0)
        if encode_start_time == 0:
            encode_start_time = time.time()
            redis_client.hset(f"job:{job_id}", "encode_start_time", encode_start_time)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"VEM [{job_id}] Encoding chunk {chunk_index} FAILED")
            logger.error(f"VEM [{job_id}] Encode failed with return code {result.returncode}")
            logger.error(f"VEM [{job_id}] FFmpeg stderr:\n{result.stderr}")
            redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
            return {'status': 'FAILED', 'error': str(result.stderr)}

        # Store encoded chunk URL in Redis
        hostname = get_hostname()
        encoded_url = f"http://{hostname}:8000/job_{job_id}/encoded_chunk_{chunk_index:03d}.mp4"
        redis_client.hset(f"job:{job_id}:encoded_chunks", f"chunk_{chunk_index}", encoded_url)

        current_time = time.time()
        encode_end_time = float(redis_client.hget(f"job:{job_id}", "encode_end_time") or 0)
        if current_time > encode_end_time:
            encode_end_time = current_time

        redis_client.hincrby(f"job:{job_id}", 'completed_chunks', 1)

        total_chunks = int(redis_client.hget(f"job:{job_id}", 'total_chunks') or 0)
        completed = int(redis_client.hget(f"job:{job_id}", 'completed_chunks') or 0)
        redis_client.hset(
            f"job:{job_id}", mapping={
                'encode_progress': int((completed / total_chunks) * 100) if total_chunks > 0 else 0,
                'status': f"ENC {completed}/{total_chunks}",
                'encode_end_time': current_time,
                'encode_elapsed': round(encode_end_time - encode_start_time, 2)
            })

        # Clean up downloaded chunk
        try:
            os.remove(chunk_path)
            logger.info(f"VEM [{job_id}] Removed local chunk {chunk_path}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to remove local chunk {chunk_path}: {e}")

        return {'status': 'COMPLETED', 'chunk_index': chunk_index}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Unexpected encode error: {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        return {'status': 'FAILED', 'error': str(e)}

@huey.task()
def monitor_encoding(job_id):
    """
    Monitor encoding progress and enqueue stitching when complete.
    """
    job_key = f"job:{job_id}"
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        data = redis_client.hgetall(job_key)
        total = int(data.get('total_chunks', '0'))
        completed = int(data.get('completed_chunks', '0'))

        if completed >= total and total > 0:
            start = float(data.get('encode_start_time', '0'))
            end = float(data.get('encode_end_time', '0'))
            if start > 0 and end > 0:
                encode_elapsed = end - start
                redis_client.hset(job_key, 'encode_elapsed_time', encode_elapsed)

                frame_count_val = data.get('total_frames')
                if frame_count_val:
                    frame_count = int(frame_count_val)
                    encode_fps = frame_count / encode_elapsed
                    redis_client.hset(job_key, 'encoding_fps', round(encode_fps, 2))

            logger.info(f"VEM [{job_id}] Encoding complete. Starting combine.")
            stitch_video(job_id)
            return {'status': 'DONE'}

        time.sleep(5)
        monitor_encoding(job_id)
        return {'status': 'WAITING', 'completed': completed, 'total': total}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Monitor failed: {e}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}

def validate_ffmpeg_file(file_path):
    """
    Validate if a file is a valid FFmpeg input using ffprobe.
    """
    cmd = ['ffprobe', '-v', 'error', '-show_streams', '-of', 'json', file_path]
    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        stdout, stderr = process.communicate(timeout=10)
        if process.returncode != 0:
            logger.error(f"VEM ffprobe failed for {file_path}: {stderr}")
            return False
        probe_data = json.loads(stdout)
        return len(probe_data.get('streams', [])) > 0
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        logger.error(f"VEM ffprobe error for {file_path}: {str(e)}")
        return False

@huey.task()
def stitch_video_save(job_id):
    """
    Stitch encoded chunks into a final video (alternative method) and write to NFS.
    """
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM No job found with id: {job_id}")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get('job_dir', '')
        output_path = os.path.join(job_dir, 'output.mp4')
        total_chunks = int(job_data['total_chunks'])
        concat_file = os.path.join(job_dir, "concat_list.txt")

        previous_file = None
        for i in range(total_chunks):
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                return {'status': 'ABORTED'}

            current_file = f"encoded_chunk_{i:03d}.mp4"
            path = os.path.join(job_dir, current_file)
            while not os.path.exists(path):
                logger.warning(f"VEM [{job_id}] Waiting for chunks to appear: {current_file}...")
                time.sleep(1)
                if is_job_halted(job_id):
                    return {'status': 'ABORTED'}

            while not validate_ffmpeg_file(path):
                logger.info(f"VEM [{job_id}] Chunk invalid, waiting: {path}")
                time.sleep(1)
                if is_job_halted(job_id):
                    return {'status': 'ABORTED'}

            if i == 0:
                previous_file = current_file
                combine_start_time = time.time()
                redis_client.hset(job_key, 'status', 'STITCHING')
            else:
                with open(concat_file, 'w') as f:
                    f.write(f"file '{previous_file}'\n")
                    f.write(f"file '{current_file}'\n")
                previous_file = 'output.mp4'
                cmd = [
                    'ffmpeg', '-f', 'concat', '-safe', '0',
                    '-i', concat_file, '-c', 'copy', '-y', output_path
                ]
                logger.info(f"VEM [{job_id}] Stitching files: {previous_file} & {current_file}")
                logger.info(f"VEM [{job_id}] Stitching command: {' '.join(cmd)}")

                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=job_dir
                )
                while process.poll() is None:
                    time.sleep(1)
                    if is_job_halted(job_id):
                        logger.warning(f"VEM [{job_id}] Job deleted. Terminating stitching.")
                        process.terminate()
                        process.wait(timeout=5)
                        return {'status': 'ABORTED'}

                stdout, stderr = process.communicate()
                combine_end_time = time.time()
                redis_client.hset(f"job:{job_id}", 'combine_end_time', combine_end_time)
                redis_client.hset(f"job:{job_id}", 'combine_elapsed', round(combine_end_time - combine_start_time, 2))

                if process.returncode != 0:
                    logger.error(f"VEM [{job_id}] FFmpeg stitch failed with return code {process.returncode}")
                    redis_client.hset(job_key, 'status', 'FAILED')
                    return {'status': 'FAILED'}

        redis_client.hset(job_key, mapping={'status': 'MOVING'})

        input_path = job_data.get('filename')
        logger.info(f"VEM input_path {input_path}")
        base_path, _ = os.path.splitext(input_path)
        logger.info(f"VEM base_path {base_path}")
        final_path = os.path.join('/library', base_path + '.mp4')
        logger.info(f"VEM final_path {final_path}")
        final_dir = os.path.dirname(final_path)
        logger.info(f"VEM final_dir {final_dir}")

        try:
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(output_path, final_path)
            logger.info(f"VEM [{job_id}] Moved stitched file to {final_path}")
            redis_client.hset(job_key, 'output_path', final_path)
        except Exception as e:
            logger.error(f"VEM [{job_id}] Failed to move output to {final_path}: {e}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED', 'error': str(e)}

        if is_job_halted(job_id):
            logger.info(f"VEM [{job_id}] Job aborted, preserving job directory {job_dir}")
            redis_client.hset(job_key, mapping={
                'status': 'ABORTED',
                'ended_at': time.time()
            })
            return {'status': 'ABORTED'}

        redis_client.hset(job_key, 'status', 'CLEANUP')
        worker_hostnames = redis_client.smembers(f"job:{job_id}:workers") or ['thinman01', 'thinman02', 'thinman03', 'thinman04', 'thinman05', 'thinman06', 'thinman07', 'thinman08', 'thinman09', 'thinman10', 'thinman11', 'thinman12', 'thinman13', 'thinman14', 'thinman15']
        for hostname in worker_hostnames:
            cleanup_url = f"http://{hostname}:8000/job_{job_id}/cleanup"
            try:
                response = requests.post(cleanup_url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"VEM [{job_id}] Triggered cleanup on worker {hostname}")
                else:
                    logger.warning(f"VEM [{job_id}] Cleanup failed on worker {hostname}: HTTP {response.status_code}")
            except Exception as e:
                logger.warning(f"VEM [{job_id}] Cleanup failed on worker {hostname}: {e}")

        redis_client.hset(job_key, mapping={
            'status': 'COMPLETED',
            'output_path': final_path,
            'ended_at': time.time(),
            'combine_progress': 100
        })

        return {'status': 'COMPLETED', 'output': final_path}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] FFmpeg error: {e.stderr}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Stitching error: {e}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}

@huey.task()
def stitch_video(job_id):
    """
    Stitch encoded chunks into a final video and write to NFS.
    """
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM No job found with id: {job_id}")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get('job_dir', '')
        local_output_path = f"/chunks/job_{job_id}_output.mp4"
        total_chunks = int(job_data['total_chunks'])
        concat_file = os.path.join(job_dir, "concat_list.txt")

        # Fetch encoded chunks via HTTP
        for i in range(total_chunks):
            encoded_url = redis_client.hget(f"job:{job_id}:encoded_chunks", f"chunk_{i}")
            if not encoded_url:
                logger.error(f"VEM [{job_id}] Missing encoded chunk URL for chunk {i}")
                redis_client.hset(job_key, 'status', 'FAILED')
                return {'status': 'FAILED', 'reason': f'Missing chunk {i}'}

            chunk_path = os.path.join(job_dir, f"encoded_chunk_{i:03d}.mp4")
            logger.info(f"VEM [{job_id}] Downloading encoded chunk {i} from {encoded_url}")
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    response = requests.get(encoded_url, timeout=15)
                    if response.status_code != 200:
                        raise Exception(f"HTTP {response.status_code}")
                    os.makedirs(job_dir, exist_ok=True)
                    with open(chunk_path, 'wb') as f:
                        f.write(response.content)
                    break
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"VEM [{job_id}] Failed to download encoded chunk {i} after {max_attempts} attempts: {e}")
                        redis_client.hset(job_key, 'status', 'FAILED')
                        return {'status': 'FAILED', 'reason': f'Failed to download chunk {i}'}
                    logger.warning(f"VEM [{job_id}] Download attempt {attempt + 1} failed for chunk {i}: {e}")
                    time.sleep(2)

        combine_start_time = time.time()

        # Write concat list
        with open(concat_file, 'w') as f:
            for i in range(total_chunks):
                f.write(f"file 'encoded_chunk_{i:03d}.mp4'\n")

        redis_client.hset(job_key, 'status', 'STITCHING')

        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0',
            '-i', concat_file, '-c', 'copy',
            '-progress', 'pipe:1', local_output_path
        ]
        logger.info(f"VEM [{job_id}] Stitching command: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        current_out_time = 0
        while True:
            if is_job_halted(job_id):
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

                combine_end_time = time.time()
                redis_client.hset(f"job:{job_id}", 'combine_end_time', combine_end_time)
                redis_client.hset(f"job:{job_id}", 'combine_elapsed', round(combine_end_time - combine_start_time, 2))

            elif key == 'progress' and value == 'end':
                break

        process.wait()

        combine_end_time = time.time()
        redis_client.hset(f"job:{job_id}", 'combine_end_time', combine_end_time)
        redis_client.hset(f"job:{job_id}", 'combine_elapsed', round(combine_end_time - combine_start_time, 2))

        if process.returncode != 0:
            logger.error(f"VEM [{job_id}] FFmpeg stitch failed with return code {process.returncode}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED'}

        redis_client.hset(job_key, mapping={'status': 'MOVING'})

        input_path = job_data.get('filename')
        logger.info(f"VEM input_path {input_path}")
        base_path, _ = os.path.splitext(input_path)
        logger.info(f"VEM base_path {base_path}")
        final_path = os.path.join('/library', base_path + '.mp4')
        logger.info(f"VEM final_path {final_path}")
        final_dir = os.path.dirname(final_path)
        logger.info(f"VEM final_dir {final_dir}")

        try:
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(local_output_path, final_path)
            logger.info(f"VEM [{job_id}] Moved stitched file from {local_output_path} to {final_path}")
            redis_client.hset(job_key, 'output_path', final_path)
        except Exception as e:
            logger.error(f"VEM [{job_id}] Failed to move output to {final_path}: {e}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED', 'error': str(e)}

        if is_job_halted(job_id):
            logger.info(f"VEM [{job_id}] Job aborted, preserving job directory {job_dir}")
            redis_client.hset(job_key, mapping={
                'status': 'ABORTED',
                'ended_at': time.time()
            })
            return {'status': 'ABORTED'}

        redis_client.hset(job_key, 'status', 'CLEANUP')
        worker_hostnames = redis_client.smembers(f"job:{job_id}:workers") or ['thinman01', 'thinman02', 'thinman03', 'thinman04', 'thinman05', 'thinman06', 'thinman07', 'thinman08', 'thinman09', 'thinman10', 'thinman11', 'thinman12', 'thinman13', 'thinman14', 'thinman15']
        for hostname in worker_hostnames:
            cleanup_url = f"http://{hostname}:8000/job_{job_id}/cleanup"
            try:
                response = requests.post(cleanup_url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"VEM [{job_id}] Triggered cleanup on worker {hostname}")
                else:
                    logger.warning(f"VEM [{job_id}] Cleanup failed on worker {hostname}: HTTP {response.status_code}")
            except Exception as e:
                logger.warning(f"VEM [{job_id}] Cleanup failed on worker {hostname}: {e}")

        redis_client.hset(job_key, mapping={
            'status': 'COMPLETED',
            'output_path': final_path,
            'ended_at': time.time(),
            'combine_progress': 100
        })

        return {'status': 'COMPLETED', 'output': final_path}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] FFmpeg error: {e.stderr}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Stitching error: {e}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}