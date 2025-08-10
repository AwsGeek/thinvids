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

huey = RedisHuey('tasks', host='192.168.0.120', port=6379, db=0)
redis_client = redis.Redis(host='192.168.0.120', port=6379, db=1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEGMENT_DURATION = 10  # seconds
CHUNKS_PER_GROUP = 5
IDEAL_SEGMENT_COUNT = 301

def is_job_halted(job_id):
    status = redis_client.hget(f"job:{job_id}", "status")
    if not status:
        return True
    return status.decode() in ("FAILED", "ABORTED")

@huey.task()
def segment_part(job_id, file_path, start_time, part_duration, chunk_offset):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting segment_part.")
            return {'status': 'ABORTED'}

        segment_start_time = time.time()
        logger.info(f"VEM [{job_id}] Segmenting part from {start_time}s for {part_duration}s with chunk offset {chunk_offset}")

        job_dir = redis_client.hget(f"job:{job_id}", 'job_dir').decode()
        chunk_prefix = os.path.join(job_dir, "chunk_%03d.ts")
        estimated_chunks = math.ceil(part_duration / SEGMENT_DURATION)

        # Use copy method for supported codecs
        codec = redis_client.hget(f"job:{job_id}", 'source_codec').decode()
        bsf = 'hevc_mp4toannexb' if codec == 'hevc' else 'h264_mp4toannexb'
        cmd = [
            'ffmpeg', '-i', file_path, '-ss', str(start_time), '-t', str(part_duration),
            '-map', '0', '-c', 'copy', '-bsf:v', bsf, '-f', 'segment',
            '-segment_time', str(SEGMENT_DURATION),
            '-force_key_frames', f"expr:gte(t,n_forced*{SEGMENT_DURATION})",
            '-reset_timestamps', '1', '-segment_start_number', str(chunk_offset),
            chunk_prefix
        ]
        logger.info(f"VEM [{job_id}] Segment part command: {' '.join(cmd)}")

        segment_re = re.compile(r"Opening\s+['\"]?(.*/chunk_(\d+)\.ts)['\"]? for writing")
        previous_chunk_path = None
        previous_chunk_index = None
        queued_chunks = set()

        process = subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            bufsize=1, env={**os.environ, 'FFMPEG_LOGLEVEL': 'info'}
        )

        while True:
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job deleted. Terminating segment_part.")
                process.terminate()
                process.wait(timeout=5)
                return {'status': 'ABORTED'}

            line = process.stderr.readline().strip()
            if not line:
                if process.poll() is not None:
                    break
                continue

            logger.debug(f"VEM [{job_id}] FFmpeg stderr: {line}")
            match = segment_re.search(line)
            if match:
                chunk_path = match.group(1)
                global_chunk_index = int(match.group(2))  # FFmpeg uses global index due to -segment_start_number
                chunk_index = global_chunk_index - chunk_offset  # Local index for this part
                logger.info(f"VEM [{job_id}] Detected chunk: {chunk_path} (global index {global_chunk_index})")

                segment_end_time = time.time()
                total_chunks = int(redis_client.hget(f"job:{job_id}", 'total_chunks') or 0)
                redis_client.hset(f"job:{job_id}", mapping={
                    'status': f"SEG {global_chunk_index + 1}/{total_chunks}",
                    'segment_progress': min(100, int(((global_chunk_index + 1) / total_chunks) * 100)),
                    'segment_elapsed': round(segment_end_time - segment_start_time, 2)
                })

                if previous_chunk_index is not None:
                    encode_chunk(job_id, previous_chunk_path, previous_chunk_index + chunk_offset)
                    queued_chunks.add(previous_chunk_index)
                    logger.info(f"VEM [{job_id}] Queued chunk {previous_chunk_index + chunk_offset}")

                previous_chunk_path = chunk_path
                previous_chunk_index = chunk_index

        process.wait()

        if process.returncode != 0:
            stderr = process.communicate()[1] or ""
            logger.error(f"VEM [{job_id}] FFmpeg segment_part failed with code {process.returncode}: {stderr}")
            redis_client.hset(f"job:{job_id}", 'status', 'FAILED')
            return {'status': 'FAILED', 'error': stderr or 'FFmpeg segmentation failed'}

        if previous_chunk_path:
            encode_chunk(job_id, previous_chunk_path, previous_chunk_index + chunk_offset)
            queued_chunks.add(previous_chunk_index)
            logger.info(f"VEM [{job_id}] Queued final chunk {previous_chunk_index + chunk_offset}")

        total_part_chunks = previous_chunk_index + 1 if previous_chunk_index is not None else 0

        # Check for missing chunks
        missing_chunks = set(range(total_part_chunks)) - queued_chunks
        for index in sorted(missing_chunks):
            chunk_path = os.path.join(job_dir, f"chunk_{(index + chunk_offset):03d}.ts")
            logger.warning(f"VEM [{job_id}] Missing encode job for {chunk_path}.")
            if os.path.exists(chunk_path):
                encode_chunk(job_id, chunk_path, index + chunk_offset)
                logger.info(f"VEM [{job_id}] Queued missing chunk {index + chunk_offset}")

        # Update part completion
        redis_client.hincrby(f"job:{job_id}", 'segment_parts_completed', 1)
        parts_completed = int(redis_client.hget(f"job:{job_id}", 'segment_parts_completed') or 0)
        if parts_completed >= 2:
            segment_end_time = time.time()
            redis_client.hset(f"job:{job_id}", mapping={
                'status': 'SEGMENTED',
                'segment_progress': 100,
                'segment_end_time': segment_end_time,
                'segment_elapsed': round(segment_end_time - float(redis_client.hget(f"job:{job_id}", 'segment_start_time') or 0), 2)
            })
            total_frames = int(redis_client.hget(f"job:{job_id}", 'total_frames') or 0)
            if total_frames > 0:
                segment_fps = total_frames / (segment_end_time - float(redis_client.hget(f"job:{job_id}", 'segment_start_time') or 0))
                redis_client.hset(f"job:{job_id}", 'segmentation_fps', round(segment_fps, 2))
            monitor_encoding(job_id,)

        return {'status': 'SEGMENTED_PART', 'part_chunks': total_part_chunks}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] segment_part failed: {e}")
        redis_client.hset(f"job:{job_id}", 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}

@huey.task()
def segment_video(job_id, file_path, filename):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        segment_start_time = time.time()
        logger.info(f"VEM Segmenting {file_path}")

        job_dir = os.path.join('/chunks', f"job_{job_id}")
        os.makedirs(job_dir, exist_ok=True)

        log_path = os.path.join(job_dir, 'log.txt')
        log_handler = logging.FileHandler(log_path)
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(log_handler)

        redis_client.hset(f"job:{job_id}", mapping={
            'status': 'SEGMENTING',
            'segment_progress': 0,
            'encode_progress': 0,
            'combine_progress': 0,
            'encode_start_time': 0,
            'encode_end_time': 0,
            'job_dir': job_dir,
            'log_path': log_path,
            'started_at': time.time(),
            'segment_parts_completed': 0
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

        # Get duration and estimate chunks
        try:
            duration_output = subprocess.check_output([
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', file_path
            ], text=True).strip()
            duration = float(duration_output)
            segment_duration = max(10, int(duration / IDEAL_SEGMENT_COUNT))
            logger.info(f"VEM [{job_id}] Calculated segment duration: {segment_duration}")
            total_chunks = math.ceil(duration / segment_duration)
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Duration estimate failed: {e}")
            total_chunks = 0
            segment_duration = SEGMENT_DURATION

        redis_client.hset(f"job:{job_id}", 'segment_duration', segment_duration)

        # Log video resolution and duration
        try:
            video_info = subprocess.check_output([
                'ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height', '-of', 'csv=p=0:s=x',
                file_path
            ], text=True).strip()
            logger.info(f"VEM [{job_id}] Resolution: {video_info}, Duration: {duration:.2f} seconds")
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
        fps = eval(video.get('avg_frame_rate', '0'))  # safely evaluate "30000/1001"
        duration = float(fmt.get('duration', 0))
        file_size = int(fmt.get('size', 0))

        frame_count = int(fps * duration)
        redis_client.hset(f"job:{job_id}", mapping={
            'total_frames': frame_count,
            'source_codec': codec,
            'source_resolution': f"{width}x{height}",
            'source_duration': f"{duration:.2f}",
            'source_fps': f"{fps:.2f}",
            'source_file_size': file_size,
            'total_chunks': total_chunks,
            'completed_chunks': 0
        })
        logger.info(f"VEM [{job_id}] Detected codec: {codec}")

        # Split video into two parts
        part_duration = duration / 2
        first_part_chunks = math.ceil(part_duration / segment_duration)
        second_part_chunks = total_chunks - first_part_chunks

        logger.info(f"VEM [{job_id}] Splitting into two parts: Part 1 ({part_duration}s, {first_part_chunks} chunks), Part 2 ({part_duration}s, {second_part_chunks} chunks)")

        # Queue first part (chunks 0 to first_part_chunks-1)
        segment_part(job_id, file_path, 0, part_duration, 0)
        # Queue second part (chunks first_part_chunks to total_chunks-1)
        segment_part(job_id, file_path, part_duration, part_duration, first_part_chunks)

        return {'status': 'SEGMENTING_QUEUED', 'total_chunks': total_chunks}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Segmenting failed: {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        return {'status': 'FAILED', 'error': str(e)}


@huey.task(retries=3, retry_delay=5)
def encode_chunk(job_id, chunk_path, chunk_index):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job marked ABORTED before encoding chunk {chunk_index}. Exiting.")
            return

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
            '-qp', '27',
            '-c:a', 'aac',
            '-ac', '2',
            '-b:a', '192k',
            encoded_path
        ]
        logger.info(f"VEM [{job_id}] Encoding chunk {chunk_index}")

        encode_start_time = float(redis_client.hget(f"job:{job_id}", "encode_start_time").decode())
        if encode_start_time == 0:
            encode_start_time = time.time()
            redis_client.hset(f"job:{job_id}", "encode_start_time", encode_start_time)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"VEM [{job_id}] Encoding chunk {chunk_index} FAILED")
            logger.error(f"VEM [{job_id}] Encode failed with return code {result.returncode}")
            logger.error(f"VEM [{job_id}] FFmpeg stderr:\n{result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)

        current_time = time.time()
        encode_end_time = float(redis_client.hget(f"job:{job_id}", "encode_end_time").decode())
        if current_time > encode_end_time:
            encode_end_time = current_time

        redis_client.hincrby(f"job:{job_id}", 'completed_chunks', 1)

        total_chunks = int(redis_client.hget(f"job:{job_id}", 'total_chunks').decode())
        completed = int(redis_client.hget(f"job:{job_id}", 'completed_chunks').decode())
        redis_client.hset(
            f"job:{job_id}", mapping={
                'encode_progress': int((completed / total_chunks) * 100),
                'status': f"ENC {completed}/{total_chunks}",
                'encode_end_time': current_time,
                'encode_elapsed': round(encode_end_time - encode_start_time, 2)
            })

        return {'status': 'COMPLETED', 'chunk_index': chunk_index}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] Encoding chunk {chunk_index} FAILED")
        logger.error(f"VEM [{job_id}] Encode failed: {e}")
        logger.error(f"VEM [{job_id}] FFmpeg stderr:\n{e.stderr}")
        try:
            raise RetryTask()
        except RetryTask:
            redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
            raise
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Unexpected encode error")
        logger.exception(f"VEM [{job_id}] {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        raise

@huey.task()
def monitor_encoding(job_id):
    job_key = f"job:{job_id}"
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        data = redis_client.hgetall(job_key)
        total = int(data.get(b'total_chunks', b'0').decode())
        completed = int(data.get(b'completed_chunks', b'0').decode())

        if completed >= total:
            start = float(data.get(b'encode_start_time', b'0').decode())
            end = float(data.get(b'encode_end_time', b'0').decode())
            if start > 0 and end > 0:
                encode_elapsed = end - start
                redis_client.hset(job_key, 'encode_elapsed_time', encode_elapsed)

                frame_count_val = data.get(b'total_frames')
                if frame_count_val:
                    frame_count = int(frame_count_val.decode())
                    encode_fps = frame_count / encode_elapsed
                    redis_client.hset(job_key, 'encoding_fps', round(encode_fps, 2))

            logger.info(f"VEM [{job_id}] Encoding complete. Starting combine.")
            stitch_video(job_id)
            return {'status': 'DONE'}

        # Schedule the next monitor check in 5 seconds
        monitor_encoding(job_id)
        return {'status': 'WAITING', 'completed': completed, 'total': total}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Monitor failed")
        logger.exception(f"VEM [{job_id}] {e}")
        redis_client.hset(job_key, 'status', 'FAILED')
        return {'status': 'FAILED', 'error': str(e)}


def validate_ffmpeg_file(file_path):
    """Validate if a file is a valid FFmpeg input using ffprobe."""
    cmd = ['ffprobe', '-v', 'error', '-show_streams', '-of', 'json', file_path]
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(timeout=10)
        if process.returncode != 0:
            logger.error(f"VEM ffprobe failed for {file_path}: {stderr}")
            return False
        # Check if the file has at least one stream
        probe_data = json.loads(stdout)
        return len(probe_data.get('streams', [])) > 0
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        logger.error(f"VEM ffprobe error for {file_path}: {str(e)}")
        return False

@huey.task()
def stitch_video_save(job_id):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM No job found with id: {job_id}")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get(b'job_dir', b'').decode()
        output_path = os.path.join(job_dir, 'output.mp4')
        total_chunks = int(job_data[b'total_chunks'].decode())
        concat_file = os.path.join(job_dir, "concat_list.txt")

        previous_file = None
        for i in range(total_chunks):

            waiting = []

            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                return {'status': 'ABORTED'}

            current_file = f"encoded_chunk_{i:03d}.mp4"
            path = os.path.join(job_dir, current_file)
            while not os.path.exists(path):
                logger.warning(f"VEM [{job_id}] Waiting for chunks to appear: {current_file}...")
                time.sleep(1)

                if is_job_halted(job_id):
                    logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                    return {'status': 'ABORTED'}

            # Validate chunk with FFmpeg
            while not validate_ffmpeg_file(path):
                logger.info(f"VEM [{job_id}] Chunk invalid, waiting: {path}")
                time.sleep(1)

                if is_job_halted(job_id):
                    logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                    return {'status': 'ABORTED'}


            if i == 0:
                previous_file = current_file
                combine_start_time = time.time()
                redis_client.hset(job_key, 'status', 'STITCHING')
            else:
                # Write concat list
                with open(concat_file, 'w') as f:
                    f.write(f"file '{previous_file}'\n")
                    f.write(f"file '{current_file}'\n")
                previous_file = 'output.mp4'
                cmd = [
                    'ffmpeg', '-f', 'concat', '-safe', '0', 
                    '-i', concat_file,
                    '-c', 'copy', 
                    '-y', output_path
                ]
                logger.info(f"VEM [{job_id}] Stitching files: {previous_file} & {current_file}")
                logger.info(f"VEM [{job_id}] Stitching command: {' '.join(cmd)}")

                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=job_dir)
                # Wait for completion, checking for job halt
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

        # Final output path: /library/... (same structure as /watch input)
        input_path = job_data.get(b'filename').decode()
        logger.info(f"VEM input_path {input_path}")
        base_path, _ = os.path.splitext(input_path)
        logger.info(f"VEM base_path {base_path}")
        final_path = os.path.join('/library', base_path + '.mp4')
        logger.info(f"VEM final_path {final_path}")
        final_dir = os.path.dirname(final_path)
        logger.info(f"VEM final_dir {final_dir}")

        try:
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(output_path, final_path)  # use shutils because this is a cross device move
            logger.info(f"VEM [{job_id}] Moved stitched file to {final_path}")
            redis_client.hset(job_key, 'output_path', final_path)
        except Exception as e:
            logger.error(f"VEM [{job_id}] Failed to move output to {final_path}: {e}")
            logger.exception(f"VEM [{job_id}] {e}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED', 'error': str(e)}

        # Skip cleanup for ABORTED jobs
        if is_job_halted(job_id):
            logger.info(f"VEM [{job_id}] Job aborted, preserving job directory {job_dir}")
            redis_client.hset(job_key, mapping={
                'status': 'ABORTED',
                'ended_at': time.time()
            })
            return {'status': 'ABORTED'}

        # Cleanup entire project directory
        redis_client.hset(job_key, 'status', 'CLEANUP')
        try:
            shutil.rmtree(job_dir)
            logger.info(f"VEM [{job_id}] Removed job directory {job_dir}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to remove job dir {job_dir}: {e}")

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



@huey.task()
def stitch_video(job_id):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM No job found with id: {job_id}")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get(b'job_dir', b'').decode()
        output_path = os.path.join(job_dir, 'output.mp4')
        total_chunks = int(job_data[b'total_chunks'].decode())
        concat_file = os.path.join(job_dir, "concat_list.txt")

        # Wait up to 10 seconds for all encoded chunks to appear
        retries = 10
        for attempt in range(retries):
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                return {'status': 'ABORTED'}

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

        combine_start_time = time.time()

        # Write concat list
        with open(concat_file, 'w') as f:
            for i in range(total_chunks):
                f.write(f"file 'encoded_chunk_{i:03d}.mp4'\n")

        redis_client.hset(job_key, 'status', 'STITCHING')
        logger.warning(f"VEM [{job_id}] Waiting for {len(missing)} chunks to appear: {missing[:3]}...")

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
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job deleted. Terminating stitching.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stdout.readline()
            if not line:
                break
            line = line.strip()
            logger.info(f"VEM [{job_id}] {line}")
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

        # Final output path: /library/... (same structure as /watch input)
        input_path = job_data.get(b'filename').decode()
        logger.info(f"VEM input_path {input_path}")
        base_path, _ = os.path.splitext(input_path)
        logger.info(f"VEM base_path {base_path}")
        final_path = os.path.join('/library', base_path + '.mp4')
        logger.info(f"VEM final_path {final_path}")
        final_dir = os.path.dirname(final_path)
        logger.info(f"VEM final_dir {final_dir}")

        try:
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(output_path, final_path)  # use shutils because this is a cross device move
            logger.info(f"VEM [{job_id}] Moved stitched file to {final_path}")
            redis_client.hset(job_key, 'output_path', final_path)
        except Exception as e:
            logger.error(f"VEM [{job_id}] Failed to move output to {final_path}: {e}")
            logger.exception(f"VEM [{job_id}] {e}")
            redis_client.hset(job_key, 'status', 'FAILED')
            return {'status': 'FAILED', 'error': str(e)}

        # Skip cleanup for ABORTED jobs
        if is_job_halted(job_id):
            logger.info(f"VEM [{job_id}] Job aborted, preserving job directory {job_dir}")
            redis_client.hset(job_key, mapping={
                'status': 'ABORTED',
                'ended_at': time.time()
            })
            return {'status': 'ABORTED'}

        return {'status': 'ABORTED'}

        # Cleanup entire project directory
        redis_client.hset(job_key, 'status', 'CLEANUP')
        try:
            shutil.rmtree(job_dir)
            logger.info(f"VEM [{job_id}] Removed job directory {job_dir}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to remove job dir {job_dir}: {e}")

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

