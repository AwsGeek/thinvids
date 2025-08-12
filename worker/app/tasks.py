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
redis_client = redis.Redis(host='192.168.0.120', port=6379, db=1, decode_responses=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SEGMENT_LENGTH = 10

STATUS_READY   = 'READY'
STATUS_RUNNING = 'RUNNING'
STATUS_STOPPED = 'STOPPED'
STATUS_FAILED  = 'FAILED'
STATUS_DONE    = 'DONE'

def is_job_halted(job_id):
    status = redis_client.hget(f"job:{job_id}", "status")
    if not status:
        return True
    return status in (STATUS_FAILED, STATUS_STOPPED)

def serialize_enabled(job_key: str) -> bool:
    v = redis_client.hget(job_key, 'serialize_pipeline')
    return str(v).lower() in ('1', 'true', 'yes')

# --- add near the top with the other imports in worker/app/tasks.py ---
import shlex

# --- add alongside other @huey.task() defs ---
@huey.task()
def probe_source(job_id: str, file_path: str):
    job_key = f"job:{job_id}"
    try:
        if not os.path.exists(file_path):
            job = redis_client.hgetall(job_key) or {}
            alt = job.get('filename')
            if alt and os.path.exists(alt):
                file_path = alt

        # One shot: show streams + format as JSON
        probe_cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration,size:stream=index,codec_type,codec_name,width,height,avg_frame_rate,nb_frames,channels,channel_layout,disposition:stream_tags=language,title',
            '-of', 'json', file_path
        ]
        res = subprocess.run(probe_cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise subprocess.CalledProcessError(res.returncode, probe_cmd, res.stdout, res.stderr)

        info = json.loads(res.stdout or '{}')
        fmt = info.get('format', {}) or {}
        streams = info.get('streams', []) or []

        # Format
        try:
            duration_s = float(fmt.get('duration', 0) or 0)
        except Exception:
            duration_s = 0.0
        size_b = int(fmt.get('size', 0) or 0)

        # Split streams for UI
        video_streams = []
        audio_streams = []
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
                v = {
                    **base,
                    'width': s.get('width') or 0,
                    'height': s.get('height') or 0,
                    'fps': fps,
                    'nb_frames': int(s.get('nb_frames', 0) or 0)
                }
                video_streams.append(v)
            elif s.get('codec_type') == 'audio':
                a = {
                    **base,
                    'channels': int(s.get('channels') or 0),
                    'channel_layout': s.get('channel_layout') or ''
                }
                audio_streams.append(a)

        # Choose sensible defaults if not set:
        #  - video: first stream (or one with disposition default)
        #  - audio: default-disposition if present else with most channels else first
        job_now = redis_client.hgetall(job_key) or {}

        # --- VIDEO selection (unchanged behavior, but keep prior if valid) ---
        v_sel = 0
        try:
            prior_v = int(job_now.get('selected_v_stream', 0))
            if 0 <= prior_v < len(video_streams):
                v_sel = prior_v
        except Exception:
            pass
            
        # --- AUDIO selection with English preference ---
        a_sel = None
        # 1) keep prior if valid
        try:
            prior_a = int(job_now.get('selected_a_stream', -1))
            if 0 <= prior_a < len(audio_streams):
                a_sel = prior_a
        except Exception:
            a_sel = None

        # 2) default-disposition
        if a_sel is None:
            for i, a in enumerate(audio_streams):
                if a.get('disposition_default'):
                    a_sel = i
                    break

        # 3) first English stream (eng, en, en-*)
        if a_sel is None:
            for i, a in enumerate(audio_streams):
                lang = (a.get('language') or '').strip().lower()
                if lang in ('eng', 'en', 'en-us', 'en_us', 'en-gb', 'en_gb') or lang.startswith('en'):
                    a_sel = i
                    break

        # 4) fallback to 0 if nothing matched
        if a_sel is None:
            a_sel = 0 if audio_streams else 0

        redis_client.hset(job_key, 'selected_v_stream', 0)
        redis_client.hset(job_key, 'selected_a_stream', a_sel)

        # Primary (first) video stream fields for top table
        primary_v = video_streams[0] if video_streams else {}
        codec = primary_v.get('codec', '')
        reso = f"{primary_v.get('width',0)}x{primary_v.get('height',0)}" if primary_v else ''
        fps = float(primary_v.get('fps', 0) or 0)

        total_frames = int(primary_v.get('nb_frames') or 0)
        if total_frames == 0 and duration_s and fps:
            total_frames = int(duration_s * fps)

        mapping = {
            'source_file_size': size_b,
            'source_duration': f"{duration_s:.2f}" if duration_s else '0',
            'source_codec': codec,
            'source_resolution': reso,
            'source_fps': f"{fps:.2f}" if fps else '0',
            'streams_json': json.dumps({'video': video_streams, 'audio': audio_streams})
        }
        if total_frames:
            mapping['total_frames'] = total_frames

        redis_client.hset(job_key, mapping=mapping)
        logger.info(f"VEM [{job_id}] Probed streams: {mapping['streams_json']}")
        return {'status': 'OK'}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] ffprobe error")
        return {'status': 'ERROR', 'error': str(e)}



@huey.task()
def segment_video(job_id, file_path, filename):
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
            return {'status': 'ABORTED'}

        segment_start_time = time.time()
        logger.info(f"VEM [{job_id}] Segmenting (planner) {file_path}")

        job_key = f"job:{job_id}"
        job_settings = redis_client.hgetall(job_key) or {}

        job_dir = os.path.join('/chunks', f"job_{job_id}")
        os.makedirs(job_dir, exist_ok=True)

        # Settings
        segment_duration = int(job_settings.get('segment_duration', DEFAULT_SEGMENT_LENGTH))
        num_parts = int(job_settings.get('number_parts', 2))
        num_parts = max(1, num_parts)

        # Probe duration
        try:
            duration_output = subprocess.check_output(
                ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                 '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
                text=True
            ).strip()
            duration = float(duration_output)
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Duration probe failed: {e}")
            duration = 0.0

        # Probe video stream
        try:
            probe = subprocess.check_output([
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=codec_name,width,height,avg_frame_rate',
                '-show_entries', 'format=size',
                '-of', 'json',
                file_path
            ], text=True)
            probe_data = json.loads(probe)
            video = probe_data['streams'][0]
            fmt = probe_data['format']
            codec = video.get('codec_name', '')
            width = video.get('width', '')
            height = video.get('height', '')
            fps = eval(video.get('avg_frame_rate', '0')) if video.get('avg_frame_rate') else 0.0
            file_size = int(fmt.get('size', 0))
        except Exception as e:
            logger.warning(f"VEM [{job_id}] ffprobe stream failed: {e}")
            codec, width, height, fps, file_size = '', '', '', 0.0, 0

        # Estimated total chunks (based on duration / segment_duration)
        estimated_chunks = math.ceil(duration / segment_duration) if duration > 0 else 0
        frame_count = int(fps * duration) if fps and duration else 0

        # Persist job metadata (note: total_chunks is an *estimate* until finalize)
        redis_client.hset(job_key, mapping={
            'status': STATUS_RUNNING,
            'segment_progress': 0,
            'encode_progress': 0,
            'combine_progress': 0,
            'encode_start_time': 0,
            'encode_end_time': 0,
            'job_dir': job_dir,
            'started_at': time.time(),
            'source_codec': codec,
            'source_resolution': f"{width}x{height}" if width and height else '',
            'source_duration': f"{duration:.2f}" if duration else '0',
            'source_fps': f"{fps:.2f}" if fps else '0',
            'source_file_size': file_size,
            'segment_duration': segment_duration,
            'total_chunks': estimated_chunks or 0,   # estimate used for progress
            'segmented_chunks': 0,                   # actual chunks produced so far
            'completed_chunks': 0,
            'filename': filename,                    # used by stitch_video
            'num_parts': num_parts
        })
        if frame_count:
            redis_client.hset(job_key, 'total_frames', frame_count)

        # Choose bitstream filter (copy mode)
        bsf = 'hevc_mp4toannexb' if codec == 'hevc' else 'h264_mp4toannexb'
        redis_client.hset(job_key, 'bsf', bsf)

        # Set counter for remaining parts & index tracking
        redis_client.set(f"{job_key}:parts_remaining", num_parts)
        redis_client.hset(job_key, 'max_chunk_index', -1)

        # Plan parts (equal slices)
        part_len = (duration / num_parts) if duration else 0

        for part in range(num_parts):
            start_time = part * part_len
            length = (duration - start_time) if part == num_parts - 1 else part_len
            start_index = int(round(start_time / segment_duration)) if segment_duration > 0 else 0

            logger.info(
                f"VEM [{job_id}] Dispatch part {part+1}/{num_parts}: "
                f"start={start_time:.3f}s len={length:.3f}s start_index={start_index}"
            )

            segment_part(
                job_id=job_id,
                file_path=file_path,
                part=part,
                start_time=start_time,
                length=length,
                start_index=start_index,
            )

        redis_client.hset(job_key, 'segment_planner_started', segment_start_time)
        return {'status': 'SEGMENTING', 'parts': num_parts, 'estimated_chunks': estimated_chunks}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Segment planner failed: {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': 'FAILED'})
        return {'status': 'FAILED'}

@huey.task()
def segment_part(job_id, file_path, part, start_time, length, start_index):
    job_key = f"job:{job_id}"
    try:
        if is_job_halted(job_id):
            logger.warning(f"VEM [{job_id}] Part {part}: job halted before start.")
            return {'status': 'ABORTED'}

        job_data = redis_client.hgetall(job_key)
        if not job_data:
            logger.error(f"VEM [{job_id}] Part {part}: missing job.")
            return {'status': 'FAILED', 'reason': 'Missing job'}

        job_dir = job_data.get('job_dir', os.path.join('/chunks', f"job_{job_id}"))
        # NEW: per-part subdir
        part_dir = os.path.join(job_dir, f"part_{int(part):03d}")
        os.makedirs(part_dir, exist_ok=True)

        segment_duration = int(job_data.get('segment_duration', DEFAULT_SEGMENT_LENGTH))
        bsf = job_data.get('bsf', 'h264_mp4toannexb')

        # NEW: per-part, always start numbering at 0
        out_pattern = os.path.join(part_dir, "chunk_%03d.ts")

        selected_v_stream = job_data.get('selected_v_stream', 0)
        selected_a_stream = job_data.get('selected_a_stream', 0)

        cmd = [
            'ffmpeg',
            '-ss', str(max(0.0, start_time)),
            '-t', str(max(0.0, length)),
            '-i', file_path,
            '-map', f"0:v:{selected_v_stream}",
            '-map', f"0:a:{selected_a_stream}",
            '-c', 'copy',
            '-bsf:v', bsf,
            '-f', 'segment',
            '-segment_time', str(segment_duration),
            '-reset_timestamps', '1',
            '-segment_start_number', '0',
            out_pattern
        ]

        logger.info(f"VEM [{job_id}] Part {part}: { ' '.join(cmd) }")

        segment_re = re.compile(r"Opening '.*?chunk_(\d+)\.ts' for writing")
        previous_chunk_index = None
        previous_chunk_path = None

        part_start_t = time.time()
        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        def update_progress(increment=False):
            """Optionally increment segmented_chunks, then recompute segment_progress."""
            if increment:
                new_created = redis_client.hincrby(job_key, 'segmented_chunks', 1)
            else:
                new_created = int(redis_client.hget(job_key, 'segmented_chunks') or 0)

            est_total = int(redis_client.hget(job_key, 'total_chunks') or 0) or 1
            progress = min(99, int((new_created / est_total) * 100))
            redis_client.hset(job_key, mapping={
                'segment_progress': progress,
                'segment_elapsed': round(time.time() - part_start_t, 2)
            })
            return new_created, est_total, progress

        while True:
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Part {part}: job aborted during segmenting.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stderr.readline()
            if not line:
                break

            m = segment_re.search(line)
            if m:
                curr_index = int(m.group(1))
                curr_path = os.path.join(part_dir, f"chunk_{curr_index:03d}.ts")

                # Previous chunk just closed -> enqueue encode, count it as created
                if previous_chunk_index is not None and previous_chunk_path:
                    if not serialize_enabled(job_key):
                        encode_chunk(job_id, previous_chunk_path, previous_chunk_index)
                    created, est_total, prog = update_progress(increment=True)
                    logger.info(f"VEM [{job_id}] Segmented chunk {created}/{est_total}, {prog}%")

                previous_chunk_index = curr_index
                previous_chunk_path = curr_path

        process.wait()
        if process.returncode != 0:
            stderr = process.stderr.read()
            logger.error(f"VEM [{job_id}] Part {part}: ffmpeg failed ({process.returncode}): {stderr}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED'}

        # Final chunk of this part (if any)
        if previous_chunk_path is not None:
            if not serialize_enabled(job_key):
                encode_chunk(job_id, previous_chunk_path, previous_chunk_index)
            created, est_total, prog = update_progress(increment=True)
            logger.info(
                f"VEM [{job_id}] Segmented last chunk P{part}:{previous_chunk_index} "
                f"({created}/{est_total}, {prog}%)"
            )

        # Record this part's max index for later exact totals
        try:
            redis_client.hset(job_key, f'part_{int(part)}_max_idx', previous_chunk_index if previous_chunk_index is not None else -1)
        except Exception:
            pass

        # Part done — decrement remaining and maybe finalize
        remaining = redis_client.decr(f"{job_key}:parts_remaining")
        logger.info(f"VEM [{job_id}] Part {part} complete. parts_remaining={remaining}")

        if remaining == 0:
            # compute exact_total across parts (existing code) ...
            # mark segment_progress etc (existing) ...

            if serialize_enabled(job_key):
                # Enumerate ALL .ts chunks and enqueue *now*
                all_ts = []
                num_parts = int(redis_client.hget(job_key, 'num_parts') or redis_client.hget(job_key, 'number_parts') or 2)
                for p in range(num_parts):
                    part_dir = os.path.join(job_dir, f"part_{int(p):03d}")
                    files = sorted(glob.glob(os.path.join(part_dir, "chunk_*.ts")))
                    for fpath in files:
                        m = re.search(r"chunk_(\d+)\.ts$", os.path.basename(fpath))
                        if m:
                            all_ts.append((p, int(m.group(1)), fpath))
                # Order encodes by part then index to be deterministic
                all_ts.sort(key=lambda t: (t[0], t[1]))
                enc_total = len(all_ts)
                redis_client.set(f"{job_key}:enc_pending", enc_total)
                logger.info(f"VEM [{job_id}] Serialized mode: enqueue {enc_total} encodes after segmentation completes.")
                for _, idx, path in all_ts:
                    encode_chunk(job_id, path, idx)
                # DO NOT stitch yet; encode_chunk will trigger stitch when pending hits 0
            else:
                # Non-serialized (existing behavior): start stitch monitor now
                stitch_video(job_id)

        return {'status': 'COMPLETED', 'part': part}

    except Exception as e:
        logger.exception(f"VEM [{job_id}] Part {part}: segment error")
        redis_client.hset(job_key, 'status', STATUS_FAILED)
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

        job_key = f"job:{job_id}"
        job_data = redis_client.hgetall(job_key) or {}

        encoded_path = chunk_path.replace('chunk_', 'encoded_chunk_').replace('.ts', '.mp4')

        cmd = [
            'ffmpeg', '-vaapi_device', '/dev/dri/renderD128',
            '-i', chunk_path,
            '-vf', 'scale=-1:720,format=nv12,hwupload',
            '-map', '0:v:0',
            '-map', '0:a:0',
            '-c:v', 'h264_vaapi',
            '-rc_mode', 'CQP',
            '-qp', '27',
            '-c:a', 'aac',
            '-ac', '2',
            '-b:a', '192k',
            encoded_path
        ]
        logger.info(f"VEM [{job_id}] Encoding chunk {chunk_index}")

        encode_start_time = float(redis_client.hget(f"job:{job_id}", "encode_start_time"))
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
        encode_end_time = float(redis_client.hget(f"job:{job_id}", "encode_end_time"))
        if current_time > encode_end_time:
            encode_end_time = current_time

        redis_client.hincrby(f"job:{job_id}", 'completed_chunks', 1)

        total_chunks = int(redis_client.hget(f"job:{job_id}", 'total_chunks'))
        completed = int(redis_client.hget(f"job:{job_id}", 'completed_chunks'))
        redis_client.hset(
            f"job:{job_id}", mapping={
                'encode_progress': int((completed / total_chunks) * 100),
                'encode_end_time': current_time,
                'encode_elapsed': round(encode_end_time - encode_start_time, 2)
            })

        # existing progress updates above...
        if serialize_enabled(f"job:{job_id}"):
            pending = redis_client.decr(f"job:{job_id}:enc_pending")
            logger.info(f"VEM [{job_id}] Encoded chunk {chunk_index}, pending encodes left = {pending}")
            if pending == 0:
                logger.info(f"VEM [{job_id}] All encodes complete (serialized). Launch stitching.")
                stitch_video(job_id)
                
        return {'status': 'COMPLETED', 'chunk_index': chunk_index}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] Encoding chunk {chunk_index} FAILED")
        logger.error(f"VEM [{job_id}] Encode failed: {e}")
        logger.error(f"VEM [{job_id}] FFmpeg stderr:\n{e.stderr}")
        try:
            raise RetryTask()
        except RetryTask:
            redis_client.hset(f"job:{job_id}", mapping={'status': STATUS_FAILED})
            raise
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Unexpected encode error")
        logger.exception(f"VEM [{job_id}] {e}")
        redis_client.hset(f"job:{job_id}", mapping={'status': STATUS_FAILED})
        raise

import select

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

        job_dir = job_data.get('job_dir', '')
        local_output_path = f"/tmp/job_{job_id}_output.mp4"
        concat_file = os.path.join(job_dir, "concat_list.txt")
        num_parts = int(job_data.get('num_parts') or job_data.get('number_parts') or 2)

        # Wait for all encoded chunks, checking per-part continuity rather than global indices.
        retries = 20
        wait_sec = 1.0
        for attempt in range(retries):
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job no longer exists. Aborting task.")
                return {'status': 'ABORTED'}

            missing_report = {}
            all_entries = []  # (part, idx, path)

            for p in range(num_parts):
                part_dir = os.path.join(job_dir, f"part_{int(p):03d}")
                if not os.path.isdir(part_dir):
                    missing_report[f"part_{p}"] = "no part directory yet"
                    continue

                files = sorted(glob.glob(os.path.join(part_dir, "encoded_chunk_*.mp4")))
                # Parse indices present
                indexed = []
                for fpath in files:
                    m = re.search(r"encoded_chunk_(\d+)\.mp4$", os.path.basename(fpath))
                    if m:
                        indexed.append((int(m.group(1)), fpath))

                if not indexed:
                    missing_report[f"part_{p}"] = "no encoded chunks yet"
                    continue

                indexed.sort(key=lambda t: t[0])
                # Tolerate non-zero starts: only report gaps between seen files
                base = indexed[0][0]
                expected = base
                holes = []
                for idx, _ in indexed:
                    while expected < idx:
                        holes.append(expected)
                        expected += 1
                    expected = idx + 1

                if holes:
                    missing_report[f"part_{p}"] = f"gaps after {base}: {holes[:50]}{' ...' if len(holes)>50 else ''}"

                # Stash for concat (ordered later by part then idx)
                for idx, fpath in indexed:
                    all_entries.append((p, idx, fpath))

            if not missing_report:
                # Good to go
                break

            if attempt == 0 or attempt % 5 == 0:
                logger.error(f"VEM [{job_id}] Missing/irregular chunks: {missing_report}")
            time.sleep(wait_sec)
        else:
            logger.error(f"VEM [{job_id}] Still missing/irregular chunks after waiting.")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'reason': f'Missing/irregular chunks: {missing_report}'}

        # Exact total = count of files we will concat
        all_entries.sort(key=lambda t: (t[0], t[1]))
        total_chunks = len(all_entries)
        redis_client.hset(job_key, 'total_chunks', total_chunks)

        combine_start_time = time.time()

        # Write concat list in deterministic order
        with open(concat_file, 'w') as f:
            for _, _, path in all_entries:
                f.write(f"file '{path}'\n")

        # Get total duration of input video (for progress)
        total_duration = float(job_data.get('source_duration'))
        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0',
            '-i', concat_file,
            '-c', 'copy',
            '-progress', 'pipe:1',
            local_output_path
        ]
        logger.info(f"VEM [{job_id}] Stitching command: {' '.join(cmd)}")

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        current_out_time = 0
        last_out_time = 0
        last_update_time = time.time()
        timeout_seconds = 60
        near_end_threshold = 0.95

        import select  # ensure available even if moved
        while True:
            if is_job_halted(job_id):
                logger.warning(f"VEM [{job_id}] Job deleted. Terminating stitching.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            if time.time() - last_update_time > timeout_seconds and current_out_time == last_out_time:
                if current_out_time > total_duration * near_end_threshold and os.path.exists(local_output_path):
                    try:
                        probe_cmd = ['ffprobe', '-v', 'error', '-show_format', '-of', 'json', local_output_path]
                        probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
                        probe_data = json.loads(probe_result.stdout)
                        output_duration = float(probe_data.get('format', {}).get('duration', 0))
                        if output_duration >= total_duration * 0.95:
                            logger.info(f"VEM [{job_id}] Output file valid with duration {output_duration}s, treating as success")
                            break
                    except Exception as e:
                        logger.error(f"VEM [{job_id}] Output validation failed: {e}")
                logger.error(f"VEM [{job_id}] FFmpeg progress stalled at {current_out_time}s for {timeout_seconds} seconds")
                process.terminate()
                process.wait()
                stderr = process.stderr.read()
                logger.error(f"VEM [{job_id}] FFmpeg stderr: {stderr}")
                redis_client.hset(job_key, 'status', STATUS_FAILED)
                return {'status': 'FAILED', 'reason': f'FFmpeg stalled: {stderr}'}

            readable, _, _ = select.select([process.stdout, process.stderr], [], [], 1.0)
            for stream in readable:
                line = stream.readline().strip()
                if not line:
                    continue
                if stream == process.stdout:
                    if '=' not in line:
                        continue
                    key, value = line.split('=', 1)
                    if key == 'out_time_ms':
                        current_out_time = int(value) / 1_000_000
                        if current_out_time != last_out_time:
                            last_update_time = time.time()
                            last_out_time = current_out_time
                            denom = total_duration if total_duration else (total_chunks * float(job_data.get('segment_duration') or DEFAULT_SEGMENT_LENGTH))
                            progress = int((current_out_time / denom) * 100)
                            redis_client.hset(job_key, 'combine_progress', min(progress, 100))
                            redis_client.hset(job_key, 'combine_elapsed', round(time.time() - combine_start_time, 2))
                    elif key == 'progress' and value == 'end':
                        break
                else:
                    logger.warning(f"VEM [{job_id}] stderr: {line}")

            if process.poll() is not None:
                break

        process.wait()

        if process.returncode != 0:
            stderr = process.stderr.read()
            logger.error(f"VEM [{job_id}] FFmpeg stitch failed with return code {process.returncode}: {stderr}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'error': stderr}

        # Move output to final path
        input_path = job_data.get('filename')
        base_path, _ = os.path.splitext(input_path)
        final_path = os.path.join('/library', base_path.lstrip('/') + '.mp4')
        final_dir = os.path.dirname(final_path)

        try:
            os.makedirs(final_dir, exist_ok=True)
            shutil.move(local_output_path, final_path)
            logger.info(f"VEM [{job_id}] Moved stitched file from {local_output_path} to {final_path}")
            redis_client.hset(job_key, 'output_path', final_path)
        except Exception as e:
            logger.error(f"VEM [{job_id}] Failed to move output to {final_path}: {e}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'error': str(e)}

        # Cleanup
        try:
            shutil.rmtree(job_dir)
            logger.info(f"VEM [{job_id}] Removed job directory {job_dir}")
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to remove job dir {job_dir}: {e}")

        # Probe the final output for Dst metadata
        try:
            # file size
            dst_size_b = os.path.getsize(final_path)

            # ffprobe
            probe_cmd = [
                'ffprobe', '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=codec_name,width,height,avg_frame_rate',
                '-show_entries', 'format=duration',
                '-of', 'json', final_path
            ]
            pr = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
            info = json.loads(pr.stdout or '{}')
            fmt = info.get('format', {}) or {}
            streams = info.get('streams', []) or []
            v0 = streams[0] if streams else {}

            # duration
            try:
                dst_dur = float(fmt.get('duration', 0) or 0)
            except Exception:
                dst_dur = 0.0

            # codec / resolution / fps
            dst_codec = v0.get('codec_name') or ''
            w = v0.get('width') or 0
            h = v0.get('height') or 0
            afr = v0.get('avg_frame_rate') or '0'
            try:
                dst_fps = (float(afr.split('/')[0]) / float(afr.split('/')[1])) if '/' in afr else float(afr)
            except Exception:
                dst_fps = 0.0

            # persist Dst fields
            redis_client.hset(job_key, mapping={
                'dest_file_size': dst_size_b,
                'dest_duration': f"{dst_dur:.2f}" if dst_dur else '0',
                'dest_codec': dst_codec,
                'dest_resolution': f"{w}x{h}" if (w and h) else '',
                'dest_fps': f"{dst_fps:.2f}" if dst_fps else '0'
            })
        except Exception as e:
            logger.warning(f"VEM [{job_id}] Failed to probe/stash dst metadata: {e}")


        redis_client.hset(job_key, mapping={
            'status': STATUS_DONE,
            'output_path': final_path,
            'ended_at': time.time(),
            'combine_progress': 100
        })
        return {'status': 'COMPLETED', 'output': final_path}

    except subprocess.CalledProcessError as e:
        logger.error(f"VEM [{job_id}] FFmpeg error: {e.stderr}")
        redis_client.hset(job_key, 'status', STATIS_FAILED)
        return {'status': 'FAILED', 'error': str(e.stderr)}
    except Exception as e:
        logger.exception(f"VEM [{job_id}] Stitching error: {e}")
        redis_client.hset(job_key, 'status', STATIS_FAILED)
        return {'status': 'FAILED', 'error': str(e)}
