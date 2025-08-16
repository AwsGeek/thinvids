# worker/app/tasks.py
# Master-orchestrated pipeline with tmpfs + built-in HTTP server.
# - transcode_video(job_id, file_path): runs on the node that picks up this task ("master")
# - encode_part(job_id, idx, master_host, ...): runs on any Thinman node
#
# Also includes probe_source() and a legacy shim segment_video()->transcode_video().

from huey import RedisHuey
from huey.exceptions import RetryTask

import os
import sys
import time
import math
import json
import glob
import shutil
import socket
import logging
import subprocess
import threading
import re
from typing import List, Tuple, Optional

import redis

# ----------------- Configuration -----------------

ENV = os.environ.get

REDIS_HOST = ENV("REDIS_HOST", "192.168.0.120")
REDIS_PORT = int(ENV("REDIS_PORT", "6379"))
REDIS_DB_TASKS = int(ENV("REDIS_DB_TASKS", "0"))  # Huey broker
REDIS_DB_DATA  = int(ENV("REDIS_DB_DATA",  "1"))  # App/job state

# Huey broker + Redis client
huey = RedisHuey('tasks', host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_TASKS)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_DATA, decode_responses=True)

# Filesystem layout (ensure /tmpfs is mounted with enough RAM)
PROJECT_ROOT = ENV("PROJECT_ROOT", "/projects")
WATCH_ROOT   = ENV("WATCH_ROOT",   "/watch")
LIBRARY_ROOT = ENV("LIBRARY_ROOT", "/library")

# Metrics TTL heuristic to consider a node "active"
METRICS_TTL_SEC   = int(ENV("TTL_SEC", "15"))
METRICS_GRACE_SEC = int(ENV("TTL_GRACE_SEC", "5"))

# HTTP server (master node)
HTTP_BIND_HOST = "0.0.0.0"
HTTP_PORT      = int(ENV("MASTER_HTTP_PORT", "8000"))

# Encoding parameters (centralized; identical across workers)
VAAPI_DEVICE  = ENV("VAAPI_DEVICE", "/dev/dri/renderD128")
SCALE_FILTER  = ENV("VEM_SCALE_FILTER", "scale=-1:720,format=nv12,hwupload")
VAAPI_RC_MODE = ENV("VEM_RC_MODE", "CQP")
VAAPI_QP      = ENV("VEM_QP", "27")
AUDIO_ARGS    = ENV("VEM_AUDIO_ARGS", "-c:a aac -ac 2 -b:a 192k")  # space-separated

# Status constants
STATUS_READY   = 'READY'
STATUS_RUNNING = 'RUNNING'
STATUS_STOPPED = 'STOPPED'
STATUS_FAILED  = 'FAILED'
STATUS_DONE    = 'DONE'

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tasks")

# --------------- Helpers ----------------

def _job_key(job_id: str) -> str:
    return f"job:{job_id}"

def _now() -> float:
    return time.time()

def _ensure_dirs(*paths: str):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def _active_nodes() -> List[str]:
    """
    List node hostnames currently active, based on metrics heartbeat keys.
    """
    cutoff = int(_now()) - (METRICS_TTL_SEC + METRICS_GRACE_SEC)
    nodes = []
    for key in redis_client.scan_iter("metrics:node:*"):
        try:
            d = redis_client.hgetall(key) or {}
            ts = int(float(d.get("ts", 0)))
            h = d.get("hostname") or key.split(":")[-1]
            if ts >= cutoff:
                nodes.append(h)
        except Exception:
            continue
    nodes.sort()
    return nodes

def _bsf_for_codec(codec: str) -> str:
    return 'hevc_mp4toannexb' if codec == 'hevc' else 'h264_mp4toannexb'

def _ffprobe_duration(input_path: str) -> float:
    try:
        out = subprocess.check_output(
            ['ffprobe','-v','error','-show_entries','format=duration',
             '-of','default=noprint_wrappers=1:nokey=1', input_path],
            text=True
        ).strip()
        return float(out)
    except Exception:
        return 0.0

def _ffprobe_stream0(input_path: str):
    """
    Probe v:0 + container size/bitrate.
    """
    try:
        probe = subprocess.check_output([
            'ffprobe','-v','error',
            '-select_streams','v:0',
            '-show_entries','stream=codec_name,width,height,avg_frame_rate,nb_frames',
            '-show_entries','format=size,bit_rate',
            '-of','json', input_path
        ], text=True)
        data = json.loads(probe)
        v = (data.get('streams') or [{}])[0]
        fmt = data.get('format', {}) or {}
        afr = v.get('avg_frame_rate') or '0'
        fps = 0.0
        if '/' in afr:
            try:
                a,b = afr.split('/',1)
                fps = float(a)/float(b) if float(b) != 0 else 0.0
            except Exception:
                fps = 0.0
        else:
            try:
                fps = float(afr)
            except Exception:
                fps = 0.0
        return {
            "codec":  v.get('codec_name') or '',
            "width":  int(v.get('width') or 0),
            "height": int(v.get('height') or 0),
            "fps":    fps,
            "nb_frames": int(v.get('nb_frames') or 0),
            "size":   int(fmt.get('size') or 0),
            "bit_rate": int(fmt.get('bit_rate') or 0),
        }
    except Exception:
        return {"codec":"","width":0,"height":0,"fps":0.0,"nb_frames":0,"size":0,"bit_rate":0}

def _pick_stream_indices(job_key: str) -> Tuple[int, int]:
    j = redis_client.hgetall(job_key) or {}
    try: v = int(j.get("selected_v_stream", 0))
    except Exception: v = 0
    try: a = int(j.get("selected_a_stream", 0))
    except Exception: a = 0
    return max(0, v), max(0, a)

def _plan_parts(duration: float, num_parts: int) -> List[Tuple[float, float]]:
    if num_parts <= 1 or duration <= 0:
        return [(0.0, duration if duration > 0 else 0.0)]
    slice_len = duration / num_parts
    parts = []
    for i in range(num_parts):
        start = i * slice_len
        length = (duration - start) if i == num_parts - 1 else slice_len
        parts.append((max(0.0, start), max(0.0, length)))
    return parts

def _part_paths(job_id: str, idx: int) -> Tuple[str, str]:
    base_dir  = os.path.join(PROJECT_ROOT, job_id)
    parts_dir = os.path.join(base_dir, "parts")
    enc_dir   = os.path.join(base_dir, "encoded")
    _ensure_dirs(base_dir, parts_dir, enc_dir)
    return (os.path.join(parts_dir, f"part_{idx:03d}.ts"),
            os.path.join(enc_dir,   f"enc_{idx:03d}.mp4"))

def _final_output_path(src_filename: str) -> str:
    base, _ = os.path.splitext(src_filename)
    return os.path.join(LIBRARY_ROOT, base.lstrip('/') + '.mp4')

def _is_job_halted(job_id: str) -> bool:
    s = redis_client.hget(_job_key(job_id), "status") or ""
    return s in (STATUS_FAILED, STATUS_STOPPED)

# --------------- Built-in HTTP server (master node) ----------------

_HTTP_SERVER = None
_HTTP_THREAD = None
_HTTP_STARTED = False
_HTTP_LOCK = threading.Lock()

def _start_master_http_once():
    """
    Start a tiny HTTP server (GET parts, PUT results) on this worker node.
    Idempotent across multiple calls.
    """
    global _HTTP_SERVER, _HTTP_THREAD, _HTTP_STARTED
    with _HTTP_LOCK:
        if _HTTP_STARTED:
            return

        from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
        from urllib.parse import urlparse

        class PartHandler(BaseHTTPRequestHandler):
            server_version = "ThinmanParts/1.0"

            def log_message(self, fmt, *args):
                logger.info("HTTP %s - " + fmt, self.address_string(), *args)

            def _safe_job_idx(self, path_parts):
                # Expect: ["", "job", "<job_id>", "part|result", "<idx>"]
                if len(path_parts) != 5 or path_parts[1] != "job":
                    return None, None, None
                job_id = path_parts[2]
                kind   = path_parts[3]   # "part" or "result"
                try:
                    idx = int(path_parts[4])
                    if idx <= 0 or idx > 9999:
                        return None, None, None
                except Exception:
                    return None, None, None
                return job_id, kind, idx

            def do_GET(self):
                try:
                    u = urlparse(self.path)
                    parts = u.path.split('/')
                    job_id, kind, idx = self._safe_job_idx(parts)
                    if job_id is None or kind != "part":
                        self.send_error(404, "Not found")
                        return
                    part_path, _ = _part_paths(job_id, idx)
                    if not os.path.isfile(part_path):
                        self.send_error(404, "Part not found")
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "video/MP2T")
                    self.send_header("Content-Length", str(os.path.getsize(part_path)))
                    self.end_headers()
                    with open(part_path, 'rb') as f:
                        shutil.copyfileobj(f, self.wfile)
                except BrokenPipeError:
                    pass
                except Exception as e:
                    logger.exception("HTTP GET error")
                    try:
                        self.send_error(500, f"GET error: {e}")
                    except Exception:
                        pass

            def do_PUT(self):
                try:
                    u = urlparse(self.path)
                    parts = u.path.split('/')
                    job_id, kind, idx = self._safe_job_idx(parts)
                    if job_id is None or kind != "result":
                        self.send_error(404, "Not found")
                        return

                    _, enc_path = _part_paths(job_id, idx)
                    tmp_path = enc_path + ".uploading"

                    # Require Content-Length (no chunked decoding here)
                    length = self.headers.get('Content-Length')
                    if length is None:
                        self.send_error(411, "Content-Length required")
                        return
                    try:
                        to_read = int(length)
                        if to_read < 0:
                            raise ValueError()
                    except Exception:
                        self.send_error(400, "Invalid Content-Length")
                        return

                    _ensure_dirs(os.path.dirname(enc_path))
                    with open(tmp_path, 'wb') as f:
                        remaining = to_read
                        while remaining > 0:
                            chunk = self.rfile.read(min(1024*1024, remaining))
                            if not chunk:
                                break
                            f.write(chunk)
                            remaining -= len(chunk)

                    if remaining != 0:
                        # Incomplete upload
                        try: os.remove(tmp_path)
                        except Exception: pass
                        self.send_error(400, "Incomplete upload")
                        return

                    os.replace(tmp_path, enc_path)   # atomic move
                    # Mark progress (best-effort)
                    try:
                        job_key = _job_key(job_id)
                        done = int(redis_client.hincrby(job_key, 'parts_done', 1))
                        total = int(redis_client.hget(job_key, 'parts_total') or 0)
                        if total:
                            redis_client.hset(job_key, 'encode_progress', int((done/total)*100))
                    except Exception:
                        pass

                    self.send_response(200)
                    self.end_headers()
                except Exception as e:
                    logger.exception("HTTP PUT error")
                    try:
                        self.send_error(500, f"PUT error: {e}")
                    except Exception:
                        pass

        try:
            _HTTP_SERVER = ThreadingHTTPServer((HTTP_BIND_HOST, HTTP_PORT), PartHandler)
        except OSError as e:
            logger.error(f"Failed to bind HTTP server on {HTTP_BIND_HOST}:{HTTP_PORT}: {e}")
            raise

        def _serve():
            logger.info(f"Master HTTP server listening on {HTTP_BIND_HOST}:{HTTP_PORT}")
            try:
                _HTTP_SERVER.serve_forever(poll_interval=0.5)
            except Exception:
                logger.exception("HTTP server terminated")

        _HTTP_THREAD = threading.Thread(target=_serve, name="thinman-http", daemon=True)
        _HTTP_THREAD.start()
        _HTTP_STARTED = True

# --------------- Probe (kept) ----------------

@huey.task()
def probe_source(job_id: str, file_path: str):
    """
    One-shot ffprobe; stores source metadata + stream lists.
    """
    job_key = _job_key(job_id)
    try:
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

        # select indices
        v_sel, a_sel = _pick_stream_indices(job_key)
        if not (0 <= v_sel < len(video_streams)): v_sel = 0
        if not (0 <= a_sel < len(audio_streams)): a_sel = 0

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
        redis_client.hset(job_key, mapping=mapping)
        logger.info(f"[{job_id}] Probe complete")
        return {'status':'OK'}
    except Exception as e:
        logger.exception(f"[{job_id}] Probe error")
        return {'status':'ERROR','error':str(e)}

# --------------- New pipeline ----------------

@huey.task()
def transcode_video(job_id: str, file_path: str):
    """
    Master node orchestration (runs where this task is executed):
    1) Probe source + set metadata
    2) Determine N active nodes; P = max(1, N-1)
    3) Split source into P TS parts in /tmpfs/<job_id>/parts
    4) Start local HTTP server (GET parts, PUT results) on port 8000
    5) Dispatch encode_part tasks pointing to this master
    6) Wait for all encoded parts to be PUT back to /tmpfs/<job_id>/encoded
    7) Stitch (concat copy) and move final to /library
    """
    job_key = _job_key(job_id)

    try:
        # Make sure HTTP server is up on this node
        _start_master_http_once()

        # Resolve input path
        src_path = file_path
        if not os.path.exists(src_path):
            # allow filename from job record
            job = redis_client.hgetall(job_key) or {}
            filename = job.get('filename') or ''
            alt = os.path.join(WATCH_ROOT, filename.lstrip('/'))
            if os.path.exists(alt):
                src_path = alt

        if not os.path.exists(src_path):
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status':'FAILED','reason':f'input not found: {src_path}'}

        # Mark as running
        now = _now()
        redis_client.hset(job_key, mapping={'status': STATUS_RUNNING, 'started_at': now})

        # Probe essentials (also fills metadata fields)
        meta = _ffprobe_stream0(src_path)
        duration = _ffprobe_duration(src_path)
        codec = meta.get("codec") or ""
        width = meta.get("width") or 0
        height = meta.get("height") or 0
        fps = meta.get("fps") or 0.0
        size_b = meta.get("size") or 0
        fmt_bps = meta.get("bit_rate") or 0
        kbps_calc = (fmt_bps/1000.0) if fmt_bps > 0 else (
            ((size_b*8)/duration/1000.0) if (size_b and duration) else 0.0
        )
        redis_client.hset(job_key, mapping={
            'source_codec': codec,
            'source_resolution': f"{width}x{height}" if (width and height) else '',
            'source_duration': f"{duration:.2f}" if duration else '0',
            'source_fps': f"{fps:.2f}" if fps else '0',
            'source_file_size': size_b,
            'source_bitrate_kbps': f"{kbps_calc:.0f}" if kbps_calc>0 else '0'
        })

        # Decide parts
        nodes = _active_nodes()
        # Compute master URL other nodes can reach (host network)
        master_host_name = ENV("HOSTNAME") or socket.gethostname()
        master_url = f"http://{master_host_name}:{HTTP_PORT}"

        P1 = int(size_b / 107374182)
        P2 = 4 * max(1, len(nodes))  # reserve one slot for this master
        P = max(P1, P2)              # sanity cap
        seg_t0 = _now()

        redis_client.hset(job_key, mapping={'parts_total': P, 'parts_done': 0, 'master_host': master_url})

        v_sel, a_sel = _pick_stream_indices(job_key)
        redis_client.hset(job_key, mapping={'selected_v_stream': v_sel, 'selected_a_stream': a_sel})

        # Plan time slices and split into TS parts in tmpfs
        parts = _plan_parts(duration, P)
        bsf = _bsf_for_codec(codec)

        for idx, (start, length) in enumerate(parts, start=1):

            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] Halted during encoding wait")
                return {'status':'ABORTED'}

            part_path, _ = _part_paths(job_id, idx)
            _ensure_dirs(os.path.dirname(part_path))
            cmd = [
                'ffmpeg', '-hide_banner',
                '-ss', f"{start:.6f}",
                '-t', f"{length:.6f}",
                '-i', src_path,
                '-map', f"0:v:{v_sel}",
                '-map', f"0:a:{a_sel}?",
                '-c', 'copy',
                '-bsf:v', bsf,
                '-f', 'mpegts',
                part_path
            ]
            logger.info(f"[{job_id}] Split part {idx}/{P}: {' '.join(cmd)}")
            pr = subprocess.run(cmd, capture_output=True, text=True)
            if pr.returncode != 0:
                logger.error(f"[{job_id}] Split part {idx} failed: {pr.stderr[-600:]}")
                redis_client.hset(job_key, 'status', STATUS_FAILED)
                return {'status':'FAILED','reason':f'split part {idx} failed'}

           # --- Update split/segment progress after each successful part ---
            created = idx
            # Clamp to 99% until the last part is done for a nicer UX
            prog = int((created / P) * 100)
            if created < P:
                prog = min(prog, 99)
            redis_client.hset(job_key, mapping={
                'segmented_chunks': created,
                'segment_progress': prog,
                'segment_elapsed': round(_now() - seg_t0, 2),
            })
            encode_part(job_id, idx, master_url, v_sel, a_sel)

        # Finalize split progress
        redis_client.hset(job_key, mapping={
            'segment_progress': 100,
            'segment_elapsed': round(_now() - seg_t0, 2),
        })

        # Dispatch encode tasks
        #for idx in range(1, P+1):
        #    encode_part(job_id, idx, master_url, v_sel, a_sel)

        # Wait for encoded parts to arrive via PUT
        enc_dir = os.path.join(PROJECT_ROOT, job_id, "encoded")
        _ensure_dirs(enc_dir)

        deadline = _now() + max(300.0, duration * 3)  # generous timeout
        while _now() < deadline:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] Halted during encoding wait")
                return {'status':'ABORTED'}

            present = []
            for f in sorted(glob.glob(os.path.join(enc_dir, "enc_*.mp4"))):
                m = re.search(r"enc_(\d+)\.mp4$", os.path.basename(f))
                if m:
                    present.append(int(m.group(1)))

            done = len(set(present))
            redis_client.hset(job_key, 'parts_done', done)
            if P:
                redis_client.hset(job_key, 'encode_progress', int((done / P) * 100))
            if done >= P:
                break
            time.sleep(1.0)

        if int(redis_client.hget(job_key, 'parts_done') or 0) < P:
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status':'FAILED','reason':'timeout waiting for encoded parts'}

        # Stitch in tmpfs (concat copy)
        base_dir = os.path.join(PROJECT_ROOT, job_id)
        concat_path = os.path.join(base_dir, "concat.txt")
        enc_paths = [os.path.join(enc_dir, f"enc_{i:03d}.mp4") for i in range(1, P+1)]
        with open(concat_path, 'w') as f:
            for pth in enc_paths:
                f.write(f"file '{pth}'\n")

        local_out = os.path.join(base_dir, f"job_{job_id}_output.mp4")
        stitch_cmd = [
            'ffmpeg', '-hide_banner',
            '-f', 'concat', '-safe', '0',
            '-i', concat_path,
            '-c', 'copy',
            '-movflags', '+faststart',
            local_out
        ]
        logger.info(f"[{job_id}] Stitch: {' '.join(stitch_cmd)}")
        pr = subprocess.run(stitch_cmd, capture_output=True, text=True)
        if pr.returncode != 0 or not os.path.exists(local_out):
            logger.error(f"[{job_id}] Stitch failed: {pr.stderr[-1000:]}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status':'FAILED','reason':'stitch failed'}

        # Move final to NFS library and set metadata
        job = redis_client.hgetall(job_key) or {}
        src_filename = job.get('filename') or os.path.basename(src_path)
        final_path = _final_output_path(src_filename)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        shutil.move(local_out, final_path)

        # Probe final for UI fields
        try:
            dst_size_b = os.path.getsize(final_path)
        except Exception:
            dst_size_b = 0
        try:
            pr = subprocess.run([
                'ffprobe','-v','error',
                '-select_streams','v:0',
                '-show_entries','stream=codec_name,width,height,avg_frame_rate',
                '-show_entries','format=duration,bit_rate',
                '-of','json', final_path
            ], capture_output=True, text=True, check=True)
            info = json.loads(pr.stdout or '{}')
            fmt = info.get('format', {}) or {}
            streams = info.get('streams', []) or []
            v0 = streams[0] if streams else {}
            dst_codec = v0.get('codec_name') or ''
            w = v0.get('width') or 0
            h = v0.get('height') or 0
            afr = v0.get('avg_frame_rate') or '0'
            try:
                dst_fps = (float(afr.split('/')[0])/float(afr.split('/')[1])) if '/' in afr else float(afr)
            except Exception:
                dst_fps = 0.0
            try:
                dst_dur = float(fmt.get('duration', 0) or 0)
            except Exception:
                dst_dur = 0.0
            try:
                dst_bps = int(fmt.get('bit_rate', 0) or 0)
            except Exception:
                dst_bps = 0
            dst_kbps = (dst_bps/1000.0) if dst_bps>0 else (
                ((dst_size_b*8)/dst_dur/1000.0) if (dst_size_b and dst_dur) else 0.0
            )
            redis_client.hset(job_key, mapping={
                'dest_file_size': dst_size_b,
                'dest_duration': f"{dst_dur:.2f}" if dst_dur else '0',
                'dest_codec': dst_codec,
                'dest_resolution': f"{w}x{h}" if (w and h) else '',
                'dest_fps': f"{dst_fps:.2f}" if dst_fps else '0',
                'dest_bitrate_kbps': f"{dst_kbps:.0f}" if dst_kbps>0 else '0'
            })
        except Exception:
            pass

        # Cleanup tmpfs (optional)
        try:
            shutil.rmtree(os.path.join(PROJECT_ROOT, job_id))
        except Exception:
            pass

        redis_client.hset(job_key, mapping={
            'status': STATUS_DONE,
            'output_path': final_path,
            'ended_at': _now(),
            'combine_progress': 100
        })
        return {'status':'COMPLETED','output': final_path}

    except Exception as e:
        logger.exception(f"[{job_id}] transcode_video failed")
        redis_client.hset(job_key, 'status', STATUS_FAILED)
        return {'status':'FAILED','error': str(e)}

@huey.task(retries=1, retry_delay=5)
def encode_part(job_id: str, idx: int, master_host: str, v_sel: int = 0, a_sel: int = 0):
    """
    Worker task:
      - GET /job/<job_id>/part/<idx> from master
      - ffmpeg VAAPI transcode
      - PUT /job/<job_id>/result/<idx> back to master
    """
    try:
        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode_part {idx}: halted before start")
            return {'status':'ABORTED'}

        import requests

        # Paths in local tmpfs (worker)
        wtmp = os.path.join(PROJECT_ROOT, job_id)
        _ensure_dirs(wtmp)
        in_path  = os.path.join(wtmp, f"in_{idx:03d}.ts")
        out_path = os.path.join(wtmp, f"out_{idx:03d}.mp4")

        # Download part (stream)
        get_url = f"{master_host.rstrip('/')}/job/{job_id}/part/{idx}"
        with requests.get(get_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(in_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)

        # Encode (identical settings across all workers)
        cmd = [
            'ffmpeg','-hide_banner',
            '-vaapi_device', VAAPI_DEVICE,
            '-i', in_path,
            '-map','0:v:0',
            '-vf', SCALE_FILTER,
            '-c:v','h264_vaapi',
            '-rc_mode', VAAPI_RC_MODE,
            '-qp', VAAPI_QP,
            '-map','0:a?', *AUDIO_ARGS.split(),
            out_path
        ]
        logger.info(f"[{job_id}] encode_part {idx}: {' '.join(cmd)}")
        pr = subprocess.run(cmd, capture_output=True, text=True)
        if pr.returncode != 0 or not os.path.exists(out_path):
            logger.error(f"[{job_id}] encode_part {idx} failed: {pr.stderr[-1000:]}")
            raise subprocess.CalledProcessError(pr.returncode, cmd, pr.stdout, pr.stderr)

        # Upload result (PUT)
        put_url = f"{master_host.rstrip('/')}/job/{job_id}/result/{idx}"
        with open(out_path, 'rb') as f:
            headers = {'Content-Type':'video/mp4', 'Content-Length': str(os.path.getsize(out_path))}
            r = requests.put(put_url, data=f, headers=headers, timeout=120)
            if r.status_code // 100 != 2:
                raise RuntimeError(f"PUT failed {r.status_code}: {r.text[:300]}")

        # Cleanup local tmpfs
        for p in (in_path, out_path):
            try: os.remove(p)
            except Exception: pass

        # Best-effort UI bump
        try:
            job_key = _job_key(job_id)
            completed = int(redis_client.hincrby(job_key, 'completed_chunks', 1))
            total = int(redis_client.hget(job_key, 'parts_total') or 0)
            if total:
                redis_client.hset(job_key, 'encode_progress', int((completed/total)*100))
        except Exception:
            pass

        return {'status':'COMPLETED','idx': idx}

    except subprocess.CalledProcessError as e:
        logger.error(f"[{job_id}] encode_part {idx} failed: {e.stderr}")
        raise
    except Exception as e:
        logger.exception(f"[{job_id}] encode_part {idx} unexpected error")
        raise

# --------------- Legacy shim ----------------

@huey.task()
def segment_video(job_id: str, file_path: str, filename: Optional[str] = None):
    """
    Legacy entry kept for compatibility with existing Flask app calls.
    Delegates to transcode_video().
    """
    return transcode_video(job_id, file_path)
