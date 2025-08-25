# worker/app/tasks.py
# Master-orchestrated pipeline with tmpfs + built-in HTTP server.
# - transcode_video(job_id, file_path): runs on the node that picks up this task ("master") – segments & dispatches encodes
# - stitch_parts(job_id): runs on any node ("stitcher") – receives encoded parts via HTTP PUT, stitches, writes final to NFS
# - encode_part(job_id, idx, master_host, ...): runs on any node; GETs from master, PUTs to stitcher
#
# Also includes probe_source() and a legacy shim segment_video()->transcode_video().

from huey import RedisHuey

import os
import time
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
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

# ----------------- Configuration -----------------

ENV = os.environ.get

REDIS_HOST = ENV("REDIS_HOST", "192.168.0.120")
REDIS_PORT = int(ENV("REDIS_PORT", "6379"))
REDIS_DB_TASKS = int(ENV("REDIS_DB_TASKS", "0"))  # Huey broker
REDIS_DB_DATA  = int(ENV("REDIS_DB_DATA",  "1"))  # App/job state

huey = RedisHuey(
    'tasks',
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB_TASKS,
    retry_on_timeout=True,
    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB_DATA,
    decode_responses=True,
    socket_keepalive=True,
    socket_timeout=5,
    socket_connect_timeout=5,
    health_check_interval=30,
    retry_on_timeout=True,
    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=16),
)

# Filesystem layout
PROJECT_ROOT = ENV("PROJECT_ROOT", "/projects")
WATCH_ROOT   = ENV("WATCH_ROOT",   "/watch")
LIBRARY_ROOT = ENV("LIBRARY_ROOT", "/library")

# Metrics TTL heuristic to consider a node "active"
METRICS_TTL_SEC   = int(ENV("TTL_SEC", "15"))
METRICS_GRACE_SEC = int(ENV("TTL_GRACE_SEC", "5"))

# HTTP server (used by BOTH master and stitcher)
HTTP_BIND_HOST = "0.0.0.0"
HTTP_PORT      = int(ENV("MASTER_HTTP_PORT", "8000"))

# Encoding parameters
VAAPI_DEVICE  = ENV("VAAPI_DEVICE", "/dev/dri/renderD128")
SCALE_FILTER  = ENV("VEM_SCALE_FILTER", "scale=-1:720,format=nv12,hwupload")
VAAPI_RC_MODE = ENV("VEM_RC_MODE", "CQP")
VAAPI_QP      = ENV("VEM_QP", "27")
AUDIO_ARGS    = ENV("VEM_AUDIO_ARGS", "-c:a aac -ac 2 -b:a 192k")  # space-separated

# Parts planning (used only as fallback if manager didn't hint)
PARTS_PER_WORKER = int(ENV("PARTS_PER_WORKER", "4"))
MIN_PARTS        = int(ENV("MIN_PARTS", "4"))
MAX_PARTS        = int(ENV("MAX_PARTS", "200"))

# Status constants
STATUS_READY    = 'READY'
STATUS_STARTING = 'STARTING'
STATUS_RUNNING  = 'RUNNING'
STATUS_STOPPED  = 'STOPPED'
STATUS_FAILED   = 'FAILED'
STATUS_DONE     = 'DONE'

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
    Fast active host detection without SCAN:
      - derive host universe from nodes:mac
      - pipeline HGET metrics:node:<host> ts
      - compare against cutoff
    """
    cutoff = int(_now()) - (METRICS_TTL_SEC + METRICS_GRACE_SEC)
    mac_map = redis_client.hgetall("nodes:mac") or {}
    hosts = list(mac_map.keys())
    if not hosts:
        return []

    pipe = redis_client.pipeline()
    for h in hosts:
        pipe.hget(f"metrics:node:{h}", "ts")
    ts_vals = pipe.execute()

    actives = []
    for h, ts in zip(hosts, ts_vals):
        try:
            t = int(float(ts or 0))
        except Exception:
            t = 0
        if t >= cutoff:
            actives.append(h)
    actives.sort()
    return actives

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

# --------------- Built-in HTTP server (shared) ----------------

_HTTP_SERVER = None
_HTTP_THREAD = None
_HTTP_STARTED = False
_HTTP_LOCK = threading.Lock()

def _start_http_once():
    """
    Start a tiny HTTP server supporting:
      - GET  /job/<job_id>/part/<idx>   -> serve raw .ts part
      - PUT  /job/<job_id>/result/<idx> -> accept encoded .mp4
    Used by BOTH master and stitcher nodes.
    """
    global _HTTP_SERVER, _HTTP_THREAD, _HTTP_STARTED
    with _HTTP_LOCK:
        if _HTTP_STARTED:
            return

        from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
        from urllib.parse import urlparse

        class PartHandler(BaseHTTPRequestHandler):
            server_version = "ThinmanParts/1.1"

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
                    if idx <= 0 or idx > 99999:
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
                        try: os.remove(tmp_path)
                        except Exception: pass
                        self.send_error(400, "Incomplete upload")
                        return

                    os.replace(tmp_path, enc_path)
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
            logger.info(f"HTTP server listening on {HTTP_BIND_HOST}:{HTTP_PORT}")
            try:
                _HTTP_SERVER.serve_forever(poll_interval=0.5)
            except Exception:
                logger.exception("HTTP server terminated")

        _HTTP_THREAD = threading.Thread(target=_serve, name="thinman-http", daemon=True)
        _HTTP_THREAD.start()
        _HTTP_STARTED = True

# -------------------- Tasks --------------------

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


@huey.task()
def stitch_parts(job_id: str):
    """
    Runs on the "stitcher" node.
    - Starts HTTP server for receiving encoded parts via PUT.
    - Publishes stitch_host to Redis so encoders know where to upload.
    - Waits for all encoded parts; stitches and moves final to /library.
    """
    job_key = _job_key(job_id)
    try:
        _start_http_once()
        stitch_host = f"http://{ENV('HOSTNAME') or socket.gethostname()}:{HTTP_PORT}"
        # advertise stitcher location ASAP
        redis_client.hset(job_key, mapping={'stitch_host': stitch_host})
        logger.info(f"[{job_id}] Stitcher ready at {stitch_host}")

        # read total parts (wait a bit until master sets it)
        deadline = _now() + 300
        P = 0
        while _now() < deadline:
            try:
                P = int(redis_client.hget(job_key, 'parts_total') or 0)
                if P > 0:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if P <= 0:
            logger.error(f"[{job_id}] stitch_parts: parts_total not set")
            return {'status':'FAILED','reason':'parts_total not set'}

        enc_dir = os.path.join(PROJECT_ROOT, job_id, "encoded")
        _ensure_dirs(enc_dir)

        def _ready_set():
            now_ts = _now()
            ready = set()
            for f in glob.glob(os.path.join(enc_dir, "enc_*.mp4")):
                base = os.path.basename(f)
                m = re.match(r"enc_(\d+)\.mp4$", base)
                if not m:
                    continue
                try:
                    st = os.stat(f)
                except Exception:
                    continue
                if st.st_size > 0 and (now_ts - st.st_mtime) > 0.8:
                    i = int(m.group(1))
                    if 1 <= i <= P:
                        ready.add(i)
            return ready

        # wait for all encoded parts
        expected = set(range(1, P + 1))
        try:
            src_dur = float(redis_client.hget(job_key, 'source_duration') or 0)
        except Exception:
            src_dur = 0.0
        wait_deadline = _now() + max(300.0, (src_dur or 0) * 3)

        while _now() < wait_deadline:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] stitch_parts aborted (job halted)")
                return {'status':'ABORTED'}
            ready = _ready_set()
            done_fs = len(ready)

            # mirror encode progress as a convenience (in case encoders didn't)
            try:
                cur = int(redis_client.hget(job_key, 'encode_progress') or 0)
                prog = int((done_fs / P) * 100) if P else 0
                if prog > cur:
                    redis_client.hset(job_key, 'encode_progress', prog)
                redis_client.hset(job_key, 'parts_done', done_fs)
            except Exception:
                pass

            if done_fs >= P:
                break
            time.sleep(1.0)

        if len(_ready_set()) < P:
            missing = sorted(expected - _ready_set())
            logger.error(f"[{job_id}] stitch_parts timeout; missing: {missing}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status':'FAILED','reason':'timeout waiting for encoded parts'}

        # ---- Stitch (concat copy) ----
        base_dir = os.path.join(PROJECT_ROOT, job_id)
        concat_path = os.path.join(base_dir, "concat.txt")
        enc_paths = [os.path.join(enc_dir, f"enc_{i:03d}.mp4") for i in range(1, P + 1)]
        with open(concat_path, 'w') as f:
            for pth in enc_paths:
                f.write(f"file '{pth}'\n")

        local_out = os.path.join(base_dir, f"job_{job_id}_output.mp4")
        combine_t0 = _now()
        redis_client.hset(job_key, mapping={'combine_progress': 0, 'combine_elapsed': 0})

        total_us = int((src_dur or 0) * 1_000_000)
        stitch_cmd = [
            'ffmpeg', '-hide_banner',
            '-f', 'concat', '-safe', '0',
            '-i', concat_path,
            '-c', 'copy',
            '-movflags', '+faststart',
            '-nostats', '-loglevel', 'error',
            '-progress', 'pipe:1',
            local_out
        ]
        logger.info(f"[{job_id}] Stitch cmd: {' '.join(stitch_cmd)}")

        proc = subprocess.Popen(stitch_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            while True:
                line = proc.stdout.readline() if proc.stdout else ''
                if not line:
                    if proc.poll() is not None:
                        break
                    time.sleep(0.05)
                    continue
                line = line.strip()
                if line.startswith('out_time_ms='):
                    try:
                        out_ms = int(line.split('=', 1)[1] or '0')
                    except Exception:
                        out_ms = 0
                    if total_us > 0:
                        pct = int(min(100, max(0, (out_ms / total_us) * 100)))
                        pct = min(pct, 99)
                        redis_client.hset(job_key, mapping={
                            'combine_progress': pct,
                            'combine_elapsed': round(_now() - combine_t0, 2),
                        })
                    else:
                        redis_client.hset(job_key, 'combine_elapsed', round(_now() - combine_t0, 2))
                elif line.startswith('progress=') and line.split('=',1)[1].strip() == 'end':
                    break
        finally:
            rc = proc.wait()

        if rc != 0 or not os.path.exists(local_out):
            err_tail = ''
            try:
                err_tail = (proc.stderr.read() or '')[-1000:]
            except Exception:
                pass
            logger.error(f"[{job_id}] Stitch failed (rc={rc}): {err_tail}")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'reason': 'stitch failed'}

        # Move final to library + set metadata
        job = redis_client.hgetall(job_key) or {}
        src_filename = job.get('filename') or os.path.basename(job.get('input_path','') or '')
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

        # Cleanup project temp dir
        try:
            shutil.rmtree(os.path.join(PROJECT_ROOT, job_id))
        except Exception:
            pass

        # Final combine progress update
        redis_client.hset(job_key, mapping={
            'status': STATUS_DONE,
            'output_path': final_path,
            'ended_at': _now(),
            'combine_progress': 100,
            'combine_elapsed': round(_now() - combine_t0, 2),
        })

        try:
            redis_client.delete(f"job_done_parts:{job_id}")
            redis_client.hdel(f"job:{job_id}", "awaiting_parts")
        except Exception:
            pass

        return {'status': 'COMPLETED', 'output': final_path}

    except Exception as e:
        logger.exception(f"[{job_id}] stitch_parts failed")
        redis_client.hset(job_key, 'status', STATUS_FAILED)
        return {'status': 'FAILED', 'error': str(e)}

@huey.task()
def transcode_video(job_id: str, file_path: str):
    """
    Master-side orchestration:
      - Start HTTP server (serve parts).
      - Kick off stitch_parts(job_id) so the stitcher publishes stitch_host.
      - Segment source and dispatch encode_part for each segment.
      - No waiting or stitching here anymore.
    """
    job_key = _job_key(job_id)
    job = redis_client.hgetall(job_key)

    try:
        _start_http_once()  # HTTP server for parts

        # Resolve input path
        src_path = file_path
        if not os.path.exists(src_path):
            filename = job.get('filename') or ''
            alt = os.path.join(WATCH_ROOT, filename.lstrip('/'))
            if os.path.exists(alt):
                src_path = alt
        if not os.path.exists(src_path):
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'reason': f'input not found: {src_path}'}

        # Mark as RUNNING & remember input path
        now = _now()
        redis_client.hset(job_key, mapping={'status': STATUS_RUNNING, 'started_at': now, 'input_path': src_path})

        # Probe essentials for UI (cheap)
        meta     = _ffprobe_stream0(src_path)
        duration = _ffprobe_duration(src_path)
        codec    = meta.get("codec") or ""
        width    = meta.get("width") or 0
        height   = meta.get("height") or 0
        fps      = meta.get("fps") or 0.0
        size_b   = meta.get("size") or 0
        fmt_bps  = meta.get("bit_rate") or 0
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

        # Advertise master URL
        master_host_name = ENV("HOSTNAME") or socket.gethostname()
        master_url = f"http://{master_host_name}:{HTTP_PORT}"
        redis_client.hset(job_key, 'master_host', master_url)

        # Kick off stitcher immediately so encoders have a destination
        stitch_parts(job_id)

        # Decide number of parts:
        # 1) Prefer parts_hint from manager
        # 2) Else derive from active workers * PARTS_PER_WORKER
        # 3) Clamp to [MIN_PARTS, MAX_PARTS]
        actives = _active_nodes()
        P = max(MIN_PARTS, min(MAX_PARTS, max(1, len(actives)) * PARTS_PER_WORKER))
        logger.info(f"[{job_id}] Target parts (estimate): {P}")

        seg_t0 = _now()
        redis_client.hset(job_key, mapping={'parts_total': P, 'parts_done': 0})

        # Which streams to carry
        v_sel, a_sel = _pick_stream_indices(job_key)
        redis_client.hset(job_key, mapping={'selected_v_stream': v_sel, 'selected_a_stream': a_sel})

        # Prepare parts dir + naming
        parts_dir = os.path.join(PROJECT_ROOT, job_id, "parts")
        _ensure_dirs(parts_dir)
        temp_pattern = os.path.join(parts_dir, "chunk_%03d.ts")

        # Segment duration target from P (fallback 10s if unknown)
        segment_duration = max(1.0, float(duration) / float(P)) if (duration and P) else 10.0
        bsf = _bsf_for_codec(codec)

        cmd = [
            'ffmpeg', '-hide_banner',
            '-nostats', '-loglevel', 'info',
            '-i', src_path,
            '-map', f'0:v:{v_sel}',
            '-map', f'0:a:{a_sel}?',
            '-sn', '-dn',
            '-map_metadata', '-1',
            '-map_chapters', '-1',
            '-c', 'copy',
            '-bsf:v', bsf,
            '-f', 'segment',
            '-segment_time', f"{segment_duration:.6f}",
            '-force_key_frames', f"expr:gte(t,n_forced*{segment_duration:.6f})",
            '-reset_timestamps', '1',
            temp_pattern
        ]
        logger.info(f"[{job_id}] Segment command: {' '.join(cmd)}")

        # Parse ffmpeg stderr for new-chunk openings
        segment_re = re.compile(r"Opening '(.+?/chunk_(\d+)\.ts)' for writing")
        process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        previous_chunk_path = None
        previous_chunk_index = None
        estimated_chunks = max(1, P)
        queued_count = 0

        while True:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] Halted. Terminating segmentation.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stderr.readline()
            if not line:
                break

            m = segment_re.search(line)
            if m:
                chunk_path = m.group(1)
                chunk_index = int(m.group(2))  # 0-based

                # update progress on opening next chunk
                try:
                    prog = int(((min(chunk_index + 1, estimated_chunks)) / max(1, estimated_chunks)) * 100)
                    prog = min(prog, 99)
                    redis_client.hset(job_key, mapping={'segment_progress': prog})
                except Exception:
                    pass

                # queue the *previous* chunk which just closed
                if previous_chunk_index is not None and previous_chunk_path:
                    part_idx = previous_chunk_index + 1  # 1-based
                    dest_path, _ = _part_paths(job_id, part_idx)
                    _ensure_dirs(os.path.dirname(dest_path))
                    try:
                        os.replace(previous_chunk_path, dest_path)
                    except Exception as e:
                        logger.error(f"[{job_id}] Move part {part_idx} failed: {e}")
                        process.terminate(); process.wait()
                        redis_client.hset(job_key, 'status', STATUS_FAILED)
                        return {'status': 'FAILED', 'reason': f'move part {part_idx} failed'}

                    queued_count = part_idx
                    try:
                        prog = int((queued_count / max(1, estimated_chunks)) * 100)
                        if queued_count < estimated_chunks:
                            prog = min(prog, 99)
                        redis_client.hset(job_key, mapping={
                            'segmented_chunks': queued_count,
                            'segment_progress': prog,
                            'segment_elapsed': round(_now() - seg_t0, 2),
                        })
                    except Exception:
                        pass

                    encode_part(job_id, part_idx, master_url, v_sel, a_sel)

                previous_chunk_path = chunk_path
                previous_chunk_index = chunk_index

        process.wait()
        if process.returncode != 0:
            logger.error(f"[{job_id}] ffmpeg segmentation failed (rc={process.returncode})")
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED'}

        # Queue final chunk
        total_chunks = 0
        if previous_chunk_index is not None and previous_chunk_path:
            part_idx = previous_chunk_index + 1
            dest_path, _ = _part_paths(job_id, part_idx)
            _ensure_dirs(os.path.dirname(dest_path))
            try:
                os.replace(previous_chunk_path, dest_path)
            except Exception as e:
                logger.error(f"[{job_id}] Move final part {part_idx} failed: {e}")
                redis_client.hset(job_key, 'status', STATUS_FAILED)
                return {'status': 'FAILED', 'reason': f'move part {part_idx} failed'}

            encode_part(job_id, part_idx, master_url, v_sel, a_sel)
            total_chunks = part_idx

        if total_chunks <= 0:
            redis_client.hset(job_key, 'status', STATUS_FAILED)
            return {'status': 'FAILED', 'reason': 'no segments produced'}

        # Finalize segmentation metadata
        redis_client.hset(job_key, mapping={
            'parts_total': total_chunks,
            'segmented_chunks': total_chunks,
            'segment_progress': 100,
            'segment_elapsed': round(_now() - seg_t0, 2),
        })

        # Master exits — stitcher will finish the job
        logger.info(f"[{job_id}] Segmentation complete; {total_chunks} parts queued for encoding")
        return {'status': 'QUEUED', 'parts': total_chunks}

    except Exception as e:
        logger.exception(f"[{job_id}] transcode_video failed")
        redis_client.hset(job_key, 'status', STATUS_FAILED)
        return {'status': 'FAILED', 'error': str(e)}

@huey.task(retries=1, retry_delay=5)
def encode_part(job_id: str, idx: int, master_host: str, v_sel: int = 0, a_sel: int = 0, stitch_host: Optional[str] = None):
    """
    Worker task:
      - GET /job/<job_id>/part/<idx> from MASTER
      - ffmpeg VAAPI transcode
      - PUT /job/<job_id>/result/<idx> to STITCHER (publishes 'stitch_host')
      - Update encode_progress/parts_done/completed_chunks idempotently on completion
    """
    logger.info(f"[{job_id}] Preparing to encode part {idx}")
    try:
        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode_part {idx}: halted before start")
            return {'status': 'ABORTED'}

        encode_started_now = _now()

        import requests

        # resolve stitch_host if not provided
        if not stitch_host:
            job_key = _job_key(job_id)
            # wait a bit for the stitcher to publish its host
            deadline = _now() + 60
            while _now() < deadline and not stitch_host:
                stitch_host = redis_client.hget(job_key, 'stitch_host') or None
                if stitch_host:
                    break
                time.sleep(0.25)
            # last resort: fall back to master (keeps pipeline from wedging)
            if not stitch_host:
                stitch_host = master_host

        # Paths in local tmpfs (worker)
        wtmp = os.path.join(PROJECT_ROOT, job_id)
        _ensure_dirs(wtmp)
        in_path  = os.path.join(wtmp, f"in_{idx:03d}.ts")
        out_path = os.path.join(wtmp, f"out_{idx:03d}.mp4")

        # Download part from master
        get_url = f"{master_host.rstrip('/')}/job/{job_id}/part/{idx}"
        logger.info(f"[{job_id}] GET part {idx} from {get_url}")
        with requests.get(get_url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(in_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        # Encode
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

        # Upload result to stitcher
        put_url = f"{stitch_host.rstrip('/')}/job/{job_id}/result/{idx}"
        logger.info(f"[{job_id}] PUT result {idx} to {put_url}")
        with open(out_path, 'rb') as f:
            headers = {'Content-Type':'video/mp4', 'Content-Length': str(os.path.getsize(out_path))}
            r = requests.put(put_url, data=f, headers=headers, timeout=120)
            if r.status_code // 100 != 2:
                raise RuntimeError(f"PUT failed {r.status_code}: {r.text[:300]}")

        # Cleanup local tmpfs
        for p in (in_path, out_path):
            try: os.remove(p)
            except Exception: pass

        # ------- Progress & timing (idempotent) -------
        try:
            job_key  = _job_key(job_id)
            done_set = f"job_done_parts:{job_id}"

            added = redis_client.sadd(done_set, idx)  # 1 if new, 0 if already present
            if added:
                pipe = redis_client.pipeline()
                pipe.hincrby(job_key, 'completed_chunks', 1)
                pipe.hincrby(job_key, 'parts_done', 1)
                pipe.execute()

            total = int(redis_client.hget(job_key, 'parts_total') or 0)

            # set encode_started once
            try:
                redis_client.hsetnx(job_key, 'encode_started', encode_started_now)
                enc_start = float(redis_client.hget(job_key, 'encode_started') or encode_started_now)
            except Exception:
                enc_start = encode_started_now

            elapsed = round(_now() - enc_start, 2)
            redis_client.hset(job_key, 'encode_elapsed', elapsed)

            if total:
                done = int(redis_client.hget(job_key, 'parts_done') or 0)
                cur  = int(redis_client.hget(job_key, 'encode_progress') or 0)
                prog = int((done / total) * 100)
                if done >= total:
                    prog = 100
                if prog > cur:
                    redis_client.hset(job_key, 'encode_progress', prog)

        except Exception as e:
            logger.error(f"[{job_id}] encode_part {idx} progress update error: {e}")

        return {'status':'COMPLETED','idx': idx}

    except subprocess.CalledProcessError as e:
        logger.error(f"[{job_id}] encode_part {idx} failed: {e.stderr}")
        raise
    except Exception:
        logger.exception(f"[{job_id}] encode_part {idx} unexpected error")
        raise
