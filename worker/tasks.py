# worker/app/tasks.py
# Master-orchestrated pipeline with tmpfs + built-in HTTP server.
# - transcode(job_id, file_path): orchestration entrypoint (master) – runs stitch() then split()
# - split(job_id, file_path): runs on the node that picks up this task ("master") – segments & dispatches encodes
# - stitch(job_id): runs on any node ("stitcher") – receives encoded parts via HTTP PUT, stitches, writes final to NFS
# - encode(job_id, idx, master_host, ...): runs on any node; GETs from master, PUTs to stitcher
#
# Also includes probe_source() utilities.

from huey import RedisHuey

import os
import re
import time
import json
import glob
import uuid
import shutil
import socket
import logging
import subprocess
import threading
from math import ceil
from typing import List, Tuple, Optional
from collections import deque

from common import ENV, Status, get_huey, get_redis, get_logging, emit_activity
huey = get_huey()
redis = get_redis()
logger = get_logging("worker")

# Filesystem layout
PROJECT_ROOT = ENV("PROJECT_ROOT", "/projects")
WATCH_ROOT   = ENV("WATCH_ROOT",   "/watch")
SOURCE_MEDIA_ROOT = ENV("SOURCE_MEDIA_ROOT", "/source_media")
LIBRARY_ROOT = ENV("LIBRARY_ROOT", "/library")

# Metrics TTL heuristic to consider a node "active"
METRICS_TTL_SEC   = int(ENV("TTL_SEC", "15"))
METRICS_GRACE_SEC = int(ENV("TTL_GRACE_SEC", "5"))

# HTTP server (used by BOTH master and stitcher)
HTTP_BIND_HOST = "0.0.0.0"
HTTP_PORT      = int(ENV("MASTER_HTTP_PORT", "8000"))

# Encoding parameters
VAAPI_DEVICE  = ENV("VAAPI_DEVICE", "/dev/dri/renderD128")
ALLOWED_TARGET_HEIGHTS = {480, 576, 720, 1080}
try:
    DEFAULT_TARGET_HEIGHT = int(ENV("VEM_DEFAULT_TARGET_HEIGHT", "1080"))
except Exception:
    DEFAULT_TARGET_HEIGHT = 1080
SCALE_FILTER_480 = ENV("VEM_SCALE_FILTER_480", "bwdif=mode=send_frame:parity=auto:deint=all,scale=-2:480,format=nv12,hwupload")
SCALE_FILTER_576 = ENV("VEM_SCALE_FILTER_576", "bwdif=mode=send_frame:parity=auto:deint=all,scale=-2:576,format=nv12,hwupload")
SCALE_FILTER_720 = ENV("VEM_SCALE_FILTER_720", "scale=-2:720,format=nv12,hwupload")
SCALE_FILTER_1080 = ENV("VEM_SCALE_FILTER_1080", "scale=-2:1080,format=nv12,hwupload")
VAAPI_RC_MODE = ENV("VEM_RC_MODE", "CQP")
VAAPI_QP      = ENV("VEM_QP", "27")
AUDIO_ARGS    = ENV("VEM_AUDIO_ARGS", "-c:a aac -ac 2 -b:a 192k")  # space-separated

# Parts planning (used only as fallback if manager didn't hint)
PARTS_PER_WORKER = int(ENV("PARTS_PER_WORKER", "4"))
MIN_PARTS        = int(ENV("MIN_PARTS", "4"))
MAX_PARTS        = int(ENV("MAX_PARTS", "200"))
DEFAULT_TARGET_SEGMENT_MB = 10.0
DEFAULT_TARGET_SEGMENT_BYTES = max(1, int(DEFAULT_TARGET_SEGMENT_MB * 1024 * 1024))


# --------------- Helpers ----------------

def _job_key(job_id: str) -> str:
    return f"job:{job_id}"

def _job_title(job):
    filename = (job.get("filename") or "").strip()
    base = os.path.basename(filename)
    if not base:
        return "Unknown"
    return os.path.splitext(base)[0] or base

def _now() -> float:
    return time.time()

def _elapsed_ms(started_at):
    try:
        return max(0, int(round((_now() - float(started_at)) * 1000)))
    except Exception:
        return 0

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
    mac_map = redis.hgetall("nodes:mac") or {}
    hosts = list(mac_map.keys())
    if not hosts:
        return []

    pipe = redis.pipeline()
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
    codec = (codec or "").strip().lower()
    if codec == 'h264':
        return 'h264_mp4toannexb'
    if codec == 'hevc':
        return 'hevc_mp4toannexb'
    return ""

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

def _job_project_root(job_id: str, job=None) -> str:
    if job is None:
        job = redis.hgetall(_job_key(job_id)) or {}
    root = (job.get("scratch_root") or "").strip()
    return root or PROJECT_ROOT

def _job_base_dir(job_id: str, job=None) -> str:
    preferred_root = _job_project_root(job_id, job)
    preferred_base = os.path.join(preferred_root, job_id)
    try:
        os.makedirs(preferred_base, exist_ok=True)
        try:
            redis.hset(_job_key(job_id), mapping={
                "scratch_root_effective": preferred_root,
            })
        except Exception:
            pass
        return preferred_base
    except Exception as e:
        fallback_root = PROJECT_ROOT
        fallback_base = os.path.join(fallback_root, job_id)
        os.makedirs(fallback_base, exist_ok=True)
        if preferred_root != fallback_root:
            logger.warning(
                "[%s] scratch root '%s' unavailable (%s); falling back to '%s'",
                job_id,
                preferred_root,
                e,
                fallback_root,
            )
            try:
                redis.hset(_job_key(job_id), mapping={
                    "scratch_root_effective": fallback_root,
                    "scratch_root_fallback_error": str(e),
                })
            except Exception:
                pass
        return fallback_base

def _part_paths(job_id: str, idx: int) -> Tuple[str, str]:
    base_dir  = _job_base_dir(job_id)
    parts_dir = os.path.join(base_dir, "parts")
    enc_dir   = os.path.join(base_dir, "encoded")
    _ensure_dirs(base_dir, parts_dir, enc_dir)
    return (os.path.join(parts_dir, f"part_{idx:03d}.ts"),
            os.path.join(enc_dir,   f"enc_{idx:03d}.mp4"))


def _reset_job_run_state(job_id: str, job=None):
    """
    Clear stale per-run files/counters so retries/restarts don't reuse old parts.
    """
    job_key = _job_key(job_id)
    base_dir = _job_base_dir(job_id, job)
    parts_dir = os.path.join(base_dir, "parts")
    enc_dir = os.path.join(base_dir, "encoded")

    for path in (parts_dir, enc_dir):
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass
        except Exception:
            pass
        _ensure_dirs(path)

    for path in (
        os.path.join(base_dir, "concat.txt"),
        os.path.join(base_dir, f"job_{job_id}_output.mp4"),
    ):
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    try:
        redis.hset(job_key, mapping={
            'parts_total': 0,
            'parts_done': 0,
            'segmented_chunks': 0,
            'completed_chunks': 0,
            'stitched_chunks': 0,
            'segment_progress': 0,
            'segment_elapsed': 0,
            'encode_progress': 0,
            'encode_elapsed': 0,
            'combine_progress': 0,
            'combine_elapsed': 0,
            'error': '',
            'failed_part': 0,
            'failed_stage': '',
            'failed_worker': '',
            'processing_mode_effective': '',
            'processing_mode_reason': '',
            'direct_segment_duration': '',
            'ended_at': 0,
        })
        redis.delete(f"job_done_parts:{job_id}")
        redis.delete(f"job_retry_counts:{job_id}")
        redis.delete(f"job_retry_ts:{job_id}")
        redis.delete(f"job_missing_first_seen:{job_id}")
        redis.delete(f"job_retry_inflight:{job_id}")
    except Exception:
        pass

def _final_output_path(src_filename: str) -> str:
    base, _ = os.path.splitext(src_filename)
    return os.path.join(LIBRARY_ROOT, base.lstrip('/') + '.mp4')


def _final_output_path_with_ext(src_filename: str, extension: str) -> str:
    base, _ = os.path.splitext(src_filename)
    ext = (extension or ".mp4").strip()
    if not ext.startswith("."):
        ext = f".{ext}"
    return os.path.join(LIBRARY_ROOT, base.lstrip('/') + ext)

def _is_job_halted(job_id: str) -> bool:
    s = Status.parse(redis.hget(_job_key(job_id), "status"))
    return s in (Status.FAILED, Status.REJECTED, Status.STOPPED)

def _normalize_target_height(value) -> int:
    try:
        h = int(value)
    except Exception:
        h = DEFAULT_TARGET_HEIGHT
    return h if h in ALLOWED_TARGET_HEIGHTS else DEFAULT_TARGET_HEIGHT

def _target_height_for_job(job_key: str) -> int:
    return _normalize_target_height(redis.hget(job_key, "target_height"))

def _vaapi_scale_filter(target_height: int) -> str:
    if target_height == 480:
        return SCALE_FILTER_480
    if target_height == 576:
        return SCALE_FILTER_576
    return SCALE_FILTER_1080 if target_height == 1080 else SCALE_FILTER_720


def _software_scale_filter(target_height: int, *, deinterlace: bool) -> str:
    filters = []
    if deinterlace:
        filters.append("bwdif=mode=send_frame:parity=auto:deint=all")
    filters.append(f"scale=-2:{target_height}")
    return ",".join(filters)


def _reset_segment_video_pts_filter(filtergraph: str) -> str:
    """
    Encoded chunks are later concat-copied, so each chunk should start its
    video timeline at its first decoded frame. TS segments can otherwise carry
    a small leading PTS gap that ffmpeg fills by duplicating the first frame.
    """
    filtergraph = (filtergraph or "").strip()
    if not filtergraph:
        return "setpts=PTS-STARTPTS"
    return f"setpts=PTS-STARTPTS,{filtergraph}"


def _source_dimensions_for_job(job) -> Tuple[int, int]:
    raw = str(job.get("source_resolution") or "").strip().lower()
    match = re.match(r"^\s*(\d+)\s*x\s*(\d+)\s*$", raw)
    if not match:
        return (0, 0)
    try:
        return (int(match.group(1)), int(match.group(2)))
    except Exception:
        return (0, 0)


def _dvd_native_target_height(job) -> int | None:
    filename = str(job.get("filename") or "").strip().lower()
    codec = str(job.get("source_codec") or "").strip().lower()
    width, height = _source_dimensions_for_job(job)
    is_dvd_path = (
        filename.startswith("dvd/")
        or "/dvd/" in filename
        or filename.startswith("movies/")
        or "/movies/" in filename
    )
    is_sd_dimensions = width > 0 and width <= 720 and height > 0 and height <= 576
    if not is_dvd_path or not is_sd_dimensions:
        return None
    if codec and codec != "mpeg2video":
        return None
    if height <= 480:
        return 480
    return 576


def _effective_target_height_for_job(job_key: str) -> Tuple[int, bool]:
    job = redis.hgetall(job_key) or {}
    dvd_height = _dvd_native_target_height(job)
    if dvd_height:
        return (dvd_height, True)
    return (_normalize_target_height(job.get("target_height")), False)


def _source_english_subtitle_streams(src_path: str) -> list[dict]:
    if not src_path or not os.path.exists(src_path):
        return []
    try:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "s",
                "-show_entries",
                "stream=index,codec_name:stream_tags=language,title",
                "-of",
                "json",
                src_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(completed.stdout or "{}")
    except Exception:
        return []

    english_streams = []
    for stream in list(payload.get("streams") or []):
        tags = stream.get("tags") or {}
        language = str(tags.get("language") or "").strip().lower()
        if language in {"en", "eng"}:
            english_streams.append(stream)
    return english_streams

def _as_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "t"}

def _as_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default

def _load_global_settings():
    settings = {}
    try:
        settings.update(redis.hgetall("settings:global") or {})
    except Exception:
        pass
    try:
        settings.update(redis.hgetall("global:settings") or {})
    except Exception:
        pass
    settings.setdefault("low_disk_direct_enabled", "1")
    settings.setdefault("low_disk_min_free_gb", "20")
    settings.setdefault("target_segment_mb", "10")
    return settings

def _disk_free_bytes(path: str) -> int:
    try:
        return int(shutil.disk_usage(path).free)
    except Exception:
        return -1

def _direct_source_plan_for_part(source_duration: float, segment_duration: float, idx: int):
    if idx <= 0:
        return None, None
    segment_duration = max(1.0, float(segment_duration or 10.0))
    start_s = (idx - 1) * segment_duration
    if source_duration and start_s >= source_duration:
        return None, None
    part_duration = segment_duration
    if source_duration:
        part_duration = max(0.05, min(segment_duration, source_duration - start_s))
    return start_s, part_duration


def _parts_for_target_size(size_b: int, target_segment_bytes: int = DEFAULT_TARGET_SEGMENT_BYTES) -> int:
    size_b = int(size_b or 0)
    target_segment_bytes = max(1, int(target_segment_bytes or 1))
    if size_b <= 0:
        return 0
    return max(1, int(ceil(float(size_b) / float(target_segment_bytes))))


def _target_segment_bytes_from_settings(settings) -> Tuple[float, int]:
    target_mb = _as_float((settings or {}).get("target_segment_mb", DEFAULT_TARGET_SEGMENT_MB), DEFAULT_TARGET_SEGMENT_MB)
    if target_mb <= 0:
        target_mb = DEFAULT_TARGET_SEGMENT_MB
    return target_mb, max(1, int(target_mb * 1024 * 1024))


def _host_from_endpoint(endpoint: str) -> str:
    raw = str(endpoint or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)
    return raw.split("/", 1)[0].split(":", 1)[0].strip().lower()

def _resolve_processing_mode(job_id: str, job, source_size_b: int) -> Tuple[str, str]:
    input_path = os.path.realpath((job.get("input_path") or "").strip())
    source_root = os.path.realpath(SOURCE_MEDIA_ROOT)
    if (
        str(job.get("source_origin") or "").strip().lower() == "source_media"
        or (input_path and (input_path == source_root or input_path.startswith(source_root + os.sep)))
    ):
        return "split", "source_media_forces_split"

    mode = str(job.get("processing_mode") or "split").strip().lower()
    if mode not in ("split", "direct"):
        mode = "split"
    if mode == "direct":
        return "direct", "policy"
    if str(job.get("scratch_mode") or "local").strip().lower() != "local":
        return "split", "non_local_scratch"

    settings = _load_global_settings()
    low_disk_direct_enabled = _as_bool(settings.get("low_disk_direct_enabled", "1"), True)
    if not low_disk_direct_enabled:
        return "split", "policy"

    low_disk_min_free_gb = max(1.0, _as_float(settings.get("low_disk_min_free_gb", 20), 20.0))
    scratch_root = _job_project_root(job_id, job)
    free_b = _disk_free_bytes(scratch_root)
    if free_b < 0:
        return "split", "unknown_free_space"

    min_free_b = int(low_disk_min_free_gb * (1024 ** 3))
    estimated_need_b = max(min_free_b, int(source_size_b * 1.1) + (2 * 1024 ** 3))
    if free_b < estimated_need_b:
        return "direct", f"low_disk free={free_b} need={estimated_need_b} root={scratch_root}"
    return "split", "policy"


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

                    job_data = redis.hgetall(_job_key(job_id)) or {}
                    title = _job_title(job_data)
                    filename = job_data.get("filename") or ""
                    put_t0 = _now()
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
                    emit_activity(
                        f'Stitching "{title}" part {idx} completed in {_elapsed_ms(put_t0)}ms',
                        job_id=job_id,
                        filename=filename,
                        stage='stitch',
                        source='worker',
                    )
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
LOCK_NAME = "transcode-video-lock"

@huey.task(retries=999999, retry_delay=5)        # keep retrying while another run holds the lock
@huey.lock_task(LOCK_NAME)     # <- must be the innermost decorator
def transcode(job_id: str, file_path: str):
    """
    Orchestration entrypoint:
      - Kick off stitch(job_id) so the stitcher publishes stitch_host.
      - Run split(job_id, file_path) to segment and dispatch encodes.
    """
    _reset_job_run_state(job_id)
    # Start stitcher first so encoders have a destination
    stitch(job_id)
    # Perform split/dispatch on this node (master)
    return split(job_id, file_path)


@huey.task()
def split(job_id: str, file_path: str):
    """
    Master-side segmentation & dispatch:
      - Start HTTP server (serve parts).
      - Probe source, set metadata, advertise master_host.
      - Segment source and dispatch encode(job_id, idx, ...) for each segment.
      - No waiting or stitching here anymore.
    """
    job_key = _job_key(job_id)
    job = redis.hgetall(job_key)
    title = _job_title(job)

    logger.info(f"[{job_id}] Starting transcode job")

    try:
        _start_http_once()  # HTTP server for parts

        target_height = _normalize_target_height(job.get("target_height"))

        # Resolve input path. Newer jobs may carry an explicit source path
        # (for example /source_media) while legacy jobs derive it from /watch.
        src_path = (job.get('input_path') or '').strip() or file_path
        if not os.path.exists(src_path):
            if file_path and os.path.exists(file_path):
                src_path = file_path
            else:
                filename = job.get('filename') or ''
                alt = os.path.join(WATCH_ROOT, filename.lstrip('/'))
                if os.path.exists(alt):
                    src_path = alt
        if not os.path.exists(src_path):
            redis.hset(job_key, 'status', Status.FAILED.value)
            return {'status': 'FAILED', 'reason': f'input not found: {src_path}'}

        # Mark as RUNNING & remember input path
        now = _now()
        redis.hset(job_key, mapping={
            'status': Status.RUNNING.value,
            'started_at': now,
            'input_path': src_path,
            'target_height': target_height,
        })

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
        redis.hset(job_key, mapping={
            'source_codec': codec,
            'source_resolution': f"{width}x{height}" if (width and height) else '',
            'source_duration': f"{duration:.2f}" if duration else '0',
            'source_fps': f"{fps:.2f}" if fps else '0',
            'source_file_size': size_b,
            'source_bitrate_kbps': f"{kbps_calc:.0f}" if kbps_calc>0 else '0'
        })

        # --- FAIL FAST: reject AV1 sources ---
        # Normalize codec name (ffprobe reports "av1")
        if (codec or "").strip().lower() in {"av1", "av01"}:
            msg = "Unsupported input codec: AV1. Rejected."
            logger.error(f"[{job_id}] {msg}")
            # Mark job rejected and store a human-friendly reason
            redis.hset(job_key, mapping={
                'status': Status.REJECTED.value,
                'error': msg,
                'rejected_reason': 'av1_rejected',
                'rejected_at': str(_now()),
            })
            return {'status': 'REJECTED', 'reason': msg}


        # Advertise master URL
        master_host_name = ENV("HOSTNAME") or socket.gethostname()
        master_url = f"http://{master_host_name}:{HTTP_PORT}"
        redis.hset(job_key, 'master_host', master_url)

        try:
            v_sel = int(job.get("selected_v_stream") or 0)
        except Exception:
            v_sel = 0
        try:
            a_sel = int(job.get("selected_a_stream") or 0)
        except Exception:
            a_sel = 0
        logger.info(f"[{job_id}] Video stream: {v_sel}")
        logger.info(f"[{job_id}] Audio stream: {a_sel}")

        processing_mode, processing_reason = _resolve_processing_mode(job_id, job, int(size_b or 0))
        redis.hset(job_key, mapping={
            "processing_mode_effective": processing_mode,
            "processing_mode_reason": processing_reason,
        })
        logger.info(f"[{job_id}] Processing mode: {processing_mode} ({processing_reason})")
        if processing_mode == "direct" and duration <= 0:
            processing_mode = "split"
            processing_reason = "missing_source_duration"
            redis.hset(job_key, mapping={
                "processing_mode_effective": processing_mode,
                "processing_mode_reason": processing_reason,
            })
            logger.warning(f"[{job_id}] Falling back to split mode because source duration is unavailable.")

        split_settings = _load_global_settings()
        target_segment_mb, target_segment_bytes = _target_segment_bytes_from_settings(split_settings)

        # requested parts from configured segment size
        requested_parts = _parts_for_target_size(int(size_b or 0), target_segment_bytes)
        if requested_parts <= 0:
            requested_parts = 100
            logger.warning(f"[{job_id}] Source size unavailable; falling back to {requested_parts} requested parts.")

        # estimate usable encoder workers: active workers minus reserved master/stitcher nodes
        stitch_endpoint = redis.hget(job_key, "stitch_host") or ""
        if not stitch_endpoint:
            # stitch task is enqueued first; give it a brief chance to publish stitch_host
            deadline = _now() + 3.0
            while _now() < deadline and not stitch_endpoint:
                if _is_job_halted(job_id):
                    break
                stitch_endpoint = redis.hget(job_key, "stitch_host") or ""
                if stitch_endpoint:
                    break
                time.sleep(0.1)

        master_host_lc = str(master_host_name or "").strip().lower()
        stitch_host_lc = _host_from_endpoint(stitch_endpoint)
        reserved_hosts = {h for h in (master_host_lc, stitch_host_lc) if h}

        active_hosts = {str(h or "").strip().lower() for h in _active_nodes() if str(h or "").strip()}
        if not active_hosts:
            try:
                warmup_hosts = json.loads(job.get("warmup_workers_json") or "[]")
            except Exception:
                warmup_hosts = []
            if isinstance(warmup_hosts, list):
                active_hosts = {str(h or "").strip().lower() for h in warmup_hosts if str(h or "").strip()}

        if active_hosts:
            usable_encoder_workers = max(0, len(active_hosts - reserved_hosts))
        else:
            # fallback heuristic when host-level visibility is unavailable
            try:
                warmup_count = int(job.get("warmup_worker_count") or 0)
            except Exception:
                warmup_count = 0
            usable_encoder_workers = max(0, warmup_count - len(reserved_hosts))

        # effective parts / segment size:
        # - guarantee at least one segment per usable encoder worker
        # - when above that, round up to a full multiple of usable workers
        effective_parts = requested_parts
        if usable_encoder_workers > 0:
            if requested_parts <= usable_encoder_workers:
                effective_parts = usable_encoder_workers
            else:
                effective_parts = int(ceil(float(requested_parts) / float(usable_encoder_workers)) * usable_encoder_workers)

        P = max(1, effective_parts)
        effective_segment_bytes = max(1, int(ceil(float(size_b) / float(P)))) if int(size_b or 0) > 0 else target_segment_bytes
        effective_segment_mb = float(effective_segment_bytes) / float(1024 * 1024)
        redis.hset(job_key, mapping={
            "requested_segment_size_mb": f"{target_segment_mb:.6f}",
            "requested_segment_size_bytes": int(target_segment_bytes),
            "effective_segment_size_mb": f"{effective_segment_mb:.6f}",
            "effective_segment_size_bytes": int(effective_segment_bytes),
            "requested_parts": int(requested_parts),
            "effective_parts": int(P),
            "usable_encoder_workers": int(usable_encoder_workers),
        })
        if processing_mode == "direct":
            segment_duration_hint = max(1.0, float(duration) / float(P)) if duration else max(1.0, float(job.get("segment_duration") or 10))
            redis.hset(job_key, 'direct_segment_duration', f"{segment_duration_hint:.6f}")
        else:
            segment_duration_hint = max(1.0, float(duration) / float(P)) if duration else 10.0
            redis.hdel(job_key, 'direct_segment_duration')
        logger.info(
            f"[{job_id}] Parts plan: requested={requested_parts}, effective={P}, "
            f"usable_encoder_workers={usable_encoder_workers}, source_size={int(size_b or 0)} bytes, "
            f"requested_segment_mb={target_segment_mb:g}, effective_segment_mb={effective_segment_mb:.3f}, "
            f"reserved_hosts={sorted(reserved_hosts)}"
        )
        emit_activity(
            f'Splitting "{title}" into {P} parts',
            job_id=job_id,
            filename=job.get('filename'),
            stage='split',
            source='worker',
        )

        seg_t0 = _now()
        emit_activity(
            f'Segmenting "{title}" started',
            job_id=job_id,
            filename=job.get('filename'),
            stage='segment_start',
            source='worker',
        )
        redis.hset(job_key, mapping={'parts_total': P, 'parts_done': 0, 'segmented_chunks': 0, 'segment_progress': 0, 'segment_elapsed': 0})

        if processing_mode == "direct":
            total_chunks = 0
            part_opened_at = _now()
            for part_idx in range(1, P + 1):
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] Halted while scheduling direct-source parts.")
                    return {'status': 'ABORTED'}

                start_s, part_duration = _direct_source_plan_for_part(duration, segment_duration_hint, part_idx)
                if start_s is None:
                    break

                total_chunks = part_idx
                part_elapsed_ms = _elapsed_ms(part_opened_at)
                part_opened_at = _now()
                emit_activity(
                    f'Segmenting "{title}" part {part_idx} completed in {part_elapsed_ms}ms',
                    job_id=job_id,
                    filename=job.get('filename'),
                    stage='segment',
                    source='worker',
                )
                encode(
                    job_id,
                    part_idx,
                    master_url,
                    v_sel,
                    a_sel,
                    source_path=src_path,
                    source_start_s=start_s,
                    source_duration_s=part_duration,
                )
                prog = int((part_idx / max(1, P)) * 100)
                if part_idx < P:
                    prog = min(prog, 99)
                redis.hset(job_key, mapping={
                    'segmented_chunks': part_idx,
                    'segment_progress': prog,
                    'segment_elapsed': round(_now() - seg_t0, 2),
                })

            if total_chunks <= 0:
                redis.hset(job_key, 'status', Status.FAILED.value)
                return {'status': 'FAILED', 'reason': 'no segments produced'}

            redis.hset(job_key, mapping={
                'parts_total': total_chunks,
                'segmented_chunks': total_chunks,
                'segment_progress': 100,
                'segment_elapsed': round(_now() - seg_t0, 2),
            })
            emit_activity(
                f'Segmenting "{title}" completed in {_elapsed_ms(seg_t0)}ms',
                job_id=job_id,
                filename=job.get('filename'),
                stage='segment_complete',
                source='worker',
            )
            logger.info(f"[{job_id}] Direct-source scheduling complete; {total_chunks} parts queued for encoding")
            return {'status': 'QUEUED', 'parts': total_chunks}

        # Prepare parts dir + naming
        parts_dir = os.path.join(_job_base_dir(job_id, job), "parts")
        _ensure_dirs(parts_dir)
        temp_pattern = os.path.join(parts_dir, "chunk_%03d.ts")

        # Segment duration target from P (fallback 10s if unknown)
        segment_duration = int(max(1.0, float(duration) / float(P)) if (duration and P) else 10.0)
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
            '-f', 'segment',
            '-segment_time', f"{segment_duration:.6f}",
            '-reset_timestamps', '1',
            temp_pattern
        ]
        if bsf:
            cmd[cmd.index('-f'):cmd.index('-f')] = ['-bsf:v', bsf]
        logger.info(f"[{job_id}] Segment command: {' '.join(cmd)}")

        # Parse ffmpeg stderr for new-chunk openings
        segment_re = re.compile(r"Opening '(.+?/chunk_(\d+)\.ts)' for writing")

        if not _is_job_halted(job_id):
            process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        previous_chunk_path = None
        previous_chunk_index = None
        chunk_opened_at = None
        estimated_chunks = max(1, P)
        queued_count = 0

        segment_stderr_tail = deque(maxlen=120)
        while True:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] Halted. Terminating segmentation.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            line = process.stderr.readline()
            if not line:
                break
            segment_stderr_tail.append(line.rstrip())

            m = segment_re.search(line)
            if m:
                chunk_detected_at = _now()
                chunk_path = m.group(1)
                chunk_index = int(m.group(2))  # 0-based
                # update progress on opening next chunk
                try:
                    prog = int(((min(chunk_index + 1, estimated_chunks)) / max(1, estimated_chunks)) * 100)
                    prog = min(prog, 99)
                    redis.hset(job_key, mapping={'segment_progress': prog})
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
                        redis.hset(job_key, 'status', Status.FAILED.value)
                        return {'status': 'FAILED', 'reason': f'move part {part_idx} failed'}

                    queued_count = part_idx
                    try:
                        prog = int((queued_count / max(1, estimated_chunks)) * 100)
                        if queued_count < estimated_chunks:
                            prog = min(prog, 99)
                        redis.hset(job_key, mapping={
                            'segmented_chunks': queued_count,
                            'segment_progress': prog,
                            'segment_elapsed': round(_now() - seg_t0, 2),
                        })
                    except Exception:
                        pass

                    if not _is_job_halted(job_id):
                        logger.info(f"[{job_id}] Split part {part_idx}")
                        part_elapsed_ms = max(0, int(round((chunk_detected_at - (chunk_opened_at or chunk_detected_at)) * 1000)))
                        emit_activity(
                            f'Segmenting "{title}" part {part_idx} completed in {part_elapsed_ms}ms',
                            job_id=job_id,
                            filename=job.get('filename'),
                            stage='segment',
                            source='worker',
                        )
                        encode(job_id, part_idx, master_url, v_sel, a_sel)

                previous_chunk_path = chunk_path
                previous_chunk_index = chunk_index
                chunk_opened_at = chunk_detected_at

        process.wait()
        if process.returncode != 0:
            err_tail = "\n".join(segment_stderr_tail).strip()
            reason = f"ffmpeg segmentation failed (rc={process.returncode})"
            if err_tail:
                reason = f"{reason}: {err_tail[-1200:]}"
            logger.error(f"[{job_id}] {reason}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'failed_stage': 'segment',
                'failed_worker': master_host_name,
            })
            return {'status': 'FAILED', 'reason': reason}

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
                redis.hset(job_key, 'status', Status.FAILED.value)
                return {'status': 'FAILED', 'reason': f'move part {part_idx} failed'}

            if not _is_job_halted(job_id):
                logger.info(f"[{job_id}] Split last part {part_idx}")
                part_elapsed_ms = _elapsed_ms(chunk_opened_at or _now())
                emit_activity(
                    f'Segmenting "{title}" part {part_idx} completed in {part_elapsed_ms}ms',
                    job_id=job_id,
                    filename=job.get('filename'),
                    stage='segment',
                    source='worker',
                )
                encode(job_id, part_idx, master_url, v_sel, a_sel)
                total_chunks = part_idx

        if total_chunks <= 0:
            redis.hset(job_key, 'status', Status.FAILED.value)
            return {'status': 'FAILED', 'reason': 'no segments produced'}

        # Finalize segmentation metadata
        redis.hset(job_key, mapping={
            'parts_total': total_chunks,
            'segmented_chunks': total_chunks,
            'segment_progress': 100,
            'segment_elapsed': round(_now() - seg_t0, 2),
        })

        # Master exits — stitcher will finish the job
        logger.info(f"[{job_id}] Segmentation complete; {total_chunks} parts queued for encoding")
        emit_activity(
            f'Segmenting "{title}" completed in {_elapsed_ms(seg_t0)}ms',
            job_id=job_id,
            filename=job.get('filename'),
            stage='segment_complete',
            source='worker',
        )
        return {'status': 'QUEUED', 'parts': total_chunks}

    except Exception as e:
        logger.exception(f"[{job_id}] split failed")
        redis.hset(job_key, 'status', Status.FAILED.value)
        return {'status': 'FAILED', 'error': str(e)}


@huey.task()
def encode(
    job_id: str,
    idx: int,
    master_host: str,
    v_sel: int = 0,
    a_sel: int = 0,
    stitch_host: Optional[str] = None,
    source_path: Optional[str] = None,
    source_start_s: Optional[float] = None,
    source_duration_s: Optional[float] = None,
):
    """
    Worker task:
      - Either GET /job/<job_id>/part/<idx> from MASTER, or read source range directly
      - ffmpeg VAAPI transcode (monitored only to detect halt)
      - PUT /job/<job_id>/result/<idx> to STITCHER
      - After successful upload, update parts_done/completed_chunks and encode_progress
      - Early-exit if the job is halted at any point
    """
    logger.info(f"[{job_id}] Preparing to encode part {idx}")

    worker_name = ENV("HOSTNAME") or socket.gethostname()
    job_key = _job_key(job_id)
    filename = redis.hget(job_key, "filename") or ""
    title = _job_title({"filename": filename})
    part_t0 = _now()
    def _fail(reason: str, stage: str):
        # Mark whole job failed and save a useful reason
        mapping = {
            'status': Status.FAILED.value,
            'error': f"part {idx} ({stage}): {reason}",
            'failed_part': idx,
            'failed_stage': stage,
            'failed_worker': worker_name,
            'ended_at': _now(),
        }
        try:
            redis.hset(job_key, mapping=mapping)
        except Exception:
            pass
        logger.error(f"[{job_id}] {mapping['error']}")
        # best-effort cleanup
        for p in (locals().get('in_path'), locals().get('out_path')):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return {'status': 'FAILED', 'reason': mapping['error']}

    try:
        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted before start")
            return {'status': 'ABORTED'}

        import requests
        stage = "resolve-stitcher"

        # resolve stitch_host if not provided
        if not stitch_host:
            deadline = _now() + 60
            while _now() < deadline and not stitch_host:
                stitch_host = redis.hget(job_key, 'stitch_host') or None
                if stitch_host:
                    break
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] encode {idx}: halted while waiting for stitch_host")
                    return {'status': 'ABORTED'}
                time.sleep(0.25)
            if not stitch_host:
                stitch_host = master_host  # fallback keeps pipeline moving

        # Paths in local tmpfs (worker)
        wtmp = _job_base_dir(job_id)
        _ensure_dirs(wtmp)
        in_path = None
        in_path  = os.path.join(wtmp, f"in_{idx:03d}.ts")
        out_path = os.path.join(wtmp, f"out_{idx:03d}.mp4")
        direct_source = bool(source_path) and source_start_s is not None and source_duration_s is not None
        if not direct_source:
            # Download part from master
            stage = "download"
            get_url = f"{master_host.rstrip('/')}/job/{job_id}/part/{idx}"
            logger.info(f"[{job_id}] GET part {idx} from {get_url}")

            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] encode {idx}: halted before downloading part")
                return {'status': 'ABORTED'}

            try:
                with requests.get(get_url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(in_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                            if _is_job_halted(job_id):
                                logger.warning(f"[{job_id}] encode {idx}: halted during download")
                                try:
                                    os.remove(in_path)
                                except Exception:
                                    pass
                                return {'status': 'ABORTED'}
            except Exception as e:
                return _fail(f"download failed: {e}", stage)

            logger.info(f"[{job_id}] Downloaded part {idx}")
        else:
            logger.info(
                f"[{job_id}] Direct-source encode part {idx} "
                f"start={float(source_start_s or 0):.3f}s dur={float(source_duration_s or 0):.3f}s source={source_path}"
            )

        # Build ffmpeg command
        stage = "encode"
        software_encode = int(redis.hget(job_key, 'software_encode') or 0)
        target_height, force_deinterlace = _effective_target_height_for_job(job_key)
        try:
            redis.hset(job_key, mapping={
                'target_height_effective': target_height,
                'deinterlace_effective': '1' if force_deinterlace else '0',
            })
        except Exception:
            pass
        input_args = ['-fflags', '+genpts', '-i', in_path]
        map_args = ['-map', '0:v:0', '-map', '0:a?']
        if direct_source:
            input_args = [
                '-ss', f"{float(source_start_s or 0):.6f}",
                '-i', source_path,
                '-t', f"{max(0.05, float(source_duration_s or 0.0)):.6f}",
            ]
            map_args = [
                '-map', f'0:v:{int(v_sel)}',
                '-map', f'0:a:{int(a_sel)}?',
                '-sn', '-dn',
                '-map_metadata', '-1',
                '-map_chapters', '-1',
            ]
        if software_encode:
            cmd = [
                'ffmpeg','-hide_banner','-nostats','-loglevel','error',
                '-y',
                '-progress','pipe:2',
                *input_args,
                *map_args,
                '-vf', _reset_segment_video_pts_filter(
                    _software_scale_filter(target_height, deinterlace=force_deinterlace)
                ),
                '-c:v','libx264','-preset','veryfast','-crf','23',
                *AUDIO_ARGS.split(),
                out_path
            ]
        else:
            cmd = [
                'ffmpeg','-hide_banner','-nostats','-loglevel','error',
                '-y',
                '-progress','pipe:2',
                '-vaapi_device', VAAPI_DEVICE,
                *input_args,
                *map_args,
                '-vf', _reset_segment_video_pts_filter(_vaapi_scale_filter(target_height)),
                '-c:v','h264_vaapi',
                '-rc_mode', VAAPI_RC_MODE,
                '-qp', VAAPI_QP,
                *AUDIO_ARGS.split(),
                out_path
            ]

        logger.info(f"[{job_id}] encode {idx}: {' '.join(cmd)}")

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted before encoding part")
            return {'status': 'ABORTED'}

        encode_started = int(redis.hget(job_key, 'encode_started') or 0)
        if encode_started == 0:
            encode_started = int(_now())
            redis.hset(job_key, 'encode_started', encode_started)
            if redis.set(f"{job_key}:encode_stage_started", "1", nx=True, ex=7 * 24 * 3600):
                emit_activity(
                    f'Encoding "{title}" started',
                    job_id=job_id,
                    filename=filename,
                    stage='encode_start',
                    source='worker',
                )

        # capture last ~120 lines of ffmpeg stderr for diagnostics
        stderr_tail = deque(maxlen=120)

        try:
            pr = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except Exception as e:
            return _fail(f"spawn ffmpeg failed: {e}", stage)

        try:
            # Read progress lines to keep ffmpeg responsive and check halts
            if pr.stderr is not None:
                for _line in pr.stderr:
                    line = _line.rstrip("\n")
                    if line:
                        stderr_tail.append(line)
                    if _is_job_halted(job_id):
                        logger.warning(f"[{job_id}] encode {idx}: halted — terminating ffmpeg")
                        pr.terminate()
                        try:
                            pr.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pr.kill()
                        try:
                            if os.path.exists(out_path): os.remove(out_path)
                        except Exception:
                            pass
                        return {'status': 'ABORTED'}
        finally:
            try:
                if pr.stderr: pr.stderr.close()
            except Exception:
                pass

        rc = pr.wait()
        if rc != 0 or not os.path.exists(out_path):
            tail = "\n".join(list(stderr_tail)[-40:])  # keep it short
            return _fail(f"ffmpeg rc={rc}; tail:\n{tail}", stage)

        logger.info(f"[{job_id}] Encoded part {idx}")

        # Upload result to stitcher
        stage = "upload"
        put_url = f"{stitch_host.rstrip('/')}/job/{job_id}/result/{idx}"
        logger.info(f"[{job_id}] PUT result {idx} to {put_url}")

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted before uploading encoded part")
            try: os.remove(out_path)
            except Exception: pass
            return {'status': 'ABORTED'}

        try:
            with open(out_path, 'rb') as f:
                headers = {'Content-Type':'video/mp4', 'Content-Length': str(os.path.getsize(out_path))}
                r = requests.put(put_url, data=f, headers=headers, timeout=120)
                if r.status_code // 100 != 2:
                    return _fail(f"PUT failed {r.status_code}: {r.text[:300]}", stage)
        except Exception as e:
            return _fail(f"upload failed: {e}", stage)

        logger.info(f"[{job_id}] Uploaded part {idx}")
        emit_activity(
            f'Encoding "{title}" part {idx} completed in {_elapsed_ms(part_t0)}ms',
            job_id=job_id,
            filename=filename,
            stage='encode',
            source='worker',
        )

        # Cleanup local tmpfs
        cleanup_paths = [out_path]
        if not direct_source:
            cleanup_paths.append(in_path)
        for p in cleanup_paths:
            try: os.remove(p)
            except Exception: pass

        # ------- Commit part & update encode stats (one-shot at completion) -------
        try:
            done_set = f"job_done_parts:{job_id}"
            added = redis.sadd(done_set, idx)  # 1 if new, 0 if already present
            if added:
                pipe = redis.pipeline()
                pipe.hincrby(job_key, 'completed_chunks', 1)
                pipe.hincrby(job_key, 'parts_done', 1)
                pipe.execute()

            # encode_elapsed (compute once based on first encode_started)
            encode_elapsed = int(_now() - encode_started)
            redis.hset(job_key, 'encode_elapsed', encode_elapsed)

            # encode_progress = int(parts_done / parts_total * 100)
            try:
                total = int(redis.hget(job_key, 'parts_total') or 0)
                done  = int(redis.hget(job_key, 'parts_done') or 0)
            except Exception:
                total, done = 0, 0
            if total > 0:
                prog = int((done / total) * 100)
                # only move forward
                cur = int(redis.hget(job_key, 'encode_progress') or 0)
                if prog > cur:
                    redis.hset(job_key, 'encode_progress', prog)
                if done >= total and redis.set(f"{job_key}:encode_stage_complete", "1", nx=True, ex=7 * 24 * 3600):
                    stage_elapsed_ms = _elapsed_ms(encode_started or part_t0)
                    emit_activity(
                        f'Encoding "{title}" completed in {stage_elapsed_ms}ms',
                        job_id=job_id,
                        filename=filename,
                        stage='encode_complete',
                        source='worker',
                    )

        except Exception as e:
            logger.error(f"[{job_id}] encode {idx} progress update error: {e}")

        return {'status':'COMPLETED','idx': idx}

    except Exception as e:
        # Catch-all — fail job with a generic reason
        return _fail(f"unexpected error: {e}", stage if 'stage' in locals() else 'unknown')



@huey.task()
def stitch(job_id: str):
    """
    Stitcher node:
      - Serves PUTs for encoded parts and advertises stitch_host.
      - Waits for all parts with conservative, head-of-line-focused retries.
      - On persistent misses/timeouts -> fail. On success -> concat-copy to final.
    """
    job_key = _job_key(job_id)
    stitch_stage_t0 = _now()
    initial_job = redis.hgetall(job_key) or {}
    title = _job_title(initial_job)
    emit_activity(
        f'Stitching "{title}" started',
        job_id=job_id,
        filename=initial_job.get('filename'),
        stage='stitch_start',
        source='worker',
    )
    try:
        _start_http_once()
        stitch_host = f"http://{ENV('HOSTNAME') or socket.gethostname()}:{HTTP_PORT}"
        redis.hset(job_key, mapping={'stitch_host': stitch_host})
        logger.info(f"[{job_id}] Stitcher ready at {stitch_host}")

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] stitch: job already halted; exiting")
            return {'status':'ABORTED'}

        # ---- Tunables (safe defaults; can be overriden via env) ----
        MAX_RETRIES                    = int(ENV("STITCH_MAX_RETRIES", "3"))
        RETRY_INTERVAL_SEC             = float(ENV("STITCH_RETRY_INTERVAL_SEC", "45"))
        STALL_BEFORE_RETRY_SEC         = float(ENV("STITCH_STALL_BEFORE_RETRY_SEC", "90"))
        MISS_MIN_AGE_SEC               = float(ENV("STITCH_MISS_MIN_AGE_SEC", "90"))
        RETRY_WINDOW_AHEAD             = int(ENV("STITCH_RETRY_WINDOW_AHEAD", "8"))   # how far past the contiguous frontier we consider
        MAX_PARALLEL_REDISPATCH        = int(ENV("STITCH_MAX_PARALLEL_REDISPATCH", "3"))

        # ---- Wait for parts_total ----
        deadline = _now() + 300
        P = 0
        while _now() < deadline:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] stitch aborted before parts_total")
                return {'status':'ABORTED'}

            try:
                P = int(redis.hget(job_key, 'parts_total') or 0)
                if P > 0:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if P <= 0:
            logger.error(f"[{job_id}] stitch: parts_total not set")
            return {'status':'FAILED','reason':'parts_total not set'}

        enc_dir = os.path.join(_job_base_dir(job_id), "encoded")
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
                # consider file ready if stable for a short moment
                if st.st_size > 0 and (now_ts - st.st_mtime) > 0.8:
                    i = int(m.group(1))
                    if 1 <= i <= P:
                        ready.add(i)
            return ready

        # Keys to persist state across restarts
        retry_cnt_key   = f"job_retry_counts:{job_id}"        # H[idx] = int
        retry_ts_key    = f"job_retry_ts:{job_id}"            # H[idx] = epoch float
        miss_seen_key   = f"job_missing_first_seen:{job_id}"  # H[idx] = epoch float
        inflight_key    = f"job_retry_inflight:{job_id}"      # S = {idx,...}

        expected = set(range(1, P + 1))
        try:
            src_dur = float(redis.hget(job_key, 'source_duration') or 0.0)
        except Exception:
            src_dur = 0.0

        # Overall deadline (unchanged heuristic)
        wait_deadline = _now() + max(300.0, (src_dur or 0) * 3)
        est_part_secs = max(5.0, (src_dur / P) if (src_dur and P) else 10.0)

        # Stall detection: only retry if nothing new arrived for a while
        last_ready_count = -1
        last_change_ts   = _now()

        # Helper: schedule (re)encode conservatively
        def _retry_part(idx: int):
            job = redis.hgetall(job_key) or {}
            master_host = job.get('master_host') or stitch_host
            source_path = (job.get('input_path') or '').strip()
            try:
                v_sel = int(job.get('selected_v_stream') or 0)
            except Exception:
                v_sel = 0
            try:
                a_sel = int(job.get('selected_a_stream') or 0)
            except Exception:
                a_sel = 0
            try:
                seg_dur = float(job.get('direct_segment_duration') or job.get('segment_duration') or 10.0)
            except Exception:
                seg_dur = 10.0

            # Avoid duplicate redispatch if already inflight
            added = redis.sadd(inflight_key, idx)
            if not added:
                return False  # already inflight from a prior retry

            logger.warning(f"[{job_id}] stitch: re-dispatching part {idx}")
            if str(job.get("processing_mode_effective") or job.get("processing_mode") or "split").strip().lower() == "direct":
                start_s, part_dur = _direct_source_plan_for_part(src_dur, seg_dur, idx)
                if start_s is None or part_dur is None or not source_path:
                    logger.error(f"[{job_id}] stitch: cannot retry direct-source part {idx}; missing segment plan")
                    redis.srem(inflight_key, idx)
                    return False
                encode(
                    job_id,
                    idx,
                    master_host,
                    v_sel,
                    a_sel,
                    stitch_host=stitch_host,
                    source_path=source_path,
                    source_start_s=start_s,
                    source_duration_s=part_dur,
                )
            else:
                encode(job_id, idx, master_host, v_sel, a_sel, stitch_host=stitch_host)

            now_ts = _now()
            pipe = redis.pipeline()
            pipe.hincrby(retry_cnt_key, idx, 1)
            pipe.hset(retry_ts_key, idx, now_ts)
            pipe.execute()
            return True

        while _now() < wait_deadline:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] stitch aborted (job halted)")
                return {'status':'ABORTED'}

            ready = _ready_set()
            done_fs = len(ready)

            # Clear inflight markers for any that have arrived
            try:
                if ready:
                    redis.srem(inflight_key, *list(ready))
            except Exception:
                pass

            # progress mirroring
            try:
                cur = int(redis.hget(job_key, 'encode_progress') or 0)
                prog = int((done_fs / P) * 100) if P else 0
                if prog > cur:
                    redis.hset(job_key, 'encode_progress', prog)
                redis.hset(job_key, 'parts_done', done_fs)
            except Exception:
                pass

            # stall tracking
            if done_fs != last_ready_count:
                last_ready_count = done_fs
                last_change_ts   = _now()

            # update P if master corrected it
            try:
                P_current = int(redis.hget(job_key, 'parts_total') or P)
                if P_current != P and P_current > 0:
                    expected = set(range(1, P_current + 1))
                    P = P_current
            except Exception:
                pass

            if done_fs >= P:
                break

            # --- Conservative retry strategy ---
            now_ts = _now()
            # Find largest contiguous ready prefix (head-of-line)
            frontier = 0
            for i in range(1, P + 1):
                if i in ready:
                    frontier = i
                else:
                    break

            # Only consider a small window beyond the frontier
            horizon = min(P, frontier + RETRY_WINDOW_AHEAD)
            try:
                max_segmented = int(redis.hget(job_key, 'segmented_chunks') or 0)
            except Exception:
                max_segmented = 0
            # Never retry a part that has not been segmented yet, or we'll trigger 404 on master.
            if max_segmented > 0:
                horizon = min(horizon, max_segmented)
            window_missing = [i for i in range(frontier + 1, horizon + 1) if i not in ready]

            # Record first-seen-missing timestamps
            if window_missing:
                pipe = redis.pipeline()
                for idx in window_missing:
                    # set if absent
                    if not redis.hexists(miss_seen_key, idx):
                        pipe.hset(miss_seen_key, idx, now_ts)
                pipe.execute()

            # Retry only if globally stalled
            stalled = (now_ts - last_change_ts) >= STALL_BEFORE_RETRY_SEC

            dispatched = 0
            if stalled and window_missing:
                for idx in window_missing:
                    if dispatched >= MAX_PARALLEL_REDISPATCH:
                        break

                    try:
                        first_seen = float(redis.hget(miss_seen_key, idx) or 0.0)
                    except Exception:
                        first_seen = 0.0
                    # per-part age guard
                    if first_seen <= 0 or (now_ts - first_seen) < max(MISS_MIN_AGE_SEC, est_part_secs * 1.5):
                        continue

                    # retry budget & spacing
                    try:
                        cnt = int(redis.hget(retry_cnt_key, idx) or 0)
                    except Exception:
                        cnt = 0
                    try:
                        last_retry = float(redis.hget(retry_ts_key, idx) or 0.0)
                    except Exception:
                        last_retry = 0.0

                    if cnt >= MAX_RETRIES:
                        continue
                    if (now_ts - last_retry) < RETRY_INTERVAL_SEC:
                        continue

                    if _retry_part(idx):
                        dispatched += 1

            # Early-fail if some window-missing parts exhausted retries and have been stale well beyond expectation
            early_fail = False
            for idx in window_missing:
                try:
                    cnt = int(redis.hget(retry_cnt_key, idx) or 0)
                    last_retry = float(redis.hget(retry_ts_key, idx) or 0.0)
                    first_seen = float(redis.hget(miss_seen_key, idx) or 0.0)
                except Exception:
                    cnt, last_retry, first_seen = 0, 0.0, 0.0

                if cnt >= MAX_RETRIES:
                    # give extra grace after last retry proportional to part estimate
                    if (now_ts - max(last_retry, first_seen)) > max(2 * est_part_secs, STALL_BEFORE_RETRY_SEC):
                        early_fail = True
                        logger.error(f"[{job_id}] stitch: giving up on part {idx} after {cnt} retries")
                        break

            if early_fail:
                break

            # pacing
            time.sleep(0.5 if dispatched == 0 else 1.0)

        # Final readiness check
        final_ready = _ready_set()
        if len(final_ready) < P:
            missing = sorted(expected - final_ready)
            logger.error(f"[{job_id}] stitch timeout or retries exhausted; missing: {missing}")
            redis.hset(job_key, 'status', Status.FAILED.value)
            try:
                redis.delete(f"job_done_parts:{job_id}")
                redis.delete(retry_cnt_key)
                redis.delete(retry_ts_key)
                redis.delete(miss_seen_key)
                redis.delete(inflight_key)
            except Exception:
                pass
            return {'status':'FAILED','reason':'missing encoded parts after conservative retries'}

        # ---- Stitch (concat copy) ----
        base_dir = _job_base_dir(job_id)
        concat_path = os.path.join(base_dir, "concat.txt")
        enc_paths = [os.path.join(enc_dir, f"enc_{i:03d}.mp4") for i in range(1, P + 1)]
        with open(concat_path, 'w') as f:
            for pth in enc_paths:
                f.write(f"file '{pth}'\n")

        local_out = os.path.join(base_dir, f"job_{job_id}_output.mp4")
        combine_t0 = _now()
        redis.hset(job_key, mapping={'combine_progress': 0, 'combine_elapsed': 0})

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

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] stitch halted before stitching")
            return {'status': 'ABORTED'}

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
                    if _is_job_halted(job_id):
                        logger.warning(f"[{job_id}] stitch halted while stitching")
                        proc.terminate()
                        proc.wait()
                        return {'status': 'ABORTED'}
                    try:
                        out_ms = int(line.split('=', 1)[1] or 0)
                    except Exception:
                        out_ms = 0
                    if total_us > 0:
                        pct = int(min(100, max(0, (out_ms / total_us) * 100)))
                        pct = min(pct, 99)
                        redis.hset(job_key, mapping={
                            'combine_progress': pct,
                            'combine_elapsed': round(_now() - combine_t0, 2),
                        })
                    else:
                        redis.hset(job_key, 'combine_elapsed', round(_now() - combine_t0, 2))
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
            redis.hset(job_key, 'status', Status.FAILED.value)
            return {'status': 'FAILED', 'reason': 'stitch failed'}

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] stitch halted before moving file")
            return {'status': 'ABORTED'}

        # Decide whether to remux English subtitles from the source into the final file.
        job = redis.hgetall(job_key) or {}
        src_filename = job.get('filename') or os.path.basename(job.get('input_path','') or '')
        src_input_path = (job.get('input_path') or '').strip()
        english_subtitle_streams = _source_english_subtitle_streams(src_input_path)
        emit_activity(
            f'Writing "{_job_title(job)}"',
            job_id=job_id,
            filename=src_filename,
            stage='write',
            source='worker',
        )
        final_extension = ".mkv" if english_subtitle_streams else ".mp4"
        final_path = _final_output_path_with_ext(src_filename, final_extension)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        if english_subtitle_streams:
            subtitle_mux_path = os.path.join(base_dir, f"job_{job_id}_output_with_subs.mkv")
            subtitle_cmd = [
                "ffmpeg",
                "-hide_banner",
                "-nostats",
                "-loglevel",
                "error",
                "-y",
                "-i",
                local_out,
                "-i",
                src_input_path,
                "-map",
                "0:v",
                "-map",
                "0:a?",
                "-map",
                "1:s:m:language:eng?",
                "-map",
                "1:s:m:language:en?",
                "-c:v",
                "copy",
                "-c:a",
                "copy",
                "-c:s",
                "copy",
                subtitle_mux_path,
            ]
            logger.info(f"[{job_id}] Subtitle remux cmd: {' '.join(subtitle_cmd)}")
            subtitle_mux = subprocess.run(
                subtitle_cmd,
                capture_output=True,
                text=True,
                check=False,
            )
            if subtitle_mux.returncode != 0 or not os.path.exists(subtitle_mux_path):
                err_tail = ((subtitle_mux.stderr or "") or (subtitle_mux.stdout or ""))[-1200:]
                logger.error(f"[{job_id}] Subtitle remux failed (rc={subtitle_mux.returncode}): {err_tail}")
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': f"subtitle remux failed: {err_tail or subtitle_mux.returncode}",
                    'failed_stage': 'subtitle_mux',
                    'failed_worker': ENV("HOSTNAME") or socket.gethostname(),
                })
                return {'status': 'FAILED', 'reason': 'subtitle remux failed'}
            shutil.move(subtitle_mux_path, final_path)
        else:
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
            redis.hset(job_key, mapping={
                'dest_file_size': dst_size_b,
                'dest_duration': f"{dst_dur:.2f}" if dst_dur else '0',
                'dest_codec': dst_codec,
                'dest_resolution': f"{w}x{h}" if (w and h) else '',
                'dest_fps': f"{dst_fps:.2f}" if dst_fps else '0',
                'dest_bitrate_kbps': f"{dst_kbps:.0f}" if dst_kbps>0 else '0',
                'english_subtitles_kept': '1' if english_subtitle_streams else '0',
            })
        except Exception:
            pass

        # Cleanup project temp dir
        try:
            shutil.rmtree(_job_base_dir(job_id))
        except Exception:
            pass

        # Final combine progress update + cleanup of retry metadata
        redis.hset(job_key, mapping={
            'status': Status.DONE.value,
            'output_path': final_path,
            'ended_at': _now(),
            'combine_progress': 100,
            'combine_elapsed': round(_now() - combine_t0, 2),
        })
        emit_activity(
            f'Stitching "{_job_title(job)}" completed in {_elapsed_ms(stitch_stage_t0)}ms',
            job_id=job_id,
            filename=src_filename,
            stage='stitch_complete',
            source='worker',
        )
        try:
            redis.delete(f"job_done_parts:{job_id}")
            redis.delete(retry_cnt_key)
            redis.delete(retry_ts_key)
            redis.delete(miss_seen_key)
            redis.delete(inflight_key)
            redis.hdel(f"job:{job_id}", "awaiting_parts")
        except Exception:
            pass

        return {'status': 'COMPLETED', 'output': final_path}

    except Exception as e:
        logger.exception(f"[{job_id}] stitch failed")
        redis.hset(job_key, 'status', Status.FAILED.value)
        return {'status': 'FAILED', 'error': str(e)}

@huey.task()
def stamp(job_id: str):
    """
    Software re-encode with frame numbers burned-in for visual verification.

    Behavior:
      - Sets job status to STAMPING.
      - Runs ffmpeg with drawtext(text=%{n}) to burn frame numbers.
      - Streams -progress pipe:1 and mirrors to encode_progress / encode_elapsed.
      - On STOPPED/FAILED, terminates cleanly.
      - On success:
          * Moves tmp -> final stamped file (same dir as source, with ".stamped" suffix).
          * Updates THIS job to point to the stamped file (filename -> stamped path).
          * Creates a NEW READY job that also points to the stamped file.
    """

    job_key = _job_key(job_id)
    job = redis.hgetall(job_key) or {}

    # --------- helpers ----------
    def _fail(reason: str):
        logger.error(f"[{job_id}] STAMP failed: {reason}")
        try:
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'ended_at': _now(),
            })
        except Exception:
            pass
        return {'status': 'FAILED', 'reason': reason}

    # --------- resolve paths ----------
    # Prefer explicit input_path; fall back to WATCH_ROOT + filename for legacy jobs.
    filename = job.get('filename') or ''
    src_path = (job.get('input_path') or '').strip()
    if not src_path or not os.path.exists(src_path):
        alt = os.path.join(WATCH_ROOT, filename.lstrip('/')) if filename else ''
        if alt and os.path.exists(alt):
            src_path = alt
        else:
            return _fail(f"source not found: {filename or src_path or '(empty)'}")
    # If this was absolute to WATCH_ROOT, derive relative filename for stamped output.
    if src_path.startswith(WATCH_ROOT.rstrip('/') + '/'):
        filename = src_path[len(WATCH_ROOT.rstrip('/') + '/'):]

    # stamped path: same dir, ".stamped" suffix before extension
    base, ext = os.path.splitext(src_path)
    out_ext = ext if ext.lower() in ('.mkv', '.mp4') else '.mkv'  # keep sane containers
    stamped_path = base + '.stamped' + out_ext
    tmp_path = stamped_path + '.tmp'

    # output container format (fixes the '.tmp' autodetect failure)
    if out_ext.lower() == '.mkv':
        out_fmt = 'matroska'
        out_tail = []  # matroska accepts almost anything
    else:
        out_fmt = 'mp4'
        out_tail = ['-movflags', '+faststart']

    # Drawtext: prefer a specific font if present, else monospace
    # Drawtext: bigger and centered
    fontsize = int(ENV('STAMP_FONTSIZE', '72'))  # override via env if desired
    fontfile = ENV('STAMP_FONTFILE', '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf')
    if os.path.isfile(fontfile):
        draw = (
            f"drawtext=fontfile={fontfile}:"
            f"fontsize={fontsize}:fontcolor=white:"
            f"borderw=5:bordercolor=black:"
            f"x=(w-tw)/2:y=(h-th)/2:text=%{{n}}"
        )
    else:
        # fallback: fontconfig alias
        draw = (
            "drawtext=font=monospace:"
            f"fontsize={fontsize}:fontcolor=white:"
            "borderw=5:bordercolor=black:"
            "x=(w-tw)/2:y=(h-th)/2:text=%{n}"
        )

    # Selected streams
    try:
        v_sel = int(job.get('selected_v_stream') or 0)
    except Exception:
        v_sel = 0
    try:
        a_sel = int(job.get('selected_a_stream') or 0)
    except Exception:
        a_sel = 0

    # Duration for progress %
    duration = _ffprobe_duration(src_path)
    total_us = int(max(0.0, duration) * 1_000_000)

    # Update job status -> STAMPING and zero bars
    t0 = _now()
    redis.hset(job_key, mapping={
        'status': Status.STAMPING.value,
        'encode_started': int(t0),
        'encode_elapsed': 0,
        'encode_progress': 0,
        'stamp_source': filename or src_path,
        'stamp_tmp': tmp_path,
    })

    # ffmpeg command
    cmd = [
        'ffmpeg', '-hide_banner', '-y',
        '-nostats', '-loglevel', 'error',
        '-progress', 'pipe:1',
        '-i', src_path,
        '-map', f'0:v:{v_sel}',
        '-vf', draw,
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '18',
        '-map', f'0:a:{a_sel}?',
        '-c:a', 'copy',
        *out_tail,
        '-f', out_fmt,
        tmp_path
    ]
    logger.info(f"[{job_id}] STAMP cmd: {' '.join(cmd)}")

    # Early stop?
    if _is_job_halted(job_id):
        logger.warning(f"[{job_id}] STAMP halted before start")
        redis.hset(job_key, 'status', Status.STOPPED.value)
        return {'status': 'ABORTED'}

    err_tail = ''
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,   # -progress pipe:1
            stderr=subprocess.PIPE,   # capture tail for diagnostics
            text=True,
            bufsize=1,
        )
    except Exception as e:
        return _fail(f"spawn ffmpeg failed: {e}")

    try:
        # progress loop
        while True:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] STAMP halted — terminating ffmpeg")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
                redis.hset(job_key, 'status', Status.STOPPED.value)
                return {'status': 'ABORTED'}

            line = proc.stdout.readline() if proc.stdout else ''
            if not line:
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
                continue

            line = line.strip()
            if not line:
                continue

            if line.startswith('out_time_ms='):
                try:
                    out_ms = int(line.split('=', 1)[1] or '0')
                except Exception:
                    out_ms = 0
                if total_us > 0:
                    pct = int(min(99, max(0, (out_ms / total_us) * 100)))
                    redis.hset(job_key, mapping={
                        'encode_progress': pct,
                        'encode_elapsed': round(_now() - t0, 2),
                    })
            elif line.startswith('progress=') and line.split('=', 1)[1].strip() == 'end':
                break
    finally:
        rc = proc.wait()
        try:
            # keep a short diagnostic tail
            err = (proc.stderr.read() or '')
            err_tail = err[-1000:]
        except Exception:
            err_tail = ''

    if rc != 0 or not os.path.exists(tmp_path):
        # common pitfall fixed: missing -f with .tmp; also bubble any other errors
        reason = f"ffmpeg rc={rc}: {err_tail.strip() or 'no stderr'}"
        return _fail(reason)

    # Move tmp -> final stamped path (atomic replace)
    try:
        os.replace(tmp_path, stamped_path)
    except Exception as e:
        # clean tmp on error
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return _fail(f"finalize stamped file failed: {e}")

    # Finalize progress
    redis.hset(job_key, mapping={
        'encode_progress': 100,
        'encode_elapsed': round(_now() - t0, 2),
    })

    # Update THIS job to point at stamped file
    rel_stamped = os.path.relpath(stamped_path, WATCH_ROOT).lstrip(os.sep)
    now = str(_now())
    redis.hset(job_key, mapping={
        'filename': rel_stamped,
        'stamp_output': rel_stamped,
        'stamp_finished_at': now,
        # Return to READY so user can run pipeline on the stamped source,
        # but keep output metrics visible in the bars we just updated.
        'status': Status.READY.value,
    })

    # Create a NEW job for the stamped video (READY / paused)
    try:
        new_job_id = str(uuid.uuid4())
        new_job_key = _job_key(new_job_id)

        # Probe quick metadata for UI
        meta     = _ffprobe_stream0(stamped_path)
        duration = _ffprobe_duration(stamped_path)
        codec    = meta.get("codec") or ""
        width    = meta.get("width") or 0
        height   = meta.get("height") or 0
        fps      = meta.get("fps") or 0.0
        size_b   = meta.get("size") or 0
        fmt_bps  = meta.get("bit_rate") or 0
        kbps_calc = (fmt_bps/1000.0) if fmt_bps > 0 else (
            ((size_b*8)/duration/1000.0) if (size_b and duration) else 0.0
        )

        # carry over some settings or fall back to globals
        globals_map = redis.hgetall('settings:global') or {}
        def _ival(v, d):
            try: return int(v)
            except Exception: return d
        seg   = _ival(job.get('segment_duration', globals_map.get('segment_duration', 10)), 10)
        parts = _ival(job.get('number_parts', globals_map.get('number_parts', 2)), 2)
        serialize = '1' if (job.get('serialize_pipeline', globals_map.get('serialize_pipeline', '0')) in ('1','true','True')) else '0'
        target_height = _normalize_target_height(job.get('target_height'))

        new_mapping = {
            'job_id': new_job_id,
            'filename': rel_stamped,
            'status': Status.READY.value,
            'created_at': now,
            'started_at': '0',
            'total_chunks': 0,
            'completed_chunks': 0,
            'stitched_chunks': 0,
            'segment_duration': seg,
            'number_parts': parts,
            'serialize_pipeline': serialize,
            'target_height': target_height,
            'software_encode': '0',  # regular pipeline, not stamping
            'source_codec': codec,
            'source_resolution': f"{width}x{height}" if (width and height) else '',
            'source_duration': f"{duration:.2f}" if duration else '0',
            'source_fps': f"{fps:.2f}" if fps else '0',
            'source_file_size': size_b,
            'source_bitrate_kbps': f"{kbps_calc:.0f}" if kbps_calc>0 else '0',
            # default stream selections
            'selected_v_stream': 0,
            'selected_a_stream': 0,
        }
        redis.hset(new_job_key, mapping=new_mapping)
        # index for UI
        try:
            redis.sadd("jobs:all", new_job_key)
        except Exception:
            pass

        # record link
        redis.hset(job_key, 'stamp_new_job_id', new_job_id)

    except Exception as e:
        # non-fatal: stamping succeeded; just log job creation failure
        logger.warning(f"[{job_id}] stamped OK, but new job creation failed: {e}")

    logger.info(f"[{job_id}] STAMP completed → {stamped_path}")
    return {'status': 'COMPLETED', 'output': stamped_path}
