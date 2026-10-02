# worker/app/tasks.py
# Master-orchestrated pipeline with tmpfs + built-in HTTP server.
# - transcode(job_id, file_path): orchestration entrypoint (master) – queues split()
# - split(job_id, file_path): publishes parts_total, queues stitch(), segments & dispatches encodes
# - stitch(job_id): runs on any node ("stitcher") – receives encoded parts via HTTP PUT, stitches, writes final to NFS
# - encode(job_id, idx, master_host, ...): runs on any node; GETs from master, PUTs to stitcher
#
# Also includes probe_source() utilities.

from huey import RedisHuey

import os
import re
import time
import json
import csv
import glob
import uuid
import shutil
import socket
import logging
import subprocess
import select
import sys
import threading
from math import ceil
from typing import List, Tuple, Optional
from collections import deque

from common import ENV, Status, get_encode_huey, get_pipeline_huey, get_pipeline_huey_for_host, get_redis, get_logging, emit_activity, normalize_video_encoder, get_settings, as_bool
pipeline_huey = get_pipeline_huey()
encode_huey = get_encode_huey()
huey = pipeline_huey
_TARGET_PIPELINE_TASKS = {}
redis = get_redis()
logger = get_logging("worker")
WORKER_NAME = ENV("HOSTNAME") or socket.gethostname()
DISABLED_NODES_KEY = "nodes:disabled"
if redis.sismember(DISABLED_NODES_KEY, WORKER_NAME):
    logger.error("Worker node %s is disabled/quarantined; refusing to start Huey consumer.", WORKER_NAME)
    sys.exit(75)

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
VIDEO_ENCODER_DEFAULT = normalize_video_encoder(ENV("VEM_VIDEO_ENCODER", "h265"), "h265")
VAAPI_H264_ENCODER = ENV("VEM_VAAPI_H264_ENCODER", "h264_vaapi")
VAAPI_H265_ENCODER = ENV("VEM_VAAPI_H265_ENCODER", "hevc_vaapi")
SOFTWARE_H264_ENCODER = ENV("VEM_SOFTWARE_H264_ENCODER", "libx264")
SOFTWARE_H265_ENCODER = ENV("VEM_SOFTWARE_H265_ENCODER", "libx265")
SOFTWARE_H264_PRESET = ENV("VEM_SOFTWARE_H264_PRESET", "veryfast")
SOFTWARE_H265_PRESET = ENV("VEM_SOFTWARE_H265_PRESET", "fast")
SOFTWARE_H264_CRF = ENV("VEM_SOFTWARE_H264_CRF", "23")
SOFTWARE_H265_CRF = ENV("VEM_SOFTWARE_H265_CRF", "28")
DIRECT_SOURCE_REQUIRED_CODECS = {"vc1", "vc-1", "wmv3", "av1", "av01"}
DIRECT_SOURCE_REQUIRED_AUDIO_CODECS = {"truehd", "mlp", "flac", "alac"}
PART_FAILURE_MAX_RETRIES = int(ENV("PART_FAILURE_MAX_RETRIES", "5"))
SEGMENT_IO_IDLE_TIMEOUT_SEC = max(30, int(ENV("SEGMENT_IO_IDLE_TIMEOUT_SEC", "900")))
FFPROBE_TIMEOUT_SEC = max(10, int(ENV("FFPROBE_TIMEOUT_SEC", "120")))
JOB_HEARTBEAT_INTERVAL_SEC = max(2.0, float(ENV("JOB_HEARTBEAT_INTERVAL_SEC", "15")))
STITCH_HOST_READY_TIMEOUT_SEC = max(60.0, float(ENV("STITCH_HOST_READY_TIMEOUT_SEC", "1800")))
DIRECT_SOURCE_SEEK_PREROLL_SEC = max(0.0, float(ENV("DIRECT_SOURCE_SEEK_PREROLL_SEC", "30")))
AUDIO_MUX_MAX_TIMESTAMP_STRETCH_SEC = max(0.0, float(ENV("AUDIO_MUX_MAX_TIMESTAMP_STRETCH_SEC", "2.0")))
AUDIO_MUX_MAX_TIMESTAMP_STRETCH_PCT = max(0.0, float(ENV("AUDIO_MUX_MAX_TIMESTAMP_STRETCH_PCT", "0.00025")))
AUDIO_MUX_FRAME_COUNT_TOLERANCE_PCT = max(0.0, float(ENV("AUDIO_MUX_FRAME_COUNT_TOLERANCE_PCT", "0.001")))
AUDIO_MUX_FRAME_COUNT_TOLERANCE_FRAMES = max(0, int(ENV("AUDIO_MUX_FRAME_COUNT_TOLERANCE_FRAMES", "2")))
AUDIO_MUX_FRAME_COUNT_TIMEOUT_SEC = max(
    FFPROBE_TIMEOUT_SEC,
    int(ENV("AUDIO_MUX_FRAME_COUNT_TIMEOUT_SEC", "900")),
)

# Parts planning (used only as fallback if manager didn't hint)
PARTS_PER_WORKER = int(ENV("PARTS_PER_WORKER", "4"))
MIN_PARTS        = int(ENV("MIN_PARTS", "4"))
MAX_PARTS        = int(ENV("MAX_PARTS", "200"))
DEFAULT_TARGET_SEGMENT_MB = 10.0
DEFAULT_TARGET_SEGMENT_BYTES = max(1, int(DEFAULT_TARGET_SEGMENT_MB * 1024 * 1024))


# --------------- Helpers ----------------

def _job_key(job_id: str) -> str:
    return f"job:{job_id}"

_JOB_HEARTBEAT_CACHE: dict[str, float] = {}

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

def _terminal_status_raw(status_raw: str) -> bool:
    try:
        status = Status.parse(status_raw)
    except Exception:
        return False
    return status in (Status.FAILED, Status.REJECTED, Status.STOPPED, Status.DONE)

def _job_heartbeat(job_id: str, stage: str, *, force: bool = False, note: str = "") -> None:
    now = _now()
    stage = str(stage or "").strip() or "unknown"
    last = _JOB_HEARTBEAT_CACHE.get(job_id, 0.0)
    if not force and (now - last) < JOB_HEARTBEAT_INTERVAL_SEC:
        return
    try:
        if _terminal_status_raw(redis.hget(_job_key(job_id), "status")):
            return
    except Exception:
        pass
    mapping = {
        "last_heartbeat_at": str(now),
        "last_heartbeat_stage": stage,
        "last_heartbeat_host": WORKER_NAME,
    }
    if note:
        mapping["last_heartbeat_note"] = str(note)[:500]
    try:
        redis.hset(_job_key(job_id), mapping=mapping)
        _JOB_HEARTBEAT_CACHE[job_id] = now
    except Exception:
        pass

def _mark_part_inflight(job_id: str, idx: int) -> None:
    try:
        redis.sadd(f"job_retry_inflight:{job_id}", int(idx))
    except Exception:
        logger.exception("[%s] failed to mark part %s inflight", job_id, idx)

def _clear_part_inflight(job_id: str, idx: int) -> None:
    try:
        redis.srem(f"job_retry_inflight:{job_id}", int(idx))
    except Exception:
        logger.exception("[%s] failed to clear part %s inflight", job_id, idx)

def _dispatch_encode_part(job_id: str, idx: int, *args, **kwargs):
    _mark_part_inflight(job_id, idx)
    try:
        return encode(job_id, idx, *args, **kwargs)
    except Exception:
        _clear_part_inflight(job_id, idx)
        raise

def _quarantine_current_node(reason: str, *, job_id: str | None = None, part_idx: int | None = None) -> None:
    worker_name = WORKER_NAME
    now = _now()
    try:
        redis.sadd(DISABLED_NODES_KEY, worker_name)
        redis.hset(f"node:quarantine:{worker_name}", mapping={
            "hostname": worker_name,
            "reason": str(reason or "")[:1000],
            "job_id": job_id or "",
            "part_idx": "" if part_idx is None else str(part_idx),
            "quarantined_at": now,
        })
        redis.delete(f"metrics:node:{worker_name}")
    except Exception:
        logger.exception("Failed to quarantine worker node %s", worker_name)

def _stop_quarantined_worker() -> None:
    logger.error("Worker node %s is quarantined; exiting without restart.", WORKER_NAME)
    os._exit(75)

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
    disabled = set(redis.smembers("nodes:disabled") or [])
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
        if t >= cutoff and h not in disabled:
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

def _requires_direct_source_codec(codec: str) -> bool:
    return (codec or "").strip().lower() in DIRECT_SOURCE_REQUIRED_CODECS

def _audio_codec_requires_direct_source(codec: str) -> bool:
    codec = str(codec or "").strip().lower()
    return codec.startswith("pcm_") or codec in DIRECT_SOURCE_REQUIRED_AUDIO_CODECS

def _selected_audio_codec_from_job(job) -> str:
    try:
        selected = int((job or {}).get("selected_a_stream") or 0)
    except Exception:
        selected = 0
    try:
        streams = json.loads((job or {}).get("streams_json") or "{}")
    except Exception:
        streams = {}
    audio_streams = streams.get("audio") or []
    if 0 <= selected < len(audio_streams):
        return str((audio_streams[selected] or {}).get("codec") or "").strip().lower()
    return ""

def _requires_direct_source_audio(job) -> bool:
    return _audio_codec_requires_direct_source(_selected_audio_codec_from_job(job))

def _job_has_selected_audio(job) -> bool:
    try:
        selected = int((job or {}).get("selected_a_stream") or 0)
    except Exception:
        selected = 0
    try:
        streams = json.loads((job or {}).get("streams_json") or "{}")
    except Exception:
        streams = {}
    audio_streams = streams.get("audio") or []
    return 0 <= selected < len(audio_streams)

def _run_ffprobe(args: list[str]) -> Tuple[str, str, bool]:
    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT_SEC,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ("", f"ffprobe timed out after {FFPROBE_TIMEOUT_SEC}s", True)
    except Exception as exc:
        return ("", str(exc), False)

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        if detail:
            detail = detail[-600:]
        return ("", f"ffprobe rc={completed.returncode}{(': ' + detail) if detail else ''}", False)
    return (completed.stdout or "", "", False)

def _ffprobe_duration_details(input_path: str) -> Tuple[float, str, bool]:
    out, err, timed_out = _run_ffprobe([
        'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1', input_path
    ])
    if err:
        return (0.0, err, timed_out)
    try:
        return (float((out or '').strip()), "", False)
    except Exception as exc:
        return (0.0, f"ffprobe duration parse failed: {exc}", False)

def _ffprobe_duration(input_path: str) -> float:
    duration, _, _ = _ffprobe_duration_details(input_path)
    return duration

def _ffprobe_video_frame_count_details(
    input_path: str,
    *,
    stream_index: int = 0,
    heartbeat_stage: str = "audio_mux_frame_count",
    job_id: Optional[str] = None,
    timeout_sec: Optional[int] = None,
) -> Tuple[int, dict, str, bool]:
    try:
        stream_index = max(0, int(stream_index or 0))
    except Exception:
        stream_index = 0
    args = [
        'ffprobe', '-v', 'error',
        '-count_frames',
        '-select_streams', f'v:{stream_index}',
        '-show_entries', 'stream=nb_read_frames,nb_frames,duration,avg_frame_rate',
        '-of', 'json',
        input_path,
    ]
    if not job_id:
        out, err, timed_out = _run_ffprobe(args)
        if err:
            return (0, {}, err, timed_out)
    else:
        deadline = _now() + max(1, int(timeout_sec or AUDIO_MUX_FRAME_COUNT_TIMEOUT_SEC))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        while proc.poll() is None:
            _job_heartbeat(job_id, heartbeat_stage, note="ffprobe-count-frames")
            if _now() >= deadline:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                return (0, {}, f"ffprobe frame count timed out after {int(timeout_sec or AUDIO_MUX_FRAME_COUNT_TIMEOUT_SEC)}s", True)
            time.sleep(5)
        out, err = proc.communicate()
        if proc.returncode != 0:
            detail = (err or out or "").strip()
            if detail:
                detail = detail[-600:]
            return (0, {}, f"ffprobe frame count rc={proc.returncode}{(': ' + detail) if detail else ''}", False)
    try:
        payload = json.loads(out or "{}")
        streams = payload.get("streams") or []
        stream = streams[0] if streams else {}
        raw_count = stream.get("nb_read_frames") or stream.get("nb_frames") or 0
        if str(raw_count).upper() == "N/A":
            raw_count = 0
        return (int(raw_count or 0), stream, "", False)
    except Exception as exc:
        return (0, {}, f"ffprobe frame count parse failed: {exc}", False)


def _ffprobe_video_packet_count_details(
    input_path: str,
    *,
    stream_index: int = 0,
    heartbeat_stage: str = "source_packet_count",
    job_id: Optional[str] = None,
    timeout_sec: Optional[int] = None,
) -> Tuple[int, dict, str, bool]:
    try:
        stream_index = max(0, int(stream_index or 0))
    except Exception:
        stream_index = 0
    args = [
        'ffprobe', '-v', 'error',
        '-count_packets',
        '-select_streams', f'v:{stream_index}',
        '-show_entries', 'stream=nb_read_packets,nb_frames,duration,avg_frame_rate',
        '-of', 'json',
        input_path,
    ]
    if not job_id:
        out, err, timed_out = _run_ffprobe(args)
        if err:
            return (0, {}, err, timed_out)
    else:
        deadline = _now() + max(1, int(timeout_sec or FFPROBE_TIMEOUT_SEC))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        while proc.poll() is None:
            _job_heartbeat(job_id, heartbeat_stage, note="ffprobe-count-packets")
            if _now() >= deadline:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                return (0, {}, f"ffprobe packet count timed out after {int(timeout_sec or FFPROBE_TIMEOUT_SEC)}s", True)
            time.sleep(2)
        out, err = proc.communicate()
        if proc.returncode != 0:
            detail = (err or out or "").strip()
            if detail:
                detail = detail[-600:]
            return (0, {}, f"ffprobe packet count rc={proc.returncode}{(': ' + detail) if detail else ''}", False)
    try:
        payload = json.loads(out or "{}")
        streams = payload.get("streams") or []
        stream = streams[0] if streams else {}
        raw_count = stream.get("nb_read_packets") or stream.get("nb_frames") or 0
        if str(raw_count).upper() == "N/A":
            raw_count = 0
        return (int(raw_count or 0), stream, "", False)
    except Exception as exc:
        return (0, {}, f"ffprobe packet count parse failed: {exc}", False)

def _ffprobe_stream0_details(input_path: str) -> Tuple[dict, str, bool]:
    out, err, timed_out = _run_ffprobe([
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name,width,height,avg_frame_rate,nb_frames,duration:stream_tags=NUMBER_OF_FRAMES,NUMBER_OF_PACKETS,DURATION',
        '-show_entries', 'format=size,bit_rate',
        '-of', 'json', input_path
    ])
    if err:
        return ({"codec":"","width":0,"height":0,"fps":0.0,"nb_frames":0,"size":0,"bit_rate":0}, err, timed_out)
    try:
        data = json.loads(out)
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
        tags = v.get('tags') or {}
        raw_nb_frames = v.get('nb_frames') or tags.get('NUMBER_OF_FRAMES') or tags.get('NUMBER_OF_PACKETS') or 0
        if str(raw_nb_frames).upper() == "N/A":
            raw_nb_frames = 0
        return ({
            "codec":  v.get('codec_name') or '',
            "width":  int(v.get('width') or 0),
            "height": int(v.get('height') or 0),
            "fps":    fps,
            "nb_frames": int(raw_nb_frames or 0),
            "stream_duration": v.get('duration') or tags.get('DURATION') or '',
            "tags": tags,
            "size":   int(fmt.get('size') or 0),
            "bit_rate": int(fmt.get('bit_rate') or 0),
        }, "", False)
    except Exception as exc:
        return ({"codec":"","width":0,"height":0,"fps":0.0,"nb_frames":0,"size":0,"bit_rate":0}, f"ffprobe stream parse failed: {exc}", False)

def _ffprobe_stream0(input_path: str):
    meta, _, _ = _ffprobe_stream0_details(input_path)
    return meta

def _job_project_root(job_id: str, job=None) -> str:
    if job is None:
        job = redis.hgetall(_job_key(job_id)) or {}
    root = (job.get("scratch_root") or "").strip()
    return root or PROJECT_ROOT

def _job_project_roots(job_id: str, job=None) -> List[str]:
    if job is None:
        job = redis.hgetall(_job_key(job_id)) or {}
    roots = [
        (job.get("scratch_root_effective") or "").strip(),
        (job.get("scratch_root") or "").strip(),
        PROJECT_ROOT,
    ]
    result = []
    seen = set()
    for root in roots:
        if not root:
            continue
        root_abs = os.path.abspath(root)
        if root_abs in seen:
            continue
        seen.add(root_abs)
        result.append(root_abs)
    return result or [os.path.abspath(PROJECT_ROOT)]

def _job_base_dir_path(job_id: str, root: Optional[str] = None, job=None) -> str:
    project_root = os.path.abspath(root or _job_project_root(job_id, job))
    return os.path.join(project_root, job_id)

def _job_base_dir(job_id: str, job=None) -> str:
    preferred_root = _job_project_root(job_id, job)
    preferred_base = _job_base_dir_path(job_id, preferred_root)
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
        fallback_base = _job_base_dir_path(job_id, fallback_root)
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

def _job_status(job_id: str) -> Optional[Status]:
    try:
        return Status.parse(redis.hget(_job_key(job_id), "status"))
    except Exception:
        return None

def _job_is_terminal(job_id: str) -> bool:
    return _job_status(job_id) in (Status.FAILED, Status.REJECTED, Status.STOPPED, Status.DONE)

def _cleanup_token_allows_job_dir_removal(job_id: str, run_token: Optional[str], reason: str) -> bool:
    if _job_is_terminal(job_id):
        return True
    token = str(run_token or "").strip()
    if not token:
        return True
    try:
        current = str(redis.hget(_job_key(job_id), "pipeline_run_token") or "").strip()
    except Exception:
        current = ""
    if current and current == token:
        return True
    logger.warning(
        "[%s] skipping task cleanup for %s; token no longer owns live job (token=%s current=%s)",
        job_id,
        reason,
        token[:8] or "missing",
        current[:8] or "missing",
    )
    return False

def _safe_job_dir_paths(job_id: str, job=None) -> List[str]:
    paths = []
    seen = set()
    for root in _job_project_roots(job_id, job):
        root_abs = os.path.abspath(root)
        path_abs = os.path.abspath(_job_base_dir_path(job_id, root_abs))
        try:
            if os.path.commonpath([root_abs, path_abs]) != root_abs:
                continue
        except Exception:
            continue
        if os.path.basename(path_abs) != job_id:
            continue
        if path_abs == root_abs or path_abs in seen:
            continue
        seen.add(path_abs)
        paths.append(path_abs)
    return paths

def _cleanup_local_job_dir(job_id: str, job=None, *, reason: str, run_token: Optional[str] = None):
    if not _cleanup_token_allows_job_dir_removal(job_id, run_token, reason):
        return
    try:
        if _as_bool(redis.hget(_job_key(job_id), "preserve_project_dir"), False):
            logger.warning("[%s] preserving task directory after %s", job_id, reason)
            return
    except Exception:
        pass
    for path in _safe_job_dir_paths(job_id, job):
        if not os.path.exists(path):
            continue
        try:
            size_b = 0
            for dirpath, _, filenames in os.walk(path):
                for filename in filenames:
                    try:
                        size_b += os.path.getsize(os.path.join(dirpath, filename))
                    except Exception:
                        pass
            shutil.rmtree(path)
            logger.info(
                "[%s] cleaned task directory %s after %s (%.1f MiB)",
                job_id,
                path,
                reason,
                size_b / (1024 * 1024),
            )
        except Exception:
            logger.exception("[%s] failed to clean task directory %s after %s", job_id, path, reason)

def _cleanup_empty_local_job_dir(job_id: str, job=None, *, reason: str, run_token: Optional[str] = None):
    if not _cleanup_token_allows_job_dir_removal(job_id, run_token, reason):
        return
    try:
        if _as_bool(redis.hget(_job_key(job_id), "preserve_project_dir"), False):
            return
    except Exception:
        pass
    for path in _safe_job_dir_paths(job_id, job):
        try:
            os.rmdir(path)
            logger.info("[%s] removed empty task directory %s after %s", job_id, path, reason)
        except FileNotFoundError:
            pass
        except OSError:
            pass
        except Exception:
            logger.exception("[%s] failed to remove empty task directory %s after %s", job_id, path, reason)

def _cleanup_local_job_dir_when_terminal(
    job_id: str,
    job=None,
    *,
    reason: str,
    run_token: Optional[str] = None,
    poll_sec: float = 10.0,
    max_wait_sec: float = 7 * 24 * 3600,
):
    started_at = _now()

    def _wait_and_clean():
        while (_now() - started_at) < max_wait_sec:
            if _job_is_terminal(job_id):
                _cleanup_local_job_dir(job_id, job, reason=reason, run_token=run_token)
                return
            if run_token and not _task_token_is_current(job_id, run_token, "cleanup"):
                return
            time.sleep(max(1.0, poll_sec))

    threading.Thread(target=_wait_and_clean, name=f"cleanup-{job_id[:8]}", daemon=True).start()

def _part_paths(job_id: str, idx: int) -> Tuple[str, str]:
    base_dir  = _job_base_dir(job_id)
    parts_dir = os.path.join(base_dir, "parts")
    enc_dir   = os.path.join(base_dir, "encoded")
    _ensure_dirs(base_dir, parts_dir, enc_dir)
    return (os.path.join(parts_dir, f"part_{idx:03d}.ts"),
            os.path.join(enc_dir,   f"enc_{idx:03d}.mp4"))


def _audio_path(job_id: str) -> str:
    base_dir = _job_base_dir(job_id)
    audio_dir = os.path.join(base_dir, "audio")
    _ensure_dirs(base_dir, audio_dir)
    return os.path.join(audio_dir, "audio.m4a")


def _segment_manifest_path(job_id: str, job=None) -> str:
    return os.path.join(_job_base_dir(job_id, job), "segments.csv")


def _segment_manifest_durations(job_id: str, job=None, *, expected_parts: int = 0) -> list[float]:
    """
    Return source-timeline durations from ffmpeg's segment CSV.

    Encoded chunks may start their local PTS at zero. The segment manifest is
    the authoritative source timeline, so stitch uses it to avoid collapsing
    tiny inter-segment timestamp gaps hundreds of times.
    """
    path = _segment_manifest_path(job_id, job)
    if not os.path.exists(path):
        return []

    durations: list[float] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) < 3:
                    continue
                try:
                    start_s = float(row[1])
                    end_s = float(row[2])
                except Exception:
                    continue
                duration = end_s - start_s
                if duration > 0:
                    durations.append(duration)
    except Exception:
        logger.exception("failed to read segment manifest %s", path)
        return []

    if expected_parts > 0 and len(durations) != expected_parts:
        logger.warning(
            "segment manifest part count mismatch for %s: manifest=%s expected=%s",
            job_id,
            len(durations),
            expected_parts,
        )
    return durations


def _reset_job_run_state(job_id: str, job=None):
    """
    Clear stale per-run files/counters so retries/restarts don't reuse old parts.
    """
    job_key = _job_key(job_id)
    base_dir = _job_base_dir(job_id, job)
    parts_dir = os.path.join(base_dir, "parts")
    enc_dir = os.path.join(base_dir, "encoded")
    audio_dir = os.path.join(base_dir, "audio")
    try:
        old_parts_total = int(redis.hget(job_key, "parts_total") or 0)
    except Exception:
        old_parts_total = 0

    for path in (parts_dir, enc_dir, audio_dir):
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
        os.path.join(base_dir, f"job_{job_id}_output_av.mp4"),
        _segment_manifest_path(job_id, job),
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
            'combine_started': 0,
            'audio_status': '',
            'audio_progress': 0,
            'audio_elapsed': 0,
            'audio_path': '',
            'audio_error': '',
            'audio_mux_sync_mode': '',
            'audio_mux_video_frame_count': '',
            'audio_mux_source_frame_count': '',
            'audio_mux_frame_count_delta': '',
            'audio_mux_frame_count_tolerance': '',
            'audio_mux_frame_count_error': '',
            'audio_mux_video_frame_details_json': '',
            'audio_mux_video_itsscale': '',
            'audio_mux_target_duration': '',
            'audio_mux_sync_correction_sec': '',
            'video_duration_before_audio_mux': '',
            'frame_chunking_mode': '',
            'frame_plan_json': '',
            'frame_plan_total_frames': '',
            'frame_plan_parts': '',
            'frame_validation_status': '',
            'frame_validation_error': '',
            'final_video_frame_count': '',
            'final_video_frame_delta': '',
            'final_video_frame_details_json': '',
            'source_frame_count_exact': '',
            'source_frame_count_method': '',
            'source_frame_count_details_json': '',
            'part_frame_count_error': '',
            'stitch_expected_video_duration': '',
            'segment_manifest_count': '',
            'segment_manifest_path': '',
            'preserve_project_dir': '0',
            'preserve_project_dir_reason': '',
            'error': '',
            'failed_part': 0,
            'failed_stage': '',
            'failed_worker': '',
            'processing_mode_effective': '',
            'processing_mode_reason': '',
            'direct_segment_duration': '',
            'last_heartbeat_at': 0,
            'last_heartbeat_stage': '',
            'last_heartbeat_host': '',
            'last_heartbeat_note': '',
            'ended_at': 0,
        })
        if old_parts_total > 0:
            stale_part_fields = []
            for i in range(1, min(old_parts_total, 5000) + 1):
                stale_part_fields.extend([
                    f'part_{i:03d}_expected_frames',
                    f'part_{i:03d}_actual_frames',
                    f'part_{i:03d}_frame_details_json',
                ])
            if stale_part_fields:
                redis.hdel(job_key, *stale_part_fields)
        redis.hdel(job_key, 'master_host', 'stitch_host', 'stitch_enqueued_at')
        redis.delete(f"job_done_parts:{job_id}")
        redis.delete(f"job_retry_counts:{job_id}")
        redis.delete(f"job_retry_ts:{job_id}")
        redis.delete(f"job_missing_first_seen:{job_id}")
        redis.delete(f"job_retry_inflight:{job_id}")
    except Exception:
        pass


def _reset_job_run_state_for_token(job_id: str, run_token: Optional[str], job=None):
    """
    Reset stale runtime state once per manager dispatch token.

    The manager launches split() directly now, so restarts must not inherit
    master/stitch endpoints from previous runs. Keep this token-scoped so a
    Huey retry of the same split task does not delete current-run parts.
    """
    job_key = _job_key(job_id)
    token = str(run_token or "__legacy__")
    try:
        if (redis.hget(job_key, "run_state_reset_token") or "") == token:
            return
    except Exception:
        pass

    _reset_job_run_state(job_id, job)
    try:
        redis.hset(job_key, "run_state_reset_token", token)
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

def _task_token_is_current(job_id: str, run_token: Optional[str], task_name: str) -> bool:
    """
    Ignore stale Huey work from an older dispatch of the same job.

    Huey can legitimately replay unacked tasks after a worker restart. A per-run
    token keeps those old tasks from spawning split/stitch/encode work after the
    manager has already reserved a newer run of the job.
    """
    status_raw = redis.hget(_job_key(job_id), "status")
    try:
        status = Status.parse(status_raw)
    except Exception:
        status = None
    if status in (Status.FAILED, Status.REJECTED, Status.STOPPED, Status.DONE):
        logger.warning(
            "[%s] %s: task ignored because job is terminal (%s)",
            job_id,
            task_name,
            status.value,
        )
        return False

    token = str(run_token or "").strip()
    current = str(redis.hget(_job_key(job_id), "pipeline_run_token") or "").strip()
    if current:
        if token == current:
            return True
        logger.warning(
            "[%s] %s: stale task ignored (token=%s current=%s)",
            job_id,
            task_name,
            token[:8] or "missing",
            current[:8],
        )
        return False
    if token:
        logger.warning(
            "[%s] %s: tokened task ignored because job has no current token",
            job_id,
            task_name,
        )
        return False
    return True

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

def _direct_source_video_filter(filtergraph: str, frame_count: Optional[int]) -> str:
    """
    Direct-source chunks are planned from the source timeline. The input seek
    may include decode preroll, so trim first by local post-seek time and then
    optionally cap by frame count to avoid fractional rounding at chunk ends.
    """
    try:
        frames = int(frame_count or 0)
    except Exception:
        frames = 0
    prefix = f"trim=end_frame={frames},setpts=PTS-STARTPTS" if frames > 0 else "setpts=PTS-STARTPTS"
    filtergraph = (filtergraph or "").strip()
    if not filtergraph:
        return prefix
    return f"{prefix},{filtergraph}"

def _direct_source_trim_filter(
    filtergraph: str,
    *,
    trim_start_s: float,
    trim_duration_s: float,
    frame_count: Optional[int],
) -> str:
    try:
        frames = int(frame_count or 0)
    except Exception:
        frames = 0
    trim_start_s = max(0.0, float(trim_start_s or 0.0))
    trim_duration_s = max(0.05, float(trim_duration_s or 0.0))
    filters = [
        f"trim=start={trim_start_s:.6f}:duration={trim_duration_s:.6f}",
        "setpts=PTS-STARTPTS",
    ]
    if frames > 0:
        filters.extend([f"trim=end_frame={frames}", "setpts=PTS-STARTPTS"])
    filtergraph = (filtergraph or "").strip()
    if filtergraph:
        filters.append(filtergraph)
    return ",".join(filters)


def _direct_source_hybrid_filter(
    filtergraph: str,
    *,
    trim_start_s: float,
    trim_duration_s: float,
    frame_count: Optional[int],
) -> str:
    return _direct_source_trim_filter(
        filtergraph,
        trim_start_s=trim_start_s,
        trim_duration_s=trim_duration_s,
        frame_count=frame_count,
    )


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

def _subtitle_codec_mkv_copy_safe(codec_name: str) -> bool:
    codec = str(codec_name or "").strip().lower()
    return codec in {
        "ass",
        "ssa",
        "subrip",
        "srt",
        "webvtt",
        "hdmv_pgs_subtitle",
        "dvd_subtitle",
    }

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
    settings.setdefault("min_segment_count", "100")
    settings.setdefault("video_encoder", VIDEO_ENCODER_DEFAULT)
    return settings

def _video_encoder_for_job(job_key: str) -> str:
    settings = _load_global_settings()
    return normalize_video_encoder(
        redis.hget(job_key, "video_encoder") or settings.get("video_encoder"),
        VIDEO_ENCODER_DEFAULT,
    )

def _disk_free_bytes(path: str) -> int:
    try:
        return int(shutil.disk_usage(path).free)
    except Exception:
        return -1

def _selected_video_fps_from_job(job) -> float:
    try:
        selected = int((job or {}).get("selected_v_stream") or 0)
    except Exception:
        selected = 0
    try:
        streams = json.loads((job or {}).get("streams_json") or "{}")
    except Exception:
        streams = {}
    video_streams = streams.get("video") or []
    if 0 <= selected < len(video_streams):
        try:
            fps = float((video_streams[selected] or {}).get("fps") or 0.0)
            if fps > 0:
                return fps
        except Exception:
            pass
    try:
        fps = float((job or {}).get("source_fps") or 0.0)
        if fps > 0:
            return fps
    except Exception:
        pass
    return 0.0


def _selected_video_frame_count_from_job(job, meta: Optional[dict], duration: float, fps: float) -> Tuple[int, str, dict]:
    meta = meta or {}

    def _positive_int(value) -> int:
        try:
            raw = str(value or "").strip()
            if not raw or raw.upper() == "N/A":
                return 0
            return int(float(raw))
        except Exception:
            return 0

    count = _positive_int((job or {}).get("source_frame_count_exact"))
    if count > 0:
        return count, "cached_exact", {}

    try:
        selected = int((job or {}).get("selected_v_stream") or 0)
    except Exception:
        selected = 0
    try:
        streams = json.loads((job or {}).get("streams_json") or "{}").get("video") or []
    except Exception:
        streams = []
    if 0 <= selected < len(streams):
        stream = streams[selected] or {}
        count = _positive_int(stream.get("nb_frames") or stream.get("number_of_frames"))
        if count > 0:
            return count, "streams_json", stream
        tags = stream.get("tags") or {}
        count = _positive_int(tags.get("NUMBER_OF_FRAMES") or tags.get("NUMBER_OF_PACKETS"))
        if count > 0:
            return count, "streams_json_tags", stream

    count = _positive_int(meta.get("nb_frames"))
    if count > 0:
        return count, "stream_metadata", meta
    tags = meta.get("tags") or {}
    count = _positive_int(tags.get("NUMBER_OF_FRAMES") or tags.get("NUMBER_OF_PACKETS"))
    if count > 0:
        return count, "stream_tags", meta

    cached_method = str((job or {}).get("source_frame_count_method") or "").strip().lower()
    count = _positive_int((job or {}).get("total_frames"))
    if count > 0:
        method = "cached_total_frames_estimate"
        if cached_method in {"duration_x_fps_estimate", "estimated_duration_x_fps"}:
            method = "cached_duration_x_fps_estimate"
        return count, method, {}

    try:
        duration = float(duration or 0.0)
        fps = float(fps or 0.0)
    except Exception:
        duration = 0.0
        fps = 0.0
    if duration > 0 and fps > 0:
        return max(1, int(round(duration * fps))), "duration_x_fps_estimate", {}

    return 0, "", {}


def _frame_count_method_is_exact(method: str) -> bool:
    return str(method or "").strip().lower() not in {
        "",
        "cached_total_frames",
        "cached_total_frames_estimate",
        "duration_x_fps_estimate",
        "cached_duration_x_fps_estimate",
        "estimated_duration_x_fps",
    }

def _direct_source_plan_for_part(
    source_duration: float,
    segment_duration: float,
    idx: int,
    *,
    fps: float = 0.0,
    total_frames: int = 0,
    total_parts: int = 0,
):
    if idx <= 0:
        return None, None, None
    try:
        fps = float(fps or 0.0)
    except Exception:
        fps = 0.0
    try:
        total_frames = int(total_frames or 0)
    except Exception:
        total_frames = 0
    try:
        total_parts = int(total_parts or 0)
    except Exception:
        total_parts = 0
    if fps > 0 and total_frames > 0 and total_parts > 0:
        start_frame = int(((idx - 1) * total_frames) // total_parts)
        end_frame = int((idx * total_frames) // total_parts)
        if start_frame >= total_frames or end_frame <= start_frame:
            return None, None, None
        frame_count = end_frame - start_frame
        start_s = start_frame / fps
        part_duration = frame_count / fps
        return start_s, max(0.05, part_duration), frame_count
    segment_duration = max(1.0, float(segment_duration or 10.0))
    start_s = (idx - 1) * segment_duration
    if source_duration and start_s >= source_duration:
        return None, None, None
    part_duration = segment_duration
    if source_duration:
        part_duration = max(0.05, min(segment_duration, source_duration - start_s))
    return start_s, part_duration, None


def _frame_plan_for_part(total_frames: int, idx: int, total_parts: int) -> Tuple[int, int, int]:
    try:
        total_frames = max(0, int(total_frames or 0))
        idx = int(idx or 0)
        total_parts = max(1, int(total_parts or 1))
    except Exception:
        return 0, 0, 0
    if idx <= 0 or idx > total_parts or total_frames <= 0:
        return 0, 0, 0
    start_frame = int(((idx - 1) * total_frames) // total_parts)
    end_frame = int((idx * total_frames) // total_parts)
    if end_frame <= start_frame:
        return start_frame, end_frame, 0
    return start_frame, end_frame, end_frame - start_frame


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


def _min_segment_count_from_settings(settings) -> int:
    try:
        value = int((settings or {}).get("min_segment_count", 100))
    except Exception:
        value = 100
    return max(1, value)


def _host_from_endpoint(endpoint: str) -> str:
    raw = str(endpoint or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)
    return raw.split("/", 1)[0].split(":", 1)[0].strip().lower()

def _host_short(value: str) -> str:
    return str(value or "").strip().lower().split(".", 1)[0]

def _reserved_host_matches(job: dict, field: str) -> bool:
    reserved = _host_short((job or {}).get(field) or "")
    if not reserved:
        return True
    return reserved == _host_short(WORKER_NAME)

def _wrong_reserved_host_message(job_id: str, field: str) -> str:
    return (
        f"[{job_id}] {WORKER_NAME} is not reserved for {field}; "
        f"reserved={redis.hget(_job_key(job_id), field) or 'unset'}"
    )

def _pipeline_task_for_host(task_wrapper, host: str):
    host = _host_short(host)
    if not host:
        raise ValueError("pipeline task host is required")
    key = (task_wrapper.name, host)
    wrapper = _TARGET_PIPELINE_TASKS.get(key)
    if wrapper is not None:
        return wrapper
    target_huey = get_pipeline_huey_for_host(host)
    wrapper = target_huey.task(
        retries=task_wrapper.retries,
        retry_delay=task_wrapper.retry_delay,
        name=task_wrapper.name,
    )(task_wrapper.func)
    _TARGET_PIPELINE_TASKS[key] = wrapper
    return wrapper

def _enqueue_pipeline_task_for_host(task_wrapper, host: str, *args):
    return _pipeline_task_for_host(task_wrapper, host)(*args)

def _requeue_misdirected_pipeline_task(task_func, task_name: str, job_id: str, field: str, *args):
    """
    A shared pipeline queue can deliver reserved work to the wrong host.

    Treat that as queue misdelivery, not task failure. Raising here lets Huey
    put the task into its scheduled retry queue, which can delay stitcher
    startup and leave encoders waiting for stitch_host. Requeue immediately so
    the reserved idle host gets another chance to claim it.
    """
    msg = _wrong_reserved_host_message(job_id, field)
    logger.info("%s; requeueing %s", msg, task_name)
    try:
        _job_heartbeat(job_id, f"{task_name}_misdirected", note=f"host={WORKER_NAME}", force=True)
    except Exception:
        pass
    reserved = redis.hget(_job_key(job_id), field) or ""
    _enqueue_pipeline_task_for_host(task_func, reserved, *args)
    return {
        "status": "MISDIRECTED",
        "job_id": job_id,
        "task": task_name,
        "worker": WORKER_NAME,
        "reserved": redis.hget(_job_key(job_id), field) or "",
    }

def _resolve_processing_mode(job_id: str, job, source_size_b: int) -> Tuple[str, str]:
    input_path = os.path.realpath((job.get("input_path") or "").strip())
    source_root = os.path.realpath(SOURCE_MEDIA_ROOT)
    if _requires_direct_source_codec(str(job.get("source_codec") or "")):
        return "direct", "codec_requires_direct_source"
    if _job_has_selected_audio(job):
        return "direct", "continuous_audio_requires_source_timeline"
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
                    if job_id is None or kind not in ("result", "audio"):
                        self.send_error(404, "Not found")
                        return

                    job_data = redis.hgetall(_job_key(job_id)) or {}
                    if _job_is_terminal(job_id):
                        self.send_error(409, "Job is terminal")
                        return
                    title = _job_title(job_data)
                    filename = job_data.get("filename") or ""
                    put_t0 = _now()
                    if kind == "audio":
                        artifact_path = _audio_path(job_id)
                    else:
                        _, artifact_path = _part_paths(job_id, idx)
                    tmp_path = f"{artifact_path}.{uuid.uuid4().hex}.uploading"

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

                    _ensure_dirs(os.path.dirname(artifact_path))
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

                    os.replace(tmp_path, artifact_path)
                    if kind == "audio":
                        redis.hset(_job_key(job_id), mapping={
                            "audio_status": "READY",
                            "audio_path": artifact_path,
                            "audio_progress": 100,
                            "audio_uploaded_at": str(_now()),
                        })
                        emit_activity(
                            f'Audio for "{title}" uploaded in {_elapsed_ms(put_t0)}ms',
                            job_id=job_id,
                            filename=filename,
                            stage='audio_upload',
                            source='worker',
                        )
                    else:
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
                    try:
                        if 'tmp_path' in locals() and tmp_path and os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    except Exception:
                        pass
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
def _transcode_impl(job_id: str, file_path: str, run_token: Optional[str] = None):
    """
    Orchestration entrypoint:
      - Queue split(job_id, file_path).
      - split publishes parts_total, then queues stitch(job_id), then dispatches encodes.
    """
    if not _task_token_is_current(job_id, run_token, "transcode"):
        return {'status': 'STALE', 'job_id': job_id}

    _reset_job_run_state(job_id)
    _job_heartbeat(job_id, "transcode_start", force=True)
    redis.hset(_job_key(job_id), mapping={
        'status': Status.RUNNING.value,
        'started_at': _now(),
    })
    job = redis.hgetall(_job_key(job_id)) or {}
    reserved_segment_host = _host_short(job.get("reserved_segment_host") or "")
    if reserved_segment_host:
        enqueue_split_on_host(reserved_segment_host, job_id, file_path, run_token)
    else:
        split(job_id, file_path, run_token)
    return {'status': 'QUEUED', 'job_id': job_id}

@pipeline_huey.task(retries=999999, retry_delay=5)
def transcode(job_id: str, file_path: str, run_token: Optional[str] = None):
    return _transcode_impl(job_id, file_path, run_token)


def _queue_stitcher(job_id: str, run_token: Optional[str]) -> Optional[str]:
    job_key = _job_key(job_id)
    job = redis.hgetall(job_key) or {}
    reserved_stitch_host = _host_short(job.get("reserved_stitch_host") or "")
    stitch_host = job.get("stitch_host") or ""
    if stitch_host:
        existing_host = _host_short(_host_from_endpoint(stitch_host))
        if not reserved_stitch_host or existing_host == reserved_stitch_host:
            return stitch_host
        logger.warning(
            "[%s] ignoring stale stitch_host=%s; reserved_stitch_host=%s",
            job_id,
            stitch_host,
            reserved_stitch_host,
        )
        try:
            redis.hdel(job_key, "stitch_host", "stitch_enqueued_at")
        except Exception:
            pass

    redis.hset(job_key, mapping={
        "stitch_enqueued_at": str(_now()),
    })
    if reserved_stitch_host:
        enqueue_stitch_on_host(reserved_stitch_host, job_id, run_token)
    else:
        stitch(job_id, run_token)
    return redis.hget(job_key, "stitch_host") or ""


@pipeline_huey.task(retries=999999, retry_delay=5)
def split(job_id: str, file_path: str, run_token: Optional[str] = None):
    """
    Master-side segmentation & dispatch:
      - Start HTTP server (serve parts).
      - Probe source, set metadata, advertise master_host.
      - Segment source and dispatch encode(job_id, idx, ...) for each segment.
      - No waiting or stitching here anymore.
    """
    job_key = _job_key(job_id)
    if not _task_token_is_current(job_id, run_token, "split"):
        return {'status': 'STALE', 'job_id': job_id}

    job = redis.hgetall(job_key)
    if not _reserved_host_matches(job, "reserved_segment_host"):
        return _requeue_misdirected_pipeline_task(
            split,
            "split",
            job_id,
            "reserved_segment_host",
            job_id,
            file_path,
            run_token,
        )
    title = _job_title(job)
    _reset_job_run_state_for_token(job_id, run_token, job)
    job = redis.hgetall(job_key) or {}
    if not _task_token_is_current(job_id, run_token, "split"):
        return {'status': 'STALE', 'job_id': job_id}

    logger.info(f"[{job_id}] Starting transcode job")
    cleanup_on_exit = True

    try:
        _start_http_once()  # HTTP server for parts
        if not _task_token_is_current(job_id, run_token, "split"):
            return {'status': 'STALE', 'job_id': job_id}

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
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': f'input not found: {src_path}',
                'failed_stage': 'input',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': f'input not found: {src_path}'}

        # Mark as RUNNING & remember input path
        now = _now()
        redis.hset(job_key, mapping={
            'status': Status.RUNNING.value,
            'started_at': now,
            'input_path': src_path,
            'target_height': target_height,
        })
        _job_heartbeat(job_id, "probe", force=True, note="starting source probe")

        # Probe essentials for UI. Time out instead of letting a wedged source
        # file pin the whole queue indefinitely.
        meta, meta_err, meta_timed_out = _ffprobe_stream0_details(src_path)
        duration, duration_err, duration_timed_out = _ffprobe_duration_details(src_path)
        if meta_timed_out or duration_timed_out:
            probe_errors = "; ".join(x for x in (meta_err, duration_err) if x) or "ffprobe timed out"
            logger.error(f"[{job_id}] Source probe timed out for {src_path}: {probe_errors}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': f"source probe timed out: {probe_errors}",
                'failed_stage': 'probe',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': f"source probe timed out: {probe_errors}"}
        if duration_err:
            logger.error(f"[{job_id}] Source duration probe failed for {src_path}: {duration_err}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': f"source duration probe failed: {duration_err}",
                'failed_stage': 'probe',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': f"source duration probe failed: {duration_err}"}
        codec    = meta.get("codec") or ""
        width    = meta.get("width") or 0
        height   = meta.get("height") or 0
        fps      = meta.get("fps") or 0.0
        size_b   = meta.get("size") or 0
        fmt_bps  = meta.get("bit_rate") or 0
        kbps_calc = (fmt_bps/1000.0) if fmt_bps > 0 else (
            ((size_b*8)/duration/1000.0) if (size_b and duration) else 0.0
        )
        source_probe_updates = {
            'source_codec': codec,
            'source_resolution': f"{width}x{height}" if (width and height) else '',
            'source_duration': f"{duration:.2f}" if duration else '0',
            'source_fps': f"{fps:.2f}" if fps else '0',
            'source_file_size': size_b,
            'source_bitrate_kbps': f"{kbps_calc:.0f}" if kbps_calc>0 else '0'
        }
        redis.hset(job_key, mapping=source_probe_updates)
        job.update(source_probe_updates)

        # AV1 inputs are supported through direct-source encoding when allowed.
        # Keep the global rejection switch as a quick safety brake.
        if (
            (codec or "").strip().lower() in {"av1", "av01"}
            and as_bool(get_settings().get("av1_check_enabled", "1"), True)
        ):
            msg = "AV1 source rejected by global setting (av1_check_enabled)."
            logger.error(f"[{job_id}] {msg}")
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
        _job_heartbeat(job_id, "plan", force=True, note=f"mode={processing_mode}")
        logger.info(f"[{job_id}] Processing mode: {processing_mode} ({processing_reason})")
        if processing_mode == "direct" and duration <= 0:
            logger.warning(
                f"[{job_id}] Source duration is unavailable; staying in direct frame-based mode."
            )

        split_settings = _load_global_settings()
        target_segment_mb, target_segment_bytes = _target_segment_bytes_from_settings(split_settings)
        min_segment_count = _min_segment_count_from_settings(split_settings)
        source_frame_count = 0
        source_frame_details = {}
        source_frame_method = ""
        if processing_mode == "direct":
            source_frame_count, source_frame_method, source_frame_details = _selected_video_frame_count_from_job(
                job,
                meta,
                duration,
                fps,
            )
            if source_frame_count <= 0:
                probe_count, probe_details, probe_err, _ = _ffprobe_video_packet_count_details(
                    src_path,
                    stream_index=v_sel,
                    job_id=job_id,
                    timeout_sec=FFPROBE_TIMEOUT_SEC,
                )
                if probe_err:
                    logger.warning(
                        "[%s] Source packet-count fallback failed for %s: %s",
                        job_id,
                        src_path,
                        probe_err,
                    )
                else:
                    source_frame_count = int(probe_count or 0)
                    source_frame_method = "packet_count"
                    source_frame_details = probe_details or {}
            if source_frame_count <= 0:
                reason = "source frame count unavailable; refusing frame-based direct encode"
                logger.error(f"[{job_id}] {reason}")
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': reason,
                    'failed_stage': 'frame_probe',
                    'failed_worker': WORKER_NAME,
                    'ended_at': _now(),
                })
                return {'status': 'FAILED', 'reason': reason}
            source_frame_exact = _frame_count_method_is_exact(source_frame_method)
            frame_count_mapping = {
                'total_frames': source_frame_count,
                'source_frame_count_method': source_frame_method,
                'source_frame_count_details_json': json.dumps(source_frame_details, separators=(",", ":")),
                'frame_chunking_mode': 'seek_preroll_frame_count',
                'frame_validation_status': 'planned',
            }
            if source_frame_exact:
                frame_count_mapping['source_frame_count_exact'] = source_frame_count
            else:
                frame_count_mapping['source_frame_count_exact'] = ''
            redis.hset(job_key, mapping=frame_count_mapping)
            if duration <= 0 and fps > 0:
                duration = source_frame_count / fps
                redis.hset(job_key, 'source_duration', f"{duration:.2f}")
                job['source_duration'] = f"{duration:.2f}"
            job.update({
                'total_frames': str(source_frame_count),
                'source_frame_count_exact': str(source_frame_count) if source_frame_exact else '',
                'source_frame_count_method': source_frame_method,
                'source_frame_count_details_json': json.dumps(source_frame_details, separators=(",", ":")),
                'frame_chunking_mode': 'seek_preroll_frame_count',
            })

        # requested parts from configured segment size
        requested_parts = _parts_for_target_size(int(size_b or 0), target_segment_bytes)
        if requested_parts <= 0:
            requested_parts = min_segment_count
            logger.warning(f"[{job_id}] Source size unavailable; falling back to {requested_parts} requested parts.")

        # Stitch is queued after the part plan is published below, so at this
        # point we only know the master is reserved.
        stitch_endpoint = ""
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

        # Segment count is deterministic for the same source/settings. Worker
        # count is intentionally not part of the plan because live capacity can
        # change between runs and should only affect queue drain speed.
        effective_parts = max(requested_parts, min_segment_count)

        P = max(1, effective_parts)
        effective_segment_bytes = max(1, int(ceil(float(size_b) / float(P)))) if int(size_b or 0) > 0 else target_segment_bytes
        effective_segment_mb = float(effective_segment_bytes) / float(1024 * 1024)
        redis.hset(job_key, mapping={
            "requested_segment_size_mb": f"{target_segment_mb:.6f}",
            "requested_segment_size_bytes": int(target_segment_bytes),
            "min_segment_count": int(min_segment_count),
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
            f"min_segments={min_segment_count}, usable_encoder_workers={usable_encoder_workers}, source_size={int(size_b or 0)} bytes, "
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
        _job_heartbeat(job_id, "segment_start", force=True, note=f"parts={P}")

        # Queue stitch after publishing the plan, but do not wait here. Waiting
        # from the split task can deadlock a small pipeline pool: split occupies
        # one pipeline worker while waiting for another pipeline worker to claim
        # stitch. Encoders resolve stitch_host later and heartbeat while they
        # wait, so pipeline congestion becomes backpressure instead of failure.
        stitch_endpoint = _queue_stitcher(job_id, run_token)
        if _job_has_selected_audio(job):
            redis.hset(job_key, mapping={
                'audio_status': 'QUEUED',
                'audio_progress': 0,
                'audio_elapsed': 0,
                'audio_error': '',
            })
            encode_audio(job_id, stitch_host=stitch_endpoint, source_path=src_path, a_sel=a_sel, run_token=run_token)
        else:
            redis.hset(job_key, mapping={
                'audio_status': 'NONE',
                'audio_progress': 100,
                'audio_elapsed': 0,
                'audio_path': '',
                'audio_error': '',
            })

        if processing_mode == "direct":
            total_chunks = 0
            part_opened_at = _now()
            direct_fps = _selected_video_fps_from_job(job)
            try:
                direct_total_frames = int(source_frame_count or job.get("total_frames") or meta.get("nb_frames") or 0)
            except Exception:
                direct_total_frames = 0
            frame_plan = []
            for part_idx in range(1, P + 1):
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] Halted while scheduling direct-source parts.")
                    return {'status': 'ABORTED'}
                if not _task_token_is_current(job_id, run_token, "split"):
                    return {'status': 'STALE', 'job_id': job_id}

                start_s, part_duration, part_frame_count = _direct_source_plan_for_part(
                    duration,
                    segment_duration_hint,
                    part_idx,
                    fps=direct_fps,
                    total_frames=direct_total_frames,
                    total_parts=P,
                )
                if start_s is None:
                    break
                start_frame, end_frame, expected_frames = _frame_plan_for_part(direct_total_frames, part_idx, P)
                if expected_frames <= 0:
                    break

                total_chunks = part_idx
                frame_plan.append({
                    "part": part_idx,
                    "start_frame": start_frame,
                    "end_frame": end_frame,
                    "frame_count": expected_frames,
                    "start_s": start_s,
                    "duration_s": part_duration,
                })
                part_elapsed_ms = _elapsed_ms(part_opened_at)
                part_opened_at = _now()
                emit_activity(
                    f'Segmenting "{title}" part {part_idx} completed in {part_elapsed_ms}ms',
                    job_id=job_id,
                    filename=job.get('filename'),
                    stage='segment',
                    source='worker',
                )
                _job_heartbeat(job_id, "direct_dispatch", force=True, note=f"part={part_idx}/{P}")
                _dispatch_encode_part(
                    job_id,
                    part_idx,
                    master_url,
                    v_sel,
                    a_sel,
                    stitch_host=stitch_endpoint,
                    source_path=src_path,
                    source_start_s=start_s,
                    source_duration_s=part_duration,
                    source_frame_count=part_frame_count,
                    source_start_frame=start_frame,
                    source_end_frame=end_frame,
                    run_token=run_token,
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
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': 'no direct-source segments produced',
                    'failed_stage': 'segment',
                    'failed_worker': WORKER_NAME,
                    'ended_at': _now(),
                })
                return {'status': 'FAILED', 'reason': 'no segments produced'}

            redis.hset(job_key, mapping={
                'parts_total': total_chunks,
                'segmented_chunks': total_chunks,
                'segment_progress': 100,
                'segment_elapsed': round(_now() - seg_t0, 2),
                'frame_plan_json': json.dumps(frame_plan, separators=(",", ":")),
                'frame_plan_total_frames': direct_total_frames,
                'frame_plan_parts': total_chunks,
                'frame_validation_status': 'encoding',
            })
            emit_activity(
                f'Segmenting "{title}" completed in {_elapsed_ms(seg_t0)}ms',
                job_id=job_id,
                filename=job.get('filename'),
                stage='segment_complete',
                source='worker',
            )
            logger.info(f"[{job_id}] Direct-source scheduling complete; {total_chunks} parts queued for encoding")
            _cleanup_local_job_dir_when_terminal(
                job_id,
                job,
                reason="direct split task completed",
                run_token=run_token,
            )
            cleanup_on_exit = False
            return {'status': 'QUEUED', 'parts': total_chunks}

        # Prepare parts dir + naming
        parts_dir = os.path.join(_job_base_dir(job_id, job), "parts")
        _ensure_dirs(parts_dir)
        temp_pattern = os.path.join(parts_dir, "chunk_%03d.ts")
        segment_manifest = _segment_manifest_path(job_id, job)
        try:
            if os.path.exists(segment_manifest):
                os.remove(segment_manifest)
        except Exception:
            pass

        # Segment duration target from P (fallback 10s if unknown)
        segment_duration = max(1.0, float(duration) / float(P)) if (duration and P) else 10.0
        bsf = _bsf_for_codec(codec)

        cmd = [
            'ffmpeg', '-hide_banner',
            '-nostats', '-loglevel', 'info',
            '-i', src_path,
            '-map', f'0:v:{v_sel}',
            '-an', '-sn', '-dn',
            '-map_metadata', '-1',
            '-map_chapters', '-1',
            '-c', 'copy',
            '-f', 'segment',
            '-segment_time', f"{segment_duration:.6f}",
            '-segment_list', segment_manifest,
            '-segment_list_type', 'csv',
            '-reset_timestamps', '1',
            temp_pattern
        ]
        if bsf:
            cmd[cmd.index('-f'):cmd.index('-f')] = ['-bsf:v', bsf]
        logger.info(f"[{job_id}] Segment command: {' '.join(cmd)}")

        # Parse ffmpeg stderr for new-chunk openings. Guard against a silent
        # segmenter so an NFS/read stall cannot leave the queue blocked forever.
        segment_re = re.compile(r"Opening '(.+?/chunk_(\d+)\.ts)' for writing")

        if not _is_job_halted(job_id):
            process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        previous_chunk_path = None
        previous_chunk_index = None
        chunk_opened_at = None
        estimated_chunks = max(1, P)
        queued_count = 0

        segment_stderr_tail = deque(maxlen=120)
        last_segment_output_at = _now()
        while True:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] Halted. Terminating segmentation.")
                process.terminate()
                process.wait()
                return {'status': 'ABORTED'}

            if process.stderr is None:
                break

            ready, _, _ = select.select([process.stderr], [], [], 1.0)
            if not ready:
                _job_heartbeat(job_id, "segment_wait")
                if process.poll() is not None:
                    break
                if (_now() - last_segment_output_at) >= SEGMENT_IO_IDLE_TIMEOUT_SEC:
                    reason = f"ffmpeg segmentation produced no output for {SEGMENT_IO_IDLE_TIMEOUT_SEC}s"
                    logger.error(f"[{job_id}] {reason}")
                    try:
                        process.terminate()
                        process.wait(timeout=5)
                    except Exception:
                        try:
                            process.kill()
                        except Exception:
                            pass
                    redis.hset(job_key, mapping={
                        'status': Status.FAILED.value,
                        'error': reason,
                        'failed_stage': 'segment',
                        'failed_worker': master_host_name,
                        'ended_at': _now(),
                    })
                    return {'status': 'FAILED', 'reason': reason}
                continue

            line = process.stderr.readline()
            if not line:
                if process.poll() is not None:
                    break
                continue
            last_segment_output_at = _now()
            _job_heartbeat(job_id, "segment")
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
                        redis.hset(job_key, mapping={
                            'status': Status.FAILED.value,
                            'error': f'move part {part_idx} failed: {e}',
                            'failed_stage': 'segment',
                            'failed_worker': WORKER_NAME,
                            'ended_at': _now(),
                        })
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
                        if not _task_token_is_current(job_id, run_token, "split"):
                            return {'status': 'STALE', 'job_id': job_id}
                        _dispatch_encode_part(job_id, part_idx, master_url, v_sel, a_sel, stitch_host=stitch_endpoint, run_token=run_token)

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
                'ended_at': _now(),
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
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': f'move final part {part_idx} failed: {e}',
                    'failed_stage': 'segment',
                    'failed_worker': WORKER_NAME,
                    'ended_at': _now(),
                })
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
                if not _task_token_is_current(job_id, run_token, "split"):
                    return {'status': 'STALE', 'job_id': job_id}
                _dispatch_encode_part(job_id, part_idx, master_url, v_sel, a_sel, stitch_host=stitch_endpoint, run_token=run_token)
                total_chunks = part_idx

        if total_chunks <= 0:
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': 'no segments produced',
                'failed_stage': 'segment',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': 'no segments produced'}

        segment_durations = _segment_manifest_durations(job_id, job, expected_parts=total_chunks)
        if len(segment_durations) != total_chunks:
            reason = (
                f"segment manifest count mismatch: manifest={len(segment_durations)} "
                f"parts={total_chunks}; refusing timeline-unsafe split output"
            )
            logger.error(f"[{job_id}] {reason}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'failed_stage': 'segment_manifest',
                'failed_worker': WORKER_NAME,
                'segment_manifest_path': segment_manifest,
                'segment_manifest_count': len(segment_durations),
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': reason}

        # Finalize segmentation metadata
        redis.hset(job_key, mapping={
            'parts_total': total_chunks,
            'segmented_chunks': total_chunks,
            'segment_progress': 100,
            'segment_elapsed': round(_now() - seg_t0, 2),
            'segment_manifest_path': segment_manifest,
            'segment_manifest_count': len(segment_durations),
            'stitch_expected_video_duration': f"{sum(segment_durations):.6f}",
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
        _cleanup_local_job_dir_when_terminal(
            job_id,
            job,
            reason="split task completed",
            run_token=run_token,
        )
        cleanup_on_exit = False
        return {'status': 'QUEUED', 'parts': total_chunks}

    except Exception as e:
        logger.exception(f"[{job_id}] split failed")
        redis.hset(job_key, mapping={
            'status': Status.FAILED.value,
            'error': str(e),
            'failed_stage': 'split',
            'failed_worker': WORKER_NAME,
            'ended_at': _now(),
        })
        return {'status': 'FAILED', 'error': str(e)}
    finally:
        if cleanup_on_exit:
            _cleanup_local_job_dir(job_id, job, reason="split task ended", run_token=run_token)


@encode_huey.task()
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
    source_frame_count: Optional[int] = None,
    source_start_frame: Optional[int] = None,
    source_end_frame: Optional[int] = None,
    run_token: Optional[str] = None,
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

    worker_name = WORKER_NAME
    job_key = _job_key(job_id)
    if not _task_token_is_current(job_id, run_token, "encode"):
        return {'status': 'STALE', 'job_id': job_id, 'idx': idx}

    filename = redis.hget(job_key, "filename") or ""
    title = _job_title({"filename": filename})
    part_t0 = _now()
    wtmp = None
    in_path = None
    out_path = None

    def _cleanup_encode_task_files(reason: str):
        for p in (in_path, out_path):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
                    logger.info("[%s] encode %s removed %s after %s", job_id, idx, p, reason)
            except Exception:
                logger.exception("[%s] encode %s failed to remove %s after %s", job_id, idx, p, reason)
        _cleanup_empty_local_job_dir(job_id, reason=f"encode part {idx} {reason}", run_token=run_token)

    def _fail(reason: str, stage: str):
        if not _task_token_is_current(job_id, run_token, f"encode_{stage}_fail"):
            _cleanup_encode_task_files("stale failure")
            return {'status': 'STALE', 'job_id': job_id, 'idx': idx}

        retry_cnt_key = f"job_retry_counts:{job_id}"
        retry_ts_key = f"job_retry_ts:{job_id}"
        inflight_key = f"job_retry_inflight:{job_id}"
        retry_count = 0
        try:
            retry_count = int(redis.hincrby(retry_cnt_key, idx, 1))
            redis.hset(retry_ts_key, idx, _now())
            redis.srem(inflight_key, idx)
        except Exception:
            logger.exception("[%s] failed to update retry accounting for part %s", job_id, idx)

        failure_reason = f"part {idx} ({stage}): {reason}"

        if _is_job_halted(job_id):
            logger.warning(
                "[%s] encode %s failed after job became terminal; preserving original terminal status",
                job_id,
                idx,
            )
            _cleanup_encode_task_files("terminal after failure")
            return {'status': 'ABORTED', 'reason': failure_reason}

        if retry_count <= PART_FAILURE_MAX_RETRIES:
            try:
                redis.hset(job_key, mapping={
                    'last_part_error': failure_reason,
                    'last_failed_part': idx,
                    'last_failed_stage': stage,
                    'last_failed_worker': worker_name,
                    'last_failed_at': _now(),
                    'last_retry_part': idx,
                    'last_retry_stage': stage,
                    'last_retry_worker': worker_name,
                    'last_retry_at': _now(),
                })
            except Exception:
                pass
            logger.error(
                "[%s] %s on %s; requeued part (attempt %s/%s)",
                job_id,
                failure_reason,
                worker_name,
                retry_count,
                PART_FAILURE_MAX_RETRIES,
            )
            emit_activity(
                f'Retrying "{title}" part {idx} after failure on {worker_name}',
                job_id=job_id,
                filename=filename,
                stage='part_retry',
                source='worker',
            )
            _cleanup_encode_task_files("retry")
            _dispatch_encode_part(
                job_id,
                idx,
                master_host,
                v_sel,
                a_sel,
                stitch_host=stitch_host,
                source_path=source_path,
                source_start_s=source_start_s,
                source_duration_s=source_duration_s,
                source_frame_count=source_frame_count,
                source_start_frame=source_start_frame,
                source_end_frame=source_end_frame,
                run_token=run_token,
            )
            return {'status': 'RETRYING', 'reason': failure_reason}

        # If the part repeatedly fails across workers, fail the job with a useful reason.
        mapping = {
            'status': Status.FAILED.value,
            'error': f"{failure_reason}; retry budget exhausted ({retry_count}/{PART_FAILURE_MAX_RETRIES})",
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
        _cleanup_encode_task_files("failure")
        return {'status': 'FAILED', 'reason': mapping['error']}

    try:
        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted before start")
            return {'status': 'ABORTED'}
        _job_heartbeat(job_id, "encode_prepare", force=True, note=f"part={idx}")

        import requests
        stage = "resolve-stitcher"

        # resolve stitch_host if not provided
        if not stitch_host:
            deadline = _now() + STITCH_HOST_READY_TIMEOUT_SEC
            while _now() < deadline and not stitch_host:
                _job_heartbeat(job_id, "resolve_stitcher", note=f"part={idx}")
                stitch_host = redis.hget(job_key, 'stitch_host') or None
                if stitch_host:
                    break
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] encode {idx}: halted while waiting for stitch_host")
                    return {'status': 'ABORTED'}
                time.sleep(0.25)
            if not stitch_host:
                return _fail(
                    f"stitch_host was not published within {int(STITCH_HOST_READY_TIMEOUT_SEC)}s",
                    stage,
                )

        # Paths in local tmpfs (worker)
        wtmp = _job_base_dir(job_id)
        _ensure_dirs(wtmp)
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
                                _job_heartbeat(job_id, "download", note=f"part={idx}")
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
                f"frames={int(source_start_frame or 0)}:{int(source_end_frame or 0)} "
                f"start={float(source_start_s or 0):.3f}s dur={float(source_duration_s or 0):.3f}s source={source_path}"
            )

        # Build ffmpeg command
        stage = "encode"
        software_encode = int(redis.hget(job_key, 'software_encode') or 0)
        target_height, force_deinterlace = _effective_target_height_for_job(job_key)
        video_encoder = _video_encoder_for_job(job_key)
        try:
            redis.hset(job_key, mapping={
                'target_height_effective': target_height,
                'deinterlace_effective': '1' if force_deinterlace else '0',
                'video_encoder_effective': video_encoder,
            })
        except Exception:
            pass
        input_args = ['-fflags', '+genpts', '-i', in_path]
        map_args = ['-map', '0:v:0', '-an']
        frame_limit_args = []
        hybrid_direct = False
        if direct_source:
            requested_start = max(0.0, float(source_start_s or 0.0))
            requested_duration = max(0.05, float(source_duration_s or 0.0))
            try:
                planned_start_frame = max(0, int(source_start_frame or 0))
                planned_end_frame = max(planned_start_frame, int(source_end_frame or 0))
                planned_frame_count = max(0, int(source_frame_count or 0))
            except Exception:
                planned_start_frame = 0
                planned_end_frame = 0
                planned_frame_count = 0
            hybrid_direct = planned_frame_count > 0 and planned_end_frame > planned_start_frame
            preroll = min(requested_start, DIRECT_SOURCE_SEEK_PREROLL_SEC)
            seek_start = max(0.0, requested_start - preroll)
            trim_start = requested_start - seek_start
            read_duration = trim_start + requested_duration + 2.0
            input_args = [
                '-ss', f"{seek_start:.6f}",
                '-t', f"{read_duration:.6f}",
                '-i', source_path,
            ]
            if hybrid_direct:
                frame_limit_args = ['-frames:v', str(planned_frame_count)]
            map_args = [
                '-map', f'0:v:{int(v_sel)}',
                '-an',
                '-sn', '-dn',
                '-map_metadata', '-1',
                '-map_chapters', '-1',
            ]
        if software_encode:
            software_video_args = (
                ['-c:v', SOFTWARE_H265_ENCODER, '-preset', SOFTWARE_H265_PRESET, '-crf', SOFTWARE_H265_CRF, '-tag:v', 'hvc1']
                if video_encoder == 'h265'
                else ['-c:v', SOFTWARE_H264_ENCODER, '-preset', SOFTWARE_H264_PRESET, '-crf', SOFTWARE_H264_CRF]
            )
            video_filter = (
                _direct_source_hybrid_filter(
                    _software_scale_filter(target_height, deinterlace=force_deinterlace),
                    trim_start_s=trim_start,
                    trim_duration_s=requested_duration,
                    frame_count=source_frame_count,
                )
                if direct_source
                else _reset_segment_video_pts_filter(
                    _software_scale_filter(target_height, deinterlace=force_deinterlace)
                )
            )
            cmd = [
                'ffmpeg','-hide_banner','-nostats','-loglevel','error',
                '-y',
                '-progress','pipe:2',
                *input_args,
                *map_args,
                '-vf', video_filter,
                *frame_limit_args,
                *software_video_args,
                out_path
            ]
        else:
            vaapi_video_args = [
                '-c:v', VAAPI_H265_ENCODER if video_encoder == 'h265' else VAAPI_H264_ENCODER,
                '-rc_mode', VAAPI_RC_MODE,
                '-qp', VAAPI_QP,
            ]
            if video_encoder == 'h265':
                vaapi_video_args.extend(['-tag:v', 'hvc1'])
            video_filter = (
                _direct_source_hybrid_filter(
                    _vaapi_scale_filter(target_height),
                    trim_start_s=trim_start,
                    trim_duration_s=requested_duration,
                    frame_count=source_frame_count,
                )
                if direct_source
                else _reset_segment_video_pts_filter(_vaapi_scale_filter(target_height))
            )
            cmd = [
                'ffmpeg','-hide_banner','-nostats','-loglevel','error',
                '-y',
                '-progress','pipe:2',
                '-vaapi_device', VAAPI_DEVICE,
                *input_args,
                *map_args,
                '-vf', video_filter,
                *frame_limit_args,
                *vaapi_video_args,
                out_path
            ]

        logger.info(f"[{job_id}] encode {idx}: {' '.join(cmd)}")
        _job_heartbeat(job_id, "encode_start", force=True, note=f"part={idx}")

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
                    _job_heartbeat(job_id, "encode", note=f"part={idx}")
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

        if direct_source and source_frame_count:
            expected_frames = int(source_frame_count or 0)
            source_frame_method = redis.hget(job_key, "source_frame_count_method") or ""
            source_frame_exact = _frame_count_method_is_exact(source_frame_method)
            try:
                parts_total_for_validation = int(redis.hget(job_key, "parts_total") or 0)
            except Exception:
                parts_total_for_validation = 0
            actual_frames, frame_details, frame_err, _ = _ffprobe_video_packet_count_details(
                out_path,
                heartbeat_stage="encode_packet_count",
                timeout_sec=FFPROBE_TIMEOUT_SEC,
            )
            if frame_err or actual_frames != expected_frames:
                reason = (
                    f"encoded packet/frame count mismatch for part {idx}: "
                    f"expected={expected_frames} actual={actual_frames or 'unknown'}"
                    f"{('; ' + frame_err) if frame_err else ''}"
                )
                estimated_final_tail = (
                    not source_frame_exact
                    and parts_total_for_validation > 0
                    and int(idx) == parts_total_for_validation
                    and not frame_err
                    and int(actual_frames or 0) > 0
                    and int(actual_frames or 0) <= expected_frames
                )
                if estimated_final_tail:
                    logger.warning("[%s] accepting estimated final tail mismatch: %s", job_id, reason)
                    try:
                        redis.hset(job_key, mapping={
                            f'part_{idx:03d}_expected_frames': expected_frames,
                            f'part_{idx:03d}_actual_frames': actual_frames,
                            f'part_{idx:03d}_frame_details_json': json.dumps(frame_details or {}, separators=(",", ":")),
                            'frame_validation_status': 'estimated_final_tail',
                            'frame_validation_error': reason,
                        })
                    except Exception:
                        pass
                else:
                    logger.error("[%s] %s", job_id, reason)
                    redis.hset(job_key, mapping={
                        'status': Status.FAILED.value,
                        'error': reason,
                        'failed_part': idx,
                        'failed_stage': 'encode_frame_count',
                        'failed_worker': worker_name,
                        'part_frame_count_error': reason,
                        f'part_{idx:03d}_expected_frames': expected_frames,
                        f'part_{idx:03d}_actual_frames': actual_frames or '',
                        f'part_{idx:03d}_frame_details_json': json.dumps(frame_details or {}, separators=(",", ":")),
                        'frame_validation_status': 'failed',
                        'frame_validation_error': reason,
                        'ended_at': _now(),
                    })
                    _cleanup_encode_task_files("frame count mismatch")
                    return {'status': 'FAILED', 'reason': reason}
            if not (frame_err or actual_frames != expected_frames):
                try:
                    redis.hset(job_key, mapping={
                        f'part_{idx:03d}_expected_frames': expected_frames,
                        f'part_{idx:03d}_actual_frames': actual_frames,
                    })
                except Exception:
                    pass
        logger.info(f"[{job_id}] Encoded part {idx}")

        # Upload result to stitcher
        stage = "upload"
        put_url = f"{stitch_host.rstrip('/')}/job/{job_id}/result/{idx}"
        logger.info(f"[{job_id}] PUT result {idx} to {put_url}")
        _job_heartbeat(job_id, "upload_start", force=True, note=f"part={idx}")

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted before uploading encoded part")
            _cleanup_encode_task_files("halted before upload")
            return {'status': 'ABORTED'}

        try:
            with open(out_path, 'rb') as f:
                headers = {'Content-Type':'video/mp4', 'Content-Length': str(os.path.getsize(out_path))}
                r = requests.put(put_url, data=f, headers=headers, timeout=120)
                if r.status_code // 100 != 2:
                    return _fail(f"PUT failed {r.status_code}: {r.text[:300]}", stage)
        except Exception as e:
            return _fail(f"upload failed: {e}", stage)

        if not _task_token_is_current(job_id, run_token, "encode_upload_complete"):
            _cleanup_encode_task_files("stale after upload")
            return {'status': 'STALE', 'job_id': job_id, 'idx': idx}
        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] encode {idx}: halted after upload; skipping progress update")
            _cleanup_encode_task_files("halted after upload")
            return {'status': 'ABORTED'}

        logger.info(f"[{job_id}] Uploaded part {idx}")
        _job_heartbeat(job_id, "upload_complete", force=True, note=f"part={idx}")
        emit_activity(
            f'Encoding "{title}" part {idx} completed in {_elapsed_ms(part_t0)}ms',
            job_id=job_id,
            filename=filename,
            stage='encode',
            source='worker',
        )

        _cleanup_encode_task_files("successful upload")

        # ------- Commit part & update encode stats (one-shot at completion) -------
        try:
            if not _task_token_is_current(job_id, run_token, "encode_progress_update"):
                return {'status': 'STALE', 'job_id': job_id, 'idx': idx}
            if _is_job_halted(job_id):
                return {'status': 'ABORTED'}
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

        _clear_part_inflight(job_id, idx)
        return {'status':'COMPLETED','idx': idx}

    except Exception as e:
        # Catch-all — fail job with a generic reason
        return _fail(f"unexpected error: {e}", stage if 'stage' in locals() else 'unknown')
    finally:
        _cleanup_encode_task_files("task ended")


@encode_huey.task()
def encode_audio(
    job_id: str,
    stitch_host: Optional[str] = None,
    source_path: Optional[str] = None,
    a_sel: int = 0,
    run_token: Optional[str] = None,
):
    """
    Encode the selected source audio once as a continuous track.

    Video chunks are encoded independently and concat-copied later; doing the
    audio once avoids AAC priming/padding artifacts at every video chunk seam.
    """
    job_key = _job_key(job_id)
    if not _task_token_is_current(job_id, run_token, "audio"):
        return {'status': 'STALE', 'job_id': job_id}

    job = redis.hgetall(job_key) or {}
    filename = job.get("filename") or ""
    title = _job_title(job)
    worker_name = WORKER_NAME
    audio_t0 = _now()
    local_audio_path = None

    def _cleanup_audio_task_files(reason: str):
        try:
            if local_audio_path and os.path.exists(local_audio_path):
                os.remove(local_audio_path)
                logger.info("[%s] audio removed %s after %s", job_id, local_audio_path, reason)
        except Exception:
            logger.exception("[%s] audio failed to remove %s after %s", job_id, local_audio_path, reason)
        _cleanup_empty_local_job_dir(job_id, reason=f"audio {reason}", run_token=run_token)

    def _fail(reason: str, stage: str):
        if not _task_token_is_current(job_id, run_token, f"audio_{stage}_fail"):
            _cleanup_audio_task_files("stale failure")
            return {'status': 'STALE', 'job_id': job_id}
        mapping = {
            'status': Status.FAILED.value,
            'error': f"audio {stage}: {reason}",
            'audio_status': 'FAILED',
            'audio_error': reason,
            'failed_stage': f'audio_{stage}',
            'failed_worker': worker_name,
            'ended_at': _now(),
        }
        try:
            redis.hset(job_key, mapping=mapping)
        except Exception:
            pass
        logger.error("[%s] %s", job_id, mapping["error"])
        _cleanup_audio_task_files("failure")
        return {'status': 'FAILED', 'reason': mapping["error"]}

    try:
        if _is_job_halted(job_id):
            logger.warning("[%s] audio: halted before start", job_id)
            return {'status': 'ABORTED'}

        if not _job_has_selected_audio(job):
            redis.hset(job_key, mapping={
                'audio_status': 'NONE',
                'audio_progress': 100,
                'audio_elapsed': 0,
                'audio_path': '',
                'audio_error': '',
            })
            return {'status': 'NO_AUDIO'}

        if not source_path:
            source_path = (job.get('input_path') or '').strip()
        if not source_path or not os.path.exists(source_path):
            alt = os.path.join(WATCH_ROOT, filename.lstrip('/')) if filename else ''
            if alt and os.path.exists(alt):
                source_path = alt
            else:
                return _fail(f"source not found: {filename or source_path or '(empty)'}", "source")

        try:
            a_sel = int(a_sel if a_sel is not None else (job.get('selected_a_stream') or 0))
        except Exception:
            a_sel = 0

        if not stitch_host:
            deadline = _now() + STITCH_HOST_READY_TIMEOUT_SEC
            while _now() < deadline and not stitch_host:
                _job_heartbeat(job_id, "resolve_audio_stitcher")
                stitch_host = redis.hget(job_key, 'stitch_host') or None
                if stitch_host:
                    break
                if _is_job_halted(job_id):
                    logger.warning("[%s] audio: halted while waiting for stitch_host", job_id)
                    return {'status': 'ABORTED'}
                time.sleep(0.25)
            if not stitch_host:
                return _fail(
                    f"stitch_host was not published within {int(STITCH_HOST_READY_TIMEOUT_SEC)}s",
                    "resolve_stitcher",
                )

        wtmp = _job_base_dir(job_id)
        _ensure_dirs(wtmp)
        local_audio_path = os.path.join(wtmp, "source_audio.m4a")
        try:
            source_duration = float(redis.hget(job_key, 'source_duration') or 0.0)
        except Exception:
            source_duration = 0.0
        total_us = int(max(0.0, source_duration) * 1_000_000)

        redis.hset(job_key, mapping={
            'audio_status': 'ENCODING',
            'audio_progress': 0,
            'audio_elapsed': 0,
            'audio_started_at': str(audio_t0),
            'audio_worker': worker_name,
            'audio_error': '',
        })
        _job_heartbeat(job_id, "audio_encode_start", force=True)
        emit_activity(
            f'Encoding continuous audio for "{title}" started',
            job_id=job_id,
            filename=filename,
            stage='audio_encode_start',
            source='worker',
        )

        cmd = [
            'ffmpeg', '-hide_banner', '-nostats', '-loglevel', 'error',
            '-y',
            '-progress', 'pipe:2',
            '-i', source_path,
            '-map', f'0:a:{a_sel}?',
            '-vn', '-sn', '-dn',
            '-map_metadata', '-1',
            '-map_chapters', '-1',
            *AUDIO_ARGS.split(),
            local_audio_path,
        ]
        logger.info("[%s] audio encode cmd: %s", job_id, " ".join(cmd))

        stderr_tail = deque(maxlen=120)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except Exception as e:
            return _fail(f"spawn ffmpeg failed: {e}", "encode")

        try:
            if proc.stderr is not None:
                for raw_line in proc.stderr:
                    line = raw_line.rstrip("\n")
                    if line:
                        stderr_tail.append(line)
                    _job_heartbeat(job_id, "audio_encode")
                    if line.startswith("out_time_ms="):
                        try:
                            out_ms = int(line.split("=", 1)[1] or 0)
                        except Exception:
                            out_ms = 0
                        if total_us > 0:
                            progress = min(99, max(0, int((out_ms / total_us) * 100)))
                            redis.hset(job_key, mapping={
                                'audio_progress': progress,
                                'audio_elapsed': round(_now() - audio_t0, 2),
                            })
                    if _is_job_halted(job_id):
                        logger.warning("[%s] audio: halted while encoding", job_id)
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        _cleanup_audio_task_files("halted")
                        return {'status': 'ABORTED'}
        finally:
            try:
                if proc.stderr:
                    proc.stderr.close()
            except Exception:
                pass
        rc = proc.wait()
        if rc != 0 or not os.path.exists(local_audio_path) or os.path.getsize(local_audio_path) <= 0:
            tail = "\n".join(list(stderr_tail)[-40:])
            return _fail(f"ffmpeg rc={rc}; tail:\n{tail}", "encode")

        if not _task_token_is_current(job_id, run_token, "audio_upload"):
            _cleanup_audio_task_files("stale before upload")
            return {'status': 'STALE', 'job_id': job_id}
        if _is_job_halted(job_id):
            _cleanup_audio_task_files("halted before upload")
            return {'status': 'ABORTED'}

        import requests
        put_url = f"{stitch_host.rstrip('/')}/job/{job_id}/audio/1"
        redis.hset(job_key, mapping={
            'audio_status': 'UPLOADING',
            'audio_progress': 99,
            'audio_elapsed': round(_now() - audio_t0, 2),
        })
        _job_heartbeat(job_id, "audio_upload_start", force=True)
        try:
            with open(local_audio_path, 'rb') as f:
                headers = {'Content-Type': 'audio/mp4', 'Content-Length': str(os.path.getsize(local_audio_path))}
                response = requests.put(put_url, data=f, headers=headers, timeout=180)
                if response.status_code // 100 != 2:
                    return _fail(f"PUT failed {response.status_code}: {response.text[:300]}", "upload")
        except Exception as e:
            return _fail(f"upload failed: {e}", "upload")

        if not _task_token_is_current(job_id, run_token, "audio_upload_complete"):
            _cleanup_audio_task_files("stale after upload")
            return {'status': 'STALE', 'job_id': job_id}
        if _is_job_halted(job_id):
            _cleanup_audio_task_files("halted after upload")
            return {'status': 'ABORTED'}

        redis.hset(job_key, mapping={
            'audio_status': 'READY',
            'audio_progress': 100,
            'audio_elapsed': round(_now() - audio_t0, 2),
            'audio_worker': worker_name,
            'audio_completed_at': str(_now()),
        })
        _job_heartbeat(job_id, "audio_upload_complete", force=True)
        emit_activity(
            f'Encoding continuous audio for "{title}" completed in {_elapsed_ms(audio_t0)}ms',
            job_id=job_id,
            filename=filename,
            stage='audio_encode_complete',
            source='worker',
        )
        _cleanup_audio_task_files("successful upload")
        return {'status': 'COMPLETED'}

    except Exception as e:
        return _fail(f"unexpected error: {e}", "unexpected")
    finally:
        _cleanup_audio_task_files("task ended")



@pipeline_huey.task(retries=999999, retry_delay=5)
def stitch(job_id: str, run_token: Optional[str] = None):
    """
    Stitcher node:
      - Serves PUTs for encoded parts and advertises stitch_host.
      - Waits for all parts with conservative, head-of-line-focused retries.
      - On persistent misses/timeouts -> fail. On success -> concat-copy to final.
    """
    job_key = _job_key(job_id)
    if not _task_token_is_current(job_id, run_token, "stitch"):
        return {'status': 'STALE', 'job_id': job_id}

    stitch_stage_t0 = _now()
    initial_job = redis.hgetall(job_key) or {}
    if not _reserved_host_matches(initial_job, "reserved_stitch_host"):
        return _requeue_misdirected_pipeline_task(
            stitch,
            "stitch",
            job_id,
            "reserved_stitch_host",
            job_id,
            run_token,
        )
    if not _task_token_is_current(job_id, run_token, "stitch"):
        return {'status': 'STALE', 'job_id': job_id}
    title = _job_title(initial_job)
    emit_activity(
        f'Stitching "{title}" started',
        job_id=job_id,
        filename=initial_job.get('filename'),
        stage='stitch_start',
        source='worker',
    )
    _job_heartbeat(job_id, "stitch_start", force=True)
    cleanup_on_exit = True
    try:
        _start_http_once()
        if not _task_token_is_current(job_id, run_token, "stitch"):
            return {'status': 'STALE', 'job_id': job_id}
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
        FINAL_INFLIGHT_GRACE_SEC       = float(ENV("STITCH_FINAL_INFLIGHT_GRACE_SEC", "180"))

        # ---- Wait for parts_total ----
        deadline = _now() + 300
        P = 0
        while _now() < deadline:
            _job_heartbeat(job_id, "stitch_wait_total")
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
            reason = "stitch timed out waiting for parts_total; split/segment task did not publish any parts"
            logger.error(f"[{job_id}] {reason}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'failed_stage': 'stitch_wait_total',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
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
        stitched_frontier = 0
        try:
            stitched_frontier = int(redis.hget(job_key, 'stitched_chunks') or 0)
        except Exception:
            stitched_frontier = 0
        combine_started = 0.0
        try:
            combine_started = float(redis.hget(job_key, 'combine_started') or 0.0)
        except Exception:
            combine_started = 0.0
        stitch_emit_step = max(1, P // 20)
        last_stitch_emit_frontier = stitched_frontier

        def _contiguous_frontier(ready: set[int]) -> int:
            frontier = 0
            for i in range(1, P + 1):
                if i in ready:
                    frontier = i
                else:
                    break
            return frontier

        def _advance_stitch_progress(ready: set[int], force: bool = False) -> int:
            nonlocal stitched_frontier, combine_started, last_stitch_emit_frontier
            frontier = _contiguous_frontier(ready)
            if frontier <= stitched_frontier and not force:
                return stitched_frontier

            now_ts = _now()
            if combine_started <= 0:
                combine_started = now_ts

            stitched_frontier = max(stitched_frontier, frontier)
            progress = int((stitched_frontier / P) * 100) if P else 0
            # Keep 100 reserved for the final concat/remux completion.
            progress = min(99, max(0, progress))
            try:
                current_progress = int(redis.hget(job_key, 'combine_progress') or 0)
            except Exception:
                current_progress = 0
            mapping = {
                'stitched_chunks': stitched_frontier,
                'combine_started': combine_started,
                'combine_elapsed': round(now_ts - combine_started, 2),
                'combine_progress': max(current_progress, progress),
            }
            redis.hset(job_key, mapping=mapping)

            if stitched_frontier > 0:
                _job_heartbeat(
                    job_id,
                    "stitch_sequential",
                    note=f"parts={stitched_frontier}/{P}",
                )
            if (
                stitched_frontier >= P
                or stitched_frontier - last_stitch_emit_frontier >= stitch_emit_step
            ):
                try:
                    current_job = redis.hgetall(job_key) or {}
                    emit_activity(
                        f'Stitching "{_job_title(current_job)}" reached part {stitched_frontier}/{P}',
                        job_id=job_id,
                        filename=current_job.get('filename'),
                        stage='stitch_progress',
                        source='worker',
                    )
                    last_stitch_emit_frontier = stitched_frontier
                except Exception:
                    pass
            return stitched_frontier

        def _inflight_missing(missing: list[int]) -> list[int]:
            active = []
            for part_idx in missing:
                try:
                    if redis.sismember(inflight_key, part_idx):
                        active.append(part_idx)
                except Exception:
                    pass
            return active

        def _recent_retry_missing(missing: list[int], now_ts: float) -> list[int]:
            recent = []
            for part_idx in missing:
                try:
                    last_retry = float(redis.hget(retry_ts_key, part_idx) or 0.0)
                except Exception:
                    last_retry = 0.0
                if last_retry > 0 and (now_ts - last_retry) < FINAL_INFLIGHT_GRACE_SEC:
                    recent.append(part_idx)
            return recent

        # Helper: schedule (re)encode conservatively
        def _retry_part(idx: int):
            if not _task_token_is_current(job_id, run_token, "stitch"):
                return False
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
                direct_fps = _selected_video_fps_from_job(job)
                try:
                    direct_total_frames = int(job.get("total_frames") or 0)
                except Exception:
                    direct_total_frames = 0
                start_s, part_dur, part_frame_count = _direct_source_plan_for_part(
                    src_dur,
                    seg_dur,
                    idx,
                    fps=direct_fps,
                    total_frames=direct_total_frames,
                    total_parts=P,
                )
                if start_s is None or part_dur is None or not source_path:
                    logger.error(f"[{job_id}] stitch: cannot retry direct-source part {idx}; missing segment plan")
                    redis.srem(inflight_key, idx)
                    return False
                _dispatch_encode_part(
                    job_id,
                    idx,
                    master_host,
                    v_sel,
                    a_sel,
                    stitch_host=stitch_host,
                    source_path=source_path,
                    source_start_s=start_s,
                    source_duration_s=part_dur,
                    source_frame_count=part_frame_count,
                    run_token=run_token,
                )
            else:
                _dispatch_encode_part(job_id, idx, master_host, v_sel, a_sel, stitch_host=stitch_host, run_token=run_token)

            now_ts = _now()
            pipe = redis.pipeline()
            pipe.hincrby(retry_cnt_key, idx, 1)
            pipe.hset(retry_ts_key, idx, now_ts)
            pipe.execute()
            return True

        while _now() < wait_deadline:
            _job_heartbeat(job_id, "stitch_wait_parts")
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] stitch aborted (job halted)")
                return {'status':'ABORTED'}
            if not _task_token_is_current(job_id, run_token, "stitch"):
                return {'status': 'STALE', 'job_id': job_id}

            ready = _ready_set()
            done_fs = len(ready)
            frontier = _advance_stitch_progress(ready)

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
                    try:
                        if redis.sismember(inflight_key, idx):
                            continue
                    except Exception:
                        pass
                    # give extra grace after last retry proportional to part estimate
                    if (now_ts - max(last_retry, first_seen)) > max(2 * est_part_secs, STALL_BEFORE_RETRY_SEC):
                        early_fail = True
                        logger.error(f"[{job_id}] stitch: giving up on part {idx} after {cnt} retries")
                        break

            if early_fail:
                break

            # pacing
            time.sleep(0.5 if dispatched == 0 else 1.0)

        # Final readiness check. Do not fail while retry uploads are still in
        # flight or while their files may not yet be visible/stable locally.
        final_ready = _ready_set()
        try:
            if final_ready:
                redis.srem(inflight_key, *list(final_ready))
        except Exception:
            pass
        _advance_stitch_progress(final_ready, force=True)
        final_wait_until = _now() + FINAL_INFLIGHT_GRACE_SEC
        while len(final_ready) < P and _now() < final_wait_until:
            if _is_job_halted(job_id):
                logger.warning(f"[{job_id}] stitch aborted during final inflight grace")
                return {'status': 'ABORTED'}
            if not _task_token_is_current(job_id, run_token, "stitch"):
                return {'status': 'STALE', 'job_id': job_id}

            missing_now = sorted(expected - final_ready)
            inflight_now = _inflight_missing(missing_now)
            recent_retry_now = _recent_retry_missing(missing_now, _now())
            if not inflight_now and not recent_retry_now:
                break

            _job_heartbeat(
                job_id,
                "stitch_final_inflight_wait",
                note=(
                    f"missing={missing_now[:12]} "
                    f"inflight={inflight_now[:12]} recent_retry={recent_retry_now[:12]}"
                ),
            )
            time.sleep(0.5)
            final_ready = _ready_set()
            try:
                if final_ready:
                    redis.srem(inflight_key, *list(final_ready))
            except Exception:
                pass
            _advance_stitch_progress(final_ready, force=True)

        if len(final_ready) < P:
            missing = sorted(expected - final_ready)
            reason = f"missing encoded parts after conservative retries: {missing}"
            logger.error(f"[{job_id}] stitch timeout or retries exhausted; missing: {missing}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'failed_stage': 'stitch_wait_parts',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            try:
                redis.delete(f"job_done_parts:{job_id}")
                redis.delete(retry_cnt_key)
                redis.delete(retry_ts_key)
                redis.delete(miss_seen_key)
                redis.delete(inflight_key)
            except Exception:
                pass
            return {'status':'FAILED','reason': reason}

        # ---- Stitch (concat copy) ----
        job = redis.hgetall(job_key) or {}
        base_dir = _job_base_dir(job_id, job)
        concat_path = os.path.join(base_dir, "concat.txt")
        enc_paths = [os.path.join(enc_dir, f"enc_{i:03d}.mp4") for i in range(1, P + 1)]
        segment_durations = _segment_manifest_durations(job_id, job, expected_parts=P)
        expected_video_duration = sum(segment_durations) if len(segment_durations) == P else 0.0
        with open(concat_path, 'w') as f:
            if segment_durations:
                f.write("ffconcat version 1.0\n")
            for idx, pth in enumerate(enc_paths):
                f.write(f"file '{pth}'\n")
                if idx < len(segment_durations):
                    f.write(f"duration {segment_durations[idx]:.6f}\n")
        redis.hset(job_key, mapping={
            'segment_manifest_path': _segment_manifest_path(job_id, job) if segment_durations else '',
            'segment_manifest_count': len(segment_durations),
            'stitch_expected_video_duration': f'{expected_video_duration:.6f}' if expected_video_duration else '',
        })

        local_out = os.path.join(base_dir, f"job_{job_id}_output.mp4")
        try:
            combine_t0 = float(redis.hget(job_key, 'combine_started') or 0.0)
        except Exception:
            combine_t0 = 0.0
        if combine_t0 <= 0:
            combine_t0 = _now()
            redis.hset(job_key, 'combine_started', combine_t0)
        try:
            current_combine_progress = int(redis.hget(job_key, 'combine_progress') or 0)
        except Exception:
            current_combine_progress = 0
        redis.hset(job_key, mapping={
            'combine_progress': max(0, current_combine_progress),
            'combine_elapsed': round(_now() - combine_t0, 2),
        })

        total_us = int((expected_video_duration or src_dur or 0) * 1_000_000)
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
                    _job_heartbeat(job_id, "stitch_combine")
                    time.sleep(0.05)
                    continue
                _job_heartbeat(job_id, "stitch_combine", note="ffmpeg-progress")
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
                        try:
                            current_pct = int(redis.hget(job_key, 'combine_progress') or 0)
                        except Exception:
                            current_pct = 0
                        redis.hset(job_key, mapping={
                            'combine_progress': max(current_pct, pct),
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
            reason = f"stitch failed (rc={rc}): {err_tail}".strip()
            logger.error(f"[{job_id}] {reason}")
            redis.hset(job_key, mapping={
                'status': Status.FAILED.value,
                'error': reason,
                'failed_stage': 'stitch',
                'failed_worker': WORKER_NAME,
                'ended_at': _now(),
            })
            return {'status': 'FAILED', 'reason': reason}

        if _is_job_halted(job_id):
            logger.warning(f"[{job_id}] stitch halted before moving file")
            return {'status': 'ABORTED'}

        job = redis.hgetall(job_key) or {}
        try:
            source_frame_count = int(job.get("frame_plan_total_frames") or job.get("total_frames") or 0)
        except Exception:
            source_frame_count = 0
        source_frame_method = job.get("source_frame_count_method") or ""
        source_frame_exact = _frame_count_method_is_exact(source_frame_method)
        if source_frame_count > 0:
            final_video_frame_count, final_frame_details, final_frame_err, _ = _ffprobe_video_packet_count_details(
                local_out,
                heartbeat_stage="stitch_packet_count",
                job_id=job_id,
                timeout_sec=FFPROBE_TIMEOUT_SEC,
            )
            final_delta = (final_video_frame_count - source_frame_count) if final_video_frame_count else 0
            redis.hset(job_key, mapping={
                'final_video_frame_count': str(final_video_frame_count or ''),
                'final_video_frame_delta': str(final_delta) if final_video_frame_count else '',
                'final_video_frame_details_json': json.dumps(final_frame_details or {}, separators=(",", ":")),
                'frame_validation_status': 'stitched',
                'frame_validation_error': final_frame_err or '',
            })
            if final_frame_err or final_video_frame_count != source_frame_count:
                reason = (
                    "stitched video packet/frame count mismatch: "
                    f"expected={source_frame_count} actual={final_video_frame_count or 'unknown'}"
                    f"{('; ' + final_frame_err) if final_frame_err else ''}"
                )
                if not source_frame_exact:
                    logger.warning(
                        "[%s] accepting stitched frame count delta because source count is estimated (%s): %s",
                        job_id,
                        source_frame_method or "unknown",
                        reason,
                    )
                    redis.hset(job_key, mapping={
                        'frame_validation_status': 'stitched_estimated',
                        'frame_validation_error': reason,
                    })
                else:
                    logger.error(f"[{job_id}] {reason}")
                    redis.hset(job_key, mapping={
                        'status': Status.FAILED.value,
                        'error': reason,
                        'failed_stage': 'stitch_frame_count',
                        'failed_worker': WORKER_NAME,
                        'preserve_project_dir': '1',
                        'preserve_project_dir_reason': 'stitch_frame_count_diagnostic',
                        'frame_validation_status': 'failed',
                        'frame_validation_error': reason,
                        'ended_at': _now(),
                    })
                    cleanup_on_exit = False
                    return {'status': 'FAILED', 'reason': reason}

        # New pipeline mode: encoded chunks are video-only; audio is encoded
        # once from the original source and uploaded as a single continuous m4a.
        audio_status = str(job.get("audio_status") or "").strip().upper()
        audio_path = _audio_path(job_id)
        if audio_status and audio_status != "NONE":
            audio_deadline = _now() + max(300.0, (src_dur or 0) * 1.5)
            while _now() < audio_deadline:
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] stitch halted while waiting for continuous audio")
                    return {'status': 'ABORTED'}
                if not _task_token_is_current(job_id, run_token, "stitch_audio_wait"):
                    return {'status': 'STALE', 'job_id': job_id}

                job = redis.hgetall(job_key) or {}
                audio_status = str(job.get("audio_status") or "").strip().upper()
                if audio_status == "READY" and os.path.exists(audio_path) and os.path.getsize(audio_path) > 0:
                    break
                if audio_status == "FAILED":
                    reason = job.get("audio_error") or "continuous audio encode failed"
                    redis.hset(job_key, mapping={
                        'status': Status.FAILED.value,
                        'error': f"audio failed before mux: {reason}",
                        'failed_stage': 'audio_mux_wait',
                        'failed_worker': WORKER_NAME,
                        'ended_at': _now(),
                    })
                    return {'status': 'FAILED', 'reason': reason}
                _job_heartbeat(job_id, "audio_wait", note=f"status={audio_status or 'unknown'}")
                time.sleep(0.5)

            if audio_status != "READY" or not os.path.exists(audio_path) or os.path.getsize(audio_path) <= 0:
                reason = f"continuous audio was not ready before timeout; status={audio_status or 'unknown'}"
                logger.error(f"[{job_id}] {reason}")
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': reason,
                    'failed_stage': 'audio_mux_wait',
                    'failed_worker': WORKER_NAME,
                    'ended_at': _now(),
                })
                return {'status': 'FAILED', 'reason': reason}

            muxed_out = os.path.join(base_dir, f"job_{job_id}_output_av.mp4")
            stitched_video_duration = _ffprobe_duration(local_out)
            audio_duration = _ffprobe_duration(audio_path)
            target_mux_duration = audio_duration if audio_duration > 0 else (
                expected_video_duration if expected_video_duration > 0 else (src_dur if src_dur and src_dur > 0 else 0.0)
            )
            video_input_args = []
            if stitched_video_duration > 0 and target_mux_duration > 0:
                drift_sec = stitched_video_duration - target_mux_duration
                if abs(drift_sec) <= 0.05:
                    redis.hset(job_key, mapping={
                        'video_duration_before_audio_mux': f'{stitched_video_duration:.6f}',
                        'audio_mux_target_duration': f'{target_mux_duration:.6f}',
                        'audio_mux_sync_correction_sec': f'{drift_sec:.6f}',
                        'audio_mux_video_itsscale': '',
                        'audio_mux_sync_mode': 'none',
                    })
                else:
                    allowed_stretch_sec = max(
                        AUDIO_MUX_MAX_TIMESTAMP_STRETCH_SEC,
                        target_mux_duration * AUDIO_MUX_MAX_TIMESTAMP_STRETCH_PCT,
                    )
                    if abs(drift_sec) > allowed_stretch_sec:
                        try:
                            source_frame_count = int(job.get("total_frames") or 0)
                        except Exception:
                            source_frame_count = 0
                        video_frame_count, video_frame_details, frame_count_error, _ = _ffprobe_video_frame_count_details(
                            local_out,
                            job_id=job_id,
                            timeout_sec=AUDIO_MUX_FRAME_COUNT_TIMEOUT_SEC,
                        )
                        frame_tolerance = max(
                            AUDIO_MUX_FRAME_COUNT_TOLERANCE_FRAMES,
                            int(source_frame_count * AUDIO_MUX_FRAME_COUNT_TOLERANCE_PCT) if source_frame_count > 0 else 0,
                        )
                        frame_delta = video_frame_count - source_frame_count if video_frame_count and source_frame_count else 0
                        redis.hset(job_key, mapping={
                            'video_duration_before_audio_mux': f'{stitched_video_duration:.6f}',
                            'audio_mux_target_duration': f'{target_mux_duration:.6f}',
                            'audio_mux_sync_correction_sec': f'{drift_sec:.6f}',
                            'audio_mux_video_frame_count': str(video_frame_count or ''),
                            'audio_mux_source_frame_count': str(source_frame_count or ''),
                            'audio_mux_frame_count_delta': str(frame_delta) if video_frame_count and source_frame_count else '',
                            'audio_mux_frame_count_tolerance': str(frame_tolerance),
                            'audio_mux_frame_count_error': frame_count_error or '',
                            'audio_mux_video_frame_details_json': json.dumps(video_frame_details or {}, separators=(",", ":")),
                        })
                        reason = (
                            "unsafe continuous audio mux drift: "
                            f"video={stitched_video_duration:.6f}s "
                            f"target={target_mux_duration:.6f}s "
                            f"drift={drift_sec:.6f}s "
                            f"max={allowed_stretch_sec:.6f}s "
                            f"frames={video_frame_count or 'unknown'}/{source_frame_count or 'unknown'} "
                            f"delta={frame_delta if video_frame_count and source_frame_count else 'unknown'}"
                        )
                        logger.error(f"[{job_id}] {reason}")
                        redis.hset(job_key, mapping={
                            'status': Status.FAILED.value,
                            'error': reason,
                            'audio_status': 'FAILED',
                            'audio_error': reason,
                            'audio_mux_video_itsscale': '',
                            'audio_mux_sync_mode': 'failed_duration_drift',
                            'preserve_project_dir': '1',
                            'preserve_project_dir_reason': 'audio_mux_sync_duration_diagnostic',
                            'failed_stage': 'audio_mux_sync',
                            'failed_worker': WORKER_NAME,
                            'ended_at': _now(),
                        })
                        cleanup_on_exit = False
                        return {'status': 'FAILED', 'reason': reason}
                    redis.hset(job_key, mapping={
                        'video_duration_before_audio_mux': f'{stitched_video_duration:.6f}',
                        'audio_mux_target_duration': f'{target_mux_duration:.6f}',
                        'audio_mux_sync_correction_sec': f'{drift_sec:.6f}',
                        'audio_mux_video_itsscale': '',
                        'audio_mux_sync_mode': 'duration_tolerated_no_scale',
                    })
                    logger.warning(
                        "[%s] tolerating small mux duration difference without retiming video: video=%.6fs target_audio=%.6fs drift=%.6fs max=%.6fs",
                        job_id,
                        stitched_video_duration,
                        target_mux_duration,
                        drift_sec,
                        allowed_stretch_sec,
                    )
            audio_mux_cmd = [
                'ffmpeg', '-hide_banner',
                '-nostats', '-loglevel', 'error',
                '-y',
                *video_input_args,
                '-i', local_out,
                '-i', audio_path,
                '-map', '0:v:0',
                '-map', '1:a:0',
                '-c:v', 'copy',
                '-c:a', 'copy',
                '-map_metadata', '-1',
                '-map_chapters', '-1',
                '-movflags', '+faststart',
                '-progress', 'pipe:1',
                muxed_out,
            ]
            logger.info(f"[{job_id}] Audio mux cmd: {' '.join(audio_mux_cmd)}")
            emit_activity(
                f'Muxing continuous audio for "{_job_title(job)}"',
                job_id=job_id,
                filename=job.get('filename'),
                stage='audio_mux',
                source='worker',
            )
            redis.hset(job_key, mapping={'audio_status': 'MUXING'})
            audio_mux = subprocess.Popen(audio_mux_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                while True:
                    line = audio_mux.stdout.readline() if audio_mux.stdout else ''
                    if not line:
                        if audio_mux.poll() is not None:
                            break
                        _job_heartbeat(job_id, "audio_mux")
                        time.sleep(0.05)
                        continue
                    _job_heartbeat(job_id, "audio_mux", note="ffmpeg-progress")
                    if _is_job_halted(job_id):
                        logger.warning(f"[{job_id}] stitch halted while muxing continuous audio")
                        audio_mux.terminate()
                        try:
                            audio_mux.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            audio_mux.kill()
                        return {'status': 'ABORTED'}
                    if line.startswith('progress=') and line.split('=', 1)[1].strip() == 'end':
                        break
            finally:
                audio_rc = audio_mux.wait()
            if audio_rc != 0 or not os.path.exists(muxed_out):
                err_tail = ''
                try:
                    err_tail = (audio_mux.stderr.read() or '')[-1000:]
                except Exception:
                    pass
                reason = f"continuous audio mux failed (rc={audio_rc}): {err_tail}".strip()
                logger.error(f"[{job_id}] {reason}")
                redis.hset(job_key, mapping={
                    'status': Status.FAILED.value,
                    'error': reason,
                    'audio_status': 'FAILED',
                    'audio_error': reason,
                    'failed_stage': 'audio_mux',
                    'failed_worker': WORKER_NAME,
                    'ended_at': _now(),
                })
                return {'status': 'FAILED', 'reason': reason}

            try:
                os.remove(local_out)
            except Exception:
                pass
            local_out = muxed_out
            redis.hset(job_key, mapping={
                'audio_status': 'MUXED',
                'audio_progress': 100,
                'audio_muxed_at': str(_now()),
            })

        # Decide whether to remux English subtitles from the source into the final file.
        job = redis.hgetall(job_key) or {}
        src_filename = job.get('filename') or os.path.basename(job.get('input_path','') or '')
        src_input_path = (job.get('input_path') or '').strip()
        english_subtitle_streams = _source_english_subtitle_streams(src_input_path)
        supported_english_subtitle_streams = [
            s for s in english_subtitle_streams
            if _subtitle_codec_mkv_copy_safe(s.get("codec_name"))
        ]
        unsupported_english_subtitle_streams = [
            s for s in english_subtitle_streams
            if not _subtitle_codec_mkv_copy_safe(s.get("codec_name"))
        ]
        emit_activity(
            f'Writing "{_job_title(job)}"',
            job_id=job_id,
            filename=src_filename,
            stage='write',
            source='worker',
        )
        final_extension = ".mkv" if supported_english_subtitle_streams else ".mp4"
        final_path = _final_output_path_with_ext(src_filename, final_extension)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        subtitle_warning = ""
        if unsupported_english_subtitle_streams:
            skipped_codecs = sorted({
                str(stream.get("codec_name") or "unknown").strip().lower() or "unknown"
                for stream in unsupported_english_subtitle_streams
            })
            subtitle_warning = f"Skipped unsupported English subtitle codecs: {', '.join(skipped_codecs)}"
            logger.warning(f"[{job_id}] {subtitle_warning}")
            emit_activity(
                f'Skipped unsupported English subtitles for "{_job_title(job)}": {", ".join(skipped_codecs)}',
                job_id=job_id,
                filename=src_filename,
                stage='subtitle_warning',
                source='worker',
            )
        if supported_english_subtitle_streams:
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
                "-c:v",
                "copy",
                "-c:a",
                "copy",
                "-c:s",
                "copy",
            ]
            for stream in supported_english_subtitle_streams:
                try:
                    stream_index = int(stream.get("index"))
                except Exception:
                    continue
                subtitle_cmd.extend(["-map", f"1:{stream_index}"])
            subtitle_cmd.extend(["-progress", "pipe:1"])
            subtitle_cmd.append(subtitle_mux_path)
            logger.info(f"[{job_id}] Subtitle remux cmd: {' '.join(subtitle_cmd)}")
            subtitle_stderr_tail = deque(maxlen=80)
            subtitle_mux = subprocess.Popen(
                subtitle_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            while True:
                if _is_job_halted(job_id):
                    logger.warning(f"[{job_id}] stitch halted during subtitle remux")
                    subtitle_mux.terminate()
                    try:
                        subtitle_mux.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        subtitle_mux.kill()
                    return {'status': 'ABORTED'}

                ready_stdout = []
                ready_stderr = []
                try:
                    streams = [s for s in (subtitle_mux.stdout, subtitle_mux.stderr) if s is not None]
                    if streams:
                        ready_stdout, _, _ = select.select(streams, [], [], 1.0)
                except Exception:
                    ready_stdout = []

                if not ready_stdout:
                    _job_heartbeat(job_id, "subtitle_mux", note="waiting")
                    if subtitle_mux.poll() is not None:
                        break
                    continue

                for stream in ready_stdout:
                    line = stream.readline()
                    if not line:
                        continue
                    if stream is subtitle_mux.stderr:
                        subtitle_stderr_tail.append(line.rstrip())
                    else:
                        _job_heartbeat(job_id, "subtitle_mux", note="ffmpeg-progress")

                if subtitle_mux.poll() is not None:
                    break

            subtitle_rc = subtitle_mux.wait()
            try:
                if subtitle_mux.stderr:
                    for line in subtitle_mux.stderr.readlines():
                        if line:
                            subtitle_stderr_tail.append(line.rstrip())
            except Exception:
                pass
            if subtitle_rc != 0 or not os.path.exists(subtitle_mux_path):
                err_tail = "\n".join(subtitle_stderr_tail)[-1200:]
                subtitle_warning = f"subtitle remux failed; output kept without subtitles: {err_tail or subtitle_rc}"
                logger.warning(f"[{job_id}] {subtitle_warning}")
                emit_activity(
                    f'Finished "{_job_title(job)}" without subtitles after remux failure',
                    job_id=job_id,
                    filename=src_filename,
                    stage='subtitle_warning',
                    source='worker',
                )
                final_path = _final_output_path_with_ext(src_filename, ".mp4")
                if os.path.exists(subtitle_mux_path):
                    try:
                        os.remove(subtitle_mux_path)
                    except Exception:
                        pass
                shutil.move(local_out, final_path)
            else:
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
                'english_subtitles_found': str(len(english_subtitle_streams)),
                'english_subtitles_supported': str(len(supported_english_subtitle_streams)),
                'english_subtitles_kept': '1' if final_path.endswith('.mkv') and bool(supported_english_subtitle_streams) else '0',
                'subtitle_warning': subtitle_warning,
            })
        except Exception:
            pass

        _cleanup_local_job_dir(job_id, job, reason="stitch success", run_token=run_token)
        cleanup_on_exit = False

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
        redis.hset(job_key, mapping={
            'status': Status.FAILED.value,
            'error': str(e),
            'failed_stage': 'stitch',
            'failed_worker': WORKER_NAME,
            'ended_at': _now(),
        })
        return {'status': 'FAILED', 'error': str(e)}
    finally:
        if cleanup_on_exit:
            _cleanup_local_job_dir(job_id, initial_job, reason="stitch task ended", run_token=run_token)

@pipeline_huey.task()
def stamp(job_id: str, run_token: Optional[str] = None):
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
    if not _task_token_is_current(job_id, run_token, "stamp"):
        return {'status': 'STALE', 'job_id': job_id}

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
    _job_heartbeat(job_id, "stamp_start", force=True)

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
                _job_heartbeat(job_id, "stamp")
                time.sleep(0.05)
                continue

            _job_heartbeat(job_id, "stamp", note="ffmpeg-progress")
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


def enqueue_split_on_host(host: str, job_id: str, file_path: str, run_token: Optional[str] = None):
    return _enqueue_pipeline_task_for_host(split, host, job_id, file_path, run_token)


def enqueue_stitch_on_host(host: str, job_id: str, run_token: Optional[str] = None):
    return _enqueue_pipeline_task_for_host(stitch, host, job_id, run_token)


def enqueue_stamp_on_host(host: str, job_id: str, run_token: Optional[str] = None):
    return _enqueue_pipeline_task_for_host(stamp, host, job_id, run_token)
