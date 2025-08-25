#!/usr/bin/env python3
"""
Agent: publishes node metrics/identity to Redis once per second and (optionally)
suspends the machine when idle. Right before suspending, it garbage-collects
stale per-job project directories whose names look like UUID/GUIDs.

Change requested:
- Report ONLY the percent used of the primary drive as a metric named "disk".
  (Primary drive defaults to "/"; override with DISK_PATH env var.)
"""

import os
import time
import json
import socket
import subprocess
import signal
import re
import shutil
import psutil

import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

import logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [agent] %(message)s"
)
log = logging.getLogger("agent")

# ---------------- Env / Config ----------------
REDIS_HOST = os.getenv("REDIS_HOST", "swarm3")  # central Redis host
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "1"))
TTL_SEC    = int(os.getenv("TTL_SEC", "15"))

# Optional overrides for interface/MAC detection
AGENT_IFACE = os.getenv("AGENT_IFACE", "").strip()  # e.g. "enp1s0"
AGENT_MAC   = os.getenv("AGENT_MAC", "").strip()    # if set, use this MAC as-is

# --- Optional suspend-on-idle controls ---
SUSPEND_ENABLED           = str(os.getenv("SUSPEND_ENABLED", "1")).lower() in ("1", "true", "yes", "on")
SUSPEND_AFTER_IDLE_SEC    = int(os.getenv("SUSPEND_AFTER_IDLE_SEC", "300"))   # 5 minutes
IDLE_CPU_PCT_MAX          = float(os.getenv("IDLE_CPU_PCT_MAX", "10"))        # <= 10% CPU considered idle
IDLE_GPU_PCT_MAX          = float(os.getenv("IDLE_GPU_PCT_MAX", "10"))        # <= 10% GPU considered idle
MIN_UPTIME_BEFORE_SUSPEND = int(os.getenv("MIN_UPTIME_BEFORE_SUSPEND", "300"))# don't suspend within five minutes

# --- Disk usage metric (single value) ---
DISK_PATH = os.getenv("DISK_PATH", "/")  # which mount to consider the "primary drive"

# --- Garbage collection of stale project dirs (per-job UUID folders) ---
GC_ENABLED        = str(os.getenv("GC_ENABLED", "1")).lower() in ("1", "true", "yes", "on")
# Default roots include your manual path and the pipeline's typical /projects
GC_ROOTS          = [p for p in os.getenv("GC_ROOTS", "/opt/jerry/current,/projects").split(",") if p.strip()]
GC_MIN_AGE_SEC    = int(os.getenv("GC_MIN_AGE_SEC", "1800"))   # only delete if older than 30 minutes
GC_MAX_DELETE     = int(os.getenv("GC_MAX_DELETE", "20"))      # cap per sweep
UUID_RE           = re.compile(r'^[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$')

HOSTNAME = os.getenv("HOSTNAME", socket.gethostname())  # e.g. thinman07
KEY      = f"metrics:node:{HOSTNAME}"

r = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True,
    socket_keepalive=True, socket_timeout=5, socket_connect_timeout=5,
    health_check_interval=30, retry_on_timeout=True,
    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
)

# ---------------- GPU sampling helpers ----------------
def _extract_video_busy(sample: dict):
    if not isinstance(sample, dict): return None
    eng = sample.get("engines")
    if not isinstance(eng, dict): return None
    v0 = eng.get("Video/0")
    if isinstance(v0, dict) and "busy" in v0:
        try: return float(v0["busy"])
        except: pass
    for name, rec in eng.items():
        if isinstance(name, str) and name.startswith("Video/") and isinstance(rec, dict) and "busy" in rec:
            try: return float(rec["busy"])
            except: continue
    return None

def _last_json_object(stream_text: str):
    if not stream_text: return None
    depth = 0; buf = []; last_obj = None; in_obj = False
    for ch in stream_text:
        if ch == '{':
            depth += 1; in_obj = True
        if in_obj: buf.append(ch)
        if ch == '}':
            depth -= 1
            if in_obj and depth == 0:
                raw = ''.join(buf).strip()
                buf.clear(); in_obj = False
                try: last_obj = json.loads(raw)
                except: last_obj = None
    return last_obj

def sample_gpu_percent():
    """
    Start intel_gpu_top -J streaming, read briefly, parse one sample, then stop it.
    Returns 0..100 float, or None if unavailable.
    """
    try:
        proc = subprocess.Popen(
            ["intel_gpu_top", "-J", "-s", "200"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
        )
    except FileNotFoundError:
        return None
    except Exception:
        return None

    buf = ""
    deadline = time.time() + 1.2  # ~1s budget
    gpu_busy = None

    try:
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            chunk = proc.stdout.read(4096) if proc.stdout else ""
            if not chunk:
                time.sleep(0.05)
                continue
            buf += chunk
            obj = _last_json_object(buf)
            if obj:
                val = _extract_video_busy(obj)
                if val is not None:
                    gpu_busy = max(0.0, min(100.0, float(val)))
                    break
    finally:
        try:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass

    return gpu_busy

# ---------------- Network identity (IP/MAC) helpers ----------------
def _norm_mac_colon(mac: str) -> str:
    """
    Normalize to 'aa:bb:cc:dd:ee:ff' if possible; otherwise return lowercased input.
    """
    if not mac:
        return ""
    hexonly = re.sub(r'[^0-9A-Fa-f]', '', mac)
    if len(hexonly) == 12:
        return ":".join(hexonly[i:i+2] for i in range(0, 12, 2)).lower()
    return mac.strip().lower()

def _read_sysfs_mac(iface: str) -> str:
    try:
        with open(f"/sys/class/net/{iface}/address", "r") as f:
            return _norm_mac_colon(f.read().strip())
    except Exception:
        return ""

def _primary_ip_and_iface():
    """
    Try to determine primary IPv4 and interface using iproute2.
    Fallback to psutil scanning for the first UP, non-loopback IPv4 interface.
    Returns (ip, iface) or ("","") on failure.
    """
    # iproute2 approach (fast, common)
    try:
        pr = subprocess.run(
            ["ip", "-4", "route", "get", "1.1.1.1"],
            capture_output=True, text=True, timeout=1.0
        )
        if pr.returncode == 0:
            out = pr.stdout or ""
            ip_match  = re.search(r"\bsrc\s+(\d+\.\d+\.\d+\.\d+)", out)
            dev_match = re.search(r"\bdev\s+(\S+)", out)
            ip    = ip_match.group(1)  if ip_match  else ""
            iface = dev_match.group(1) if dev_match else ""
            if ip and iface:
                return (ip, iface)
    except Exception:
        pass

    # psutil fallback
    try:
        stats = psutil.net_if_stats()
        addrs = psutil.net_if_addrs()
        for ifname, infos in addrs.items():
            st = stats.get(ifname)
            if st and not st.isup:
                continue
            if ifname.startswith(("lo", "docker", "veth", "br-", "kube", "flannel", "cni", "tailscale")):
                continue
            ip = ""
            for a in infos:
                if a.family == socket.AF_INET and a.address and not a.address.startswith("127."):
                    ip = a.address
                    break
            if ip:
                return (ip, ifname)
    except Exception:
        pass
    return ("", "")

def _detect_ip_and_mac():
    """
    Determine the primary IP and MAC to report.
    Respects AGENT_IFACE/AGENT_MAC if provided.
    """
    # Overrides
    if AGENT_IFACE:
        ip = ""
        try:
            for a in psutil.net_if_addrs().get(AGENT_IFACE, []):
                if a.family == socket.AF_INET and a.address and not a.address.startswith("127."):
                    ip = a.address; break
        except Exception:
            ip = ""
        mac = AGENT_MAC or _read_sysfs_mac(AGENT_IFACE)
        return (ip, mac)

    # Auto-detect
    ip, iface = _primary_ip_and_iface()
    mac = AGENT_MAC or (_read_sysfs_mac(iface) if iface else "")
    return (ip, mac)

def _uptime_seconds() -> float:
    try:
        return time.time() - psutil.boot_time()
    except Exception:
        return 10**9

# ---------------- Disk metric ----------------
def _disk_percent_used(path: str = "/") -> float:
    """Return percent-used (0..100) of the filesystem containing 'path'."""
    try:
        usage = shutil.disk_usage(path)
        if usage.total <= 0:
            return 0.0
        return round(usage.used / usage.total * 100.0, 2)
    except Exception as e:
        log.debug("disk metric failed for %s: %s", path, e)
        return 0.0

# ---------------- GC helpers ----------------
def _dir_mtime(path: str) -> float:
    try:
        return os.stat(path, follow_symlinks=False).st_mtime
    except Exception:
        return 0.0

def _job_is_active(job_id: str) -> bool:
    """
    Use Redis job state (if present) to avoid deleting an in-flight project dir.
    STARTING/RUNNING -> active.
    If DONE/FAILED/STOPPED and ended_at is recent (< GC_MIN_AGE_SEC), treat as active.
    """
    try:
        key = f"job:{job_id}"
        if not r.exists(key):
            return False
        j = r.hgetall(key) or {}
        status = (j.get("status") or "").upper()
        if status in ("STARTING", "RUNNING"):
            return True
        ended = j.get("ended_at") or j.get("created_at") or "0"
        try:
            ended_ts = float(ended)
        except Exception:
            ended_ts = 0.0
        if ended_ts > 0 and (time.time() - ended_ts) < GC_MIN_AGE_SEC:
            return True
    except Exception:
        # if Redis is unavailable or unexpected data, err on the safe side: treat as active
        return True
    return False

def _gc_sweep_once():
    if not GC_ENABLED:
        return
    now = time.time()
    deleted = 0
    for root in GC_ROOTS:
        root = root.strip()
        if not root or not os.path.isdir(root):
            continue
        try:
            with os.scandir(root) as it:
                for entry in it:
                    if deleted >= GC_MAX_DELETE:
                        return
                    try:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        name = entry.name
                        if not UUID_RE.match(name):
                            continue
                        path = os.path.join(root, name)
                        # Recent activity? Skip until older than threshold
                        age = now - _dir_mtime(path)
                        if age < GC_MIN_AGE_SEC:
                            continue
                        # Cross-check Redis to avoid deleting an active job dir
                        if _job_is_active(name):
                            continue
                        shutil.rmtree(path)
                        deleted += 1
                        log.info("gc: removed stale project dir %s (age=%ds)", path, int(age))
                    except FileNotFoundError:
                        continue
                    except Exception as e:
                        log.warning("gc: failed to remove %s: %s", entry.path, e)
        except Exception as e:
            log.warning("gc: scan failed for %s: %s", root, e)

# ---------------- Main loop: metrics + heartbeat (1 Hz); GC before suspend ----------------
def main():
    last_net = psutil.net_io_counters()
    last_ts = time.time()

    # Cache IP/MAC and refresh occasionally
    node_ip, node_mac = _detect_ip_and_mac()
    next_ident_refresh = 0.0  # refresh immediately on first loop
    next_mac_publish   = 0.0  # publish nodes:mac immediately on first loop

    # Idle detection state
    idle_since = None
    last_suspend_ts = 0.0

    while True:
        t0 = time.time()

        # Refresh IP/MAC identity periodically (e.g., hourly) or if empty
        if time.time() >= next_ident_refresh or not (node_ip and node_mac):
            ip, mac = _detect_ip_and_mac()
            if ip:  node_ip = ip
            if mac: node_mac = mac
            next_ident_refresh = time.time() + 3600  # 1 hour

        # Persist hostname -> MAC (no EXPIRE) for controller discovery
        if node_mac and time.time() >= next_mac_publish:
            try:
                r.hset("nodes:mac", HOSTNAME, node_mac)
                log.debug("Updated nodes:mac mapping: %s -> %s", HOSTNAME, node_mac)
            except Exception as e:
                log.warning("Failed to publish nodes:mac mapping: %s", e)
            next_mac_publish = time.time() + 3600  # refresh hourly

        # ---- Collect metrics (acts as heartbeat too) ----
        cpu = psutil.cpu_percent(interval=None)
        vm = psutil.virtual_memory()
        mem_percent = vm.percent

        gpu = sample_gpu_percent()
        gpu_val = -1.0 if gpu is None else float(gpu)

        # Network deltas -> bytes/sec
        now_net = psutil.net_io_counters()
        now_ts = time.time()
        dt = max(1e-6, now_ts - last_ts)
        rx_bps = int((now_net.bytes_recv - last_net.bytes_recv) / dt)
        tx_bps = int((now_net.bytes_sent - last_net.bytes_sent) / dt)
        last_net, last_ts = now_net, now_ts

        # Disk percent-used (ONLY metric requested for disk)
        disk_pct = _disk_percent_used(DISK_PATH)

        payload = {
            "ts": int(time.time()),
            "hostname": HOSTNAME,
            "ip": node_ip or "",
            "mac": node_mac or "",
            "cpu": float(cpu),
            "gpu": gpu_val,
            "mem": mem_percent,
            "rx_bps": rx_bps,
            "tx_bps": tx_bps,
            "disk": disk_pct,  # <—— single disk metric as requested
        }

        # Single write/update per second; TTL makes liveness easy to detect
        try:
            r.hset(KEY, mapping=payload)
            r.expire(KEY, TTL_SEC)
        except Exception as e:
            log.warning("Failed to publish metrics: %s", e)

        # ---- Idle detection & suspend (optional) ----
        if SUSPEND_ENABLED:
            # Consider idle if CPU and GPU are both below thresholds (GPU may be absent)
            gpu_idle = (gpu is None) or (gpu_val <= IDLE_GPU_PCT_MAX)
            is_idle  = (cpu <= IDLE_CPU_PCT_MAX) and gpu_idle

            now = time.time()
            if is_idle:
                if idle_since is None:
                    idle_since = now
                idle_dur = now - idle_since

                enough_time_since_boot = _uptime_seconds() >= MIN_UPTIME_BEFORE_SUSPEND
                enough_time_since_last = (now - last_suspend_ts) >= SUSPEND_AFTER_IDLE_SEC

                if idle_dur >= SUSPEND_AFTER_IDLE_SEC and enough_time_since_boot and enough_time_since_last:
                    # --------- Run GC right before suspending ----------
                    if GC_ENABLED:
                        log.info("Idle threshold reached; running GC sweep before suspend…")
                        try:
                            _gc_sweep_once()
                        except Exception as e:
                            log.warning("GC sweep failed: %s", e)

                    log.info("Suspending via systemctl suspend (cpu=%.1f%%, gpu=%.1f%%, idle=%ds)",
                             cpu, (gpu_val if gpu is not None else -1), int(idle_dur))
                    try:
                        # Best-effort flush of current metrics before suspend
                        r.hset(KEY, mapping=payload)
                        r.expire(KEY, TTL_SEC)
                    except Exception:
                        pass
                    try:
                        subprocess.run(["systemctl", "suspend"], check=False)
                    except Exception as e:
                        log.warning("Suspend command failed: %s", e)
                    # On resume, reset timers
                    last_suspend_ts = time.time()
                    idle_since = None
            else:
                idle_since = None

        # Pace loop to ~1 Hz including work time
        elapsed = time.time() - t0
        time.sleep(max(0.0, 1.0 - elapsed))

if __name__ == "__main__":
    main()
