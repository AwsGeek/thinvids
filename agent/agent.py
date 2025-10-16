#!/usr/bin/env python3
import os, time, json, socket, subprocess, signal, re, shutil
import psutil

from common import get_redis, get_logging, get_settings, all_jobs_are_idle, as_bool, as_int
redis_client = get_redis()
logger = get_logging("agent")

# ---------------- Env / Config ----------------
REDIS_HOST = os.getenv("REDIS_HOST", "swarm3")  # central Redis host
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "1"))
TTL_SEC    = int(os.getenv("TTL_SEC", "15"))

# Optional overrides for interface/MAC detection
AGENT_IFACE = os.getenv("AGENT_IFACE", "").strip()  # e.g. "enp1s0"
AGENT_MAC   = os.getenv("AGENT_MAC", "").strip()    # if set, use this MAC as-is

# --- Local defaults for suspend/idle behavior (can be overridden by global settings) ---
SUSPEND_ENABLED           = str(os.getenv("SUSPEND_ENABLED", "1")).lower() in ("1", "true", "yes", "on")
SUSPEND_AFTER_IDLE_SEC    = int(os.getenv("SUSPEND_AFTER_IDLE_SEC", "300"))  # default 5 minutes
IDLE_CPU_PCT_MAX          = float(os.getenv("IDLE_CPU_PCT_MAX", "10"))       # <= 10% CPU considered idle
IDLE_GPU_PCT_MAX          = float(os.getenv("IDLE_GPU_PCT_MAX", "10"))       # <= 10% GPU considered idle
MIN_UPTIME_BEFORE_SUSPEND = int(os.getenv("MIN_UPTIME_BEFORE_SUSPEND", "300"))# don't suspend within 5 minutes of boot

# Garbage collection (local default: off). Global setting can enable per controller.
GC_BASE_DIR = os.getenv("GC_BASE_DIR", "/opt/jerry/current")

HOSTNAME = os.getenv("HOSTNAME", socket.gethostname())  # e.g. thinman07
KEY      = f"metrics:node:{HOSTNAME}"

# Global settings key (controller-managed)
GLOBAL_SETTINGS_KEY = "global:settings"

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
    try:
        pr = subprocess.run(
            ["ip", "-4", "route", "get", "1.1.1.1"],
            capture_output=True, text=True, timeout=1.0
        )
        if pr.returncode == 0:
            out = pr.stdout or ""
            ip_match = re.search(r"\bsrc\s+(\d+\.\d+\.\d+\.\d+)", out)
            dev_match = re.search(r"\bdev\s+(\S+)", out)
            ip = ip_match.group(1) if ip_match else ""
            iface = dev_match.group(1) if dev_match else ""
            if ip and iface:
                return (ip, iface)
    except Exception:
        pass

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

# ---------------- Garbage collection of stale projects ----------------
_GUID_RE = re.compile(r"^[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$")

def remove_stale_projects(base_dir: str):
    """
    Remove immediate subdirectories under base_dir whose names are GUIDs.
    Equivalent to:
      find <base_dir> -mindepth 1 -maxdepth 1 -type d -regextype posix-extended \
           -regex '.*/[0-9A-Fa-f]{8}(-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$' -exec rm -rf {} +
    """
    try:
        entries = os.listdir(base_dir)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.warning("GC: listdir failed for %s: %s", base_dir, e)
        return

    removed = 0
    for name in entries:
        path = os.path.join(base_dir, name)
        if not os.path.isdir(path):
            continue
        if not _GUID_RE.match(name):
            continue
        try:
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
        except Exception as e:
            logger.warning("GC: failed to remove %s: %s", path, e)

    if removed:
        logger.info("GC: removed %d GUID project dirs under %s", removed, base_dir)

def _get_global_suspend_settings():

    settings = get_settings()
    
    # merge with local defaults
    suspend_enabled = SUSPEND_ENABLED and as_bool(settings.get("suspend_enabled"), True)
    idle_sec = as_int(settings.get("suspend_idle_sec"), SUSPEND_AFTER_IDLE_SEC)
    if idle_sec <= 0:
        idle_sec = SUSPEND_AFTER_IDLE_SEC
    gc_before = as_bool(settings.get("suspend_gc_enabled"), False)

    return suspend_enabled, idle_sec, gc_before


# ---------------- Main loop: unified metrics + heartbeat (1 Hz) ----------------
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
                redis_client.hset("nodes:mac", HOSTNAME, node_mac)
                logger.debug("Updated nodes:mac mapping: %s -> %s", HOSTNAME, node_mac)
            except Exception as e:
                logger.warning("Failed to publish nodes:mac mapping: %s", e)
            next_mac_publish = time.time() + 3600  # refresh hourly

        # ---- Collect metrics (acts as heartbeat too) ----
        cpu = psutil.cpu_percent(interval=None)
        vm = psutil.virtual_memory()
        mem_percent = vm.percent

        # Disk usage (simple: percent used of primary drive '/')
        try:
            disk_percent = float(psutil.disk_usage("/").percent)
        except Exception:
            disk_percent = 0.0

        gpu = sample_gpu_percent()
        gpu_val = -1.0 if gpu is None else float(gpu)

        # Network deltas -> bytes/sec
        now_net = psutil.net_io_counters()
        now_ts = time.time()
        dt = max(1e-6, now_ts - last_ts)
        rx_bps = int((now_net.bytes_recv - last_net.bytes_recv) / dt)
        tx_bps = int((now_net.bytes_sent - last_net.bytes_sent) / dt)
        last_net, last_ts = now_net, now_ts

        payload = {
            "ts": int(time.time()),  # heartbeat timestamp
            "hostname": HOSTNAME,
            "ip": node_ip or "",
            "mac": node_mac or "",
            "cpu": float(cpu),
            "gpu": gpu_val,
            "mem": mem_percent,
            "disk": disk_percent,   # <--- NEW metric key (percent used)
            "rx_bps": rx_bps,
            "tx_bps": tx_bps,
        }

        # Single write/update per second; TTL makes liveness easy to detect
        try:
            redis_client.hset(KEY, mapping=payload)
            redis_client.expire(KEY, TTL_SEC)
        except Exception as e:
            logger.warning("Failed to publish metrics: %s", e)

        # ---- Idle detection & suspend (controller-gated) ----
        # Pull global flags (with small cache)
        g_suspend_enabled, g_idle_sec, g_gc_enabled = _get_global_suspend_settings()

        # Determine effective enable/threshold (global must allow it; local can further restrict)
        effective_suspend_enabled = bool(g_suspend_enabled)
        effective_idle_threshold  = int(g_idle_sec)

        # Consider idle if CPU and GPU are both below thresholds (GPU may be absent)
        gpu_idle = (gpu is None) or (gpu_val <= IDLE_GPU_PCT_MAX)
        is_idle  = (cpu <= IDLE_CPU_PCT_MAX) and gpu_idle
        is_idle  = all_jobs_are_idle() and is_idle

        now = time.time()

        if is_idle and effective_suspend_enabled:
            if idle_since is None:
                idle_since = now
            idle_dur = now - idle_since

            # Avoid immediate re-suspend loops after resume
            enough_time_since_boot = _uptime_seconds() >= MIN_UPTIME_BEFORE_SUSPEND
            enough_time_since_last = (now - last_suspend_ts) >= effective_idle_threshold

            if idle_dur >= effective_idle_threshold and enough_time_since_boot and enough_time_since_last:
                # Optional GC before suspend (controller-controlled)
                if g_gc_enabled:
                    remove_stale_projects(GC_BASE_DIR)

                logger.info(
                    "Idle for %ds (cpu=%.1f%%, gpu=%.1f%%), threshold=%ds, suspend_enabled=%s -> suspending",
                    int(idle_dur), cpu, (gpu_val if gpu is not None else -1), effective_idle_threshold,
                    effective_suspend_enabled
                )
                try:
                    # Best-effort flush of current metrics before suspend
                    redis_client.hset(KEY, mapping=payload)
                    redis_client.expire(KEY, TTL_SEC)
                except Exception:
                    pass
                try:
                    subprocess.run(["systemctl", "suspend"], check=False)
                except Exception as e:
                    logger.warning("Suspend command failed: %s", e)
                # On resume, reset timers
                last_suspend_ts = time.time()
                idle_since = None
            else:
                logger.info(f"Sleeping in {int(effective_idle_threshold - idle_dur)}s")

        else:
            idle_since = None

        # Pace loop to ~1 Hz including work time
        elapsed = time.time() - t0
        time.sleep(max(0.0, 1.0 - elapsed))

if __name__ == "__main__":
    main()
