#!/usr/bin/env python3
import os, time, json, socket, subprocess, signal, re
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
SUSPEND_ENABLED          = str(os.getenv("SUSPEND_ENABLED", "1")).lower() in ("1", "true", "yes", "on")
SUSPEND_AFTER_IDLE_SEC   = int(os.getenv("SUSPEND_AFTER_IDLE_SEC", "300"))  # 5 minutes
IDLE_CPU_PCT_MAX         = float(os.getenv("IDLE_CPU_PCT_MAX", "10"))        # <= 5% CPU considered idle
IDLE_GPU_PCT_MAX         = float(os.getenv("IDLE_GPU_PCT_MAX", "10"))        # <= 5% GPU considered idle
MIN_UPTIME_BEFORE_SUSPEND= int(os.getenv("MIN_UPTIME_BEFORE_SUSPEND", "30060"))# don't suspend within five minutes

HOSTNAME = os.getenv("HOSTNAME", socket.gethostname())  # e.g. thinman07
KEY      = f"metrics:node:{HOSTNAME}"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
# Hardened connection (optional):
# r = redis.Redis(
#     host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True,
#     socket_keepalive=True, socket_timeout=5, socket_connect_timeout=5,
#     health_check_interval=30, retry_on_timeout=True,
#     retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
# )

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
            # find 'src X.X.X.X' and 'dev IFACE'
            ip_match = re.search(r"\bsrc\s+(\d+\.\d+\.\d+\.\d+)", out)
            dev_match = re.search(r"\bdev\s+(\S+)", out)
            ip = ip_match.group(1) if ip_match else ""
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

        payload = {
            "ts": int(time.time()),  # heartbeat timestamp
            "hostname": HOSTNAME,
            "ip": node_ip or "",
            "mac": node_mac or "",
            "cpu": float(cpu),
            "gpu": gpu_val,
            "mem": mem_percent,
            "rx_bps": rx_bps,
            "tx_bps": tx_bps,
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
                # Avoid immediate re-suspend loops after resume
                enough_time_since_boot = _uptime_seconds() >= MIN_UPTIME_BEFORE_SUSPEND
                enough_time_since_last = (now - last_suspend_ts) >= SUSPEND_AFTER_IDLE_SEC
                if idle_dur >= SUSPEND_AFTER_IDLE_SEC and enough_time_since_boot and enough_time_since_last:
                    log.info("Idle for %ds (cpu=%.1f%%, gpu=%.1f%%) -> suspending via systemctl suspend",
                             int(idle_dur), cpu, (gpu_val if gpu is not None else -1))
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
