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

# Sleep policy — suspend if NO RUNNING jobs cluster-wide for 1 minute
SLEEP_AFTER_IDLE_SEC        = int(os.getenv("SLEEP_AFTER_IDLE_SEC", "300"))   # 5 minute
IDLE_CHECK_EVERY_SEC        = int(os.getenv("IDLE_CHECK_EVERY_SEC", "10"))
SUSPEND_CMD                 = os.getenv("SUSPEND_CMD", "systemctl suspend")
SLEEP_INHIBIT_FILE          = os.getenv("SLEEP_INHIBIT_FILE", "/run/no-suspend")
ALWAYS_ON_HOST              = os.getenv("ALWAYS_ON_HOST", "").strip()  # e.g. "thinman09"
MIN_UPTIME_BEFORE_SLEEP_SEC = int(os.getenv("MIN_UPTIME_BEFORE_SLEEP_SEC", "300")) # 5 minute

# MAC detection overrides
AGENT_IFACE = os.getenv("AGENT_IFACE", "").strip()  # e.g. "eth0" to force interface
AGENT_MAC   = os.getenv("AGENT_MAC", "").strip()    # if set, use this MAC as-is

HOSTNAME   = os.getenv("HOSTNAME", socket.gethostname())  # e.g. thinman07
KEY        = f"metrics:node:{HOSTNAME}"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
#r = redis.Redis(
#    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True,
#    socket_keepalive=True, socket_timeout=5, socket_connect_timeout=5,
#    health_check_interval=30, retry_on_timeout=True,
#    retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
#)
# ---------------- GPU sampling helpers (kept) ----------------
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
    Start intel_gpu_top -J streaming, read briefly, parse one sample, then kill it.
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

# ---------------- MAC address detection ----------------
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

def _iface_from_default_route() -> str:
    try:
        # Works on most Linux systems with iproute2
        pr = subprocess.run(["ip", "-4", "route", "get", "1.1.1.1"],
                            capture_output=True, text=True, timeout=1.0)
        if pr.returncode == 0:
            m = re.search(r"\bdev\s+(\S+)", pr.stdout)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""

def _detect_primary_mac() -> str:
    # Env override takes precedence
    if AGENT_MAC:
        return _norm_mac_colon(AGENT_MAC)

    # Specific interface override
    if AGENT_IFACE:
        mac = _read_sysfs_mac(AGENT_IFACE)
        if mac:
            return mac

    # Try default route interface
    iface = _iface_from_default_route()
    if iface:
        mac = _read_sysfs_mac(iface)
        if mac:
            return mac

    # Fallback: first reasonable interface with IPv4 addr and a MAC
    try:
        addrs = psutil.net_if_addrs()
        stats = psutil.net_if_stats()
        exclude_prefixes = ("lo", "docker", "veth", "br-", "vmnet", "kube", "flannel", "cni", "tailscale")
        AF_LINK = getattr(psutil, "AF_LINK", object())

        best = ""
        for ifname, infos in addrs.items():
            if ifname.startswith(exclude_prefixes):
                continue
            # must be up if stats available
            if stats.get(ifname) and not stats[ifname].isup:
                continue

            mac = ""
            has_ipv4 = False
            for a in infos:
                if a.family == socket.AF_INET and not (a.address or "").startswith("127."):
                    has_ipv4 = True
                if (AF_LINK is not object() and a.family == AF_LINK) or a.family == socket.AF_PACKET:
                    mac = _norm_mac_colon(a.address)
            if has_ipv4 and mac:
                best = mac
                break
        if best:
            return best
    except Exception:
        pass

    # Last resort: try common names
    for guess in ("eth0", "enp0s3", "enp1s0", "ens18"):
        mac = _read_sysfs_mac(guess)
        if mac:
            return mac

    return ""

NODE_MAC = _detect_primary_mac()

# ---------------- System-idle detection (status-only) ----------------
def _cluster_has_running():
    """
    Return True if ANY job:* hash has status == RUNNING (cluster-wide).
    Fail-safe: on Redis error, assume running to avoid unintended suspend.
    """
    try:
        for key in r.scan_iter("job:*"):
            try:
                # Skip anything that isn't a hash (e.g., sets like job_done_parts:* if misnamed)
                if r.type(key) != 'hash':
                    continue
                s = (r.hget(key, "status") or "").upper()
                if s == "RUNNING":
                    return True
            except redis.exceptions.ResponseError:
                # Wrong type or other hash-op error — skip
                continue
        return False
    except Exception:
        # Be conservative on broader Redis issues
        return True

def _host_uptime_sec():
    try:
        return time.time() - psutil.boot_time()
    except Exception:
        return 10**9  # if unknown, pretend very long uptime

def _maybe_suspend(state):
    now = time.time()

    if ALWAYS_ON_HOST and HOSTNAME == ALWAYS_ON_HOST:
        log.debug("Skipping suspend: ALWAYS_ON_HOST=%s", ALWAYS_ON_HOST); return
    if os.path.exists(SLEEP_INHIBIT_FILE):
        log.debug("Skipping suspend: inhibit file present at %s", SLEEP_INHIBIT_FILE); return
    if _host_uptime_sec() < MIN_UPTIME_BEFORE_SLEEP_SEC:
        log.debug("Skipping suspend: uptime < %ss", MIN_UPTIME_BEFORE_SLEEP_SEC); return

    if _cluster_has_running():
        if state.get("last_state") != "running":
            log.info("Cluster has RUNNING jobs; deferring suspend")
            state["last_state"] = "running"
        state["last_seen_running"] = now
        return

    quiet_for = now - state["last_seen_running"]
    if state.get("last_state") != "idle":
        log.info("Cluster idle; timer started (threshold=%ss)", SLEEP_AFTER_IDLE_SEC)
        state["last_state"] = "idle"

    if quiet_for >= SLEEP_AFTER_IDLE_SEC:
        log.info("No RUNNING jobs for %ss -> suspending via: %s", int(quiet_for), SUSPEND_CMD)
        rc = subprocess.run(SUSPEND_CMD, shell=True, capture_output=True, text=True)
        log.info("suspend rc=%s stdout=%s stderr=%s",
                 rc.returncode, rc.stdout.strip()[:200], rc.stderr.strip()[:200])
        state["last_seen_running"] = time.time()
        time.sleep(60)


# ---------------- Main loop (metrics + idle watcher) ----------------
def main():
    last_net = psutil.net_io_counters()
    last_ts  = time.time()

    # idle/sleep state — start with "recently running" to avoid suspending right after boot
    state = {"last_seen_running": time.time()}
    next_idle_check = 0.0

    # persistent hostname->mac publisher (no TTL)
    node_mac = NODE_MAC  # local copy we can re-detect if empty
    mac_next_attempt = 0.0  # attempt immediately
    last_published_mac = None

    while True:
        t0 = time.time()

        # -------- Persist hostname -> MAC (no EXPIRE) --------
        if time.time() >= mac_next_attempt:
            if not node_mac:
                node_mac = _detect_primary_mac()
            if node_mac:
                try:
                    r.hset("nodes:mac", HOSTNAME, node_mac)
                    if node_mac != last_published_mac:
                        log.info("Published persistent MAC mapping: %s -> %s", HOSTNAME, node_mac)
                    last_published_mac = node_mac
                    mac_next_attempt = time.time() + 3600  # refresh roughly hourly
                except Exception as e:
                    log.warning("Failed to publish nodes:mac mapping: %s", e)
                    mac_next_attempt = time.time() + 30
            else:
                # no MAC yet; try again soon
                mac_next_attempt = time.time() + 30

        # -------- Metrics (1 Hz) --------
        cpu = psutil.cpu_percent(interval=None)

        vm = psutil.virtual_memory()
        mem_used  = int(vm.used)
        mem_total = int(vm.total)
        mem_percent = vm.percent

        now_net = psutil.net_io_counters()
        now_ts  = time.time()
        dt = max(1e-6, now_ts - last_ts)
        rx_bps = int((now_net.bytes_recv - last_net.bytes_recv) / dt)
        tx_bps = int((now_net.bytes_sent - last_net.bytes_sent) / dt)
        last_net, last_ts = now_net, now_ts

        gpu = sample_gpu_percent()
        gpu_val = -1.0 if gpu is None else float(gpu)

        payload = {
            "ts": int(now_ts),
            "hostname": HOSTNAME,
            "mac": node_mac or "",     # included in volatile metrics too
            "cpu": float(cpu),
            "gpu": gpu_val,
            "mem_used": mem_used,
            "mem_total": mem_total,
            "mem": mem_percent,
            "rx_bps": rx_bps,
            "tx_bps": tx_bps
        }

        r.hset(KEY, mapping=payload)
        r.expire(KEY, TTL_SEC)

        # -------- Idle watcher (every IDLE_CHECK_EVERY_SEC) --------
        if time.time() >= next_idle_check:
            _maybe_suspend(state)
            next_idle_check = time.time() + IDLE_CHECK_EVERY_SEC

        # ~1 Hz pacing (accounting for work done)
        elapsed = time.time() - t0
        time.sleep(max(0, 1.0 - elapsed))

if __name__ == "__main__":
    main()
