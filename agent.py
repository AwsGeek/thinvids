#!/usr/bin/env python3
import os, time, json, socket, subprocess, signal
import redis
import psutil

REDIS_HOST = os.getenv("REDIS_HOST", "swarm1")  # central Redis host
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "1"))
TTL_SEC    = int(os.getenv("TTL_SEC", "15"))

HOSTNAME   = os.getenv("NODE_NAME", socket.gethostname())  # e.g. thinman07
KEY        = f"metrics:node:{HOSTNAME}"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

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
            if proc.poll() is not None:  # exited unexpectedly
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
        # terminate the child quickly and clean up
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

def main():
    last_net = psutil.net_io_counters()
    last_ts  = time.time()

    while True:
        t0 = time.time()

        # CPU overall %
        cpu = psutil.cpu_percent(interval=None)  # non-blocking in a 1 Hz loop

        # Memory
        vm = psutil.virtual_memory()
        mem_used  = int(vm.used)
        mem_total = int(vm.total)

        # Network deltas (bytes/s)
        now_net = psutil.net_io_counters()
        now_ts  = time.time()
        dt = max(1e-6, now_ts - last_ts)
        rx_bps = int((now_net.bytes_recv - last_net.bytes_recv) / dt)
        tx_bps = int((now_net.bytes_sent - last_net.bytes_sent) / dt)
        last_net, last_ts = now_net, now_ts

        # GPU % (Video engine)
        gpu = sample_gpu_percent()
        gpu_val = -1.0 if gpu is None else float(gpu)

        payload = {
            "ts": int(now_ts),
            "hostname": HOSTNAME,
            "cpu": float(cpu),        # 0..100
            "gpu": gpu_val,           # 0..100 or -1 if unavailable
            "mem_used": mem_used,     # bytes
            "mem_total": mem_total,   # bytes
            "rx_bps": rx_bps,         # bytes/sec
            "tx_bps": tx_bps          # bytes/sec
        }

        # HSET + EXPIRE so stale nodes drop off
        r.hset(KEY, mapping=payload)
        r.expire(KEY, TTL_SEC)

        # sleep to ~1 Hz
        elapsed = time.time() - t0
        time.sleep(max(0, 1.0 - elapsed))

if __name__ == "__main__":
    main()
