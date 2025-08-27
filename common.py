# repo/common.py
import os, re, time, redis
from typing import Dict

# ----- Status constants -----
STATUS_READY    = 'READY'
STATUS_STARTING = 'STARTING'
STATUS_WAITING  = 'WAITING'
STATUS_RUNNING  = 'RUNNING'
STATUS_STOPPED  = 'STOPPED'
STATUS_FAILED   = 'FAILED'
STATUS_DONE     = 'DONE'

# ----- Natural sort for hostnames -----
def natural_key(host: str):
    m = re.search(r'(\d+)', host or '')
    return (int(m.group(1)) if m else 0, host or '')

# ----- Global settings -----
GLOBAL_SETTINGS_KEY = "global:settings"
DEFAULTS = {
    "suspend_enabled": "0",
    "suspend_idle_sec": "300",
    "suspend_gc_enabled": "0",
}

def _to_bool(v) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")

def load_global_settings(r: redis.Redis) -> Dict:
    data = r.hgetall(GLOBAL_SETTINGS_KEY) or {}
    merged = {**DEFAULTS, **data}
    return {
        "suspend_enabled": _to_bool(merged.get("suspend_enabled")),
        "suspend_idle_sec": int(merged.get("suspend_idle_sec") or 300),
        "suspend_gc_enabled": _to_bool(merged.get("suspend_gc_enabled")),
    }

# ----- Small time helpers -----
def now() -> float:
    return time.time()
