"""
Shared types/utilities for manager/worker/agent.
"""

# repo/common.py
import os, re
from functools import lru_cache


# ----- Small time helpers -----
import time
def time_now() -> float:
    return time.time()

from typing import Dict

import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff

ENV = os.environ.get

REDIS_HOST = ENV("REDIS_HOST", "swarm3")
REDIS_PORT = int(ENV("REDIS_PORT", "6379"))
REDIS_DB_TASKS = int(ENV("REDIS_DB_TASKS", "0"))  # Huey broker
REDIS_DB_DATA  = int(ENV("REDIS_DB_DATA",  "1"))  # App/job state

@lru_cache(maxsize=None)
def get_redis() -> redis.Redis:
    return redis.Redis(
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
from huey import RedisHuey

@lru_cache(maxsize=None)
def get_huey() -> RedisHuey:
    return RedisHuey(
        'tasks',
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB_TASKS,
        retry_on_timeout=True,
        retry=Retry(ExponentialBackoff(cap=10, base=1), retries=8),
    )

"""
Provides a string-backed Status enum that is safe to persist to Redis/JSON
and helpers for parsing and sorting by status.
"""
from enum import Enum

class Status(str, Enum):
    READY    = 'READY'
    STARTING = 'STARTING'
    WAITING  = 'WAITING'
    RUNNING  = 'RUNNING'
    STOPPED  = 'STOPPED'
    FAILED   = 'FAILED'
    DONE     = 'DONE'

    # --- helpers -----------------------------------------------------------
    @staticmethod
    def parse(value: str) -> "Status":
        """
        lenient ↓
        * Accepts None, empty strings, wrong-casing, even an existing Status.
        * Raise ValueError if unknown status value.
        """
        if isinstance(value, Status):
            return value
        try:
            return Status[str(value).strip().upper()]
        except (KeyError, AttributeError):
            raise ValueError(f"Unknown Status: {value!r}")


# ---------------- Logging setup (shared) ----------------
import logging, socket, sys
from typing import Optional

_HOST = socket.gethostname()

class _HostnameFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.hostname = _HOST
        return True

def _coerce_level(level: Optional[str | int]) -> int:
    if isinstance(level, int):
        return level
    env = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    return getattr(logging, env, logging.INFO)

def get_logging(
    app_name: str = "thinvids",
    level: Optional[str | int] = None,
    use_utc: bool = False,
    quiet_libs: bool = True,
) -> logging.Logger:
    """
    Idempotent root logging setup to stdout with a consistent format.
    Call this once near process startup, then use `logging.getLogger(__name__)` or
    the returned logger.

    Env overrides:
      - LOG_LEVEL=DEBUG|INFO|WARNING|ERROR
    """
    root = logging.getLogger()

    if not root.handlers:
        # format: time level host logger [pid] message
        fmt = "%(asctime)s %(levelname)s %(hostname)s %(name)s [%(process)d] VTT %(message)s"
        datefmt = "%Y-%m-%dT%H:%M:%S%z"
        handler = logging.StreamHandler(sys.stdout)
        handler.addFilter(_HostnameFilter())
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
        if use_utc:
            # use UTC timestamps if desired
            formatter.converter = time.gmtime  # type: ignore[attr-defined]
        handler.setFormatter(formatter)

        root.addHandler(handler)
        root.setLevel(_coerce_level(level))

        if quiet_libs:
            # tone down noisy deps
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("werkzeug").setLevel(logging.WARNING)
            logging.getLogger("watchdog").setLevel(logging.WARNING)
            logging.getLogger("huey").setLevel(logging.INFO)

    else:
        # even if already configured, honor explicit level override
        if level is not None:
            root.setLevel(_coerce_level(level))

    # convenience: return a named logger for the caller
    logger = logging.getLogger(app_name)
    return logger

# ----- Natural sort for hostnames -----
def natural_key(host: str):
    m = re.search(r'(\d+)', host or '')
    return (int(m.group(1)) if m else 0, host or '')

# ----- Global settings -----
SETTINGS_KEY = "global:settings"
DEFAULT_SETTINGS = {
    "suspend_enabled": "0",
    "suspend_idle_sec": "300",
    "suspend_gc_enabled": "0",
}

# ---------------- Global settings fetch (with light caching) ----------------
cached_settings = {"ts": 0, "data": {}}
CACHED_SETTINGS_TTL = int(ENV("CACHED_SETTINGS_TTL", "10")) 

def as_bool(x, default=False) -> bool:
    if x is None: return default
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "on", "y", "t")

def as_int(x, default=0) -> int:
    try: return int(x)
    except: return default

def get_settings() -> Dict:

    redis_client = get_redis()
    
    now = time_now()
    # refresh at most every 10 seconds
    if now - cached_settings["ts"] >= CACHED_SETTINGS_TTL:
        try:
            settings = redis_client.hgetall(SETTINGS_KEY) or {}
            settings = {**DEFAULT_SETTINGS, **settings}
        except Exception as e:
            logger.debug("Failed to fetch global settings: %s", e)
            settings = DEFAULT_SETTINGS

        cached_settings["data"] = settings
        cached_settings["ts"] = now
    else:
        settings = cached_settings["data"]

    return settings

def all_jobs_are_idle() -> bool:

    redis_client = get_redis()
    
    active = [Status.RUNNING, Status.WAITING, Status.STARTING]

    # Pull indexed job keys; seed from keyspace if empty
    keys = list(redis_client.smembers("jobs:all") or [])
    if not keys:
        keys = [k for k in redis_client.scan_iter("job:*", count=1000)]
        if keys:
            try:
                redis_client.sadd("jobs:all", *keys)
            except Exception:
                pass
    if not keys:
        return False

    # Batch HGET status for each job
    pipe = redis_client.pipeline()
    for k in keys:
        pipe.hget(k, "status")
    try:
        statuses = pipe.execute()
    except Exception:
        # On error, be conservative
        return False

    for s in statuses:
        if s in active:
            return False

    return True