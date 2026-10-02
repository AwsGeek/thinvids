"""Inspect/disable only the selected node; intentionally do not inspect jobs."""

import json
import re
import sys
import time


def node_action(data, host, action):
    if not re.fullmatch(r"thinman[0-9]{2,3}", host) or action not in {"check", "disable"}:
        raise ValueError("Invalid node arguments")
    mac = data.hget("nodes:mac", host)
    if not isinstance(mac, str) or not re.fullmatch(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", mac):
        raise RuntimeError("Target has no valid registered MAC address")
    result = {"host": host, "mac": mac, "disabled": bool(data.sismember("nodes:disabled", host))}
    if action == "disable":
        with data.pipeline(transaction=True) as pipe:
            pipe.sadd("nodes:disabled", host)
            pipe.hset(f"node:quarantine:{host}", mapping={
                "hostname": host, "reason": "dedicated inference migration",
                "quarantined_at": time.time(),
            })
            pipe.delete(f"metrics:node:{host}")
            pipe.execute()
        result["disabled"] = True
    return result


def main():
    import redis
    from common import REDIS_HOST, REDIS_PORT, REDIS_DB_DATA

    host, action = sys.argv[1:]
    data = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_DATA,
                       decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
    print(json.dumps(node_action(data, host, action)))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
