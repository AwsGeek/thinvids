#!/usr/bin/env python3
import os, json, socket, re, sys
import redis

# sudo install -m0755 wol-proxy.py /usr/local/bin/wol-proxy.py

REDIS_HOST = os.getenv("REDIS_HOST", "swarm3")   # or "127.0.0.1" if Redis is on this host
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "1"))
CHANNEL    = os.getenv("WOL_CHANNEL", "wol:wake")

def norm_mac_bytes(mac: str) -> bytes:
    s = re.sub(r"[^0-9A-Fa-f]", "", mac or "")
    if len(s) != 12:
        raise ValueError(f"Bad MAC: {mac}")
    return bytes.fromhex(s)

def send_magic(mac: str, bcast: str = "255.255.255.255", port: int = 9, repeats: int = 3):
    pkt = b"\xff"*6 + norm_mac_bytes(mac)*16
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        for _ in range(max(1, int(repeats))):
            s.sendto(pkt, (bcast, int(port)))

def main():
    print(f"[wol-proxy] connecting to redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}", flush=True)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    ps = r.pubsub()
    ps.subscribe(CHANNEL)
    print(f"[wol-proxy] listening on channel '{CHANNEL}'", flush=True)
    for msg in ps.listen():
        if msg.get("type") != "message":
            continue
        try:
            data = json.loads(msg["data"])
            mac     = data["mac"]
            bcast   = data.get("bcast", "255.255.255.255")
            port    = int(data.get("port", 9))
            repeats = int(data.get("repeats", 3))
            send_magic(mac, bcast, port, repeats)
            print(f"[wol-proxy] sent WOL mac={mac} bcast={bcast} port={port} x{repeats}", flush=True)
        except Exception as e:
            print(f"[wol-proxy] error: {e} payload={msg.get('data')!r}", file=sys.stderr, flush=True)

if __name__ == "__main__":
    main()
