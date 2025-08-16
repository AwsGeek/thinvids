#!/usr/bin/env python3
import socket, re, argparse

def wake(mac, bcast="255.255.255.255", port=9, count=3):
    s = re.sub(r"[^0-9A-Fa-f]", "", mac)
    if len(s) != 12: raise ValueError(f"Bad MAC: {mac}")
    pkt = b"\xff"*6 + bytes.fromhex(s)*16
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        for _ in range(max(1, count)):
            sock.sendto(pkt, (bcast, port))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Send a Wake-on-LAN magic packet")
    ap.add_argument("mac", help="MAC like aa:bb:cc:dd:ee:ff")
    ap.add_argument("-b","--broadcast", default="255.255.255.255")
    ap.add_argument("-p","--port", type=int, default=9)
    ap.add_argument("-n","--count", type=int, default=3)
    a = ap.parse_args()
    wake(a.mac, a.broadcast, a.port, a.count)