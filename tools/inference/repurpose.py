#!/usr/bin/env python3
"""Retire one Thinvids worker and install a reproducible local inference pilot."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[2]
MODEL_REPO = "Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF"
MODEL_FILE = "qwen2.5-coder-1.5b-instruct-q4_k_m.gguf"
SERVICES = ("watcher", "housekeeping", "manager")


def run(args, *, capture=False, input=None, timeout=None):
    result = subprocess.run(args, cwd=REPO, input=input, text=True,
                            stdout=subprocess.PIPE if capture else None,
                            check=True, timeout=timeout)
    return result.stdout.strip() if capture else ""


def atomic_write(path, content, mode=0o600):
    path = Path(path)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as out:
        tmp = Path(out.name)
        try:
            os.fchmod(out.fileno(), mode)
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()


def save_json(path, obj):
    atomic_write(path, (json.dumps(obj, indent=2) + "\n").encode())


def validate_host(host):
    if not re.fullmatch(r"thinman[0-9]{2,3}", host):
        raise ValueError("Expected one exact hostname, such as thinman01; patterns are not accepted")
    return host


def moved_inventory(raw, host):
    """Move one direct host record while preserving comments, vars, and CRLF."""
    lines = raw.decode().splitlines(keepends=True)
    group = None
    records = []
    destination = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        match = re.fullmatch(r"\[([^\]]+)\]\s*(?:[#;].*)?", stripped)
        if match:
            group = match.group(1)
            if group == "inference_workers":
                if destination is not None:
                    raise ValueError("Duplicate inference_workers section")
                destination = i
            continue
        if not stripped or stripped.startswith(("#", ";")) or not group or ":" in group:
            continue
        tokens = shlex.split(stripped, comments=True)
        if tokens and tokens[0] == host:
            records.append((i, group, line))
    if len(records) != 1 or records[0][1] not in {"thinman_workers", "inference_workers"}:
        raise ValueError("Target must have exactly one direct record in thinman_workers or inference_workers")
    index, group, record = records[0]
    if group == "inference_workers":
        return raw
    newline = "\r\n" if record.endswith("\r\n") else "\n"
    if not record.endswith("\n"):
        record += newline
    lines.pop(index)
    if destination is None:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += newline
        lines.extend([newline, "[inference_workers]" + newline, record])
    else:
        if index < destination:
            destination -= 1
        lines.insert(destination + 1, record)
    return "".join(lines).encode()


def inventory_hosts(inventory, group):
    found = set(inventory.get(group, {}).get("hosts", []))
    for child in inventory.get(group, {}).get("children", []):
        found.update(inventory_hosts(inventory, child))
    return found


def validate_inventory(path, host, retired=False):
    inv = json.loads(run(["ansible-inventory", "-i", str(path), "--list"], capture=True))
    old = host in inventory_hosts(inv, "thinman_workers")
    new = host in inventory_hosts(inv, "inference_workers")
    if retired and (old or not new):
        raise ValueError("Retired node still belongs to Thinvids inventory, or is missing")
    if not retired and old == new:
        raise ValueError("Target must belong to exactly one worker group")


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=45) as response:
        return json.load(response)


def validate_versions(value):
    for field in ("llama_revision", "model_revision"):
        if not re.fullmatch(r"[0-9a-f]{40}", value.get(field, "")):
            raise ValueError(f"Invalid pinned {field}")
    if not re.fullmatch(r"[0-9a-f]{64}", value.get("model_sha256", "")):
        raise ValueError("Missing official model SHA-256")
    return value


def versions(path):
    if path.exists():
        return validate_versions(json.loads(path.read_text()))
    revision = run(["git", "ls-remote", "https://github.com/ggml-org/llama.cpp.git", "HEAD"],
                   capture=True, timeout=60).split()[0]
    metadata = fetch_json(f"https://huggingface.co/api/models/{MODEL_REPO}?blobs=true")
    file = next(item for item in metadata["siblings"] if item["rfilename"] == MODEL_FILE)
    result = validate_versions({"llama_revision": revision, "model_revision": metadata["sha"],
                                "model_sha256": file["lfs"]["sha256"]})
    save_json(path, result)
    return result


class Migration:
    def __init__(self, host, state_dir):
        self.host = host
        self.state_dir = state_dir
        self.journal_path = state_dir / f"{host}.json"
        self.journal = json.loads(self.journal_path.read_text()) if self.journal_path.exists() else {}
        self.recovery_path = state_dir / "service-recovery.json"

    def record(self, **values):
        self.journal.update(values, host=self.host, updated_at=time.time())
        save_json(self.journal_path, self.journal)

    def compose(self, *args, **kwargs):
        return run(["docker", "compose", *args], **kwargs)

    def restore_services(self):
        if not self.recovery_path.exists():
            return
        recovery = json.loads(self.recovery_path.read_text())
        if not recovery.get("safe_to_restore", False):
            print("Retirement is unconfirmed: Thinvids controllers remain stopped. Rerun this host to recover.", file=sys.stderr)
            return
        services = recovery["running"]
        if any(s not in SERVICES for s in services):
            raise ValueError("Invalid service recovery record")
        if services:
            print("Restoring previously running Thinvids services...", flush=True)
            self.compose("start", *reversed(services))
        self.recovery_path.unlink()

    def guard(self, action):
        code = (REPO / "tools/inference/redis_guard.py").read_text()
        name = f"thinvids-retirement-{self.host}-{uuid.uuid4().hex[:12]}"
        try:
            output = self.compose("run", "--rm", "--no-deps", "-T", "--name", name,
                                  "--entrypoint", "python", "manager", "-", self.host, action,
                                  input=code, capture=True, timeout=120)
            result = json.loads(output)
            if result.get("host") != self.host:
                raise RuntimeError("Unexpected guard response")
            return result
        except BaseException:
            # Killing the Compose client alone does not guarantee the helper exits.
            try:
                cleanup = subprocess.run(["docker", "rm", "--force", name],
                                         capture_output=True, text=True, timeout=20)
                if cleanup.returncode and "No such container" not in cleanup.stderr:
                    print(f"Could not remove temporary guard {name}: {cleanup.stderr.strip()}",
                          file=sys.stderr)
            except Exception as exc:
                print(f"Guard cleanup failed for {name}: {exc}", file=sys.stderr)
            raise

    def wake(self, mac):
        if not re.fullmatch(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", mac):
            raise ValueError("Invalid MAC")
        packet = b"\xff" * 6 + bytes.fromhex(mac.replace(":", "")) * 16
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            for _ in range(3):
                sock.sendto(packet, ("255.255.255.255", 9))

    def play(self, tag):
        run(["ansible-playbook", "-i", str(REPO / "ansible_hosts.ini"),
             str(REPO / "ansible_inference.yml"), "--limit", self.host, "--tags", tag,
             "--extra-vars", "@" + str(self.state_dir / "versions.json"),
             "--extra-vars", json.dumps({"migration_controller": True})])

    def retire(self):
        inventory = REPO / "ansible_hosts.ini"
        if inventory.is_symlink():
            raise ValueError("Review the symlinked inventory before automated migration")
        initial = inventory.read_bytes()
        updated = moved_inventory(initial, self.host)
        # Ask Ansible to validate semantics before touching services or inventory.
        with tempfile.TemporaryDirectory(prefix="thinvids-inventory-") as tmp:
            candidate = Path(tmp) / "hosts.ini"
            candidate.write_bytes(updated)
            validate_inventory(candidate, self.host, retired=True)
        prior = self.guard("check")
        if "original_node" not in self.journal:
            self.record(original_node=prior)
        self.record(phase="retiring")
        result = self.guard("disable")
        if not result.get("disabled"):
            raise RuntimeError("Node disable was not confirmed")
        if inventory.read_bytes() != initial:
            raise RuntimeError("Inventory changed during migration; node remains disabled. Rerun after review.")
        backup = self.state_dir / f"{self.host}.inventory.before"
        if not backup.exists():
            atomic_write(backup, initial)
        atomic_write(inventory, updated, inventory.stat().st_mode & 0o777)
        validate_inventory(inventory, self.host, retired=True)
        print(f"Waking only {self.host} and removing its Thinvids installation...", flush=True)
        self.wake(prior["mac"])
        self.play("retire")
        self.record(phase="retired")
        # Older script versions may have stopped controllers before failing.
        if self.recovery_path.exists():
            recovery = json.loads(self.recovery_path.read_text())
            recovery["safe_to_restore"] = True
            save_json(self.recovery_path, recovery)
            self.restore_services()

    def execute(self):
        if self.recovery_path.exists():
            recovery = json.loads(self.recovery_path.read_text())
            if recovery.get("host") != self.host:
                raise RuntimeError("Finish the interrupted migration for " + str(recovery.get("host")) + " first")
        validate_inventory(REPO / "ansible_hosts.ini", self.host)
        versions(self.state_dir / "versions.json")
        # Recheck retirement on reruns; never assume a previous journal is proof.
        print("Direct retirement: you are responsible for ensuring this node has no running tasks.", flush=True)
        self.retire()
        self.record(phase="installing")
        print("Building the pinned inference runtime and measuring the baseline...", flush=True)
        self.play("install")
        self.record(phase="complete")
        print(f"\n{self.host} is dedicated to inference. Thinvids services and application files are removed.")
        print("Baseline results: /opt/inference/results/baseline-cpu.json on the node")
        print("RPC is installed but stopped; this run serves the single-node baseline.")
        print(f"Open a tunnel from your browser's computer:\n  ssh -N -L 18080:127.0.0.1:8080 jerry@{self.host}")
        print("Chat: http://127.0.0.1:18080  API: http://127.0.0.1:18080/v1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host", type=validate_host)
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("Run as your normal swarm4 user, not sudo; Ansible elevates on the target")
    for command in ("docker", "ansible-playbook", "ansible-inventory", "git"):
        if not shutil.which(command):
            parser.error(f"Required command not installed: {command}")
    identity = hashlib.sha256(str(REPO).encode()).hexdigest()[:12]
    state_dir = Path.home() / ".local/state/thinvids-inference" / identity
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    with (state_dir / "migration.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another inference migration is running")
        try:
            Migration(args.host, state_dir).execute()
        except (Exception, KeyboardInterrupt) as exc:
            print(f"Migration stopped: {exc or 'interrupted'}. State: {state_dir}", file=sys.stderr)
            print("A retired node stays disabled on failure. Rerun the same command to resume; media paths are not deleted.", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
