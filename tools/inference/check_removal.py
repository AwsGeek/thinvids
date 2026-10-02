"""Refuse recursive application removal through symlinks or mounted data."""

import json
from pathlib import Path
import subprocess

APP_PATHS = (Path("/opt/jerry/current"), Path("/opt/jerry/venv"))


def check_paths(paths, mounts):
    for path in paths:
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise RuntimeError(f"Refusing application removal through a symlink: {path}")
        for mount in mounts:
            target = Path(mount["target"])
            # Keep the usual root filesystem, but refuse mounts in /opt/jerry,
            # including any nested bind mount under a tree being removed.
            if target == path or path in target.parents or (
                target != Path("/") and target in path.parents
            ):
                raise RuntimeError(f"Refusing application removal across mount {target}: {path}")


def main():
    mounts = json.loads(subprocess.check_output(
        ["findmnt", "--json", "--list", "--output", "TARGET"], text=True
    ))["filesystems"]
    check_paths(APP_PATHS, mounts)


if __name__ == "__main__":
    main()
