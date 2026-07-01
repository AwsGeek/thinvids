#!/usr/bin/env python3
"""
Rip the main feature from a DVD and hand it off to thinvids.

This script is designed to run on the host that has:
  - the DVD drive
  - makemkvcon
  - access to thinvids WATCH_ROOT

Workflow:
  1. Probe the disc with makemkvcon robot mode.
  2. Pick the main title automatically, or let the user choose the title.
  3. Optionally choose which video, audio, and subtitle streams to keep from
     the selected title before ripping.
  4. Rip that title to a temporary directory as MKV.
  5. If ffmpeg is available, remux to keep the chosen streams, or default to
     video, audio, and English subtitle streams only.
  6. Move the finished MKV into the thinvids watch root so the existing watcher
     can queue it, or optionally submit it directly to /add_job.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_DEVICE = "/dev/sr0"
DEFAULT_SOURCE = "auto"
DEFAULT_ENV_FILE = "/etc/default/thinvids-dvd-auto"
DEFAULT_MANAGER_URL = "http://localhost:5005/add_job"
DEFAULT_QUEUE_MODE = "watch"
DEFAULT_SCRATCH_ROOT = str((Path.home() / "thinvids-dvd-tmp").resolve())
DEFAULT_STAGING_ROOT = str((Path.home() / "thinvids-dvd-staging").resolve())
DEFAULT_OUTPUT_SUBDIR = "movies"
MAKEMKV_TITLE_INFO = {
    1: "type",
    2: "name",
    3: "lang_code",
    4: "lang_name",
    5: "codec_id",
    6: "codec_short",
    7: "codec_long",
    8: "chapters",
    9: "duration",
    10: "size_human",
    11: "bytes",
    12: "extension",
    13: "bitrate",
    14: "audio_channels",
    15: "angle_info",
    16: "source_name",
    17: "sample_rate",
    18: "sample_size",
    19: "video_size",
    20: "aspect_ratio",
    21: "frame_rate",
    22: "stream_flags",
    23: "date_time",
    27: "output_name",
    30: "description",
    49: "title_name",
}
ENGLISH_LANGUAGE_CODES = {"en", "eng"}
YEAR_HINT_PATTERN = re.compile(r"\b((?:19|20)\d{2})\b")
AUTO_TITLE_FALLBACK = "dvd-rip"
GENERIC_DISC_HINTS = {
    "",
    "disc",
    "dvd",
    "dvd video",
    "movie",
    "not identified",
    "unknown",
    "untitled",
    "video ts",
    "video_ts",
}
LOW_INFORMATION_HINT_PATTERN = re.compile(r"^[a-z]{1,3}\d{1,3}[a-z]{0,2}$", re.IGNORECASE)
AUTO_TITLE_SOURCE_BONUS = {
    "disc-label": 18.0,
    "device-label": 12.0,
    "disc-info": 4.0,
    "title-source_name": -2.0,
    "title-output_name": -12.0,
    "title-title_name": -8.0,
    "title-name": -8.0,
}


def load_default_env_file(path: str) -> dict[str, str]:
    env_path = Path(path).expanduser()
    if not env_path.is_file():
        return {}

    loaded: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if not match:
            continue

        key = match.group(1)
        raw_value = match.group(2).strip()
        if raw_value:
            try:
                pieces = shlex.split(raw_value, comments=False, posix=True)
            except ValueError:
                value = raw_value
            else:
                value = "" if not pieces else " ".join(pieces)
        else:
            value = ""
        loaded[key] = value
    return loaded


def configured_string(
    file_config: dict[str, str],
    *,
    env_names: tuple[str, ...],
    file_name: str,
    fallback: str | None = None,
) -> str | None:
    for env_name in env_names:
        value = os.environ.get(env_name)
        if value is not None and value != "":
            return value
    file_value = file_config.get(file_name)
    if file_value is not None and file_value != "":
        return file_value
    return fallback


def configured_int(
    file_config: dict[str, str],
    *,
    env_names: tuple[str, ...],
    file_name: str,
    fallback: int,
) -> int:
    value = configured_string(file_config, env_names=env_names, file_name=file_name)
    if value is None:
        return fallback
    try:
        return int(value)
    except ValueError:
        return fallback


def configured_float(
    file_config: dict[str, str],
    *,
    env_names: tuple[str, ...],
    file_name: str,
    fallback: float,
) -> float:
    value = configured_string(file_config, env_names=env_names, file_name=file_name)
    if value is None:
        return fallback
    try:
        return float(value)
    except ValueError:
        return fallback


def debug(message: str, *, enabled: bool) -> None:
    if enabled:
        print(f"[dvd-rip-queue] {message}", file=sys.stderr)


def progress(message: str) -> None:
    print(message, file=sys.stderr)


def debug_stream_line(prefix: str, line: str, *, enabled: bool) -> None:
    if not enabled:
        return
    cleaned = line.rstrip("\r\n")
    if not cleaned:
        return
    debug(f"{prefix}: {cleaned}", enabled=enabled)


def run_command(
    args: list[str],
    *,
    debug_enabled: bool,
    check: bool = True,
    stream_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    debug(f"running: {' '.join(args)}", enabled=debug_enabled)
    if stream_output:
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        stdout_lines: list[str] = []
        if process.stdout is not None:
            for raw_line in process.stdout:
                stdout_lines.append(raw_line)
                debug_stream_line("subprocess", raw_line, enabled=debug_enabled)
        returncode = process.wait()
        completed = subprocess.CompletedProcess(
            args=args,
            returncode=returncode,
            stdout="".join(stdout_lines),
            stderr="",
        )
        if check and returncode != 0:
            raise subprocess.CalledProcessError(
                returncode,
                args,
                output=completed.stdout,
                stderr=completed.stderr,
            )
        return completed
    return subprocess.run(
        args,
        check=check,
        capture_output=True,
        text=True,
    )


def parse_makemkv_progress_label(payload: str) -> str:
    fields = parse_csv_fields(payload, 3)
    if fields:
        return str(fields[-1] or "").strip()
    return payload.strip()


def stream_makemkv_command(
    args: list[str],
    *,
    debug_enabled: bool,
    progress_prefix: str,
) -> subprocess.CompletedProcess[str]:
    debug(f"running: {' '.join(args)}", enabled=debug_enabled)
    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    stdout_lines: list[str] = []
    task_label = ""
    action_label = ""
    last_progress_marker: tuple[str, int, int] | None = None

    if process.stdout is not None:
        for raw_line in process.stdout:
            stdout_lines.append(raw_line)
            debug_stream_line("makemkv", raw_line, enabled=debug_enabled)
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("PRGT:"):
                task_label = parse_makemkv_progress_label(line[len("PRGT:") :])
                if task_label:
                    progress(f"{progress_prefix}: {task_label}")
                continue

            if line.startswith("PRGC:"):
                action_label = parse_makemkv_progress_label(line[len("PRGC:") :])
                if action_label and action_label != task_label:
                    progress(f"{progress_prefix}: {action_label}")
                continue

            if line.startswith("PRGV:"):
                fields = parse_csv_fields(line[len("PRGV:") :], 3)
                if not fields:
                    continue
                try:
                    current_value = int(fields[0])
                    total_value = int(fields[1])
                    maximum_value = int(fields[2])
                except Exception:
                    continue
                if maximum_value <= 0:
                    continue

                total_percent = max(0, min(100, round((total_value / maximum_value) * 100)))
                current_percent = max(0, min(100, round((current_value / maximum_value) * 100)))
                marker = (
                    action_label or task_label or "Working",
                    20 if total_percent >= 100 else total_percent // 5,
                    10 if current_percent >= 100 else current_percent // 10,
                )
                if marker == last_progress_marker:
                    continue
                last_progress_marker = marker

                label = action_label or task_label or "Working"
                if total_percent != current_percent:
                    progress(
                        f"{progress_prefix}: {label} ({total_percent}% overall, {current_percent}% current)"
                    )
                else:
                    progress(f"{progress_prefix}: {label} ({total_percent}%)")

    returncode = process.wait()
    return subprocess.CompletedProcess(
        args=args,
        returncode=returncode,
        stdout="".join(stdout_lines),
        stderr="",
    )


def command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def parse_hms_seconds(value: str | None) -> int:
    if not value:
        return 0
    match = re.search(r"(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})", value)
    if not match:
        return 0
    hours = int(match.group("h"))
    minutes = int(match.group("m"))
    seconds = int(match.group("s"))
    return hours * 3600 + minutes * 60 + seconds


def safe_filename(title: str) -> str:
    cleaned = re.sub(r"[<>:\"/\\|?*\x00-\x1f]", "", title or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.rstrip(". ")
    return cleaned or "dvd-rip"


def ensure_unique_directory(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        candidate = path.with_name(f"{path.name} ({counter})")
        if not candidate.exists():
            return candidate
        counter += 1


def remove_if_empty(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        pass


def normalize_title(value: str) -> str:
    value = value.replace("_", " ").replace(".", " ")
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"[^a-zA-Z0-9]+", " ", value).strip().lower()
    stop_words = {
        "anniversary",
        "bonus",
        "collector",
        "deluxe",
        "disc",
        "edition",
        "fullscreen",
        "movie",
        "special",
        "theatrical",
        "unrated",
        "version",
        "widescreen",
    }
    words = [word for word in value.split() if word not in stop_words]
    return " ".join(words)


def parse_csv_fields(payload: str, expected_min_fields: int) -> list[str] | None:
    try:
        row = next(csv.reader([payload]))
    except Exception:
        return None
    if len(row) < expected_min_fields:
        return None
    return row


def parse_makemkv_robot_output(text: str) -> dict[str, Any]:
    titles: dict[int, dict[str, Any]] = {}
    disc_info: dict[str, Any] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("CINFO:"):
            fields = parse_csv_fields(line[len("CINFO:") :], 3)
            if not fields:
                continue
            info_id = int(fields[0])
            value = fields[2]
            disc_info[str(info_id)] = value
            continue
        if line.startswith("TINFO:"):
            fields = parse_csv_fields(line[len("TINFO:") :], 4)
            if not fields:
                continue
            title_index = int(fields[0])
            info_id = int(fields[1])
            value = fields[3]
            title = titles.setdefault(title_index, {"index": title_index, "streams": []})
            key = MAKEMKV_TITLE_INFO.get(info_id, f"field_{info_id}")
            title[key] = value
            continue
        if line.startswith("SINFO:"):
            fields = parse_csv_fields(line[len("SINFO:") :], 5)
            if not fields:
                continue
            title_index = int(fields[0])
            stream_index = int(fields[1])
            info_id = int(fields[2])
            value = fields[4]
            title = titles.setdefault(title_index, {"index": title_index, "streams": []})
            while len(title["streams"]) <= stream_index:
                title["streams"].append({"index": len(title["streams"])})
            stream = title["streams"][stream_index]
            key = MAKEMKV_TITLE_INFO.get(info_id, f"field_{info_id}")
            stream[key] = value

    ordered_titles = []
    for title in titles.values():
        duration_seconds = parse_hms_seconds(title.get("duration"))
        try:
            size_bytes = int(title.get("bytes") or 0)
        except Exception:
            size_bytes = 0
        try:
            chapters = int(title.get("chapters") or 0)
        except Exception:
            chapters = 0
        title["duration_seconds"] = duration_seconds
        title["size_bytes"] = size_bytes
        title["chapters_count"] = chapters
        ordered_titles.append(title)

    ordered_titles.sort(
        key=lambda item: (
            item.get("duration_seconds", 0),
            item.get("size_bytes", 0),
            item.get("chapters_count", 0),
            -item.get("index", 0),
        ),
        reverse=True,
    )
    return {"disc_info": disc_info, "titles": ordered_titles}


def choose_main_title(parsed: dict[str, Any], *, min_seconds: int) -> dict[str, Any]:
    candidates = [
        title
        for title in parsed.get("titles", [])
        if int(title.get("duration_seconds") or 0) >= min_seconds
    ]
    if not candidates:
        candidates = list(parsed.get("titles", []))
    if not candidates:
        raise RuntimeError("MakeMKV did not return any titles for this disc.")
    return candidates[0]


def write_makemkv_probe_diagnostic(
    *,
    probe_stdout: str,
    probe_stderr: str,
    device: str,
    source_spec: str,
    scratch_root: Path,
) -> Path:
    scratch_root.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    device_name = Path(device).name or "unknown-device"
    diagnostic_path = scratch_root / f"makemkv-info-{device_name}-{timestamp}.log"
    diagnostic_path.write_text(
        "\n".join(
            [
                f"device={device}",
                f"source={source_spec}",
                "",
                "=== stdout ===",
                probe_stdout or "",
                "",
                "=== stderr ===",
                probe_stderr or "",
            ]
        ),
        encoding="utf-8",
    )
    return diagnostic_path


def parse_makemkv_drive_scan_output(text: str) -> list[dict[str, Any]]:
    drives: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith("DRV:"):
            continue
        fields = parse_csv_fields(line[len("DRV:") :], 7)
        if not fields:
            continue
        try:
            drive_index = int(fields[0])
        except Exception:
            continue
        drives.append(
            {
                "index": drive_index,
                "visible": fields[1],
                "enabled": fields[2],
                "flags": fields[3],
                "drive_name": fields[4],
                "disc_name": fields[5],
                "device_path": fields[6],
            }
        )
    return drives


def resolve_source_spec(source: str, device: str, *, debug_enabled: bool) -> str:
    if source and source != "auto":
        return source

    device_real = os.path.realpath(str(Path(device).expanduser()))
    scan = run_command(
        ["makemkvcon", "--robot", "info", "disc:9999"],
        debug_enabled=debug_enabled,
        check=False,
        stream_output=debug_enabled,
    )
    if scan.returncode == 0:
        for drive in parse_makemkv_drive_scan_output(scan.stdout):
            drive_path = str(drive.get("device_path") or "").strip()
            if drive_path and os.path.realpath(drive_path) == device_real:
                resolved = f"disc:{int(drive['index'])}"
                debug(f"resolved MakeMKV source {resolved} for {device_real}", enabled=debug_enabled)
                return resolved

    debug(
        f"could not resolve MakeMKV source for {device_real}; falling back to disc:0",
        enabled=debug_enabled,
    )
    return "disc:0"


def probe_disc_label(device: str, *, debug_enabled: bool) -> str:
    device_path = str(Path(device).expanduser())
    if command_exists("blkid"):
        result = run_command(
            ["blkid", "-o", "value", "-s", "LABEL", device_path],
            debug_enabled=debug_enabled,
            check=False,
        )
        label = (result.stdout or "").strip()
        if result.returncode == 0 and label:
            return label

    if command_exists("udevadm"):
        result = run_command(
            ["udevadm", "info", "--query=property", "--name", device_path],
            debug_enabled=debug_enabled,
            check=False,
        )
        if result.returncode == 0:
            for raw_line in (result.stdout or "").splitlines():
                if raw_line.startswith("ID_FS_LABEL="):
                    return raw_line.partition("=")[2].strip()
    return ""


def cleanup_title_hint(value: str) -> str:
    cleaned = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.replace("_", " ").replace(".", " ")
    cleanup_patterns = (
        r"\b16x9\b",
        r"\bws\b",
        r"\bfullscreen\b",
        r"\bwidescreen\b",
        r"\bspecial edition\b",
        r"\bcollector'?s edition\b",
        r"\btheatrical\b",
        r"\bunrated\b",
        r"\bblu[- ]?ray\b",
        r"\bdvd\b",
        r"\bdisc\s*\d+\b",
        r"\bside\s*[ab12]\b",
        r"\bsku\b",
    )
    for pattern in cleanup_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[\[\](){}]", " ", cleaned)
    cleaned = re.sub(r"\s*-\s*", " - ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_")
    return cleaned


def split_title_year_hint(value: str) -> tuple[str, str | None]:
    cleaned = cleanup_title_hint(value)
    if not cleaned:
        return "", None
    year_match = YEAR_HINT_PATTERN.search(cleaned)
    year_hint = year_match.group(1) if year_match else None
    title_only = YEAR_HINT_PATTERN.sub(" ", cleaned)
    title_only = re.sub(r"\s+", " ", title_only).strip(" -_")
    if not title_only:
        title_only = cleaned
    return title_only, year_hint


def prettify_title_hint(value: str) -> str:
    title, _year = split_title_year_hint(value)
    if not title:
        return AUTO_TITLE_FALLBACK
    return title.title()


def is_generic_title_hint(value: str) -> bool:
    normalized = normalize_title(cleanup_title_hint(value))
    return normalized in GENERIC_DISC_HINTS or len(normalized) < 2


def is_low_information_title_hint(value: str) -> bool:
    normalized = normalize_title(cleanup_title_hint(value))
    if not normalized:
        return True
    words = normalized.split()
    if len(words) != 1:
        return False
    compact = re.sub(r"[^a-z0-9]", "", words[0].lower())
    return bool(LOW_INFORMATION_HINT_PATTERN.fullmatch(compact))


def auto_title_source_bonus(source_name: str) -> float:
    return float(AUTO_TITLE_SOURCE_BONUS.get(source_name, 0.0))


def build_auto_title_hints(
    *,
    parsed: dict[str, Any],
    selected_title: dict[str, Any],
    disc_label: str,
    device: str,
    debug_enabled: bool,
) -> list[dict[str, str | None]]:
    candidates: list[tuple[str, str]] = []

    if disc_label:
        candidates.append(("disc-label", disc_label))

    probed_label = probe_disc_label(device, debug_enabled=debug_enabled)
    if probed_label:
        candidates.append(("device-label", probed_label))

    for key in ("source_name", "output_name", "title_name", "name"):
        value = str(selected_title.get(key) or "").strip()
        if value:
            candidates.append((f"title-{key}", value))

    for raw_value in (parsed.get("disc_info") or {}).values():
        value = str(raw_value or "").strip()
        if value:
            candidates.append(("disc-info", value))

    hints: list[dict[str, str | None]] = []
    seen: set[str] = set()
    for source_name, raw_value in candidates:
        title_hint, year_hint = split_title_year_hint(raw_value)
        if not title_hint or is_generic_title_hint(title_hint):
            continue
        if is_low_information_title_hint(title_hint):
            debug(
                f"skipping low-information title hint from {source_name}: {raw_value!r}",
                enabled=debug_enabled,
            )
            continue
        dedupe_key = normalize_title(title_hint)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        hints.append(
            {
                "query": title_hint,
                "year_hint": year_hint,
                "source": source_name,
                "raw_value": raw_value,
            }
        )

    debug(f"auto-title hints: {hints}", enabled=debug_enabled)
    return hints


def title_display_label(title: dict[str, Any]) -> str:
    parts = [f"title={title.get('index', '?')}"]

    duration = str(title.get("duration") or "").strip()
    if duration:
        parts.append(duration)

    chapters = title.get("chapters_count")
    if chapters:
        parts.append(f"chapters={chapters}")

    size_bytes = int(title.get("size_bytes") or 0)
    if size_bytes > 0:
        parts.append(f"size={round(size_bytes / (1024**3), 2)}GiB")

    source_name = str(title.get("source_name") or title.get("output_name") or title.get("title_name") or "").strip()
    if source_name:
        parts.append(source_name)

    return " | ".join(parts)


def prompt_for_title_selection(
    *,
    parsed: dict[str, Any],
    default_title: dict[str, Any],
) -> dict[str, Any]:
    if not sys.stdin.isatty():
        raise RuntimeError("Interactive title selection requires an interactive terminal.")

    titles = list(parsed.get("titles") or [])
    if not titles:
        raise RuntimeError("MakeMKV did not return any titles for this disc.")

    print("Select the title to rip:", file=sys.stderr)
    for menu_index, title in enumerate(titles, start=1):
        print(f"  {menu_index}. {title_display_label(title)}", file=sys.stderr)

    default_menu_index = titles.index(default_title) + 1
    print(
        f"Auto-selected title: {default_menu_index} ({title_display_label(default_title)})",
        file=sys.stderr,
    )

    while True:
        raw = input(f"Choose one title [{default_menu_index}]: ").strip()
        if not raw:
            return default_title
        if raw.isdigit():
            selection = int(raw)
            if 1 <= selection <= len(titles):
                return titles[selection - 1]
        print("Enter one number from the title list.", file=sys.stderr)


def tmdb_get_json(url: str, *, debug_enabled: bool) -> dict[str, Any] | None:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "thinvids-dvd-rip-queue/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        debug(f"TMDb request failed: {exc}", enabled=debug_enabled)
        return None


def tmdb_search_movies(
    query: str,
    api_key: str,
    *,
    year: str | None = None,
    debug_enabled: bool,
) -> list[dict[str, Any]]:
    query_params = {
        "api_key": api_key,
        "query": query,
        "include_adult": "true",
    }
    if year:
        query_params["year"] = year
    params = urllib.parse.urlencode(query_params)
    data = tmdb_get_json(f"https://api.themoviedb.org/3/search/movie?{params}", debug_enabled=debug_enabled)
    if not data:
        return []
    return list(data.get("results") or [])


def tmdb_movie_details(movie_id: int, api_key: str, *, debug_enabled: bool) -> dict[str, Any] | None:
    params = urllib.parse.urlencode({"api_key": api_key})
    return tmdb_get_json(f"https://api.themoviedb.org/3/movie/{movie_id}?{params}", debug_enabled=debug_enabled)


def score_tmdb_title_similarity(
    *,
    query_norm: str,
    candidate_title: str,
    runtime_seconds: int | None,
) -> float:
    candidate_norm = normalize_title(candidate_title)
    sequence_score = difflib.SequenceMatcher(None, query_norm, candidate_norm).ratio()

    query_words = query_norm.split()
    candidate_words = candidate_norm.split()
    if runtime_seconds and len(query_words) == 1 and query_words[0] in candidate_words:
        # A one-word disc label like "FELLOWSHIP" should not overwhelmingly
        # prefer a short exact-title match over a longer title that contains the
        # same word and has a much better runtime match.
        return 0.76
    return sequence_score


def runtime_score_adjustment(runtime_seconds: int | None, candidate_runtime_minutes: Any) -> float:
    if not runtime_seconds or not candidate_runtime_minutes:
        return 0.0
    runtime_delta_minutes = abs((int(candidate_runtime_minutes) * 60) - runtime_seconds) / 60.0
    return max(-90.0, 25.0 - runtime_delta_minutes)


def score_tmdb_candidate(
    *,
    query: str,
    candidate: dict[str, Any],
    runtime_seconds: int | None,
) -> float:
    query_norm = normalize_title(query)
    title = candidate.get("title") or ""
    original_title = candidate.get("original_title") or ""
    title_score = score_tmdb_title_similarity(
        query_norm=query_norm,
        candidate_title=title,
        runtime_seconds=runtime_seconds,
    )
    original_score = score_tmdb_title_similarity(
        query_norm=query_norm,
        candidate_title=original_title,
        runtime_seconds=runtime_seconds,
    )
    score = max(title_score, original_score) * 100.0
    score += runtime_score_adjustment(runtime_seconds, candidate.get("runtime"))
    if candidate.get("release_date"):
        score += 1.0
    return round(score, 2)


def enrich_tmdb_candidates(
    *,
    query: str,
    api_key: str,
    year_hint: str | None = None,
    runtime_seconds: int | None,
    debug_enabled: bool,
) -> list[dict[str, Any]]:
    raw_results = tmdb_search_movies(query, api_key, year=year_hint, debug_enabled=debug_enabled)
    if not raw_results:
        return []

    candidates: list[dict[str, Any]] = []
    for movie in raw_results[:5]:
        movie_id = movie.get("id")
        if not isinstance(movie_id, int):
            continue
        details = tmdb_movie_details(movie_id, api_key, debug_enabled=debug_enabled) or {}
        merged = {**movie, **details}
        merged["score"] = score_tmdb_candidate(
            query=query,
            candidate=merged,
            runtime_seconds=runtime_seconds,
        )
        candidates.append(merged)

    candidates.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    return candidates


def candidate_display_label(candidate: dict[str, Any]) -> str:
    year = (candidate.get("release_date") or "????")[:4]
    runtime = candidate.get("runtime") or "?"
    score = candidate.get("score", "?")
    title = candidate.get("title") or candidate.get("original_title") or "<untitled>"
    return f"{title} ({year}) runtime={runtime}m score={score}"


def select_tmdb_candidate(
    candidates: list[dict[str, Any]],
    *,
    allow_noninteractive_best: bool = False,
    noninteractive_min_score: float = 0.0,
) -> dict[str, Any]:
    if len(candidates) == 1:
        candidate = candidates[0]
        print(f"Using TMDb match: {candidate_display_label(candidate)}", file=sys.stderr)
        return candidate
    if not sys.stdin.isatty():
        best = candidates[0]
        best_score = float(best.get("score") or 0.0)
        if allow_noninteractive_best and best_score >= noninteractive_min_score:
            print(
                f"Auto-selected best TMDb match: {candidate_display_label(best)}",
                file=sys.stderr,
            )
            return best
        raise RuntimeError("Multiple TMDb matches found, but no interactive terminal is available to choose one.")

    print("Select the matching movie:", file=sys.stderr)
    for index, candidate in enumerate(candidates, start=1):
        print(f"  {index}. {candidate_display_label(candidate)}", file=sys.stderr)
    print("  0. none of these", file=sys.stderr)

    while True:
        raw = input("> ").strip()
        if raw == "0":
            raise RuntimeError("No TMDb title was selected.")
        if raw.isdigit():
            selection = int(raw)
            if 1 <= selection <= len(candidates):
                return candidates[selection - 1]
        print("Enter a number from the list.", file=sys.stderr)


def choose_movie_metadata(
    *,
    query: str,
    year_hint: str | None = None,
    runtime_seconds: int | None,
    api_key: str,
    debug_enabled: bool,
    allow_noninteractive_best: bool = False,
    noninteractive_min_score: float = 0.0,
) -> dict[str, Any]:
    candidates = enrich_tmdb_candidates(
        query=query,
        api_key=api_key,
        year_hint=year_hint,
        runtime_seconds=runtime_seconds,
        debug_enabled=debug_enabled,
    )
    if not candidates:
        raise RuntimeError(f'No TMDb movie matches found for "{query}".')
    selected = select_tmdb_candidate(
        candidates,
        allow_noninteractive_best=allow_noninteractive_best,
        noninteractive_min_score=noninteractive_min_score,
    )
    return {
        "tmdb_id": selected.get("id"),
        "title": selected.get("title") or selected.get("original_title") or query,
        "year": ((selected.get("release_date") or "")[:4] or None),
        "release_date": selected.get("release_date") or "",
        "runtime_minutes": selected.get("runtime"),
        "score": selected.get("score"),
        "source": "tmdb",
        "query_used": query,
        "needs_manual_review": False,
        "review_reason": "",
        "candidates": [
            {
                "id": candidate.get("id"),
                "title": candidate.get("title"),
                "release_date": candidate.get("release_date"),
                "runtime": candidate.get("runtime"),
                "score": candidate.get("score"),
            }
            for candidate in candidates
        ],
    }


def build_basic_movie_metadata(
    title: str,
    *,
    year: str | None = None,
    source: str,
    query_used: str | None = None,
) -> dict[str, Any]:
    display_title, extracted_year = split_title_year_hint(title)
    resolved_title = prettify_title_hint(display_title or title)
    resolved_year = year or extracted_year
    return {
        "tmdb_id": None,
        "title": resolved_title,
        "year": resolved_year,
        "release_date": f"{resolved_year}-01-01" if resolved_year else "",
        "runtime_minutes": None,
        "score": None,
        "source": source,
        "query_used": query_used or title,
        "needs_manual_review": False,
        "review_reason": "",
        "candidates": [],
    }


def auto_detect_movie_metadata(
    *,
    parsed: dict[str, Any],
    selected_title: dict[str, Any],
    disc_label: str,
    device: str,
    api_key: str | None,
    debug_enabled: bool,
    min_score: float,
) -> dict[str, Any]:
    runtime_seconds = int(selected_title.get("duration_seconds") or 0) or None
    hints = build_auto_title_hints(
        parsed=parsed,
        selected_title=selected_title,
        disc_label=disc_label,
        device=device,
        debug_enabled=debug_enabled,
    )
    if not hints:
        fallback = build_basic_movie_metadata(
            AUTO_TITLE_FALLBACK,
            source="auto-title-unavailable",
            query_used=AUTO_TITLE_FALLBACK,
        )
        fallback["needs_manual_review"] = True
        fallback["review_reason"] = "could not derive usable title hints from the disc metadata"
        print("Could not derive a reliable title from disc metadata; staging for manual review.", file=sys.stderr)
        return fallback

    if api_key:
        merged: dict[int, dict[str, Any]] = {}
        for hint in hints:
            query = str(hint.get("query") or "").strip()
            if not query:
                continue
            candidates = enrich_tmdb_candidates(
                query=query,
                api_key=api_key,
                year_hint=str(hint.get("year_hint") or "") or None,
                runtime_seconds=runtime_seconds,
                debug_enabled=debug_enabled,
            )
            debug(
                f'auto-title query "{query}" returned {len(candidates)} candidate(s)',
                enabled=debug_enabled,
            )
            for candidate in candidates:
                movie_id = candidate.get("id")
                if not isinstance(movie_id, int):
                    continue
                candidate_with_query = {
                    **candidate,
                    "query_used": query,
                    "query_source": hint.get("source"),
                }
                weighted_score = float(candidate_with_query.get("score") or 0.0) + auto_title_source_bonus(
                    str(hint.get("source") or "")
                )
                candidate_with_query["auto_score"] = round(weighted_score, 2)
                existing = merged.get(movie_id)
                if existing is None or float(candidate_with_query.get("auto_score") or 0.0) > float(existing.get("auto_score") or 0.0):
                    merged[movie_id] = candidate_with_query

        auto_candidates = sorted(
            merged.values(),
            key=lambda item: item.get("auto_score", item.get("score", 0.0)),
            reverse=True,
        )
        if auto_candidates:
            best_candidate = auto_candidates[0]
            best_score = float(best_candidate.get("auto_score") or best_candidate.get("score") or 0.0)
            candidate_summaries = [
                {
                    "id": candidate.get("id"),
                    "title": candidate.get("title"),
                    "release_date": candidate.get("release_date"),
                    "runtime": candidate.get("runtime"),
                    "score": candidate.get("score"),
                    "auto_score": candidate.get("auto_score"),
                    "query_used": candidate.get("query_used"),
                    "query_source": candidate.get("query_source"),
                }
                for candidate in auto_candidates
            ]
            if best_score < min_score:
                print(
                    (
                        "Automatic TMDb title match is below the confidence threshold; "
                        f"best candidate was {candidate_display_label(best_candidate)} "
                        f'from {best_candidate.get("query_source") or "unknown"}'
                    ),
                    file=sys.stderr,
                )
                return {
                    "tmdb_id": best_candidate.get("id"),
                    "title": best_candidate.get("title") or best_candidate.get("original_title") or hints[0]["query"],
                    "year": ((best_candidate.get("release_date") or "")[:4] or None),
                    "release_date": best_candidate.get("release_date") or "",
                    "runtime_minutes": best_candidate.get("runtime"),
                    "score": best_candidate.get("score"),
                    "source": "tmdb-auto-low-confidence",
                    "query_used": best_candidate.get("query_used") or hints[0]["query"],
                    "needs_manual_review": True,
                    "review_reason": (
                        f'best weighted TMDb score {best_score:.2f} is below the configured minimum '
                        f"of {min_score:.2f}"
                    ),
                    "candidates": candidate_summaries,
                }

            if sys.stdin.isatty():
                try:
                    selected = select_tmdb_candidate(auto_candidates)
                except RuntimeError as exc:
                    debug(f"TMDb auto-select declined: {exc}", enabled=debug_enabled)
                else:
                    return {
                        "tmdb_id": selected.get("id"),
                        "title": selected.get("title") or selected.get("original_title") or hints[0]["query"],
                        "year": ((selected.get("release_date") or "")[:4] or None),
                        "release_date": selected.get("release_date") or "",
                        "runtime_minutes": selected.get("runtime"),
                        "score": selected.get("score"),
                        "source": "tmdb-auto",
                        "query_used": selected.get("query_used") or hints[0]["query"],
                        "needs_manual_review": False,
                        "review_reason": "",
                        "candidates": candidate_summaries,
                    }
            else:
                if len(auto_candidates) == 1:
                    print(f"Using TMDb match: {candidate_display_label(best_candidate)}", file=sys.stderr)
                else:
                    print(
                        f"Auto-selected best TMDb match: {candidate_display_label(best_candidate)}",
                        file=sys.stderr,
                    )
                return {
                    "tmdb_id": best_candidate.get("id"),
                    "title": best_candidate.get("title") or best_candidate.get("original_title") or hints[0]["query"],
                    "year": ((best_candidate.get("release_date") or "")[:4] or None),
                    "release_date": best_candidate.get("release_date") or "",
                    "runtime_minutes": best_candidate.get("runtime"),
                    "score": best_candidate.get("score"),
                    "source": "tmdb-auto",
                    "query_used": best_candidate.get("query_used") or hints[0]["query"],
                    "needs_manual_review": False,
                    "review_reason": "",
                    "candidates": candidate_summaries,
                }

    fallback_hint = hints[0]
    fallback = build_basic_movie_metadata(
        str(fallback_hint.get("query") or AUTO_TITLE_FALLBACK),
        year=str(fallback_hint.get("year_hint") or "") or None,
        source="disc-label-fallback",
        query_used=str(fallback_hint.get("raw_value") or fallback_hint.get("query") or AUTO_TITLE_FALLBACK),
    )
    fallback["needs_manual_review"] = True
    fallback["review_reason"] = "automatic title detection fell back to local disc metadata"
    print(
        f'Using disc title fallback: {fallback["title"]}' + (f' ({fallback["year"]})' if fallback.get("year") else ""),
        file=sys.stderr,
    )
    return fallback


def rip_title(
    *,
    source: str,
    title_index: int,
    output_dir: Path,
    debug_enabled: bool,
) -> subprocess.CompletedProcess[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    args = [
        "makemkvcon",
        "--robot",
        "--progress=-same",
        "mkv",
        source,
        str(title_index),
        str(output_dir),
    ]
    debug(f"streaming MakeMKV output for title {title_index}", enabled=debug_enabled)
    completed = stream_makemkv_command(
        args,
        debug_enabled=debug_enabled,
        progress_prefix=f"MakeMKV rip title {title_index}",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "makemkvcon mkv failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def find_single_mkv(path: Path, *, rip_output: subprocess.CompletedProcess[str] | None = None) -> Path:
    mkvs = sorted(candidate for candidate in path.rglob("*.mkv") if candidate.is_file())
    if len(mkvs) != 1:
        details = f"Expected exactly one MKV under {path}, found {len(mkvs)}"
        if mkvs:
            details += "\nFound:\n" + "\n".join(str(item) for item in mkvs)
        if rip_output is not None:
            details += (
                "\nMakeMKV stdout:\n"
                f"{rip_output.stdout}\n"
                "MakeMKV stderr:\n"
                f"{rip_output.stderr}"
            )
        raise RuntimeError(details)
    return mkvs[0]


def ffprobe_streams(path: Path, *, debug_enabled: bool) -> list[dict[str, Any]]:
    completed = run_command(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            (
                "stream=index,codec_type,codec_name,width,height,channels,channel_layout"
                ":stream_tags=language,title"
            ),
            "-of",
            "json",
            str(path),
        ],
        debug_enabled=debug_enabled,
    )
    payload = json.loads(completed.stdout or "{}")
    return list(payload.get("streams") or [])


def resolution_label_from_path(path: Path, *, debug_enabled: bool) -> str:
    if not command_exists("ffprobe"):
        debug("ffprobe not available; using unknown resolution label", enabled=debug_enabled)
        return "unknown"
    streams = ffprobe_streams(path, debug_enabled=debug_enabled)
    for stream in streams:
        if stream.get("codec_type") != "video":
            continue
        height = stream.get("height")
        try:
            height_int = int(height)
        except Exception:
            continue
        if height_int > 0:
            return f"{height_int}p"
    return "unknown"


def format_movie_display_name(title: str, year: str | None) -> str:
    safe_title = safe_filename(title)
    if year:
        return f"{safe_title} ({year})"
    return safe_title


def build_final_path(
    *,
    watch_root: Path,
    output_subdir: str,
    movie_title: str,
    movie_year: str | None,
    resolution_label: str,
    suffix: str,
    current_path: Path | None = None,
) -> Path:
    display_name = format_movie_display_name(movie_title, movie_year)
    folder = watch_root / output_subdir / display_name
    filename = f"{display_name} {resolution_label} h264{suffix}"
    return ensure_unique_destination(folder / filename, current_path=current_path)


def build_planned_output_path(
    *,
    watch_root: Path,
    output_subdir: str,
    movie_title: str,
    movie_year: str | None,
    suffix: str,
) -> Path:
    display_name = format_movie_display_name(movie_title, movie_year)
    folder = watch_root / output_subdir / display_name
    filename = f"{display_name} <resolution> h264{suffix}"
    return folder / filename


def normalize_output_subdir(value: str) -> str:
    cleaned = str(Path(value)).strip()
    cleaned = cleaned.replace("\\", "/").strip("/")
    if not cleaned or cleaned == ".":
        raise argparse.ArgumentTypeError("Output directory must be a relative path under WATCH_ROOT.")
    return cleaned


def canonical_stream_type(stream: dict[str, Any]) -> str:
    raw_type = str(stream.get("codec_type") or stream.get("type") or "").strip().lower()
    if raw_type.startswith("video"):
        return "video"
    if raw_type.startswith("audio"):
        return "audio"
    if raw_type.startswith("sub"):
        return "subtitle"
    return raw_type


def stream_language_value(stream: dict[str, Any]) -> str:
    tags = stream.get("tags") or {}
    for value in (
        tags.get("language"),
        stream.get("lang_code"),
        stream.get("lang_name"),
    ):
        cleaned = str(value or "").strip().lower()
        if cleaned:
            return cleaned
    return ""


def stream_codec_value(stream: dict[str, Any]) -> str:
    for value in (
        stream.get("codec_name"),
        stream.get("codec_short"),
        stream.get("codec_long"),
    ):
        cleaned = str(value or "").strip().lower()
        if cleaned:
            return cleaned
    return ""


def has_english_subtitles(streams: list[dict[str, Any]]) -> bool:
    for stream in streams:
        if canonical_stream_type(stream) != "subtitle":
            continue
        if is_english_stream(stream):
            return True
    return False


def is_english_stream(stream: dict[str, Any]) -> bool:
    language = stream_language_value(stream)
    return language in ENGLISH_LANGUAGE_CODES or language == "english"


def choose_default_audio_stream(audio_streams: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not audio_streams:
        return None

    for stream in audio_streams:
        if is_english_stream(stream) and stream_codec_value(stream) == "ac3":
            return stream

    for stream in audio_streams:
        if is_english_stream(stream):
            return stream

    return audio_streams[0]


def choose_default_subtitle_stream(subtitle_streams: list[dict[str, Any]]) -> dict[str, Any] | None:
    for stream in subtitle_streams:
        if is_english_stream(stream):
            return stream
    return None


def stream_display_label(stream: dict[str, Any]) -> str:
    tags = stream.get("tags") or {}
    parts = [f"stream={stream.get('index', '?')}"]

    codec_type = canonical_stream_type(stream)
    codec_name = stream_codec_value(stream)
    if codec_type:
        parts.append(codec_type)
    if codec_name:
        parts.append(codec_name)

    if codec_type == "video":
        width = stream.get("width")
        height = stream.get("height")
        video_size = str(stream.get("video_size") or "").strip()
        if width and height:
            parts.append(f"{width}x{height}")
        elif video_size:
            parts.append(video_size)
    elif codec_type == "audio":
        channels = stream.get("channels") or stream.get("audio_channels")
        channel_layout = str(stream.get("channel_layout") or "").strip()
        if channels:
            parts.append(f"{channels}ch")
        if channel_layout:
            parts.append(channel_layout)

    language = str(
        (tags.get("language") or stream.get("lang_code") or stream.get("lang_name") or "")
    ).strip()
    title = str(tags.get("title") or stream.get("name") or stream.get("title_name") or "").strip()
    if language:
        parts.append(f"lang={language}")
    if title:
        parts.append(f"title={title}")
    return " | ".join(parts)


def print_stream_group(label: str, streams: list[dict[str, Any]]) -> None:
    print(f"Select {label} stream(s):", file=sys.stderr)
    for menu_index, stream in enumerate(streams, start=1):
        print(f"  {menu_index}. {stream_display_label(stream)}", file=sys.stderr)


def parse_menu_selection(
    raw_value: str,
    *,
    streams: list[dict[str, Any]],
    allow_multiple: bool,
    allow_none: bool,
) -> list[dict[str, Any]] | None:
    raw_value = raw_value.strip().lower()
    if raw_value == "none" and allow_none:
        return []
    if not raw_value:
        return None

    pieces = [piece.strip() for piece in raw_value.split(",") if piece.strip()]
    if not pieces:
        return None
    if not allow_multiple and len(pieces) != 1:
        return None

    selected: list[dict[str, Any]] = []
    seen_indices: set[int] = set()
    for piece in pieces:
        if not piece.isdigit():
            return None
        menu_index = int(piece)
        if menu_index < 1 or menu_index > len(streams):
            return None
        stream = streams[menu_index - 1]
        stream_index = int(stream.get("index"))
        if stream_index in seen_indices:
            continue
        seen_indices.add(stream_index)
        selected.append(stream)
    return selected


def prompt_for_stream_selection(
    *,
    title_streams: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not sys.stdin.isatty():
        raise RuntimeError("Manual stream selection requires an interactive terminal.")

    video_streams = [stream for stream in title_streams if canonical_stream_type(stream) == "video"]
    audio_streams = [stream for stream in title_streams if canonical_stream_type(stream) == "audio"]
    subtitle_streams = [stream for stream in title_streams if canonical_stream_type(stream) == "subtitle"]

    if not video_streams:
        raise RuntimeError("No video streams were found in the selected MakeMKV title.")

    print_stream_group("video", video_streams)
    print(
        f"Auto-selected video stream: 1 ({stream_display_label(video_streams[0])})",
        file=sys.stderr,
    )
    while True:
        raw = input("Choose one video stream [1]: ")
        selected = parse_menu_selection(
            raw,
            streams=video_streams,
            allow_multiple=False,
            allow_none=False,
        )
        if selected is None:
            if not raw.strip():
                selected = [video_streams[0]]
            else:
                print("Enter one number from the video list.", file=sys.stderr)
                continue
        selected_video = selected[0]
        break

    selected_audio: list[dict[str, Any]] = []
    if audio_streams:
        default_audio = choose_default_audio_stream(audio_streams)
        print_stream_group("audio", audio_streams)
        if default_audio is not None:
            print(
                (
                    "Auto-selected audio stream: "
                    f"{audio_streams.index(default_audio) + 1} ({stream_display_label(default_audio)})"
                ),
                file=sys.stderr,
            )
        while True:
            default_audio_menu_index = 1 if default_audio is None else audio_streams.index(default_audio) + 1
            raw = input(f"Choose one audio stream [{default_audio_menu_index}]: ")
            selected = parse_menu_selection(
                raw,
                streams=audio_streams,
                allow_multiple=False,
                allow_none=False,
            )
            if selected is None:
                if not raw.strip():
                    selected_audio = [] if default_audio is None else [default_audio]
                    break
                print("Enter one number from the audio list.", file=sys.stderr)
                continue
            selected_audio = selected
            break

    selected_subtitles: list[dict[str, Any]] = []
    if subtitle_streams:
        print_stream_group("subtitle", subtitle_streams)
        default_subtitle = choose_default_subtitle_stream(subtitle_streams)
        if default_subtitle is not None:
            print(
                (
                    "Auto-selected subtitle stream: "
                    f"{subtitle_streams.index(default_subtitle) + 1} ({stream_display_label(default_subtitle)})"
                ),
                file=sys.stderr,
            )
        subtitle_prompt = "Choose one subtitle stream"
        if default_subtitle is not None:
            subtitle_default_menu_index = subtitle_streams.index(default_subtitle) + 1
            subtitle_prompt += f" [{subtitle_default_menu_index}]"
        else:
            subtitle_prompt += " [none]"
        while True:
            raw = input(f"{subtitle_prompt} (or 'none'): ")
            selected = parse_menu_selection(
                raw,
                streams=subtitle_streams,
                allow_multiple=False,
                allow_none=True,
            )
            if selected is None:
                if not raw.strip():
                    selected_subtitles = [default_subtitle] if default_subtitle is not None else []
                    break
                print("Enter one subtitle number, or 'none'.", file=sys.stderr)
                continue
            selected_subtitles = selected
            break

    selected_streams = [selected_video, *selected_audio, *selected_subtitles]
    resolved: list[dict[str, Any]] = []
    for stream in selected_streams:
        stream_type = canonical_stream_type(stream)
        type_streams = [item for item in title_streams if canonical_stream_type(item) == stream_type]
        resolved.append(
            {
                "codec_type": stream_type,
                "ordinal": type_streams.index(stream),
            }
        )
    return resolved


def resolve_selected_streams(
    *,
    selected_stream_specs: list[dict[str, Any]],
    input_path: Path,
    debug_enabled: bool,
) -> list[dict[str, Any]]:
    streams = ffprobe_streams(input_path, debug_enabled=debug_enabled)
    resolved: list[dict[str, Any]] = []
    for spec in selected_stream_specs:
        stream_type = str(spec.get("codec_type") or "").strip().lower()
        ordinal = int(spec.get("ordinal") or 0)
        type_streams = [stream for stream in streams if canonical_stream_type(stream) == stream_type]
        if ordinal < 0 or ordinal >= len(type_streams):
            raise RuntimeError(
                f"Selected {stream_type} stream #{ordinal + 1} was not present in the ripped MKV."
            )
        resolved.append(type_streams[ordinal])
    return resolved


def build_manual_map_args(streams: list[dict[str, Any]]) -> list[str]:
    map_args: list[str] = []
    for stream in streams:
        map_args.extend(["-map", f"0:{int(stream['index'])}"])
    return map_args


def remux_with_english_subtitles(
    *,
    input_path: Path,
    output_path: Path,
    title: str,
    debug_enabled: bool,
    selected_streams: list[dict[str, Any]] | None = None,
) -> bool:
    if not command_exists("ffmpeg") or not command_exists("ffprobe"):
        debug("ffmpeg/ffprobe not available; keeping raw MakeMKV output", enabled=debug_enabled)
        return False

    streams = ffprobe_streams(input_path, debug_enabled=debug_enabled)
    if selected_streams is None:
        audio_streams = [stream for stream in streams if stream.get("codec_type") == "audio"]
        subtitle_streams = [stream for stream in streams if stream.get("codec_type") == "subtitle"]
        default_audio = choose_default_audio_stream(audio_streams)
        default_subtitle = choose_default_subtitle_stream(subtitle_streams)
        map_args = [
            "-map",
            "0:v",
        ]
        if default_audio is not None:
            map_args.extend(["-map", f"0:{int(default_audio['index'])}"])
        if default_subtitle is not None:
            map_args.extend(["-map", f"0:{int(default_subtitle['index'])}"])
        english_subtitles_kept = default_subtitle is not None
    else:
        map_args = build_manual_map_args(selected_streams)
        english_subtitles_kept = has_english_subtitles(
            [stream for stream in selected_streams if stream.get("codec_type") == "subtitle"]
        )

    ffmpeg_args = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(input_path),
        *map_args,
        "-map_metadata",
        "0",
        "-map_chapters",
        "0",
        "-metadata",
        f"title={title}",
        "-c",
        "copy",
        str(output_path),
    ]
    completed = run_command(ffmpeg_args, debug_enabled=debug_enabled, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            "ffmpeg remux failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return english_subtitles_kept


def ensure_unique_destination(path: Path, *, current_path: Path | None = None) -> Path:
    if current_path is not None and path.resolve() == current_path.resolve():
        return path
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem} ({counter}){suffix}")
        if current_path is not None and candidate.resolve() == current_path.resolve():
            return candidate
        if not candidate.exists():
            return candidate
        counter += 1


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in manifest: {path}")
    return payload


def infer_bundle_paths(path: Path) -> tuple[Path, Path | None]:
    candidate = path.expanduser().resolve()
    if not candidate.exists():
        raise RuntimeError(f"Bundle path does not exist: {candidate}")

    if candidate.is_dir():
        mkvs = sorted(item for item in candidate.iterdir() if item.is_file() and item.suffix.lower() == ".mkv")
        manifests = sorted(item for item in candidate.iterdir() if item.is_file() and item.suffix.lower() == ".json")
        if len(mkvs) != 1:
            raise RuntimeError(f"Expected exactly one MKV in {candidate}, found {len(mkvs)}.")
        if len(manifests) > 1:
            raise RuntimeError(f"Expected at most one manifest in {candidate}, found {len(manifests)}.")
        return mkvs[0], manifests[0] if manifests else None

    if candidate.suffix.lower() == ".json":
        manifest_path = candidate
        manifest = load_manifest(manifest_path)
        staged_mkv_raw = str(manifest.get("staged_mkv") or "").strip()
        if staged_mkv_raw:
            staged_mkv = Path(staged_mkv_raw).expanduser().resolve()
            if staged_mkv.exists():
                return staged_mkv, manifest_path
        sibling_mkv = manifest_path.with_suffix(".mkv")
        if sibling_mkv.exists():
            return sibling_mkv, manifest_path
        raise RuntimeError(f"Could not locate MKV for manifest: {manifest_path}")

    staged_mkv = candidate
    manifest_path = staged_mkv.with_suffix(".json")
    if manifest_path.exists():
        return staged_mkv, manifest_path
    return staged_mkv, None


def load_bundle(path: str) -> tuple[Path, Path | None, dict[str, Any]]:
    bundle_mkv, manifest_path = infer_bundle_paths(Path(path))
    manifest = load_manifest(manifest_path) if manifest_path is not None else {}
    return bundle_mkv, manifest_path, manifest


def load_staged_bundle(path: str) -> tuple[Path, Path | None, dict[str, Any]]:
    return load_bundle(path)


def stage_bundle_name(
    *,
    disc_label: str,
    movie_title: str,
) -> str:
    label_hint = safe_filename(disc_label or movie_title or AUTO_TITLE_FALLBACK)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    return f"{timestamp} {label_hint}"


def stage_for_manual_review(
    *,
    finished_path: Path,
    manifest: dict[str, Any],
    staging_root: Path,
    disc_label: str,
    movie_title: str,
) -> tuple[Path, Path]:
    staging_root.mkdir(parents=True, exist_ok=True)
    bundle_dir = ensure_unique_directory(
        staging_root / stage_bundle_name(disc_label=disc_label, movie_title=movie_title)
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)

    staged_mkv = bundle_dir / finished_path.name
    shutil.move(str(finished_path), str(staged_mkv))

    manifest["staged_mkv"] = str(staged_mkv)
    manifest["review_status"] = "pending"
    manifest["staged_at_epoch"] = time.time()

    manifest_path = staged_mkv.with_suffix(".json")
    write_manifest(manifest_path, manifest)
    return staged_mkv, manifest_path


def cleanup_staged_bundle(manifest_path: Path | None, staged_mkv: Path) -> None:
    if manifest_path is not None and manifest_path.exists():
        manifest_path.unlink()
    remove_if_empty(staged_mkv.parent)


def cleanup_original_bundle(manifest_path: Path | None, bundle_mkv: Path) -> None:
    if manifest_path is not None and manifest_path.exists():
        manifest_path.unlink()
    remove_if_empty(bundle_mkv.parent)


def submit_add_job(manager_url: str, filename: str) -> dict[str, Any]:
    payload = {
        "filename": filename,
        # Prevent the watcher from also auto-submitting the same file later.
        "mark_watcher_processed": True,
    }
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        manager_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        body = response.read().decode("utf-8")
    return json.loads(body or "{}")


def main() -> int:
    env_file = os.environ.get("THINVIDS_DVD_ENV_FILE", DEFAULT_ENV_FILE)
    file_config = load_default_env_file(env_file)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("title", nargs="?", help="Movie title to use for the ripped file; omit to auto-detect")
    parser.add_argument("--device", default=DEFAULT_DEVICE, help="Optical device path, e.g. /dev/sr0 or /dev/sr1")
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help='MakeMKV source specifier, e.g. disc:0 or "auto" to resolve from --device',
    )
    parser.add_argument(
        "--watch-root",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_WATCH_ROOT", "WATCH_ROOT"),
            file_name="THINVIDS_DVD_WATCH_ROOT",
            fallback="/watch",
        ),
        help="Thinvids watch root",
    )
    parser.add_argument(
        "--queue-mode",
        choices=("watch", "api"),
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_QUEUE_MODE",),
            file_name="THINVIDS_DVD_QUEUE_MODE",
            fallback=DEFAULT_QUEUE_MODE,
        ),
        help="Queue by dropping into WATCH_ROOT or by calling /add_job directly",
    )
    parser.add_argument(
        "--manager-url",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_MANAGER_URL",),
            file_name="THINVIDS_DVD_MANAGER_URL",
            fallback=DEFAULT_MANAGER_URL,
        ),
        help="thinvids /add_job URL",
    )
    parser.add_argument(
        "--tmdb-api-key",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_TMDB_API_KEY", "TMDB_API_KEY"),
            file_name="THINVIDS_DVD_TMDB_API_KEY",
        ),
        help="TMDb API key",
    )
    parser.add_argument(
        "--disc-label",
        default="",
        help="Optional optical-disc label hint, e.g. from udev ID_FS_LABEL",
    )
    parser.add_argument(
        "--auto-title-min-score",
        type=float,
        default=configured_float(
            file_config,
            env_names=("THINVIDS_DVD_AUTO_TITLE_MIN_SCORE",),
            file_name="THINVIDS_DVD_AUTO_TITLE_MIN_SCORE",
            fallback=60.0,
        ),
        help="Minimum TMDb score for unattended auto-title selection before falling back to the disc label",
    )
    parser.add_argument(
        "--output-subdir",
        "--output-dir",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_OUTPUT_SUBDIR",),
            file_name="THINVIDS_DVD_OUTPUT_SUBDIR",
            fallback=DEFAULT_OUTPUT_SUBDIR,
        ),
        type=normalize_output_subdir,
        help="Relative output directory under WATCH_ROOT, e.g. movies or movies/dvdrips",
    )
    parser.add_argument(
        "--min-seconds",
        type=int,
        default=configured_int(
            file_config,
            env_names=("THINVIDS_DVD_MIN_SECONDS",),
            file_name="THINVIDS_DVD_MIN_SECONDS",
            fallback=40 * 60,
        ),
        help="Minimum duration for the automatic main-title pick",
    )
    parser.add_argument(
        "--title-index",
        type=int,
        help="Override the automatic title selection with a MakeMKV title index",
    )
    parser.add_argument(
        "--scratch-root",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_SCRATCH_ROOT", "DVD_RIP_SCRATCH_ROOT"),
            file_name="THINVIDS_DVD_SCRATCH_ROOT",
            fallback=DEFAULT_SCRATCH_ROOT,
        ),
        help="Temporary working directory root",
    )
    parser.add_argument(
        "--staging-root",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_STAGING_ROOT", "DVD_RIP_STAGING_ROOT"),
            file_name="THINVIDS_DVD_STAGING_ROOT",
            fallback=DEFAULT_STAGING_ROOT,
        ),
        help="Directory used to hold low-confidence rips for later manual review",
    )
    parser.add_argument(
        "--staged-path",
        help="Reuse a staged MKV, manifest, or staging directory instead of scanning and ripping a DVD",
    )
    parser.add_argument(
        "--rename-path",
        help="Reuse an existing finalized MKV, manifest, or bundle directory and rename it in place",
    )
    parser.add_argument(
        "--select-streams",
        action="store_true",
        help="Interactively choose the title plus video, audio, and subtitle streams before ripping",
    )
    parser.add_argument("--keep-temp", action="store_true", help="Do not delete the temporary rip directory")
    parser.add_argument("--dry-run", action="store_true", help="Probe and print the plan without ripping")
    parser.add_argument(
        "--debug",
        action="store_true",
        default=configured_string(
            file_config,
            env_names=("THINVIDS_DVD_DEBUG",),
            file_name="THINVIDS_DVD_DEBUG",
            fallback="0",
        )
        == "1",
        help="Print debug logging to stderr",
    )
    args = parser.parse_args()

    staged_mode = bool(args.staged_path)
    rename_mode = bool(args.rename_path)
    reuse_bundle_mode = staged_mode or rename_mode
    if staged_mode and rename_mode:
        raise SystemExit("--staged-path and --rename-path cannot be used together.")
    if not reuse_bundle_mode and not command_exists("makemkvcon"):
        raise SystemExit("makemkvcon is required but was not found in PATH.")
    if args.select_streams and (not command_exists("ffmpeg") or not command_exists("ffprobe")):
        raise SystemExit("--select-streams requires both ffmpeg and ffprobe to be installed.")
    if reuse_bundle_mode and args.select_streams:
        raise SystemExit("--select-streams cannot be used with --staged-path or --rename-path.")
    if reuse_bundle_mode and args.title_index is not None:
        raise SystemExit("--title-index cannot be used with --staged-path or --rename-path.")
    if reuse_bundle_mode and not args.title:
        raise SystemExit("--staged-path and --rename-path require an explicit title so the bundle can be renamed.")

    watch_root = Path(args.watch_root).expanduser()
    scratch_root = Path(args.scratch_root).expanduser()
    staging_root = Path(args.staging_root).expanduser()
    staged_manifest_path: Path | None = None
    staged_mkv_path: Path | None = None
    staged_manifest: dict[str, Any] = {}
    rename_manifest_path: Path | None = None
    rename_mkv_path: Path | None = None
    rename_manifest: dict[str, Any] = {}

    if staged_mode:
        progress("Loading staged rip...")
        staged_mkv_path, staged_manifest_path, staged_manifest = load_bundle(args.staged_path)
        source_spec = str(staged_manifest.get("source") or "staged-review")
        parsed = {"disc_info": {}, "titles": []}
        selected_title_raw = staged_manifest.get("selected_title")
        main_title = selected_title_raw if isinstance(selected_title_raw, dict) else {"index": "staged"}
    elif rename_mode:
        progress("Loading existing rip...")
        rename_mkv_path, rename_manifest_path, rename_manifest = load_bundle(args.rename_path)
        source_spec = str(rename_manifest.get("source") or "rename-existing")
        parsed = {"disc_info": {}, "titles": []}
        selected_title_raw = rename_manifest.get("selected_title")
        main_title = selected_title_raw if isinstance(selected_title_raw, dict) else {"index": "existing"}
    else:
        progress("Resolving MakeMKV source...")
        source_spec = resolve_source_spec(args.source, args.device, debug_enabled=args.debug)

        progress("Scanning disc with MakeMKV...")
        probe = stream_makemkv_command(
            ["makemkvcon", "--robot", "--progress=-same", "info", source_spec],
            debug_enabled=args.debug,
            progress_prefix="MakeMKV scan",
        )
        if probe.returncode != 0:
            raise SystemExit(
                "makemkvcon info failed:\n"
                f"stdout:\n{probe.stdout}\n"
                f"stderr:\n{probe.stderr}"
            )
        parsed = parse_makemkv_robot_output(probe.stdout)

        progress("Choosing main title...")
        titles = list(parsed.get("titles", []))
        if not titles:
            diagnostic_path = write_makemkv_probe_diagnostic(
                probe_stdout=probe.stdout,
                probe_stderr=probe.stderr,
                device=args.device,
                source_spec=source_spec,
                scratch_root=scratch_root,
            )
            raise SystemExit(
                "MakeMKV did not return any titles for this disc. "
                f"Saved MakeMKV info output to {diagnostic_path}. "
                "Inspect that log for drive, disc, AACS, or parser clues."
            )
        if args.title_index is not None:
            matching = [title for title in titles if title.get("index") == args.title_index]
            if not matching:
                raise SystemExit(f"Title index {args.title_index} was not found in MakeMKV output.")
            main_title = matching[0]
        else:
            main_title = choose_main_title(parsed, min_seconds=args.min_seconds)
            if args.select_streams:
                main_title = prompt_for_title_selection(
                    parsed=parsed,
                    default_title=main_title,
                )

    runtime_seconds = int(main_title.get("duration_seconds") or 0) or None
    existing_manifest = staged_manifest if staged_mode else rename_manifest if rename_mode else {}
    disc_label = str(args.disc_label or existing_manifest.get("disc_label") or "").strip()
    progress("Resolving movie title...")
    if args.title:
        if args.tmdb_api_key:
            movie_metadata = choose_movie_metadata(
                query=args.title,
                year_hint=None,
                runtime_seconds=runtime_seconds,
                api_key=args.tmdb_api_key,
                debug_enabled=args.debug,
            )
        else:
            movie_metadata = build_basic_movie_metadata(
                args.title,
                source="manual-input",
                query_used=args.title,
            )
            print(
                f'Using manual title without TMDb lookup: {movie_metadata["title"]}'
                + (f' ({movie_metadata["year"]})' if movie_metadata.get("year") else ""),
                file=sys.stderr,
            )
    else:
        movie_metadata = auto_detect_movie_metadata(
            parsed=parsed,
            selected_title=main_title,
            disc_label=disc_label,
            device=args.device,
            api_key=args.tmdb_api_key,
            debug_enabled=args.debug,
            min_score=args.auto_title_min_score,
        )

    resolved_movie_title = str(movie_metadata.get("title") or args.title or AUTO_TITLE_FALLBACK)
    resolved_movie_year = str(movie_metadata.get("year") or "") or None
    resolved_display_name = format_movie_display_name(
        resolved_movie_title,
        resolved_movie_year,
    )
    progress(f"Using output title: {resolved_display_name}")

    planned_path = build_planned_output_path(
        watch_root=watch_root,
        output_subdir=args.output_subdir,
        movie_title=resolved_movie_title,
        movie_year=resolved_movie_year,
        suffix=".mkv",
    )

    plan = {
        "device": args.device,
        "source": source_spec,
        "queue_mode": args.queue_mode,
        "watch_root": str(watch_root),
        "staged_input": str(staged_mkv_path) if staged_mkv_path is not None else "",
        "rename_input": str(rename_mkv_path) if rename_mkv_path is not None else "",
        "output_path": str(planned_path),
        "movie": {
            "title": movie_metadata.get("title"),
            "year": movie_metadata.get("year"),
            "tmdb_id": movie_metadata.get("tmdb_id"),
            "release_date": movie_metadata.get("release_date"),
            "source": movie_metadata.get("source"),
            "query_used": movie_metadata.get("query_used"),
            "needs_manual_review": bool(movie_metadata.get("needs_manual_review")),
            "review_reason": movie_metadata.get("review_reason") or "",
        },
        "selected_title": {
            "index": main_title.get("index"),
            "duration": main_title.get("duration"),
            "duration_seconds": main_title.get("duration_seconds"),
            "size_bytes": main_title.get("size_bytes"),
            "chapters_count": main_title.get("chapters_count"),
            "output_name": main_title.get("output_name"),
            "source_name": main_title.get("source_name"),
        },
    }
    if args.dry_run:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    scratch_root.mkdir(parents=True, exist_ok=True)

    api_result: dict[str, Any] | None = None
    english_subtitles_kept = bool(existing_manifest.get("english_subtitles_kept") or False)
    selected_stream_specs: list[dict[str, Any]] | None = None
    selected_streams: list[dict[str, Any]] | None = None
    temp_dir_path: str | None = None
    final_path: Path | None = None
    manifest_path: Path | None = None
    staged_output_path: Path | None = None
    original_bundle_manifest_path: Path | None = None
    original_finished_path: Path | None = None
    temp_basename = safe_filename(resolved_display_name) + ".mkv"
    if args.select_streams:
        selected_stream_specs = prompt_for_stream_selection(
            title_streams=list(main_title.get("streams") or []),
        )
    try:
        if staged_mode:
            cleanup_temp = False
            finished_path = staged_mkv_path
            if finished_path is None:
                raise RuntimeError("Staged MKV path was not resolved.")
        elif rename_mode:
            cleanup_temp = False
            finished_path = rename_mkv_path
            if finished_path is None:
                raise RuntimeError("Existing MKV path was not resolved.")
            original_bundle_manifest_path = rename_manifest_path
            original_finished_path = finished_path
        else:
            temp_dir_path = tempfile.mkdtemp(prefix="thinvids-dvd-", dir=str(scratch_root))
            cleanup_temp = not args.keep_temp

            temp_path = Path(temp_dir_path)
            raw_dir = temp_path / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)

            progress("Starting rip...")
            rip_result = rip_title(
                source=source_spec,
                title_index=int(main_title["index"]),
                output_dir=raw_dir,
                debug_enabled=args.debug,
            )
            raw_mkv = find_single_mkv(raw_dir, rip_output=rip_result)

            if selected_stream_specs is not None:
                selected_streams = resolve_selected_streams(
                    selected_stream_specs=selected_stream_specs,
                    input_path=raw_mkv,
                    debug_enabled=args.debug,
                )

            remuxed_path = temp_path / temp_basename
            english_subtitles_kept = remux_with_english_subtitles(
                input_path=raw_mkv,
                output_path=remuxed_path,
                title=str(movie_metadata.get("title") or args.title or AUTO_TITLE_FALLBACK),
                debug_enabled=args.debug,
                selected_streams=selected_streams,
            )
            if remuxed_path.exists():
                finished_path = remuxed_path
            else:
                finished_path = temp_path / temp_basename
                shutil.copy2(raw_mkv, finished_path)
                english_subtitles_kept = False

        resolution_label = resolution_label_from_path(finished_path, debug_enabled=args.debug)
        final_path = build_final_path(
            watch_root=watch_root,
            output_subdir=args.output_subdir,
            movie_title=resolved_movie_title,
            movie_year=resolved_movie_year,
            resolution_label=resolution_label,
            suffix=finished_path.suffix or ".mkv",
            current_path=original_finished_path if rename_mode else None,
        )
        final_path.parent.mkdir(parents=True, exist_ok=True)

        manifest = {
            **existing_manifest,
            "created_at_epoch": float(existing_manifest.get("created_at_epoch") or time.time()),
            "requested_title": args.title or "",
            "movie_title": movie_metadata.get("title"),
            "movie_year": movie_metadata.get("year"),
            "tmdb_id": movie_metadata.get("tmdb_id"),
            "movie_source": movie_metadata.get("source"),
            "movie_query_used": movie_metadata.get("query_used"),
            "movie_candidates": movie_metadata.get("candidates") or [],
            "movie_score": movie_metadata.get("score"),
            "needs_manual_review": bool(movie_metadata.get("needs_manual_review")),
            "review_reason": movie_metadata.get("review_reason") or "",
            "disc_label": disc_label,
            "final_filename": final_path.name,
            "device": args.device,
            "source": source_spec,
            "selected_title": plan["selected_title"],
            "english_subtitles_kept": english_subtitles_kept,
            "queue_mode": args.queue_mode,
            "temp_dir": temp_dir_path if args.keep_temp else "",
        }

        if bool(movie_metadata.get("needs_manual_review")) and not staged_mode:
            progress("Staging rip for manual review...")
            staged_output_path, manifest_path = stage_for_manual_review(
                finished_path=finished_path,
                manifest=manifest,
                staging_root=staging_root,
                disc_label=disc_label,
                movie_title=resolved_display_name,
            )
            final_path = None
        else:
            if finished_path.resolve() != final_path.resolve():
                shutil.move(str(finished_path), str(final_path))
            if rename_mode:
                manifest["review_status"] = "corrected"
                manifest["corrected_at_epoch"] = time.time()
                manifest["original_filename"] = str(original_finished_path.name) if original_finished_path is not None else ""
            else:
                manifest["review_status"] = "resolved" if staged_mode else "not_needed"
            manifest["resolved_at_epoch"] = time.time()
            manifest_path = final_path.with_suffix(".json")
            write_manifest(manifest_path, manifest)
            if staged_mode:
                cleanup_staged_bundle(staged_manifest_path, staged_mkv_path)
            elif rename_mode and original_finished_path is not None:
                if original_bundle_manifest_path is not None and original_bundle_manifest_path != manifest_path:
                    cleanup_original_bundle(original_bundle_manifest_path, original_finished_path)

        if final_path is not None and args.queue_mode == "api" and not rename_mode:
            relative_filename = final_path.relative_to(watch_root).as_posix()
            api_result = submit_add_job(args.manager_url, relative_filename)
    except Exception as exc:
        raise SystemExit(str(exc))
    finally:
        if temp_dir_path and 'cleanup_temp' in locals() and cleanup_temp:
            shutil.rmtree(temp_dir_path, ignore_errors=True)

    result = {
        **plan,
        "english_subtitles_kept": english_subtitles_kept,
        "manual_review_required": bool(movie_metadata.get("needs_manual_review")) and final_path is None,
        "final_path": str(final_path) if final_path is not None else "",
        "manifest_path": str(manifest_path) if manifest_path is not None else "",
        "staged_path": str(staged_output_path) if staged_output_path is not None else "",
        "rename_path": str(final_path) if rename_mode and final_path is not None else "",
        "temp_dir": temp_dir_path if args.keep_temp else "",
        "api_result": api_result,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
