#!/usr/bin/env python3
"""
Benchmark one worker node for local AI video upscaling.

The harness intentionally stays outside the production thinvids queue. It:
  1. extracts a short video clip into local PNG frames,
  2. runs an ncnn/Vulkan upscaler over those frames,
  3. encodes the result to a sample MP4,
  4. prints timing and throughput metrics.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from fractions import Fraction
from pathlib import Path
from typing import Any


DEFAULT_REALESRGAN_MODEL = "realesrgan-x4plus"
DEFAULT_VAAPI_DEVICE = "/dev/dri/renderD128"


class BenchmarkError(RuntimeError):
    pass


def command_path(name_or_path: str) -> str | None:
    if not name_or_path:
        return None
    expanded = os.path.expanduser(name_or_path)
    if os.path.sep in expanded:
        return expanded if os.path.isfile(expanded) and os.access(expanded, os.X_OK) else None
    return shutil.which(expanded)


def run_command(
    cmd: list[str],
    *,
    dry_run: bool = False,
    timeout: float | None = None,
) -> tuple[subprocess.CompletedProcess[str] | None, float]:
    print("+ " + " ".join(cmd), file=sys.stderr)
    started = time.monotonic()
    if dry_run:
        return None, 0.0
    try:
        completed = subprocess.run(cmd, text=True, capture_output=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise BenchmarkError(f"command timed out after {timeout}s:\n{' '.join(cmd)}") from exc
    elapsed = time.monotonic() - started
    if completed.returncode != 0:
        stderr_tail = (completed.stderr or completed.stdout or "")[-2000:]
        raise BenchmarkError(f"command failed with rc={completed.returncode}:\n{stderr_tail}")
    return completed, elapsed


def ffprobe_video(path: Path) -> dict[str, Any]:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name,width,height,avg_frame_rate,r_frame_rate,field_order",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(path),
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise BenchmarkError(f"ffprobe failed:\n{completed.stderr[-1200:]}")
    payload = json.loads(completed.stdout or "{}")
    stream = (payload.get("streams") or [{}])[0]
    fmt = payload.get("format") or {}
    fps_raw = stream.get("avg_frame_rate") or stream.get("r_frame_rate") or "0/0"
    return {
        "codec": stream.get("codec_name") or "",
        "width": int(stream.get("width") or 0),
        "height": int(stream.get("height") or 0),
        "fps": fps_to_float(fps_raw),
        "fps_raw": fps_raw,
        "field_order": stream.get("field_order") or "",
        "duration": float(fmt.get("duration") or 0.0),
    }


def fps_to_float(value: str) -> float:
    try:
        frac = Fraction(str(value))
    except Exception:
        return 0.0
    if frac.denominator == 0:
        return 0.0
    return float(frac)


def count_files(path: Path, suffix: str = ".png") -> int:
    return sum(1 for p in path.iterdir() if p.is_file() and p.suffix.lower() == suffix)


def sorted_frame_paths(path: Path) -> list[Path]:
    return sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() == ".png")


def choose_scale(input_height: int, target_height: int, requested_scale: int | None) -> int:
    if requested_scale:
        return requested_scale
    if input_height <= 0:
        return 2
    return max(2, min(4, int(math.ceil(float(target_height) / float(input_height)))))


def build_extract_filter(args: argparse.Namespace, probe: dict[str, Any]) -> str | None:
    filters: list[str] = []
    field_order = str(probe.get("field_order") or "").strip().lower()
    should_deinterlace = args.deinterlace == "bwdif" or (
        args.deinterlace == "auto" and field_order not in {"", "unknown", "progressive"}
    )
    if should_deinterlace:
        filters.append("bwdif=mode=send_frame:parity=auto:deint=all")
    if args.pre_vf:
        filters.append(args.pre_vf)
    return ",".join(filters) if filters else None


def default_output_path(input_path: Path, target_height: int) -> Path:
    stem = input_path.stem.replace(" ", "_")
    return Path.cwd() / f"{stem}.upscale-bench.{target_height}p.mp4"


def encode_output(
    frames_pattern: Path,
    output_path: Path,
    fps: float,
    target_height: int,
    args: argparse.Namespace,
) -> float:
    fps_arg = f"{fps:.6f}" if fps > 0 else "24000/1001"
    if args.encode_backend == "vaapi":
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-framerate",
            fps_arg,
            "-i",
            str(frames_pattern),
            "-vaapi_device",
            args.vaapi_device,
            "-vf",
            f"scale=-2:{target_height}:flags=lanczos,format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-qp",
            str(args.vaapi_qp),
            "-movflags",
            "+faststart",
            str(output_path),
        ]
    else:
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-y",
            "-framerate",
            fps_arg,
            "-i",
            str(frames_pattern),
            "-vf",
            f"scale=-2:{target_height}:flags=lanczos,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            args.x264_preset,
            "-crf",
            str(args.x264_crf),
            "-movflags",
            "+faststart",
            str(output_path),
        ]
    _, elapsed = run_command(cmd, dry_run=args.dry_run)
    return elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark local AI upscaling throughput on one thinvids worker node."
    )
    parser.add_argument("input", type=Path, help="Input video file to sample.")
    parser.add_argument("--start", default="00:10:00", help="Clip start time for ffmpeg -ss.")
    parser.add_argument("--seconds", type=float, default=30.0, help="Clip duration to benchmark.")
    parser.add_argument("--target-height", type=int, default=720, choices=(720, 1080), help="Final sample height.")
    parser.add_argument("--scale", type=int, choices=(2, 3, 4), help="AI upscale factor. Defaults to target/input height.")
    parser.add_argument(
        "--engine-bin",
        default="realesrgan-ncnn-vulkan",
        help="Upscaler binary name or path, for example realesrgan-ncnn-vulkan.",
    )
    parser.add_argument("--model", default=DEFAULT_REALESRGAN_MODEL, help="Upscaler model name passed with -n.")
    parser.add_argument(
        "--model-path",
        type=Path,
        help="Model directory passed with -m. For realesrgan-ncnn-vulkan, use a path relative to the binary directory, such as models.",
    )
    parser.add_argument("--tile", type=int, default=200, help="Tile size passed to the ncnn upscaler with -t.")
    parser.add_argument("--jobs", default="1:2:2", help="ncnn load:proc:save jobs passed with -j.")
    parser.add_argument("--gpu-id", default="", help="GPU id passed with -g, for example 0.")
    parser.add_argument(
        "--upscale-mode",
        choices=("directory", "single"),
        default="directory",
        help="Run the upscaler once on the frame directory, or once per frame.",
    )
    parser.add_argument(
        "--frame-timeout",
        type=float,
        default=120.0,
        help="Per-frame timeout in seconds for --upscale-mode single.",
    )
    parser.add_argument("--tta", action="store_true", help="Enable test-time augmentation if supported by the engine.")
    parser.add_argument("--deinterlace", choices=("auto", "none", "bwdif"), default="auto")
    parser.add_argument("--pre-vf", default="", help="Extra ffmpeg filter(s) applied while extracting frames.")
    parser.add_argument("--encode-backend", choices=("software", "vaapi"), default="software")
    parser.add_argument("--vaapi-device", default=DEFAULT_VAAPI_DEVICE)
    parser.add_argument("--vaapi-qp", type=int, default=23)
    parser.add_argument("--x264-crf", type=int, default=18)
    parser.add_argument("--x264-preset", default="veryfast")
    parser.add_argument("--work-dir", type=Path, help="Directory for temporary frames. Created if missing.")
    parser.add_argument("--keep-work", action="store_true", help="Keep extracted/upscaled frames after completion.")
    parser.add_argument("--output", type=Path, help="Output sample MP4 path.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing output file.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input.expanduser().resolve()
    if not input_path.is_file():
        raise BenchmarkError(f"input file not found: {input_path}")

    for tool in ("ffmpeg", "ffprobe"):
        if not command_path(tool):
            raise BenchmarkError(f"required command not found in PATH: {tool}")

    engine = command_path(args.engine_bin)
    if not engine:
        raise BenchmarkError(
            f"upscale engine not found or not executable: {args.engine_bin}\n"
            "Install/copy an ncnn Vulkan upscaler on the worker, or pass --engine-bin /path/to/binary."
        )
    engine_path = Path(engine).resolve()
    model_path = Path(os.path.expanduser(str(args.model_path))) if args.model_path else None

    probe = ffprobe_video(input_path)
    if probe["height"] <= 0 or probe["fps"] <= 0:
        raise BenchmarkError(f"could not read usable video metadata from {input_path}")

    output_path = (args.output or default_output_path(input_path, args.target_height)).expanduser().resolve()
    if output_path.exists() and not args.overwrite:
        raise BenchmarkError(f"output already exists; pass --overwrite to replace it: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    scale = choose_scale(probe["height"], args.target_height, args.scale)
    work_root = args.work_dir.expanduser().resolve() if args.work_dir else Path(
        tempfile.mkdtemp(prefix="thinvids-upscale-bench-")
    )
    frames_dir = work_root / "frames"
    upscaled_dir = work_root / "upscaled"
    for path in (frames_dir, upscaled_dir):
        if path.exists() and any(path.iterdir()) and not args.dry_run:
            raise BenchmarkError(f"work subdirectory is not empty; choose a fresh --work-dir: {path}")
    frames_dir.mkdir(parents=True, exist_ok=True)
    upscaled_dir.mkdir(parents=True, exist_ok=True)

    metrics: dict[str, Any] = {
        "input": str(input_path),
        "output": str(output_path),
        "work_dir": str(work_root),
        "engine": str(engine_path),
        "model": args.model,
        "model_path": str(model_path) if model_path else "",
        "source": probe,
        "clip_start": args.start,
        "clip_seconds": args.seconds,
        "target_height": args.target_height,
        "upscale_scale": scale,
        "tile": args.tile,
        "jobs": args.jobs,
        "gpu_id": args.gpu_id,
        "upscale_mode": args.upscale_mode,
        "encode_backend": args.encode_backend,
    }

    total_started = time.monotonic()
    extract_filter = build_extract_filter(args, probe)
    frame_pattern = frames_dir / "frame_%08d.png"
    extract_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-ss",
        str(args.start),
        "-t",
        f"{args.seconds:.3f}",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-an",
        "-sn",
        "-dn",
    ]
    if extract_filter:
        extract_cmd.extend(["-vf", extract_filter])
    extract_cmd.extend(["-fps_mode", "passthrough", str(frame_pattern)])

    _, extract_elapsed = run_command(extract_cmd, dry_run=args.dry_run)
    extracted_frames = 0 if args.dry_run else count_files(frames_dir)
    if not args.dry_run and extracted_frames <= 0:
        raise BenchmarkError("ffmpeg did not extract any frames")

    upscale_base_cmd = [
        str(engine_path),
        "-n",
        args.model,
        "-s",
        str(scale),
        "-t",
        str(args.tile),
        "-j",
        args.jobs,
        "-f",
        "png",
    ]
    if args.gpu_id:
        upscale_base_cmd.extend(["-g", str(args.gpu_id)])
    if model_path:
        upscale_base_cmd.extend(["-m", str(model_path)])
    if args.tta:
        upscale_base_cmd.append("-x")

    if args.upscale_mode == "single":
        upscale_started = time.monotonic()
        for frame_path in sorted_frame_paths(frames_dir):
            out_path = upscaled_dir / frame_path.name
            cmd = [
                *upscale_base_cmd,
                "-i",
                str(frame_path),
                "-o",
                str(out_path),
            ]
            run_command(cmd, dry_run=args.dry_run, timeout=args.frame_timeout)
        upscale_elapsed = time.monotonic() - upscale_started
    else:
        upscale_cmd = [
            *upscale_base_cmd,
            "-i",
            str(frames_dir),
            "-o",
            str(upscaled_dir),
        ]
        _, upscale_elapsed = run_command(upscale_cmd, dry_run=args.dry_run)
    upscaled_frames = 0 if args.dry_run else count_files(upscaled_dir)
    if not args.dry_run and upscaled_frames <= 0:
        raise BenchmarkError("upscaler did not produce any frames")

    encode_elapsed = encode_output(upscaled_dir / "frame_%08d.png", output_path, probe["fps"], args.target_height, args)
    total_elapsed = time.monotonic() - total_started

    metrics.update(
        {
            "extract_filter": extract_filter or "",
            "frames_extracted": extracted_frames,
            "frames_upscaled": upscaled_frames,
            "extract_elapsed_s": round(extract_elapsed, 3),
            "upscale_elapsed_s": round(upscale_elapsed, 3),
            "encode_elapsed_s": round(encode_elapsed, 3),
            "total_elapsed_s": round(total_elapsed, 3),
            "upscale_fps": round((upscaled_frames / upscale_elapsed), 3) if upscale_elapsed > 0 else 0,
            "total_fps": round((upscaled_frames / total_elapsed), 3) if total_elapsed > 0 else 0,
            "output_size_bytes": output_path.stat().st_size if output_path.exists() else 0,
        }
    )

    print(json.dumps(metrics, indent=2, sort_keys=True))

    if not args.keep_work and not args.work_dir and not args.dry_run:
        shutil.rmtree(work_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchmarkError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
