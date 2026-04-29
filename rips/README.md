# DVD Rip Helpers

`dvd_title_lookup.py` is a host-side helper for identifying a DVD before you rip
the main feature. It is meant to be copied to the Ubuntu machine that has the
DVD drive.

`dvd_rip_queue.py` is the follow-on tool for the workflow you settled on:
pass the movie name explicitly, rip the main title with MakeMKV, keep English
subtitle streams when possible, and hand the result to `thinvids`.

`auto_dvd/` contains a thinvids-native automatic insert workflow:

1. a `udev` rule that watches for DVD inserts on `/dev/sr*`
2. a tiny launcher script that reads `udev` properties and calls `dvd_rip_queue.py`
3. a `systemd` template unit so the actual rip work runs outside of `udev`

## What it does

1. Looks for a mounted `VIDEO_TS` directory.
2. Computes a repeatable structural fingerprint from the disc.
3. Checks a local JSON cache for a known title.
4. If the disc is unknown, optionally searches TMDb using the disc label and any
   runtime information it can discover.
5. Saves the confirmed match so the same disc becomes automatic later.

This is intentionally honest about DVDs: there is usually no reliable movie
title stored on-disc, and there is not a widely-used public lookup service that
accepts a DVD fingerprint directly. The fingerprint is mainly for your own cache.
TMDb is used only to seed the first match.

## Suggested host packages

For the best results on Ubuntu 20.04:

```bash
sudo apt install python3 genisoimage lsdvd dvdbackup ffmpeg
```

Optional:

- `makemkvcon` if you use MakeMKV for ripping
- a TMDb API key in `TMDB_API_KEY`

## Basic usage

Interactive lookup:

```bash
python3 dvd_title_lookup.py --device /dev/sr0
```

Print just the title for scripting:

```bash
python3 dvd_title_lookup.py --device /dev/sr0 --print-title-only
```

Point it at a known mount path:

```bash
python3 dvd_title_lookup.py --device /dev/sr0 --mount /media/jerry/MOVIE_NAME
```

Write a probe report for debugging:

```bash
python3 dvd_title_lookup.py --device /dev/sr0 --dump-probe alien.probe.json --debug
```

Non-interactive mode will only auto-save a TMDb match when the score is high:

```bash
python3 dvd_title_lookup.py --device /dev/sr0 --non-interactive
```

## Cache format

The default cache file is `dvd_title_cache.json` next to the script. It maps:

```json
{
  "disc_fingerprint_here": {
    "disc_label": "ALIEN_WS",
    "fingerprint": "disc_fingerprint_here",
    "saved_at": "2026-04-18T03:00:00+00:00",
    "source": "tmdb-confirmed",
    "title": "Alien",
    "tmdb_id": 348,
    "tmdb_title": "Alien",
    "year": "1979"
  }
}
```

## Suggested next step

Once this script is working on the DVD host, the next useful addition is a small
wrapper that:

1. runs `dvd_title_lookup.py`
2. picks the main title to rip
3. calls `makemkvcon` or `dvdbackup`
4. renames the output with the chosen title
5. moves it into the `thinvids` watch path

## Rip And Queue

`dvd_rip_queue.py` is meant to run on the same host as the `thinvids` media
stack. It uses `makemkvcon --robot info` to choose the main feature, then rips
that title and moves the final `.mkv` into `WATCH_ROOT`.

Basic usage:

```bash
python3 dvd_rip_queue.py "Alien"
```

Automatic title detection is also supported. If you omit the title, the script
will:

1. probe MakeMKV for the disc label and main-feature runtime
2. search TMDb using cleaned disc hints
3. auto-pick the best match when running non-interactively if it meets the
   configured score threshold
4. otherwise fall back to a cleaned disc-label title so unattended ripping can
   continue

Example:

```bash
python3 dvd_rip_queue.py --device /dev/sr0 --source auto --tmdb-api-key "$TMDB_API_KEY"
```

Preview the selected title without ripping:

```bash
python3 dvd_rip_queue.py "Alien" --dry-run --debug
```

Queue by calling the manager API directly instead of relying on the watcher:

```bash
python3 dvd_rip_queue.py "Alien" --queue-mode api --manager-url http://localhost:5005/add_job
```

Important behavior:

1. The output goes under `WATCH_ROOT/movies/` by default.
2. If `ffmpeg` is available, the final MKV keeps video, audio, and English
   subtitle streams only.
3. If `ffmpeg` is not available, the raw MakeMKV output is moved into place.
4. `--queue-mode api` asks the manager to mark the file as watcher-processed so
   the watcher does not submit it a second time.
5. A JSON sidecar manifest is written next to the MKV.

Current thinvids caveat:

The existing `thinvids` worker pipeline appears video/audio focused. This rip
script can preserve English subtitle streams in the source MKV it queues, but
the current transcode pipeline may not carry subtitle streams into the final
encoded output.

## Automatic DVD Insert Workflow

The `auto_dvd/` folder contains:

- `99-thinvids-dvd-auto.rules`
- `thinvids-dvd-auto@.service`
- `thinvids-dvd-auto.sh`
- `thinvids-dvd-auto.env.example`

This setup is designed for a host system, not for `udev` running inside a
container.

### Install

1. Copy the launcher script onto the DVD host:

```bash
sudo install -m 0755 auto_dvd/thinvids-dvd-auto.sh /usr/local/bin/thinvids-dvd-auto.sh
```

2. Copy the systemd service template:

```bash
sudo install -m 0644 auto_dvd/thinvids-dvd-auto@.service /etc/systemd/system/thinvids-dvd-auto@.service
```

3. Copy the `udev` rule:

```bash
sudo install -m 0644 auto_dvd/99-thinvids-dvd-auto.rules /etc/udev/rules.d/99-thinvids-dvd-auto.rules
```

4. Create the environment file from the example:

```bash
sudo cp auto_dvd/thinvids-dvd-auto.env.example /etc/default/thinvids-dvd-auto
```

### Configure

Edit `/etc/default/thinvids-dvd-auto` and set at least:

- `THINVIDS_DVD_RIP_SCRIPT`
- `THINVIDS_DVD_WATCH_ROOT`
- `THINVIDS_DVD_SCRATCH_ROOT`
- `THINVIDS_DVD_TMDB_API_KEY`
- `THINVIDS_DVD_QUEUE_MODE`

Recommended values for your current setup are approximately:

```bash
THINVIDS_DVD_RIP_SCRIPT=/home/jerry/apps/thinvids/rips/dvd_rip_queue.py
THINVIDS_DVD_WATCH_ROOT=/home/jerry/media/data/media
THINVIDS_DVD_OUTPUT_SUBDIR=movies/dvdrips
THINVIDS_DVD_SCRATCH_ROOT=/home/jerry/thinvids-dvd-tmp
THINVIDS_DVD_QUEUE_MODE=watch
THINVIDS_DVD_TMDB_API_KEY=...
```

If you prefer the rip script to submit directly to thinvids instead of relying
on the watcher, set:

```bash
THINVIDS_DVD_QUEUE_MODE=api
THINVIDS_DVD_MANAGER_URL=http://localhost:5005/add_job
```

### Enable

Reload systemd and `udev`:

```bash
sudo systemctl daemon-reload
sudo udevadm control --reload
sudo udevadm trigger --subsystem-match=block
```

### Test Manually

Before relying on disc-insert events, run the launcher directly:

```bash
sudo /usr/local/bin/thinvids-dvd-auto.sh sr0
```

Or run the systemd unit directly:

```bash
sudo systemctl start thinvids-dvd-auto@sr0.service
sudo journalctl -u thinvids-dvd-auto@sr0.service -n 200 --no-pager
```

### Run

Once the rule is installed and reloaded, insert a DVD into `/dev/sr0` (or
another optical drive). `udev` will start the corresponding
`thinvids-dvd-auto@srX.service`, which will:

1. wait briefly for the drive to settle
2. read `ID_FS_LABEL` from `udev`
3. call `dvd_rip_queue.py` with `--source auto`
4. let `dvd_rip_queue.py` resolve the MakeMKV disc index, identify the title,
   rip the main feature, and queue the result into thinvids
