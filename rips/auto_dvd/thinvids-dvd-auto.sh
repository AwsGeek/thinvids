#!/usr/bin/env bash
set -euo pipefail

DEVICE_BASENAME="${1:?usage: thinvids-dvd-auto.sh sr0}"
DEVICE_PATH="/dev/${DEVICE_BASENAME}"
ENV_FILE="${THINVIDS_DVD_ENV_FILE:-/etc/default/thinvids-dvd-auto}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

log() {
  printf '[thinvids-dvd-auto] %s\n' "$*" >&2
}

read_udev_property() {
  local key="$1"
  udevadm info --query=property --name "${DEVICE_PATH}" 2>/dev/null \
    | awk -F= -v search_key="${key}" '$1 == search_key { print substr($0, index($0, "=") + 1); exit }'
}

LOCK_DIR="${THINVIDS_DVD_LOCK_DIR:-/run/lock}"
mkdir -p "${LOCK_DIR}"
LOCK_FILE="${LOCK_DIR}/thinvids-dvd-${DEVICE_BASENAME}.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  log "Another rip for ${DEVICE_PATH} is already running; exiting."
  exit 0
fi

if [[ ! -b "${DEVICE_PATH}" ]]; then
  log "Device ${DEVICE_PATH} does not exist."
  exit 1
fi

udevadm settle || true
sleep "${THINVIDS_DVD_START_DELAY_SEC:-8}"

MEDIA_DVD="$(read_udev_property ID_CDROM_MEDIA_DVD || true)"
MEDIA_BD="$(read_udev_property ID_CDROM_MEDIA_BD || true)"
MEDIA_STATE="$(read_udev_property ID_CDROM_MEDIA_STATE || true)"
DISC_LABEL="$(read_udev_property ID_FS_LABEL || true)"

MEDIA_KIND=""
if [[ "${MEDIA_DVD}" == "1" ]]; then
  MEDIA_KIND="DVD"
elif [[ "${MEDIA_BD}" == "1" ]]; then
  MEDIA_KIND="Blu-ray"
fi

if [[ -z "${MEDIA_KIND}" ]]; then
  log "Skipping ${DEVICE_PATH}; inserted media is not DVD/Blu-ray video."
  exit 0
fi

if [[ "${MEDIA_STATE}" == "blank" ]]; then
  log "Skipping ${DEVICE_PATH}; media state is blank."
  exit 0
fi

RIP_SCRIPT="${THINVIDS_DVD_RIP_SCRIPT:-/home/jerry/apps/thinvids/rips/dvd_rip_queue.py}"
if [[ ! -f "${RIP_SCRIPT}" ]]; then
  log "Rip script not found at ${RIP_SCRIPT}"
  exit 1
fi

PYTHON_BIN="${THINVIDS_DVD_PYTHON:-python3}"
QUEUE_MODE="${THINVIDS_DVD_QUEUE_MODE:-watch}"
OUTPUT_SUBDIR="${THINVIDS_DVD_OUTPUT_SUBDIR:-movies}"
MIN_SECONDS="${THINVIDS_DVD_MIN_SECONDS:-2400}"
AUTO_TITLE_MIN_SCORE="${THINVIDS_DVD_AUTO_TITLE_MIN_SCORE:-60}"

cmd=(
  "${PYTHON_BIN}" "${RIP_SCRIPT}"
  --device "${DEVICE_PATH}"
  --source auto
  --queue-mode "${QUEUE_MODE}"
  --output-dir "${OUTPUT_SUBDIR}"
  --min-seconds "${MIN_SECONDS}"
  --auto-title-min-score "${AUTO_TITLE_MIN_SCORE}"
)

if [[ -n "${DISC_LABEL}" ]]; then
  cmd+=(--disc-label "${DISC_LABEL}")
fi
if [[ -n "${THINVIDS_DVD_WATCH_ROOT:-}" ]]; then
  cmd+=(--watch-root "${THINVIDS_DVD_WATCH_ROOT}")
fi
if [[ -n "${THINVIDS_DVD_MANAGER_URL:-}" ]]; then
  cmd+=(--manager-url "${THINVIDS_DVD_MANAGER_URL}")
fi
if [[ -n "${THINVIDS_DVD_SCRATCH_ROOT:-}" ]]; then
  cmd+=(--scratch-root "${THINVIDS_DVD_SCRATCH_ROOT}")
fi
if [[ -n "${THINVIDS_DVD_TMDB_API_KEY:-${TMDB_API_KEY:-}}" ]]; then
  cmd+=(--tmdb-api-key "${THINVIDS_DVD_TMDB_API_KEY:-${TMDB_API_KEY:-}}")
fi
if [[ "${THINVIDS_DVD_DEBUG:-0}" == "1" ]]; then
  cmd+=(--debug)
fi

log "Starting automatic ${MEDIA_KIND} rip for ${DEVICE_PATH}${DISC_LABEL:+ (label=${DISC_LABEL})} using ${RIP_SCRIPT}"
"${cmd[@]}"
