#!/usr/bin/env bash
set -Eeuo pipefail

INV=${INV:-./ansible_hosts.ini}
GROUP=${GROUP:-thinman_workers}
UNIT=${UNIT:-thinman-worker}
FOLLOW=${FOLLOW:--f}   # export FOLLOW= to not follow

# Get clean hostnames from the inventory
mapfile -t HOSTS < <(ansible -i "$INV" --list-hosts "$GROUP" | awk 'NR>1{print $1}')
if ((${#HOSTS[@]}==0)); then
  echo "No hosts found for group '$GROUP' in '$INV'"; exit 1
fi

pids=()

cleanup() {
  # Send SIGTERM to each pipeline's process group, then SIGKILL as a backstop
  for pid in "${pids[@]}"; do
    kill -TERM -"${pid}" 2>/dev/null || true
  done
  sleep 0.2
  for pid in "${pids[@]}"; do
    kill -KILL -"${pid}" 2>/dev/null || true
  done
  wait || true
}
trap cleanup INT TERM EXIT

for h in "${HOSTS[@]}"; do
  [[ $h =~ ^[A-Za-z0-9._-]+$ ]] || { echo "Skipping invalid hostname: $h" >&2; continue; }

  # Start each stream in its own session/process-group so we can nuke it cleanly.
  setsid bash -c "
    exec ssh -o StrictHostKeyChecking=accept-new jerry@${h} \
      sudo journalctl -u '${UNIT}' ${FOLLOW} -o cat \
    | stdbuf -oL sed -u 's/^/[${h}] /'
  " &
  pids+=("$!")
done

wait
