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

for h in "${HOSTS[@]}"; do
  [[ $h =~ ^[A-Za-z0-9._-]+$ ]] || { echo "Skipping invalid hostname: $h" >&2; continue; }
  echo "$h"
  ssh jerry@${h} "sudo systemctl suspend"
done

