#!/bin/bash

# Usage: ./get_mac_addresses.sh [prefix]
# If a prefix is provided, only hostnames starting with that prefix are processed

PREFIX="$1"

# Ensure you're running this on the Swarm manager
if ! docker info | grep -q 'Swarm: active'; then
  echo "This script must be run on a Docker Swarm manager."
  exit 1
fi

# Get list of node IDs
node_ids=$(docker node ls -q)

# Output header
echo "mac address,ip address,hostname,infinite"

# Loop through nodes
for node_id in $node_ids; do
  # Get hostname and IP address
  node_info=$(docker node inspect "$node_id" --format '{{ .Description.Hostname }},{{ .Status.Addr }}')
  hostname=$(echo "$node_info" | cut -d',' -f1)
  ip=$(echo "$node_info" | cut -d',' -f2)

  # Skip if hostname does not match prefix (if given)
  if [[ -n "$PREFIX" && "$hostname" != "$PREFIX"* ]]; then
    continue
  fi

  # Trigger ARP resolution
  ping -c 1 -W 1 "$ip" > /dev/null 2>&1

  # Get MAC address using ip neigh
  mac=$(ip neigh show "$ip" | awk '{print $5}')

  # If MAC address is empty, mark as unknown
  if [ -z "$mac" ]; then
    mac="UNKNOWN"
  fi

  echo "$mac,$ip,$hostname,infinite"
done
