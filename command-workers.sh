#!/bin/bash

# Require node name prefix and SSH username as arguments
if [ $# -ne 3 ]; then
  echo "Usage: $0 <node_name_prefix> <ssh_username> <command>"
  exit 1
fi

prefix="$1"
ssh_user="$2"
cmd="$3"

# Discover node names matching the prefix
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}"))

if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

for node in "${nodes[@]}"; do
  (
    ip=$(docker node inspect "$node" --format '{{ .Status.Addr }}')
    echo "Processing $node at $ip..."

    ssh -t "${ssh_user}@$ip" "$cmd"

    echo "Finished $node"
  ) &
done

wait
echo "All nodes processed."
