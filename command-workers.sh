#!/bin/bash

# Require node name prefix and SSH username as arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 <node_name_prefix> <command>"
  exit 1
fi

prefix="$1"
ssh_user="jerry"
cmd="$2"

# Discover node names matching the prefix
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}"))

if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

for node in "${nodes[@]}"; do
  (
    echo "Processing $node"

    ssh -t "${ssh_user}@$node" "$cmd"

    echo "Finished $node"
  ) &
done

wait
echo "All nodes processed."
