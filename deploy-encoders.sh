#!/bin/bash

# Require node name prefix and SSH username as arguments
if [ $# -ne 1 ]; then
  echo "Usage: $0 <node_name_prefix>"
  exit 1
fi

prefix="$1"
ssh_user="jerry"

# Discover node names matching the prefix
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}"))

if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

for node in "${nodes[@]}"; do
  (
    echo "Processing $node..."
    docker node update --label-add encoder=vaacp $node

    ssh -t "${ssh_user}@${node}" "touch .env"
    scp docker-compose-worker.yml "${ssh_user}@${node}":docker-compose.yml
    ssh -t "${ssh_user}@${node}" "sudo docker compose pull && sudo docker compose down && echo HOSTNAME=${node} > .env && sudo docker compose up -d worker"

    echo "Finished $node"
  ) &
done

wait
echo "All nodes processed."
