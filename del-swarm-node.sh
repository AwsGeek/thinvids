#!/bin/bash

# Usage: ./provision-node.sh <node_prefix> <ssh_username>
if [ $# -ne 1 ]; then
  echo "Usage: $0 <node_name>"
  exit 1
fi

node="$1"
ssh_user="jerry"

SWARM_MANAGER_IP="192.168.0.120"
SWARM_JOIN_TOKEN=$(docker swarm join-token worker -q)

echo "Provisioning node..."

# -------------------------------------------------------------------------------------------------
# PROVISION WORKER NODE
# -------------------------------------------------------------------------------------------------

ssh "$ssh_user@$node" bash -s <<EOF

sudo docker swarm leave
EOF

sudo docker node ls
echo "Finished provisioning swarm on $node"
