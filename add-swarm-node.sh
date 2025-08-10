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

# Install Docker
sudo apt-get update
sudo apt-get -y install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \$(. /etc/os-release && echo "\$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Join Swarm
sudo docker swarm join --token $SWARM_JOIN_TOKEN $SWARM_MANAGER_IP:2377
EOF

sudo docker node ls
echo "Finished provisioning swarm on $node"
