#!/bin/bash

# Usage: ./add-node.sh <hostname> <ip_address> <ssh_username>
if [ $# -ne 3 ]; then
  echo "Usage: $0 <hostname> <ip_address> <ssh_username>"
  exit 1
fi

hostname="$1"
ip="$2"
ssh_user="$3"

SWARM_MANAGER_IP="192.168.0.120"
SWARM_JOIN_TOKEN=$(docker swarm join-token worker -q)
GLUSTER_MOUNT_SOURCE="$SWARM_MANAGER_IP:/chunks"
GLUSTER_MOUNT_TARGET="/mnt/chunks"
GLUSTER_BRICK="/glusterfs/brick1"
VOLUME_NAME="chunks"

log_file="logs/add-$hostname.log"
mkdir -p logs

echo "Provisioning $hostname at $ip..." | tee "$log_file"

ssh "$ssh_user@$ip" bash -s <<EOF 2>&1 | tee -a "$log_file"
echo "Provisioning $hostname at $ip..."

# Set system hostname
sudo sed -i '/127.0.1.1/d' /etc/hosts
echo "127.0.1.1 $hostname" | sudo tee -a /etc/hosts
echo "$hostname" | sudo tee /etc/hostname
sudo hostnamectl set-hostname "$hostname"

# Install Docker
sudo apt-get update
sudo apt-get -y install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \$(. /etc/os-release && echo "\$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Join Docker Swarm
sudo docker swarm leave || true
sudo docker swarm join --token $SWARM_JOIN_TOKEN $SWARM_MANAGER_IP:2377

# Install GlusterFS
sudo apt-get -y install glusterfs-server glusterfs-client
sudo systemctl enable --now glusterd

# Reset GlusterFS state
sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd
sudo systemctl start glusterd

# Prepare Gluster brick
sudo mkdir -p $GLUSTER_BRICK

# Mount preparation
sudo umount "$GLUSTER_MOUNT_TARGET" 2>/dev/null || true
sudo rm -rf "$GLUSTER_MOUNT_TARGET"
sudo mkdir -p "$GLUSTER_MOUNT_TARGET"
sudo sed -i '\|'"$GLUSTER_MOUNT_SOURCE"'|d' /etc/fstab
echo "$GLUSTER_MOUNT_SOURCE $GLUSTER_MOUNT_TARGET glusterfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab

echo "Node provisioning complete."
EOF

# Probe from master
echo "Probing $ip from GlusterFS master..."
sudo gluster peer probe "$ip"

# Wait for it to be accepted
echo "Waiting for GlusterFS peer to join..."
for i in {1..12}; do
  if sudo gluster peer status | grep -A1 "$ip" | grep -q "Peer in Cluster"; then
    echo "✅ GlusterFS peer $ip is now part of the cluster."
    break
  fi
  echo "Waiting... ($i/12)"
  sleep 5
done

# Add brick to existing volume
echo "Adding new brick to GlusterFS volume '$VOLUME_NAME'..."
sudo gluster volume add-brick "$VOLUME_NAME" "$ip:$GLUSTER_BRICK" force

# Mount GlusterFS volume on the new node
echo "Mounting GlusterFS volume on new node $hostname..."
ssh "$ssh_user@$ip" "sudo mount -a"

echo "✅ New node $hostname ($ip) successfully added to Docker Swarm and GlusterFS volume '$VOLUME_NAME'."
