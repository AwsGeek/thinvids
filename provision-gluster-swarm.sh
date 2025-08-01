#!/bin/bash

# Usage: ./provision-gluster-swarm.sh <node_prefix> <ssh_username>
if [ $# -ne 2 ]; then
  echo "Usage: $0 <node_prefix> <ssh_username>"
  exit 1
fi

prefix="$1"
ssh_user="$2"

SWARM_MANAGER_IP="192.168.0.120"
SWARM_JOIN_TOKEN=$(docker swarm join-token worker -q)
GLUSTER_MOUNT_SOURCE="$SWARM_MANAGER_IP:/chunks"
GLUSTER_MOUNT_TARGET="/mnt/chunks"
GLUSTER_BRICK="/glusterfs/brick1"
VOLUME_NAME="chunks"

mkdir -p logs

# -------------------------------------------------------------------------------------------------
# PRE-PROVISIONING: SETUP GLUSTERFS MASTER (NO BRICK)
# -------------------------------------------------------------------------------------------------
echo "Setting up GlusterFS master on $SWARM_MANAGER_IP..."

sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd

sudo apt-get update
sudo apt-get -y install glusterfs-server glusterfs-client
sudo systemctl enable --now glusterd

echo "GlusterFS master setup complete."

# -------------------------------------------------------------------------------------------------
# CLEANUP: Remove Down nodes before provisioning
# -------------------------------------------------------------------------------------------------
echo "Cleaning up duplicate Down nodes (pre-provisioning)..."
down_nodes=$(docker node ls --format '{{.ID}} {{.Hostname}} {{.Status}}' | grep "$prefix" | grep "Down" | awk '{print $1}')
for node_id in $down_nodes; do
  echo "Removing Down node ID: $node_id"
  docker node rm "$node_id"
done

# -------------------------------------------------------------------------------------------------
# DISCOVER NODES
# -------------------------------------------------------------------------------------------------
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}" | sort -u))
if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

# Get IPs of discovered nodes
declare -A node_ips
for node in "${nodes[@]}"; do
  ip=$(docker node inspect "$node" --format '{{ .Status.Addr }}')
  node_ips["$node"]=$ip
done

echo "Provisioning ${#node_ips[@]} nodes..."

# -------------------------------------------------------------------------------------------------
# PROVISION EACH WORKER NODE
# -------------------------------------------------------------------------------------------------
for node in "${!node_ips[@]}"; do
  (
    ip=${node_ips[$node]}
    log_file="logs/provision-$node.log"
    echo "Setting up $node at $ip..." | tee "$log_file"

    ssh "$ssh_user@$ip" bash -s <<EOF 2>&1 | tee -a "$log_file"
echo "Provisioning $node at $ip..."

# Install Docker
sudo apt-get update
sudo apt-get -y install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \$(. /etc/os-release && echo "\$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Join Swarm
sudo docker swarm leave || true
sudo docker swarm join --token $SWARM_JOIN_TOKEN $SWARM_MANAGER_IP:2377

# Install GlusterFS
sudo apt-get -y install glusterfs-server glusterfs-client
sudo systemctl enable --now glusterd

# Reset GlusterFS state
sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd
sudo systemctl start glusterd

# Prepare brick
sudo mkdir -p $GLUSTER_BRICK

echo "Provisioning complete on $node"
EOF

    echo "Finished provisioning $node at $ip" | tee -a "$log_file"
  ) &
done

wait
echo "All worker nodes provisioned."

# -------------------------------------------------------------------------------------------------
# GLUSTER PEERING
# -------------------------------------------------------------------------------------------------
echo "Peering GlusterFS nodes..."
ips=(${node_ips[@]})
for ip1 in "${ips[@]}"; do
    sudo gluster peer probe $ip1
#  (
#    for ip2 in "${ips[@]}"; do
#      ssh "$ssh_user@$ip1" "sudo gluster peer probe $SWARM_MANAGER_IP || true"
#      if [ "$ip1" != "$ip2" ]; then
#        echo "Probing $ip2 from $ip1"
#        ssh "$ssh_user@$ip1" "sudo gluster peer probe $ip2 || true"
#      fi
#    done
#  ) &
done
#wait
echo "GlusterFS peering complete."

# -------------------------------------------------------------------------------------------------
# CREATE GLUSTER VOLUME
# -------------------------------------------------------------------------------------------------
echo "Creating GlusterFS volume '$VOLUME_NAME' on master..."

brick_list=""
for ip in "${ips[@]}"; do
  brick_list="$brick_list $ip:$GLUSTER_BRICK"
done

sudo gluster volume stop "$VOLUME_NAME" force
sudo gluster volume delete "$VOLUME_NAME" force
sudo gluster volume create "$VOLUME_NAME" $brick_list force
sudo gluster volume start "$VOLUME_NAME"

echo "GlusterFS volume '$VOLUME_NAME' created and started."

# -------------------------------------------------------------------------------------------------
# MOUNT GLUSTER VOLUME ON MASTER (optional)
# -------------------------------------------------------------------------------------------------
echo "Mounting GlusterFS volume on master..."
sudo umount "$GLUSTER_MOUNT_TARGET"
sudo rm -rf "$GLUSTER_MOUNT_TARGET"
sudo mkdir -p "$GLUSTER_MOUNT_TARGET"
# Clean up existing duplicate GlusterFS entries from /etc/fstab
sudo sed -i '\|'"$GLUSTER_MOUNT_SOURCE"'|d' /etc/fstab
grep -q "$GLUSTER_MOUNT_SOURCE" /etc/fstab || \
  echo "$GLUSTER_MOUNT_SOURCE $GLUSTER_MOUNT_TARGET glusterfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
sudo mount -a

# -------------------------------------------------------------------------------------------------
# MOUNT GLUSTER VOLUME ON WORKERS
# -------------------------------------------------------------------------------------------------
echo "Mounting GlusterFS volume on all worker nodes..."
for node in "${!node_ips[@]}"; do
  ip=${node_ips[$node]}
  echo "Mounting on $node at $ip"
  ssh "$ssh_user@$ip" bash -s <<EOF
# Handle stale or broken GlusterFS mount
sudo umount "$GLUSTER_MOUNT_TARGET"
sudo rm -rf "$GLUSTER_MOUNT_TARGET"
sudo mkdir -p "$GLUSTER_MOUNT_TARGET"
# Clean up existing duplicate GlusterFS entries from /etc/fstab
sudo sed -i '\|'"$GLUSTER_MOUNT_SOURCE"'|d' /etc/fstab
echo "$GLUSTER_MOUNT_SOURCE $GLUSTER_MOUNT_TARGET glusterfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
sudo mount -a
EOF
done

# -------------------------------------------------------------------------------------------------
# CLEANUP DOWN NODES AGAIN
# -------------------------------------------------------------------------------------------------
echo "Cleaning up duplicate Down nodes (post-provisioning)..."
down_nodes=$(docker node ls --format '{{.ID}} {{.Hostname}} {{.Status}}' | grep "$prefix" | grep "Down" | awk '{print $1}')
for node_id in $down_nodes; do
  echo "Removing Down node ID: $node_id"
  docker node rm "$node_id"
done

echo "All GlusterFS volumes mounted."
