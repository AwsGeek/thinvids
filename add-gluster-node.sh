#!/bin/bash

# Usage: ./provision-gluster-swarm.sh <node_prefix> <ssh_username>
if [ $# -ne 1 ]; then
  echo "Usage: $0 <node_name>"
  exit 1
fi

node="$1"
ssh_user="jerry"

GLUSTER_MANAGER_IP="192.168.0.120"
GLUSTER_MOUNT_SOURCE="$GLUSTER_MANAGER_IP:/chunks"
GLUSTER_MOUNT_TARGET="/mnt/chunks"
GLUSTER_BRICK="/glusterfs/brick1"
VOLUME_NAME="chunks"

brick_path="${node}:${GLUSTER_BRICK}"

echo "Checking if brick ${brick_path} exists in volume ${VOLUME_NAME}..."

# Get current list of bricks in the volume
bricks=$(sudo gluster volume info "$VOLUME_NAME" | grep -E 'Brick[0-9]+:' | awk '{print $2}')

# Check if the brick exists
if echo "$bricks" | grep -q "^${brick_path}$"; then
  echo "Brick found. Starting remove-brick operation for $brick_path"

  # Start remove-brick with data migration
  sudo gluster volume remove-brick "$VOLUME_NAME" "$brick_path" start
  if [ $? -ne 0 ]; then
    echo "Failed to start remove-brick operation."
    exit 1
  fi

  echo "Waiting for remove-brick to complete data migration..."
  while true; do
    output=$(sudo gluster volume remove-brick "$VOLUME_NAME" "$brick_path" status)
    echo "$output"

    if echo "$output" | grep -q "completed"; then
      echo "Remove-brick data migration completed."
      break
    elif echo "$output" | grep -q "failed"; then
      echo "Remove-brick operation failed."
      exit 1
    else
      echo "Still migrating... waiting 15 seconds"
      sleep 15
    fi
  done

  echo "Committing brick removal..."
  sudo gluster volume remove-brick "$VOLUME_NAME" "$brick_path" commit
  if [ $? -eq 0 ]; then
    echo "Brick $brick_path successfully removed and committed."
  else
    echo "Failed to commit brick removal."
    exit 1
  fi
fi

sudo gluster peer detach "$node"

echo "Setting up $node..."
ssh "$ssh_user@$node" bash -s <<EOF
echo "Provisioning $node..."

# Install GlusterFS
sudo apt-get -y install glusterfs-server glusterfs-client
sudo systemctl enable --now glusterd

# Reset GlusterFS state
sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd
sudo systemctl start glusterd

# Prepare brick
sudo mkdir -p $GLUSTER_BRICK

# Mount preparation
sudo umount "$GLUSTER_MOUNT_TARGET" 2>/dev/null || true
sudo rm -rf "$GLUSTER_MOUNT_TARGET"
sudo mkdir -p "$GLUSTER_MOUNT_TARGET"
sudo sed -i '\|'"$GLUSTER_MOUNT_SOURCE"'|d' /etc/fstab
echo "$GLUSTER_MOUNT_SOURCE $GLUSTER_MOUNT_TARGET glusterfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab

# sudo gluster peer probe $GLUSTER_MANAGER_IP

echo "Provisioning complete on $node"
EOF

# -------------------------------------------------------------------------------------------------
# GLUSTER PEERING
# -------------------------------------------------------------------------------------------------
sudo gluster peer probe $node

# Wait for it to be accepted
echo "Waiting for GlusterFS peer to join..."
for i in {1..12}; do
  if sudo gluster peer status | grep -A2 "$node" | grep -q "Peer in Cluster"; then
    echo "GlusterFS peer $node is now part of the cluster."
    break
  fi
  echo "Waiting... ($i/12)"
  sleep 5
done

# Add brick to existing volume
echo "Adding new brick to GlusterFS volume '$VOLUME_NAME'..."
sudo gluster volume add-brick "$VOLUME_NAME" "$node:$GLUSTER_BRICK" force

# Mount GlusterFS volume on the new node
echo "Mounting GlusterFS volume on new node $hostname..."
ssh "$ssh_user@$node" "sudo mount -a; sudo touch $GLUSTER_MOUNT_TARGET/$node.txt"

sudo gluster volume info 
sudo ls $GLUSTER_MOUNT_TARGET
echo "All GlusterFS volumes mounted."
