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
      sudo gluster volume remove-brick "$VOLUME_NAME" "$brick_path" force
      echo "Remove-brick operation failed. Forcing"
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
    sudo gluster volume remove-brick "$VOLUME_NAME" "$brick_path" force
    echo "Failed to commit brick removal. Forcing removal"
    exit 1
  fi
fi

sudo gluster peer detach "$node"

ssh "$ssh_user@$node" bash -s <<EOF
echo "Decommissioning $node..."

# Reset GlusterFS state
sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd

# Prepare brick
sudo rm -rf $GLUSTER_BRICK

# Mount preparation
sudo umount "$GLUSTER_MOUNT_TARGET" 2>/dev/null || true
sudo rm -rf "$GLUSTER_MOUNT_TARGET"
sudo sed -i '\|'"$GLUSTER_MOUNT_SOURCE"'|d' /etc/fstab
EOF

sudo gluster volume info 
sudo ls $GLUSTER_MOUNT_TARGET
