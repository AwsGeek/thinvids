#!/bin/bash

prefix="$1"
ssh_user="jerry"

GLUSTER_MANAGER_IP="192.168.0.120"
GLUSTER_MOUNT_SOURCE="$GLUSTER_MANAGER_IP:/chunks"
GLUSTER_MOUNT_TARGET="/mnt/chunks"
GLUSTER_BRICK="/glusterfs/brick1"
VOLUME_NAME="chunks"

mkdir -p logs

# -------------------------------------------------------------------------------------------------
# PRE-PROVISIONING: SETUP GLUSTERFS MASTER (NO BRICK)
# -------------------------------------------------------------------------------------------------
echo "Setting up GlusterFS master on $GLUSTER_MANAGER_IP..."

sudo systemctl stop glusterd
sudo rm -rf /var/lib/glusterd

sudo apt-get update
sudo apt-get -y install glusterfs-server glusterfs-client
sudo systemctl enable --now glusterd

# Prepare brick
sudo mkdir -p $GLUSTER_BRICK

# -------------------------------------------------------------------------------------------------
# CREATE GLUSTER VOLUME
# -------------------------------------------------------------------------------------------------
echo "Creating GlusterFS volume '$VOLUME_NAME' on master..."

brick_list="$GLUSTER_MANAGER_IP:$GLUSTER_BRICK"

sudo gluster volume stop "$VOLUME_NAME" force
sudo gluster volume delete "$VOLUME_NAME"
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

echo "All GlusterFS volumes mounted."
