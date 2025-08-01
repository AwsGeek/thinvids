#!/bin/bash

# Require node name prefix and SSH username as arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 <node_name_prefix> <ssh_username>"
  exit 1
fi

prefix="$1"
ssh_user="$2"

# Discover node names matching the prefix
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}"))

if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

declare -A node_ips

# Phase 1: Setup Gluster on all nodes in parallel and collect IPs
for node in "${nodes[@]}"; do
  (
    ip=$(docker node inspect "$node" --format '{{ .Status.Addr }}')
    node_ips["$node"]="$ip"
    echo "Processing $node at $ip..."

    ssh -t "${ssh_user}@${ip}" 'sudo apt-get -y install glusterfs-server glusterfs-client && \
                                sudo systemctl enable --now glusterd && \
                                sudo mkdir -p "/glusterfs/brick1" && \
                                sudo mkdir -p "/mnt/chunks" && \
                                echo "192.168.0.120:/chunks /mnt/chunks glusterfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab && \
                                sudo mount -a'

    echo "$node $ip" >> /tmp/gluster_nodes.tmp
    echo "Finished $node"
  ) &
done

wait
echo "Initial provisioning complete."

# Read node IPs from temp file into array
mapfile -t ip_lines < /tmp/gluster_nodes.tmp
rm /tmp/gluster_nodes.tmp

# Extract just the IPs
ips=()
for line in "${ip_lines[@]}"; do
  IFS=' ' read -r node ip <<< "$line"
  ips+=("$ip")
done

# Phase 2: Full mesh peer probe from each node to every other node
for src_ip in "${ips[@]}"; do
  for target_ip in "${ips[@]}"; do
    if [ "$src_ip" != "$target_ip" ]; then
      echo "[$src_ip] -> Probing $target_ip"
      ssh -o LogLevel=ERROR -t "${ssh_user}@${src_ip}" "sudo gluster peer probe ${target_ip}"
    fi
  done
done

echo "Gluster peer probing complete."
