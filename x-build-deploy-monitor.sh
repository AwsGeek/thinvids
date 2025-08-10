#!/bin/bash

# Require node name prefix and SSH username as arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 <node_name_prefix> <ssh_username>"
  exit 1
fi

prefix="$1"
ssh_user="$2"
PROM_FILE="./prometheus/prometheus.yml"
BACKUP_FILE="./prometheus/prometheus.yml.bak"

# Discover nodes
nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}"))
if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

echo "Discovered ${#nodes[@]} nodes with prefix '$prefix'"

# Extract IPs
node_targets=()
cadvisor_targets=()
for node in "${nodes[@]}"; do
  ip=$(docker node inspect "$node" --format '{{ .Status.Addr }}')
  node_targets+=("\"${ip}:9100\"")     # node-exporter port
  cadvisor_targets+=("\"${ip}:8080\"") # cadvisor port
done

# Join IPs into target lists
node_targets_string=$(IFS=,; echo "${node_targets[*]}")
cadvisor_targets_string=$(IFS=,; echo "${cadvisor_targets[*]}")

# Backup current config
cp "$PROM_FILE" "$BACKUP_FILE"
echo "Backed up existing $PROM_FILE to $BACKUP_FILE"

# Generate new prometheus.yml with dynamic IPs
cat > "$PROM_FILE" <<EOF
global:
  scrape_interval: 15s

scrape_configs:

  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'node-exporter'
    static_configs:
      - targets: [${node_targets_string}]

  - job_name: 'cadvisor'
    static_configs:
      - targets: [${cadvisor_targets_string}]

  - job_name: 'redis'
    static_configs:
      - targets: ['192.168.0.120:9121']

  - job_name: 'celery'
    static_configs:
      - targets: ['192.168.0.120:8888']
EOF

echo "Updated $PROM_FILE with new node and cadvisor targets."


docker compose build && docker compose push && docker stack deploy --prune --compose-file ./docker-compose.yml transcoder

