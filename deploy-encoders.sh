#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <node_name_prefix>"
  exit 1
fi

prefix="$1"
ssh_user="jerry"

nodes=($(docker node ls --format '{{.Hostname}}' | grep "^${prefix}")) || true
if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

for node in "${nodes[@]}"; do
  (
    echo "Processing $node..."
    docker node update --label-add encoder=vaacp "$node"

    # Copy compose file
    scp docker-compose-worker.yml "${ssh_user}@${node}:docker-compose.yml"

    # Copy agent to a user-writable path, then install with sudo
    scp agent.py "${ssh_user}@${node}:agent.py"
    ssh "${ssh_user}@${node}" "
      #sudo apt-get update
      #sudo apt-get -y install python3-psutil python3-redis intel-gpu-tools
      set -e
      sudo install -m 0755 -o root -g root agent.py /usr/local/bin/agent.py
      rm -f agent.py
      # Write/refresh systemd unit
      sudo tee /etc/systemd/system/thinman-agent.service >/dev/null <<'UNIT'
[Unit]
Description=Thinman Metrics Agent
After=network.target

[Service]
ExecStart=/usr/bin/python3 /usr/local/bin/agent.py
Restart=always
RestartSec=2
# Run as root so it can read host metrics and intel_gpu_top
User=root
# Tune these as needed; agent can also default from hostname/env
Environment=REDIS_HOST=swarm1
Environment=REDIS_DB=1
Environment=TTL_SEC=15
Environment=NODE_NAME=${node}

[Install]
WantedBy=multi-user.target
UNIT
      sudo systemctl daemon-reload
      sudo systemctl enable --now thinman-agent
      sudo systemctl restart thinman-agent

    "

    # Deploy worker container
    ssh "${ssh_user}@${node}" "
      set -e
      echo HOSTNAME=${node} > .env
      sudo docker compose pull
      sudo docker compose down
      sudo docker compose up -d worker
    "

    echo "Finished $node"
  ) &
done

wait
echo "All nodes processed."
