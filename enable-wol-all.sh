#!/usr/bin/env bash
set -euo pipefail

# Usage: ./enable-wol-all.sh <node_name_prefix>
if [ $# -ne 1 ]; then
  echo "Usage: $0 <node_name_prefix>"
  exit 1
fi

prefix="$1"
ssh_user="${SSH_USER:-jerry}"
IFACE="${IFACE:-enp1s0}"

# Discover nodes by Swarm hostname prefix
mapfile -t nodes < <(docker node ls --format '{{.Hostname}}' | grep -E "^${prefix}" || true)
if [ ${#nodes[@]} -eq 0 ]; then
  echo "No nodes found matching prefix '$prefix'"
  exit 1
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new)

for node in "${nodes[@]}"; do
  (
    echo "==== ${node}: configuring WoL on ${IFACE} ===="

    if ! ssh "${SSH_OPTS[@]}" "${ssh_user}@${node}" 'echo "[remote] SSH OK: $(hostname) uid=$(id -u)"'; then
      echo "!! ${node}: SSH failed (host down? no key auth?). Skipping."
      exit 0
    fi

    # Do all work on the remote host
    ssh "${SSH_OPTS[@]}" "${ssh_user}@${node}" bash -s -- "$IFACE" <<'REMOTE'
set -euo pipefail

IFACE="$1"

# Require interface to exist
if [ ! -e "/sys/class/net/${IFACE}" ]; then
  echo "ERROR: Interface ${IFACE} not found on $(hostname)." >&2
  exit 3
fi

# Decide sudo invocation (non-interactive)
if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
else
  if sudo -n true 2>/dev/null; then
    SUDO="sudo -n"
  else
    echo "ERROR: sudo requires a password and non-interactive use is not permitted." >&2
    exit 2
  fi
fi

# Install ethtool if apt-based; ignore failures elsewhere
if command -v apt-get >/dev/null 2>&1; then
  $SUDO apt-get update -y -o Dpkg::Use-Pty=0 || true
  $SUDO apt-get install -y ethtool || true
fi

# Ensure target dirs exist (fixes: "No such file or directory")
$SUDO install -d -m 0755 /etc/systemd/system-sleep
$SUDO install -d -m 0755 /etc/udev/rules.d

# 1) systemd unit to enable WoL + NIC wakeup each boot
$SUDO tee /etc/systemd/system/wol@.service >/dev/null <<'UNIT'
[Unit]
Description=Enable WoL + NIC wakeup on %i
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/bin/sh -c 'ETHTOOL="$(command -v ethtool || echo /usr/sbin/ethtool)"; [ -x "$ETHTOOL" ] && "$ETHTOOL" -s %i wol g || true'
ExecStart=/bin/sh -c 'echo enabled > /sys/class/net/%i/device/power/wakeup 2>/dev/null || true'

[Install]
WantedBy=multi-user.target
UNIT

$SUDO systemctl daemon-reload
$SUDO systemctl enable --now "wol@${IFACE}.service" || true

# 2) system-sleep hook to re-assert on suspend/resume
$SUDO tee /etc/systemd/system-sleep/wol >/dev/null <<EOF
#!/bin/sh
IF="${IFACE}"
ETHTOOL="\$(command -v ethtool || echo /usr/sbin/ethtool)"
case "\$1" in
  pre)
    [ -x "\$ETHTOOL" ] && "\$ETHTOOL" -s "\$IF" wol g || true
    echo enabled > "/sys/class/net/\$IF/device/power/wakeup" 2>/dev/null || true
    ;;
  post)
    [ -x "\$ETHTOOL" ] && "\$ETHTOOL" -s "\$IF" wol g || true
    ;;
esac
EOF
$SUDO chmod +x /etc/systemd/system-sleep/wol

# 3) udev rule to assert wakeup if NIC reappears
$SUDO tee /etc/udev/rules.d/99-nic-wakeup.rules >/dev/null <<EOF
SUBSYSTEM=="net", KERNEL=="${IFACE}", RUN+="/bin/sh -c 'echo enabled > /sys/class/net/%k/device/power/wakeup'"
EOF
$SUDO udevadm control --reload
$SUDO udevadm trigger -s net || true

# 4) Apply immediately for this boot
ETHTOOL="$(command -v ethtool || echo /usr/sbin/ethtool)"
[ -x "$ETHTOOL" ] && $SUDO "$ETHTOOL" -s "${IFACE}" wol g || true
echo enabled | $SUDO tee "/sys/class/net/${IFACE}/device/power/wakeup" >/dev/null || true

# 5) Verify
echo "--- verification ---"
[ -x "$ETHTOOL" ] && $SUDO "$ETHTOOL" "${IFACE}" | grep -i 'Wake-on' || true
cat "/sys/class/net/${IFACE}/device/power/wakeup" 2>/dev/null || true
$SUDO systemctl is-enabled "wol@${IFACE}.service" || true
$SUDO systemctl is-active  "wol@${IFACE}.service" || true
REMOTE

    echo "==== ${node}: DONE ===="
  ) &
done

wait
echo "All nodes processed."

echo
echo "Reminder: enable WoL from S5 in Dell Wyse 5070 BIOS (Power Management > Wake on LAN)."
