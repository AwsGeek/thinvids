# One-command dedicated inference pilot

From swarm4, as `jerry`:

```bash
cd /home/jerry/apps/thinvids
./repurpose-node.sh thinman25
```

The only argument is an exact registered inventory hostname. Do not use sudo.
Existing Ansible SSH credentials and noninteractive sudo are used on the worker.

**You must ensure the selected node has no running tasks.** This is direct
removal, not a drain: there are no job-history scans, queue checks, or waits for
media work to finish. Any remaining worker tasks will be interrupted. Other
nodes and the manager, watcher, and housekeeping services are left running.
Do not deploy Thinvids or manually re-enable the selected host during migration.

## What it does

1. Validates the host and pins llama.cpp plus the official Qwen model revision
   and checksum. Subsequent nodes reuse these pins.
2. Disables that node in Redis and moves its inventory entry from
   `thinman_workers` to `inference_workers`, preserving host variables and a backup.
3. Sends Wake-on-LAN only to that node, waits for SSH, and verifies its hostname.
4. Removes Thinvids resume hooks; stops and disables the agent and worker services;
   removes their unit files and drop-ins, application code, and private virtualenv.
5. Installs a pinned CPU-native llama.cpp runtime with RPC support and downloads
   Qwen2.5-Coder-1.5B-Instruct Q4_K_M with SHA-256 verification.
6. Benchmarks the CPU, starts local chat, and checks health and an actual completion.
   RPC is installed but remains stopped for this single-node baseline.
7. Reboots on the first successful installation, checks chat and retirement,
   and retrieves benchmark results to swarm4.

The baseline needs at least 3 GB RAM, two logical CPUs, and 5 GiB free on the
`/opt` filesystem after retirement. Compilation and benchmarking can take a while.

## Removal scope

- `thinman-agent.service`, `thinman-worker-encode.service`,
  `thinman-worker-pipeline.service`, and legacy `thinman-worker.service`:
  files and drop-ins under `/etc/systemd/system`.
- Resume hooks at `/lib/systemd/system-sleep/thinman-resume` and
  `/etc/systemd/system-sleep/thinman-resume`.
- `/opt/jerry/current` and `/opt/jerry/venv`.
- `/etc/sudoers.d/thinvids-worker-reboot`.

Shared media, archives, scratch files, NFS mounts/fstab, SSH access, network
configuration, logging configuration, and OS packages (including FFmpeg) are
preserved. The private virtualenv removal uninstalls Thinvids' Python dependencies.
Application removal refuses symlinked paths or mounts at/above/below its two
application directories, except the ordinary root filesystem. Unknown Thinman
service names require review. Inference services cannot access known media mounts.

## Connect to chat

From the computer running your browser:

```bash
ssh -N -L 18080:127.0.0.1:8080 jerry@thinman25
```

Open `http://127.0.0.1:18080`. API: `http://127.0.0.1:18080/v1`;
model: `qwen-baseline`. No provider API key is needed.

## Results and recovery

- Worker results: `/opt/inference/results/baseline-cpu.json` and
  `/opt/inference/automation.json`.
- Controller results: `~/.local/state/thinvids-inference/results/<host>/`.
- Controller journals, pins, and inventory backups: a repository-specific
  subdirectory of `~/.local/state/thinvids-inference/`.
- Original worker service/hook metadata: `/var/lib/thinvids-retirement/original.json`.

Rerun the same command after failure. Once disabled, a node is never automatically
re-enabled, including when SSH, uninstallation, or inference setup fails. Repeated
removal tolerates already-removed services and files. Successful baseline results
and reboot validation are preserved. Let interrupted builds/benchmarks release
their locks before retrying; do not delete locks to force concurrent runs.

An older script version may have left a `service-recovery.json` checkpoint after
stopping controllers. For that case only, a same-host rerun restores the previously
running controllers after successful worker removal. It does not pause them anew.

Do not run the playbook directly or edit pins mid-trial. The wrapper owns
retirement ordering, Wake-on-LAN, inventory updates, and checkpoints.

## More nodes

Repeat with each additional hostname once the first baseline is satisfactory.
Each run dedicates one host; it does not automatically combine models or create
an agent framework. Follow the [scaling experiments](README.md#6-expand-to-distributed-inference)
for 2/5/10/20 nodes.

## Returning a node to Thinvids

Removal is not reversible by simply re-enabling services. Stop and disable
`qwen-inference.service` and `qwen-rpc.service`, disconnect the node from inference,
move only its inventory record back into `thinman_workers`, and redeploy
`ansible_workers.yml --limit <host>` using your normal worker deployment command.
Remove the dedicated-inference and software-removed checkpoint files and any
startup-condition drop-ins left by an interrupted uninstall before redeployment.
Keep the node disabled in the manager until services, mounts, and heartbeat have
been verified; then enable it and test a video task.
Never overwrite the current inventory with an entire old backup.

## Local validation

These checks do not wake nodes or deploy anything:

```bash
python3 -B -m unittest discover -s tools/inference -p 'test_*.py' -v
ansible-playbook -i ansible_hosts.ini ansible_inference.yml --syntax-check
bash -n repurpose-node.sh
```
