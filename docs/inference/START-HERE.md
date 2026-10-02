# Start here while all Thinvids nodes are asleep

For direct removal and inference installation, run `./repurpose-node.sh thinman25`
from the Thinvids repository on swarm4. You are responsible for ensuring that
node has no running tasks. The script skips job/queue scans, leaves the controllers
running, and removes Thinvids services and application files. See
[the automation guide](AUTOMATION.md) for the exact removal scope and recovery.

The older manual procedure below disables services but retains their files.
It is an alternative, not a prerequisite for the script. Its re-enable-only
rollback does not apply to automated removal; use the automation guide's
redeployment instructions instead.

This is an instruction set, not an executed migration. `thinman01` is the example
pilot. No wake request, service change, inventory change, or model download has
been performed. Keep all other nodes asleep for the one-node baseline.

1. On swarm4, begin [the runbook's maintenance section](README.md#1-retire-the-first-node-from-thinvids):
   record the watcher's current state and stop it so it cannot submit new jobs.
   Pause other submitters and avoid manual launches during the migration.
2. Check the manager's saved job states. WAITING, STARTING, RUNNING, and STAMPING
   must all be empty. Sleeping nodes alone do not establish this. Resolve pending
   work before proceeding; do not clear queues or delete media.
3. When the queue is clear, wake only the pilot using the command below. Use it
   before marking the node disabled, because the wake endpoint rejects disabled
   hosts. The endpoint sends a magic packet only to the named host.

```bash
curl --fail-with-body -sS -X POST \
  http://192.168.0.108:5005/nodes/wake/thinman01
```

Allow resume recovery to finish, then connect promptly:

```bash
ssh -o ConnectTimeout=8 jerry@thinman01
```

If SSH remains unavailable, inspect power and network access before proceeding.
If the node is already disabled, use physical wake or a direct Wake-on-LAN packet
to its confirmed MAC instead of temporarily returning it to rotation just to use
the manager's wake endpoint. Do not select Wake All.

4. Recheck jobs and local service/process state, then continue the runbook at
   **Stop services and prevent their return**. Stop the agent first, then the two
   consumers, disable the resume hook, mark the node disabled in the manager, and
   move its inventory entry to `inference_workers`.
5. Verify retirement before restarting any previously running watcher. Build and
   benchmark the CPU model on this dedicated node, then install the local-serving
   unit and connect through its SSH tunnel. The agent must remain stopped so its
   idle timer cannot suspend the pilot.

The [full runbook](README.md) includes build and model commands, benchmarking,
local chat, expansion to 2/5/10/20 nodes, independent-agent comparisons, and rollback.
The service templates are [local serving](qwen-inference.service) and
[RPC worker](qwen-rpc.service). Start with local serving only.

## Installing the RPC template later

At the distributed trial stage, from swarm4, for each already-retired host
(shown here for the pilot):

```bash
scp /home/jerry/apps/thinvids/docs/inference/qwen-rpc.service \
  jerry@thinman01:/home/jerry/inference-migration/qwen-rpc.service
```

Then on that host:

```bash
sudo install -m 0644 /home/jerry/inference-migration/qwen-rpc.service /etc/systemd/system/qwen-rpc.service
sudo systemctl disable --now qwen-inference.service
sudo systemctl daemon-reload
sudo systemctl enable --now qwen-rpc.service
sudo journalctl -u qwen-rpc -n 40 --no-pager
```

Continue with the loopback SSH forwards and coordinator commands in the runbook.
Do not run the baseline server and RPC worker together during measurements.
