# Dedicated inference pilot: 1, 5, 20, then more nodes

For direct removal and inference installation, run `./repurpose-node.sh thinman25`
from the Thinvids repository on swarm4. You are responsible for ensuring that
node has no running tasks. The script skips job/queue scans, leaves the controllers
running, and removes Thinvids services and application files. See
[the automation guide](AUTOMATION.md) for the exact removal scope and recovery.

The older manual procedure below disables services but retains their files.
It is an alternative, not a prerequisite for the script. Its re-enable-only
rollback does not apply to automated removal; use the automation guide's
redeployment instructions instead.

These are operator instructions, not a record of an executed migration. No node
has been removed from Thinvids and no model has been installed by writing this guide.
Commands were checked against repository code and current upstream documentation;
the installation and inference have not yet been tested on a client.

Use `thinman01` as the example pilot: an earlier read-only check found a J5005,
8 GB RAM, UHD 605, and a 28 GB root disk. The latest SSH check timed out; wake it
from Thinvids before maintenance if needed. Substitute a different confirmed host
throughout if preferred. Inventory currently lists 25 workers; verify additional
hosts before treating the quoted 50-node layout as the actual fleet.

Phase 1 establishes a single-user baseline with a local model. Later phases compare
one shared model over RPC against independently served models coordinated as agents.
Those are different experiments; installing a local server does not shard a model.

## 1. Retire the first node from Thinvids

### Obtain a safe maintenance window

The current code has no verified per-node drain operation:

- `POST /nodes/disable/<hostname>` marks a node disabled and removes metrics.
- The encode consumer checks that marker at startup, not before every task.
- The agent actively starts worker services and can suspend the machine.
- `/lib/systemd/system-sleep/thinman-resume` restarts services after resume.

Therefore, do not disable a busy node and assume it will finish gracefully. The
conservative procedure is a short cluster maintenance window after existing work
has drained. If the backlog cannot drain, implement a real drain operation first;
do not stop a worker in the middle of a media pipeline.

On swarm4, record whether the watcher is running, then stop new watcher submissions:

```bash
cd /home/jerry/apps/thinvids
docker compose ps watcher
docker compose stop watcher
```

Stop other automated submissions and avoid manually starting/retrying jobs during
maintenance. Let existing jobs finish. In the dashboard at
`http://192.168.0.108:5005`, filter separately for WAITING, STARTING, RUNNING, and
STAMPING; all four must be empty. READY jobs may remain. Stopping the watcher does
not pause already queued work. Resolve stuck jobs before proceeding; do not delete
queue contents or media to force an empty display.

On the pilot, inspect service status and processes as a second check:

```bash
ssh jerry@thinman01
hostname
systemctl list-unit-files 'thinman*'
systemctl status thinman-agent thinman-worker-encode thinman-worker-pipeline --no-pager
ps -eo pid,ppid,comm,args | grep -E '[f]fmpeg|[f]fprobe|[h]uey_consumer'
```

Idle Huey consumers are expected at this point. There must be no unfinished
segmenting, encoding, stitching, upload, or mux task. Low CPU or no ffmpeg process
alone does not prove the node is idle; reconcile with the manager jobs and logs.

### Stop services and prevent their return

After the drain checks, on the pilot, record existing state and stop the agent
first so it cannot restart the consumers:

```bash
mkdir -p /home/jerry/inference-migration
systemctl list-unit-files 'thinman*' > /home/jerry/inference-migration/unit-state.txt
sudo systemctl disable --now thinman-agent.service
sudo systemctl disable --now thinman-worker-encode.service thinman-worker-pipeline.service
```

If the initial listing showed a legacy `thinman-worker.service`, disable and stop
that specific unit too. If a command fails, investigate before continuing.

Disable the resume hook without removing it:

```bash
if test -f /lib/systemd/system-sleep/thinman-resume; then
  sudo chmod a-x /lib/systemd/system-sleep/thinman-resume
fi
if test -f /etc/systemd/system-sleep/thinman-resume; then
  sudo chmod a-x /etc/systemd/system-sleep/thinman-resume
fi
```

Back on swarm4, mark the node disabled. This endpoint does not require a model API key:

```bash
curl --fail-with-body -sS -X POST \
  http://192.168.0.108:5005/nodes/disable/thinman01
```

Require a successful response with `disabled: true`. Keep the disabled entry as a
record; deleting it is not a substitute for stopping the services.

Edit `ansible_hosts.ini`: move only this host from `[thinman_workers]` into a new
`[inference_workers]` group. Keep all other hosts and variables. For example:

```ini
[inference_workers]
thinman01
```

Verify that it appears only in the new group:

```bash
ansible -i ansible_hosts.ini thinman_workers --list-hosts
ansible -i ansible_hosts.ini inference_workers --list-hosts
```

The existing deployment, cleanup, and suspend scripts select `thinman_workers`.
This inventory change prevents them from targeting the dedicated node. Do not
later apply `ansible_workers.yml` to it or override those scripts to target `all`.
Check separately for any external automation with hard-coded host lists.

On the pilot, verify the three services are inactive and disabled. `is-active`
and `is-enabled` return nonzero for these expected states:

```bash
systemctl is-active thinman-agent thinman-worker-encode thinman-worker-pipeline
systemctl is-enabled thinman-agent thinman-worker-encode thinman-worker-pipeline
```

Do not remove `/projects`, `/watch`, `/library`, `/source_media`, or any media.
Leave old configuration in place for rollback. The supplied inference services
run under a separate account and cannot access those media paths.

On swarm4, restart the watcher only if it was running before maintenance:

```bash
docker compose start watcher
```

The remaining Thinvids workers can now continue normally.

## 2. Build the one-node baseline

Run on the retired pilot. Use a fresh `/opt/inference`; if it already contains an
installation, inspect it before proceeding. Execute blocks in order and stop on
any error. These instructions assume Debian/Ubuntu and the existing `jerry` account.

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git curl jq libssl-dev libcurl4-openssl-dev lm-sensors
sudo useradd --system --user-group --create-home --home-dir /var/lib/inference --shell /usr/sbin/nologin inference
sudo install -d -o jerry -g jerry /opt/inference
mkdir -p /opt/inference/models /opt/inference/results
git clone https://github.com/ggml-org/llama.cpp.git /opt/inference/llama.cpp
cd /opt/inference/llama.cpp
git rev-parse HEAD | tee /opt/inference/llama-revision.txt
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DGGML_NATIVE=ON -DGGML_RPC=ON -DLLAMA_BUILD_TESTS=OFF
cmake --build build --config Release -j 2 \
  --target llama-server llama-bench ggml-rpc-server
./build/bin/llama-server --version
```

Build on the actual pilot: `GGML_NATIVE=ON` targets its instruction set. Do not
copy a native build made on a newer AVX2 machine onto a J5005. Start CPU-only;
Vulkan is a separate experiment after the CPU baseline works. Reuse the recorded
git commit on every later worker, building for each hardware class. Do not pull
new source in the middle of a benchmark series. If a target or flag differs in
your recorded revision, check its documentation and help before proceeding.

Use Qwen2.5-Coder-1.5B-Instruct Q4_K_M as the first controlled model because it
matches the small-model proposal and fits a modest RAM budget. It is a benchmark
baseline, not a claim that this is the best current chat model. Download just that
GGUF from Qwen's official repository, pinning its repository revision:

```bash
cd /opt/inference
QWEN_REV=$(curl -fsSL https://huggingface.co/api/models/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF | jq -er '.sha')
[[ "$QWEN_REV" =~ ^[0-9a-f]{40}$ ]] || exit 1
printf '%s\n' "$QWEN_REV" > model-revision.txt
curl -fL --retry 3 \
  "https://huggingface.co/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF/resolve/${QWEN_REV}/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf" \
  -o models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf.part
```

Only after curl succeeds, finalize the download and record the exact bytes:

```bash
cd /opt/inference
mv models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf.part models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf
sha256sum models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf | tee model-sha256.txt
```

Retain that artifact and checksum for subsequent nodes; a local checksum records
identity, not an independent authenticity attestation. No cloud API key is needed.

## 3. Measure before starting a persistent server

On the pilot, while no inference service is running:

```bash
cd /opt/inference/llama.cpp
./build/bin/llama-bench \
  -m /opt/inference/models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf \
  -ngl 0 -t 2,4 -p 512 -n 128 -r 5 -o json \
  > /opt/inference/results/baseline-cpu.json
lscpu > /opt/inference/results/lscpu.txt
free -h > /opt/inference/results/memory.txt
```

Record prompt-processing tokens/second and generation tokens/second separately.
Select the faster stable thread count for serving; the template starts at four.
Use `vmstat 1` and `sensors` in another terminal during a repeat run to observe
swap activity and temperatures where sensors are available. Stop those monitors
with Ctrl-C. Do not compare a swapping or thermally unstable run as a normal result.

## 4. Start chat on the dedicated node

From swarm4, transfer the supplied unit template:

```bash
scp /home/jerry/apps/thinvids/docs/inference/qwen-inference.service \
  jerry@thinman01:/home/jerry/inference-migration/qwen-inference.service
```

On the pilot:

```bash
sudo install -m 0644 /home/jerry/inference-migration/qwen-inference.service /etc/systemd/system/qwen-inference.service
sudo systemctl daemon-reload
sudo systemctl enable --now qwen-inference.service
sudo journalctl -u qwen-inference -n 60 --no-pager
curl -fsS http://127.0.0.1:8080/health
```

Wait for model loading and a healthy response. The service has one request slot,
4,096 tokens of context, and no GPU offload. Its HTTP port listens on loopback.
From the computer where you will open the browser, keep an SSH tunnel running:

```bash
ssh -N -o ExitOnForwardFailure=yes -L 18080:127.0.0.1:8080 jerry@thinman01
```

Open `http://127.0.0.1:18080` on that computer for the built-in chat UI. If the
computer needs swarm4 as a jump host, add `-J jerry@swarm4` using its reachable SSH
address. The tunnel can also serve a local coding client at
`http://127.0.0.1:18080/v1` with model `qwen-baseline`.

Example request from that computer, in another terminal:

```bash
curl -fsS http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen-baseline","messages":[{"role":"user","content":"Explain the difference between a process and a thread in five sentences."}],"temperature":0,"seed":42,"max_tokens":256,"stream":false}'
```

Finish the pilot with a reboot, then verify inference starts and the three
Thinvids services stay inactive. Confirm no new Thinvids jobs use this node.
Observe it through an interval longer than the old idle-suspend threshold.
If it still sleeps, inspect OS power settings and external automation before
changing them; stopping the Thinvids agent removes only Thinvids' suspend logic.

## 5. Establish quality and latency baselines

Use a fixed set of 10-20 tasks, including ordinary chat, a short code explanation,
a small function with independent tests, a bug fix, and a change spanning two files.
Keep the original prompts, repository revision, expected outputs, and tests fixed.
Execute generated code in isolated test environments without media mounts or secrets.

For each configuration:

1. Record source/model revisions, GGUF SHA-256, hardware, backend, thread count,
   context size, sampling settings, and participating hosts.
2. Record cold model-loading time separately from warm request latency.
3. Repeat each task three times. Keep model, context, and token budget identical.
4. Measure first content token time with a streaming client and total completion
   time. HTTP header timing is not first-token timing. Use fresh slots or disable
   prompt reuse consistently so cached prompts do not skew the comparison.
5. Count independently passing tests, incomplete/truncated answers, retries,
   peak memory, swap, and failures. Include planning and integration time for agents.
6. Check a longer conversation within the 4,096-token context limit and a sustained
   workload of at least 15 minutes. Do not confuse synthetic throughput with quality.

Retain raw outputs and timings, not only averages. A small sample's median and
range are more useful than an unstable p95. Define your acceptable chat delay and
task success rate before deciding whether to expand.

## 6. Expand to distributed inference

Retire each additional host with section 1. Use the same llama.cpp commit and
baseline GGUF. Verify RAM and CPU on every host; do not assume all have 8 GB.

| Stage | Experiment | Advancement gate |
| --- | --- | --- |
| 1 node | Local CPU baseline; optional separate Vulkan comparison | Stable service, no Thinvids work or suspend, recorded speed and quality |
| 2 nodes | Same small model across two CPU RPC workers | Logs prove both workers execute layers; correct output; measured RPC overhead |
| 5 nodes | Same model for scaling, then a separately labeled larger-model test | No swapping; acceptable latency; all selected workers actually used |
| 10 nodes | Repeat five-node protocol and recovery test | Benefit justifies additional machines |
| 20 nodes | Repeat, then test models needing more aggregate RAM | Useful quality/capacity or completion-time improvement |
| More nodes | Repeat only after inventory and hardware checks | Measured benefit rather than idle devices counted as participating |

At two nodes and beyond, use swarm4 as the HTTP coordinator and retired clients as
RPC workers. Build the same source revision on swarm4 for its own CPU; retain the
complete model artifact there. Record coordinator compute and memory too. Ensure
logs show weights assigned to the intended remote devices, not silently executed
mostly on swarm4. Aggregate RAM is not all usable: reserve OS, runtime, and cache
headroom per node and measure coordinator loading memory.

Install `qwen-rpc.service` using the same scp/install pattern as section 4, then
on each retired worker switch modes:

```bash
sudo systemctl disable --now qwen-inference.service
sudo systemctl daemon-reload
sudo systemctl enable --now qwen-rpc.service
sudo journalctl -u qwen-rpc -n 40 --no-pager
```

The RPC service uses loopback port 50052. Keep private SSH forwards on swarm4,
one per worker, in separate terminals for the initial trial:

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
  -L 51001:127.0.0.1:50052 jerry@thinman01
```

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
  -L 51002:127.0.0.1:50052 jerry@thinman02
```

RPC is experimental and not a secure public service; keep it off the general LAN.
Tunnels contribute overhead, which must remain consistent across comparisons.
Before five or more nodes, manage their lifecycle with systemd/Ansible instead of
leaving many manual terminals running. A private restricted network is an optional
later transport experiment, with its own benchmark series.

On swarm4, assuming the same `/opt/inference` build/model layout, list remote devices:

```bash
/opt/inference/llama.cpp/build/bin/llama-server \
  --rpc 127.0.0.1:51001,127.0.0.1:51002 --list-devices
```

Use exactly the two remote device names returned, in that order, as
`RPC_DEVICE_ONE` and `RPC_DEVICE_TWO` below. They are placeholders, not literal
device names. Exclude coordinator accelerators from this controlled comparison.

```bash
/opt/inference/llama.cpp/build/bin/llama-server \
  -m /opt/inference/models/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf \
  --rpc 127.0.0.1:51001,127.0.0.1:51002 \
  --device RPC_DEVICE_ONE,RPC_DEVICE_TWO \
  --split-mode layer --tensor-split 1,1 -ngl 99 \
  --alias qwen-baseline --host 127.0.0.1 --port 8080 \
  -c 4096 -np 1 -t 4 -tb 4
```

The offload option is named `gpu-layers` even when the remote device is a CPU.
Confirm actual allocation and successful output in logs; RPC compatibility is a
test gate. Check the pinned version's help if its flags or names differ. More
registered endpoints do not prove the model uses them all: small models have a
finite number of layers, and allocation granularity can leave devices unused.
For five nodes, extend the endpoint/device lists and split proportions, initially
using equal shares only for equal-capacity machines. Later tune against measured
available memory and execution time, keeping the original equal-split results.

Only after the same-model comparison, try a larger quantized Qwen model, such as
a 7B/14B model or the previously discussed 30B/35B MoE candidates if supported.
Label those as model-capacity/quality experiments; they are not same-model speedups.
Do not promise larger-model fit until per-device and coordinator memory are measured.

Perform a controlled failure test with disposable chat: stop one RPC worker, verify
the request fails visibly, restore the worker, and restart/reload the coordinator.
Expect lost in-memory conversation cache; do not assume transparent failover or
live model redistribution. Preserve chat history on the frontend for replay.

## 7. Compare independent agents at five nodes

Use this as a separate experiment after the five-node RPC result. Stop the shared
model coordinator, disable/stop `qwen-rpc` on the workers, and enable the local
`qwen-inference` services. Keep remote HTTP access private through SSH forwards.
Each worker loads the full small model locally. Run the orchestrator on swarm4.

Start with a fixed workflow: plan, independent patch candidates, run independent
acceptance tests, then integrate one passing result. Use isolated checkouts and
one integration owner. Do not let multiple agents mutate the same checkout.
Run the identical workflow serially on one node as well as in parallel on five,
holding task count, retries, and total generated-token budget constant. This
separates benefits of more compute from benefits of distributing the work.

Record total wall-clock time through integration and tests. Scale to 20 only when
there are enough independent tasks or candidates to use that parallel capacity.
Local model servers alone do not provide this orchestrator. Framework selection
comes after the inference measurements; do not install a 50-agent architecture
as a prerequisite to the one-node baseline.

## 8. Rollback

To restore a retired host, first stop and disable its inference/RPC services and
stop coordinator requests using it. Keep model files and results for later use.
Move the host from `inference_workers` back to `thinman_workers`, then on swarm4:

```bash
curl --fail-with-body -sS -X POST \
  http://192.168.0.108:5005/nodes/enable/thinman01
```

On the pilot, restore the previously executable resume hook and the recorded
service enablement state. For the current deployment, the agent and encode worker
are enabled, while the agent controls pipeline-worker startup:

```bash
sudo chmod 0755 /lib/systemd/system-sleep/thinman-resume
sudo systemctl enable --now thinman-agent.service thinman-worker-encode.service
```

Restore the legacy `/etc` hook only if it existed and was executable beforehand.
Verify heartbeat, mounts, and a successful video task before considering rollback
complete. No media or archive files are deleted by this procedure.

## References

- Thinvids: `manager/app.py` node disable/enable routes; `worker/tasks.py` startup
  disabled-node check; `agent/agent.py` role synchronization; `ansible_workers.yml`
  service definitions and resume hook.
- [llama.cpp build instructions](https://github.com/ggml-org/llama.cpp/blob/master/docs/build.md)
- [CPU build feature selection](https://github.com/ggml-org/llama.cpp/blob/master/ggml/src/ggml-cpu/CMakeLists.txt)
- [Qwen's baseline GGUF](https://huggingface.co/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF/tree/main)
- [llama-bench options](https://github.com/ggml-org/llama.cpp/blob/master/tools/llama-bench/README.md)
- [Server options](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md)
- [RPC setup and limitations](https://github.com/ggml-org/llama.cpp/blob/master/tools/rpc/README.md)
- [RPC worker command-line options](https://github.com/ggml-org/llama.cpp/blob/master/tools/rpc/rpc-server.cpp)
