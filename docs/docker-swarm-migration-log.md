# ThinVids Docker/Swarm Migration Log

## Purpose
This file is a running log of architecture decisions, experiments, and next steps for moving ThinVids from Ansible-managed hosts to a hybrid Docker + Docker Swarm model.

## How To Use
- Add a new entry for each discussion/decision.
- Keep entries chronological.
- Capture: context, decision, rationale, risks, and action items.
- Mark uncertain items as `Open`.

## Current Target Architecture (Working Draft)
- Manager plane in Docker Swarm.
- Worker plane on standalone Docker Compose per worker host.
- Manager handles orchestration, queue/state, watch directory, and stitching.
- Workers handle GPU-backed segment transcodes and sleep when idle.
- Wake/sleep behavior coordinated via manager (WOL + idle policy).

---

## Log Entries

### 2026-03-01 - Initial Hybrid Direction
- Context:
  ThinVids workers need direct GPU access. Swarm scheduling for these GPU worker patterns is a poor fit for the current environment.
- Decision:
  Adopt a hybrid model:
  - Swarm for manager and shared control-plane services.
  - Compose on each worker for GPU transcode workers.
- Rationale:
  - Reduces dependence on Ansible for day-to-day app lifecycle.
  - Keeps worker GPU access straightforward.
  - Preserves low-power behavior (sleep when idle).
- Risks:
  - Two deployment planes to manage.
  - Need clear worker registration/heartbeat/lease model.
- Actions:
  - Define manager stack services (API/scheduler, queue, state DB, watcher).
  - Define worker Compose spec and host bootstrap process.
  - Define wake/sleep control flow and failure handling.

### 2026-03-01 - Migration Constraint Noted
- Context:
  Desire to move more components from Ansible-managed scripts to container-managed services.
- Decision:
  Keep host provisioning minimal, but move runtime orchestration into containers.
- Rationale:
  - Ansible remains useful for one-time host baseline setup.
  - Operational changes should happen via `docker stack deploy` / `docker compose up`.
- Open:
  Decide whether to keep a minimal Ansible role for host bootstrap only, or replace with scripted bootstrap + systemd units.

### 2026-03-01 - Next Deliverables
- Planned:
  1. Swarm stack file for manager plane.
  2. Worker Compose file with GPU access and restart policy.
  3. Systemd units/timers for worker auto-start, update, and idle suspend.
  4. Runbook for cutover from current Ansible flow.

### 2026-03-01 - Manager Plane Stack v1 Created
- Context:
  Start implementing the Swarm-managed manager plane.
- Decision:
  Added first-pass manager stack artifacts:
  - `Dockerfile.manager` (shared image for manager app + watcher)
  - `manager/requirements.txt`
  - `stack.manager.yml` (services: `manager`, `watcher`, `redis`)
- Rationale:
  - Current runtime only requires Redis as datastore (Huey queue + app state).
  - `manager/app.py` imports `tasks`, so image includes `worker/tasks.py` and `common.py`.
  - Watcher and manager both mount NFS-backed `/watch` and `/config` to preserve current behavior.
- Placement:
  - `manager` + `watcher` pinned to `node.labels.purpose == general`
  - `redis` pinned to `node.labels.purpose == light` (swarm1-style placement)
- Build/Deploy Commands:
  1. `docker build -f Dockerfile.manager -t 127.0.0.1:5000/thinvids-manager:latest .`
  2. `docker push 127.0.0.1:5000/thinvids-manager:latest`
  3. `docker stack deploy -c stack.manager.yml thinvids`
- Open:
  - Confirm whether to keep Redis inside this stack or continue using existing `thinvidsredis` stack.
  - Confirm if `manager` should be constrained to a specific hostname instead of `purpose=general`.
