# AGENTS.md

This file defines how AI coding agents should operate in the `thinvids` repository.

## 1. Scope and priorities
- Make the smallest safe change that solves the user request.
- Preserve the existing manager/worker/agent architecture unless a refactor is explicitly requested.
- Prefer correctness, operability, and maintainability over cleverness.
- Keep momentum: implement, verify, and report concrete outcomes.

## 2. Project architecture context
- `manager/`: web UI and watcher logic that discovers inbound media and queues work.
- `worker/`: task pipeline for segmenting, encoding, and stitching media.
- `agent/`: worker-side metrics/heartbeat and idle-state behavior.
- `common.py`: shared constants/utilities used across manager, worker, and agent.
- `ansible_manager.yml` and `ansible_workers.yml`: source of truth for deployment and systemd wiring.

## 3. Where to work
- Work only inside this repository unless explicitly asked to touch another repo.
- Treat infra and environment values as sensitive (hosts, mounts, credentials, tokens).
- Do not modify generated media artifacts or large archives unless explicitly requested.

## 4. Change policy
- Avoid broad rewrites; use focused diffs.
- Keep naming and runtime conventions consistent with surrounding code.
- Add comments only when behavior is non-obvious.
- Update docs when behavior, commands, deployment steps, or topology assumptions change.
- If a request is ambiguous, choose the safest interpretation and state assumptions.

## 5. Deployment and operations conventions
- Prefer Ansible playbooks over ad-hoc host changes.
- Keep manager and worker changes compatible with existing systemd service expectations.
- Preserve mount/path assumptions unless explicitly asked to change them (`/watch`, `/library`, `/config`, `/opt/jerry`).
- Preserve Redis contract compatibility unless a coordinated change is requested.
- Reuse repo scripts where relevant (`command-workers.sh`, `tail-workers.sh`, `nodes-suspend.sh`).

## 6. Git safety rules
- Never run destructive commands unless explicitly requested: `git reset --hard`, `git checkout --`, mass `rm`, history rewrites.
- Never revert unrelated local changes.
- If unrelated dirty changes exist, ignore them unless they block the task.
- Keep commits scoped to the task.

## 7. Validation standard
Run the narrowest useful checks first, then broader checks when needed.

- If Python app code changed (`manager/`, `worker/`, `agent/`, `common.py`):
  - Run targeted tests if present.
  - At minimum, run syntax/import validation where practical.
- If Ansible/systemd wiring changed:
  - Validate YAML syntax and verify service/unit references remain coherent.
- If operational scripts changed:
  - Dry-run or lint shell scripts when possible.

If checks cannot be run, state exactly what could not be run and why.

## 8. Security and reliability
- Never print secret values from local configs or environment files.
- Call out behavior changes that could affect job durability, queue semantics, or node power-state handling.
- For changes touching suspend/resume behavior, explicitly note risk and rollback approach.

## 9. Response format to user
- Start with what changed.
- List touched files with purpose.
- Report validation performed and results.
- List assumptions, risks, and next steps only when relevant.

## 10. Default command preferences
- Search files with `rg` (fallback to `grep`).
- Use targeted reads instead of full tree dumps.
- Use non-interactive, reproducible commands.

## 11. Definition of done
A task is done when:
1. Requested change is implemented.
2. Reasonable validation is run (or limitations are explicitly stated).
3. User-facing summary is clear and actionable.
