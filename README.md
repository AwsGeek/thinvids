Thinvids is a distributed video encoding system using inexpensive thin clients
and low-end GPUs.

## Manager on swarm4 with Docker Compose

The manager-side services can run locally on `swarm4`:

- `thinvids_manager`: Flask/Gunicorn web UI on port `5005`
- `thinvids_housekeeping`: scheduler/watchdog housekeeping loop
- `thinvids_watcher`: media watcher that submits jobs to the manager

Redis remains external on `swarm1:6379`. Worker nodes remain managed by
`ansible_workers.yml`.

### Cutover

1. Stop the old manager services on `thinman01`:

   ```bash
   ssh thinman01 'sudo systemctl disable --now thinvids_manager thinvids_manager_housekeeping thinvids_watcher'
   ```

2. Start the manager stack on `swarm4`:

   ```bash
   cd ~/apps/thinvids
   docker compose up -d --build
   ```

3. Confirm the UI is available:

   ```bash
   curl -sS http://localhost:5005/settings
   docker compose ps
   docker compose logs -f watcher
   ```

4. Repurpose `thinman01` as a worker:

   ```bash
   ansible-playbook -i ansible_hosts.ini ansible_workers.yml --limit thinman01
   ```

5. Deploy worker updates normally:

   ```bash
   ansible-playbook -i ansible_hosts.ini ansible_workers.yml --tags build_app
   ```

### Notes

- `docker-compose.yml` mounts the same NFS-backed paths used by the legacy
  manager: `/watch`, `/library`, `/config`, and `/source_media`.
- The Watcher page uses Docker control mode via `/var/run/docker.sock` to
  start, stop, restart, and read logs from the `thinvids_watcher` container.
- Watcher settings are written to `/config/watcher.env`. In Compose mode,
  `watcher.py` reads that file on process start, so saving settings and
  restarting the watcher container applies the changes.
- Worker reboot actions use `MANAGER_SSH_USER` and `MANAGER_SSH_KEY_PATH` from
  `thinvids.env`. The matching public key is distributed to workers by
  `ansible_workers.yml`.
