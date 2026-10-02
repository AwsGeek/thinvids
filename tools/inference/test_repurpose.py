import json
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

import repurpose
from redis_guard import node_action
from check_removal import check_paths


class InventoryTests(unittest.TestCase):
    def test_moves_only_target_preserving_host_vars_and_line_endings(self):
        before = b"[thinman_workers]\r\nthinman01 ansible_host=10.0.0.1 # pilot\r\nthinman02\r\n[all:vars]\r\nansible_user=jerry\r\n"
        after = repurpose.moved_inventory(before, "thinman01")
        self.assertIn(b"[thinman_workers]\r\nthinman02\r\n", after)
        self.assertIn(b"[all:vars]\r\nansible_user=jerry\r\n", after)
        self.assertIn(b"[inference_workers]\r\nthinman01 ansible_host=10.0.0.1 # pilot\r\n", after)

    def test_existing_destination_and_idempotence(self):
        raw = b"[inference_workers]\nthinman02\n[thinman_workers]\nthinman01\n"
        moved = repurpose.moved_inventory(raw, "thinman01")
        self.assertEqual(moved, b"[inference_workers]\nthinman01\nthinman02\n[thinman_workers]\n")
        self.assertEqual(repurpose.moved_inventory(moved, "thinman01"), moved)

    def test_absent_duplicate_and_wrong_group_rejected(self):
        for raw in (b"[thinman_workers]\nthinman02\n",
                    b"[thinman_workers]\nthinman01\nthinman01\n",
                    b"[other]\nthinman01\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                repurpose.moved_inventory(raw, "thinman01")

    def test_shell_and_ansible_patterns_rejected(self):
        for host in ("all", "thinman*", "thinman01;reboot", "-i", "thinman01:thinman02", "../thinman01"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                repurpose.validate_host(host)
        self.assertEqual(repurpose.validate_host("thinman25"), "thinman25")

    def test_children_are_included_in_semantic_validation(self):
        inv = {"thinman_workers": {"children": ["nested"]}, "nested": {"hosts": ["thinman01"]}}
        self.assertEqual(repurpose.inventory_hosts(inv, "thinman_workers"), {"thinman01"})

    def test_missing_checksum_rejected(self):
        with self.assertRaises(ValueError):
            repurpose.validate_versions({"llama_revision": "a" * 40, "model_revision": "b" * 40})


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.data = MagicMock()
        self.data.hget.return_value = "aa:bb:cc:dd:ee:ff"
        self.data.sismember.return_value = False
        self.pipe = self.data.pipeline.return_value.__enter__.return_value

    def test_check_reads_only_target_without_scanning_jobs(self):
        self.assertFalse(node_action(self.data, "thinman25", "check")["disabled"])
        self.data.hget.assert_called_once_with("nodes:mac", "thinman25")
        self.data.scan_iter.assert_not_called()
        self.data.pipeline.assert_not_called()

    def test_disable_only_touches_target_and_never_scans_jobs(self):
        self.assertTrue(node_action(self.data, "thinman25", "disable")["disabled"])
        self.data.pipeline.assert_called_once_with(transaction=True)
        self.pipe.sadd.assert_called_once_with("nodes:disabled", "thinman25")
        self.pipe.delete.assert_called_once_with("metrics:node:thinman25")
        self.pipe.hset.assert_called_once()
        self.assertEqual(self.pipe.hset.call_args.args[0], "node:quarantine:thinman25")
        self.pipe.execute.assert_called_once_with()
        self.data.scan_iter.assert_not_called()

    def test_missing_mac_or_invalid_arguments_cannot_disable(self):
        for host, action, mac in (("all", "disable", "aa:bb:cc:dd:ee:ff"),
                                  ("thinman25", "delete", "aa:bb:cc:dd:ee:ff"),
                                  ("thinman25", "disable", None)):
            with self.subTest(host=host, action=action, mac=mac):
                self.data.hget.return_value = mac
                with self.assertRaises((ValueError, RuntimeError)):
                    node_action(self.data, host, action)
        self.data.pipeline.assert_not_called()

    def test_redis_failure_propagates(self):
        self.pipe.execute.side_effect = RuntimeError("Redis unavailable")
        with self.assertRaisesRegex(RuntimeError, "Redis unavailable"):
            node_action(self.data, "thinman25", "disable")


class RemovalPathTests(unittest.TestCase):
    def test_root_and_unrelated_media_mounts_are_allowed(self):
        check_paths([Path("/opt/jerry/current")],
                    [{"target": "/"}, {"target": "/watch"}])

    def test_mount_at_above_or_under_app_is_rejected(self):
        for target in ("/opt", "/opt/jerry", "/opt/jerry/current", "/opt/jerry/current/media"):
            with self.subTest(target=target), self.assertRaises(RuntimeError):
                check_paths([Path("/opt/jerry/current")], [{"target": target}])

    def test_symlink_in_parent_or_leaf_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "target").mkdir()
            (root / "linked").symlink_to(root / "target")
            for path in (root / "linked", root / "linked" / "current"):
                with self.subTest(path=path), self.assertRaises(RuntimeError):
                    check_paths([path], [{"target": "/"}])


class GuardContainerTests(unittest.TestCase):
    def test_timeout_cleans_only_its_own_container(self):
        with tempfile.TemporaryDirectory() as tmp:
            migration = repurpose.Migration("thinman25", Path(tmp))
            migration.compose = Mock(side_effect=subprocess.TimeoutExpired("compose", 120))
            with patch.object(repurpose.subprocess, "run") as cleanup:
                cleanup.return_value = subprocess.CompletedProcess([], 0, "", "")
                with self.assertRaises(subprocess.TimeoutExpired):
                    migration.guard("check")
            args = migration.compose.call_args.args
            name = args[args.index("--name") + 1]
            self.assertRegex(name, r"^thinvids-retirement-thinman25-[0-9a-f]{12}$")
            self.assertEqual(cleanup.call_args.args[0], ["docker", "rm", "--force", name])

    def test_success_uses_auto_remove_without_forced_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            migration = repurpose.Migration("thinman25", Path(tmp))
            migration.compose = Mock(return_value='{"host":"thinman25","disabled":false}')
            with patch.object(repurpose.subprocess, "run") as cleanup:
                self.assertFalse(migration.guard("check")["disabled"])
                cleanup.assert_not_called()
            self.assertIn("--rm", migration.compose.call_args.args)


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.inventory = self.root / "ansible_hosts.ini"
        self.original = b"[thinman_workers]\nthinman01\nthinman02\n"
        self.inventory.write_bytes(self.original)
        self.repo_patch = patch.object(repurpose, "REPO", self.root)
        self.repo_patch.start()
        self.validation_patch = patch.object(repurpose, "validate_inventory")
        self.validation_patch.start()
        self.migration = repurpose.Migration("thinman01", self.state)
        self.migration.compose = Mock()
        self.migration.guard = Mock(side_effect=lambda action: {
            "host": "thinman01", "mac": "aa:bb:cc:dd:ee:ff", "disabled": action == "disable"})
        self.migration.wake = Mock()
        self.migration.play = Mock()

    def tearDown(self):
        self.validation_patch.stop()
        self.repo_patch.stop()
        self.tmp.cleanup()

    def test_success_does_not_stop_controllers_and_removes_inventory_before_wake(self):
        def wake(mac):
            self.assertEqual(self.migration.guard.call_args.args, ("disable",))
            self.assertIn(b"[inference_workers]\nthinman01", self.inventory.read_bytes())
        self.migration.wake.side_effect = wake
        self.migration.retire()
        self.migration.compose.assert_not_called()
        self.migration.play.assert_called_once_with("retire")
        self.assertEqual(self.migration.journal["phase"], "retired")
        self.assertEqual((self.state / "thinman01.inventory.before").read_bytes(), self.original)

    def test_disable_failure_does_not_wake_or_uninstall(self):
        self.migration.guard.side_effect = [dict(mac="aa:bb:cc:dd:ee:ff"), RuntimeError("Redis failed")]
        with self.assertRaises(RuntimeError):
            self.migration.retire()
        self.migration.wake.assert_not_called()
        self.migration.play.assert_not_called()
        self.assertEqual(self.inventory.read_bytes(), self.original)

    def test_false_disable_confirmation_does_not_wake(self):
        self.migration.guard.side_effect = None
        self.migration.guard.return_value = dict(disabled=False)
        with self.assertRaisesRegex(RuntimeError, "not confirmed"):
            self.migration.retire()
        self.migration.wake.assert_not_called()

    def test_retirement_failure_leaves_node_out_of_rotation(self):
        self.migration.play.side_effect = RuntimeError("SSH failed")
        with self.assertRaises(RuntimeError):
            self.migration.retire()
        self.assertIn(b"[inference_workers]\nthinman01", self.inventory.read_bytes())
        self.assertEqual([c.args[0] for c in self.migration.guard.call_args_list], ["check", "disable"])
        self.migration.compose.assert_not_called()
        self.assertFalse(self.migration.recovery_path.exists())

    def test_retry_is_idempotent_and_preserves_original_inventory_backup(self):
        self.migration.retire()
        moved = self.inventory.read_bytes()
        self.migration.retire()
        self.assertEqual(self.inventory.read_bytes(), moved)
        self.assertEqual((self.state / "thinman01.inventory.before").read_bytes(), self.original)

    def test_legacy_recovery_restored_only_after_removal(self):
        repurpose.save_json(self.migration.recovery_path,
                            {"host": "thinman01", "running": ["watcher", "manager"], "safe_to_restore": False})
        self.migration.play.side_effect = lambda tag: self.migration.compose.assert_not_called()
        self.migration.retire()
        self.migration.compose.assert_called_once_with("start", "manager", "watcher")
        self.assertFalse(self.migration.recovery_path.exists())

    def test_failed_legacy_recovery_does_not_restore_controllers(self):
        repurpose.save_json(self.migration.recovery_path,
                            {"host": "thinman01", "running": ["manager"], "safe_to_restore": False})
        self.migration.play.side_effect = RuntimeError("SSH failed")
        with self.assertRaises(RuntimeError):
            self.migration.retire()
        self.migration.compose.assert_not_called()
        self.assertFalse(json.loads(self.migration.recovery_path.read_text())["safe_to_restore"])

    def test_concurrent_inventory_change_is_not_overwritten(self):
        def guard(action):
            if action == "disable":
                self.inventory.write_bytes(b"user edited inventory\n")
            return dict(mac="aa:bb:cc:dd:ee:ff", disabled=action == "disable")
        self.migration.guard.side_effect = guard
        with self.assertRaisesRegex(RuntimeError, "Inventory changed"):
            self.migration.retire()
        self.assertEqual(self.inventory.read_bytes(), b"user edited inventory\n")
        self.migration.wake.assert_not_called()

    def test_different_host_cannot_consume_recovery_record(self):
        repurpose.save_json(self.migration.recovery_path,
                            {"host": "thinman02", "running": ["manager"], "safe_to_restore": False})
        with self.assertRaisesRegex(RuntimeError, "thinman02"):
            self.migration.execute()
        self.migration.compose.assert_not_called()


if __name__ == "__main__":
    unittest.main()
