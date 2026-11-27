"""Focused tests for the Azure upstream sync entry points using disposable Git repos."""

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("upstream-sync.sh").resolve()


class SyncTest(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix="t3-upstream-test-")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        self.pull_requests = self.root / "pull-requests.json"
        self.pull_requests.write_text('{"value": []}\n')
        curl = fake_bin / "curl"
        curl.write_text(f'#!/bin/sh\ncat "{self.pull_requests}"\n')
        curl.chmod(0o755)
        self.repo = self.root / "hosted"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Upstream sync test")
        self.git("config", "user.email", "sync-test@example.com")
        self.origin = self.root / "azure.git"
        self.upstream = self.root / "upstream.git"
        self.git("init", "--bare", "-q", str(self.origin))
        self.git("init", "--bare", "-q", str(self.upstream))
        self.git("remote", "add", "origin", str(self.origin))
        self.git("config", f"url.{self.upstream}.insteadOf", "https://github.com/pingdotgg/t3code.git")
        self.common = self.commit("common\n")
        self.base = self.commit("azure\n")
        self.git("push", "origin", "HEAD:refs/heads/main", "HEAD:refs/heads/sync/azure-upstream-trial/fixture")
        self.git("checkout", "--detach", self.common)
        self.source = self.commit("upstream\n")
        self.git("push", str(self.upstream), "HEAD:refs/heads/main")
        self.scratch = self.root / "sync"
        self.env = {
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "BUILD_REASON": "Manual",
            "BUILD_SOURCEBRANCH": "refs/heads/slice",
            "SYSTEM_ACCESSTOKEN": "test",
            "DRY_RUN": "false",
            "TRIAL_TARGET": "fixture",
            "TRIAL_BASE": self.base,
            "TRIAL_UPSTREAM": self.source,
            "TRIAL_MODE": "repair",
            "SYNC_DIR": str(self.scratch),
            "BUILD_BUILDID": "42",
            "BUILD_REPOSITORY_ID": "test-repo",
            "SYSTEM_COLLECTIONURI": "https://dev.azure.com/test/",
            "SYSTEM_TEAMPROJECTID": "test-project",
            "SYSTEM_JOBATTEMPT": "1",
        }

    def git(self, *args, cwd=None, **kwargs):
        return subprocess.run(
            ["git", *args], cwd=cwd or self.repo, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, **kwargs,
        ).stdout

    def commit(self, contents):
        (self.repo / "conflict.txt").write_text(contents)
        self.git("add", "conflict.txt")
        self.git("commit", "-qm", contents.strip())
        return self.git("rev-parse", "HEAD").decode().strip()

    def sync(self, step, **env):
        return subprocess.run(
            ["bash", str(SCRIPT), step], cwd=self.repo, env={**self.env, **env},
            capture_output=True, text=True,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def prepare_conflict(self):
        result = self.sync("prepare")
        self.assert_success(result)
        self.assertIn("variable=repairNeeded]true", result.stdout)
        self.assertIn("conflict.txt", result.stdout)
        self.assertEqual(self.git("ls-remote", "origin", "refs/heads/sync/azure-upstream/*"), b"")
        bundle = self.scratch / "artifact" / "input.bundle"
        self.assertEqual(hashlib.sha256(bundle.read_bytes()).hexdigest(),
                         (self.scratch / "input.sha256").read_text().strip())
        return (self.scratch / "inputs").read_text().splitlines()[2]

    def return_candidate(self, baseline, contents, parents):
        guest = self.root / "guest"
        guest.mkdir(exist_ok=True)
        self.git("init", "-q", cwd=guest)
        self.git("config", "user.name", "Pilot test", cwd=guest)
        self.git("config", "user.email", "pilot-test@example.com", cwd=guest)
        self.git("fetch", str(self.scratch / "artifact" / "input.bundle"), "refs/t3-upstream/input", cwd=guest)
        self.git("checkout", "--detach", self.base, cwd=guest)
        (guest / "conflict.txt").write_text(contents)
        self.git("add", "conflict.txt", cwd=guest)
        tree = self.git("write-tree", cwd=guest).decode().strip()
        args = [arg for parent in parents for arg in ("-p", parent)]
        commit = self.git("commit-tree", tree, *args, "-m", "repair", cwd=guest).decode().strip()
        transfer = self.git("commit-tree", tree, "-p", baseline, "-m", "transfer", cwd=guest).decode().strip()
        pack = self.git("pack-objects", "--stdout", "--revs", "--thin",
                        input=f"{commit}\n{transfer}\n^{baseline}\n".encode(), cwd=guest)
        (self.scratch / "repaired.pack").write_bytes(pack)
        (self.scratch / "repaired.commit").write_text(commit + "\n")
        return commit

    def test_conflict_dry_run_and_disabled_schedule_do_not_prepare_a_repair(self):
        for env in ({"DRY_RUN": "true"}, {
            "BUILD_REASON": "Schedule", "BUILD_SOURCEBRANCH": "refs/heads/main",
            "TRIAL_TARGET": "none", "TRIAL_BASE": "none", "TRIAL_UPSTREAM": "none",
            "VM_REPAIR_ENABLED": "false",
            "CRON_SCHEDULE": subprocess.check_output(["date", "+%Z"],
                                                       env={**os.environ, "TZ": "America/New_York"},
                                                       text=True).strip().replace(
                                                           "EDT", "Eastern daylight time").replace(
                                                           "EST", "Eastern standard time"),
        }):
            result = self.sync("prepare", **env)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("Result: Conflict", result.stdout)
            self.assertFalse(self.scratch.exists())

    def test_repaired_pack_publishes_only_to_disposable_candidate(self):
        baseline = self.prepare_conflict()
        commit = self.return_candidate(baseline, "azure and upstream\n", [self.base, self.source])
        self.assert_success(self.sync("publish"))
        self.assertEqual(self.git("ls-remote", "origin", f"refs/heads/sync/azure-upstream/trial/42/{commit}"),
                         f"{commit}\trefs/heads/sync/azure-upstream/trial/42/{commit}\n".encode())
        self.assertIn(self.base.encode(), self.git("ls-remote", "origin", "refs/heads/main"))

    def test_rejects_wrong_parents_and_conflict_markers_before_push(self):
        baseline = self.prepare_conflict()
        for contents, parents, failure in (
            ("azure and upstream\n", [self.base], "exact target and upstream parents"),
            ("<<<<<<< HEAD\nazure\n=======\nupstream\n>>>>>>> upstream\n",
             [self.base, self.source], "conflict marker check"),
        ):
            self.return_candidate(baseline, contents, parents)
            result = self.sync("publish")
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn(failure, result.stdout)
        self.assertEqual(self.git("ls-remote", "origin", "refs/heads/sync/azure-upstream/*"), b"")

    def test_clean_and_current_are_hosted_only(self):
        self.git("push", "origin", f"+{self.common}:refs/heads/sync/azure-upstream-trial/fixture")
        result = self.sync("prepare", TRIAL_BASE=self.common)
        self.assert_success(result)
        self.assertIn("Clean candidate", result.stdout)
        self.assertNotIn("repairNeeded", result.stdout)
        self.assert_success(self.sync("publish", TRIAL_BASE=self.common))
        self.git("push", "origin", f"{self.source}:refs/heads/sync/azure-upstream-trial/fixture")
        result = self.sync("prepare", SYNC_DIR=str(self.root / "current"), TRIAL_BASE=self.source)
        self.assert_success(result)
        self.assertIn("Already current", result.stdout)
        self.assertFalse((self.root / "current").exists())

    def test_ci_batch_continuation_publishes_despite_manual_dry_run_default(self):
        self.git("push", "origin", f"+{self.common}:refs/heads/main")
        self.pull_requests.write_text(json.dumps({"value": [{
            "status": "active", "isDraft": True, "targetRefName": "refs/heads/main",
            "sourceRefName": "refs/heads/sync/azure-upstream/old-proof",
        }]}))
        production = {
            "BUILD_REASON": "BatchedCI", "BUILD_SOURCEBRANCH": "refs/heads/main",
            "DRY_RUN": "false", "TRIAL_TARGET": "none", "TRIAL_BASE": "none",
            "TRIAL_UPSTREAM": "none",
        }
        result = self.sync("prepare", **{**production, "DRY_RUN": "true"})
        self.assert_success(result)
        self.assertIn("Clean candidate", result.stdout)
        self.assertTrue((self.scratch / "inputs").exists())

    def test_ci_conflict_uses_the_worker_when_repair_is_enabled(self):
        result = self.sync(
            "prepare", BUILD_REASON="BatchedCI", BUILD_SOURCEBRANCH="refs/heads/main",
            DRY_RUN="true", TRIAL_TARGET="none", TRIAL_BASE="none",
            TRIAL_UPSTREAM="none", VM_REPAIR_ENABLED="true",
        )
        self.assert_success(result)
        self.assertIn("variable=repairNeeded]true", result.stdout)

    def test_pending_upstream_sync_pr_stops_a_new_main_batch(self):
        self.pull_requests.write_text(json.dumps({"value": [
            {"status": "active", "targetRefName": "refs/heads/main",
             "sourceRefName": "refs/heads/feature/unrelated"},
            {"status": "active", "targetRefName": "refs/heads/main",
             "sourceRefName": "refs/heads/sync/azure-upstream/pending"},
        ]}))
        result = self.sync(
            "prepare", BUILD_REASON="BatchedCI", BUILD_SOURCEBRANCH="refs/heads/main",
            TRIAL_TARGET="none", TRIAL_BASE="none", TRIAL_UPSTREAM="none",
        )
        self.assert_success(result)
        self.assertIn("an upstream sync PR is already pending", result.stdout)
        self.assertFalse(self.scratch.exists())

    def test_production_selects_fifty_commits_and_stops_after_four_merged_batches(self):
        self.git("checkout", "--detach", self.common)
        upstream_commits = []
        for number in range(201):
            (self.repo / "upstream.txt").write_text(f"change {number}\n")
            self.git("add", "upstream.txt")
            self.git("commit", "-qm", f"upstream {number}")
            upstream_commits.append(self.git("rev-parse", "HEAD").decode().strip())
        self.git("push", "--force", str(self.upstream), "HEAD:refs/heads/main")

        base = self.base
        for index in range(4):
            run_dir = self.root / f"batch-{index}"
            result = self.sync(
                "prepare", BUILD_SOURCEBRANCH="refs/heads/main", DRY_RUN="false",
                TRIAL_TARGET="none", TRIAL_BASE="none", TRIAL_UPSTREAM="none",
                SYNC_DIR=str(run_dir),
            )
            self.assert_success(result)
            candidate_base, selected, candidate = (run_dir / "inputs").read_text().splitlines()
            self.assertEqual(candidate_base, base)
            self.assertEqual(selected, upstream_commits[(index + 1) * 50 - 1])
            tree = self.git("rev-parse", f"{candidate}^{{tree}}").decode().strip()
            if index == 0:
                candidate = self.git(
                    "commit-tree", tree, "-p", base, "-p", selected,
                    "-m", "chore: repair T3 Code upstream merge",
                ).decode().strip()
            merge = self.git("commit-tree", tree, "-p", base, "-p", candidate,
                             "-m", "Merge protected upstream PR").decode().strip()
            self.git("push", "origin", f"{merge}:refs/heads/main")
            base = merge

        capped = self.sync(
            "prepare", BUILD_SOURCEBRANCH="refs/heads/main", DRY_RUN="false",
            TRIAL_TARGET="none", TRIAL_BASE="none", TRIAL_UPSTREAM="none",
            SYNC_DIR=str(self.root / "capped"),
        )
        self.assert_success(capped)
        self.assertIn("daily cap of four merged upstream batches reached", capped.stdout)
        self.assertFalse((self.root / "capped").exists())

    def test_disposable_target_selects_only_first_fifty_pinned_commits(self):
        self.git("checkout", "--detach", self.common)
        upstream_commits = []
        for number in range(101):
            (self.repo / "upstream.txt").write_text(f"change {number}\n")
            self.git("add", "upstream.txt")
            self.git("commit", "-qm", f"upstream {number}")
            upstream_commits.append(self.git("rev-parse", "HEAD").decode().strip())
        self.git("push", "--force", str(self.upstream), "HEAD:refs/heads/main")
        result = self.sync("prepare", TRIAL_UPSTREAM=upstream_commits[-1])
        self.assert_success(result)
        self.assertEqual((self.scratch / "inputs").read_text().splitlines()[1],
                         upstream_commits[49])

    def test_pilot_reservation_is_held_until_deallocation_is_verified(self):
        self.scratch.mkdir()
        self.assert_success(self.sync("acquire-pilot"))
        result = self.sync("acquire-pilot", BUILD_BUILDID="43")
        self.assertEqual(result.returncode, 1)
        self.assertIn("reserved by run 42", result.stdout)
        (self.scratch / "pilot-start-requested").touch()
        result = self.sync("release-pilot")
        self.assertEqual(result.returncode, 1)
        self.assertIn("deallocation could not be verified", result.stdout)
        (self.scratch / "pilot-deallocated").touch()
        commands = self.scratch / "pilot-commands"
        commands.write_text("t3-upstream-42-1-cleanup\n")
        result = self.sync("release-pilot")
        self.assertEqual(result.returncode, 1)
        self.assertIn("Run Command deletion could not be verified", result.stdout)
        commands.write_text("")
        self.assert_success(self.sync("release-pilot"))

        self.assert_success(self.sync("acquire-pilot", BUILD_BUILDID="43"))

    def test_pilot_fallback_cannot_release_after_unconfirmed_guest_cleanup(self):
        self.scratch.mkdir()
        self.assert_success(self.sync("acquire-pilot"))
        for marker in ("pilot-start-requested", "pilot-guest-workspace-requested"):
            (self.scratch / marker).touch()

        spec = importlib.util.spec_from_file_location(
            "upstream_sync_vm", SCRIPT.with_name("upstream-sync-vm.py"),
        )
        vm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(vm)
        with patch("subprocess.check_output", return_value="test"):
            pilot = vm.Pilot(self.scratch, "t3-upstream-42-1")

        power = "PowerState/running"

        def api(path, method):
            nonlocal power
            self.assertEqual((path, method), ("deallocate", "POST"))
            power = "PowerState/deallocated"

        with (
            patch.object(pilot, "power", side_effect=lambda: power),
            patch.object(pilot, "api", side_effect=api),
            patch.object(pilot, "drain_commands", side_effect=[RuntimeError("repair still running"), None]),
            patch.object(pilot, "run") as guest_cleanup,
            patch.object(vm, "wait_for", side_effect=lambda _, __, check: check() or check()),
        ):
            with self.assertRaisesRegex(RuntimeError, "repair still running"):
                pilot.cleanup("unused")
            guest_cleanup.assert_not_called()
        self.assertTrue((self.scratch / "pilot-deallocated").exists())

        with self.assertRaisesRegex(RuntimeError, "guest workspace cleanup could not be verified"):
            pilot.cleanup("unused")

        result = self.sync("release-pilot")
        self.assertEqual(result.returncode, 1)
        self.assertIn("guest workspace cleanup could not be verified", result.stdout)

        (self.scratch / "pilot-guest-workspace-cleaned").touch()
        pilot.cleanup("unused")
        self.assert_success(self.sync("release-pilot"))


class WorkerRefreshTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "upstream_sync_vm", SCRIPT.with_name("upstream-sync-vm.py"),
        )
        cls.vm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.vm)

    def test_pilot_artifact_is_a_verified_successful_main_build(self):
        archive = b"verified T3 archive"
        info = {
            "branch": "main", "commit": "a" * 40,
            "runUrl": "https://dev.azure.com/Instablinds/t3code/_build/results?buildId=42",
            "version": "0.1.12-nightly.20261127.42",
        }
        info_bytes = json.dumps(info).encode()
        archive_name = f't3-{info["version"]}-linux-x64.tar.gz'
        manifest = (f'{hashlib.sha256(archive).hexdigest()}  {archive_name}\n'
                    f'{hashlib.sha256(info_bytes).hexdigest()}  build-info.json\n').encode()
        zipped = io.BytesIO()
        with zipfile.ZipFile(zipped, "w") as output:
            output.writestr(f"t3-linux-server/{archive_name}", archive)
            output.writestr("t3-linux-server/build-info.json", info_bytes)
            output.writestr("t3-linux-server/SHA256SUMS", manifest)
        build = {
            "id": 42, "sourceVersion": info["commit"], "sourceBranch": "refs/heads/main",
            "status": "completed", "result": "succeeded", "definition": {"id": 26},
        }
        environment = {
            "SYSTEM_COLLECTIONURI": "https://dev.azure.com/Instablinds/",
            "SYSTEM_TEAMPROJECTID": "project-id", "SYSTEM_TEAMPROJECT": "t3code",
            "SYSTEM_ACCESSTOKEN": "test",
        }
        with (patch.dict(os.environ, environment),
              patch.object(self.vm, "request", side_effect=[
                  {"value": [build]}, {"signedContent": {"url": "https://artifact.example/test"}},
              ]),
              patch.object(self.vm.urllib.request, "urlopen", return_value=io.BytesIO(zipped.getvalue()))):
            result = self.vm.current_worker_artifact()
        self.assertEqual(result[:5], (42, "a" * 40, archive_name,
                                      hashlib.sha256(archive).hexdigest(),
                                      hashlib.sha256(info_bytes).hexdigest()))

        wrong = manifest.replace(hashlib.sha256(archive).hexdigest().encode(), b"0" * 64)
        zipped = io.BytesIO()
        with zipfile.ZipFile(zipped, "w") as output:
            output.writestr(f"t3-linux-server/{archive_name}", archive)
            output.writestr("t3-linux-server/build-info.json", info_bytes)
            output.writestr("t3-linux-server/SHA256SUMS", wrong)
        with (patch.dict(os.environ, environment),
              patch.object(self.vm, "request", side_effect=[
                  {"value": [build]}, {"signedContent": {"url": "https://artifact.example/test"}},
              ]),
              patch.object(self.vm.urllib.request, "urlopen", return_value=io.BytesIO(zipped.getvalue()))):
            with self.assertRaisesRegex(RuntimeError, "archive checksum"):
                self.vm.current_worker_artifact()

    def test_pilot_reports_the_install_stage_that_failed(self):
        with tempfile.TemporaryDirectory() as scratch:
            with patch.object(self.vm.subprocess, "check_output", return_value="test"):
                pilot = self.vm.Pilot(Path(scratch), "t3-upstream-42-1")

            def api(_path, method="GET", _body=None, **_kwargs):
                if method == "GET":
                    return {"properties": {"instanceView": {
                        "executionState": "Failed", "exitCode": 1,
                        "output": "T3_INSTALL_ERROR download line=16 status=1\n",
                        "error": "T3_INSTALL_ERROR download_http_403\n",
                    }}}
                return {}

            with (patch.object(pilot, "api", side_effect=api),
                  patch.object(self.vm, "wait_for", side_effect=lambda _, __, check: check())):
                with self.assertRaisesRegex(RuntimeError, "T3_INSTALL_ERROR download_http_403"):
                    pilot.run("refresh", "unused", 900, {})

    def test_repair_refreshes_before_starting_a_t3_turn(self):
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch)
            (directory / "inputs").write_text("a\nb\nc\n")
            (directory / "input.sha256").write_text("d" * 64)
            events = []
            refresh_output = ["Installation complete"]

            class Pilot:
                name = "t3-upstream-42-1"

                def __init__(self, *_args):
                    self.directory = directory
                    self.guest_workspace_requested = directory / "workspace-requested"

                def boot(self):
                    events.append("boot")

                def run(self, suffix, *_args):
                    events.append(suffix)
                    return {"output": refresh_output[0] if suffix == "refresh" else ""}

                def cleanup(self, _script):
                    events.append("cleanup")

            environment = {
                "SYNC_DIR": str(directory), "BUILD_BUILDID": "42", "SYSTEM_JOBATTEMPT": "1",
                "TRIAL_MODE": "repair",
            }
            def invoke():
                with (patch.dict(os.environ, environment),
                      patch.object(self.vm, "Pilot", Pilot),
                      patch.object(self.vm, "current_worker_artifact", return_value=(
                          42, "a" * 40, "archive.tar.gz", "b" * 64, "c" * 64,
                      )),
                      patch.object(self.vm, "artifact_signed_url", side_effect=lambda _run: (
                          events.append("sign") or "https://artifact.example/test"
                      )),
                      patch.object(self.vm, "download_url", return_value="https://input.example/test"),
                      patch.object(self.vm, "collect"),
                      patch.object(self.vm.sys, "argv", ["upstream-sync-vm.py", "repair"])):
                    self.vm.main()

            invoke()
            self.assertEqual(events, ["boot", "sign", "refresh", "repair", "cleanup"])
            events.clear()
            refresh_output[0] = ""
            with self.assertRaisesRegex(RuntimeError, "did not confirm its T3 refresh"):
                invoke()
            self.assertEqual(events, ["boot", "sign", "refresh", "cleanup"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
