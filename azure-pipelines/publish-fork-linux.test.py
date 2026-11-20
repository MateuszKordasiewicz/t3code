import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


PUBLISHER = Path(__file__).with_name("publish-fork-linux.sh")
OLD = "v0.1.11-nightly.20261120.17"
NEW = "v0.1.11-nightly.20261120.1000001552"
FEED = "update-feed"
IMAGE = "T3-Code-Nightly-x86_64.AppImage"
MANIFEST = "nightly-linux.yml"


def fake_gh():
    state_file = Path(os.environ["FAKE_GH_STATE"])
    state = json.loads(state_file.read_text())
    args = sys.argv[1:]
    if args[0] == "api":
        if "/compare/" in args[1]:
            print(state["compare"])
        elif args[1] == "--paginate":
            print(json.dumps([list(state["releases"].values())]))
        elif "/git/refs" in args[3]:
            ref, commit = [field.split("=", 1)[1] for field in args[5:8:2]]
            subprocess.run(["git", "update-ref", ref, commit], cwd=state["bare"], check=True)
        elif args[2] == "DELETE":
            release_id = int(args[3].rsplit("/", 1)[1])
            tag = next(tag for tag, data in state["releases"].items() if data["id"] == release_id)
            del state["releases"][tag]
        else:
            tag = args[1].rsplit("/", 1)[1]
            print(json.dumps({"tag_name": tag, "name": tag, "body": "Notes"}))
    else:
        _, action, tag, *flags = args
        if action == "create":
            assert tag not in state["releases"]
            state["creates"] += 1
            state["releases"][tag] = {
                "id": state["creates"],
                "tag_name": tag,
                "draft": "--draft" in flags,
                "prerelease": True,
                "target_commitish": flags[flags.index("--target") + 1] if "--target" in flags else "main",
                "assets": [
                    {"name": Path(arg).name, "state": "uploaded"}
                    for arg in flags if Path(arg).is_file()
                ],
            }
        elif action == "edit":
            state["releases"][tag]["draft"] = False
        elif action == "download":
            if state.get("fail_download"):
                sys.exit(1)
            print(json.dumps(state["pointer"]))
        elif action == "upload":
            state["releases"][tag]["assets"] = [{"name": "nightly.json", "state": "uploaded"}]
            state["pointer"] = json.loads(Path(flags[0]).read_text())
            state["uploads"] += 1
        else:
            raise AssertionError(args)
    state_file.write_text(json.dumps(state))


class PublisherTest(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bare = self.root / "remote"
        self.git("init", "--bare", "--quiet", self.bare)
        self.git("init", "--quiet", self.root)
        for message in ("old", "new"):
            self.git("commit", "--quiet", "--allow-empty", "-m", message)
        self.old_sha = self.git("rev-parse", "HEAD^")
        self.new_sha = self.git("rev-parse", "HEAD")
        self.git("push", "--quiet", self.bare, "HEAD:refs/heads/main")
        (self.root / "gh").write_text(f'#!/usr/bin/env bash\nexec python3 "{Path(__file__).absolute()}" --fake-gh "$@"\n')
        (self.root / "gh").chmod(0o755)
        self.state_file = self.root / "state.json"
        self.state_file.write_text(json.dumps({
            "bare": str(self.bare), "compare": "ahead", "creates": 0,
            "releases": {}, "pointer": None, "uploads": 0,
        }))

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-C", self.root, "-c", "user.name=Publisher test",
             "-c", "user.email=test@example.invalid", *args],
            text=True,
        ).strip()

    def read_state(self):
        return json.loads(self.state_file.read_text())

    def publish(self, tag=NEW, sha=None, run="azure-1552", succeeds=True):
        assets = self.root / "assets"
        assets.mkdir(exist_ok=True)
        (assets / IMAGE).write_text("AppImage fixture")
        (assets / MANIFEST).write_text(f"version: {tag[1:]}\n")
        with (assets / "SHA256SUMS").open("w") as checksums:
            subprocess.run(["sha256sum", IMAGE, MANIFEST], cwd=assets, stdout=checksums, check=True)
        env = os.environ | {
            "PATH": f"{self.root}:{os.environ['PATH']}",
            "FAKE_GH_STATE": str(self.state_file),
            "GH_REPO": "example/fork",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": f"url.{self.bare}.insteadOf",
            "GIT_CONFIG_VALUE_0": "https://github.com/example/fork.git",
        }
        result = subprocess.run(
            ["bash", PUBLISHER, tag, sha or self.new_sha, run, assets],
            env=env, capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode == 0, succeeds, result.stdout + result.stderr)
        self.assertEqual(self.git("ls-remote", self.bare, "refs/heads/fork-linux-publish-lock"), "")
        return self.read_state()

    def test_out_of_order_and_retry_keep_highest_pointer(self):
        state = self.publish()
        self.assertEqual(state["pointer"]["tag_name"], NEW)
        self.assertEqual(self.git("ls-remote", self.bare, f"refs/tags/{NEW}").split()[0], self.new_sha)
        state = self.publish(OLD, self.old_sha, "github-17")
        self.assertEqual(state["pointer"]["tag_name"], NEW)
        self.assertEqual(state["uploads"], 1)
        self.assertEqual(self.publish()["creates"], state["creates"])

        # A failed overwrite may remove nightly.json before the replacement uploads.
        state["releases"][FEED]["assets"] = []
        state["pointer"] = None
        self.state_file.write_text(json.dumps(state))
        self.assertEqual(self.publish(OLD, self.old_sha, "github-17")["pointer"]["tag_name"], NEW)

    def test_recover_draft_feed_without_overwriting_after_download_error(self):
        state = self.publish()
        state["releases"][FEED]["draft"] = True
        state["pointer"] = {"tag_name": "invalid"}
        state["fail_download"] = True
        self.state_file.write_text(json.dumps(state))
        self.assertEqual(self.publish(succeeds=False)["uploads"], state["uploads"])

        state["fail_download"] = False
        self.state_file.write_text(json.dumps(state))
        recovered = self.publish()
        self.assertFalse(recovered["releases"][FEED]["draft"])
        self.assertEqual(recovered["pointer"]["tag_name"], NEW)

    def test_reject_unmirrored_commit_and_tag_collision(self):
        state = self.read_state()
        state["compare"] = "behind"
        self.state_file.write_text(json.dumps(state))
        self.assertEqual(self.publish(succeeds=False)["releases"], {})
        state["compare"] = "ahead"
        self.state_file.write_text(json.dumps(state))
        self.git("push", "--quiet", self.bare, f"{self.old_sha}:refs/tags/{NEW}")
        self.assertEqual(self.publish(succeeds=False)["releases"], {})


if __name__ == "__main__":
    if sys.argv[1:2] == ["--fake-gh"]:
        sys.argv.pop(1)
        fake_gh()
    else:
        unittest.main()
