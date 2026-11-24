#!/usr/bin/env python3
"""Run the upstream repair through the pilot's scoped ARM connection."""

import base64
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

VM = ("https://management.azure.com/subscriptions/59e703b0-a79b-49da-9f12-c1604e2ffc11"
      "/resourceGroups/rg-t3-private-pilot-canadacentral/providers/Microsoft.Compute"
      "/virtualMachines/t3-private-pilot")
API = "api-version=2025-04-01"
CHUNK = 5600
MAX_PACK = 128 * 1024
SHA = r"[0-9a-f]{40}"


def request(url, token, method="GET", body=None, missing_ok=False, timeout=25):
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            data = response.read()
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        if missing_ok and error.code == 404:
            return None
        raise RuntimeError(f"Azure returned HTTP {error.code} for {method}.") from None
    except (OSError, ValueError) as error:
        raise RuntimeError(f"Azure {method} did not return a usable response ({type(error).__name__}).") from None


def wait_for(description, seconds, check):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(4)
    raise RuntimeError(f"Timed out waiting for {description}.")


class Pilot:
    def __init__(self, directory, name):
        self.directory = directory
        self.name = name
        self.commands = directory / "pilot-commands"
        self.start_requested = directory / "pilot-start-requested"
        self.deallocated = directory / "pilot-deallocated"
        self.guest_workspace_requested = directory / "pilot-guest-workspace-requested"
        self.guest_workspace_cleaned = directory / "pilot-guest-workspace-cleaned"
        self.arm_token = subprocess.check_output(
            ["az", "account", "get-access-token", "--resource", "https://management.azure.com/",
             "--query", "accessToken", "--output", "tsv", "--only-show-errors"],
            text=True, timeout=30,
        ).strip()

    def api(self, path, method="GET", body=None, *, expand=False, missing_ok=False, timeout=25):
        url = f"{VM}/{path}?{API}" + ("&%24expand=instanceView" if expand else "")
        return request(url, self.arm_token, method, body, missing_ok, timeout)

    def power(self):
        view = self.api("instanceView")
        return next((item["code"] for item in view["statuses"]
                     if item["code"].startswith("PowerState/")), "unknown")

    def boot(self):
        power = self.power()
        print(f"Initial pilot state {power}", flush=True)
        if power != "PowerState/deallocated":
            raise RuntimeError("The pilot is in use. Release it and verify deallocation before retrying.")
        self.start_requested.touch()
        print("Starting the pilot VM.", flush=True)
        self.api("start", "POST")
        wait_for("pilot startup", 180, lambda: self.power() == "PowerState/running")

        def guest_ready():
            statuses = self.api("instanceView").get("vmAgent", {}).get("statuses", [])
            return any(item.get("displayStatus") == "Ready" for item in statuses)

        wait_for("the Azure guest agent", 120, guest_ready)

    def run(self, suffix, script, seconds, parameters, protected=None):
        name = f"{self.name}-{suffix}"
        path = f"runCommands/{name}"
        with self.commands.open("a") as stream:
            print(name, file=stream)
        body = {
            "location": "canadacentral",
            "properties": {
                "source": {"script": script},
                "timeoutInSeconds": seconds,
                "asyncExecution": True,
                "parameters": [{"name": name, "value": value} for name, value in parameters.items()],
                "protectedParameters": [{"name": name, "value": value}
                                        for name, value in (protected or {}).items()],
            },
        }
        def finished():
            resource = self.api(path, expand=True)
            view = resource.get("properties", {}).get("instanceView") or {}
            state = view.get("executionState")
            if state == "Succeeded" and view.get("exitCode") == 0:
                return view
            if state in {"Succeeded", "Failed", "TimedOut", "Canceled", "Cancelled"}:
                detail = re.search(r"^T3_REPAIR_ERROR [a-z0-9 -]+$", view.get("output") or "", re.M)
                message = detail[0] if detail else "No guest diagnostic was returned"
                raise RuntimeError(f"Pilot command {suffix} ended with {state}: {message}.")
            return None

        try:
            self.api(path, "PUT", body)
            return wait_for(f"pilot command {suffix}", seconds + 35, finished)
        finally:
            try:
                self.api(path, "DELETE", missing_ok=True, timeout=5)
            except RuntimeError as error:
                print(f"Will retry deletion of {name} during cleanup. {error}", flush=True)

    def drain_commands(self, seconds):
        if not self.commands.exists():
            return
        pending = self.commands.read_text().splitlines()
        requested = set()
        deadline = time.monotonic() + seconds
        while pending and time.monotonic() < deadline:
            for name in pending[:]:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                path = f"runCommands/{name}"
                try:
                    resource = self.api(path, missing_ok=True, timeout=min(5, remaining))
                    if resource is None:
                        pending.remove(name)
                    elif name not in requested and resource.get("properties", {}).get("provisioningState") != "Deleting":
                        remaining = deadline - time.monotonic()
                        if remaining > 0:
                            self.api(path, "DELETE", missing_ok=True, timeout=min(5, remaining))
                            requested.add(name)
                except RuntimeError:
                    continue
            if pending:
                time.sleep(max(0, min(4, deadline - time.monotonic())))
        remaining_file = self.commands.with_suffix(".pending")
        remaining_file.write_text("".join(f"{name}\n" for name in pending))
        remaining_file.replace(self.commands)
        if pending:
            raise RuntimeError("Could not verify Run Command deletion: " + ", ".join(pending))

    def cleanup(self, script):
        if not self.start_requested.exists():
            return
        errors = []
        try:
            self.drain_commands(60)
        except RuntimeError as error:
            errors.append(str(error))
        needs_guest_cleanup = (
            self.guest_workspace_requested.exists() and not self.guest_workspace_cleaned.exists()
        )
        if self.deallocated.exists():
            if needs_guest_cleanup:
                errors.append("Pilot guest workspace cleanup could not be verified.")
            if errors:
                raise RuntimeError("Pilot cleanup reported: " + " ".join(errors))
            return
        try:
            # A pending repair command could recreate the workspace after cleanup.
            if needs_guest_cleanup and not errors:
                if self.power() != "PowerState/running":
                    raise RuntimeError("Pilot guest workspace cleanup could not be verified.")
                self.run("cleanup", script, 30, {"T3_SYNC_NAME": self.name, "T3_SYNC_ACTION": "cleanup"})
                self.guest_workspace_cleaned.touch()
        except RuntimeError as error:
            errors.append(str(error))
        finally:
            try:
                self.drain_commands(40)
            except RuntimeError as error:
                errors.append(str(error))
            print("Deallocating the pilot VM.", flush=True)
            requested = False

            def deallocate():
                nonlocal requested
                power = self.power()
                if requested and power == "PowerState/deallocated":
                    return True
                if not requested and power != "PowerState/deallocating":
                    try:
                        self.api("deallocate", "POST")
                    except RuntimeError:
                        return False
                    requested = True
                elif power == "PowerState/deallocating":
                    requested = True
                return False

            wait_for("pilot deallocation", 150, deallocate)
            self.deallocated.touch()
            print("Verified VM state PowerState/deallocated.", flush=True)
        if errors:
            raise RuntimeError("Pilot cleanup reported: " + " ".join(errors))


def download_url():
    project = os.environ["SYSTEM_TEAMPROJECTID"]
    pipeline = os.environ["SYSTEM_DEFINITIONID"]
    run = os.environ["BUILD_BUILDID"]
    attempt = os.environ["SYSTEM_JOBATTEMPT"]
    url = (f"{os.environ['SYSTEM_COLLECTIONURI']}{project}/_apis/pipelines/{pipeline}"
           f"/runs/{run}/artifacts?artifactName=upstream-repair-input-{attempt}&%24expand=signedContent"
           "&api-version=7.1-preview.1")
    return request(url, os.environ["SYSTEM_ACCESSTOKEN"])["signedContent"]["url"]


def collect(pilot, script, view, mode):
    lines = (view.get("output") or "").strip().splitlines()
    if mode == "repair":
        if len(lines) != 2 or not re.fullmatch(r"T3_TURN [\w:%.-]+ [\w:%.-]+", lines[0], re.ASCII):
            raise RuntimeError("The pilot did not confirm a completed T3 turn.")
        print(f"Completed T3 turn {lines.pop(0).removeprefix('T3_TURN ')}.", flush=True)
    header = re.fullmatch(rf"T3_REPAIR_OK ({SHA}) ([0-9a-f]{{64}}) ([0-9]{{1,6}})", "\n".join(lines))
    if not header:
        raise RuntimeError("The pilot did not return candidate metadata.")
    commit, checksum, size_text = header.groups()
    size = int(size_text)
    if not 0 < size <= MAX_PACK:
        raise RuntimeError(f"The repair exceeds the {MAX_PACK}-byte Git pack limit.")

    print(f"Retrieving {size} bytes for candidate {commit}.", flush=True)
    deadline = time.monotonic() + 480
    pack = bytearray()
    for chunk, offset in enumerate(range(0, size, CHUNK)):
        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out retrieving the pilot's candidate.")
        result = pilot.run(f"part-{chunk}", script, 20, {
            "T3_SYNC_NAME": pilot.name,
            "T3_SYNC_ACTION": "read",
            "T3_SYNC_PART": str(chunk),
        })
        for channel in ("output", "error"):
            pack.extend(base64.b64decode((result.get(channel) or "").strip(), validate=True))
        if len(pack) != min(offset + CHUNK, size):
            raise RuntimeError("The pilot returned a truncated Git pack.")
    if hashlib.sha256(pack).hexdigest() != checksum:
        raise RuntimeError("The pilot Git pack checksum does not match.")
    (pilot.directory / "repaired.pack").write_bytes(pack)
    (pilot.directory / "repaired.commit").write_text(f"{commit}\n")


def interrupted(_signum, _frame):
    raise InterruptedError("The sync was interrupted.")


def main():
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    action, = sys.argv[1:]
    directory = Path(os.environ["SYNC_DIR"])
    name = f"t3-upstream-{os.environ['BUILD_BUILDID']}-{os.environ['SYSTEM_JOBATTEMPT']}"
    if not re.fullmatch(r"t3-upstream-[0-9]+-[0-9]+", name):
        raise RuntimeError("Invalid build or job attempt ID.")
    script = Path(__file__).with_name("upstream-sync-guest.sh").read_text()
    pilot = Pilot(directory, name)
    if action == "repair":
        base, upstream, baseline = (directory / "inputs").read_text().splitlines()
        checksum = (directory / "input.sha256").read_text().strip()
        try:
            pilot.boot()
            print("Submitting the exact merge inputs to the pilot.", flush=True)
            mode = os.environ["TRIAL_MODE"]
            download = download_url()
            pilot.guest_workspace_requested.touch()
            view = pilot.run("repair", script, 1570, {
                "T3_SYNC_NAME": name, "T3_SYNC_ACTION": "repair",
                "T3_SYNC_BASE": base, "T3_SYNC_UPSTREAM": upstream, "T3_SYNC_BASELINE": baseline,
                "T3_SYNC_CHECKSUM": checksum, "T3_SYNC_MODE": mode,
            }, {"T3_SYNC_DOWNLOAD": download})
            collect(pilot, script, view, mode)
        except Exception:
            try:
                pilot.cleanup(script)
            except (RuntimeError, OSError) as error:
                print(f"##vso[task.logissue type=error]{error}", flush=True)
            raise
        else:
            pilot.cleanup(script)
    elif action == "cleanup":
        pilot.cleanup(script)
    else:
        raise RuntimeError("Unknown pilot operation.")
    print("##vso[task.setvariable variable=pilotClean]true", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(f"##vso[task.logissue type=error]{error}", flush=True)
        sys.exit(1)
