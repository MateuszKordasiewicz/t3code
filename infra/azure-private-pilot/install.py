#!/usr/bin/env python3
"""Install a verified Azure Pipelines main artifact on the private pilot VM."""

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

SUBSCRIPTION = "DevOps"
ORG = "https://dev.azure.com/Instablinds"
PROJECT = "t3code"
GROUP = "rg-t3-private-pilot-canadacentral"
VM = "t3-private-pilot"
COMMAND = "install-t3"
PIPELINE = "26"
ARTIFACT = "t3-linux-server"
DEVOPS = ("--org", ORG, "--project", PROJECT, "--only-show-errors")
ARM = ("--subscription", SUBSCRIPTION, "--only-show-errors")


def az_json(*args):
    return json.loads(subprocess.check_output([*args, "--output", "json"], text=True))


def token(cli, resource, *args):
    return subprocess.check_output(
        [cli, "account", "get-access-token", "--resource", resource, *args,
         "--query", "accessToken", "--output", "tsv"],
        text=True,
    ).strip()


def request(url, credential, payload=None):
    headers = {"Authorization": f"Bearer {credential}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=payload, headers=headers,
                                 method="PUT" if payload is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise SystemExit(f"Azure returned HTTP {error.code}.") from None
    except urllib.error.URLError:
        raise SystemExit("Could not reach Azure.") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, help="Install an exact successful main run")
    args = parser.parse_args()

    vm = az_json("az-admin", "vm", "show", "-g", GROUP, "-n", VM, *ARM)
    run_id = args.run_id
    if run_id is None:
        run_id = az_json(
            "az", "pipelines", "runs", "list", "--pipeline-ids", PIPELINE,
            "--branch", "refs/heads/main", "--status", "completed", "--result",
            "succeeded", "--top", "1", "--query", "[0].id", *DEVOPS,
        )
        if not run_id:
            raise SystemExit("Pipeline 26 has no successful main run.")
    run = az_json("az", "pipelines", "runs", "show", "--id", str(run_id), *DEVOPS)
    if (str(run["definition"]["id"]) != PIPELINE or run["sourceBranch"] != "refs/heads/main"
            or run["status"] != "completed" or run["result"] != "succeeded"):
        raise SystemExit(f"Run {run_id} is not a successful main build from pipeline {PIPELINE}.")

    print(f"Downloading artifact from main run {run_id}", flush=True)
    with tempfile.TemporaryDirectory(prefix="t3-pilot-artifact-") as path:
        subprocess.run(
            ["az", "pipelines", "runs", "artifact", "download", "--run-id", str(run_id),
             "--artifact-name", ARTIFACT, "--path", path, *DEVOPS], check=True,
        )
        directory = Path(path)
        info = json.loads((directory / "build-info.json").read_text())
        if (info["branch"] != "main" or info["commit"] != run["sourceVersion"]
                or info["runUrl"] != f"{ORG}/{PROJECT}/_build/results?buildId={run_id}"
                or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+-nightly\.[0-9]+\.[0-9]+", info["version"])):
            raise SystemExit("Artifact metadata does not match the selected pipeline run.")
        archive = f't3-{info["version"]}-linux-x64.tar.gz'
        sums = {}
        for line in (directory / "SHA256SUMS").read_text().splitlines():
            match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
            if not match or match[2] in sums:
                raise SystemExit("Malformed artifact checksum manifest.")
            sums[match[2]] = match[1]
        if sums.keys() != {archive, "build-info.json"}:
            raise SystemExit("Unexpected artifact checksum manifest entries.")
        for name, expected in sums.items():
            with (directory / name).open("rb") as source:
                if hashlib.file_digest(source, "sha256").hexdigest() != expected:
                    raise SystemExit(f"Artifact checksum mismatch for {name}.")
    print(f'Verified {info["commit"]} and SHA-256 {sums[archive]}', flush=True)

    # Warm the VM agent extension before minting the short-lived download URL.
    print("Preparing Azure VM Run Command", flush=True)
    subprocess.run(
        ["az-admin", "vm", "run-command", "create", "-g", GROUP, "--vm-name", VM,
         "--name", COMMAND, "--script", "true", "--output", "none", *ARM], check=True,
    )
    arm_token = token("az-admin", "https://management.azure.com/", *ARM)
    devops_token = token("az", "499b84ac-1321-427f-aa17-267ca6975798")
    signed = json.loads(request(
        f"{ORG}/{PROJECT}/_apis/pipelines/{PIPELINE}/runs/{run_id}/artifacts"
        f"?artifactName={ARTIFACT}&%24expand=signedContent&api-version=7.1-preview.1",
        devops_token,
    ))
    payload = {
        "location": vm["location"],
        "properties": {
            "source": {"script": Path(__file__).with_name("install.sh").read_text()},
            "parameters": [
                {"name": "T3_ARCHIVE", "value": archive},
                {"name": "T3_SHA256", "value": sums[archive]},
                {"name": "T3_BUILD_INFO_SHA256", "value": sums["build-info.json"]},
            ],
            "protectedParameters": [{"name": "T3_ARTIFACT_URL", "value": signed["signedContent"]["url"]}],
            "timeoutInSeconds": 900,
            "treatFailureAsDeploymentFailure": True,
        },
    }
    request(
        f'https://management.azure.com{vm["id"]}/runCommands/{COMMAND}?api-version=2024-07-01',
        arm_token, json.dumps(payload).encode(),
    )
    print("Installation submitted. Check the install-t3 Run Command instance view.")


if __name__ == "__main__":
    main()
