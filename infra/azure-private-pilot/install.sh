#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 && "$(uname -m)" == x86_64 ]]
umask 077
exec 9>/run/lock/t3-private-pilot.lock
flock -n 9
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

echo 'Downloading the verified Azure main artifact'
python3 - "$scratch" <<'PY'
import os
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

directory = Path(sys.argv[1])
try:
    bundle = directory / "artifact.zip"
    with urllib.request.urlopen(os.environ["T3_ARTIFACT_URL"], timeout=120) as response:
        with bundle.open("wb") as output:
            shutil.copyfileobj(response, output)
    with zipfile.ZipFile(bundle) as artifact:
        for name in (os.environ["T3_ARCHIVE"], "build-info.json"):
            member, = (entry for entry in artifact.namelist() if PurePosixPath(entry).name == name)
            with artifact.open(member) as source, (directory / name).open("wb") as output:
                shutil.copyfileobj(source, output)
    bundle.unlink()
except Exception:
    sys.exit("Artifact download failed. Rerun the installer to request a fresh signed URL.")
PY
unset T3_ARTIFACT_URL
(
    cd "$scratch"
    printf '%s  %s\n' "$T3_SHA256" "$T3_ARCHIVE" "$T3_BUILD_INFO_SHA256" build-info.json |
        sha256sum --check --strict
)

echo 'Preparing the persistent data disk at Azure LUN 0'
device=/dev/disk/azure/scsi1/lun0
mount_dir=/srv/t3
udevadm settle
if [[ -b /dev/disk/azure/data/by-lun/0 ]]; then
    device=/dev/disk/azure/data/by-lun/0
fi
[[ -b "$device" ]]
signatures=$(wipefs --noheadings --output TYPE "$device")
if [[ -z "$signatures" ]]; then
    mkfs.ext4 -q -L t3-pilot "$device"
fi
[[ "$(blkid -s TYPE -o value "$device")" == ext4 ]]
[[ "$(blkid -s LABEL -o value "$device")" == t3-pilot ]]
uuid=$(blkid -s UUID -o value "$device")
install -d -m 0755 "$mount_dir"
source=$(findmnt --fstab --noheadings --output SOURCE --mountpoint "$mount_dir" || true)
if [[ -z "$source" ]]; then
    printf 'UUID=%s %s ext4 defaults,nofail 0 2\n' "$uuid" "$mount_dir" >> /etc/fstab
else
    [[ "$source" == "UUID=$uuid" ]]
fi
systemctl daemon-reload
mountpoint -q "$mount_dir" || mount "$mount_dir"
[[ "$(findmnt --noheadings --output UUID --mountpoint "$mount_dir")" == "$uuid" ]]
id t3 >/dev/null 2>&1 || useradd --home-dir "$mount_dir/home" --shell /bin/bash --user-group t3
install -d -m 0700 -o t3 -g t3 "$mount_dir/home" "$mount_dir/workspaces"
systemctl mask --now ssh.socket ssh.service >/dev/null

echo 'Installing T3 and the provider CLIs'
export DEBIAN_FRONTEND=noninteractive
apt-get -qq update
apt-get -qq --yes -o DPkg::Lock::Timeout=120 install ca-certificates curl git libatomic1
install -d -m 0755 /opt/t3 /opt/t3/releases /opt/t3/providers /usr/local/bin

release="/opt/t3/releases/$T3_SHA256"
if [[ ! -x "$release/t3" ]]; then
    mkdir "$scratch/t3"
    tar --extract --gzip --no-same-owner --file "$scratch/$T3_ARCHIVE" --strip-components=1 -C "$scratch/t3"
    install -m 0644 "$scratch/build-info.json" "$scratch/t3/build-info.json"
    mv -T "$scratch/t3" "$release"
fi

codex_version=0.171.0
codex_dir="/opt/t3/providers/codex-$codex_version"
if [[ ! -x "$codex_dir/bin/codex" ]]; then
    curl -fsSL --retry 3 "https://registry.npmjs.org/@openai/codex/-/codex-$codex_version-linux-x64.tgz" -o "$scratch/codex.tgz"
    printf '%s  %s\n' e13d67afb0533cc6160b5f4140e4ad07e2acd87adbaa081bbf69b82e90cf4e55 "$scratch/codex.tgz" | sha256sum -c
    mkdir "$scratch/codex"
    tar -xzf "$scratch/codex.tgz" --no-same-owner --strip-components=3 -C "$scratch/codex" \
        package/vendor/x86_64-unknown-linux-musl
    mv -T "$scratch/codex" "$codex_dir"
fi
chmod -R u+rwX,go+rX,go-w "$release" "$codex_dir"
ln -sfn "$codex_dir/bin/codex" /usr/local/bin/codex

claude_version=2.1.329
claude="/opt/t3/providers/claude-$claude_version"
if [[ ! -x "$claude" ]]; then
    curl -fsSL --retry 3 "https://registry.npmjs.org/@anthropic-ai/claude-code-linux-x64/-/claude-code-linux-x64-$claude_version.tgz" -o "$scratch/claude.tgz"
    printf '%s  %s\n' a74d6c8441a19b2d2f44c1f5735b37dc0a34b2d4f224f8333d08ce03cf2060bb "$scratch/claude.tgz" | sha256sum -c
    tar -xOzf "$scratch/claude.tgz" package/claude > "$scratch/claude"
    chmod 0755 "$scratch/claude"
    mv -T "$scratch/claude" "$claude"
fi
ln -sfn "$claude" /usr/local/bin/claude
ln -sfn "$release" /opt/t3/current
ln -sfn /opt/t3/current/t3 /usr/local/bin/t3

cat > /etc/systemd/system/t3-private-pilot.service <<'UNIT'
[Unit]
Description=T3 Code private pilot
Wants=network-online.target
After=network-online.target
RequiresMountsFor=/srv/t3
ConditionPathIsMountPoint=/srv/t3

[Service]
User=t3
Group=t3
Environment=HOME=/srv/t3/home
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=DISABLE_AUTOUPDATER=1
WorkingDirectory=/srv/t3/workspaces
ExecStart=/opt/t3/current/t3 serve --host 127.0.0.1 --port 3773
Restart=on-failure
RestartSec=5
UMask=0077
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
UNIT
chmod 0644 /etc/systemd/system/t3-private-pilot.service
systemctl daemon-reload
systemctl enable t3-private-pilot.service >/dev/null
systemctl restart t3-private-pilot.service

runuser -u t3 -- /usr/local/bin/t3 --version
runuser -u t3 -- /usr/local/bin/codex --version
runuser -u t3 -- /usr/local/bin/claude --version
echo 'Installation complete'
