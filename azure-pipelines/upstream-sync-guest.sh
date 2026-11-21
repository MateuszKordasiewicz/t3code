#!/usr/bin/env bash
set -euo pipefail
umask 077

guest_error() {
  printf 'T3_REPAIR_ERROR %s\n' "$*"
  exit 1
}

[[ "$T3_SYNC_NAME" =~ ^t3-upstream-[0-9]+-[0-9]+$ ]]
directory="/srv/t3/$T3_SYNC_NAME"
unit="$T3_SYNC_NAME.service"

remove_checkout() {
  if systemctl is-active --quiet "$unit"; then
    systemctl stop "$unit"
  fi
  rm -rf -- "$directory"
}

case "$T3_SYNC_ACTION" in
  cleanup)
    remove_checkout
    exit
    ;;
  read)
    [[ "$T3_SYNC_PART" =~ ^[0-9]{1,2}$ ]]
    # Run Command retains at most 4 KiB per stream. Each half fits after Base64 encoding.
    part=$((10#$T3_SYNC_PART * 2))
    dd if="$directory/candidate.pack" bs=2800 skip="$part" count=1 status=none | base64 -w0
    dd if="$directory/candidate.pack" bs=2800 skip="$((part + 1))" count=1 status=none | base64 -w0 >&2
    exit
    ;;
  repair) ;;
  *) guest_error 'unknown guest action' ;;
esac

mountpoint -q /srv/t3
mkdir -m 0711 "$directory"
trap remove_checkout EXIT
install -d -o t3 -g t3 -m 0700 "$directory/work"
if ! python3 - "$directory" 2>/dev/null <<'PY'
import hashlib
import os
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

directory = Path(sys.argv[1])
url = os.environ["T3_SYNC_DOWNLOAD"]
if not url.startswith("https://"):
    sys.exit(1)
archive = directory / "input.zip"
bundle = directory / "input.bundle"
with urllib.request.urlopen(url, timeout=90) as source, archive.open("wb") as dest:
    shutil.copyfileobj(source, dest)
with zipfile.ZipFile(archive) as zipped, bundle.open("wb") as dest:
    member, = (name for name in zipped.namelist() if PurePosixPath(name).name == "input.bundle")
    with zipped.open(member) as source:
        shutil.copyfileobj(source, dest)
with bundle.open("rb") as source:
    if hashlib.file_digest(source, "sha256").hexdigest() != os.environ["T3_SYNC_CHECKSUM"]:
        sys.exit(1)
archive.unlink()
bundle.chmod(0o444)
PY
then
  guest_error 'input download or checksum failed'
fi

cat > "$directory/worker.sh" <<'WORKER'
#!/usr/bin/env bash
set -euo pipefail
work=$1 base=$2 upstream=$3 baseline=$4 mode=$5
deadline=$((SECONDS + 1290))
cd "$work"
git -c init.defaultBranch=merge init -q checkout
cd checkout
git config core.hooksPath /dev/null
git fetch -q "$work/../input.bundle" refs/t3-upstream/input:refs/t3-upstream/input
test "$(git rev-parse refs/t3-upstream/input)" = "$baseline"
test "$(git show -s --format=%P "$baseline")" = "$base $upstream"
git checkout -q --detach "$base"
merge_status=0
git -c user.name='T3 Code upstream sync' -c user.email='t3code-upstream-sync@users.noreply.github.com' \
  merge --no-commit --no-ff "$upstream" || merge_status=$?
test "$merge_status" = 1
test "$(git rev-parse MERGE_HEAD)" = "$upstream"
test -n "$(git ls-files -u)"

if [[ "$mode" == invalid ]]; then
  tree=$(git rev-parse "$base^{tree}")
  parents=(-p "$base")
else
  cli=/opt/t3/current/t3
  t3_home="$work/t3-home"
  "$cli" serve --base-dir "$t3_home" --host 127.0.0.1 > "$work/server.log" 2>&1 &
  server=$!
  trap 'kill "$server" 2>/dev/null || true' EXIT
  for ((attempt = 1; attempt <= 30; attempt++)); do
    kill -0 "$server"
    [[ -f "$t3_home/userdata/server-runtime.json" ]] && break
    sleep 2
  done
  test -f "$t3_home/userdata/server-runtime.json"
  "$cli" project add --base-dir "$t3_home" "$PWD"
  seconds=$((deadline - SECONDS))
  ((seconds > 0)) || exit 124
  if ((seconds > 1200)); then
    seconds=1200
  fi
  if [[ "$mode" == timeout ]]; then
    seconds=3
  fi
  status=0
  timeout --kill-after=5 "$seconds" "$cli" thread start --base-dir "$t3_home" "$PWD" - \
    --provider codex --model gpt-6-luna --title 'Repair Azure upstream merge conflicts' --wait --json \
    > "$work/turn.json" 2> "$work/turn.stderr" <<PROMPT || status=$?
Task: Repair the upstream merge conflicts

This repository is a disposable checkout of Azure target $base. Git is merging public upstream $upstream.
Resolve the in-progress merge. Preserve the fork's existing features and upstream's changes where they can coexist.
Inspect both parents and the merge base. Use focused checks where practical and stage all intended resolutions.
Leave HEAD and MERGE_HEAD in place. Do not make a commit, push, change other workspaces, launch another agent, or edit Azure pipeline or pilot configuration.
If the merge cannot be resolved within this turn, leave the remaining conflicts and report the blocker.
PROMPT
  case "$status" in
    0) ;;
    124|137) exit 124 ;;
    2) exit 24 ;;
    *) exit 21 ;;
  esac
  test "$mode" != timeout
  test "$(git rev-parse HEAD)" = "$base"
  test "$(git rev-parse MERGE_HEAD)" = "$upstream"
  test -z "$(git ls-files -u)" || exit 22
  git diff --quiet || exit 23
  tree=$(git write-tree)
  parents=(-p "$base" -p "$upstream")
fi

commit=$(git -c user.name='T3 Code upstream sync' -c user.email='t3code-upstream-sync@users.noreply.github.com' \
  commit-tree "$tree" "${parents[@]}" -m 'chore: repair T3 Code upstream merge')
printf '%s\n^%s\n' "$commit" "$baseline" |
  git pack-objects --stdout --revs --thin > "$work/candidate.pack"
printf '%s\n' "$commit" > "$work/candidate.commit"
WORKER

chmod 0555 "$directory/worker.sh"
status=0
systemd-run --quiet --wait --pipe --collect --unit "$unit" --uid=t3 --gid=t3 \
  --property=RuntimeMaxSec=1350 --property=TimeoutStopSec=8 --property=ProtectSystem=strict \
  --property=PrivateTmp=true --property=NoNewPrivileges=true \
  --property="ReadWritePaths=$directory/work /srv/t3/home/.codex" \
  --setenv=HOME=/srv/t3/home --setenv=PATH=/usr/local/bin:/usr/bin:/bin \
  --setenv=DISABLE_AUTOUPDATER=1 \
  "$directory/worker.sh" "$directory/work" "$T3_SYNC_BASE" "$T3_SYNC_UPSTREAM" \
  "$T3_SYNC_BASELINE" "$T3_SYNC_MODE" > "$directory/worker.log" 2>&1 || status=$?
case "$status" in
  0) ;;
  124|137) guest_error 't3 turn timed out' ;;
  21)
    # Provider diagnostics may contain credentials. Only emit recognized error categories.
    reason=$(runuser -u t3 -- timeout 5 python3 - "$directory/work" 2>/dev/null <<'PY' || true
import json
import re
import sys
from pathlib import Path

work = Path(sys.argv[1])
with (work / "turn.stderr").open(errors="replace") as source:
    error = source.read(65536)
with (work / "turn.json").open(errors="replace") as source:
    try:
        error += "\n" + (json.loads(source.read(65536)).get("error") or "")
    except ValueError:
        pass
for pattern, message in (
    (r"read-only file system|os error 30", "read only filesystem"),
    (r"permission denied|operation not permitted|os error 13", "permission denied"),
    (r"not authenticated|unauthorized|login required|authentication failed", "provider authentication failed"),
    (r"usage limit|rate.limit|too many requests|quota exceeded", "provider usage limit"),
    (r"model.*(?:not found|does not exist|not supported|unavailable)", "provider model unavailable"),
    (r"no available provider|no provider.*available|provider.*not ready", "provider unavailable"),
    (r"could not reach|connection refused|server is not running", "t3 server unavailable"),
):
    if re.search(pattern, error, re.IGNORECASE):
        print(message)
        break
PY
)
    guest_error "t3 turn failed ${reason:-without a recognized diagnostic}"
    ;;
  22) guest_error 'unresolved merge paths remain' ;;
  23) guest_error 'unstaged tracked changes remain' ;;
  24) guest_error 't3 turn requested input' ;;
  *) guest_error 'guest checkout or t3 worker failed' ;;
esac

# Read workspace files as t3. The model controls that directory.
runuser -u t3 -- timeout 5 head -c 131073 "$directory/work/candidate.pack" > "$directory/candidate.pack"
size=$(stat -c %s "$directory/candidate.pack")
((size > 0 && size <= 131072)) || guest_error 'candidate pack exceeds the transfer limit'
commit=$(runuser -u t3 -- timeout 5 head -c 42 "$directory/work/candidate.commit")
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || guest_error 'invalid candidate id'
if [[ "$T3_SYNC_MODE" == repair ]]; then
  if ! runuser -u t3 -- timeout 5 python3 -c '
import json
import re
import sys

turn = json.load(sys.stdin)
ids = (turn["threadId"], turn["runId"])
if turn["status"] != "completed" or not all(re.fullmatch(r"[\w:%.-]{1,300}", id, re.ASCII) for id in ids):
    sys.exit(1)
print("T3_TURN", *ids)
' < <(runuser -u t3 -- timeout 5 head -c 1048576 "$directory/work/turn.json") 2>/dev/null; then
    guest_error 'completed t3 turn evidence missing'
  fi
fi
read -r checksum _ < <(sha256sum "$directory/candidate.pack")
printf 'T3_REPAIR_OK %s %s %s\n' "$commit" "$checksum" "$size"
trap - EXIT
