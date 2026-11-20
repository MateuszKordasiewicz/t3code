#!/usr/bin/env bash
set -euo pipefail

echo 'Checking service, private listener, and persistent storage'
systemctl is-enabled t3-private-pilot.service
systemctl is-active t3-private-pilot.service
findmnt --mountpoint /srv/t3 --output SOURCE,FSTYPE,TARGET
curl --noproxy '*' --silent --show-error --fail --retry 5 --retry-connrefused \
    --max-time 5 --output /dev/null --write-out 'T3 web HTTP %{response_code}\n' http://127.0.0.1:3773/
[[ "$(ss -H -ltn 'sport = :3773' | awk '{print $4}')" == 127.0.0.1:3773 ]]
test -f /srv/t3/home/.t3/userdata/statev2.sqlite
test -d /srv/t3/workspaces
runuser -u t3 -- /usr/local/bin/t3 --version
runuser -u t3 -- /usr/local/bin/codex --version
runuser -u t3 -- /usr/local/bin/claude --version
cat /opt/t3/current/build-info.json
echo 'Pilot guest checks passed'
