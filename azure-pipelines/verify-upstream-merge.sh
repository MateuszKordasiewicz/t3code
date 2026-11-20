#!/usr/bin/env bash
set -euo pipefail

candidate="${SYSTEM_PULLREQUEST_SOURCEBRANCH##*/}"
readarray -t parents < <(git cat-file -p HEAD | sed -n '/^$/q; s/^parent //p')
if [[ "${#parents[@]}" != 2 || "${parents[1]}" != "$candidate" ]]; then
  echo '##vso[task.logissue type=error]The PR source moved. Run the upstream sync again.'
  exit 1
fi

base=$(git cat-file -p "$candidate" | sed -n '/^$/q; s/^parent //p' | sed -n '1p')
if [[ "${parents[0]}" != "$base" ]] ||
  [[ "$(git rev-parse HEAD^{tree})" != "$(git rev-parse "$candidate^{tree}")" ]]; then
  echo '##vso[task.logissue type=error]The PR target or merge tree changed. Run the upstream sync again.'
  exit 1
fi
echo "Azure is validating candidate $candidate against its original target $base."
