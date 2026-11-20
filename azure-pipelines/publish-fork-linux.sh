#!/usr/bin/env bash
set -euo pipefail

tag=${1:?Expected nightly tag}
sha=${2:?Expected source commit}
run=${3:?Expected publisher and run ID}
assets=${4:?Expected release assets directory}
: "${GH_REPO:?Expected GitHub release repository}"

nightly_pattern='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-nightly\.[0-9]{8}\.[1-9][0-9]*$'
[[ $tag =~ $nightly_pattern && $sha =~ ^[0-9a-f]{40}$ && $run =~ ^(azure|github)-[0-9]+$ ]]
(
  cd "$assets"
  sha256sum --check SHA256SUMS
  test -f nightly-linux.yml
  test -f T3-Code-Nightly-x86_64.AppImage
  grep -Fx "version: ${tag#v}" nightly-linux.yml
)

repo="repos/$GH_REPO"
remote="https://github.com/$GH_REPO.git"
if ! status="$(gh api "$repo/compare/$sha...main" --jq .status)" || [[ $status != @(ahead|identical) ]]; then
  echo "GitHub main does not contain $sha. Let the source mirror finish and retry." >&2
  exit 1
fi

# Both Azure and GitHub Actions use this ref as a lock. The run ID gives a retry
# the same lock commit so it can recover if the original agent stopped abruptly.
lock=refs/heads/fork-linux-publish-lock
work="$(mktemp -d)"
git init -q "$work"
git_auth() {
  git -C "$work" -c credential.helper= -c 'credential.helper=!gh auth git-credential' "$@"
}
lock_owner="$(
  export GIT_AUTHOR_DATE=2000-01-01T00:00:00Z GIT_COMMITTER_DATE=2000-01-01T00:00:00Z
  git -C "$work" -c user.name='T3 Code publisher' -c user.email='release@t3.codes' \
    commit-tree "$(git -C "$work" mktree </dev/null)" -m "$run $tag $sha"
)"
locked=false
cleanup() {
  local result=$?
  if $locked && ! git_auth push --quiet --force-with-lease="$lock:$lock_owner" "$remote" ":$lock"; then
    echo "Could not release $lock for $run. Retry the same run to recover its lock." >&2
    result=1
  fi
  rm -rf "$work"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for ((attempt = 1; attempt <= 60; attempt++)); do
  current="$(git_auth ls-remote "$remote" "$lock" | cut -f1)"
  if [[ $current == "$lock_owner" ]] ||
    { [[ -z $current ]] && git_auth push --quiet --force-with-lease="$lock:" "$remote" "$lock_owner:$lock"; }; then
    locked=true
    break
  fi
  if [[ $attempt == 1 ]]; then
    echo "Waiting for the fork Linux publish lock..."
  fi
  sleep 5
done
if ! $locked; then
  echo "Timed out acquiring $lock. Inspect its commit for the owning run before recovering it." >&2
  exit 1
fi

list_releases() {
  gh api --paginate --slurp "$repo/releases?per_page=100" | jq 'add // []'
}
find_release() {
  jq -ce --arg tag "$1" '[.[] | select(.tag_name == $tag)] |
    if length > 1 then error("Duplicate release for " + $tag) else .[0] // {} end'
}
read_tag_commit() {
  git_auth ls-remote "$remote" "refs/tags/$tag" "refs/tags/$tag^{}" | tail -1 | cut -f1
}

release="$(list_releases | find_release "$tag")"
tag_commit="$(read_tag_commit)"
if [[ -n $tag_commit && $tag_commit != "$sha" ]]; then
  echo "Tag $tag points to $tag_commit, expected $sha." >&2
  exit 1
fi
if [[ $(jq -r '.draft' <<< "$release") == false ]]; then
  test "$tag_commit" = "$sha"
  echo "$tag is already published at $sha."
else
  if [[ $(jq -r '.draft' <<< "$release") == true ]]; then
    test "$(jq -er '.target_commitish' <<< "$release")" = "$sha"
    gh api --method DELETE "$repo/releases/$(jq -er '.id' <<< "$release")"
  fi
  gh release create "$tag" "$assets"/* \
    --draft --prerelease --latest=false --target "$sha" \
    --title "T3 Code Fork nightly ${tag#v} (${sha:0:12})" \
    --notes "Fork build of $GH_REPO main at $sha. This app updates from the fork's releases."
  if [[ -z $(read_tag_commit) ]]; then
    gh api --method POST "$repo/git/refs" -f ref="refs/tags/$tag" -f sha="$sha" --silent
  fi
  test "$(read_tag_commit)" = "$sha"
  gh release edit "$tag" --draft=false --latest=false
fi

# Recover the highest complete release even if a prior run stopped after
# publishing that release or after removing the previous pointer asset.
releases="$(list_releases)"
newest="$(
  jq -r --arg pattern "$nightly_pattern" '
    .[] | select(.draft == false and .prerelease and (.tag_name | test($pattern))) |
    select(any(.assets[]; .name == "nightly-linux.yml" and .state == "uploaded")) |
    select(any(.assets[]; (.name | endswith(".AppImage")) and .state == "uploaded")) |
    .tag_name' <<< "$releases" | sort -V | tail -1
)"
test -n "$newest"
feed="$(find_release update-feed <<< "$releases")"
current=''
if jq -e 'any(.assets[]?; .name == "nightly.json" and .state == "uploaded")' <<< "$feed" >/dev/null; then
  pointer="$(gh release download update-feed --pattern nightly.json --output -)"
  current="$(jq -er --arg pattern "$nightly_pattern" '.tag_name | select(test($pattern))' <<< "$pointer")" || current=''
fi

if [[ $feed == '{}' ]]; then
  gh release create update-feed --draft --prerelease --latest=false --title "Update feed" \
    --notes "The fork desktop updater reads this release to find the newest fork nightly. Do not delete it."
  feed='{"draft":true}'
fi
if [[ $(printf '%s\n' "$newest" "$current" | sort -V | tail -1) == "$current" ]]; then
  echo "Update feed already points to $current."
else
  gh api "$repo/releases/tags/$newest" --jq '{tag_name, name, body}' > "$work/nightly.json"
  gh release upload update-feed "$work/nightly.json" --clobber
  echo "Update feed points to $newest."
fi
if [[ $(jq -r '.draft' <<< "$feed") == true ]]; then
  gh release edit update-feed --draft=false --prerelease --latest=false
fi
