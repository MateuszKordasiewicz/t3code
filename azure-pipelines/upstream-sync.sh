#!/usr/bin/env bash
set -euo pipefail

main_ref=refs/heads/main
upstream_url=https://github.com/pingdotgg/t3code.git
target_ref="$main_ref"
pilot_ref=refs/heads/sync/azure-upstream-control/pilot

fail() {
  printf '##vso[task.logissue type=error]%s\n' "$*"
  exit 1
}

azure_git() {
  git -c "http.extraheader=AUTHORIZATION: bearer $SYSTEM_ACCESSTOKEN" "$@"
}

azure_api() {
  local path=$1
  shift
  local endpoint
  endpoint="${SYSTEM_COLLECTIONURI}${SYSTEM_TEAMPROJECTID}/_apis"
  curl --silent --show-error --fail-with-body --max-time 30 \
    -H "Authorization: Bearer $SYSTEM_ACCESSTOKEN" "$@" "$endpoint/$path?api-version=7.1"
}

azure_git_api() {
  local path=$1
  shift
  azure_api "git/repositories/$BUILD_REPOSITORY_ID/$path" "$@"
}

select_target() {
  [[ "${TRIAL_TARGET:-}" != none ]] || TRIAL_TARGET=''
  [[ "${TRIAL_BASE:-}" != none ]] || TRIAL_BASE=''
  [[ "${TRIAL_UPSTREAM:-}" != none ]] || TRIAL_UPSTREAM=''
  if [[ -z "${TRIAL_TARGET:-}" ]]; then
    [[ -z "${TRIAL_UPSTREAM:-}" && -z "${TRIAL_BASE:-}" && "${TRIAL_MODE:-repair}" == repair ]] ||
      fail "Pinned trial commits and test modes require a disposable target."
    return
  fi
  if [[ "$BUILD_REASON" != Manual ||
        ! "$TRIAL_TARGET" =~ ^[a-z0-9][a-z0-9-]{0,59}$ ||
        ! "${TRIAL_BASE:-}" =~ ^[0-9a-f]{40}$ ||
        ! "${TRIAL_UPSTREAM:-}" =~ ^[0-9a-f]{40}$ ]]; then
    fail "A trial requires a manual run, a disposable target name, and full target and upstream SHAs."
  fi
  target_ref="refs/heads/sync/azure-upstream-trial/$TRIAL_TARGET"
}

sync_commit() {
  local tree=$1 base=$2 upstream=$3 date
  date=$(git show -s --format=%cI "$upstream")
  GIT_AUTHOR_DATE="$date" GIT_COMMITTER_DATE="$date" git \
    -c user.name='T3 Code upstream sync' -c user.email='t3code-upstream-sync@users.noreply.github.com' \
    commit-tree "$tree" -p "$base" -p "$upstream" -m "chore: sync T3 Code upstream"
}

prepare() {
  if [[ "$BUILD_REASON" == Schedule ]]; then
    case "$CRON_SCHEDULE:$(TZ=America/New_York date +%z)" in
      'Eastern daylight time:-0400'|'Eastern standard time:-0500') ;;
      *)
        echo "Result: skipped UTC trigger outside the current Eastern offset."
        return
        ;;
    esac
  fi
  local dry_run=false
  if [[ "$BUILD_REASON" != Schedule && "${DRY_RUN,,}" == true ]]; then
    dry_run=true
  fi
  if [[ "$dry_run" == false && "$target_ref" == "$main_ref" && "$BUILD_SOURCEBRANCH" != "$main_ref" ]]; then
    fail "Publishing an upstream sync requires running the pipeline on main."
  fi

  echo "Fetching Azure target $target_ref and GitHub upstream."
  azure_git fetch --no-tags origin "$target_ref"
  local base upstream result exit_code=0 tree commit
  base=$(git rev-parse FETCH_HEAD)
  [[ -z "${TRIAL_BASE:-}" || "$base" == "$TRIAL_BASE" ]] ||
    fail "The fetched Azure target does not match the trial pin."
  git fetch --no-tags "$upstream_url" "${TRIAL_UPSTREAM:-$main_ref}"
  upstream=$(git rev-parse FETCH_HEAD)
  [[ -z "${TRIAL_UPSTREAM:-}" || "$upstream" == "$TRIAL_UPSTREAM" ]] ||
    fail "The fetched upstream commit does not match the trial pin."
  echo "Azure target: $base"
  echo "GitHub upstream: $upstream"

  if git merge-base --is-ancestor "$upstream" "$base"; then
    [[ "${TRIAL_MODE:-repair}" == repair ]] || fail "This test mode requires an upstream conflict."
    echo "Result: Already current"
    return
  fi

  result=$(git merge-tree --write-tree --name-only --no-messages "$base" "$upstream") || exit_code=$?
  tree=${result%%$'\n'*}
  case "$exit_code" in
    0) [[ "${TRIAL_MODE:-repair}" == repair ]] || fail "This test mode requires an upstream conflict." ;;
    1)
      printf 'Conflicting files:\n%s\n' "${result#*$'\n'}"
      if [[ "$dry_run" == true ||
            ( "$target_ref" == "$main_ref" &&
              ( "$BUILD_REASON" != Schedule || "${VM_REPAIR_ENABLED:-}" != true ) ) ]]; then
        fail "Result: Conflict. Resolve the files listed in this step before syncing."
      fi
      ;;
    *) fail "Git could not compute the merge (exit $exit_code)." ;;
  esac

  commit=$(sync_commit "$tree" "$base" "$upstream")
  if ((exit_code == 0)); then
    echo "Result: Clean candidate $commit"
    if [[ "$dry_run" == true ]]; then
      echo "Dry run complete."
      return
    fi
  else
    echo "Result: Conflict. Preparing the exact inputs for one pilot repair."
  fi
  mkdir -p "$SYNC_DIR"
  printf '%s\n' "$base" "$upstream" "$commit" > "$SYNC_DIR/inputs"
  if ((exit_code == 1)); then
    mkdir -p "$SYNC_DIR/artifact"
    git update-ref refs/t3-upstream/input "$commit"
    git bundle create "$SYNC_DIR/artifact/input.bundle" refs/t3-upstream/input
    git update-ref -d refs/t3-upstream/input "$commit"
    sha256sum "$SYNC_DIR/artifact/input.bundle" | cut -d ' ' -f 1 > "$SYNC_DIR/input.sha256"
    touch "$SYNC_DIR/repair-needed"
    echo '##vso[task.setvariable variable=repairNeeded]true'
  fi
}

publish() {
  [[ -f "$SYNC_DIR/inputs" ]] || return 0
  local base upstream baseline commit tree ref existing inputs
  readarray -t inputs < "$SYNC_DIR/inputs"
  base=${inputs[0]} upstream=${inputs[1]} baseline=${inputs[2]}
  commit=$baseline
  if [[ -f "$SYNC_DIR/repair-needed" ]]; then
    [[ -f "$SYNC_DIR/repaired.commit" && -f "$SYNC_DIR/repaired.pack" ]] ||
      fail "The pilot did not return a candidate."
    commit=$(cat "$SYNC_DIR/repaired.commit")
    [[ "$commit" =~ ^[0-9a-f]{40}$ && "$commit" != "$baseline" ]] ||
      fail "The pilot returned an invalid candidate commit ID."
    git index-pack --strict --fix-thin --stdin < "$SYNC_DIR/repaired.pack" >/dev/null ||
      fail "The pilot returned an invalid Git pack."
    [[ "$(git show -s --format=%P "$commit")" == "$base $upstream" ]] ||
      fail "The repaired candidate does not have the exact target and upstream parents."
    if ! git diff --quiet "$baseline" "$commit" -- azure-pipelines/ infra/azure-private-pilot/; then
      fail "The repair changed the Azure pipeline or pilot configuration. Review the conflicts manually."
    fi
    if ! git -c core.whitespace=-blank-at-eol,-blank-at-eof,-space-before-tab diff --check "$base" "$commit"; then
      fail "The repaired candidate failed Git's conflict marker check."
    fi
    echo "Verified repaired candidate $commit for parents $base and $upstream."
  fi
  tree=$(git rev-parse "$commit^{tree}")
  ref="refs/heads/sync/azure-upstream/$commit"
  if [[ "$target_ref" != "$main_ref" ]]; then
    ref="refs/heads/sync/azure-upstream/trial/$BUILD_BUILDID/$commit"
  fi
  if [[ "$(azure_git ls-remote origin "$target_ref")" != "$base"$'\t'"$target_ref" ]]; then
    fail "The Azure target changed during preparation. Run the sync again."
  fi
  existing=$(azure_git ls-remote origin "$ref")
  if [[ "$existing" == "$commit"$'\t'"$ref" ]]; then
    echo "Candidate branch already exists: $ref"
  else
    echo "Creating candidate branch: $ref"
    azure_git push --force-with-lease="$ref:" origin "$commit:$ref"
  fi
  printf '##vso[task.setvariable variable=%s;isOutput=true]%s\n' \
    commit "$commit" ref "$ref" base "$base" upstream "$upstream" tree "$tree"
}

pilot_head() {
  local remote
  remote=$(azure_git ls-remote origin "$pilot_ref") || return
  if [[ -n "$remote" ]]; then
    azure_git fetch --no-tags origin "$pilot_ref" >&2 || return
    git rev-parse FETCH_HEAD
  fi
}

pilot_commit() {
  local message=$1 parent=$2 tree
  local args=()
  if [[ -n "$parent" ]]; then
    args=(-p "$parent")
  fi
  tree=$(git mktree </dev/null)
  git -c user.name='T3 Code upstream sync' -c user.email='t3code-upstream-sync@users.noreply.github.com' \
    commit-tree "$tree" "${args[@]}" -m "$message"
}

acquire_pilot() {
  local head lease
  head=$(pilot_head)
  if [[ -n "$head" && "$(git show -s --format=%s "$head")" != released ]]; then
    fail "The pilot is reserved by $(git show -s --format=%s "$head"). Check that run before retrying."
  fi
  lease=$(pilot_commit "run $BUILD_BUILDID attempt $SYSTEM_JOBATTEMPT" "$head")
  printf '%s\n' "$lease" > "$SYNC_DIR/pilot-lease"
  azure_git push --force-with-lease="$pilot_ref:$head" origin "$lease:$pilot_ref" ||
    fail "Another sync reserved the pilot."
  echo '##vso[task.setvariable variable=pilotReserved]true'
}

release_pilot() {
  [[ -f "$SYNC_DIR/pilot-lease" ]] || return 0
  local lease head released
  lease=$(cat "$SYNC_DIR/pilot-lease")
  head=$(pilot_head)
  [[ "$head" == "$lease" ]] || return 0
  if [[ -f "$SYNC_DIR/pilot-start-requested" && ! -f "$SYNC_DIR/pilot-deallocated" ]]; then
    fail "Pilot deallocation could not be verified. The reservation is held for run $BUILD_BUILDID."
  fi
  if [[ -s "$SYNC_DIR/pilot-commands" ]]; then
    fail "Pilot Run Command deletion could not be verified. The reservation is held for run $BUILD_BUILDID."
  fi
  if [[ -f "$SYNC_DIR/pilot-guest-workspace-requested" && ! -f "$SYNC_DIR/pilot-guest-workspace-cleaned" ]]; then
    fail "Pilot guest workspace cleanup could not be verified. The reservation is held for run $BUILD_BUILDID."
  fi
  released=$(pilot_commit released "$lease")
  azure_git push --force-with-lease="$pilot_ref:$lease" origin "$released:$pilot_ref"
  echo "Released the pilot reservation."
}

check_candidate_refs() {
  local refs
  refs=$(azure_git ls-remote origin "$target_ref" "$CANDIDATE_REF")
  if ! grep -Fxq "$CANDIDATE_BASE"$'\t'"$target_ref" <<< "$refs" ||
    ! grep -Fxq "$CANDIDATE_COMMIT"$'\t'"$CANDIDATE_REF" <<< "$refs"; then
    fail "The Azure target or the candidate moved during validation. Run the sync again."
  fi
}

verify_pr_preview() {
  local pr_id=$1 pr status preview attempt
  for ((attempt = 1; attempt <= 12; attempt++)); do
    pr=$(azure_git_api "pullrequests/$pr_id")
    status=$(jq -er '.mergeStatus' <<< "$pr")
    case "$status" in
      succeeded) break ;;
      queued|notSet) if ((attempt < 12)); then sleep 2; fi ;;
      *) fail "Azure could not prepare the merge preview for PR $pr_id ($status)." ;;
    esac
  done
  if [[ "$status" != succeeded ]]; then
    fail "Azure did not finish preparing the merge preview for PR $pr_id."
  fi

  if ! jq -e --arg source "$CANDIDATE_REF" --arg target "$target_ref" \
    --arg commit "$CANDIDATE_COMMIT" --arg base "$CANDIDATE_BASE" \
    '.status == "active" and .sourceRefName == $source and .targetRefName == $target
     and .lastMergeSourceCommit.commitId == $commit and .lastMergeTargetCommit.commitId == $base' \
    <<< "$pr" >/dev/null; then
    fail "PR $pr_id no longer points to the validated source and target. Run the sync again."
  fi
  local preview_commit
  preview_commit=$(jq -er '.lastMergeCommit.commitId' <<< "$pr")
  preview=$(azure_git_api "commits/$preview_commit")
  if ! jq -e --arg tree "$CANDIDATE_TREE" --arg commit "$CANDIDATE_COMMIT" --arg base "$CANDIDATE_BASE" \
    '.treeId == $tree and .parents == [$base, $commit]' <<< "$preview" >/dev/null; then
    fail "The Azure merge preview for PR $pr_id does not match the validated tree and parents."
  fi
  check_candidate_refs
  echo "Verified Azure merge preview $preview_commit for source $CANDIDATE_COMMIT, target $CANDIDATE_BASE, and tree $CANDIDATE_TREE."
}

require_build_policy() {
  local policies
  policies=$(azure_api policy/configurations --get --data-urlencode '$top=1000')
  if ! jq -e --arg repo "$BUILD_REPOSITORY_ID" --arg target "$target_ref" '
    any(.value[];
      .isEnabled and .isBlocking
      and .settings.buildDefinitionId == 26
      and .settings.manualQueueOnly == false
      and .settings.queueOnSourceUpdateOnly == false
      and .settings.validDuration == 0
      and ((.settings.filenamePatterns // []) | length) == 0
      and any(.settings.scope[]?; .repositoryId == $repo and .refName == $target and .matchKind == "Exact"))
  ' <<< "$policies" >/dev/null; then
    fail "The target requires the blocking Linux server build policy with immediate expiration on target updates."
  fi
}

enable_auto_complete() {
  local pr_id=$1 pr author body armed
  require_build_policy
  pr=$(azure_git_api "pullrequests/$pr_id")
  author=$(jq -er '.createdBy.id' <<< "$pr")
  if ! jq -e '.status == "active" and .isDraft != true' <<< "$pr" >/dev/null; then
    fail "PR $pr_id must be active and ready for review to use automatic completion."
  fi
  verify_pr_preview "$pr_id"
  body=$(jq -n --arg id "$author" \
    '{autoCompleteSetBy:{id:$id},completionOptions:{mergeStrategy:"noFastForward",bypassPolicy:false}}')
  armed=$(azure_git_api "pullrequests/$pr_id" -X PATCH -H 'Content-Type: application/json' --data "$body")
  if ! jq -e '.status == "completed"' <<< "$armed" >/dev/null; then
    jq -e --arg id "$author" '.autoCompleteSetBy.id == $id' <<< "$armed" >/dev/null
    body=$(jq -n --arg id "$author" '{vote:10,id:$id}')
    azure_git_api "pullrequests/$pr_id/reviewers/$author" \
      -X PUT -H 'Content-Type: application/json' --data "$body" | jq -e '.vote == 10' >/dev/null
  fi
  echo "Azure will complete PR $pr_id when its required policies pass."
}

open_pr() {
  local existing run_url description body response pr_id
  echo "Checking that the Azure target and candidate still match the successful build."
  check_candidate_refs
  if [[ "$target_ref" != "$main_ref" ]]; then
    require_build_policy
  fi
  existing=$(azure_git_api pullrequests --get --data-urlencode "searchCriteria.status=active" \
    --data-urlencode "searchCriteria.targetRefName=$target_ref" \
    --data-urlencode "searchCriteria.sourceRefName=$CANDIDATE_REF")
  pr_id=$(jq -r '.value[0].pullRequestId // empty' <<< "$existing")
  if [[ -z "$pr_id" ]]; then
    run_url="${SYSTEM_COLLECTIONURI}${SYSTEM_TEAMPROJECT}/_build/results?buildId=${BUILD_BUILDID}"
    description=$(printf 'Sync upstream `%s` into `%s` at `%s`.\n\nCandidate `%s` passed the [Linux server build and archive smoke test](%s).\n\nThe required Linux build policy verifies that the merge preview still matches this candidate. If either branch advanced, rerun the sync. Use a merge commit to retain upstream history.' \
      "$UPSTREAM_COMMIT" "${target_ref#refs/heads/}" "$CANDIDATE_BASE" "$CANDIDATE_COMMIT" "$run_url")
    body=$(jq -n --arg source "$CANDIDATE_REF" --arg target "$target_ref" --arg description "$description" \
      '{sourceRefName:$source,targetRefName:$target,title:"chore: sync T3 Code upstream",description:$description}')
    response=$(azure_git_api pullrequests -H 'Content-Type: application/json' --data "$body")
    pr_id=$(jq -er '.pullRequestId' <<< "$response")
    echo "Created by $(jq -er '.createdBy.displayName' <<< "$response")."
  fi
  if [[ "$BUILD_REASON" == Schedule ]]; then
    enable_auto_complete "$pr_id"
  else
    verify_pr_preview "$pr_id"
  fi
  echo "Result: protected PR at ${SYSTEM_COLLECTIONURI}${SYSTEM_TEAMPROJECT}/_git/${BUILD_REPOSITORY_NAME}/pullrequest/$pr_id"
}

select_target
case "$1" in
  prepare) prepare ;;
  publish) publish ;;
  acquire-pilot) acquire_pilot ;;
  release-pilot) release_pilot ;;
  open-pr) open_pr ;;
  *) fail "Unknown upstream sync step: $1" ;;
esac
