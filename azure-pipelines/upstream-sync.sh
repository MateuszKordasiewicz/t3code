#!/usr/bin/env bash
set -euo pipefail

main_ref=refs/heads/main
upstream_url=https://github.com/pingdotgg/t3code.git

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
  if [[ "$dry_run" == false && "$BUILD_SOURCEBRANCH" != "$main_ref" ]]; then
    fail "Publishing an upstream sync requires running the pipeline on main."
  fi

  echo "Fetching Azure main and GitHub upstream main."
  azure_git fetch --no-tags origin "$main_ref"
  local base upstream result exit_code=0 commit ref existing
  base=$(git rev-parse FETCH_HEAD)
  git fetch --no-tags "$upstream_url" "$main_ref"
  upstream=$(git rev-parse FETCH_HEAD)
  echo "Azure main: $base"
  echo "GitHub upstream main: $upstream"

  if git merge-base --is-ancestor "$upstream" "$base"; then
    echo "Result: Already current"
    return
  fi

  result=$(git merge-tree --write-tree --name-only --no-messages "$base" "$upstream") || exit_code=$?
  case "$exit_code" in
    0) ;;
    1)
      printf 'Conflicting files:\n%s\n' "${result#*$'\n'}"
      fail "Result: Conflict. Resolve the files listed in this step before syncing."
      ;;
    *) fail "Git could not compute the merge (exit $exit_code)." ;;
  esac

  # Fixed metadata gives retries of the same parent pair the same commit and branch.
  local commit_date
  commit_date=$(git show -s --format=%cI "$upstream")
  commit=$(GIT_AUTHOR_DATE="$commit_date" GIT_COMMITTER_DATE="$commit_date" git \
    -c user.name='T3 Code upstream sync' -c user.email='t3code-upstream-sync@users.noreply.github.com' \
    commit-tree "$result" -p "$base" -p "$upstream" -m "chore: sync T3 Code upstream")
  echo "Result: Clean candidate $commit"
  if [[ "$dry_run" == true ]]; then
    echo "Dry run complete."
    return
  fi

  ref="refs/heads/sync/azure-upstream/$commit"
  existing=$(azure_git ls-remote origin "$ref")
  if [[ "$existing" == "$commit"$'\t'"$ref" ]]; then
    echo "Candidate branch already exists: $ref"
  else
    echo "Creating candidate branch: $ref"
    azure_git push --force-with-lease="$ref:" origin "$commit:$ref"
  fi
  printf '##vso[task.setvariable variable=%s;isOutput=true]%s\n' \
    commit "$commit" ref "$ref" base "$base" upstream "$upstream" tree "$result"
}

check_candidate_refs() {
  local refs
  refs=$(azure_git ls-remote origin "$main_ref" "$CANDIDATE_REF")
  if ! grep -Fxq "$CANDIDATE_BASE"$'\t'"$main_ref" <<< "$refs" ||
    ! grep -Fxq "$CANDIDATE_COMMIT"$'\t'"$CANDIDATE_REF" <<< "$refs"; then
    fail "Azure main or the candidate moved during validation. Run the sync again."
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

  if ! jq -e --arg source "$CANDIDATE_REF" --arg target "$main_ref" \
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
  if ! jq -e --arg repo "$BUILD_REPOSITORY_ID" --arg target "$main_ref" '
    any(.value[];
      .isEnabled and .isBlocking
      and .settings.buildDefinitionId == 26
      and .settings.manualQueueOnly == false
      and .settings.queueOnSourceUpdateOnly == false
      and .settings.validDuration == 0
      and ((.settings.filenamePatterns // []) | length) == 0
      and any(.settings.scope[]?; .repositoryId == $repo and .refName == $target and .matchKind == "Exact"))
  ' <<< "$policies" >/dev/null; then
    fail "Automatic completion requires the blocking Linux server build policy with immediate expiration on target updates."
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
  echo "Checking that Azure main and the candidate still match the successful build."
  check_candidate_refs
  existing=$(azure_git_api pullrequests --get --data-urlencode "searchCriteria.status=active" \
    --data-urlencode "searchCriteria.targetRefName=$main_ref" \
    --data-urlencode "searchCriteria.sourceRefName=$CANDIDATE_REF")
  pr_id=$(jq -r '.value[0].pullRequestId // empty' <<< "$existing")
  if [[ -z "$pr_id" ]]; then
    run_url="${SYSTEM_COLLECTIONURI}${SYSTEM_TEAMPROJECT}/_build/results?buildId=${BUILD_BUILDID}"
    description=$(printf 'Sync upstream `%s` into Azure main at `%s`.\n\nCandidate `%s` passed the [Linux server build and archive smoke test](%s).\n\nThe required Linux build policy verifies that the merge preview still matches this candidate. If either branch advanced, rerun the sync. Use a merge commit to retain upstream history.' \
      "$UPSTREAM_COMMIT" "$CANDIDATE_BASE" "$CANDIDATE_COMMIT" "$run_url")
    body=$(jq -n --arg source "$CANDIDATE_REF" --arg target "$main_ref" --arg description "$description" \
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

case "$1" in
  prepare) prepare ;;
  open-pr) open_pr ;;
  *) fail "Unknown upstream sync step: $1" ;;
esac
