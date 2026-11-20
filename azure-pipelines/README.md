# Fork Linux desktop releases

`t3code-linux-desktop` packages the Linux x64 nightly AppImage. Queue it on the
Azure PR's source branch and commit to validate the exact PR head. The default
`publishRelease=false` omits the GitHub release stage and its variable group
on manual runs.

```sh
az pipelines run --project t3code --name t3code-linux-desktop \
  --branch "refs/heads/$branch" --commit-id "$commit"
az pipelines runs artifact download --project t3code --run-id "$run_id" \
  --artifact-name t3-linux-desktop --path ./t3-linux-desktop
(cd t3-linux-desktop/assets && sha256sum -c SHA256SUMS)
```

`build-info.json` identifies the source commit and version. The build checks the
manifest, the AppImage's embedded blockmap and SHA-512, and the packaged
`app-update.yml`. It launches the AppImage with `--version` under Xvfb.

An eligible `main` CI run of `t3code-github-mirror` marks its Azure run
`desktop-release-ready` after it has mirrored the source commit. Successful
runs with that tag trigger the desktop pipeline at the same commit, including
when the mirror pushed a newer `main` that contains it. Git tag pushes and
manual mirror runs do not qualify.

Keep the desktop pipeline's default branch at `refs/heads/main` and its
scoped access to the branch-restricted `github-mirror` group. For a manual
public release, queue an exact mirrored `main` commit with
`--parameters publishRelease=true`. Keep the GitHub publisher available until
the first automatic Azure release resolves to the mirrored commit and an
existing fork installation can install it.

Azure uses the commit's UTC date and a nightly sequence of 1,000,000,000 plus
the mirror run ID for automatic releases or the desktop run ID for manual
releases. Retrying an automatic release keeps its sequence. Both publishers
take the same GitHub ref lock before writing GitHub releases. Retry the same
run after a failed publish so it can
reclaim its lock and repair the feed. If the owning run cannot be retried,
inspect the commit message on `fork-linux-publish-lock` for its run ID. Once
that run has stopped, a GitHub maintainer can delete the lock ref with a Git
lease against the observed lock commit.
