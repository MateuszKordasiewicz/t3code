# Fork Linux desktop releases

`t3code-linux-desktop` packages the Linux x64 nightly AppImage. Queue it on the
Azure PR's source branch and commit to validate the exact PR head. The default
`publishRelease=false` omits the GitHub release stage and its variable group.

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

For the authorized public cutover, grant only this pipeline access to the
`github-mirror` variable group and keep its branch-control check. Set the
pipeline's default branch to `refs/heads/main` after the YAML lands there.
Wait for any GitHub workflow runs from before the shared publisher was merged
to finish. Queue an exact, mirrored `main` commit with
`--parameters publishRelease=true`. Verify the release tag resolves to that
commit and use an existing fork installation to check and install the update.
Keep the GitHub publisher available through that proof. The Azure pipeline
uses manual queues during this phase.

Azure uses the commit's UTC date and a nightly sequence of 1,000,000,000 plus
its immutable build ID. Both publishers take the same GitHub ref lock before
writing GitHub releases. Retry the same run after a failed publish so it can
reclaim its lock and repair the feed. If the owning run cannot be retried,
inspect the commit message on `fork-linux-publish-lock` for its run ID. Once
that run has stopped, a GitHub maintainer can delete the lock ref with a Git
lease against the observed lock commit.
