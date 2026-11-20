# Azure upstream sync

Open [t3code-upstream-sync](https://dev.azure.com/Instablinds/t3code/_build?definitionScope=%5C) in the private t3code project and select **Run pipeline**. Leave **Dry run** selected to compare current Azure `main` with public upstream `main`. The prepare job reports **Already current**, **Clean candidate**, or **Conflict**. On conflict the job fails and lists the files.

Run on `main` with **Dry run** cleared to publish a candidate branch. The sync builds that exact commit on Microsoft-hosted Ubuntu 24.04 using the Linux server pipeline's build and archive smoke steps. A failed validation leaves the candidate branch available for diagnosis. A passing validation publishes `t3-linux-server` on the sync run and opens an Azure PR. The sync verifies that Azure's merge preview has the tested source, target, and file tree. The PR description records the candidate SHA, the tested base SHA, and the validation run. Approve and complete it with a merge commit to retain the upstream history. If either SHA changed, rerun the sync and review the new candidate before completing. Completing a sync PR starts the regular Linux server build on `main`.

The pipeline uses project-scoped job authorization. Its Git operations use the t3code Build Service job token. The identity needs repository read, branch creation, permission to contribute to pull requests, and standard Contribute on `main`. Manual runs leave PR completion to a reviewer. During scheduled runs the Build Service supplies the required vote and requests automatic completion through Azure.

The required [Azure build validation policy](https://learn.microsoft.com/en-us/azure/devops/repos/git/branch-policies#build-validation) runs `t3code-linux-server` on the PR merge commit and expires immediately when `main` changes. For an upstream sync PR, the build verifies that the merge commit still has the original target, candidate SHA, and file tree. If either branch moves, rerun the sync to create and validate a fresh candidate.

After schedule approval, set the pipeline's default branch to `main` and set its variable `UPSTREAM_SYNC_SCHEDULE_ENABLED` to `true`. Leave queue-time overrides off. Azure cron uses UTC, so the pipeline evaluates both 07:00 and 08:00 UTC and runs the sync at the one that corresponds to 03:00 in `America/New_York`. Manual runs are available independently of this switch.

Azure DevOps sends failure email for **t3code-upstream-sync** runs. Open the linked run to see the conflicting files or validation error. The PR shows any separate failure from the required merge build.
