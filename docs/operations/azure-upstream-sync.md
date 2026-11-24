# Azure upstream sync

Open [t3code-upstream-sync](https://dev.azure.com/Instablinds/t3code/_build?definitionScope=%5C) in the private t3code project and select **Run pipeline**. Leave **Dry run** selected to compare current Azure `main` with public upstream `main`. The prepare job reports **Already current**, **Clean candidate**, or **Conflict**. On conflict the job fails and lists the files.

Run on `main` with **Dry run** cleared to publish a candidate branch. The sync builds that exact commit on Microsoft-hosted Ubuntu 24.04 using the Linux server pipeline's build and archive smoke steps. A failed validation leaves the candidate branch available for diagnosis. A passing validation publishes `t3-linux-server` on the sync run and opens an Azure PR. The sync verifies that Azure's merge preview has the tested source, target, and file tree. The PR description records the candidate SHA, the tested base SHA, and the validation run. Approve and complete it with a merge commit to retain the upstream history. If either SHA changed, rerun the sync and review the new candidate before completing. Completing a sync PR starts the regular Linux server build on `main`.

The pipeline uses project-scoped job authorization. Its Git operations use the t3code Build Service job token. The identity needs repository read, branch creation, permission to contribute to pull requests, and standard Contribute on `main`. Manual runs leave PR completion to a reviewer. During scheduled runs the Build Service supplies the required vote and requests automatic completion through Azure.

The required [Azure build validation policy](https://learn.microsoft.com/en-us/azure/devops/repos/git/branch-policies#build-validation) runs `t3code-linux-server` on the PR merge commit and expires immediately when `main` changes. For an upstream sync PR, the build verifies that the merge commit still has the original target, candidate SHA, and file tree. If either branch moves, rerun the sync to create and validate a fresh candidate.

After schedule approval, set the pipeline's default branch to `main` and set its variable `UPSTREAM_SYNC_SCHEDULE_ENABLED` to `true`. Leave queue-time overrides off. Azure cron uses UTC, so the pipeline evaluates both 07:00 and 08:00 UTC and runs the sync at the one that corresponds to 03:00 in `America/New_York`. Manual runs are available independently of this switch.

After a successful disposable trial and approval to activate scheduled repair, set `UPSTREAM_SYNC_VM_REPAIR_ENABLED` to `true` on pipeline 29 with queue-time overrides off. A conflicting scheduled run then gives one Codex `gpt-6-luna` turn up to 20 minutes on the existing private pilot. The hosted job sends an expiring download link for the exact merge inputs and retrieves the resulting Git pack through Azure Run Command. The hosted job verifies that Azure deallocated the pilot, then checks the pack, commit parents, and conflict markers before it publishes a candidate. The usual validation and completion policies apply.

## Daytime trial

Point a disposable target under `sync/azure-upstream-trial/` at the Azure commit you want to test. Apply the same blocking pipeline 26 build policy and reviewer policy as on `main`, scoped to that exact disposable branch. Verify that the pilot is deallocated and its previous operator has released it.

In pipeline 29, select **Run pipeline** and choose the branch with the repair workflow. Clear **Dry run**, enter the target suffix under **Disposable target suffix**, enter both full SHAs, and select **repair**. The pipeline uses the scheduled repair, validation, and PR checks against the disposable target. Manual runs leave the PR open. [Azure pipeline 29](https://dev.azure.com/Instablinds/t3code/_build?definitionId=29)

Select **invalid** to test a wrong-parent result or **timeout** to give the T3 turn three seconds. Both runs should fail before candidate publication and record deallocation. To check the hosted-only paths, select **repair** and use an upstream SHA that merges cleanly or is already an ancestor of the target. The pilot steps should be skipped.

The `sync/azure-upstream-control/pilot` ref prevents simultaneous sync runs from taking the pilot. If a failed or canceled run retains its reservation, verify that the guest unit and workspace for that run and attempt were removed, Azure Run Commands were deleted, and the VM is deallocated before releasing the reservation.

Azure DevOps sends failure email for **t3code-upstream-sync** runs. Open the linked run to see the conflicting files or validation error. The PR shows any separate failure from the required merge build.
