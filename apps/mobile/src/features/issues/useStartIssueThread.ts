import { StackActions, useNavigation } from "@react-navigation/native";
import { issueThreadLinkTarget } from "@t3tools/client-runtime/issue-links";
import { issueThreadPrompt } from "@t3tools/client-runtime/issue-presentation";
import { settlePromise, squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { AuthOrchestrationOperateScope, type IssueDetail } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { useGT } from "gt-react-native";
import { useRef } from "react";

import * as Dialog from "../../components/ui/Dialog";
import { useEnvironmentScope } from "../../state/session";
import {
  createNewTaskDraft,
  setComposerDraftText,
  updateComposerDraftSettings,
  waitForComposerDraftsLoaded,
} from "../../state/use-composer-drafts";
import { usePullRequestConnected } from "../pull-requests/usePullRequestConnected";
import { issueTarget, type IssueParams } from "./issueParams";

/**
 * Opens an unsent new-task draft in a fresh worktree of the project the issue is read through,
 * with the issue as its prompt. The thread links the issue once its first turn starts.
 * Null until the detail (and its body) has loaded, or when this connection cannot run threads.
 */
export function useStartIssueThread(params: IssueParams, detail: IssueDetail | null) {
  const gt = useGT();
  const target = issueTarget(params);
  const navigation = useNavigation();
  const connected = usePullRequestConnected(params.environmentId);
  const canOperate = useEnvironmentScope(target.environmentId, AuthOrchestrationOperateScope);
  const pending = useRef(false);
  // Offline, access is unknown until reconnecting; the outbox checks it before sending.
  if (!detail || (connected && !canOperate)) return null;
  return async () => {
    if (pending.current) return;
    pending.current = true;
    const result = await settlePromise(async () => {
      await waitForComposerDraftsLoaded();
      const draftKey = createNewTaskDraft({
        environmentId: target.environmentId,
        projectId: target.input.projectId,
      });
      updateComposerDraftSettings(draftKey, {
        workspaceSelection: {
          mode: "worktree",
          branch: null,
          worktreePath: null,
          issueLink: issueThreadLinkTarget(target.input, detail),
        },
      });
      setComposerDraftText(draftKey, issueThreadPrompt(detail));
      navigation.dispatch(
        StackActions.push("Thread", { draft: { draftId: draftKey, title: detail.title } }),
      );
    });
    pending.current = false;
    if (AsyncResult.isFailure(result)) {
      const cause = squashAtomCommandFailure(result);
      Dialog.alert(
        gt("Could not start a thread"),
        cause instanceof Error ? cause.message : gt("Try again."),
      );
    }
  };
}
