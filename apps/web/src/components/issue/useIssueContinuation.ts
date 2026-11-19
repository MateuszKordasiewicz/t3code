import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { issueThreadPrompt } from "@t3tools/client-runtime/issue-presentation";
import type {
  EnvironmentId,
  IssueContinuationResult,
  IssueDetail,
  ThreadId,
  ThreadIssueLinkTarget,
} from "@t3tools/contracts";
import { useRouter } from "@tanstack/react-router";
import { msg } from "gt-react";
import { useRef, useState } from "react";

import { toastManager } from "~/components/ui/toast";
import { useNewThreadHandler } from "~/hooks/useHandleNewThread";
import { m } from "~/i18n/messages";
import { issueEnvironment } from "~/state/issues";
import { useAtomCommand } from "~/state/use-atom-command";
import { buildThreadRouteParams } from "~/threadRoutes";

const blockedCopy = {
  "source-unavailable": msg("The original thread is no longer linked to this issue."),
  "answer-needed": msg("The original thread has a request that needs an answer."),
  "thread-busy": msg("The original thread is still working."),
  "worktree-unavailable": msg("The original Git worktree or branch could not be verified."),
  "worktree-busy": msg("Another thread is using the original worktree."),
} satisfies Record<Exclude<IssueContinuationResult["status"], "ready">, ReturnType<typeof msg>>;

export function useIssueContinuation(environmentId: EnvironmentId) {
  const router = useRouter();
  const newThread = useNewThreadHandler();
  const check = useAtomCommand(issueEnvironment.checkContinuation, { reportFailure: false });
  const checking = useRef(false);
  const [pending, setPending] = useState(false);

  const start = async (
    thread: { id: ThreadId; title: string },
    issue: IssueDetail,
    link: ThreadIssueLinkTarget,
  ) => {
    if (checking.current) return;
    checking.current = true;
    setPending(true);
    const href = router.state.location.href;
    const source = { threadId: thread.id, issue: link };
    const showBlocked = (description: string) =>
      toastManager.add({
        type: "warning",
        title: m(msg("Could not continue in this worktree")),
        description,
        timeout: 0,
        actionProps: {
          children: m(msg("Open original thread")),
          onClick: () =>
            void router.navigate({
              to: "/$environmentId/$threadId",
              params: buildThreadRouteParams(scopeThreadRef(environmentId, thread.id)),
            }),
        },
      });
    try {
      const checked = await check({ environmentId, input: source });
      if (router.state.location.href !== href) return;
      if (checked._tag === "Failure") {
        showBlocked(m(msg("The original worktree could not be verified.")));
        return;
      }
      const result = checked.value;
      if (result.status !== "ready") {
        showBlocked(m(blockedCopy[result.status]));
        return;
      }
      await newThread(scopeProjectRef(environmentId, result.projectId), {
        envMode: "worktree",
        worktreePath: result.worktreePath,
        branch: result.branch,
        startFromOrigin: false,
        issueLink: link,
        prompt: [
          m(
            msg(
              "Continue work on this issue. Read the original thread and inspect the existing changes first.",
            ),
          ),
          issueThreadPrompt(issue),
        ].join("\n\n"),
        continuation: { source, title: thread.title },
      });
    } catch {
      showBlocked(m(msg("The continuation draft could not be opened.")));
    } finally {
      checking.current = false;
      setPending(false);
    }
  };
  return { pending, start };
}
