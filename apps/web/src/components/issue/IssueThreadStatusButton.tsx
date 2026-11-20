import { useAtomValue } from "@effect/atom-react";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { issueThreadLinkTarget } from "@t3tools/client-runtime/issue-links";
import { issueThreadPrompt } from "@t3tools/client-runtime/issue-presentation";
import {
  issueThreads,
  type IssueThread,
  type IssueThreadStatus,
} from "@t3tools/client-runtime/issue-threads";
import { visibleLinkedThreads } from "@t3tools/client-runtime/state/threads";
import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import type { EnvironmentId, IssueDetail, IssueRef, ThreadId } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import {
  ActivityIcon,
  CheckIcon,
  ChevronDownIcon,
  MessageSquareIcon,
  PlayIcon,
  SquarePenIcon,
} from "lucide-react";
import { msg, useGT } from "gt-react";
import { useState } from "react";

import { useNewThreadHandler } from "~/hooks/useHandleNewThread";
import { m } from "~/i18n/messages";
import {
  useAnyEnvironmentHiding,
  useEnvironmentThreadRefs,
  useQuietThreadShells,
} from "~/state/entities";
import { useServerConfigs } from "~/state/entities";
import { issueEnvironment } from "~/state/issues";
import { useEnvironmentQuery } from "~/state/query";
import { useEnvironmentScope } from "~/state/session";
import { environmentThreadShells } from "~/state/threads";
import { buildThreadRouteParams } from "~/threadRoutes";

import { Button } from "../ui/button";
import { Group, GroupSeparator } from "../ui/group";
import {
  Menu,
  MenuGroup,
  MenuGroupLabel,
  MenuItem,
  MenuItemDetail,
  MenuItemLabel,
  MenuPopup,
  MenuSeparator,
  MenuSub,
  MenuSubPopup,
  MenuSubTrigger,
  MenuTrigger,
} from "../ui/menu";
import { toastManager } from "../ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { readableFailure } from "../pullRequest/pullRequestDetail.logic";

import { useIssueContinuation } from "./useIssueContinuation";

const STATUS_ICON: Record<IssueThreadStatus, typeof ActivityIcon> = {
  running: ActivityIcon,
  idle: MessageSquareIcon,
  done: CheckIcon,
};

/**
 * Whether an agent is on the issue, as one control: with no linked thread it starts one (a
 * worktree draft with the issue as an unsent prompt); otherwise it opens the running or most
 * recent linked thread, and its menu lists the linked threads and resumes the newest idle one.
 */
export function IssueThreadStatusButton({
  environmentId,
  reference,
  issue,
}: {
  environmentId: EnvironmentId;
  reference: IssueRef;
  issue: IssueDetail;
}) {
  const gt = useGT();
  const navigate = useNavigate();
  const canOperate = useEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
  const newThread = useNewThreadHandler();
  const continuation = useIssueContinuation(environmentId);
  const [pending, setPending] = useState(false);
  const { tracker, host, scope, key } = issueThreadLinkTarget(reference, issue);
  const capabilities = useServerConfigs().get(environmentId)?.environment.capabilities;
  const listsThreads = capabilities?.issueLinkedThreads === true;
  const linked = useEnvironmentQuery(
    listsThreads
      ? issueEnvironment.linkedThreads({ environmentId, input: { tracker, host, scope, key } })
      : null,
  );
  const shells = useQuietThreadShells();
  const hiddenKeys = useAtomValue(environmentThreadShells.hiddenThreadKeysAtom);
  const hidingAnywhere = useAnyEnvironmentHiding();
  const loadedThreadRefs = useEnvironmentThreadRefs(environmentId);

  const shellOf = (threadId: ThreadId) =>
    shells.find((shell) => shell.environmentId === environmentId && shell.id === threadId);
  // A server without `issues.linkedThreads` counts as having none once its config is known.
  const answered =
    linked.data !== null || linked.error !== null || (capabilities !== undefined && !listsThreads);
  const { threads, primary } = issueThreads(
    visibleLinkedThreads(
      linked.data?.threads ?? [],
      environmentId,
      loadedThreadRefs,
      hiddenKeys,
      hidingAnywhere,
    ),
    shellOf,
  );

  const start = () => {
    if (pending) return;
    setPending(true);
    newThread(scopeProjectRef(environmentId, reference.projectId), {
      envMode: "worktree",
      branch: null,
      worktreePath: null,
      prompt: issueThreadPrompt(issue),
      issueLink: issueThreadLinkTarget(reference, issue),
    }).then(
      () => setPending(false),
      (error: unknown) => {
        setPending(false);
        toastManager.add({
          type: "error",
          title: m(msg("Could not start a thread")),
          description: readableFailure(error, m(msg("Try again from the issue."))),
        });
      },
    );
  };
  const open = (thread: IssueThread) =>
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(scopeThreadRef(environmentId, thread.id)),
    });
  const statusLabel = (status: IssueThreadStatus) =>
    status === "running" ? gt("Running") : status === "idle" ? gt("Idle") : gt("Done");

  if (!answered) return null;
  if (primary === null) {
    if (!canOperate) return null;
    return (
      <Button size="xs" variant="default" disabled={pending} onClick={start}>
        <SquarePenIcon aria-hidden />
        {gt("Start thread")}
      </Button>
    );
  }
  const PrimaryIcon = STATUS_ICON[primary.status];
  const earlier = threads.filter((thread) => thread.id !== primary.id);
  // Only the newest idle thread can be resumed: done ones were settled, and while a thread runs
  // it is the current attempt, answered in place.
  const resumable =
    canOperate && tracker === "beads" && primary.status !== "running"
      ? (threads.find((thread) => thread.status === "idle") ?? null)
      : null;
  const threadItem = (thread: IssueThread) => {
    const Icon = STATUS_ICON[thread.status];
    return (
      <MenuItem key={thread.id} onClick={() => open(thread)}>
        <Icon aria-hidden />
        <MenuItemLabel>{thread.title}</MenuItemLabel>
        <MenuItemDetail>{statusLabel(thread.status)}</MenuItemDetail>
      </MenuItem>
    );
  };
  return (
    <Group aria-label={gt("Linked threads")}>
      <Tooltip>
        <TooltipTrigger
          render={<Button size="xs" variant="outline" onClick={() => open(primary)} />}
        >
          <PrimaryIcon aria-hidden />
          {primary.status === "running"
            ? gt("Thread running")
            : primary.status === "idle"
              ? gt("Thread idle")
              : gt("Thread done")}
        </TooltipTrigger>
        <TooltipPopup>{primary.title}</TooltipPopup>
      </Tooltip>
      <GroupSeparator />
      <Menu>
        <MenuTrigger
          render={<Button size="icon-xs" variant="outline" aria-label={gt("Linked threads")} />}
        >
          <ChevronDownIcon aria-hidden />
        </MenuTrigger>
        <MenuPopup align="end">
          <MenuGroup>
            <MenuGroupLabel>{gt("Linked threads")}</MenuGroupLabel>
            {threadItem(primary)}
            {earlier.length > 0 ? (
              <MenuSub>
                <MenuSubTrigger>
                  <MenuItemLabel>
                    {gt("Earlier threads ({count})", { count: earlier.length })}
                  </MenuItemLabel>
                </MenuSubTrigger>
                <MenuSubPopup>{earlier.map(threadItem)}</MenuSubPopup>
              </MenuSub>
            ) : null}
          </MenuGroup>
          {resumable !== null ? (
            <>
              <MenuSeparator />
              <MenuItem
                disabled={continuation.pending}
                onClick={() =>
                  void continuation.start(resumable, issue, issueThreadLinkTarget(reference, issue))
                }
              >
                <PlayIcon aria-hidden />
                <MenuItemLabel>{gt("Resume in a new thread")}</MenuItemLabel>
                <MenuItemDetail>{shellOf(resumable.id)?.branch ?? resumable.title}</MenuItemDetail>
              </MenuItem>
            </>
          ) : null}
          {canOperate ? (
            <>
              <MenuSeparator />
              <MenuItem disabled={pending} onClick={start}>
                <SquarePenIcon aria-hidden />
                {gt("Start another thread")}
              </MenuItem>
            </>
          ) : null}
        </MenuPopup>
      </Menu>
    </Group>
  );
}
