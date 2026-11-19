import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  createIssueThreadAtoms,
  issueThreadIndexKey,
} from "@t3tools/client-runtime/issue-thread-status";
import type { EnvironmentId, ThreadIssueKey } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useGT, useMessages } from "gt-react";
import type { CSSProperties } from "react";

import { cn } from "~/lib/utils";
import { environmentThreadShells } from "~/state/threads";
import { buildThreadRouteParams } from "~/threadRoutes";
import { useUiStateStore } from "~/uiStateStore";

import { resolveThreadLastVisitedAt } from "../Sidebar.logic";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { issueThreadStatusBox } from "./issueThreadStatusBox.logic";

const { openIssueThreadsAtom } = createIssueThreadAtoms(
  environmentThreadShells.quietThreadShellsAtom,
);

/**
 * The sidebar's status pill for the most urgent open thread linked to an issue, as a box that
 * opens that thread. Nothing when no open thread links the issue.
 */
export function IssueThreadStatusBox({
  environmentId,
  issue,
  className,
  style,
}: {
  environmentId: EnvironmentId;
  issue: ThreadIssueKey;
  className?: string;
  style?: CSSProperties;
}) {
  const threads = useAtomValue(openIssueThreadsAtom(issueThreadIndexKey(environmentId, issue)));
  if (threads.length === 0) return null;
  return (
    <IssueThreadStatusBoxButton
      threads={threads}
      {...(className === undefined ? {} : { className })}
      {...(style === undefined ? {} : { style })}
    />
  );
}

function IssueThreadStatusBoxButton({
  threads,
  className,
  style,
}: {
  threads: Parameters<typeof issueThreadStatusBox>[0];
  className?: string;
  style?: CSSProperties;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const navigate = useNavigate();
  const localLastVisitedAt = useUiStateStore((state) => state.threadLastVisitedAtById);
  const box = issueThreadStatusBox(threads, (thread) =>
    resolveThreadLastVisitedAt(
      thread.lastVisitedAt,
      localLastVisitedAt[scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id))],
    ),
  );
  if (box === null) return null;
  const { thread, status, others } = box;
  const label = resolve(status.label);
  const more =
    others > 0
      ? gt("{count, plural, one {# more linked thread} other {# more linked threads}}", {
          count: others,
        })
      : null;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={[gt("Open thread {title}", { title: thread.title }), label, more]
              .filter(Boolean)
              .join(", ")}
            onClick={() =>
              void navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(scopeThreadRef(thread.environmentId, thread.id)),
              })
            }
            className={cn(
              "inline-flex h-5 shrink-0 items-center gap-1 rounded border border-border bg-background px-1.5 text-2xs font-medium outline-none hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
              status.colorClass,
              className,
            )}
            style={style}
          />
        }
      >
        <span aria-hidden className={cn("size-1.5 shrink-0 rounded-full", status.dotClass)} />
        <span className="whitespace-nowrap">{label}</span>
        {others > 0 ? <span className="text-muted-foreground tabular-nums">+{others}</span> : null}
      </TooltipTrigger>
      <TooltipPopup>
        {thread.title}
        {more ? <span className="block text-muted-foreground">{more}</span> : null}
      </TooltipPopup>
    </Tooltip>
  );
}
