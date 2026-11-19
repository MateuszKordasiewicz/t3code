import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  createIssueThreadAtoms,
  issueThreadIndexKey,
} from "@t3tools/client-runtime/issue-thread-status";
import type { EnvironmentId, ThreadIssueKey } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useGT, useMessages } from "gt-react";

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

const NO_VISITS: Record<string, string> = {};

/** The most urgent open thread linked to an issue, with the sidebar's pill for it; or null. */
export function useIssueThreadStatus(environmentId: EnvironmentId, issue: ThreadIssueKey) {
  const threads = useAtomValue(openIssueThreadsAtom(issueThreadIndexKey(environmentId, issue)));
  // Only issues with a thread follow visits, so a visit redraws only their rows.
  const localLastVisitedAt = useUiStateStore((state) =>
    threads.length === 0 ? NO_VISITS : state.threadLastVisitedAtById,
  );
  return issueThreadStatusBox(threads, (thread) =>
    resolveThreadLastVisitedAt(
      thread.lastVisitedAt,
      localLastVisitedAt[scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id))],
    ),
  );
}

export type IssueThreadStatus = NonNullable<ReturnType<typeof useIssueThreadStatus>>;

/**
 * The sidebar's status pill for the most urgent open thread linked to an issue, as a box that
 * opens that thread. Nothing when no open thread links the issue.
 */
export function IssueThreadStatusBox({
  environmentId,
  issue,
  className,
}: {
  environmentId: EnvironmentId;
  issue: ThreadIssueKey;
  className?: string;
}) {
  const box = useIssueThreadStatus(environmentId, issue);
  if (box === null) return null;
  return <IssueThreadStatusBoxView box={box} {...(className === undefined ? {} : { className })} />;
}

const BOX_CLASS =
  "inline-flex h-5 shrink-0 items-center gap-1 rounded border border-border bg-background px-1.5 text-2xs font-medium";

function BoxContent({ box, label }: { box: IssueThreadStatus; label: string }) {
  return (
    <>
      <span aria-hidden className={cn("size-1.5 shrink-0 rounded-full", box.status.dotClass)} />
      <span className="whitespace-nowrap">{label}</span>
      {box.others > 0 ? (
        <span className="text-muted-foreground tabular-nums">+{box.others}</span>
      ) : null}
    </>
  );
}

/** The box's size with nothing to see or press, for a layout that draws the box over it. */
export function IssueThreadStatusBoxSpacer({ box }: { box: IssueThreadStatus }) {
  const resolve = useMessages();
  return (
    <span aria-hidden className={cn(BOX_CLASS, "invisible")}>
      <BoxContent box={box} label={resolve(box.status.label)} />
    </span>
  );
}

export function IssueThreadStatusBoxView({
  box,
  className,
}: {
  box: IssueThreadStatus;
  className?: string;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const navigate = useNavigate();
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
              BOX_CLASS,
              "outline-none hover:bg-accent focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
              status.colorClass,
              className,
            )}
          />
        }
      >
        <BoxContent box={box} label={label} />
      </TooltipTrigger>
      <TooltipPopup>
        {thread.title}
        {more ? <span className="block text-muted-foreground">{more}</span> : null}
      </TooltipPopup>
    </Tooltip>
  );
}
