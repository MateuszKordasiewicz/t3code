import { mostUrgentIssueThread } from "@t3tools/client-runtime/issue-thread-status";
import { msg } from "gt-react";

import type { SidebarThreadSummary } from "../../types";
import { resolveThreadStatusPill, type ThreadStatusPill } from "../Sidebar.logic";

// resolveThreadStatusPill's check order, most urgent first.
const PILL_ORDER: ReadonlyArray<string> = [
  msg("Pending Approval"),
  msg("Awaiting Input"),
  msg("Guest Request"),
  msg("Working"),
  msg("Connecting"),
  msg("Waiting"),
  msg("Failed"),
  msg("Plan Ready"),
  msg("Completed"),
];

/** An open thread the sidebar shows no pill for: its turn ended and nobody settled it. */
export const IDLE_ISSUE_THREAD_PILL: ThreadStatusPill = {
  label: msg("Waiting"),
  colorClass: "text-muted-foreground",
  dotClass: "bg-muted-foreground",
  pulse: false,
};

function pillRank(pill: ThreadStatusPill): number {
  if (pill === IDLE_ISSUE_THREAD_PILL) return PILL_ORDER.length + 1;
  const index = PILL_ORDER.indexOf(pill.label);
  return index === -1 ? PILL_ORDER.length : index;
}

/** The thread an issue's box shows, with the sidebar's pill for it. */
export function issueThreadStatusBox(
  threads: ReadonlyArray<SidebarThreadSummary>,
  lastVisitedAt: (thread: SidebarThreadSummary) => string | undefined,
) {
  return mostUrgentIssueThread(
    threads,
    (thread) =>
      resolveThreadStatusPill({ thread: { ...thread, lastVisitedAt: lastVisitedAt(thread) } }) ??
      IDLE_ISSUE_THREAD_PILL,
    pillRank,
  );
}
