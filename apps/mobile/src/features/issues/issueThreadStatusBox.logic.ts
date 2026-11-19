import { mostUrgentIssueThread } from "@t3tools/client-runtime/issue-thread-status";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

import {
  resolveThreadListV2RowStatus,
  threadHasUnseenCompletion,
  type ThreadListV2Status,
} from "../threads/threadListV2";

/** The thread list's row status, with `ready` split into an unseen `done` and a plain `idle`. */
export type IssueThreadBoxStatus = Exclude<ThreadListV2Status, "ready"> | "done" | "idle";

// Web's resolveThreadStatusPill order, most urgent first, so both clients pick the same thread.
const RANK: ReadonlyArray<IssueThreadBoxStatus> = [
  "approval",
  "input",
  "request",
  "working",
  "waiting",
  "failed",
  "limited",
  "done",
  "idle",
];

function listStatus(thread: EnvironmentThreadShell): IssueThreadBoxStatus {
  const status = resolveThreadListV2RowStatus(thread);
  if (status === "ready") return threadHasUnseenCompletion(thread) ? "done" : "idle";
  // The list shows an idle runtime as waiting; it ranks as idle, behind a failure.
  if (status === "waiting" && thread.runtime?.status === "idle") return "idle";
  return status;
}

// Web's Waiting pill also covers background work holding the thread, ahead of everything below it.
function boxStatus(thread: EnvironmentThreadShell): IssueThreadBoxStatus {
  const status = listStatus(thread);
  return RANK.indexOf(status) > RANK.indexOf("waiting") &&
    backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks)
    ? "waiting"
    : status;
}

/** The thread an issue's box shows, by the same priority as web. */
export function issueThreadStatusBox(threads: ReadonlyArray<EnvironmentThreadShell>) {
  return mostUrgentIssueThread(threads, boxStatus, (status) => RANK.indexOf(status));
}
