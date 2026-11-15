// @effect-diagnostics globalTimers:off - Shelf placement timers run in web and native clients, outside an Effect runtime.
import { isProviderNativeSubagentThread } from "@t3tools/contracts";
import { threadKey } from "./entities.ts";
import { threadRuntimeIsActive, type EnvironmentThreadShell } from "./models.ts";
import { toSortableTimestamp } from "./threadSort.ts";
import { effectiveSnoozed } from "./threadSettled.ts";

type WorkingThreadInput = Pick<
  EnvironmentThreadShell,
  | "hasActionableProposedPlan"
  | "hasPendingApprovals"
  | "hasPendingUserInput"
  | "pendingSubagentRequest"
  | "interactionMode"
  | "latestRun"
  | "runtime"
>;

export function isThreadWorking(thread: WorkingThreadInput): boolean {
  if (thread.hasPendingApprovals || thread.hasPendingUserInput) return false;
  if (thread.pendingSubagentRequest != null) return false;
  if (!threadRuntimeIsActive(thread.runtime) && thread.runtime?.status !== "idle") return false;
  const run = thread.latestRun;
  const runSettled =
    run !== null &&
    run.status !== "preparing" &&
    run.status !== "queued" &&
    run.status !== "starting" &&
    run.status !== "running" &&
    run.status !== "waiting" &&
    thread.runtime?.activeRunId !== run.runId;
  return !(thread.interactionMode === "plan" && thread.hasActionableProposedPlan && runSettled);
}

type InboxThreadInput = Pick<
  EnvironmentThreadShell,
  "id" | "environmentId" | "createdAt" | "unsettledAt" | "latestRun"
>;

export function sortInboxThreadsByReturn<T extends InboxThreadInput>(
  threads: readonly T[],
  observedReturnAt?: (thread: T) => number | undefined,
): T[] {
  const timestamps = new Map(
    threads.map((thread) => [
      thread,
      Math.max(
        toSortableTimestamp(thread.createdAt) ?? 0,
        toSortableTimestamp(thread.unsettledAt ?? undefined) ?? 0,
        toSortableTimestamp(thread.latestRun?.requestedAt ?? undefined) ?? 0,
        toSortableTimestamp(thread.latestRun?.completedAt ?? undefined) ?? 0,
        observedReturnAt?.(thread) ?? 0,
      ),
    ]),
  );
  return sortNewestFirst(threads, timestamps);
}

type WorkingSortInput = Pick<
  EnvironmentThreadShell,
  "id" | "environmentId" | "createdAt" | "latestRun" | "latestUserAuthoredMessageAt"
>;

export function sortWorkingThreadsBySend<T extends WorkingSortInput>(threads: readonly T[]): T[] {
  const timestamps = new Map(
    threads.map((thread) => [
      thread,
      Math.max(
        toSortableTimestamp(thread.createdAt) ?? 0,
        toSortableTimestamp(
          (thread.latestUserAuthoredMessageAt === undefined
            ? thread.latestRun?.requestedAt
            : thread.latestUserAuthoredMessageAt) ?? undefined,
        ) ?? 0,
      ),
    ]),
  );
  return sortNewestFirst(threads, timestamps);
}

function sortNewestFirst<T extends Pick<EnvironmentThreadShell, "id" | "environmentId">>(
  threads: readonly T[],
  timestamps: ReadonlyMap<T, number>,
): T[] {
  return [...threads].sort(
    (left, right) =>
      timestamps.get(right)! - timestamps.get(left)! ||
      left.id.localeCompare(right.id) ||
      left.environmentId.localeCompare(right.environmentId),
  );
}

type TrackedInboxThread = WorkingThreadInput &
  InboxThreadInput &
  Partial<
    Pick<
      EnvironmentThreadShell,
      | "archivedAt"
      | "deletedAt"
      | "pinnedAt"
      | "settledOverride"
      | "settledAt"
      | "snoozedAt"
      | "snoozedUntil"
      | "activeOrderKey"
      | "pendingGuestMessageCount"
    >
  >;

type ThreadKeyInput = Pick<EnvironmentThreadShell, "environmentId" | "id">;

/**
 * How long a thread that stopped working keeps reading as working. An orchestrator
 * run can end moments before its continuation starts, so clients wait this long
 * before treating the thread as done.
 */
const WORKING_EXIT_GRACE_MS = 2_000;

/**
 * Keep one at module scope so placement survives routes that unmount the list. Call
 * `observe` when the shells or the snapshot version change, or with null while the beta
 * is off. Its result stays fixed, so a list built from it can be cached.
 */
export function createInboxReturnTracker() {
  const keyOf = (thread: ThreadKeyInput) => `${thread.environmentId}:${thread.id}`;
  const states = new Map<
    string,
    { working: boolean; deadline: number | null; placement: string | null }
  >();
  const returns = new Map<string, number>();
  const listeners = new Set<() => void>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let version = 0;
  const now = () => {
    // @effect-diagnostics-next-line globalDate:off - The grace deadline is wall-clock time shared by web and native clients.
    return Date.now();
  };
  const schedule = () => {
    clearTimeout(timer);
    timer = undefined;
    let next = Infinity;
    for (const state of states.values()) {
      if (state.deadline !== null) next = Math.min(next, state.deadline);
    }
    if (next === Infinity) return;
    timer = setTimeout(
      () => {
        const at = now();
        for (const [key, state] of states) {
          if (state.deadline !== null && state.deadline <= at) {
            state.working = false;
            state.deadline = null;
            returns.set(key, at);
          }
        }
        schedule();
        version++;
        for (const listener of listeners) listener();
      },
      Math.max(0, next - now()),
    );
  };
  return {
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    getSnapshot: () => version,
    observe(threads: readonly TrackedInboxThread[] | null): {
      readonly isWorking: (thread: WorkingThreadInput & ThreadKeyInput) => boolean;
      readonly returnedAt: (thread: ThreadKeyInput) => number | undefined;
    } {
      if (threads === null) {
        clearTimeout(timer);
        timer = undefined;
        states.clear();
        returns.clear();
        return { isWorking: isThreadWorking, returnedAt: () => undefined };
      }
      const present = new Set<string>();
      const at = now();
      // @effect-diagnostics-next-line globalDate:off - Snooze checks compare wall-clock ISO times.
      const observedAt = new Date(at).toISOString();
      for (const thread of threads) {
        const key = keyOf(thread);
        present.add(key);
        const previous = states.get(key);
        const actual = isThreadWorking(thread);
        const placement =
          actual || previous?.working
            ? JSON.stringify([
                thread.archivedAt,
                thread.deletedAt,
                thread.pinnedAt,
                thread.settledOverride,
                thread.settledAt,
                thread.snoozedAt,
                thread.snoozedUntil,
                thread.activeOrderKey,
              ])
            : null;
        const actionable =
          thread.hasPendingApprovals ||
          thread.hasPendingUserInput ||
          thread.pendingSubagentRequest != null ||
          (thread.pendingGuestMessageCount ?? 0) > 0 ||
          (thread.interactionMode === "plan" && thread.hasActionableProposedPlan) ||
          thread.runtime?.lastError != null ||
          thread.latestRun?.status === "failed";
        const moved = previous !== undefined && previous.placement !== placement;
        const excluded =
          thread.archivedAt != null ||
          thread.deletedAt != null ||
          thread.pinnedAt != null ||
          thread.settledOverride === "settled" ||
          effectiveSnoozed(thread, { now: observedAt });
        // Only shelf placement is held: execution state and attention requests stay authoritative.
        const deadline =
          !actual && previous?.working && !actionable && !moved && !excluded
            ? (previous.deadline ?? at + WORKING_EXIT_GRACE_MS)
            : null;
        const working = actual || (deadline !== null && deadline > at);
        if (previous?.working && !working) returns.set(key, at);
        states.set(key, { working, deadline: working ? deadline : null, placement });
      }
      for (const key of states.keys()) {
        if (!present.has(key)) {
          states.delete(key);
          returns.delete(key);
        }
      }
      schedule();
      const observedWorking = new Map<string, boolean>();
      states.forEach((state, key) => observedWorking.set(key, state.working));
      const observedReturns = new Map(returns);
      return {
        isWorking: (thread) => observedWorking.get(keyOf(thread)) ?? isThreadWorking(thread),
        returnedAt: (thread) => observedReturns.get(keyOf(thread)),
      };
    },
    isWorking: (thread: WorkingThreadInput & ThreadKeyInput) =>
      states.get(keyOf(thread))?.working ?? isThreadWorking(thread),
    returnedAt: (thread: ThreadKeyInput) => returns.get(keyOf(thread)),
  };
}

export type WorkInboxStatus =
  | "approval"
  | "input"
  | "guest"
  | "authentication"
  | "failed"
  | "limited"
  | "preparing"
  | "queued"
  | "running"
  | "waiting"
  | "coordinator-input"
  | "idle";

function observedStatus(thread: EnvironmentThreadShell): WorkInboxStatus {
  const request = thread.source.pendingRuntimeRequest;
  if (
    request !== null &&
    request.kind !== "dynamic_tool_call" &&
    !isProviderNativeSubagentThread(thread.source)
  ) {
    if (request.kind === "auth_refresh") return "authentication";
    if (request.kind !== "user_input") return "approval";
    const coordinatorOwned =
      thread.lineage.sideChat !== true &&
      (thread.lineage.relationshipToParent === "subagent" || thread.delegatedFrom !== null);
    if (!coordinatorOwned) return "input";
    const childLive =
      thread.source.activityRunStatus !== undefined
        ? thread.source.activityRunStatus !== null
        : ["preparing", "starting", "running", "waiting"].includes(thread.source.status);
    if (childLive) return "coordinator-input";
  }
  if ((thread.pendingGuestMessageCount ?? 0) > 0) return "guest";
  if (thread.runtime?.status === "failed") {
    return thread.runtime.lastErrorClass === "usage_limit" ? "limited" : "failed";
  }
  if (thread.blockedOnThreadWait === true) return "waiting";
  switch (thread.runtime?.status) {
    case "preparing":
      return "preparing";
    case "queued":
      return "queued";
    case "starting":
    case "running":
      return "running";
    case "waiting":
      return "waiting";
    default:
      return "idle";
  }
}

const needsAttention = (status: WorkInboxStatus) =>
  status === "approval" ||
  status === "input" ||
  status === "guest" ||
  status === "authentication" ||
  status === "failed" ||
  status === "limited";

export function workInboxRows(threads: ReadonlyArray<EnvironmentThreadShell>) {
  // Resolve relationships only within the caller's authorized, tag-filtered shells.
  const visible = new Map(
    threads
      .filter((thread) => thread.deletedAt === null)
      .map(
        (thread) =>
          [
            threadKey({ environmentId: thread.environmentId, threadId: thread.id }),
            thread,
          ] as const,
      ),
  );
  return [...visible.entries()].map(([key, thread]) => {
    const status = observedStatus(thread);
    const parentRef =
      thread.delegatedFrom ??
      ((thread.lineage.relationshipToParent === "subagent" || thread.lineage.sideChat === true) &&
      thread.lineage.parentThreadId !== null
        ? { environmentId: thread.environmentId, threadId: thread.lineage.parentThreadId }
        : null);
    const candidate = parentRef === null ? null : (visible.get(threadKey(parentRef)) ?? null);
    const parent =
      candidate?.archivedAt === null &&
      (candidate.id !== thread.id || candidate.environmentId !== thread.environmentId)
        ? candidate
        : null;
    const request = thread.source.pendingRuntimeRequest;
    return {
      key,
      thread,
      status,
      parent,
      attention: needsAttention(status),
      requestKey:
        request !== null &&
        (status === "approval" || status === "input" || status === "authentication")
          ? JSON.stringify([thread.environmentId, thread.id, request.id])
          : null,
    };
  });
}
