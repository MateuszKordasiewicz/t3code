import * as Option from "effect/Option";
import type { AsyncResult } from "effect/reactivity";

/** The issue detail query's `staleTimeMs` (client-runtime `createIssueEnvironmentAtoms`). */
export const ISSUE_DETAIL_STALE_MS = 30_000;

/** Stale the way the query's own mount revalidation sees it, except that no answer yet is stale. */
function isStale(result: AsyncResult.AsyncResult<unknown, unknown>, now: number) {
  if (result.waiting) return false;
  const success =
    result._tag === "Failure" ? Option.getOrUndefined(result.previousSuccess) : result;
  if (success?._tag !== "Success") return true;
  return now - success.timestamp >= ISSUE_DETAIL_STALE_MS;
}

/**
 * What a queued read does with the detail's current result. One fetch per turn: once a fetch has
 * started, the next settled result ends the turn, even a failure that arrived without waiting.
 */
export function issueDetailReadStep(
  result: AsyncResult.AsyncResult<unknown, unknown>,
  fetched: boolean,
  now: number,
): "wait" | "fetch" | "done" {
  if (result.waiting) return "wait";
  return !fetched && isStale(result, now) ? "fetch" : "done";
}

/**
 * idle: not wanted yet. queued: waiting for its environment's one slot. fetching: its read holds
 * the slot. settled: read, or the open tab, which reads its detail outside the line. failed: read
 * without an answer; only a hover or focus tries again, so an offline environment cannot loop.
 */
export type IssueTabIconPhase = "idle" | "queued" | "fetching" | "settled" | "failed";

/**
 * Tab icons read issue detail only to draw a state mark, and every read is a tracker CLI call on
 * the server (`bd show` runs one at a time per database). One read per environment runs at a
 * time, so a strip of issue tabs cannot crowd out the Issues list's own request. `read` must call
 * `settle` once its read has finished; the slot is held until then, even if the tab closes.
 */
export function createIssueTabIconQueue<Job>(
  read: (job: Job, settle: (answered: boolean) => void) => void,
) {
  const phases = new Map<object, IssueTabIconPhase>();
  const waiting = new Map<string, Array<{ readonly ticket: object; readonly job: Job }>>();
  const busy = new Set<string>();
  const listeners = new Set<() => void>();

  const notify = () => {
    for (const listener of listeners) listener();
  };
  const leaveLine = (ticket: object) => {
    for (const line of waiting.values()) {
      const index = line.findIndex((entry) => entry.ticket === ticket);
      if (index >= 0) line.splice(index, 1);
    }
  };
  const pump = (environmentId: string) => {
    if (busy.has(environmentId)) return;
    const next = waiting.get(environmentId)?.shift();
    if (next === undefined) return;
    busy.add(environmentId);
    phases.set(next.ticket, "fetching");
    let settled = false;
    read(next.job, (answered) => {
      if (settled) return;
      settled = true;
      busy.delete(environmentId);
      if (phases.get(next.ticket) === "fetching") {
        phases.set(next.ticket, answered ? "settled" : "failed");
      }
      pump(environmentId);
      notify();
    });
  };

  return {
    phase: (ticket: object): IssueTabIconPhase => phases.get(ticket) ?? "idle",
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    /** A visible tab joins the back of the line; a hovered or focused one goes to the front. */
    request: (ticket: object, environmentId: string, job: Job, urgent: boolean) => {
      const phase = phases.get(ticket) ?? "idle";
      if (phase === "fetching" || phase === "settled") return;
      if ((phase === "queued" || phase === "failed") && !urgent) return;
      leaveLine(ticket);
      const line = waiting.get(environmentId) ?? [];
      waiting.set(environmentId, line);
      if (urgent) line.unshift({ ticket, job });
      else line.push({ ticket, job });
      phases.set(ticket, "queued");
      pump(environmentId);
      notify();
    },
    /** The open tab reads its detail at once; its panel is fetching the same detail anyway. */
    skip: (ticket: object) => {
      const phase = phases.get(ticket);
      if (phase === "settled" || phase === "fetching") return;
      leaveLine(ticket);
      phases.set(ticket, "settled");
      notify();
    },
    /** A closed tab leaves the line; a read it started keeps the slot until it settles. */
    drop: (ticket: object) => {
      if (!phases.has(ticket)) return;
      leaveLine(ticket);
      phases.delete(ticket);
      notify();
    },
  };
}
