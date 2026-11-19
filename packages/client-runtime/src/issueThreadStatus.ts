import { threadIssueKeyOf, type EnvironmentId, type ThreadIssueKey } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { arrayElementsEqual } from "./state/entities.ts";
import type { EnvironmentThreadShell } from "./state/models.ts";

type IssueLinkedShell = Pick<
  EnvironmentThreadShell,
  "environmentId" | "issues" | "archivedAt" | "deletedAt" | "settledOverride"
>;

const NO_THREADS: ReadonlyArray<never> = [];

/** An issue as one environment's threads link it; tracker-level, like `threadIssueKeysEqual`. */
export function issueThreadIndexKey(environmentId: EnvironmentId, issue: ThreadIssueKey): string {
  return `${environmentId}\u0000${threadIssueKeyOf(issue)}`;
}

/** Open threads (not archived, deleted or settled) by the issues they link, in shell order. */
export function indexOpenIssueThreads<T extends IssueLinkedShell>(
  shells: ReadonlyArray<T>,
): ReadonlyMap<string, ReadonlyArray<T>> {
  const index = new Map<string, T[]>();
  for (const shell of shells) {
    if (
      shell.archivedAt !== null ||
      shell.deletedAt !== null ||
      shell.settledOverride === "settled"
    ) {
      continue;
    }
    const linked = new Set(
      (shell.issues ?? []).map((link) => issueThreadIndexKey(shell.environmentId, link)),
    );
    for (const key of linked) {
      const threads = index.get(key);
      if (threads) threads.push(shell);
      else index.set(key, [shell]);
    }
  }
  return index;
}

export interface IssueThreadStatusChoice<T, S> {
  readonly thread: T;
  readonly status: S;
  /** Open linked threads not shown, for a "+N". */
  readonly others: number;
}

/**
 * The linked thread an issue's status box shows: the lowest `rank` of its status, the earlier
 * thread on a tie. Null when the issue has no open linked thread.
 */
export function mostUrgentIssueThread<T, S>(
  threads: ReadonlyArray<T>,
  statusOf: (thread: T) => S,
  rank: (status: S) => number,
): IssueThreadStatusChoice<T, S> | null {
  let best: { thread: T; status: S; rank: number } | null = null;
  for (const thread of threads) {
    const status = statusOf(thread);
    const statusRank = rank(status);
    if (best === null || statusRank < best.rank) best = { thread, status, rank: statusRank };
  }
  return best && { thread: best.thread, status: best.status, others: threads.length - 1 };
}

/**
 * One index over the shells, and a per-issue atom keyed by `issueThreadIndexKey` that keeps its
 * value while that issue's threads are unchanged, so a list row redraws only for its own threads.
 */
export function createIssueThreadAtoms<T extends IssueLinkedShell>(
  shellsAtom: Atom.Atom<ReadonlyArray<T>>,
) {
  const indexAtom = Atom.make((get) => indexOpenIssueThreads(get(shellsAtom))).pipe(
    Atom.withLabel("issue-open-thread-index"),
  );
  const openIssueThreadsAtom = Atom.family((key: string) => {
    let previous: ReadonlyArray<T> = NO_THREADS;
    return Atom.make((get) => {
      const next = get(indexAtom).get(key) ?? NO_THREADS;
      if (arrayElementsEqual(previous, next)) return previous;
      previous = next;
      return next;
    }).pipe(Atom.withLabel(`issue-open-threads:${key}`));
  });
  return { openIssueThreadsAtom };
}
