import type { ThreadTriageLabel } from "@t3tools/contracts";

export interface ThreadTriageShell {
  readonly triageLabel?: ThreadTriageLabel | null;
  readonly settledOverride?: "settled" | "active" | null;
  readonly latestRun?: unknown;
  readonly latestTurn?: unknown;
}

/** What a thread row shows about who acts next. `null` shows nothing. */
export type ThreadTriageDisplay =
  | { readonly kind: "my-move"; readonly note: string | null }
  | { readonly kind: "unsorted" };

/**
 * Live states always win, so only a stopped thread shows a label. Snoozed and settled threads
 * are already sorted; callers pass `snoozed` as their list already resolved it. A thread that
 * never ran has nothing to sort.
 */
export function resolveThreadTriageDisplay(
  shell: ThreadTriageShell,
  options: { readonly stopped: boolean; readonly snoozed: boolean },
): ThreadTriageDisplay | null {
  if (!options.stopped || options.snoozed) return null;
  if (shell.settledOverride === "settled") return null;
  if (shell.triageLabel != null) return { kind: "my-move", note: shell.triageLabel.note };
  if ((shell.latestRun ?? shell.latestTurn ?? null) === null) return null;
  return { kind: "unsorted" };
}
