import type { OrchestrationV2AppThread, ThreadTriageLabel } from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";

/**
 * Setting "my move" says the thread needs the user now, so like a pin it brings a settled or
 * snoozed thread back to the inbox. Clearing a label is not activity.
 */
export function applyThreadTriageLabel(
  thread: OrchestrationV2AppThread,
  label: ThreadTriageLabel | null,
  now: DateTime.Utc,
): OrchestrationV2AppThread {
  if (label === null) return { ...thread, triageLabel: null };
  const settled = thread.settledOverride === "settled";
  const snoozed = thread.snoozedUntil != null;
  return {
    ...thread,
    triageLabel: label,
    ...(settled ? { settledOverride: "active", settledAt: null, unsettledAt: now } : {}),
    ...(snoozed ? { snoozedUntil: null, snoozedAt: null } : {}),
    updatedAt: settled || snoozed ? now : thread.updatedAt,
  };
}

/** A label describes the stopped state it was set on, so a new turn retires it. */
export function threadWithTriageLabelRetired(
  thread: OrchestrationV2AppThread,
): OrchestrationV2AppThread | null {
  return thread.triageLabel == null ? null : { ...thread, triageLabel: null };
}
