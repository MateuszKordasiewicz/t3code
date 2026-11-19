import type {
  OrchestrationV2AppThread,
  OrchestrationV2Command,
  ThreadTriageLabel,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";

import { isNativeMaintenanceCommand } from "./CommandPolicy.ts";

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

/** A label describes the stopped state it was set on, so the next user message retires it. */
export function threadWithTriageLabelRetired(
  thread: OrchestrationV2AppThread,
): OrchestrationV2AppThread | null {
  return thread.triageLabel == null ? null : { ...thread, triageLabel: null };
}

/** Only a message the user sends acts on "my move"; wakes, agents and schedules leave it. */
export function sentByUser(
  command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
): boolean {
  return (
    command.createdBy === "user" &&
    command.notification === undefined &&
    command.delegatedCompletion === undefined &&
    command.scheduledTaskId === undefined &&
    !isNativeMaintenanceCommand(command)
  );
}
