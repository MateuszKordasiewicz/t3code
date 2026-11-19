import type { ContextMenuItem, ThreadTriageLabel } from "@t3tools/contracts";
import { msg } from "gt-react";

import type { MessageResolver } from "../i18n/messages";

export type ThreadTriageMenuId = "triage:my-move" | "triage:clear";

export function buildThreadTriageMenuItems(
  state: { readonly triageLabel?: ThreadTriageLabel | null | undefined },
  resolve: MessageResolver,
): ReadonlyArray<ContextMenuItem<ThreadTriageMenuId>> {
  const label = state.triageLabel ?? null;
  return [
    {
      id: "triage:my-move",
      label: label === null ? resolve(msg("My move…")) : resolve(msg("Edit my move…")),
      icon: "hand",
    },
    ...(label === null
      ? []
      : [{ id: "triage:clear" as const, label: resolve(msg("Clear label")), icon: "x" }]),
  ];
}
