import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { SidebarThreadSummary } from "../../types";
import { IDLE_ISSUE_THREAD_PILL, issueThreadStatusBox } from "./issueThreadStatusBox.logic";

const runtime = {
  status: "running" as const,
  activeRunId: null,
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerName: "Codex",
  lastError: null,
  updatedAt: "2026-03-09T10:05:00.000Z",
};
const thread = (id: string, fields: Partial<SidebarThreadSummary> = {}) =>
  ({
    id,
    hasActionableProposedPlan: false,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    interactionMode: "default",
    latestRun: null,
    runtime: null,
    ...fields,
  }) as unknown as SidebarThreadSummary;

describe("issueThreadStatusBox", () => {
  it("ranks the sidebar's pills in its own order, with an unpilled open thread last", () => {
    const idle = thread("idle");
    const working = thread("working", { runtime });
    const approval = thread("approval", { runtime, hasPendingApprovals: true });
    const box = (threads: ReadonlyArray<SidebarThreadSummary>) =>
      issueThreadStatusBox(threads, () => undefined);

    expect(box([idle, working, approval])).toMatchObject({
      thread: approval,
      status: { label: "Pending Approval" },
      others: 2,
    });
    expect(box([idle, working])).toMatchObject({ thread: working, status: { label: "Working" } });
    expect(box([idle])).toEqual({ thread: idle, status: IDLE_ISSUE_THREAD_PILL, others: 0 });
  });
});
