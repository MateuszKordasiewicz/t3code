import { ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { issueThreads } from "./issueThreads.ts";

const projectId = ProjectId.make("project-1");

function row(id: string, state: { archived?: boolean; settled?: boolean } = {}) {
  return {
    id: ThreadId.make(id),
    projectId,
    title: `Row ${id}`,
    archivedAt: state.archived === true ? "2026-11-10T00:00:00.000Z" : null,
    settled: state.settled ?? false,
  };
}

function shell(working: boolean, title = "Live title") {
  return {
    title,
    latestRun: null,
    hasActionableProposedPlan: false,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    interactionMode: "default" as const,
    runtime: working
      ? {
          status: "running" as const,
          activeRunId: null,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerName: "Codex",
          lastError: null,
          updatedAt: "2026-11-10T00:00:00.000Z",
        }
      : null,
  };
}

describe("issueThreads", () => {
  it.each([
    { name: "no linked threads", rows: [], live: {}, statuses: [], primary: null },
    {
      name: "one running thread",
      rows: [row("a")],
      live: { a: shell(true) },
      statuses: ["running"],
      primary: "a",
    },
    {
      name: "settled and archived threads, most recent first",
      rows: [row("archived", { archived: true }), row("settled", { settled: true })],
      live: { settled: shell(false) },
      statuses: ["done", "done"],
      primary: "archived",
    },
    {
      name: "an older running thread wins over newer idle and done ones",
      rows: [row("idle"), row("done", { settled: true }), row("running")],
      live: { idle: shell(false), running: shell(true) },
      statuses: ["idle", "done", "running"],
      primary: "running",
    },
    {
      name: "threads with no loaded shell",
      rows: [row("unloaded"), row("unloaded-settled", { settled: true })],
      live: {},
      statuses: ["idle", "done"],
      primary: "unloaded",
    },
    {
      name: "an archived thread ignores a running shell",
      rows: [row("archived", { archived: true })],
      live: { archived: shell(true) },
      statuses: ["done"],
      primary: "archived",
    },
  ])("derives status and primary for $name", ({ rows, live, statuses, primary }) => {
    const shells: Record<string, ReturnType<typeof shell>> = live;
    const result = issueThreads(rows, (threadId) => shells[threadId]);
    expect(result.threads.map((thread) => thread.status)).toEqual(statuses);
    expect(result.threads.map((thread) => thread.id)).toEqual(rows.map((r) => r.id));
    expect(result.primary?.id ?? null).toBe(primary);
  });

  it("shows a loaded shell's live title over the server's, except for archived threads", () => {
    const shells: Record<string, ReturnType<typeof shell>> = {
      live: shell(false, "Renamed"),
      archived: shell(false, "Renamed"),
    };
    const result = issueThreads(
      [row("live"), row("unloaded"), row("archived", { archived: true })],
      (threadId) => shells[threadId],
    );
    expect(result.threads.map((thread) => thread.title)).toEqual([
      "Renamed",
      "Row unloaded",
      "Row archived",
    ]);
  });
});
