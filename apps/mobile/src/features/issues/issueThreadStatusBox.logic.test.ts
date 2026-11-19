import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { describe, expect, it } from "vite-plus/test";

import { issueThreadStatusBox } from "./issueThreadStatusBox.logic";

const thread = (id: string, fields: Partial<EnvironmentThreadShell> = {}) =>
  ({
    id,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    latestRun: null,
    lastVisitedAt: null,
    runtime: null,
    pendingBackgroundTasks: [],
    ...fields,
  }) as unknown as EnvironmentThreadShell;
const runtime = (status: string) => ({ status, lastErrorClass: null }) as never;

describe("issueThreadStatusBox", () => {
  it("picks by web's priority, with an idle runtime behind a failure", () => {
    const idle = thread("idle", { runtime: runtime("idle") });
    const failed = thread("failed", { runtime: runtime("failed") });
    const approval = thread("approval", { runtime: runtime("running"), hasPendingApprovals: true });

    expect(issueThreadStatusBox([idle, failed])).toEqual({
      thread: failed,
      status: "failed",
      others: 1,
    });
    expect(issueThreadStatusBox([idle, failed, approval])).toMatchObject({
      thread: approval,
      status: "approval",
      others: 2,
    });
    expect(issueThreadStatusBox([idle, thread("ready")])?.status).toBe("idle");
    // Background work holds an idle runtime short of done, as web's Waiting pill does.
    const background = thread("background", {
      runtime: runtime("idle"),
      pendingBackgroundTasks: [{ kind: "monitor", awaited: true }] as never,
    });
    expect(issueThreadStatusBox([failed, background])?.thread).toBe(background);
    const failedHeld = thread("failed-held", {
      runtime: runtime("failed"),
      pendingBackgroundTasks: [{ kind: "monitor", awaited: true }] as never,
    });
    expect(issueThreadStatusBox([failed, failedHeld])?.thread).toBe(failedHeld);
  });
});
