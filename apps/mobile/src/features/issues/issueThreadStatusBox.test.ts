import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { describe, expect, it } from "vite-plus/test";

import { issueThreadStatusBox } from "./issueThreadStatusBox";

const thread = (id: string, fields: Partial<EnvironmentThreadShell> = {}) =>
  ({
    id,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    latestRun: null,
    lastVisitedAt: null,
    runtime: null,
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
  });
});
