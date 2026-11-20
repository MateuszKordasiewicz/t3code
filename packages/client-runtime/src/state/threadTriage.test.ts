import { describe, expect, it } from "vite-plus/test";

import { resolveThreadTriageDisplay, type ThreadTriageShell } from "./threadTriage.ts";

const stopped: ThreadTriageShell = { latestRun: { id: "run-1" } };
const label = { kind: "my-move", note: "try PR #49" } as const;

describe("resolveThreadTriageDisplay", () => {
  it.each([
    ["a stopped thread with no label is unsorted", stopped, true, false, { kind: "unsorted" }],
    ["my move shows its note", { ...stopped, triageLabel: label }, true, false, label],
    ["a live state wins over the label", { ...stopped, triageLabel: label }, false, false, null],
    ["a settled thread is sorted", { ...stopped, settledOverride: "settled" }, true, false, null],
    ["a snoozed thread is sorted", stopped, true, true, null],
    ["a thread that never ran has nothing to sort", { latestRun: null }, true, false, null],
  ] as const)("%s", (_name, shell, isStopped, snoozed, expected) => {
    expect(resolveThreadTriageDisplay(shell, { stopped: isStopped, snoozed })).toEqual(expected);
  });
});
