import { AsyncResult } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";

import {
  createIssueTabIconQueue,
  ISSUE_DETAIL_STALE_MS,
  issueDetailReadStep,
} from "./issueTabIconQueue.logic";

const NOW = 1_000_000;

function queueWithReads() {
  const reads: Array<{ readonly job: string; readonly settle: (answered: boolean) => void }> = [];
  const queue = createIssueTabIconQueue<string>((job, settle) => reads.push({ job, settle }));
  return { queue, reads };
}

describe("createIssueTabIconQueue", () => {
  it("runs one read per environment, in line order, with a hovered tab first", () => {
    const { queue, reads } = queueWithReads();
    const [a, b, c, hovered, other] = [{}, {}, {}, {}, {}];
    queue.request(a, "env-1", "a", false);
    queue.request(b, "env-1", "b", false);
    queue.request(c, "env-1", "c", false);
    queue.request(other, "env-2", "other", false);
    queue.request(hovered, "env-1", "hovered", true);
    expect(reads.map((read) => read.job)).toEqual(["a", "other"]);

    reads[0]!.settle(true);
    expect(queue.phase(a)).toBe("settled");
    expect(reads.map((read) => read.job)).toEqual(["a", "other", "hovered"]);

    // Hovering the waiting `c` moves it ahead of `b`.
    queue.request(c, "env-1", "c", true);
    reads[2]!.settle(true);
    expect(reads.map((read) => read.job)).toEqual(["a", "other", "hovered", "c"]);
    expect(queue.phase(b)).toBe("queued");
  });

  it("holds the slot until a closed tab's read settles, and lets the open tab skip the line", () => {
    const { queue, reads } = queueWithReads();
    const [closed, next, opened] = [{}, {}, {}];
    queue.request(closed, "env-1", "closed", false);
    queue.request(next, "env-1", "next", false);
    queue.request(opened, "env-1", "opened", false);
    queue.skip(opened);
    expect(queue.phase(opened)).toBe("settled");

    queue.drop(closed);
    expect(reads.map((read) => read.job)).toEqual(["closed"]);
    reads[0]!.settle(true);
    expect(reads.map((read) => read.job)).toEqual(["closed", "next"]);
    expect(queue.phase(closed)).toBe("idle");

    // Switched away before its detail arrived: it waits in line again.
    queue.drop(opened);
    queue.request(opened, "env-1", "opened", false);
    expect(queue.phase(opened)).toBe("queued");
  });

  it("retries a read that got no answer only when the tab is hovered or focused", () => {
    const { queue, reads } = queueWithReads();
    const tab = {};
    queue.request(tab, "env-1", "tab", false);
    reads[0]!.settle(false);
    expect(queue.phase(tab)).toBe("failed");
    queue.request(tab, "env-1", "tab", false);
    expect(reads).toHaveLength(1);
    queue.request(tab, "env-1", "tab", true);
    expect(reads).toHaveLength(2);
  });
});

describe("issueDetailReadStep", () => {
  it("fetches a stale or missing detail once and ends the turn on the next settled result", () => {
    const fresh = AsyncResult.success({}, { timestamp: NOW });
    const stale = AsyncResult.success({}, { timestamp: NOW - ISSUE_DETAIL_STALE_MS });
    const offline = AsyncResult.fail("Environment is offline.");
    expect(issueDetailReadStep(fresh, false, NOW)).toBe("done");
    expect(issueDetailReadStep(stale, false, NOW)).toBe("fetch");
    expect(issueDetailReadStep(AsyncResult.initial(true), false, NOW)).toBe("wait");
    expect(issueDetailReadStep(offline, false, NOW)).toBe("fetch");
    // An offline environment fails without ever waiting; the turn still ends.
    expect(issueDetailReadStep(offline, true, NOW)).toBe("done");
  });
});
