import { EnvironmentId, IssueTrackerKind, ProjectId, type IssueState } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";

import { appAtomRegistry } from "../rpc/atomRegistry";
import {
  issueStateQueue,
  knownIssueDetailStates,
  notifyIssueDetailState,
  rememberIssueDetailState,
  subscribeIssueDetailStates,
} from "./issueStateQueue";
import { issueEnvironment, selectLinkedIssueState } from "./issues";

function writableDetailAtom(onRefresh: () => void) {
  let state: AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown> =
    AsyncResult.initial(false);
  return Atom.writable(
    (get) => {
      void get;
      return state;
    },
    (ctx, next: AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>) => {
      state = next;
      ctx.setSelf(next);
    },
    () => {
      onRefresh();
    },
  );
}

describe("sidebar linked issue queue", () => {
  it("serializes detail reads one at a time per environment and remembers settled states", async () => {
    const envId = EnvironmentId.make("bazzite");
    const started: string[] = [];
    const atomA = writableDetailAtom(() => started.push("A"));
    const atomB = writableDetailAtom(() => started.push("B"));
    const ticketA = {};
    const ticketB = {};

    issueStateQueue.request(ticketA, envId, atomA, false);
    issueStateQueue.request(ticketB, envId, atomB, false);
    expect(started).toEqual(["A"]);
    expect(selectLinkedIssueState([atomA, atomB])).toBeNull();

    appAtomRegistry.set(atomA, AsyncResult.success({ state: { category: "open", name: "Open" } }));
    await Promise.resolve();
    expect(started).toEqual(["A", "B"]);
    expect(selectLinkedIssueState([atomA, atomB])).toEqual({ category: "open", name: "Open" });

    appAtomRegistry.set(
      atomB,
      AsyncResult.success({ state: { category: "closed", name: "Closed" } }),
    );
    await Promise.resolve();

    issueStateQueue.drop(ticketA);
    issueStateQueue.drop(ticketB);
    expect(knownIssueDetailStates.get(atomB)).toEqual({ category: "closed", name: "Closed" });
  });

  it("prioritizes open, then stopped/non-completed closed, then completed closed", () => {
    const completed = Atom.make(
      AsyncResult.initial<{ readonly state: IssueState }, unknown>(false),
    );
    const canceled = Atom.make(AsyncResult.initial<{ readonly state: IssueState }, unknown>(false));
    const open = Atom.make(AsyncResult.initial<{ readonly state: IssueState }, unknown>(false));

    rememberIssueDetailState(
      completed,
      AsyncResult.success({
        state: { category: "closed", closeReason: "completed", name: "Done" },
      }),
    );
    expect(selectLinkedIssueState([completed, canceled])).toBeNull();

    rememberIssueDetailState(
      canceled,
      AsyncResult.success({
        state: { category: "closed", closeReason: "not_planned", name: "Canceled" },
      }),
    );
    expect(selectLinkedIssueState([completed, canceled])).toEqual({
      category: "closed",
      closeReason: "not_planned",
      name: "Canceled",
    });

    rememberIssueDetailState(
      open,
      AsyncResult.success({ state: { category: "open", name: "Open" } }),
    );
    expect(selectLinkedIssueState([completed, open, canceled])).toEqual({
      category: "open",
      name: "Open",
    });
  });

  it("notifies only subscribers of the updated detail atom when state changes", async () => {
    const atom = Atom.make(AsyncResult.initial<{ readonly state: IssueState }, unknown>(false));
    const other = Atom.make(AsyncResult.initial<{ readonly state: IssueState }, unknown>(false));
    const queued = writableDetailAtom(() => {});
    let notifications = 0;
    let otherNotifications = 0;
    const unsubscribe = subscribeIssueDetailStates([atom], () => {
      notifications += 1;
    });
    const unsubscribeOther = subscribeIssueDetailStates([other], () => {
      otherNotifications += 1;
    });
    try {
      const ticket = {};
      issueStateQueue.request(ticket, EnvironmentId.make("bazzite"), queued, false);
      appAtomRegistry.set(
        queued,
        AsyncResult.success({ state: { category: "open", name: "Open" } }),
      );
      await Promise.resolve();
      issueStateQueue.drop(ticket);
      expect(notifications).toBe(0);
      expect(otherNotifications).toBe(0);

      notifyIssueDetailState(atom, { category: "open", name: "Open" });
      notifyIssueDetailState(atom, { category: "open", name: "Open" });
      expect(notifications).toBe(1);
      expect(otherNotifications).toBe(0);

      notifyIssueDetailState(atom, { category: "closed", closeReason: "completed", name: "Done" });
      expect(notifications).toBe(2);
      expect(otherNotifications).toBe(0);
      expect(selectLinkedIssueState([atom])).toEqual({
        category: "closed",
        closeReason: "completed",
        name: "Done",
      });
    } finally {
      unsubscribe();
      unsubscribeOther();
    }
  });

  it("normalizes issueEnvironment.detail ref property order", () => {
    const environmentId = EnvironmentId.make("bazzite");
    const projectId = ProjectId.make("project-1");
    const tracker = IssueTrackerKind.make("linear");
    const first = issueEnvironment.detail({
      environmentId,
      input: { projectId, tracker, host: "linear.app", scope: "WEB", key: "WEB-1" },
    });
    const second = issueEnvironment.detail({
      environmentId,
      input: { projectId, tracker, scope: "WEB", key: "WEB-1", host: "linear.app" },
    });
    expect(second).toBe(first);
  });
});
