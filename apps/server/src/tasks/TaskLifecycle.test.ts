import { WorkTaskPersistedEvent } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import { describe, expect, it } from "vite-plus/test";
import {
  ValidationReplayConflict,
  reduceTaskValidation,
  CustodyReplayConflict,
  reduceTaskCustody,
  FoundationReplayConflict,
  reduceTaskFoundation,
} from "./TaskLifecycle.ts";

const decode = Schema.decodeUnknownSync(WorkTaskPersistedEvent);
const definition = {
  revision: 1,
  title: "Task",
  objective: "Deliver",
  criteria: [{ id: "criterion", condition: "Done", verification: "Inspect" }],
  actionPolicy: { version: 1, rules: [] },
};
const identity = { managerThreadId: "manager", managerGeneration: 1 };
const evidence = [{ type: "artifact", uri: "artifact:one", hash: "hash" }];
const claim = { ...identity, claimId: "claim", evidenceId: "evidence", observedWorkSequence: 9 };
const base = {
  taskId: "task",
  occurredAt: "2026-11-12T00:00:00.000Z",
  attribution: { type: "server" },
  definitionRevision: 1,
  lifecycleRevision: 1,
  completionPolicyRevision: 1,
  actionPolicyRevision: 1,
};

function recorded(sequence: number, event: object) {
  const id = `event-${sequence}`;
  return decode({
    event: { ...base, id, ...event },
    context: {
      sequence,
      eventId: id,
      occurredAt: base.occurredAt,
      commandId: `command-${sequence}`,
    },
  }) as Parameters<typeof reduceTaskFoundation>[1];
}

function replay(events: ReadonlyArray<ReturnType<typeof recorded>>) {
  return events.reduce<ReturnType<typeof reduceTaskFoundation> | undefined>(
    reduceTaskFoundation,
    undefined,
  )!;
}

describe("foundation task replay", () => {
  it("replays recorded definitions, configuration, execution and attributable history without owning contributors", () => {
    const events = [
      recorded(1, { type: "task.created", projectId: "project", definition }),
      recorded(2, {
        type: "task.definition-accepted",
        attribution: { type: "user", subject: "human", sessionId: "session" },
        definition,
      }),
      recorded(3, {
        type: "task.configuration-updated",
        lifecycleRevision: 2,
        completionPolicyRevision: 2,
        allowanceOverride: null,
        completionPolicy: { type: "configured", policy: { type: "manual" } },
      }),
      recorded(4, {
        type: "task.execution-updated",
        lifecycleRevision: 3,
        completionPolicyRevision: 2,
        desiredMode: "active",
        previousManagerGeneration: null,
        manager: identity,
        reason: "start",
      }),
      recorded(5, {
        type: "task.thread-linked",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        threadId: "contributor",
      }),
      recorded(6, {
        type: "task.wait-updated",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        ...identity,
        waitId: "wait",
        target: { type: "source", sourceId: "source" },
        state: "registered",
      }),
      recorded(7, {
        type: "task.result-recorded",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        sourceId: "source",
        contributorThreadId: "contributor",
        summary: "result",
        results: evidence,
      }),
      recorded(8, {
        type: "task.continuation-accepted",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        ...identity,
        sourceIds: ["source"],
        runId: "run",
        messageId: "message",
        effectId: "effect",
        dirtyGeneration: 2,
      }),
      recorded(9, {
        type: "task.wait-updated",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        ...identity,
        waitId: "wait",
        target: { type: "source", sourceId: "source" },
        state: "cleared",
      }),
      recorded(10, {
        type: "task.completion-submitted",
        lifecycleRevision: 4,
        completionPolicyRevision: 2,
        ...claim,
        summary: "claim",
        evidence,
      }),
      recorded(11, {
        type: "task.completed",
        lifecycleRevision: 5,
        completionPolicyRevision: 2,
        ...claim,
      }),
      recorded(12, { type: "task.reopened", lifecycleRevision: 6, completionPolicyRevision: 2 }),
      recorded(13, {
        type: "task.archived",
        lifecycleRevision: 7,
        completionPolicyRevision: 2,
        archived: true,
      }),
      recorded(14, {
        type: "task.archived",
        lifecycleRevision: 8,
        completionPolicyRevision: 2,
        archived: false,
      }),
      recorded(15, {
        type: "task.thread-unlinked",
        lifecycleRevision: 9,
        completionPolicyRevision: 2,
        threadId: "contributor",
      }),
    ];
    const state = replay(events);
    expect(replay(events)).toEqual(state);
    expect(reduceTaskFoundation(state, events[3]!)).toBe(state);
    expect(state.created.event.projectId).toBe("project");
    expect(Array.from(state.acceptedDefinitions)[0]).toEqual(events[1]);
    expect(state.configuration).toEqual(events[2]);
    expect(state.execution).toEqual(events[3]);
    expect(Array.from(HashMap.values(state.links))[0]).toEqual({
      linked: events[4],
      unlinked: events[14],
    });
    expect(Array.from(HashMap.values(state.waits))).toEqual([events[8]]);
    expect(Array.from(state.results)).toEqual([events[6]]);
    expect(Array.from(state.continuations)).toEqual([events[7]]);
    expect(Array.from(state.claims)).toEqual([events[9]]);
    expect(state.completion).toEqual(events[11]);
    expect(state.archive).toEqual(events[13]);
    expect(state.revisions).toEqual({
      definitionRevision: 1,
      lifecycleRevision: 9,
      completionPolicyRevision: 2,
      actionPolicyRevision: 1,
    });
  });

  it("keeps draft edits distinct from explicit acceptance and independent revisions", () => {
    const draft = { ...definition, revision: 2, objective: "Revised" };
    const events = [
      recorded(1, { type: "task.created", definition }),
      recorded(2, {
        type: "task.definition-accepted",
        attribution: { type: "user", subject: "human", sessionId: "session" },
        definition,
      }),
      recorded(3, { type: "task.definition-updated", definitionRevision: 2, definition: draft }),
      recorded(4, {
        type: "task.result-recorded",
        definitionRevision: 2,
        sourceId: "source",
        summary: "result",
        results: evidence,
      }),
      recorded(5, {
        type: "task.definition-accepted",
        definitionRevision: 2,
        attribution: { type: "user", subject: "human", sessionId: "session" },
        definition: draft,
      }),
    ];
    const before = replay(events.slice(0, 4));
    expect(Array.from(before.acceptedDefinitions)).toEqual([events[1]]);
    expect(before.definition).toEqual(events[2]);
    expect(before.revisions.lifecycleRevision).toBe(1);
    const after = reduceTaskFoundation(before, events[4]!);
    expect(Array.from(after.acceptedDefinitions)).toEqual([events[1], events[4]]);
    expect(after.definition).toEqual(events[4]);
    expect(Array.from(after.results)).toEqual(Array.from(before.results));
    expect(Array.from(after.acceptedDefinitions)[0]!.event.definition).toEqual(definition);
    expect(after.configuration).toBeUndefined();
    expect(after.execution).toBeUndefined();
  });

  it("preserves independent task histories and rejects other event families", () => {
    const first = replay([
      recorded(1, { type: "task.created", definition }),
      recorded(4, { type: "task.thread-linked", threadId: "shared" }),
    ]);
    const second = replay([
      recorded(2, { type: "task.created", taskId: "second", definition }),
      recorded(5, { type: "task.thread-linked", taskId: "second", threadId: "shared" }),
    ]);
    const unlinked = reduceTaskFoundation(
      first,
      recorded(6, { type: "task.thread-unlinked", threadId: "shared" }),
    );
    expect(Array.from(HashMap.values(unlinked.links))[0]!.unlinked).toBeDefined();
    expect(Array.from(HashMap.values(second.links))[0]!.unlinked).toBeUndefined();
    const otherFamily = recorded(7, { type: "task.manager-bound", ...identity });
    expect(() => reduceTaskFoundation(first, otherFamily)).toThrow(
      expect.objectContaining({ code: "unsupported" }),
    );
  });

  it("rejects conflicting replay, identities, revisions, claims and manager completion", () => {
    const created = recorded(1, { type: "task.created", definition });
    const state = replay([created]);
    expect(() =>
      reduceTaskFoundation(
        state,
        recorded(1, { type: "task.created", definition: { ...definition, title: "Changed" } }),
      ),
    ).toThrow(FoundationReplayConflict);
    expect(() =>
      reduceTaskFoundation(
        state,
        recorded(1, { type: "task.created", definition: { ...definition, title: "Changed" } }),
      ),
    ).toThrow(
      expect.objectContaining({ _tag: "FoundationReplayConflict", code: "event_identity" }),
    );
    expect(() =>
      reduceTaskFoundation(
        state,
        recorded(2, { type: "task.created", taskId: "another", definition }),
      ),
    ).toThrow(expect.objectContaining({ code: "task_identity" }));
    expect(() =>
      reduceTaskFoundation(
        state,
        recorded(2, {
          type: "task.definition-updated",
          definition: { ...definition, title: "Changed" },
        }),
      ),
    ).toThrow(expect.objectContaining({ code: "definition" }));
    const advanced = reduceTaskFoundation(
      state,
      recorded(3, { type: "task.archived", lifecycleRevision: 2, archived: true }),
    );
    expect(() =>
      reduceTaskFoundation(advanced, recorded(4, { type: "task.archived", archived: false })),
    ).toThrow(expect.objectContaining({ code: "revision" }));
    expect(() =>
      reduceTaskFoundation(
        advanced,
        recorded(2, { type: "task.archived", lifecycleRevision: 2, archived: false }),
      ),
    ).toThrow(expect.objectContaining({ code: "sequence" }));
    expect(() =>
      reduceTaskFoundation(
        state,
        recorded(2, { type: "task.thread-unlinked", threadId: "contributor" }),
      ),
    ).toThrow(expect.objectContaining({ code: "link" }));
    const manual = recorded(2, {
      type: "task.completed",
      ...claim,
      attribution: { type: "user", subject: "human", sessionId: "session" },
    });
    expect(reduceTaskFoundation(state, manual).completion).toEqual(manual);
    const claimed = reduceTaskFoundation(
      replay([
        created,
        recorded(2, {
          type: "task.definition-accepted",
          attribution: { type: "user", subject: "human", sessionId: "session" },
          definition,
        }),
      ]),
      recorded(3, { type: "task.completion-submitted", ...claim, summary: "claim", evidence }),
    );
    expect(() =>
      reduceTaskFoundation(
        claimed,
        recorded(4, { type: "task.completion-submitted", ...claim, summary: "other", evidence }),
      ),
    ).toThrow(expect.objectContaining({ code: "claim" }));
    expect(() =>
      reduceTaskFoundation(
        claimed,
        recorded(4, {
          type: "task.completed",
          ...claim,
          attribution: {
            type: "manager",
            threadId: "manager",
            providerSessionId: "session",
            runId: "run",
            generation: 1,
          },
        }),
      ),
    ).toThrow(expect.objectContaining({ code: "completion" }));
    expect(() =>
      reduceTaskFoundation(
        claimed,
        recorded(4, { type: "task.completed", ...claim, evidenceId: "other" }),
      ),
    ).toThrow(expect.objectContaining({ code: "completion" }));
    expect(() =>
      reduceTaskFoundation(
        claimed,
        recorded(4, { type: "task.completed", ...claim, claimId: "other" }),
      ),
    ).toThrow(expect.objectContaining({ code: "completion" }));
  });
  it("rejects contradictory foundation bindings without implementing custody eligibility", () => {
    const created = recorded(1, { type: "task.created", definition });
    const accepted = recorded(2, {
      type: "task.definition-accepted",
      attribution: { type: "user", subject: "human", sessionId: "session" },
      definition,
    });
    const state = replay([created, accepted]);
    const claimed = reduceTaskFoundation(
      state,
      recorded(3, { type: "task.completion-submitted", ...claim, summary: "claim", evidence }),
    );
    expect(() =>
      reduceTaskFoundation(
        claimed,
        recorded(4, {
          type: "task.completed",
          ...claim,
          attribution: {
            type: "agent",
            threadId: "manager",
            providerSessionId: "ps",
            runId: "run",
          },
        }),
      ),
    ).toThrow(expect.objectContaining({ code: "completion" }));
    expect(() =>
      reduceTaskFoundation(
        replay([created]),
        recorded(2, { type: "task.completion-submitted", ...claim, summary: "claim", evidence }),
      ),
    ).toThrow(expect.objectContaining({ code: "claim" }));
    const draft = { ...definition, revision: 2, objective: "Draft" };
    const edited = reduceTaskFoundation(
      state,
      recorded(3, { type: "task.definition-updated", definitionRevision: 2, definition: draft }),
    );
    expect(() =>
      reduceTaskFoundation(
        edited,
        recorded(4, {
          type: "task.completion-submitted",
          definitionRevision: 2,
          ...claim,
          summary: "claim",
          evidence,
        }),
      ),
    ).toThrow(expect.objectContaining({ code: "claim" }));
    const adoption = recorded(4, {
      type: "task.definition-accepted",
      definitionRevision: 2,
      definition: draft,
      attribution: { type: "user", subject: "human", sessionId: "session" },
    });
    expect(() =>
      reduceTaskFoundation(edited, {
        ...adoption,
        context: { ...adoption.context, commandId: accepted.context.commandId },
      }),
    ).toThrow(expect.objectContaining({ code: "command" }));
    const configured = reduceTaskFoundation(
      state,
      recorded(3, {
        type: "task.configuration-updated",
        allowanceOverride: null,
        completionPolicy: { type: "configured", policy: { type: "manual" } },
      }),
    );
    expect(() =>
      reduceTaskFoundation(
        configured,
        recorded(4, {
          type: "task.configuration-updated",
          allowanceOverride: null,
          completionPolicy: { type: "inherit" },
        }),
      ),
    ).toThrow(expect.objectContaining({ code: "policy" }));
    expect(
      reduceTaskFoundation(
        configured,
        recorded(4, {
          type: "task.configuration-updated",
          lifecycleRevision: 2,
          allowanceOverride: 5,
          completionPolicy: { type: "configured", policy: { type: "manual" } },
        }),
      ).configuration?.event.allowanceOverride,
    ).toBe(5);
    const execution = {
      type: "task.execution-updated",
      desiredMode: "active",
      previousManagerGeneration: null,
      manager: identity,
      reason: "start",
    };
    const started = reduceTaskFoundation(state, recorded(3, execution));
    expect(() =>
      reduceTaskFoundation(
        started,
        recorded(4, { ...execution, manager: { ...identity, managerThreadId: "other" } }),
      ),
    ).toThrow(expect.objectContaining({ code: "manager" }));
    const replaced = reduceTaskFoundation(
      started,
      recorded(4, {
        ...execution,
        previousManagerGeneration: 1,
        manager: { managerThreadId: "other", managerGeneration: 2 },
      }),
    );
    expect(() =>
      reduceTaskFoundation(replaced, recorded(5, { ...execution, previousManagerGeneration: 2 })),
    ).toThrow(expect.objectContaining({ code: "manager" }));
    const wait = {
      type: "task.wait-updated",
      ...identity,
      waitId: "wait",
      target: { type: "source", sourceId: "a" },
      state: "registered",
    };
    const waiting = reduceTaskFoundation(state, recorded(3, wait));
    expect(() =>
      reduceTaskFoundation(
        waiting,
        recorded(4, { ...wait, state: "cleared", target: { type: "source", sourceId: "b" } }),
      ),
    ).toThrow(expect.objectContaining({ code: "wait" }));
    const launch = {
      type: "task.continuation-accepted",
      ...identity,
      runId: "run",
      messageId: "message",
      effectId: "effect",
      sourceIds: ["source"],
      dirtyGeneration: 1,
    };
    const continuing = reduceTaskFoundation(state, recorded(3, launch));
    for (const change of [
      { messageId: "other", effectId: "other" },
      { runId: "other", effectId: "other" },
      { runId: "other", messageId: "other" },
    ]) {
      expect(() => reduceTaskFoundation(continuing, recorded(4, { ...launch, ...change }))).toThrow(
        expect.objectContaining({ code: "continuation" }),
      );
    }
    expect(() =>
      reduceTaskFoundation(
        continuing,
        recorded(4, { ...launch, sourceIds: ["another"], dirtyGeneration: 2 }),
      ),
    ).not.toThrow();
    const companion = recorded(3, {
      type: "task.configuration-updated",
      allowanceOverride: null,
      completionPolicy: { type: "inherit" },
    });
    expect(() =>
      reduceTaskFoundation(state, {
        ...companion,
        context: { ...companion.context, commandId: accepted.context.commandId },
      }),
    ).not.toThrow();
  });

  it("appends history without rereading every previous event identity", () => {
    let reads = 0;
    let state = replay([recorded(1, { type: "task.created", definition })]);
    const initial = Object.freeze(state);
    for (let i = 2; i <= 1001; i++) {
      const item = recorded(i, {
        type: "task.result-recorded",
        sourceId: `source-${i}`,
        summary: "result",
        results: evidence,
      });
      const event = new Proxy(item.event, {
        get(target, key, receiver) {
          if (key === "id") reads++;
          return Reflect.get(target, key, receiver);
        },
      });
      state = reduceTaskFoundation(state, { ...item, event });
    }
    const before = reads;
    state = reduceTaskFoundation(
      state,
      recorded(1002, {
        type: "task.result-recorded",
        sourceId: "last",
        summary: "result",
        results: evidence,
      }),
    );
    expect(reads - before).toBeLessThan(30);
    expect(Array.from(state.results)).toHaveLength(1001);
    expect(Array.from(initial.results)).toEqual([]);
  });
});

describe("custody task replay", () => {
  it("preserves custody, debits, uncertain launches and concurrent wake evidence independently of foundation", () => {
    const source = { kind: "run", sourceId: "work", resultRef: "result" };
    const pending = { sources: [source], overflowThroughGeneration: null };
    const allowance = { type: "configured", cap: 1, consumed: 0 };
    const wake = {
      reservationId: "reservation",
      reservationCommandId: "launch",
      managerGeneration: 1,
      allowanceUnit: 1,
      dirtyGeneration: 1,
      acknowledgedGeneration: 0,
    };
    const payloads = [
      { type: "task.manager-bound", ...identity },
      {
        type: "task.allowance-configured",
        previous: { type: "unconfigured", consumed: 0 },
        resulting: allowance,
      },
      {
        type: "task.allowance-debited",
        previous: allowance,
        resulting: { ...allowance, consumed: 1 },
        allowanceUnit: 1,
      },
      { type: "task.wake-source-recorded", source, reservationId: null, dirtyGeneration: 1 },
      { type: "task.wake-reserved", ...wake, pending },
      { type: "task.wake-claimed", ...wake, pending, messageId: "message", runId: "run" },
      { type: "task.wake-claim-uncertain", ...wake, reason: "unknown launch outcome" },
      {
        type: "task.wake-source-recorded",
        source: { ...source, sourceId: "later" },
        reservationId: "reservation",
        dirtyGeneration: 2,
      },
      {
        type: "task.wake-acknowledged",
        ...wake,
        dirtyGeneration: 2,
        acknowledgedGeneration: 1,
        previousAcknowledgedGeneration: 0,
        observedDirtyGeneration: 1,
      },
      { type: "task.manager-retired", ...identity },
      { type: "task.manager-bound", managerThreadId: "replacement", managerGeneration: 2 },
    ];
    const events = payloads.map((payload, index) => custodyRecorded(index + 1, payload));
    let state: ReturnType<typeof reduceTaskCustody> | undefined;
    for (const event of events) {
      state = reduceTaskCustody(state, event);
      expect(reduceTaskCustody(state, event)).toBe(state);
    }
    expect(state!.allowance).toEqual({ ...allowance, consumed: 1 });
    expect(state!.currentManagerGeneration).toBe(2);
    expect(state!.acknowledgedGeneration).toBe(1);
    expect(state!.dirtyGeneration).toBe(2);
    expect(HashMap.size(state!.pendingSources)).toBe(1);
    expect(HashMap.getUnsafe(state!.reservations, "reservation").uncertain).toBeDefined();
    const foundation = replay([recorded(1, { type: "task.created", definition })]);
    expect(HashMap.size(foundation.managers)).toBe(0);
    expect(foundation.lastSequence).toBe(1);
    const before = events
      .slice(0, 5)
      .reduce<ReturnType<typeof reduceTaskCustody> | undefined>(reduceTaskCustody, undefined)!;
    expect(() =>
      reduceTaskCustody(
        before,
        custodyRecorded(20, { ...payloads[5], runId: "other", allowanceUnit: 2 }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        state,
        custodyRecorded(21, {
          type: "task.manager-bound",
          managerThreadId: "manager",
          managerGeneration: 3,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        state,
        custodyRecorded(22, {
          type: "task.allowance-debited",
          previous: allowance,
          resulting: { ...allowance, consumed: 1 },
          allowanceUnit: 1,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        before,
        custodyRecorded(23, { ...payloads[8], previousAcknowledgedGeneration: 1 }),
      ),
    ).toThrow(CustodyReplayConflict);
    const alreadyClaimed = reduceTaskCustody(before, events[5]!);
    const laterSource = { ...source, sourceId: "coalesced" };
    const withLater = reduceTaskCustody(
      alreadyClaimed,
      custodyRecorded(7, {
        type: "task.wake-source-recorded",
        source: laterSource,
        reservationId: "reservation",
        dirtyGeneration: 2,
      }),
    );
    const coalescing = {
      ...payloads[5],
      dirtyGeneration: 2,
      pending: { sources: [source, laterSource], overflowThroughGeneration: null },
    };
    const coalesced = reduceTaskCustody(withLater, custodyRecorded(8, coalescing));
    expect(coalesced.allowance).toEqual(alreadyClaimed.allowance);
    expect(HashMap.size(coalesced.debits)).toBe(1);
    expect(HashMap.size(coalesced.launches)).toBe(2);
    expect(HashMap.getUnsafe(coalesced.reservations, "reservation").claimed!.dirtyGeneration).toBe(
      2,
    );
    for (const change of [{ runId: "different" }, { messageId: "different" }])
      expect(() =>
        reduceTaskCustody(withLater, custodyRecorded(8, { ...coalescing, ...change })),
      ).toThrow(CustodyReplayConflict);
    expect(() => reduceTaskCustody(coalesced, custodyRecorded(9, coalescing))).toThrow(
      CustodyReplayConflict,
    );
    const coalescedAck = reduceTaskCustody(
      coalesced,
      custodyRecorded(9, { ...payloads[8], observedDirtyGeneration: 2, acknowledgedGeneration: 2 }),
    );
    expect(coalescedAck.acknowledgedGeneration).toBe(2);
    expect(HashMap.size(coalescedAck.pendingSources)).toBe(0);
    expect(before.acknowledgedGeneration).toBe(0);
    expect(before.dirtyGeneration).toBe(1);
  });

  it("rejects contradictory phases and identities while preserving top-up consumption and retired custody", () => {
    const bound = custodyRecorded(1, { type: "task.manager-bound", ...identity });
    const initial = reduceTaskCustody(undefined, bound);
    expect(() =>
      reduceTaskCustody(
        initial,
        custodyRecorded(2, {
          type: "task.manager-bound",
          managerThreadId: "other",
          managerGeneration: 2,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        initial,
        custodyRecorded(2, { type: "task.manager-retired", ...identity, managerThreadId: "other" }),
      ),
    ).toThrow(CustodyReplayConflict);
    const allowance = { type: "configured", cap: 2, consumed: 0 };
    const configured = reduceTaskCustody(
      initial,
      custodyRecorded(2, {
        type: "task.allowance-configured",
        previous: { type: "unconfigured", consumed: 0 },
        resulting: allowance,
      }),
    );
    const debited = reduceTaskCustody(
      configured,
      custodyRecorded(3, {
        type: "task.allowance-debited",
        previous: allowance,
        resulting: { ...allowance, consumed: 1 },
        allowanceUnit: 1,
      }),
    );
    const topup = custodyRecorded(4, {
      type: "task.allowance-topped-up",
      attribution: { type: "user", subject: "human", sessionId: "session" },
      previous: { ...allowance, consumed: 1 },
      resulting: { ...allowance, cap: 3, consumed: 1 },
    });
    const topped = reduceTaskCustody(debited, topup);
    expect(topped.allowance).toEqual({ ...allowance, cap: 3, consumed: 1 });
    expect(reduceTaskCustody(topped, topup)).toBe(topped);
    expect(debited.allowance).toEqual({ ...allowance, consumed: 1 });
    const retired = reduceTaskCustody(
      topped,
      custodyRecorded(5, { type: "task.manager-retired", ...identity }),
    );
    expect(HashMap.getUnsafe(retired.managers, 1).retired).toBeDefined();
    expect(() =>
      reduceTaskCustody(
        retired,
        custodyRecorded(6, {
          type: "task.manager-bound",
          managerThreadId: "new",
          managerGeneration: 1,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        retired,
        custodyRecorded(6, { type: "task.manager-bound", ...identity, managerGeneration: 2 }),
      ),
    ).toThrow(CustodyReplayConflict);
    const source = { kind: "run", sourceId: "source", resultRef: null };
    const sourced = reduceTaskCustody(
      retired,
      custodyRecorded(6, {
        type: "task.wake-source-recorded",
        source,
        reservationId: null,
        dirtyGeneration: 1,
      }),
    );
    const wake = {
      reservationId: "wake",
      reservationCommandId: "launch",
      managerGeneration: 1,
      allowanceUnit: 1,
      dirtyGeneration: 1,
      acknowledgedGeneration: 0,
    };
    expect(() =>
      reduceTaskCustody(
        sourced,
        custodyRecorded(7, {
          type: "task.wake-reserved",
          ...wake,
          pending: { sources: [source], overflowThroughGeneration: null },
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    const active = reduceTaskCustody(
      sourced,
      custodyRecorded(7, {
        type: "task.manager-bound",
        managerThreadId: "replacement",
        managerGeneration: 2,
      }),
    );
    const reservedEvent = custodyRecorded(8, {
      type: "task.wake-reserved",
      ...wake,
      managerGeneration: 2,
      pending: { sources: [source], overflowThroughGeneration: null },
    });
    const reserved = reduceTaskCustody(active, reservedEvent);
    expect(() =>
      reduceTaskCustody(
        active,
        custodyRecorded(8, {
          type: "task.wake-reserved",
          ...wake,
          managerGeneration: 2,
          pending: { sources: [], overflowThroughGeneration: null },
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        reserved,
        custodyRecorded(9, {
          type: "task.wake-reserved",
          ...wake,
          reservationId: "other",
          managerGeneration: 2,
          pending: { sources: [source], overflowThroughGeneration: null },
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        reserved,
        custodyRecorded(9, {
          type: "task.wake-acknowledged",
          ...wake,
          managerGeneration: 2,
          observedDirtyGeneration: 1,
          previousAcknowledgedGeneration: 0,
          acknowledgedGeneration: 1,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    const uncertain = reduceTaskCustody(
      reserved,
      custodyRecorded(9, {
        type: "task.wake-claim-uncertain",
        ...wake,
        managerGeneration: 2,
        reason: "launch unknown",
      }),
    );
    expect(uncertain.allowance).toEqual(active.allowance);
    expect(HashMap.getUnsafe(uncertain.pendingReservations, 2)).toBe("wake");
    const confirmed = reduceTaskCustody(
      uncertain,
      custodyRecorded(10, {
        type: "task.wake-claimed",
        ...wake,
        managerGeneration: 2,
        messageId: "message",
        runId: "run",
        pending: { sources: [source], overflowThroughGeneration: null },
      }),
    );
    expect(confirmed.allowance).toEqual(uncertain.allowance);
    expect(HashMap.size(confirmed.debits)).toBe(1);
    expect(HashMap.getUnsafe(confirmed.reservations, "wake").uncertain).toBeDefined();
    const acknowledge = {
      type: "task.wake-acknowledged",
      ...wake,
      managerGeneration: 2,
      observedDirtyGeneration: 1,
      previousAcknowledgedGeneration: 0,
      acknowledgedGeneration: 1,
    };
    const acknowledgedEvent = custodyRecorded(11, acknowledge);
    const acknowledged = reduceTaskCustody(confirmed, acknowledgedEvent);
    expect(reduceTaskCustody(acknowledged, acknowledgedEvent)).toBe(acknowledged);
    expect(() =>
      reduceTaskCustody(
        acknowledged,
        custodyRecorded(12, { ...acknowledge, previousAcknowledgedGeneration: 1 }),
      ),
    ).toThrow(CustodyReplayConflict);
    const source2 = { kind: "run", sourceId: "second", resultRef: null };
    const concurrent = reduceTaskCustody(
      reserved,
      custodyRecorded(9, {
        type: "task.wake-source-recorded",
        source: source2,
        reservationId: "wake",
        dirtyGeneration: 2,
      }),
    );
    const launched = reduceTaskCustody(
      concurrent,
      custodyRecorded(10, {
        type: "task.wake-claimed",
        ...wake,
        managerGeneration: 2,
        dirtyGeneration: 2,
        messageId: "another-message",
        runId: "another-run",
        pending: { sources: [source, source2], overflowThroughGeneration: null },
      }),
    );
    const firstAck = reduceTaskCustody(
      launched,
      custodyRecorded(11, { ...acknowledge, dirtyGeneration: 2, observedDirtyGeneration: 2 }),
    );
    const secondAck = reduceTaskCustody(
      firstAck,
      custodyRecorded(12, {
        ...acknowledge,
        dirtyGeneration: 2,
        observedDirtyGeneration: 2,
        previousAcknowledgedGeneration: 1,
        acknowledgedGeneration: 2,
      }),
    );
    expect(HashMap.size(secondAck.pendingSources)).toBe(0);
    expect(secondAck.acknowledgedGeneration).toBe(2);
    expect(launched.acknowledgedGeneration).toBe(0);
    expect(HashMap.size(initial.reservations)).toBe(0);
  });

  it("retains bounded pending sources and overflow until their observed generation is acknowledged", () => {
    let state = reduceTaskCustody(
      undefined,
      custodyRecorded(1, { type: "task.manager-bound", ...identity }),
    );
    for (let n = 1; n <= 40; n++)
      state = reduceTaskCustody(
        state,
        custodyRecorded(n + 1, {
          type: "task.wake-source-recorded",
          source: { kind: "run", sourceId: `s${n}`, resultRef: null },
          reservationId: null,
          dirtyGeneration: n,
        }),
      );
    expect(HashMap.size(state.pendingSources)).toBe(32);
    expect(state.overflowThroughGeneration).toBe(40);
    expect(HashMap.size(state.sources)).toBe(40);
    const deduplicated = reduceTaskCustody(
      state,
      custodyRecorded(44, {
        type: "task.wake-source-recorded",
        source: { kind: "run", sourceId: "s1", resultRef: null },
        reservationId: null,
        dirtyGeneration: 1,
      }),
    );
    expect(deduplicated.dirtyGeneration).toBe(40);
    expect(HashMap.size(deduplicated.sources)).toBe(40);
    expect(deduplicated.overflowThroughGeneration).toBe(40);

    expect(() =>
      reduceTaskCustody(
        state,
        custodyRecorded(45, {
          type: "task.wake-source-recorded",
          source: { kind: "run", sourceId: "s1", resultRef: "changed" },
          reservationId: null,
          dirtyGeneration: 41,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
    expect(() =>
      reduceTaskCustody(
        state,
        custodyRecorded(46, {
          type: "task.wake-source-recorded",
          source: { kind: "run", sourceId: "fresh", resultRef: null },
          reservationId: null,
          dirtyGeneration: 39,
        }),
      ),
    ).toThrow(CustodyReplayConflict);
  });
});

function custodyRecorded(sequence: number, event: object) {
  const id = `custody-${sequence}`;
  const commandId = `custody-command-${sequence}`;
  return decode({
    event: { ...base, id, commandId, ...event },
    context: { sequence, eventId: id, occurredAt: base.occurredAt, commandId },
  }) as Parameters<typeof reduceTaskCustody>[1];
}

describe("validation task replay", () => {
  it("retains pinned validation history and uncertainty without inventing a debit or Done", () => {
    const binding = validationBinding;
    const reserved = validationRecorded(1, { ...binding, type: "task.validation-reserved" });
    const started = validationRecorded(2, {
      ...binding,
      type: "task.validation-started",
      runId: "validator-run",
      startedAt: base.occurredAt,
    });
    const uncertain = validationRecorded(3, {
      ...binding,
      type: "task.validation-uncertain",
      runId: "validator-run",
      startedAt: base.occurredAt,
      finishedAt: base.occurredAt,
      reason: "process outcome unknown",
    });
    const first = reduceTaskValidation(undefined, reserved);
    const running = reduceTaskValidation(first, started);
    const lost = reduceTaskValidation(running, uncertain);
    const newer = reduceTaskValidation(
      lost,
      validationRecorded(4, {
        ...binding,
        type: "task.validation-reserved",
        attemptId: "newer",
        allowanceUnit: 2,
        definitionRevision: 2,
        evidenceId: "new-evidence",
      }),
    );
    const result = validationRecorded(5, {
      ...binding,
      type: "task.validation-result",
      runId: "validator-run",
      startedAt: base.occurredAt,
      finishedAt: base.occurredAt,
      outcome: "pass",
      findings: [{ criterionId: "criterion", outcome: "pass", reason: "observed", evidence }],
    });
    const complete = reduceTaskValidation(newer, result);
    const attempt = HashMap.getUnsafe(complete.attempts, "attempt");
    expect(attempt.uncertain).toEqual(uncertain.event);
    expect(attempt.result).toEqual(result.event);
    expect(HashMap.size(complete.units)).toBe(2);
    expect(HashMap.size(complete.runs)).toBe(1);
    for (const event of [reserved, started, uncertain, result])
      expect(reduceTaskValidation(complete, event)).toBe(complete);
    expect(HashMap.getUnsafe(first.attempts, "attempt").started).toBeUndefined();
    expect(HashMap.getUnsafe(lost.attempts, "attempt").result).toBeUndefined();
    expect(replay([recorded(1, { type: "task.created", definition })]).completion).toBeUndefined();
    expect(
      reduceTaskCustody(undefined, custodyRecorded(1, { type: "task.manager-bound", ...identity }))
        .allowance,
    ).toEqual({ type: "unconfigured", consumed: 0 });
    expect(() =>
      reduceTaskValidation(
        complete,
        validationRecorded(6, {
          ...result.event,
          outcome: "fail",
          findings: [{ criterionId: "criterion", outcome: "fail", reason: "changed", evidence }],
        }),
      ),
    ).toThrow(ValidationReplayConflict);
  });

  it("rejects validation binding, run, allowance-unit and phase contradictions", () => {
    const reserved = validationRecorded(1, {
      ...validationBinding,
      type: "task.validation-reserved",
    });
    const state = reduceTaskValidation(undefined, reserved);
    const start = {
      ...validationBinding,
      type: "task.validation-started",
      runId: "run",
      startedAt: base.occurredAt,
    };
    for (const change of [
      { evidenceId: "other" },
      { claimId: "other" },
      { managerGeneration: 2 },
      { completionPolicyRevision: 2 },
      { definitionRevision: 2 },
      { allowanceUnit: 2 },
      { workWatermark: "other" },
      { validator: { ...validationBinding.validator, projectId: "other" } },
    ])
      expect(() =>
        reduceTaskValidation(state, validationRecorded(2, { ...start, ...change })),
      ).toThrow(ValidationReplayConflict);
    expect(() => reduceTaskValidation(undefined, validationRecorded(2, start))).toThrow(
      ValidationReplayConflict,
    );
    expect(() =>
      reduceTaskValidation(
        state,
        validationRecorded(2, {
          ...validationBinding,
          type: "task.validation-reserved",
          attemptId: "other",
        }),
      ),
    ).toThrow(ValidationReplayConflict);
    const running = reduceTaskValidation(state, validationRecorded(2, start));
    expect(() => reduceTaskValidation(running, validationRecorded(3, start))).toThrow(
      ValidationReplayConflict,
    );
    const second = reduceTaskValidation(
      running,
      validationRecorded(3, {
        ...validationBinding,
        type: "task.validation-reserved",
        attemptId: "other",
        allowanceUnit: 2,
      }),
    );
    expect(() =>
      reduceTaskValidation(
        second,
        validationRecorded(4, { ...start, attemptId: "other", allowanceUnit: 2 }),
      ),
    ).toThrow(ValidationReplayConflict);
    expect(() =>
      reduceTaskValidation(
        running,
        validationRecorded(3, {
          ...start,
          type: "task.validation-uncertain",
          runId: "different",
          finishedAt: base.occurredAt,
          reason: "unknown",
        }),
      ),
    ).toThrow(ValidationReplayConflict);
    expect(HashMap.size(state.runs)).toBe(0);
  });
});

const validationBinding = {
  attemptId: "attempt",
  managerGeneration: 1,
  claimId: "claim",
  evidenceId: "evidence",
  workWatermark: "watermark",
  validator: {
    projectId: "project",
    modelSelection: { instanceId: "codex", model: "model" },
    workspace: { type: "root" },
  },
  allowanceUnit: 1,
  createdAt: base.occurredAt,
};
function validationRecorded(sequence: number, event: object) {
  const id = `validation-${sequence}`;
  const commandId = `validation-command-${sequence}`;
  return decode({
    event: { ...base, ...event, id, commandId },
    context: { sequence, eventId: id, occurredAt: base.occurredAt, commandId },
  }) as Parameters<typeof reduceTaskValidation>[1];
}
