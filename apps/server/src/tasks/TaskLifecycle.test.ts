import { WorkTaskPersistedEvent } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import { describe, expect, it } from "vite-plus/test";
import { FoundationReplayConflict, reduceTaskFoundation } from "./TaskLifecycle.ts";

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
