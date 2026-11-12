import { assert, it } from "@effect/vitest";
import { PlanningPassV1, PlanningSessionV1 } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Observation from "./PlanningPassObservation.ts";

const sessionId = Schema.decodeSync(PlanningSessionV1.fields.id)("session");
const passId = Schema.decodeSync(PlanningPassV1.fields.id)("pass");
const operationId = Schema.decodeSync(PlanningPassV1.fields.operationId)("operation");
const native = Schema.decodeSync(PlanningPassV1.fields.native.schema)({
  threadId: "thread",
  runId: "run",
  terminalMessageId: "message",
});
const input = { sessionId, passId, operationId, native };

it.effect("defaults to unknown regardless of native terminal identity", () =>
  Effect.gen(function* () {
    const observer = yield* Observation.PlanningPassObservation;
    assert.deepStrictEqual(yield* observer.observe(input), { kind: "unknown" });
    assert.deepStrictEqual(
      yield* observer.observe({
        ...input,
        native: { threadId: native.threadId, runId: native.runId },
      }),
      { kind: "unknown" },
    );
  }).pipe(Effect.provide(Observation.unknownObservationLayer)),
);
