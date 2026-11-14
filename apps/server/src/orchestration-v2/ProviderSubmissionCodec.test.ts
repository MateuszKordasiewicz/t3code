// @effect-diagnostics-next-line nodeBuiltinImport:off -- independent raw-byte digest oracle for the synchronous codec.
import * as NodeCrypto from "node:crypto";

import { describe, expect, it } from "@effect/vitest";
import { stableStringify } from "@t3tools/shared/relaySigning";

import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import * as SubmissionCodec from "./ProviderSubmissionCodec.ts";
import {
  assertStoredAttemptMatchesFrame,
  decodeSubmissionTransportData,
  decodeSubmissionAttemptResourceData,
  encodeScopeBindingData,
  encodeScopeSettlementData,
  encodeSubmissionTransportData,
  ProviderSubmissionCodecError,
  snapshotSubmissionFrame,
} from "./ProviderSubmissionCodec.ts";

const createdAt = "2026-11-13T17:00:00.123456Z";
const transportId = "11111111-2222-3333-4444-555555555555";
const proposedUnit = "t3code-codex-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.scope";
const scopeNamespace = "app-t3code-agents-123456abcdef.slice";
const hash = "a".repeat(64);
const tagged = (kind: string, value: unknown) =>
  WorkflowCanonicalHash.tagged(`t3.${kind}.v1`, value);
const clone = <A>(value: A): A => JSON.parse(JSON.stringify(value));

function setup() {
  const claim = {
    operationId: "operation",
    expectedFingerprint: hash,
    expectedGeneration: 0,
    bootId: "coordinator-boot",
    workerId: "worker",
  };
  const context = {
    transportId,
    environmentId: "environment",
    projectId: "later-project",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
  };
  const selection = {
    v: 1,
    kind: "start",
    threadId: "later-thread",
    runId: "run",
    runOrdinal: 2,
    attemptId: "actual-t3-attempt",
    rootNodeId: "root",
    providerTurnOrdinal: 3,
    providerInstanceId: "instance",
    providerSessionId: "session",
    providerThreadId: "provider-thread",
    nativeThreadId: "native-thread",
    invocation: 1,
  };
  const creation = {
    context: {
      v: 1,
      environmentId: "environment",
      projectId: "opening-project",
      openingThreadId: "opening-thread",
      driver: "codex",
      providerInstanceId: "instance",
      providerSessionId: "session",
      scopeNamespace,
      hostBootId: "aaaaaaaa-1111-2222-3333-444444444444",
      expectedClaim: null,
    },
    transportId,
    proposedUnit,
    creationInputHash: hash,
    createdAt,
  };
  const params = {
    threadId: "native-thread",
    input: [
      {
        type: "text",
        text: "A real turn 🛶",
        text_elements: [{ byteRange: { start: 0, end: 1 }, placeholder: null }],
      },
      { type: "image", fileId: "image-file", detail: "high" },
      { type: "image", url: "https://example.invalid/image", detail: null },
      { type: "localImage", path: "/inputs/photo.png" },
      { type: "audio", url: "https://example.invalid/audio" },
      { type: "localAudio", path: "/inputs/audio.wav" },
      { type: "skill", name: "skill", path: "/skills/skill" },
      { type: "mention", name: "reference", path: "/inputs/reference" },
    ],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    clientUserMessageId: "client-message",
    cwd: "/work",
    disabledPluginIds: ["plugin"],
    effort: "high",
    model: "model",
    outputSchema: {
      type: "object",
      properties: { answer: { type: "string" } },
      additionalProperties: false,
    },
    personality: null,
    sandboxPolicy: { type: "dangerFullAccess" },
    serviceTier: "priority",
    serviceTierForTurn: "default",
    summary: "auto",
    toolOutput: { name: "tool", namespace: null, output: [{ type: "input_text", text: "result" }] },
    turnTrigger: "user",
    collaborationMode: {
      mode: "default",
      settings: {
        model: "model",
        reasoning_effort: "high",
        developer_instructions: "instructions",
      },
    },
    additionalContext: {
      instructions: { kind: "application", value: "context" },
      note: { kind: "untrusted", value: "quoted" },
    },
  };
  const binding = {
    unit: proposedUnit,
    invocationId: "b".repeat(32),
    controlGroup: `/user.slice/user-1000.slice/${scopeNamespace}/${proposedUnit}`,
    pid: 1234,
    pidStartTicks: "900719925474099312345",
  };
  return { claim, context, selection, creation, params, binding };
}
function makeFrame(selection: unknown, params: unknown, id: string | number = 7) {
  const encoded = `${JSON.stringify({ id, method: "turn/start", params })}\n`;
  return {
    encoded,
    byteLength: new TextEncoder().encode(encoded).byteLength,
    method: "turn/start",
    requestId: id,
    selection: JSON.stringify(selection),
  };
}
function withEncoded(frame: ReturnType<typeof makeFrame>, encoded: string) {
  return { ...frame, encoded, byteLength: new TextEncoder().encode(encoded).byteLength };
}
function without(value: object, key: string) {
  return Object.fromEntries(Object.entries(value).filter(([name]) => name !== key));
}
function invalid(body: () => unknown, reason: "invalid" | "conflict" = "invalid") {
  expect(body).toThrow(ProviderSubmissionCodecError);
  try {
    body();
  } catch (error) {
    expect(error).toMatchObject({ reason, message: `Provider submission data ${reason}` });
  }
}

describe("provider submission data codec", () => {
  it("hashes complete actual SDK payloads and distinguishes exact wire bytes from semantic hashes", () => {
    const { context, selection, params } = setup();
    const frame = makeFrame(selection, params);
    const row = snapshotSubmissionFrame(context, frame, createdAt);
    expect(row.input_hash).toBe(tagged("provider-submission-input", params));
    expect(row.selection_hash).toBe(tagged("provider-submission-selection", selection));
    expect(row.frame_hash).toBe(
      NodeCrypto.createHash("sha256").update(frame.encoded, "utf8").digest("hex"),
    );
    expect(row.selection_json).toBe(stableStringify(selection));
    expect(row.request_id_json).toBe("7");
    expect(Object.keys(row)).toHaveLength(25);
    expect(Object.isFrozen(row)).toBe(true);
    assertStoredAttemptMatchesFrame(row, context, frame);
    const reordered = {
      ...frame,
      encoded: `${JSON.stringify({ params: Object.fromEntries(Object.entries(params).reverse()), method: "turn/start", id: 7 })}\n`,
    };
    reordered.byteLength = new TextEncoder().encode(reordered.encoded).byteLength;
    const sameInput = snapshotSubmissionFrame(context, reordered, createdAt);
    expect(sameInput.input_hash).toBe(row.input_hash);
    expect(sameInput.frame_hash).not.toBe(row.frame_hash);
    for (const id of ["7", "", 0, -1]) {
      const otherFrame = makeFrame(selection, params, id);
      const other = snapshotSubmissionFrame(context, otherFrame, createdAt);
      expect(other.input_hash).toBe(row.input_hash);
      expect(other.frame_hash).not.toBe(row.frame_hash);
      expect(other.request_id_json).toBe(JSON.stringify(id));
      assertStoredAttemptMatchesFrame(other, context, otherFrame);
      invalid(() => assertStoredAttemptMatchesFrame(row, context, otherFrame), "conflict");
    }
    for (const [key, replacement] of Object.entries(params)) {
      if (key === "threadId" || key === "input") continue;
      const reduced = without(params, key);
      const next = snapshotSubmissionFrame(context, makeFrame(selection, reduced), createdAt);
      expect(next.input_hash, key).not.toBe(row.input_hash);
      expect(replacement).toBeDefined();
    }
    const changedText = clone(params);
    changedText.input[0]!.text = "changed text";
    expect(
      snapshotSubmissionFrame(context, makeFrame(selection, changedText), createdAt).input_hash,
    ).not.toBe(row.input_hash);
  });

  it("captures each actual shared turn without borrowing the opening thread or historical claim", () => {
    const { creation, context, selection, params, claim } = setup();
    const transport = encodeSubmissionTransportData(creation);
    expect(Object.keys(transport)).toHaveLength(21);
    expect(decodeSubmissionTransportData(transport)).toEqual(transport);
    for (const expectedGeneration of [0, 2]) {
      const selected = { ...selection, expectedClaim: { ...claim, expectedGeneration } };
      const frame = makeFrame(selected, params);
      const row = snapshotSubmissionFrame(context, frame, createdAt);
      expect(row.project_id).toBe("later-project");
      expect(row.thread_id).toBe("later-thread");
      expect(JSON.parse(row.selection_json).expectedClaim.expectedGeneration).toBe(
        expectedGeneration,
      );
      assertStoredAttemptMatchesFrame(row, context, frame);
      expect(row.selection_hash).not.toBe(
        snapshotSubmissionFrame(context, makeFrame(selection, params), createdAt).selection_hash,
      );
    }
    for (const key of Object.keys(selection)) {
      if (key === "v" || key === "kind") continue;
      const previous = selection[key as keyof typeof selection];
      const updated = {
        ...selection,
        [key]: typeof previous === "number" ? previous + 1 : `${previous}-other`,
      };
      const newParams =
        key === "nativeThreadId" ? { ...params, threadId: updated.nativeThreadId } : params;
      const newContext = {
        ...context,
        ...(key === "providerInstanceId" ? { providerInstanceId: updated.providerInstanceId } : {}),
        ...(key === "providerSessionId" ? { providerSessionId: updated.providerSessionId } : {}),
      };
      expect(
        snapshotSubmissionFrame(newContext, makeFrame(updated, newParams), createdAt)
          .selection_hash,
        key,
      ).not.toBe(tagged("provider-submission-selection", selection));
    }
    for (const key of Object.keys(claim)) {
      const previous = claim[key as keyof typeof claim];
      const expectedClaim = {
        ...claim,
        [key]:
          typeof previous === "number"
            ? previous + 1
            : key === "expectedFingerprint"
              ? "b".repeat(64)
              : `${previous}-other`,
      };
      const row = snapshotSubmissionFrame(
        context,
        makeFrame({ ...selection, expectedClaim }, params),
        createdAt,
      );
      expect(row.selection_hash, key).not.toBe(
        tagged("provider-submission-selection", { ...selection, expectedClaim: claim }),
      );
    }
    const claimedCreation = encodeSubmissionTransportData({
      ...creation,
      context: { ...creation.context, expectedClaim: claim },
    });
    expect(claimedCreation.creation_context_hash).not.toBe(transport.creation_context_hash);
    expect(decodeSubmissionTransportData(claimedCreation)).toEqual(claimedCreation);
  });

  it("rejects unsupported requests, malformed complete params and forged frame metadata", () => {
    const { context, selection, params } = setup();
    const frame = makeFrame(selection, params);
    for (const patch of [
      { byteLength: frame.byteLength + 1 },
      { byteLength: -1 },
      { method: "turn/compact" },
      { requestId: "7" },
      { requestId: null },
      { selection: null },
      { selection: "{}" },
      { extra: 1 },
    ])
      invalid(() => snapshotSubmissionFrame(context, { ...frame, ...patch }, createdAt));
    for (const request of [
      { id: 7, method: "turn/start" },
      { id: 7, method: "turn/start", params, jsonrpc: "2.0" },
      { id: 7, method: "turn/compact", params },
      { method: "turn/start", params },
      { id: 7, result: {} },
      { id: null, method: "turn/start", params },
      { id: 1.5, method: "turn/start", params },
      { id: Number.MAX_SAFE_INTEGER + 1, method: "turn/start", params },
    ])
      invalid(() =>
        snapshotSubmissionFrame(
          context,
          withEncoded(frame, `${JSON.stringify(request)}\n`),
          createdAt,
        ),
      );
    for (const badParams of [
      { ...params, threadId: "other" },
      { ...params, extra: true },
      { ...params, input: [{ type: "text", text: "x", extra: 1 }] },
      { ...params, additionalContext: { x: { kind: "invalid", value: "x" } } },
      { ...params, collaborationMode: { mode: "default", settings: { model: "x", extra: true } } },
      { ...params, toolOutput: { ...params.toolOutput, extra: true } },
      { ...params, outputSchema: { x: "\ud800" } },
    ])
      invalid(() => snapshotSubmissionFrame(context, makeFrame(selection, badParams), createdAt));
    for (const encoded of [
      frame.encoded.slice(0, -1),
      `${frame.encoded}\n`,
      `${frame.encoded.slice(0, -1)}\r\n`,
      ` ${frame.encoded}`,
      frame.encoded.replace('"id":7', '"id":7,"id":7'),
      frame.encoded.replace('"id":7', '"id":7.0'),
      frame.encoded.replace("A real turn", "A real \\u0074urn"),
      "not-json\n",
    ])
      invalid(() => snapshotSubmissionFrame(context, withEncoded(frame, encoded), createdAt));
    const big = { ...params, input: [{ type: "text", text: "x".repeat(1_048_576) }] };
    invalid(() => snapshotSubmissionFrame(context, makeFrame(selection, big), createdAt));
    const minimal = { threadId: "native-thread", input: [{ type: "text", text: "" }] };
    const overhead = makeFrame(selection, minimal).byteLength;
    const limit = makeFrame(selection, {
      ...minimal,
      input: [{ type: "text", text: "x".repeat(1_048_576 - overhead) }],
    });
    expect(limit.byteLength).toBe(1_048_576);
    snapshotSubmissionFrame(context, limit, createdAt);
  });

  it("rejects ambiguous selectors, invalid claims and context substitutions", () => {
    const { context, selection, params, claim } = setup();
    const frame = makeFrame(selection, params);
    for (const selected of [
      without(selection, "attemptId"),
      { ...selection, extra: 1 },
      { ...selection, kind: "compact" },
      { ...selection, v: 2 },
      { ...selection, runOrdinal: 0 },
      { ...selection, providerTurnOrdinal: 1.2 },
      { ...selection, invocation: Number.MAX_SAFE_INTEGER + 1 },
      { ...selection, threadId: " " },
      { ...selection, expectedClaim: null },
      { ...selection, expectedClaim: {} },
      ...Object.keys(claim).map((key) => ({ ...selection, expectedClaim: without(claim, key) })),
      { ...selection, expectedClaim: { ...claim, expectedGeneration: -1 } },
      {
        ...selection,
        expectedClaim: { ...claim, expectedGeneration: Number.MAX_SAFE_INTEGER + 1 },
      },
      { ...selection, expectedClaim: { ...claim, expectedFingerprint: "A".repeat(64) } },
      { ...selection, expectedClaim: { ...claim, extra: 1 } },
    ])
      invalid(() => snapshotSubmissionFrame(context, makeFrame(selected, params), createdAt));
    for (const selectionJson of [
      JSON.stringify(selection).replace('"v":1', '"v":1,"v":1'),
      JSON.stringify(selection).replace('"start"', '"\\u0073tart"'),
      ` ${JSON.stringify(selection)}`,
      JSON.stringify({ ...selection, expectedClaim: undefined }).replace(
        '"v":1',
        '"v":1,"expectedClaim":null',
      ),
    ])
      invalid(() =>
        snapshotSubmissionFrame(context, { ...frame, selection: selectionJson }, createdAt),
      );
    for (const patched of [
      without(context, "environmentId"),
      { ...context, driver: "claude" },
      { ...context, providerSessionId: "other" },
      { ...context, providerInstanceId: "other" },
      { ...context, extra: 1 },
      { ...context, transportId: "not-uuid" },
    ])
      invalid(() => snapshotSubmissionFrame(patched, frame, createdAt));
    for (const time of [
      "2026-02-30T00:00:00Z",
      "2026-11-13T00:00:00+00:00",
      "now",
      "2026-11-13T25:00:00Z",
    ])
      invalid(() => snapshotSubmissionFrame(context, frame, time));
  });

  it("validates pending and observed attempts against actual bytes, never a cached digest", () => {
    const { context, selection, params } = setup();
    const frame = makeFrame(selection, params);
    const row = snapshotSubmissionFrame(context, frame, createdAt);
    for (const observation of [
      { local_outcome: "unknown", local_reason: "interrupted", local_observed_at: createdAt },
      {
        local_outcome: "not_submitted",
        local_reason: "wrapper",
        local_observed_at: "2020-01-01T00:00:00Z",
      },
      { local_outcome: "submitted", local_reason: "completed", local_observed_at: createdAt },
    ])
      assertStoredAttemptMatchesFrame({ ...row, ...observation }, context, frame);
    for (const [key, value] of Object.entries(row)) {
      const replacement =
        typeof value === "string" ? `${value}-other` : value === null ? "other" : value + 1;
      invalid(
        () => assertStoredAttemptMatchesFrame({ ...row, [key]: replacement }, context, frame),
        "conflict",
      );
    }
    for (const patch of [
      { input_hash: "b".repeat(64) },
      { frame_hash: "b".repeat(64) },
      { selection_hash: "b".repeat(64) },
      { selection_json: JSON.stringify(selection) },
      { request_id_json: '"7"' },
      { request_id_json: "7.0" },
      { local_outcome: "submitted", local_reason: "sink", local_observed_at: createdAt },
      { local_outcome: "unknown", local_reason: "completed", local_observed_at: createdAt },
      { local_outcome: "unknown", local_reason: "not-a-reason", local_observed_at: createdAt },
      { local_outcome: "unknown", local_reason: "sink", local_observed_at: "bad-time" },
      { extra: true },
    ])
      invalid(
        () => assertStoredAttemptMatchesFrame({ ...row, ...patch }, context, frame),
        "conflict",
      );
    invalid(
      () => assertStoredAttemptMatchesFrame(without(row, "frame_hash"), context, frame),
      "conflict",
    );
    const changedFrame = makeFrame(selection, { ...params, model: "different-model" });
    invalid(() => assertStoredAttemptMatchesFrame(row, context, changedFrame), "conflict");
    for (const key of ["transportId", "environmentId", "projectId"]) {
      const changed = {
        ...context,
        [key]: key === "transportId" ? "aaaaaaaa-2222-3333-4444-555555555555" : "other",
      };
      invalid(() => assertStoredAttemptMatchesFrame(row, changed, frame), "conflict");
    }
  });

  it("validates fresh, bound and settled transport data with no inferred launch or stop authority", () => {
    const { creation, binding } = setup();
    const row = encodeSubmissionTransportData(creation);
    expect(row.creation_context_hash).toBe(tagged("provider-submission-context", creation.context));
    expect(
      decodeSubmissionTransportData({ ...row, creation_input_hash: "f".repeat(64) }),
    ).toMatchObject({ creation_input_hash: "f".repeat(64) });
    const encodedBinding = encodeScopeBindingData(binding, row);
    expect(encodedBinding).toEqual({
      json: stableStringify(binding),
      hash: tagged("provider-scope-binding", binding),
    });
    const bound = {
      ...row,
      binding_json: encodedBinding.json,
      binding_hash: encodedBinding.hash,
      bound_at: createdAt,
    };
    expect(decodeSubmissionTransportData(bound)).toEqual(bound);
    for (const observation of ["empty", "removed"]) {
      const settlement = { binding, observation };
      const encoded = encodeScopeSettlementData(settlement, binding);
      expect(encoded.hash).toBe(tagged("provider-scope-settlement", settlement));
      const settled = {
        ...bound,
        settlement_json: encoded.json,
        settlement_hash: encoded.hash,
        settled_at: "2020-01-01T00:00:00Z",
      };
      expect(decodeSubmissionTransportData(settled)).toEqual(settled);
      expect(Object.isFrozen(decodeSubmissionTransportData(settled))).toBe(true);
      invalid(() =>
        decodeSubmissionTransportData({
          ...row,
          settlement_json: encoded.json,
          settlement_hash: encoded.hash,
          settled_at: createdAt,
        }),
      );
      for (const key of [
        "binding_json",
        "binding_hash",
        "bound_at",
        "settlement_json",
        "settlement_hash",
        "settled_at",
      ]) {
        invalid(() => decodeSubmissionTransportData({ ...settled, [key]: null }));
      }
      invalid(() =>
        decodeSubmissionTransportData({ ...settled, settlement_json: JSON.stringify(settlement) }),
      );
      invalid(() => decodeSubmissionTransportData({ ...settled, settlement_hash: "b".repeat(64) }));
    }
    for (const key of Object.keys(row)) {
      const value = row[key as keyof typeof row];
      const replacement =
        typeof value === "number" ? 2 : typeof value === "string" ? `${value}-other` : "other";
      invalid(() => decodeSubmissionTransportData({ ...row, [key]: replacement }));
    }
    invalid(() => decodeSubmissionTransportData({ ...row, extra: true }));
    invalid(() => decodeSubmissionTransportData(without(row, "environment_id")));
    invalid(() =>
      decodeSubmissionTransportData({
        ...row,
        creation_context_json: JSON.stringify(creation.context),
      }),
    );
    invalid(() => decodeSubmissionTransportData({ ...bound, binding_hash: "c".repeat(64) }));
    invalid(() =>
      decodeSubmissionTransportData({ ...bound, binding_json: JSON.stringify(binding) }),
    );
    invalid(() => decodeSubmissionTransportData({ ...bound, bound_at: "bad" }));
  });

  it("rejects malformed creation metadata without trimming, coercing or dropping fields", () => {
    const { creation, claim } = setup();
    for (const patch of [
      { transportId: "00000000-0000-0000-0000-000000000000" },
      { transportId: transportId.toUpperCase().replace("11111111", "AAAAAAAA") },
      { proposedUnit: "t3code-codex-12345678.scope" },
      { proposedUnit: "other.scope" },
      { creationInputHash: "A".repeat(64) },
      { createdAt: "2026-02-29T00:00:00Z" },
      { extra: 1 },
    ])
      invalid(() => encodeSubmissionTransportData({ ...creation, ...patch }));
    for (const context of [
      without(creation.context, "expectedClaim"),
      { ...creation.context, v: 2 },
      { ...creation.context, driver: "other" },
      { ...creation.context, hostBootId: "00000000-0000-0000-0000-000000000000" },
      { ...creation.context, scopeNamespace: "app-t3code-agents-123456abcdef.slice-other" },
      { ...creation.context, expectedClaim: { ...claim, extra: true } },
      { ...creation.context, extra: true },
      { ...creation.context, projectId: "" },
    ])
      invalid(() => encodeSubmissionTransportData({ ...creation, context }));
  });

  it("checks exact scope namespace, birth identity and every settlement binding scalar", () => {
    const { creation, binding } = setup();
    const row = encodeSubmissionTransportData(creation);
    const base = binding.controlGroup;
    for (const patch of [
      { unit: "other.scope" },
      { invocationId: "0".repeat(32) },
      { invocationId: "B".repeat(32) },
      { pid: 0 },
      { pid: 1.5 },
      { pid: Number.MAX_SAFE_INTEGER + 1 },
      { pidStartTicks: 1 },
      { pidStartTicks: "01" },
      { pidStartTicks: "-1" },
      { extra: true },
      ...[
        "/",
        `${base}/`,
        `${base}/..`,
        base.replace("/user.slice/", "/./"),
        base.replace("/user.slice/", "//"),
        base.replace(scopeNamespace, `${scopeNamespace}-prefix`),
        base.replace(scopeNamespace, "other.slice"),
        base.replace("user.slice", "user\u0000.slice"),
        base.replace("user.slice", "user\ud800.slice"),
      ].map((controlGroup) => ({ controlGroup })),
    ])
      invalid(() => encodeScopeBindingData({ ...binding, ...patch }, row));
    encodeScopeBindingData({ ...binding, pidStartTicks: "0" }, row);
    encodeScopeBindingData({ ...binding, controlGroup: `/${scopeNamespace}/${proposedUnit}` }, row);
    const nested = {
      ...binding,
      controlGroup: `/${scopeNamespace}/app-t3code-agents-aaaaaaaaaaaa.slice/${proposedUnit}`,
    };
    encodeScopeBindingData(nested, row);
    encodeScopeSettlementData({ binding: nested, observation: "empty" }, nested);
    for (const key of Object.keys(binding)) {
      const original = binding[key as keyof typeof binding];
      const changed = {
        ...binding,
        [key]: typeof original === "number" ? original + 1 : `${original}-other`,
      };
      invalid(() => encodeScopeSettlementData({ binding: changed, observation: "empty" }, binding));
    }
    for (const settlement of [
      true,
      null,
      {},
      { binding, observation: true },
      { binding, observation: "unknown" },
      { binding, observation: "empty", extra: 1 },
      { binding: { ...binding, extra: 1 }, observation: "empty" },
    ])
      invalid(() => encodeScopeSettlementData(settlement, binding));
    invalid(() =>
      encodeScopeSettlementData({ binding, observation: "empty" }, { ...binding, pid: "1234" }),
    );
  });

  it("owns inputs synchronously and rejects non-JSON data before invoking getters", () => {
    const { context, selection, params, creation, binding } = setup();
    const frame = makeFrame(selection, params);
    const saved = snapshotSubmissionFrame(context, frame, createdAt);
    const transport = encodeSubmissionTransportData(creation);
    const parsed = decodeSubmissionTransportData(transport);
    const encodedBinding = encodeScopeBindingData(binding, transport);
    const settlement = { binding, observation: "removed" };
    const encodedSettlement = encodeScopeSettlementData(settlement, binding);
    context.projectId = "mutated";
    frame.encoded = "mutated";
    creation.context.openingThreadId = "mutated";
    binding.pid = 99;
    settlement.observation = "empty";
    expect(saved.project_id).toBe("later-project");
    expect(parsed.opening_thread_id).toBe("opening-thread");
    expect(JSON.parse(encodedBinding.json).pid).toBe(1234);
    expect(JSON.parse(encodedSettlement.json)).toMatchObject({
      observation: "removed",
      binding: { pid: 1234 },
    });
    expect(Object.isFrozen(encodedSettlement)).toBe(true);
    let getterCalls = 0;
    const getter = Object.defineProperty({}, "projectId", {
      enumerable: true,
      get() {
        getterCalls += 1;
        return "got";
      },
    });
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    const poisoned = Object.create({ inherited: true });
    const nonEnumerable = Object.defineProperty({}, "hidden", { value: 1 });
    for (const bad of [
      getter,
      cycle,
      poisoned,
      nonEnumerable,
      new Map(),
      new (class InvalidPrototype {
        value = 1;
      })(),
      { x: undefined },
      { x: Symbol("x") },
      { [Symbol("x")]: 1 },
      { x: NaN },
      { x: Infinity },
      { x: -0 },
      { x: 1n },
      { x: () => 1 },
      { x: Object.assign([], { length: 2 }) },
      { x: "\ud800" },
      { "\ud800": 1 },
    ]) {
      invalid(() => encodeSubmissionTransportData(bad));
      invalid(() => snapshotSubmissionFrame(bad, frame, createdAt));
      invalid(() => encodeScopeBindingData(bad, transport));
    }
    expect(getterCalls).toBe(0);
    const fresh = setup();
    const unsafeFrame = Object.defineProperty(
      { ...makeFrame(fresh.selection, fresh.params) },
      "encoded",
      {
        enumerable: true,
        get() {
          getterCalls += 1;
          return "";
        },
      },
    );
    invalid(() => snapshotSubmissionFrame(fresh.context, unsafeFrame, createdAt));
    expect(getterCalls).toBe(0);
  });
});

it("decodes only present attempt resource data and never invents missing frame preimages", () => {
  const { context, selection, params } = setup();
  const frame = makeFrame(selection, params);
  const row = snapshotSubmissionFrame(context, frame, createdAt);
  expect(decodeSubmissionAttemptResourceData(row)).toEqual(row);
  expect(Object.isFrozen(decodeSubmissionAttemptResourceData(row))).toBe(true);
  const absentPreimages = { ...row, input_hash: "b".repeat(64), frame_hash: "c".repeat(64) };
  expect(decodeSubmissionAttemptResourceData(absentPreimages)).toEqual(absentPreimages);
  invalid(() => assertStoredAttemptMatchesFrame(absentPreimages, context, frame), "conflict");
  for (const [key, value] of Object.entries(row)) {
    if (key === "environment_id" || key === "project_id") {
      const changed = { ...row, [key]: "different-valid-id" };
      expect(decodeSubmissionAttemptResourceData(changed)).toEqual(changed);
      invalid(() => assertStoredAttemptMatchesFrame(changed, context, frame), "conflict");
      continue;
    }
    const replacement =
      typeof value === "string" ? `${value}-changed` : value === null ? "changed" : value + 1;
    invalid(() => decodeSubmissionAttemptResourceData({ ...row, [key]: replacement }));
  }
  for (const patch of [
    { request_id_json: "7.0" },
    { selection_json: JSON.stringify(selection) },
    { input_hash: "A".repeat(64) },
    { frame_hash: "x".repeat(64) },
    { local_outcome: "unknown", local_reason: "completed", local_observed_at: createdAt },
    { local_outcome: "unknown", local_reason: "sink", local_observed_at: "invalid" },
    { extra: 1 },
  ])
    invalid(() => decodeSubmissionAttemptResourceData({ ...row, ...patch }));
  invalid(() => decodeSubmissionAttemptResourceData(without(row, "local_outcome")));
  expect(
    decodeSubmissionAttemptResourceData({
      ...row,
      local_outcome: "not_submitted",
      local_reason: "wrapper",
      local_observed_at: createdAt,
    }),
  ).toMatchObject({ local_outcome: "not_submitted" });
});

function conversationFixture(enabled = true) {
  const selected = {
    v: 1,
    kind: "fresh-conversation",
    transportId,
    environmentId: "environment",
    projectId: "project",
    threadId: "thread",
    runId: "run",
    runOrdinal: 1,
    attemptId: "attempt",
    rootNodeId: "root",
    intentId: "intent",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    providerThreadId: "original-provider-row",
    invocation: 1,
    expectedClaim: {
      operationId: "operation",
      expectedFingerprint: hash,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
    },
  };
  const context = {
    transportId,
    environmentId: "environment",
    projectId: "project",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    expectedSelection: selected,
  };
  const secret = "synthetic_native_conversation_credential_0123456789";
  const server = {
    url: "http://127.0.0.1:9999/mcp",
    tool_timeout_sec: 3900,
    http_headers: { Authorization: `Bearer ${secret}` },
  };
  const params = {
    model: "fixture-model",
    cwd: "/fixture/workspace",
    config: {
      "tools.update_plan.enabled": true,
      mcp_optional_startup_grace_ms: 10000,
      "features.concurrent_reasoning_summaries": true,
      "shell_environment_policy.set.T3CODE_THREAD_ID": selected.threadId,
      ...(enabled
        ? {
            "mcp_servers.t3-code": { ...server, disabled_tools: ["t3_thread_wait"] },
            "mcp_servers.t3-code-direct": {
              ...server,
              enabled_tools: ["t3_thread_wait"],
              omit_tools_from: ["code_mode", "deferred"],
            },
          }
        : {}),
    },
  };
  const mcp = enabled
    ? {
        mode: "issued",
        credentialId: "credential-namespace",
        capabilities: ["orchestration", "pull-requests", "worktree"],
        browserToolsAvailable: false,
        subagent: false,
        linked: "absent",
      }
    : { mode: "disabled" };
  const encoded = JSON.stringify({ id: 3, method: "thread/start", params }) + "\n";
  const frame = {
    encoded,
    byteLength: new TextEncoder().encode(encoded).byteLength,
    method: "thread/start",
    requestId: 3,
    selection: stableStringify(selected),
  };
  return { context, selected, params, mcp, frame, secret };
}

it.each([false, true])(
  "captures fresh conversation evidence with MCP=%s without credential preimages",
  (enabled) => {
    const fixture = conversationFixture(enabled);
    const snapshot = SubmissionCodec.snapshotConversationFrame;
    const receipt = snapshot(
      fixture.context,
      fixture.frame,
      fixture.params,
      fixture.mcp,
      createdAt,
    );
    expect(receipt).toMatchObject({
      method: "thread/start",
      provider_thread_id: "original-provider-row",
      local_outcome: null,
      response_json: null,
    });
    expect(JSON.stringify(receipt)).not.toContain(fixture.secret);
    expect(JSON.stringify(receipt)).not.toContain("Authorization");
    expect(receipt).not.toHaveProperty("frame_hash");
    expect(receipt).not.toHaveProperty("native_thread_id");
    expect(receipt).not.toHaveProperty("provider_turn_ordinal");
  },
);

function conversationFrame(params: unknown, selection: unknown, id = 3) {
  const encoded = JSON.stringify({ id, method: "thread/start", params }) + "\n";
  return {
    encoded,
    byteLength: new TextEncoder().encode(encoded).byteLength,
    method: "thread/start",
    requestId: id,
    selection: stableStringify(selection),
  };
}

it("compares exact ephemeral conversation input while hashing only the nonsecret projection", () => {
  const fixture = conversationFixture();
  const snapshot = SubmissionCodec.snapshotConversationFrame;
  const row = snapshot(fixture.context, fixture.frame, fixture.params, fixture.mcp, createdAt);
  const secondSecret = "a_different_synthetic_conversation_credential_987654321";
  const changed = JSON.parse(
    JSON.stringify(fixture.params).replaceAll(fixture.secret, secondSecret),
  );
  const changedFrame = conversationFrame(changed, fixture.selected);
  invalid(() => snapshot(fixture.context, changedFrame, fixture.params, fixture.mcp, createdAt));
  expect(snapshot(fixture.context, changedFrame, changed, fixture.mcp, createdAt)).toEqual(row);
  expect(row.input_hash).toBe(tagged("provider-conversation-input", JSON.parse(row.input_json)));
  expect(SubmissionCodec.decodeConversationReceiptData(row)).toEqual(row);
  expect(Object.isFrozen(row)).toBe(true);
  SubmissionCodec.assertStoredConversationMatchesFrame(
    row,
    fixture.context,
    fixture.frame,
    fixture.params,
    fixture.mcp,
  );
  invalid(
    () =>
      SubmissionCodec.assertStoredConversationMatchesFrame(
        { ...row, request_id_json: "4" },
        fixture.context,
        fixture.frame,
        fixture.params,
        fixture.mcp,
      ),
    "conflict",
  );
});

it("rejects forged conversation frames, source selections, unsupported configuration and secret aliases", () => {
  const f = conversationFixture();
  const snapshot = SubmissionCodec.snapshotConversationFrame;
  const withParams = (params: unknown) =>
    snapshot(f.context, conversationFrame(params, f.selected), params, f.mcp, createdAt);
  for (const frame of [
    { ...f.frame, extra: 1 },
    { ...f.frame, byteLength: f.frame.byteLength + 1 },
    { ...f.frame, method: "turn/start" },
    { ...f.frame, requestId: "3" },
    { ...f.frame, selection: stableStringify({ ...f.selected, invocation: 2 }) },
    { ...f.frame, encoded: f.frame.encoded + "\n", byteLength: f.frame.byteLength + 1 },
    conversationFrame({ ...f.params, cwd: "x".repeat(1048576) }, f.selected),
    conversationFrame(f.params, f.selected, 0),
    conversationFrame(f.params, { ...f.selected, expectedClaim: null }),
    conversationFrame(f.params, { ...f.selected, nativeThreadId: "invented" }),
  ])
    invalid(() => snapshot(f.context, frame, f.params, f.mcp, createdAt));
  for (const params of [
    { ...f.params, developerInstructions: f.secret },
    { ...f.params, config: { ...f.params.config, secret: f.secret } },
    { ...f.params, config: { ...f.params.config, "mcp_servers.foreign": { token: f.secret } } },
    {
      ...f.params,
      config: {
        ...f.params.config,
        "shell_environment_policy.set.T3CODE_THREAD_ID": "foreign-thread",
      },
    },
    { ...f.params, model: f.secret },
    { ...f.params, cwd: `/workspace/${f.secret}` },
    {
      ...f.params,
      config: {
        ...f.params.config,
        "mcp_servers.t3-code-direct": {
          ...f.params.config["mcp_servers.t3-code-direct"],
          http_headers: { Authorization: "Bearer wrong_credential_value_0000000000000000000000" },
        },
      },
    },
  ]) {
    invalid(() => withParams(params));
    try {
      withParams(params);
    } catch (error) {
      expect(String(error)).toBe("ProviderSubmissionCodecError: Provider submission data invalid");
      expect(String(error)).not.toContain(f.secret);
    }
  }
  invalid(() =>
    snapshot({ ...f.context, environmentId: "foreign" }, f.frame, f.params, f.mcp, createdAt),
  );
  invalid(() => snapshot(f.context, f.frame, f.params, { mode: "disabled" }, createdAt));
  invalid(() =>
    snapshot(f.context, f.frame, f.params, { ...f.mcp, credentialId: f.secret }, createdAt),
  );
  invalid(() =>
    snapshot(f.context, f.frame, f.params, { ...f.mcp, capabilities: ["arbitrary"] }, createdAt),
  );
});

it("records one local conversation outcome and then only the original returned identity", () => {
  const f = conversationFixture();
  const row = SubmissionCodec.snapshotConversationFrame(
    f.context,
    f.frame,
    f.params,
    f.mcp,
    createdAt,
  );
  const response = {
    v: 1,
    nativeThreadId: "native-conversation",
    returnedProviderThreadId: "derived-provider-row",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    threadId: "thread",
  };
  const outcome = { kind: "submitted", reason: "completed" };
  invalid(
    () => SubmissionCodec.encodeConversationResponseData(row, response, createdAt, credential(f)),
    "conflict",
  );
  const submitted = SubmissionCodec.encodeConversationOutcomeData(row, outcome, createdAt);
  expect(SubmissionCodec.encodeConversationOutcomeData(submitted, outcome, createdAt)).toEqual(
    submitted,
  );
  const opened = SubmissionCodec.encodeConversationResponseData(
    submitted,
    response,
    createdAt,
    credential(f),
  );
  expect(
    SubmissionCodec.encodeConversationResponseData(opened, response, createdAt, credential(f)),
  ).toEqual(opened);
  expect(JSON.parse(String(opened.response_json))).toEqual(response);
  expect(opened.provider_thread_id).toBe("original-provider-row");
  expect(SubmissionCodec.decodeConversationReceiptData(opened)).toEqual(opened);
  for (const change of [
    { kind: "unknown", reason: "sink" },
    { kind: "not_submitted", reason: "wrapper" },
    { kind: "submitted", reason: "interrupted" },
    { ...outcome, frame: f.frame },
  ])
    invalid(
      () => SubmissionCodec.encodeConversationOutcomeData(submitted, change, createdAt),
      "conflict",
    );
  for (const change of [
    { ...response, nativeThreadId: "replacement" },
    { ...response, providerSessionId: "foreign" },
    { ...response, rawResponse: { token: f.secret } },
  ])
    invalid(
      () =>
        SubmissionCodec.encodeConversationResponseData(opened, change, createdAt, credential(f)),
      "conflict",
    );
  for (const kind of ["unknown", "not_submitted"] as const) {
    const unresolved = SubmissionCodec.encodeConversationOutcomeData(
      row,
      { kind, reason: "sink" },
      createdAt,
    );
    expect(SubmissionCodec.decodeConversationReceiptData(unresolved)).toEqual(unresolved);
    invalid(
      () =>
        SubmissionCodec.encodeConversationResponseData(
          unresolved,
          response,
          createdAt,
          credential(f),
        ),
      "conflict",
    );
  }
});

it("rejects malformed durable conversation evidence without accepting absent secret preimages as proof", () => {
  const f = conversationFixture();
  const row = SubmissionCodec.snapshotConversationFrame(
    f.context,
    f.frame,
    f.params,
    f.mcp,
    createdAt,
  );
  for (const patch of [
    { extra: "unknown" },
    { transport_id: "foreign" },
    { intent_id: "foreign" },
    { input_hash: "b".repeat(64) },
    { selection_hash: "c".repeat(64) },
    { request_id_json: "3.0" },
    { request_id_json: '"3"' },
    { created_at: "bad" },
    { local_outcome: "unknown" },
    { local_outcome: "submitted", local_reason: "sink", local_observed_at: createdAt },
    { response_json: "{}", response_hash: hash, response_observed_at: createdAt },
    { input_json: stableStringify({ ...JSON.parse(row.input_json), token: f.secret }) },
  ])
    invalid(() => SubmissionCodec.decodeConversationReceiptData({ ...row, ...patch }));
  const input = JSON.parse(row.input_json);
  input.mcp.capabilities.reverse();
  invalid(() =>
    SubmissionCodec.decodeConversationReceiptData({ ...row, input_json: stableStringify(input) }),
  );
  invalid(() => decodeSubmissionAttemptResourceData(row));
});

it("refuses original credential material returned as conversation identity", () => {
  const f = conversationFixture();
  const row = SubmissionCodec.snapshotConversationFrame(
    f.context,
    f.frame,
    f.params,
    f.mcp,
    createdAt,
  );
  const submitted = SubmissionCodec.encodeConversationOutcomeData(
    row,
    { kind: "submitted", reason: "completed" },
    createdAt,
  );
  for (const field of ["nativeThreadId", "returnedProviderThreadId"]) {
    for (const secret of [f.secret, `Bearer ${f.secret}`]) {
      invalid(
        () =>
          SubmissionCodec.encodeConversationResponseData(
            submitted,
            {
              v: 1,
              nativeThreadId: "original-native-thread",
              returnedProviderThreadId: "derived-row",
              [field]: secret,
              driver: "codex",
              providerInstanceId: "instance",
              providerSessionId: "session",
              threadId: "thread",
            },
            createdAt,
            credential(f),
          ),
        "conflict",
      );
    }
  }
});

function credential(f: ReturnType<typeof conversationFixture>) {
  return {
    mode: "issued",
    credentialId: "credential-namespace",
    authorizationHeader: `Bearer ${f.secret}`,
  };
}

it("bounds returned conversation evidence and requires its original ephemeral credential context", () => {
  const f = conversationFixture();
  const row = SubmissionCodec.snapshotConversationFrame(
    f.context,
    f.frame,
    f.params,
    f.mcp,
    createdAt,
  );
  const submitted = SubmissionCodec.encodeConversationOutcomeData(
    row,
    { kind: "submitted", reason: "completed" },
    createdAt,
  );
  const response = {
    v: 1,
    nativeThreadId: "original-native-thread",
    returnedProviderThreadId: "derived-row",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    threadId: "thread",
  };
  for (const descriptor of [
    undefined,
    { mode: "disabled" },
    { ...credential(f), credentialId: "foreign" },
  ])
    invalid(
      () =>
        SubmissionCodec.encodeConversationResponseData(submitted, response, createdAt, descriptor),
      "conflict",
    );
  for (const nativeThreadId of ["x".repeat(16384), "🛶".repeat(4096)]) {
    const oversized = { ...response, nativeThreadId };
    invalid(() =>
      SubmissionCodec.decodeConversationReceiptData({
        ...submitted,
        response_json: stableStringify(oversized),
        response_hash: tagged("provider-conversation-response", oversized),
        response_observed_at: createdAt,
      }),
    );
    invalid(
      () =>
        SubmissionCodec.encodeConversationResponseData(
          submitted,
          { ...response, nativeThreadId },
          createdAt,
          credential(f),
        ),
      "conflict",
    );
  }
});
