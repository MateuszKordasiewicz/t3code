// @effect-diagnostics-next-line nodeBuiltinImport:off -- exact outbound byte hashing is synchronous at the ownership boundary.
import * as NodeCrypto from "node:crypto";

import { isWorkflowTimestamp } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as Schema from "effect/Schema";

import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

const paramsSchema = CodexSchema.V2TurnStartParams.pipe(
  Schema.fieldsAssign({
    collaborationMode: Schema.optionalKey(CodexSchema.ClientRequest__CollaborationMode),
    additionalContext: Schema.optionalKey(
      Schema.Record(Schema.String, CodexSchema.V2TurnStartParams__AdditionalContextEntry),
    ),
  }),
);
const decodeParams = Schema.decodeUnknownSync(paramsSchema);
const encodeParams = Schema.encodeSync(paramsSchema);
const transportKeys =
  "format_version transport_id environment_id project_id opening_thread_id driver provider_instance_id provider_session_id host_boot_id scope_namespace proposed_unit creation_context_json creation_context_hash creation_input_hash created_at binding_json binding_hash bound_at settlement_json settlement_hash settled_at".split(
    " ",
  );
const attemptKeys =
  "format_version attempt_id transport_id environment_id project_id thread_id run_id run_ordinal root_node_id provider_turn_ordinal driver provider_instance_id provider_session_id provider_thread_id native_thread_id method selection_json selection_hash input_hash frame_hash request_id_json created_at local_outcome local_reason local_observed_at".split(
    " ",
  );
const selectionKeys =
  "v kind threadId runId runOrdinal attemptId rootNodeId providerTurnOrdinal providerInstanceId providerSessionId providerThreadId nativeThreadId invocation".split(
    " ",
  );
const claimKeys = "operationId expectedFingerprint expectedGeneration bootId workerId".split(" ");
const contextKeys =
  "v environmentId projectId openingThreadId driver providerInstanceId providerSessionId scopeNamespace hostBootId expectedClaim".split(
    " ",
  );
const turnContextKeys =
  "transportId environmentId projectId driver providerInstanceId providerSessionId".split(" ");
const bindingKeys = "unit invocationId controlGroup pid pidStartTicks".split(" ");
const reasons = new Set(
  "completed invalid_frame queue_limit output_blocked reentrant frame_limit invalid_timeout continuation wrapper sink timeout interrupted outcome_hook".split(
    " ",
  ),
);
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// @effect-diagnostics-next-line extendsNativeError:off -- callers receive a synchronous bounded error before constructing an Effect.
export class ProviderSubmissionCodecError extends Error {
  readonly reason: "invalid" | "conflict";
  constructor(reason: "invalid" | "conflict") {
    super(`Provider submission data ${reason}`);
    this.name = "ProviderSubmissionCodecError";
    this.reason = reason;
  }
}

function requireData(condition: unknown): asserts condition {
  if (!condition) throw new ProviderSubmissionCodecError("invalid");
}
function boundary<A>(body: () => A, reason: "invalid" | "conflict" = "invalid"): A {
  try {
    const result = body();
    freeze(result);
    return result;
  } catch {
    throw new ProviderSubmissionCodecError(reason);
  }
}
function freeze(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  for (const child of Object.values(value)) freeze(child);
  Object.freeze(value);
}
function unicode(value: unknown): void {
  if (typeof value === "number") requireData(!Object.is(value, -0));
  else if (typeof value === "string") requireData(value.isWellFormed());
  else if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value)) {
      requireData(key.isWellFormed());
      unicode(child);
    }
  }
}
function own(value: unknown): unknown {
  WorkflowCanonicalHash.assertCanonical(value);
  unicode(value);
  return JSON.parse(stableStringify(value));
}
function exact(value: unknown, keys: readonly string[]): Record<string, unknown> {
  requireData(value !== null && typeof value === "object" && !Array.isArray(value));
  const record = value as Record<string, unknown>;
  requireData(
    Object.keys(record).length === keys.length && keys.every((key) => Object.hasOwn(record, key)),
  );
  return record;
}
function text(value: unknown): string {
  requireData(typeof value === "string" && value.isWellFormed() && value.trim().length > 0);
  return value;
}
function integer(value: unknown, minimum: number): number {
  requireData(typeof value === "number" && Number.isSafeInteger(value) && value >= minimum);
  return value;
}
function digest(value: unknown): string {
  const result = text(value);
  requireData(/^[0-9a-f]{64}$/.test(result));
  return result;
}
function uuid(value: unknown): string {
  const result = text(value);
  requireData(uuidPattern.test(result) && result !== "00000000-0000-0000-0000-000000000000");
  return result;
}
function timestamp(value: unknown): string {
  const result = text(value);
  requireData(isWorkflowTimestamp(result));
  return result;
}
function tagged(kind: string, value: unknown): string {
  return WorkflowCanonicalHash.tagged(`t3.${kind}.v1`, value);
}
function claim(value: unknown) {
  const record = exact(value, claimKeys);
  return {
    operationId: text(record.operationId),
    expectedFingerprint: digest(record.expectedFingerprint),
    expectedGeneration: integer(record.expectedGeneration, 0),
    bootId: text(record.bootId),
    workerId: text(record.workerId),
  };
}
function creationContext(value: unknown) {
  const record = exact(value, contextKeys);
  requireData(record.v === 1 && record.driver === "codex");
  const scopeNamespace = text(record.scopeNamespace);
  requireData(/^app-t3code-agents-[0-9a-f]{12}\.slice$/.test(scopeNamespace));
  return {
    v: 1,
    environmentId: text(record.environmentId),
    projectId: text(record.projectId),
    openingThreadId: text(record.openingThreadId),
    driver: "codex",
    providerInstanceId: text(record.providerInstanceId),
    providerSessionId: text(record.providerSessionId),
    scopeNamespace,
    hostBootId: uuid(record.hostBootId),
    expectedClaim: record.expectedClaim === null ? null : claim(record.expectedClaim),
  };
}
function turnContext(value: unknown) {
  const record = exact(value, turnContextKeys);
  requireData(record.driver === "codex");
  return {
    transportId: uuid(record.transportId),
    environmentId: text(record.environmentId),
    projectId: text(record.projectId),
    driver: "codex",
    providerInstanceId: text(record.providerInstanceId),
    providerSessionId: text(record.providerSessionId),
  };
}
function selection(value: unknown) {
  requireData(value !== null && typeof value === "object");
  const hasClaim = Object.hasOwn(value, "expectedClaim");
  const record = exact(value, hasClaim ? [...selectionKeys, "expectedClaim"] : selectionKeys);
  requireData(record.v === 1 && record.kind === "start");
  return {
    v: 1,
    kind: "start",
    threadId: text(record.threadId),
    runId: text(record.runId),
    runOrdinal: integer(record.runOrdinal, 1),
    attemptId: text(record.attemptId),
    rootNodeId: text(record.rootNodeId),
    providerTurnOrdinal: integer(record.providerTurnOrdinal, 1),
    providerInstanceId: text(record.providerInstanceId),
    providerSessionId: text(record.providerSessionId),
    providerThreadId: text(record.providerThreadId),
    nativeThreadId: text(record.nativeThreadId),
    invocation: integer(record.invocation, 1),
    ...(hasClaim ? { expectedClaim: claim(record.expectedClaim) } : {}),
  };
}
function compactJson(value: unknown): unknown {
  requireData(typeof value === "string" && value.isWellFormed());
  const parsed: unknown = JSON.parse(value);
  own(parsed);
  requireData(JSON.stringify(parsed) === value);
  return parsed;
}
function storedJson(value: unknown): unknown {
  const parsed = compactJson(value);
  requireData(stableStringify(parsed) === value);
  return parsed;
}
function requestId(value: unknown): string | number {
  if (typeof value === "string") {
    requireData(value.isWellFormed());
    return value;
  }
  requireData(typeof value === "number" && Number.isSafeInteger(value));
  return value;
}
function unit(value: unknown): string {
  const result = text(value);
  requireData(result.startsWith("t3code-codex-") && result.endsWith(".scope"));
  uuid(result.slice("t3code-codex-".length, -".scope".length));
  return result;
}
function transportBase(value: unknown) {
  const row = exact(value, transportKeys);
  requireData(row.format_version === 1);
  const context = creationContext(storedJson(row.creation_context_json));
  requireData(row.creation_context_hash === tagged("provider-submission-context", context));
  const expected = {
    environment_id: context.environmentId,
    project_id: context.projectId,
    opening_thread_id: context.openingThreadId,
    driver: context.driver,
    provider_instance_id: context.providerInstanceId,
    provider_session_id: context.providerSessionId,
    host_boot_id: context.hostBootId,
    scope_namespace: context.scopeNamespace,
  };
  for (const [key, value] of Object.entries(expected)) requireData(row[key] === value);
  uuid(row.transport_id);
  unit(row.proposed_unit);
  digest(row.creation_input_hash);
  timestamp(row.created_at);
  return { row, context };
}
function bindingData(value: unknown, proposedUnit: unknown, scopeNamespace: string) {
  const record = exact(value, bindingKeys);
  requireData(record.unit === proposedUnit);
  const invocationId = text(record.invocationId);
  requireData(/^[0-9a-f]{32}$/.test(invocationId) && !/^0+$/.test(invocationId));
  const controlGroup = text(record.controlGroup);
  const components = controlGroup.split("/");
  requireData(
    components.shift() === "" &&
      components.length > 1 &&
      components.every((part) => part !== "" && part !== "." && part !== ".."),
  );
  requireData(!/[\u0000-\u001f\u007f]/.test(controlGroup));
  requireData(
    components.at(-1) === proposedUnit && components.slice(0, -1).includes(scopeNamespace),
  );
  const pidStartTicks = text(record.pidStartTicks);
  requireData(/^(0|[1-9]\d*)$/.test(pidStartTicks));
  return {
    unit: unit(record.unit),
    invocationId,
    controlGroup,
    pid: integer(record.pid, 1),
    pidStartTicks,
  };
}
function settlementData(value: unknown, expectedBinding: unknown) {
  const record = exact(value, ["binding", "observation"]);
  exact(expectedBinding, bindingKeys);
  exact(record.binding, bindingKeys);
  requireData(stableStringify(record.binding) === stableStringify(expectedBinding));
  requireData(record.observation === "empty" || record.observation === "removed");
  return { binding: record.binding, observation: record.observation };
}
function evidenceGroup(row: Record<string, unknown>, keys: readonly string[]): boolean {
  const empty = keys.every((key) => row[key] === null);
  requireData(empty || keys.every((key) => row[key] !== null));
  return !empty;
}

export function encodeSubmissionTransportData(input: unknown) {
  return boundary(() => {
    const record = exact(own(input), [
      "context",
      "transportId",
      "proposedUnit",
      "creationInputHash",
      "createdAt",
    ]);
    const context = creationContext(record.context);
    return {
      format_version: 1,
      transport_id: uuid(record.transportId),
      environment_id: context.environmentId,
      project_id: context.projectId,
      opening_thread_id: context.openingThreadId,
      driver: context.driver,
      provider_instance_id: context.providerInstanceId,
      provider_session_id: context.providerSessionId,
      host_boot_id: context.hostBootId,
      scope_namespace: context.scopeNamespace,
      proposed_unit: unit(record.proposedUnit),
      creation_context_json: stableStringify(context),
      creation_context_hash: tagged("provider-submission-context", context),
      // The launch preimage is absent here; this supplied digest carries no normalization proof.
      creation_input_hash: digest(record.creationInputHash),
      created_at: timestamp(record.createdAt),
      binding_json: null,
      binding_hash: null,
      bound_at: null,
      settlement_json: null,
      settlement_hash: null,
      settled_at: null,
    };
  });
}

export function decodeSubmissionTransportData(input: unknown) {
  return boundary(() => {
    const { row, context } = transportBase(own(input));
    let binding;
    if (evidenceGroup(row, ["binding_json", "binding_hash", "bound_at"])) {
      binding = bindingData(
        storedJson(row.binding_json),
        row.proposed_unit,
        context.scopeNamespace,
      );
      requireData(row.binding_hash === tagged("provider-scope-binding", binding));
      timestamp(row.bound_at);
    }
    if (evidenceGroup(row, ["settlement_json", "settlement_hash", "settled_at"])) {
      requireData(binding !== undefined);
      const settlement = settlementData(storedJson(row.settlement_json), binding);
      requireData(row.settlement_hash === tagged("provider-scope-settlement", settlement));
      timestamp(row.settled_at);
    }
    return row;
  });
}

export function encodeScopeBindingData(input: unknown, expectedTransportData: unknown) {
  return boundary(() => {
    const { row, context } = transportBase(own(expectedTransportData));
    const binding = bindingData(own(input), row.proposed_unit, context.scopeNamespace);
    return { json: stableStringify(binding), hash: tagged("provider-scope-binding", binding) };
  });
}

export function encodeScopeSettlementData(input: unknown, expectedBinding: unknown) {
  return boundary(() => {
    const expected = own(expectedBinding);
    const record = exact(expected, bindingKeys);
    const components = text(record.controlGroup).split("/");
    const namespaces = components.filter((part) =>
      /^app-t3code-agents-[0-9a-f]{12}\.slice$/.test(part),
    );
    requireData(namespaces.length >= 1);
    const binding = bindingData(expected, unit(record.unit), namespaces[0]!);
    const settlement = settlementData(own(input), binding);
    return {
      json: stableStringify(settlement),
      hash: tagged("provider-scope-settlement", settlement),
    };
  });
}

function snapshot(contextInput: unknown, frameInput: unknown, createdAt: unknown) {
  const context = turnContext(contextInput);
  const frame = exact(frameInput, ["encoded", "byteLength", "method", "requestId", "selection"]);
  const encoded = text(frame.encoded);
  const byteLength = new TextEncoder().encode(encoded).byteLength;
  requireData(byteLength === frame.byteLength && byteLength <= 1_048_576 && encoded.endsWith("\n"));
  const request = exact(compactJson(encoded.slice(0, -1)), ["id", "method", "params"]);
  requireData(request.method === "turn/start" && frame.method === request.method);
  const id = requestId(request.id);
  requireData(id === frame.requestId && typeof id === typeof frame.requestId);
  const selected = selection(compactJson(frame.selection));
  requireData(
    selected.providerInstanceId === context.providerInstanceId &&
      selected.providerSessionId === context.providerSessionId,
  );
  const params = decodeParams(request.params, { onExcessProperty: "error" });
  const reencoded = encodeParams(params, { onExcessProperty: "error" });
  const canonicalParams = stableStringify(request.params);
  own(params);
  own(reencoded);
  requireData(
    stableStringify(params) === canonicalParams && stableStringify(reencoded) === canonicalParams,
  );
  requireData(params.threadId === selected.nativeThreadId);
  return {
    format_version: 1,
    attempt_id: selected.attemptId,
    transport_id: context.transportId,
    environment_id: context.environmentId,
    project_id: context.projectId,
    thread_id: selected.threadId,
    run_id: selected.runId,
    run_ordinal: selected.runOrdinal,
    root_node_id: selected.rootNodeId,
    provider_turn_ordinal: selected.providerTurnOrdinal,
    driver: context.driver,
    provider_instance_id: context.providerInstanceId,
    provider_session_id: context.providerSessionId,
    provider_thread_id: selected.providerThreadId,
    native_thread_id: selected.nativeThreadId,
    method: "turn/start",
    selection_json: stableStringify(selected),
    selection_hash: tagged("provider-submission-selection", selected),
    input_hash: tagged("provider-submission-input", params),
    frame_hash: NodeCrypto.createHash("sha256").update(encoded, "utf8").digest("hex"),
    request_id_json: JSON.stringify(id),
    created_at: timestamp(createdAt),
    local_outcome: null,
    local_reason: null,
    local_observed_at: null,
  };
}

export function snapshotSubmissionFrame(
  perTurnContext: unknown,
  frame: unknown,
  createdAt: unknown,
) {
  return boundary(() => snapshot(own(perTurnContext), own(frame), own(createdAt)));
}

export function assertStoredAttemptMatchesFrame(
  input: unknown,
  perTurnContext: unknown,
  actualFrame: unknown,
): void {
  boundary(() => {
    const row = exact(own(input), attemptKeys);
    requireData(row.format_version === 1);
    const selected = selection(storedJson(row.selection_json));
    requireData(row.selection_hash === tagged("provider-submission-selection", selected));
    digest(row.input_hash);
    digest(row.frame_hash);
    requestId(storedJson(row.request_id_json));
    if (evidenceGroup(row, ["local_outcome", "local_reason", "local_observed_at"])) {
      requireData(
        row.local_outcome === "submitted" ||
          row.local_outcome === "not_submitted" ||
          row.local_outcome === "unknown",
      );
      requireData(typeof row.local_reason === "string" && reasons.has(row.local_reason));
      requireData((row.local_outcome === "submitted") === (row.local_reason === "completed"));
      timestamp(row.local_observed_at);
    }
    // Hash preimages are not stored in 088; full comparison always requires the actual frame.
    const expected = snapshot(own(perTurnContext), own(actualFrame), row.created_at);
    for (const [key, value] of Object.entries(expected)) {
      if (key !== "local_outcome" && key !== "local_reason" && key !== "local_observed_at")
        requireData(row[key] === value);
    }
  }, "conflict");
}
