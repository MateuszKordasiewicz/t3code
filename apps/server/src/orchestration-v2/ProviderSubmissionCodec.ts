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

function attemptResourceData(input: unknown) {
  const row = exact(input, attemptKeys);
  requireData(row.format_version === 1 && row.method === "turn/start");
  const context = turnContext({
    transportId: row.transport_id,
    environmentId: row.environment_id,
    projectId: row.project_id,
    driver: row.driver,
    providerInstanceId: row.provider_instance_id,
    providerSessionId: row.provider_session_id,
  });
  const selected = selection(storedJson(row.selection_json));
  const shadows = {
    attempt_id: selected.attemptId,
    thread_id: selected.threadId,
    run_id: selected.runId,
    run_ordinal: selected.runOrdinal,
    root_node_id: selected.rootNodeId,
    provider_turn_ordinal: selected.providerTurnOrdinal,
    provider_instance_id: selected.providerInstanceId,
    provider_session_id: selected.providerSessionId,
    provider_thread_id: selected.providerThreadId,
    native_thread_id: selected.nativeThreadId,
  };
  for (const [key, value] of Object.entries(shadows)) requireData(row[key] === value);
  requireData(
    selected.providerInstanceId === context.providerInstanceId &&
      selected.providerSessionId === context.providerSessionId,
  );
  requireData(row.selection_hash === tagged("provider-submission-selection", selected));
  digest(row.input_hash);
  digest(row.frame_hash);
  requestId(storedJson(row.request_id_json));
  timestamp(row.created_at);
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
  return row;
}

// This validates stored data only; input/frame digest preimages are absent from resource rows.
export function decodeSubmissionAttemptResourceData(input: unknown) {
  return boundary(() => attemptResourceData(own(input)));
}

export function assertStoredAttemptMatchesFrame(
  input: unknown,
  perTurnContext: unknown,
  actualFrame: unknown,
): void {
  boundary(() => {
    const row = attemptResourceData(own(input));
    const expected = snapshot(own(perTurnContext), own(actualFrame), row.created_at);
    for (const [key, value] of Object.entries(expected)) {
      if (key !== "local_outcome" && key !== "local_reason" && key !== "local_observed_at")
        requireData(row[key] === value);
    }
  }, "conflict");
}

const decodeConversationParams = Schema.decodeUnknownSync(CodexSchema.V2ThreadStartParams);
const conversationSelectionKeys =
  "v kind transportId environmentId projectId threadId runId runOrdinal attemptId rootNodeId intentId driver providerInstanceId providerSessionId providerThreadId invocation expectedClaim".split(
    " ",
  );
const conversationKeys =
  "format_version transport_id environment_id project_id thread_id run_id run_ordinal attempt_id root_node_id intent_id driver provider_instance_id provider_session_id provider_thread_id method selection_json selection_hash input_json input_hash request_id_json created_at local_outcome local_reason local_observed_at response_json response_hash response_observed_at".split(
    " ",
  );
const conversationObservationKeys = [
  "local_outcome",
  "local_reason",
  "local_observed_at",
  "response_json",
  "response_hash",
  "response_observed_at",
];
function conversationSelection(input: unknown) {
  const value = exact(input, conversationSelectionKeys);
  requireData(value.v === 1 && value.kind === "fresh-conversation" && value.driver === "codex");
  return {
    v: 1,
    kind: "fresh-conversation",
    transportId: uuid(value.transportId),
    environmentId: text(value.environmentId),
    projectId: text(value.projectId),
    threadId: text(value.threadId),
    runId: text(value.runId),
    runOrdinal: integer(value.runOrdinal, 1),
    attemptId: text(value.attemptId),
    rootNodeId: text(value.rootNodeId),
    intentId: text(value.intentId),
    driver: "codex",
    providerInstanceId: text(value.providerInstanceId),
    providerSessionId: text(value.providerSessionId),
    providerThreadId: text(value.providerThreadId),
    invocation: integer(value.invocation, 1),
    expectedClaim: claim(value.expectedClaim),
  };
}
function conversationMcp(input: unknown) {
  requireData(input !== null && typeof input === "object");
  if (Reflect.get(input, "mode") === "disabled") {
    exact(input, ["mode"]);
    return { mode: "disabled" as const };
  }
  const value = exact(input, [
    "mode",
    "credentialId",
    "capabilities",
    "browserToolsAvailable",
    "subagent",
    "linked",
  ]);
  requireData(
    value.mode === "issued" &&
      typeof value.browserToolsAvailable === "boolean" &&
      typeof value.subagent === "boolean",
  );
  requireData(
    value.linked === "absent" ||
      value.linked === "unavailable" ||
      typeof value.linked === "boolean",
  );
  requireData(Array.isArray(value.capabilities));
  const capabilities = value.capabilities.map(text).sort();
  requireData(
    new Set(capabilities).size === capabilities.length &&
      capabilities.every((item) =>
        ["orchestration", "worktree", "pull-requests", "preview", "device"].includes(item),
      ),
  );
  requireData(
    ["orchestration", "worktree", "pull-requests"].every((item) => capabilities.includes(item)) &&
      capabilities.includes("preview") === value.browserToolsAvailable,
  );
  return {
    mode: "issued" as const,
    credentialId: text(value.credentialId),
    capabilities,
    browserToolsAvailable: value.browserToolsAvailable,
    subagent: value.subagent,
    linked: value.linked,
  };
}
function conversationInput(input: unknown, selected: ReturnType<typeof conversationSelection>) {
  const value = exact(input, ["v", "model", "cwd", "options", "mcp"]);
  requireData(value.v === 1);
  const options = exact(value.options, [
    "updatePlan",
    "mcpStartupGraceMs",
    "concurrentReasoningSummaries",
    "shellThreadId",
  ]);
  requireData(
    options.updatePlan === true &&
      options.mcpStartupGraceMs === 10000 &&
      options.concurrentReasoningSummaries === true &&
      options.shellThreadId === selected.threadId,
  );
  return {
    v: 1,
    model: value.model === null ? null : text(value.model),
    cwd: value.cwd === null ? null : text(value.cwd),
    options: {
      updatePlan: true,
      mcpStartupGraceMs: 10000,
      concurrentReasoningSummaries: true,
      shellThreadId: selected.threadId,
    },
    mcp: conversationMcp(value.mcp),
  };
}
function conversationParams(
  input: unknown,
  selected: ReturnType<typeof conversationSelection>,
  mcpInput: unknown,
) {
  const decoded = decodeConversationParams(input, { onExcessProperty: "error" });
  requireData(stableStringify(decoded) === stableStringify(input));
  const params = exact(input, [
    "config",
    ...(Object.hasOwn(decoded, "model") ? ["model"] : []),
    ...(Object.hasOwn(decoded, "cwd") ? ["cwd"] : []),
  ]);
  const mcp = conversationMcp(mcpInput);
  const config = exact(params.config, [
    "tools.update_plan.enabled",
    "mcp_optional_startup_grace_ms",
    "features.concurrent_reasoning_summaries",
    "shell_environment_policy.set.T3CODE_THREAD_ID",
    ...(mcp.mode === "issued" ? ["mcp_servers.t3-code", "mcp_servers.t3-code-direct"] : []),
  ]);
  requireData(
    config["tools.update_plan.enabled"] === true &&
      config.mcp_optional_startup_grace_ms === 10000 &&
      config["features.concurrent_reasoning_summaries"] === true &&
      config["shell_environment_policy.set.T3CODE_THREAD_ID"] === selected.threadId,
  );
  let token: string | undefined;
  if (mcp.mode === "issued") {
    const server = exact(config["mcp_servers.t3-code"], [
      "url",
      "tool_timeout_sec",
      "http_headers",
      "disabled_tools",
    ]);
    const direct = exact(config["mcp_servers.t3-code-direct"], [
      "url",
      "tool_timeout_sec",
      "http_headers",
      "enabled_tools",
      "omit_tools_from",
    ]);
    const header = text(exact(server.http_headers, ["Authorization"]).Authorization);
    requireData(/^Bearer [A-Za-z0-9_-]{32,}$/.test(header));
    requireData(
      text(server.url) === direct.url &&
        server.tool_timeout_sec === 3900 &&
        direct.tool_timeout_sec === 3900 &&
        exact(direct.http_headers, ["Authorization"]).Authorization === header,
    );
    requireData(
      stableStringify(server.disabled_tools) === '["t3_thread_wait"]' &&
        stableStringify(direct.enabled_tools) === '["t3_thread_wait"]' &&
        stableStringify(direct.omit_tools_from) === '["code_mode","deferred"]',
    );
    token = header.slice("Bearer ".length);
  }
  const result = conversationInput(
    {
      v: 1,
      model: Object.hasOwn(params, "model") ? text(params.model) : null,
      cwd: Object.hasOwn(params, "cwd") ? text(params.cwd) : null,
      options: {
        updatePlan: true,
        mcpStartupGraceMs: 10000,
        concurrentReasoningSummaries: true,
        shellThreadId: selected.threadId,
      },
      mcp,
    },
    selected,
  );
  if (token) requireData(!stableStringify({ selected, result }).includes(token));
  return result;
}
function conversationSnapshot(
  contextInput: unknown,
  frameInput: unknown,
  expectedParams: unknown,
  mcp: unknown,
  at: unknown,
) {
  const context = exact(contextInput, [...turnContextKeys, "expectedSelection"]);
  const selected = conversationSelection(context.expectedSelection);
  const original = turnContext(
    Object.fromEntries(turnContextKeys.map((key) => [key, context[key]])),
  );
  for (const [key, value] of Object.entries(original))
    requireData(Reflect.get(selected, key) === value);
  const frame = exact(frameInput, ["encoded", "byteLength", "method", "requestId", "selection"]);
  const encoded = text(frame.encoded);
  const bytes = new TextEncoder().encode(encoded).byteLength;
  requireData(bytes === frame.byteLength && bytes <= 1048576 && encoded.endsWith("\n"));
  const request = exact(compactJson(encoded.slice(0, -1)), ["id", "method", "params"]);
  requireData(frame.method === "thread/start" && request.method === "thread/start");
  const id = integer(request.id, 1);
  requireData(frame.requestId === id && frame.selection === stableStringify(selected));
  requireData(stableStringify(request.params) === stableStringify(expectedParams));
  const input = conversationParams(request.params, selected, mcp);
  return {
    format_version: 1,
    transport_id: selected.transportId,
    environment_id: selected.environmentId,
    project_id: selected.projectId,
    thread_id: selected.threadId,
    run_id: selected.runId,
    run_ordinal: selected.runOrdinal,
    attempt_id: selected.attemptId,
    root_node_id: selected.rootNodeId,
    intent_id: selected.intentId,
    driver: selected.driver,
    provider_instance_id: selected.providerInstanceId,
    provider_session_id: selected.providerSessionId,
    provider_thread_id: selected.providerThreadId,
    method: "thread/start",
    selection_json: stableStringify(selected),
    selection_hash: tagged("provider-conversation-selection", selected),
    input_json: stableStringify(input),
    input_hash: tagged("provider-conversation-input", input),
    request_id_json: JSON.stringify(id),
    created_at: timestamp(at),
    local_outcome: null,
    local_reason: null,
    local_observed_at: null,
    response_json: null,
    response_hash: null,
    response_observed_at: null,
  };
}
export function snapshotConversationFrame(
  context: unknown,
  frame: unknown,
  expectedParams: unknown,
  mcp: unknown,
  at: unknown,
) {
  return boundary(() =>
    conversationSnapshot(own(context), own(frame), own(expectedParams), own(mcp), own(at)),
  );
}
function conversationResponse(input: unknown, selected: ReturnType<typeof conversationSelection>) {
  const response = exact(input, [
    "v",
    "nativeThreadId",
    "returnedProviderThreadId",
    "driver",
    "providerInstanceId",
    "providerSessionId",
    "threadId",
  ]);
  requireData(new TextEncoder().encode(stableStringify(response)).byteLength <= 16384);
  requireData(
    response.v === 1 &&
      response.driver === "codex" &&
      response.providerInstanceId === selected.providerInstanceId &&
      response.providerSessionId === selected.providerSessionId &&
      response.threadId === selected.threadId,
  );
  return {
    v: 1,
    nativeThreadId: text(response.nativeThreadId),
    returnedProviderThreadId: text(response.returnedProviderThreadId),
    driver: "codex",
    providerInstanceId: selected.providerInstanceId,
    providerSessionId: selected.providerSessionId,
    threadId: selected.threadId,
  };
}
function conversationReceipt(input: unknown) {
  const row = exact(input, conversationKeys);
  requireData(row.format_version === 1 && row.method === "thread/start");
  const selected = conversationSelection(storedJson(row.selection_json));
  const shadows = {
    transport_id: selected.transportId,
    environment_id: selected.environmentId,
    project_id: selected.projectId,
    thread_id: selected.threadId,
    run_id: selected.runId,
    run_ordinal: selected.runOrdinal,
    attempt_id: selected.attemptId,
    root_node_id: selected.rootNodeId,
    intent_id: selected.intentId,
    driver: selected.driver,
    provider_instance_id: selected.providerInstanceId,
    provider_session_id: selected.providerSessionId,
    provider_thread_id: selected.providerThreadId,
  };
  for (const [key, value] of Object.entries(shadows)) requireData(row[key] === value);
  requireData(row.selection_hash === tagged("provider-conversation-selection", selected));
  const inputData = conversationInput(storedJson(row.input_json), selected);
  requireData(stableStringify(inputData) === row.input_json);
  requireData(row.input_hash === tagged("provider-conversation-input", inputData));
  integer(storedJson(row.request_id_json), 1);
  timestamp(row.created_at);
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
  if (evidenceGroup(row, ["response_json", "response_hash", "response_observed_at"])) {
    requireData(row.local_outcome === "submitted");
    const response = conversationResponse(storedJson(row.response_json), selected);
    requireData(row.response_hash === tagged("provider-conversation-response", response));
    timestamp(row.response_observed_at);
  }
  return row;
}
export function decodeConversationReceiptData(input: unknown) {
  return boundary(() => conversationReceipt(own(input)));
}
export function assertStoredConversationMatchesFrame(
  input: unknown,
  context: unknown,
  frame: unknown,
  expectedParams: unknown,
  mcp: unknown,
): void {
  boundary(() => {
    const row = conversationReceipt(own(input));
    const expected = conversationSnapshot(
      own(context),
      own(frame),
      own(expectedParams),
      own(mcp),
      row.created_at,
    );
    for (const [key, value] of Object.entries(expected)) {
      if (!conversationObservationKeys.includes(key)) requireData(row[key] === value);
    }
  }, "conflict");
}
export function encodeConversationOutcomeData(input: unknown, outcome: unknown, at: unknown) {
  return boundary(() => {
    const row = conversationReceipt(own(input));
    const value = exact(own(outcome), ["kind", "reason"]);
    const update = {
      local_outcome: value.kind,
      local_reason: value.reason,
      local_observed_at: timestamp(at),
    };
    for (const [key, value] of Object.entries(update))
      requireData(row[key] === null || row[key] === value);
    return conversationReceipt({ ...row, ...update });
  }, "conflict");
}
export function encodeConversationResponseData(
  input: unknown,
  response: unknown,
  at: unknown,
  credential: unknown,
) {
  return boundary(() => {
    const row = conversationReceipt(own(input));
    requireData(row.local_outcome === "submitted");
    const value = conversationResponse(
      own(response),
      conversationSelection(storedJson(row.selection_json)),
    );
    const selected = conversationSelection(storedJson(row.selection_json));
    const mcp = conversationInput(storedJson(row.input_json), selected).mcp;
    const descriptor = exact(
      own(credential),
      mcp.mode === "disabled" ? ["mode"] : ["mode", "credentialId", "authorizationHeader"],
    );
    requireData(descriptor.mode === mcp.mode);
    if (mcp.mode === "issued") {
      requireData(descriptor.credentialId === mcp.credentialId);
      const header = text(descriptor.authorizationHeader);
      requireData(/^Bearer [A-Za-z0-9_-]{32,}$/.test(header));
      requireData(!stableStringify(value).includes(header.slice("Bearer ".length)));
    }
    const update = {
      response_json: stableStringify(value),
      response_hash: tagged("provider-conversation-response", value),
      response_observed_at: timestamp(at),
    };
    for (const [key, value] of Object.entries(update))
      requireData(row[key] === null || row[key] === value);
    return conversationReceipt({ ...row, ...update });
  }, "conflict");
}
