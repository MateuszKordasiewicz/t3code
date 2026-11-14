import { isWorkflowTimestamp } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as PreparationJob from "./PreparationJob.ts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

const invalid = () => new Error("Invalid preparation process resource");
const signals = new Set([
  "SIGHUP",
  "SIGINT",
  "SIGQUIT",
  "SIGILL",
  "SIGTRAP",
  "SIGABRT",
  "SIGIOT",
  "SIGBUS",
  "SIGFPE",
  "SIGKILL",
  "SIGUSR1",
  "SIGSEGV",
  "SIGUSR2",
  "SIGPIPE",
  "SIGALRM",
  "SIGTERM",
  "SIGSTKFLT",
  "SIGCHLD",
  "SIGCONT",
  "SIGSTOP",
  "SIGTSTP",
  "SIGTTIN",
  "SIGTTOU",
  "SIGURG",
  "SIGXCPU",
  "SIGXFSZ",
  "SIGVTALRM",
  "SIGPROF",
  "SIGWINCH",
  "SIGIO",
  "SIGPOLL",
  "SIGPWR",
  "SIGSYS",
]);
function preflight(value: unknown) {
  WorkflowCanonicalHash.assertCanonical(value);
  const visit = (item: unknown): void => {
    if (typeof item === "string" && !item.isWellFormed()) throw invalid();
    if (typeof item === "number" && Object.is(item, -0)) throw invalid();
    if (typeof item === "object" && item !== null)
      for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(item))) {
        if (!key.isWellFormed() || !("value" in descriptor)) throw invalid();
        visit(descriptor.value);
      }
  };
  visit(value);
}
function object(value: unknown, keys: string) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw invalid();
  const entries = Reflect.ownKeys(value).map((key) => {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (typeof key !== "string" || !descriptor?.enumerable || !("value" in descriptor))
      throw invalid();
    return [key, descriptor.value] as const;
  });
  const result: Record<string, unknown> = Object.fromEntries(entries);
  if (Object.keys(result).sort().join() !== keys) throw invalid();
  return result;
}
function id(value: unknown) {
  if (typeof value !== "string" || !value.trim() || !value.isWellFormed() || value.includes("\0"))
    throw invalid();
  return value;
}
function uuid(value: unknown) {
  const text = id(value);
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(text) ||
    text === "00000000-0000-0000-0000-000000000000"
  )
    throw invalid();
  return text;
}
function digest(value: unknown) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) throw invalid();
  return value;
}
function integer(value: unknown, minimum = 0) {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    Object.is(value, -0) ||
    value < minimum
  )
    throw invalid();
  return value;
}
function timestamp(value: unknown) {
  if (typeof value !== "string" || !isWorkflowTimestamp(value)) throw invalid();
  const parsed = DateTime.make(value);
  if (Option.isNone(parsed) || DateTime.formatIso(parsed.value) !== value) throw invalid();
  return value;
}
const nullableId = (value: unknown) => (value === null ? null : id(value));
const nullableTime = (value: unknown) => (value === null ? null : timestamp(value));
function parse(value: unknown) {
  if (typeof value !== "string") throw invalid();
  const decoded: unknown = JSON.parse(value);
  preflight(decoded);
  if (stableStringify(decoded) !== value) throw invalid();
  return decoded;
}
function encoded(tag: string, value: object) {
  return Object.freeze({
    text: stableStringify(value),
    hash: WorkflowCanonicalHash.tagged(tag, value),
  });
}
export function ownContext(value: unknown) {
  preflight(value);
  const raw = object(
    value,
    "creationInputHash,current,environmentId,executionClaim,host,jobSelection,profile,projectId,proposedUnit,runId,threadId,v",
  );
  if (raw.v !== 1 || raw.profile !== "linux-node-no-stdin-v1") throw invalid();
  const jobSelection = PreparationJob.ownSelection(raw.jobSelection);
  if (
    [jobSelection.jobId, jobSelection.entryKey, jobSelection.serverBootId].some((value) =>
      value.includes("\0"),
    )
  )
    throw invalid();
  const executionClaim =
    raw.executionClaim === null ? null : PreparationJob.ownNativeClaim(raw.executionClaim);
  let current = null;
  if (executionClaim !== null) {
    if (
      executionClaim.bootId !== jobSelection.serverBootId ||
      Object.values(executionClaim).some(
        (value) => typeof value === "string" && value.includes("\0"),
      )
    )
      throw invalid();
    const selected = object(
      raw.current,
      "driver,nativeThreadId,providerInstanceId,providerSessionId,providerThreadId",
    );
    current = Object.freeze({
      driver: id(selected.driver),
      providerInstanceId: id(selected.providerInstanceId),
      providerThreadId: id(selected.providerThreadId),
      providerSessionId: nullableId(selected.providerSessionId),
      nativeThreadId: nullableId(selected.nativeThreadId),
    });
  } else if (raw.current !== null) throw invalid();
  const host = object(raw.host, "hostBootId,scopeNamespace");
  const hostBootId = uuid(host.hostBootId);
  const scopeNamespace = id(host.scopeNamespace);
  if (!/^app-t3code-agents-[0-9a-f]{12}\.slice$/.test(scopeNamespace)) throw invalid();
  const proposedUnit = id(raw.proposedUnit);
  const match = /^t3code-preparation-(.+)\.scope$/.exec(proposedUnit);
  if (!match) throw invalid();
  uuid(match[1]);
  const runId = nullableId(raw.runId);
  if (executionClaim !== null && runId === null) throw invalid();
  return Object.freeze({
    v: 1 as const,
    profile: "linux-node-no-stdin-v1" as const,
    jobSelection,
    environmentId: id(raw.environmentId),
    projectId: id(raw.projectId),
    threadId: id(raw.threadId),
    runId,
    executionClaim,
    current,
    host: Object.freeze({ hostBootId, scopeNamespace }),
    proposedUnit,
    creationInputHash: digest(raw.creationInputHash),
  });
}
export function encodeContext(value: unknown) {
  return encoded("t3.preparation-process-context.v1", ownContext(value));
}
export function ownBinding(value: unknown, context: ReturnType<typeof ownContext>) {
  preflight(value);
  const input = ownContext(context);
  const raw = object(value, "controlGroup,invocationId,pid,pidStartTicks,unit");
  const unit = id(raw.unit);
  const invocationId = id(raw.invocationId);
  const controlGroup = id(raw.controlGroup);
  const pidStartTicks = id(raw.pidStartTicks);
  const components = controlGroup.split("/");
  if (
    unit !== input.proposedUnit ||
    !/^[0-9a-f]{32}$/.test(invocationId) ||
    invocationId === "0".repeat(32) ||
    !controlGroup.startsWith("/") ||
    components
      .slice(1)
      .some((part) => !part || part === "." || part === ".." || /[\x00-\x1f\x7f]/.test(part)) ||
    components.at(-1) !== unit ||
    components.at(-2) !== input.host.scopeNamespace ||
    !/^[1-9][0-9]*$/.test(pidStartTicks)
  )
    throw invalid();
  return Object.freeze({
    unit,
    invocationId,
    controlGroup,
    pid: integer(raw.pid, 1),
    pidStartTicks,
  });
}
export function encodeBinding(
  resourceId: string,
  context: ReturnType<typeof ownContext>,
  value: unknown,
) {
  const input = ownContext(context);
  const binding = ownBinding(value, input);
  return Object.freeze({
    text: stableStringify(binding),
    hash: WorkflowCanonicalHash.tagged("t3.preparation-process-binding.v1", {
      resourceId: uuid(resourceId),
      contextHash: encodeContext(input).hash,
      creationInputHash: input.creationInputHash,
      binding,
    }),
  });
}
export function ownSettlement(
  value: unknown,
  context: ReturnType<typeof ownContext>,
  resourceId: string,
  originalBinding: ReturnType<typeof ownBinding>,
  priorVersion: number,
) {
  preflight(value);
  const input = ownContext(context);
  const binding = ownBinding(originalBinding, input);
  const raw = object(value, "childExit,contextHash,priorVersion,resourceId,scope,v");
  const child = object(raw.childExit, "exitCode,pid,signal");
  const scope = object(raw.scope, "binding,observation");
  const scopeBinding = ownBinding(scope.binding, input);
  const exitCode = child.exitCode === null ? null : integer(child.exitCode);
  const signal = child.signal === null ? null : id(child.signal);
  if (
    raw.v !== 1 ||
    uuid(raw.resourceId) !== uuid(resourceId) ||
    digest(raw.contextHash) !== encodeContext(input).hash ||
    integer(raw.priorVersion) !== integer(priorVersion) ||
    integer(child.pid, 1) !== binding.pid ||
    (exitCode === null) === (signal === null) ||
    (signal !== null && !signals.has(signal)) ||
    stableStringify(scopeBinding) !== stableStringify(binding) ||
    (scope.observation !== "empty" && scope.observation !== "removed")
  )
    throw invalid();
  return Object.freeze({
    v: 1 as const,
    resourceId,
    contextHash: encodeContext(input).hash,
    priorVersion,
    childExit: Object.freeze({ pid: binding.pid, exitCode, signal }),
    scope: Object.freeze({ binding: scopeBinding, observation: scope.observation }),
  });
}
export function encodeSettlement(
  resourceId: string,
  context: ReturnType<typeof ownContext>,
  binding: ReturnType<typeof ownBinding>,
  priorVersion: number,
  value: unknown,
) {
  const input = ownContext(context);
  const settlement = ownSettlement(value, input, resourceId, binding, priorVersion);
  return Object.freeze({
    text: stableStringify(settlement),
    hash: WorkflowCanonicalHash.tagged("t3.preparation-process-settlement.v1", {
      resourceId: uuid(resourceId),
      contextHash: encodeContext(input).hash,
      bindingHash: encodeBinding(resourceId, input, binding).hash,
      settlement,
    }),
  });
}

export function decodeRow(value: unknown) {
  preflight(value);
  const raw = object(
    value,
    "binding_hash,binding_json,bound_at,child_pid,created_at,creation_context_hash,creation_context_json,creation_input_hash,creation_started_at,environment_id,format_version,host_boot_id,job_id,job_request_identity_hash,project_id,proposed_unit,record_version,release_started_at,resource_id,run_id,scope_namespace,server_boot_id,settled_at,settlement_hash,settlement_json,state,thread_id",
  );
  const context = ownContext(parse(raw.creation_context_json));
  const contextHash = encodeContext(context).hash;
  const resourceId = uuid(raw.resource_id);
  const shadows = {
    format_version: 1,
    job_id: context.jobSelection.jobId,
    job_request_identity_hash: context.jobSelection.requestIdentityHash,
    environment_id: context.environmentId,
    project_id: context.projectId,
    thread_id: context.threadId,
    run_id: context.runId,
    server_boot_id: context.jobSelection.serverBootId,
    host_boot_id: context.host.hostBootId,
    scope_namespace: context.host.scopeNamespace,
    proposed_unit: context.proposedUnit,
    creation_input_hash: context.creationInputHash,
    creation_context_hash: contextHash,
  };
  if (Object.entries(shadows).some(([key, expected]) => raw[key] !== expected)) throw invalid();
  const createdAt = timestamp(raw.created_at);
  const recordVersion = integer(raw.record_version);
  const state = raw.state;
  if (state !== "active" && state !== "quiescing" && state !== "unknown" && state !== "settled")
    throw invalid();
  const creationStartedAt = nullableTime(raw.creation_started_at);
  const childPid = raw.child_pid === null ? null : integer(raw.child_pid, 1);
  const boundAt = nullableTime(raw.bound_at);
  const releaseStartedAt = nullableTime(raw.release_started_at);
  if (childPid !== null && creationStartedAt === null) throw invalid();
  let binding = null;
  let bindingHash = null;
  if (raw.binding_json !== null || raw.binding_hash !== null || boundAt !== null) {
    if (
      raw.binding_json === null ||
      raw.binding_hash === null ||
      boundAt === null ||
      creationStartedAt === null ||
      childPid === null
    )
      throw invalid();
    binding = ownBinding(parse(raw.binding_json), context);
    bindingHash = encodeBinding(resourceId, context, binding).hash;
    if (binding.pid !== childPid || digest(raw.binding_hash) !== bindingHash) throw invalid();
  }
  if (releaseStartedAt !== null && (binding === null || recordVersion < 2)) throw invalid();
  const settledAt = nullableTime(raw.settled_at);
  let settlement = null;
  let settlementHash = null;
  if (state === "settled") {
    if (
      !binding ||
      raw.settlement_json === null ||
      raw.settlement_hash === null ||
      settledAt === null ||
      recordVersion < (releaseStartedAt === null ? 2 : 3)
    )
      throw invalid();
    settlement = ownSettlement(
      parse(raw.settlement_json),
      context,
      resourceId,
      binding,
      recordVersion - 1,
    );
    settlementHash = encodeSettlement(
      resourceId,
      context,
      binding,
      recordVersion - 1,
      settlement,
    ).hash;
    if (digest(raw.settlement_hash) !== settlementHash) throw invalid();
  } else if (raw.settlement_json !== null || raw.settlement_hash !== null || settledAt !== null)
    throw invalid();
  if (
    recordVersion === 0 &&
    (state !== "active" ||
      creationStartedAt !== null ||
      childPid !== null ||
      binding !== null ||
      releaseStartedAt !== null ||
      settlement !== null)
  )
    throw invalid();
  return Object.freeze({
    resourceId,
    context,
    contextHash,
    createdAt,
    recordVersion,
    state,
    creationStartedAt,
    childPid,
    binding,
    bindingHash,
    boundAt,
    releaseStartedAt,
    settlement,
    settlementHash,
    settledAt,
  });
}
export function selection(resource: ReturnType<typeof decodeRow>) {
  return Object.freeze({
    resourceId: resource.resourceId,
    contextHash: resource.contextHash,
    expectedVersion: resource.recordVersion,
  });
}
export type Context = ReturnType<typeof ownContext>;
export type Binding = ReturnType<typeof ownBinding>;
export type Settlement = ReturnType<typeof ownSettlement>;
export type Resource = ReturnType<typeof decodeRow>;
export type Selection = ReturnType<typeof selection>;
