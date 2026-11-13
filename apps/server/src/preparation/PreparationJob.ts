import { isWorkflowTimestamp } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

const invalid = () => new Error("Invalid preparation job");
const inputKeys =
  "entryKey,environmentId,jobId,kind,originatingLaunchJobId,parentSelection,projectId,request,runId,serverBootId,threadId";
const nativeInputKeys = inputKeys.replace("kind,", "kind,nativeSelection,");
const selectionKeys = "entryKey,expectedVersion,jobId,requestIdentityHash,serverBootId";
function json(value: unknown): void {
  WorkflowCanonicalHash.assertCanonical(value);
  const visit = (item: unknown): void => {
    if (typeof item === "string" && !item.isWellFormed()) throw invalid();
    if (typeof item === "number" && Object.is(item, -0)) throw invalid();
    if (typeof item === "object" && item !== null)
      for (const [key, child] of Object.entries(item)) {
        if (!key.isWellFormed()) throw invalid();
        visit(child);
      }
  };
  visit(value);
}
function object(value: unknown, keys?: string) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw invalid();
  if (keys !== undefined && Object.keys(value).sort().join() !== keys) throw invalid();
  return value as Record<string, unknown>;
}
function id(value: unknown) {
  if (typeof value !== "string" || !value.trim() || !value.isWellFormed()) throw invalid();
  return value;
}
function ownNativeSelection(value: unknown) {
  const raw = object(value, "driver,nativeId,providerInstanceId");
  const selected = {
    driver: id(raw.driver),
    providerInstanceId: id(raw.providerInstanceId),
    nativeId: id(raw.nativeId),
  };
  if (Object.values(selected).some((value) => value !== value.trim())) throw invalid();
  return Object.freeze(selected);
}
function digest(value: unknown) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) throw invalid();
  return value;
}
function version(value: unknown) {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw invalid();
  return value;
}
function timestamp(value: unknown) {
  if (typeof value !== "string" || !isWorkflowTimestamp(value)) throw invalid();
  const parsed = DateTime.make(value);
  if (Option.isNone(parsed) || DateTime.formatIso(parsed.value) !== value) throw invalid();
  return value;
}
function freeze<A>(value: A): Readonly<A> {
  if (typeof value === "object" && value !== null) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}
export function ownSelection(value: unknown) {
  json(value);
  const raw = object(value, selectionKeys);
  return Object.freeze({
    jobId: id(raw.jobId),
    entryKey: id(raw.entryKey),
    requestIdentityHash: digest(raw.requestIdentityHash),
    serverBootId: id(raw.serverBootId),
    expectedVersion: version(raw.expectedVersion),
  });
}
export function ownInput(value: unknown) {
  json(value);
  const raw = object(value);
  const selected = Object.hasOwn(raw, "nativeSelection")
    ? {
        nativeSelection:
          raw.nativeSelection === null ? null : ownNativeSelection(raw.nativeSelection),
      }
    : {};
  object(raw, Object.hasOwn(raw, "nativeSelection") ? nativeInputKeys : inputKeys);
  const kind = raw.kind;
  if (kind !== "launch" && kind !== "setup" && kind !== "title" && kind !== "release")
    throw invalid();
  const parentSelection = raw.parentSelection === null ? null : ownSelection(raw.parentSelection);
  const originatingLaunchJobId =
    raw.originatingLaunchJobId === null ? null : id(raw.originatingLaunchJobId);
  if (
    (parentSelection === null) !== (originatingLaunchJobId === null) ||
    (parentSelection !== null &&
      (kind !== "setup" || parentSelection.jobId !== originatingLaunchJobId))
  )
    throw invalid();
  return freeze({
    jobId: id(raw.jobId),
    entryKey: id(raw.entryKey),
    kind,
    environmentId: id(raw.environmentId),
    projectId: id(raw.projectId),
    threadId: id(raw.threadId),
    runId: raw.runId === null ? null : id(raw.runId),
    originatingLaunchJobId,
    parentSelection,
    serverBootId: id(raw.serverBootId),
    request: structuredClone(object(raw.request)),
    ...selected,
  });
}
export function identity(input: ReturnType<typeof ownInput>) {
  const version = Object.hasOwn(input, "nativeSelection") ? 2 : 1;
  const value = { v: version, ...input };
  return {
    version,
    text: stableStringify(value),
    hash: WorkflowCanonicalHash.tagged(`t3.preparation-job-identity.v${version}`, value),
  };
}
function parse(text: unknown) {
  if (typeof text !== "string") throw invalid();
  const value: unknown = JSON.parse(text);
  json(value);
  if (stableStringify(value) !== text) throw invalid();
  return object(value);
}
export function settlementHash(
  requestIdentityHash: string,
  codeClosedAt: string,
  settlement: object,
) {
  return WorkflowCanonicalHash.tagged("t3.preparation-job-settlement.v1", {
    requestIdentityHash,
    codeClosedAt,
    settlement,
  });
}
export function decodeRow(value: unknown) {
  json(value);
  const raw = object(
    value,
    "code_closed_at,created_at,entry_key,environment_id,format_version,job_id,kind,originating_launch_job_id,process_exposed,project_id,record_version,request_identity_hash,request_identity_json,run_id,server_boot_id,settled_at,settlement_hash,settlement_json,state,thread_id",
  );
  const stored = parse(raw.request_identity_json);
  if (stored.v !== 1 && stored.v !== 2) throw invalid();
  const { v: _v, ...input } = stored;
  const owned = ownInput(input);
  const encoded = identity(owned);
  if (encoded.version !== stored.v) throw invalid();
  const shadows = {
    format_version: 1,
    job_id: owned.jobId,
    entry_key: owned.entryKey,
    kind: owned.kind,
    environment_id: owned.environmentId,
    project_id: owned.projectId,
    thread_id: owned.threadId,
    run_id: owned.runId,
    originating_launch_job_id: owned.originatingLaunchJobId,
    server_boot_id: owned.serverBootId,
    request_identity_json: encoded.text,
    request_identity_hash: encoded.hash,
  };
  if (Object.entries(shadows).some(([key, item]) => raw[key] !== item)) throw invalid();
  const createdAt = timestamp(raw.created_at);
  const recordVersion = version(raw.record_version);
  const state = raw.state;
  if (state !== "active" && state !== "quiescing" && state !== "unknown" && state !== "settled")
    throw invalid();
  if (raw.process_exposed !== 0 && raw.process_exposed !== 1) throw invalid();
  const codeClosedAt = raw.code_closed_at === null ? null : timestamp(raw.code_closed_at);
  if (codeClosedAt !== null && codeClosedAt < createdAt) throw invalid();
  if (
    recordVersion === 0 &&
    (state !== "active" || raw.process_exposed !== 0 || codeClosedAt !== null)
  )
    throw invalid();
  let settlement = null;
  let settledAt = null;
  if (state === "settled") {
    const evidence = object(parse(raw.settlement_json), "entryKey,jobId,kind,priorVersion,v");
    if (
      evidence.v !== 1 ||
      evidence.jobId !== owned.jobId ||
      evidence.entryKey !== owned.entryKey ||
      version(evidence.priorVersion) !== recordVersion - 1 ||
      (evidence.kind !== "code_only" && evidence.kind !== "owned_resources") ||
      codeClosedAt === null ||
      (evidence.kind === "code_only" && raw.process_exposed !== 0) ||
      digest(raw.settlement_hash) !== settlementHash(encoded.hash, codeClosedAt, evidence)
    )
      throw invalid();
    settledAt = timestamp(raw.settled_at);
    if (settledAt < codeClosedAt) throw invalid();
    settlement = evidence;
  } else if (
    raw.settlement_json !== null ||
    raw.settlement_hash !== null ||
    raw.settled_at !== null
  )
    throw invalid();
  return freeze({
    identityVersion: encoded.version,
    input: owned,
    requestIdentityHash: encoded.hash,
    createdAt,
    recordVersion,
    state,
    processExposed: raw.process_exposed === 1,
    codeClosedAt,
    settlement,
    settledAt,
  });
}
export type Input = ReturnType<typeof ownInput>;
export type NativeSelection = ReturnType<typeof ownNativeSelection>;
export type Job = ReturnType<typeof decodeRow>;
export type Selection = ReturnType<typeof ownSelection>;
export function selection(job: Job): Selection {
  return Object.freeze({
    jobId: job.input.jobId,
    entryKey: job.input.entryKey,
    requestIdentityHash: job.requestIdentityHash,
    serverBootId: job.input.serverBootId,
    expectedVersion: job.recordVersion,
  });
}
