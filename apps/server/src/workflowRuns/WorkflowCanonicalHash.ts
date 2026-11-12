// @effect-diagnostics-next-line nodeBuiltinImport:off -- pure policy code needs synchronous hashing; Effect Digest is effectful.
import * as NodeCrypto from "node:crypto";

import type { WorkflowOperationV1, WorkflowRunSpecV1 } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";

const invalid = () => new Error("Invalid canonical hash payload");

function preflight(value: unknown, ancestors = new Set<object>()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (
      !Number.isFinite(value) ||
      Math.abs(value) > Number.MAX_SAFE_INTEGER ||
      (Number.isInteger(value) && !Number.isSafeInteger(value))
    )
      throw invalid();
    return;
  }
  if (typeof value !== "object") throw invalid();
  const array = Array.isArray(value);
  const prototype = Object.getPrototypeOf(value);
  if (
    array ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null
  ) {
    throw invalid();
  }
  if (ancestors.has(value)) throw invalid();
  ancestors.add(value);
  const keys = Reflect.ownKeys(value);
  if (array && keys.length !== value.length + 1) throw invalid();
  for (const key of keys) {
    if (array && key === "length") continue;
    if (typeof key !== "string") throw invalid();
    if (array && (!/^(0|[1-9]\d*)$/.test(key) || Number(key) >= value.length)) throw invalid();
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) throw invalid();
    preflight(descriptor.value, ancestors);
  }
  ancestors.delete(value);
}

function field(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (!descriptor || !descriptor.enumerable || !("value" in descriptor)) throw invalid();
  return descriptor.value;
}

function operation(operation: WorkflowOperationV1): string {
  if (
    typeof operation !== "object" ||
    operation === null ||
    (Object.getPrototypeOf(operation) !== Object.prototype &&
      Object.getPrototypeOf(operation) !== null)
  )
    throw invalid();
  const payload = {
    schemaVersion: field(operation, "schemaVersion"),
    owner: field(operation, "owner"),
    kind: field(operation, "kind"),
    parameters: field(operation, "parameters"),
    origin: field(operation, "origin"),
    destination: field(operation, "destination"),
  };
  return hash("t3.workflow-operation.v1", payload);
}

function hash(tag: string, payload: unknown): string {
  preflight(payload);
  return NodeCrypto.createHash("sha256")
    .update(tag, "utf8")
    .update("\0", "utf8")
    .update(stableStringify(payload), "utf8")
    .digest("hex");
}

function command(workflowRunId: string, value: object): string {
  preflight(value);
  const payload = { ...value } as Record<string, unknown>;
  delete payload.fingerprint;
  return hash("t3.workflow-command.v1", { workflowRunId, command: payload });
}

export const WorkflowCanonicalHash = {
  tagged: hash,
  assertCanonical: (value: unknown): void => preflight(value),
  command,
  operation,
  verifyOperation: (value: WorkflowOperationV1): boolean =>
    operation(value) === field(value, "fingerprint"),
  runSpec: (value: WorkflowRunSpecV1): string => hash("t3.workflow-run-spec.v1", value),
};
