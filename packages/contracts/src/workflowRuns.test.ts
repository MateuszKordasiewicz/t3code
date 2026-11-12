import { describe, expect, it } from "@effect/vitest";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  decodeWorkflowRoleResultV1,
  WorkflowRoleResultV1,
  decodeWorkflowOperationV1,
  WorkflowOperationV1,
  decodeWorkflowRunSpecV1,
  WorkflowRunSpecV1,
  decodeWorkflowRunV1,
  WorkflowRunV1,
} from "./workflowRuns.ts";

const example = () => ({
  schemaVersion: 1,
  plan: {
    planId: "plan-1",
    revisionId: "revision-1",
    revisionSchemaVersion: 1,
    revisionHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    sourcePacketHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  },
  environmentId: "env-1",
  projectId: "project-1",
  repository: {
    root: "/workspace/repo",
    baseCommit: "0123456789012345678901234567890123456789",
  },
  sourceDocument: {
    document: {
      schemaVersion: 1,
      planId: "plan-1",
      revisionId: "revision-1",
      environmentId: "env-1",
      projectId: "project-1",
      title: "Example",
      brief: "Example",
      linkedThreadIds: [],
      sources: [],
      sourceStatements: [],
      requirements: [
        {
          id: "req-1",
          text: "Behavior",
          sourceStatementIds: [],
          criteria: [
            {
              id: "criterion-1",
              text: "Observable result",
            },
          ],
        },
      ],
    },
    sourcePacketHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  },
  requirements: [
    {
      id: "req-1",
      text: "Behavior",
      criteria: [
        {
          id: "criterion-1",
          text: "Observable result",
        },
      ],
    },
  ],
  exclusions: [],
  items: [
    {
      id: "item-1",
      goal: "Implement behavior",
      files: [
        {
          kind: "modify",
          path: "src/a.ts",
        },
      ],
      criterionIds: ["criterion-1"],
      checkIds: ["check-1"],
      acceptance: "Tests pass",
    },
  ],
  implementation: {
    providerInstanceId: "codex",
    model: "example-model",
  },
  reviewer: {
    providerInstanceId: "codex",
    model: "example-model",
  },
  checks: [
    {
      id: "check-1",
      argv: ["npm", "test"],
      cwd: ".",
      configHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
  ],
  provisioning: [],
  registryHosts: [] as string[],
  permissionCeiling: {
    implementation: {
      kind: "configured",
      effectivePolicyHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    reviewer: {
      kind: "native_read_only",
      effectivePolicyHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
  },
  resources: {
    memoryBytes: 1,
    cpuQuotaMicrosPer100ms: 1,
    pidsMax: 1,
    perOperationWallSeconds: 1,
    writableBytes: 1,
    freeDiskReserveBytes: 1,
    environmentConcurrency: 1,
    brokerMaxConnections: 1,
    brokerMaxPendingRequests: 1,
    brokerBufferBytes: 1,
    brokerRequestWallSeconds: 1,
    stdoutStderrBytes: 1048576,
    structuredReviewBytes: 262144,
    rawEvidenceBytes: 134217728,
  },
  limits: {
    maxAttempts: 3,
    maxRepairRounds: 1,
    maxActiveWriters: 1,
    deadline: "2026-12-01T00:00:00Z",
  },
  publication: {
    kind: "draft_pr",
    baseRef: "main",
    requiredCi: ["build"],
  },
  access: {
    origins: [
      {
        environmentId: "env-1",
        projectId: "project-1",
        threadId: "thread-1",
      },
    ],
  },
});
const changed = (f: (value: ReturnType<typeof example>) => void) => {
  const value = example();
  f(value);
  return value;
};
const cases: { name: string; input: unknown; reason?: string }[] = [
  { name: "complete roundtrip", input: example() },
  {
    name: "copied source origin required",
    input: changed((s) => {
      s.sourceDocument.document.sources = [
        {
          id: "source-1",
          kind: "imported_document",
          documentId: "doc-1",
          environmentId: "env-2",
          projectId: "project-2",
          capturedAt: "2026-11-11T00:00:00Z",
          attribution: "user",
          bytesBase64: "WA==",
          byteLength: 1,
          sha256: "4b68ab3847feda7d6c62c1fbcbeebfa35eab7351ed5e78f4ddadea5df64b8015",
        },
      ] as never;
    }),
    reason: "invalid record",
  },
  {
    name: "empty required CI",
    input: changed((s) => {
      s.publication.requiredCi = [];
    }),
  },
  ...[NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, 0].map((n) => ({
    name: `invalid resource ${n}`,
    input: changed((s) => {
      s.resources.memoryBytes = n;
    }),
    reason: "invalid limits or item",
  })),
  {
    name: "missing bound",
    input: changed((s) => {
      Reflect.deleteProperty(s.limits, "deadline");
    }),
    reason: "invalid limits or item",
  },
  {
    name: "empty acceptance",
    input: changed((s) => {
      s.items[0]!.acceptance = "";
    }),
    reason: "invalid limits or item",
  },
  {
    name: "dangling criterion",
    input: changed((s) => {
      s.items[0]!.criterionIds = ["missing"];
    }),
    reason: "invalid mapping",
  },
  {
    name: "dangling check",
    input: changed((s) => {
      s.items[0]!.checkIds = ["missing"];
    }),
    reason: "invalid mapping",
  },
  {
    name: "duplicate item",
    input: changed((s) => {
      s.items.push(s.items[0]!);
    }),
    reason: "invalid mapping",
  },
  {
    name: "rename missing path",
    input: changed((s) => {
      Object.assign(s.items[0]!.files[0]!, { kind: "rename", fromPath: "a" });
      Reflect.deleteProperty(s.items[0]!.files[0]!, "path");
    }),
    reason: "invalid file scope",
  },
  {
    name: "parent path",
    input: changed((s) => {
      s.items[0]!.files[0]!.path = "../a";
    }),
    reason: "invalid file scope",
  },
  {
    name: "unknown version",
    input: changed((s) => {
      s.schemaVersion = 2;
    }),
    reason: "invalid variant",
  },
  {
    name: "mixed file shape",
    input: changed((s) => {
      Object.assign(s.items[0]!.files[0]!, { fromPath: "a" });
    }),
    reason: "invalid variant",
  },
  {
    name: "source hash mismatch",
    input: changed((s) => {
      s.sourceDocument.sourcePacketHash = "b".repeat(64);
    }),
    reason: "invalid record",
  },
  {
    name: "uncovered requirement",
    input: changed((s) => {
      s.requirements = [];
    }),
    reason: "invalid mapping",
  },
  {
    name: "changed criterion",
    input: changed((s) => {
      s.requirements[0]!.criteria[0]!.text = "changed";
    }),
    reason: "invalid mapping",
  },
  {
    name: "duplicate origin",
    input: changed((s) => {
      s.access.origins.push(s.access.origins[0]!);
    }),
    reason: "invalid record",
  },
  {
    name: "uppercase host",
    input: changed((s) => {
      s.registryHosts = ["Example.com"];
    }),
    reason: "invalid record",
  },
  { name: "unknown field", input: { ...example(), extra: true }, reason: "invalid record" },
];

const encodeWorkflowRunSpec = Schema.encodeEffect(WorkflowRunSpecV1);

describe("workflow run spec", () => {
  it.effect.each(cases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodeWorkflowRunSpecV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success")
          expect(yield* encodeWorkflowRunSpec(result.success)).toEqual(input);
      }
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );
});

const runExample = () => ({
  schemaVersion: 1,
  id: "workflow-1",
  spec: {
    ref: { kind: "configuration", id: "spec-1", sha256: "a".repeat(64) },
    plan: example().plan,
  },
  mode: "ordinary",
  origin: { kind: "human", id: "user-1" },
  status: "active",
  controlGeneration: 1,
  recordVersion: 1,
  allowance: {
    maxAttempts: 3,
    maxRepairRounds: 1,
    maxActiveWriters: 1,
    attemptsConsumed: 1,
    repairRoundsConsumed: 0,
    deadline: "2026-12-01T00:00:00Z",
  },
  amendmentSequence: 0,
  createdAt: "2026-11-11T00:00:00Z",
  access: example().access,
});
const changedRun = (f: (value: ReturnType<typeof runExample>) => void) => {
  const value = runExample();
  f(value);
  return value;
};
const runCases: { name: string; input: unknown; reason?: string }[] = [
  { name: "complete record", input: runExample() },
  {
    name: "exhausted needs attention",
    input: changedRun((r) => {
      r.status = "needs_attention";
      r.allowance.attemptsConsumed = 3;
      r.allowance.repairRoundsConsumed = 1;
    }),
  },
  {
    name: "amended",
    input: { ...runExample(), amendmentSequence: 1, priorSpecHash: "b".repeat(64) },
  },
  {
    name: "attempts exceed maximum",
    input: changedRun((r) => {
      r.allowance.attemptsConsumed = 4;
    }),
    reason: "invalid consumption",
  },
  {
    name: "repairs exceed attempts",
    input: changedRun((r) => {
      r.allowance.attemptsConsumed = 0;
      r.allowance.repairRoundsConsumed = 1;
    }),
    reason: "invalid consumption",
  },
  {
    name: "repairs exceed maximum",
    input: changedRun((r) => {
      r.allowance.repairRoundsConsumed = 2;
      r.allowance.attemptsConsumed = 2;
    }),
    reason: "invalid consumption",
  },
  {
    name: "initial record with prior hash",
    input: { ...runExample(), priorSpecHash: "b".repeat(64) },
    reason: "invalid lineage",
  },
  {
    name: "amendment without prior hash",
    input: { ...runExample(), amendmentSequence: 1 },
    reason: "invalid lineage",
  },
  { name: "unknown field", input: { ...runExample(), extra: true }, reason: "invalid record" },
  {
    name: "non configuration reference",
    input: changedRun((r) => {
      r.spec.ref.kind = "source";
    }),
    reason: "invalid record",
  },
  {
    name: "invalid hash",
    input: changedRun((r) => {
      r.spec.ref.sha256 = "A".repeat(64);
    }),
    reason: "invalid record",
  },
  {
    name: "invalid counter",
    input: changedRun((r) => {
      r.recordVersion = Number.MAX_SAFE_INTEGER + 1;
    }),
    reason: "invalid record",
  },
  {
    name: "invalid maximum",
    input: changedRun((r) => {
      r.allowance.maxAttempts = 0;
    }),
    reason: "invalid record",
  },
  {
    name: "invalid timestamp",
    input: changedRun((r) => {
      r.createdAt = "2026-02-30T00:00:00Z";
    }),
    reason: "invalid record",
  },
  {
    name: "deadline before creation",
    input: changedRun((r) => {
      r.allowance.deadline = "2026-11-10T00:00:00Z";
    }),
    reason: "invalid record",
  },
  {
    name: "fractional deadline before creation",
    input: changedRun((r) => {
      r.createdAt = "2026-11-11T00:00:00.0002Z";
      r.allowance.deadline = "2026-11-11T00:00:00.0001Z";
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate origins",
    input: changedRun((r) => {
      r.access.origins.push(r.access.origins[0]!);
    }),
    reason: "invalid record",
  },
];
const encodeWorkflowRun = Schema.encodeEffect(WorkflowRunV1);
describe("workflow run", () => {
  it.effect.each(runCases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodeWorkflowRunV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success")
          expect(yield* encodeWorkflowRun(result.success)).toEqual(input);
      }
    }),
  );
});

const operationHash = "a".repeat(64);
const operationExample = () => ({
  schemaVersion: 1,
  id: "op-1",
  owner: { kind: "planning_session", planningSessionId: "session-1" },
  kind: "planning_pass",
  parameters: {
    kind: "planning_pass",
    passId: "pass-1",
    binding: {
      planId: "plan-1",
      revisionId: "revision-1",
      revisionSchemaVersion: 1,
      revisionHash: operationHash,
      sourcePacketHash: operationHash,
    },
    role: "review",
    provider: { providerInstanceId: "codex", model: "example-model" },
    limits: { maxPasses: 3, maxRepairRounds: 1, deadline: "2026-12-01T00:00:00Z" },
    inputManifestHash: operationHash,
  },
  fingerprint: "bced6ed35e837b84f405446260f877f147094b212485c7cded46d1235fd0714a",
  origin: { kind: "human", id: "user-1" },
  destination: { environmentId: "env-1", projectId: "project-1" },
  controlGeneration: 1,
  status: "pending",
  access: { origins: [{ environmentId: "env-1", projectId: "project-1", threadId: "thread-1" }] },
});
const context = {
  specHash: operationHash,
  policyHash: operationHash,
  itemId: "item-1",
  attemptId: "attempt-1",
  candidateHash: operationHash,
  configurationHash: operationHash,
};
const provider = { providerInstanceId: "codex", model: "model" };
const workflowOperation = <T extends { kind: string }>(parameters: T) => ({
  ...operationExample(),
  owner: { kind: "workflow_run", workflowRunId: "run-1" },
  kind: parameters.kind,
  parameters,
});
const claim = {
  bootId: "boot-1",
  workerId: "worker-1",
  generation: 1,
  claimedAt: "2026-11-11T00:00:00Z",
};
const reference = { kind: "report", id: "ref-1", sha256: operationHash };
const outcome = { kind: "succeeded", summary: "Done", evidence: [reference] };
const operationCases: { name: string; input: unknown; reason?: string }[] = [
  { name: "canonical pending operation", input: operationExample() },
  {
    name: "reordered keys and changed generation",
    input: {
      controlGeneration: 2,
      ...Object.fromEntries(
        Object.entries(operationExample())
          .filter(([key]) => key !== "controlGeneration")
          .reverse(),
      ),
    },
  },
  ...[
    { kind: "workspace_preparation", context, workspaceId: "workspace-1", baseCommit: "abc" },
    {
      kind: "workspace_release",
      context,
      workspaceId: "workspace-1",
      retentionPolicyHash: operationHash,
    },
    { kind: "provisioning", context, commandId: "cmd-1", workspaceId: "workspace-1" },
    {
      kind: "stage_launch",
      context,
      stage: "implementation",
      provider,
      nativeThreadId: "thread-1",
      nativeRunId: "run-1",
    },
    { kind: "check", context, checkId: "check-1", workspaceId: "workspace-1" },
    {
      kind: "supervision",
      context,
      incidentId: "incident-1",
      provider,
      nativeThreadId: "thread-1",
      nativeRunId: "run-1",
    },
    {
      kind: "publication",
      context,
      repositoryId: "repo-1",
      baseRef: "main",
      headRef: "branch",
      expectedRemoteHead: "abc",
      candidateCommit: "def",
    },
    {
      kind: "ci_observation",
      context,
      repositoryId: "repo-1",
      pullRequestId: "1",
      headCommit: "def",
      requiredChecks: [] as string[],
    },
  ].map((parameters) => ({ name: parameters.kind, input: workflowOperation(parameters) })),
  { name: "claimed", input: { ...operationExample(), status: "claimed", claim } },
  {
    name: "settled retains claim",
    input: { ...operationExample(), status: "settled", claim, outcome },
  },
  {
    name: "cancelled",
    input: {
      ...operationExample(),
      status: "cancelled",
      outcome: { ...outcome, kind: "cancelled" },
    },
  },
  {
    name: "unknown",
    input: {
      ...operationExample(),
      status: "unknown",
      outcome: { ...outcome, kind: "unknown" },
      nextDueAt: "2026-12-01T00:00:00Z",
    },
  },
  {
    name: "both owner IDs",
    input: {
      ...operationExample(),
      owner: { ...operationExample().owner, workflowRunId: "run-1" },
    },
    reason: "invalid owner union",
  },
  {
    name: "owner and kind mismatch",
    input: { ...operationExample(), owner: { kind: "workflow_run", workflowRunId: "run-1" } },
    reason: "invalid owner union",
  },
  {
    name: "kind and parameters mismatch",
    input: { ...operationExample(), kind: "check" },
    reason: "invalid owner union",
  },
  {
    name: "claimed without claim",
    input: { ...operationExample(), status: "claimed" },
    reason: "invalid journal state",
  },
  {
    name: "pending with outcome",
    input: { ...operationExample(), outcome },
    reason: "invalid journal state",
  },
  {
    name: "pending with claim",
    input: { ...operationExample(), claim },
    reason: "invalid journal state",
  },
  {
    name: "settled with wrong outcome",
    input: { ...operationExample(), status: "settled", outcome: { ...outcome, kind: "unknown" } },
    reason: "invalid journal state",
  },
  {
    name: "unknown with no outcome",
    input: { ...operationExample(), status: "unknown" },
    reason: "invalid journal state",
  },
  ...(
    [
      ["final combined review launch", "code_review", {}, undefined],
      ["implementation launch without item", "implementation", {}, "invalid record"],
      [
        "review launch with item but no attempt",
        "code_review",
        { itemId: "item-1" },
        "invalid record",
      ],
    ] as const
  ).map(([name, stage, ids, reason]) => {
    const { itemId: _itemId, attemptId: _attemptId, ...combined } = context;
    return {
      name,
      input: workflowOperation({
        kind: "stage_launch",
        context: { ...combined, ...ids },
        stage,
        provider,
        nativeThreadId: "thread-1",
        nativeRunId: "run-1",
      }),
      ...(reason ? { reason } : {}),
    };
  }),
  {
    name: "invalid hash format",
    input: { ...operationExample(), fingerprint: "A".repeat(64) },
    reason: "invalid record",
  },
  { name: "extra field", input: { ...operationExample(), extra: true }, reason: "invalid record" },
  {
    name: "invalid generation",
    input: { ...operationExample(), controlGeneration: Number.MAX_SAFE_INTEGER + 1 },
    reason: "invalid record",
  },
  {
    name: "invalid claim time",
    input: {
      ...operationExample(),
      status: "claimed",
      claim: { ...claim, claimedAt: "2026-02-30T00:00:00Z" },
    },
    reason: "invalid record",
  },
  {
    name: "invalid planning limit",
    input: {
      ...operationExample(),
      parameters: {
        ...operationExample().parameters,
        limits: { ...operationExample().parameters.limits, maxPasses: 0 },
      },
    },
    reason: "invalid record",
  },
  {
    name: "duplicate access",
    input: {
      ...operationExample(),
      access: {
        origins: [operationExample().access.origins[0], operationExample().access.origins[0]],
      },
    },
    reason: "invalid record",
  },
  {
    name: "duplicate evidence",
    input: {
      ...operationExample(),
      status: "settled",
      outcome: { ...outcome, evidence: [reference, { ...reference }] },
    },
    reason: "invalid record",
  },
  {
    name: "conflicting evidence identity",
    input: {
      ...operationExample(),
      status: "settled",
      outcome: { ...outcome, evidence: [reference, { ...reference, sha256: "b".repeat(64) }] },
    },
    reason: "invalid record",
  },
  {
    name: "missing check context",
    input: workflowOperation({
      kind: "check",
      context: { specHash: operationHash, policyHash: operationHash },
      checkId: "check-1",
      workspaceId: "workspace-1",
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate required checks",
    input: workflowOperation({
      kind: "ci_observation",
      context,
      repositoryId: "repo-1",
      pullRequestId: "1",
      headCommit: "abc",
      requiredChecks: ["build", "build"],
    }),
    reason: "invalid record",
  },
];
const encodeWorkflowOperation = Schema.encodeEffect(WorkflowOperationV1);
describe("workflow operation", () => {
  it.effect.each(operationCases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodeWorkflowOperationV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success")
          expect(yield* encodeWorkflowOperation(result.success)).toEqual(input);
      }
    }),
  );
});

const roleOutput = {
  ref: { kind: "artifact", id: "artifact-1", sha256: operationHash },
  byteLength: 25,
  availability: "expired",
  access: { origins: [{ environmentId: "env-1", projectId: "project-1", threadId: "thread-1" }] },
};
const roleExample = () => ({
  schemaVersion: 1,
  id: "result-1",
  workflowRunId: "workflow-1",
  operationId: "op-2",
  binding: { ...context },
  result: {
    kind: "code_review",
    verdict: "inconclusive",
    findings: [] as unknown[],
    output: roleOutput,
  },
  access: roleOutput.access,
});
const roleWith = (result: unknown) => ({ ...roleExample(), result });
const finding = {
  id: "finding-1",
  severity: "blocking",
  description: "Problem",
  evidence: [] as unknown[],
  requiredRepair: "Fix",
};
const criterion = {
  requirementId: "req-1",
  criterionId: "criterion-1",
  verdict: "met",
  evidence: [reference],
};
const roleCases: { name: string; input: unknown; reason?: string }[] = [
  { name: "inconclusive review with expired output", input: roleExample() },
  {
    name: "nonzero check exit",
    input: roleWith({
      kind: "check",
      checkId: "check-1",
      argv: ["cmd", ""],
      cwd: ".",
      exit: { kind: "exited", code: 1 },
      truncated: false,
      stdoutStderr: roleOutput,
    }),
  },
  ...["cancelled", "unknown"].map((kind) => ({
    name: `check ${kind}`,
    input: roleWith({
      kind: "check",
      checkId: "check-1",
      argv: ["cmd"],
      cwd: ".",
      exit: { kind },
      truncated: true,
      stdoutStderr: roleOutput,
    }),
  })),
  {
    name: "semantic review",
    input: roleWith({ kind: "semantic_review", criteria: [criterion], output: roleOutput }),
  },
  {
    name: "rollup combined head",
    input: {
      ...roleWith({
        kind: "rollup",
        verdict: "accepted",
        resultRefs: [reference],
        summary: "Done",
      }),
      binding: {
        specHash: operationHash,
        candidateHash: operationHash,
        configurationHash: operationHash,
        policyHash: operationHash,
      },
    },
  },
  {
    name: "missing candidate hash",
    input: {
      ...roleExample(),
      binding: {
        specHash: operationHash,
        policyHash: operationHash,
        configurationHash: operationHash,
      },
    },
    reason: "invalid evidence identity",
  },
  {
    name: "conflicting report identity",
    input: roleWith({
      kind: "rollup",
      verdict: "accepted",
      resultRefs: [reference, { ...reference, sha256: "b".repeat(64) }],
      summary: "Done",
    }),
    reason: "malformed result",
  },
  {
    name: "pass with blocking finding",
    input: roleWith({
      kind: "code_review",
      verdict: "pass",
      findings: [finding],
      output: roleOutput,
    }),
    reason: "invalid verdict",
  },
  {
    name: "blocking finding without repair",
    input: roleWith({
      kind: "code_review",
      verdict: "changes_requested",
      findings: [
        {
          id: finding.id,
          severity: finding.severity,
          description: finding.description,
          evidence: finding.evidence,
        },
      ],
      output: roleOutput,
    }),
    reason: "invalid verdict",
  },
  {
    name: "met without evidence",
    input: roleWith({
      kind: "semantic_review",
      criteria: [{ ...criterion, evidence: [] }],
      output: roleOutput,
    }),
    reason: "invalid verdict",
  },
  {
    name: "duplicate criterion",
    input: roleWith({
      kind: "semantic_review",
      criteria: [criterion, criterion],
      output: roleOutput,
    }),
    reason: "malformed result",
  },
  {
    name: "mixed exit variants",
    input: roleWith({
      kind: "check",
      checkId: "check-1",
      argv: ["cmd"],
      cwd: ".",
      exit: { kind: "cancelled", code: 0 },
      truncated: false,
      stdoutStderr: roleOutput,
    }),
    reason: "malformed result",
  },
  {
    name: "one item binding",
    input: {
      ...roleExample(),
      binding: {
        specHash: operationHash,
        candidateHash: operationHash,
        configurationHash: operationHash,
        policyHash: operationHash,
        itemId: "item-1",
      },
    },
    reason: "malformed result",
  },
  {
    name: "empty argv",
    input: roleWith({
      kind: "check",
      checkId: "check-1",
      argv: [],
      cwd: ".",
      exit: { kind: "unknown" },
      truncated: false,
      stdoutStderr: roleOutput,
    }),
    reason: "malformed result",
  },
  {
    name: "unsafe length",
    input: roleWith({
      ...roleExample().result,
      output: { ...roleOutput, byteLength: Number.MAX_SAFE_INTEGER + 1 },
    }),
    reason: "malformed result",
  },
  {
    name: "missing output origin",
    input: roleWith({
      ...roleExample().result,
      output: { ...roleOutput, access: { origins: [] } },
    }),
    reason: "malformed result",
  },
  {
    name: "duplicate evidence",
    input: roleWith({
      kind: "semantic_review",
      criteria: [{ ...criterion, evidence: [reference, reference] }],
      output: roleOutput,
    }),
    reason: "malformed result",
  },
  { name: "extra field", input: { ...roleExample(), extra: true }, reason: "invalid record" },
];
const encodeWorkflowRoleResult = Schema.encodeEffect(WorkflowRoleResultV1);
describe("workflow role result", () => {
  it.effect.each(roleCases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodeWorkflowRoleResultV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success")
          expect(yield* encodeWorkflowRoleResult(result.success)).toEqual(input);
      }
    }),
  );
});
