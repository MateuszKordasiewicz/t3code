import { describe, expect, it } from "@effect/vitest";
import type { WorkflowOperationV1, WorkflowRunSpecV1 } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";

const payload1 = {
  schemaVersion: 1,
  owner: {
    kind: "workflow_run",
    workflowRunId: "run-1",
  },
  kind: "ci_observation",
  parameters: {
    kind: "ci_observation",
    context: {
      specHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      policyHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      candidateHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      configurationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    repositoryId: "repo-1",
    pullRequestId: "pr-1",
    headCommit: "commit-1",
    requiredChecks: ["build", "test"],
  },
  origin: {
    kind: "human",
    id: "user-1",
  },
  destination: {
    environmentId: "env-1",
    projectId: "project-1",
  },
};
const canonical1 =
  '{"destination":{"environmentId":"env-1","projectId":"project-1"},"kind":"ci_observation","origin":{"id":"user-1","kind":"human"},"owner":{"kind":"workflow_run","workflowRunId":"run-1"},"parameters":{"context":{"candidateHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","configurationHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","policyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","specHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"headCommit":"commit-1","kind":"ci_observation","pullRequestId":"pr-1","repositoryId":"repo-1","requiredChecks":["build","test"]},"schemaVersion":1}';
const digest1 = "ff315780a4b838f75c072b80a91b31efce86738338275a58c4784c5ddfdc31a2";

const payload2 = {
  schemaVersion: 1,
  owner: {
    kind: "workflow_run",
    workflowRunId: "run-1",
  },
  kind: "ci_observation",
  parameters: {
    kind: "ci_observation",
    context: {
      specHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      policyHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      candidateHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      configurationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    repositoryId: "repo-1",
    pullRequestId: "pr-1",
    headCommit: "commit-1",
    requiredChecks: ["test", "build"],
  },
  origin: {
    kind: "human",
    id: "user-1",
  },
  destination: {
    environmentId: "env-1",
    projectId: "project-1",
  },
};
const canonical2 =
  '{"destination":{"environmentId":"env-1","projectId":"project-1"},"kind":"ci_observation","origin":{"id":"user-1","kind":"human"},"owner":{"kind":"workflow_run","workflowRunId":"run-1"},"parameters":{"context":{"candidateHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","configurationHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","policyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","specHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"headCommit":"commit-1","kind":"ci_observation","pullRequestId":"pr-1","repositoryId":"repo-1","requiredChecks":["test","build"]},"schemaVersion":1}';
const digest2 = "c2c93845fdfd65f934e9661a0daebe9c74b5de7068036d53f7235f02fa599dba";

const payload3 = {
  schemaVersion: 1,
  owner: {
    kind: "workflow_run",
    workflowRunId: "run-1",
  },
  kind: "ci_observation",
  parameters: {
    kind: "ci_observation",
    context: {
      specHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      policyHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      candidateHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      configurationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },
    repositoryId: "repo-1",
    pullRequestId: "pr-1",
    headCommit: "commit-2",
    requiredChecks: ["build", "test"],
  },
  origin: {
    kind: "human",
    id: "user-1",
  },
  destination: {
    environmentId: "env-1",
    projectId: "project-1",
  },
};
const canonical3 =
  '{"destination":{"environmentId":"env-1","projectId":"project-1"},"kind":"ci_observation","origin":{"id":"user-1","kind":"human"},"owner":{"kind":"workflow_run","workflowRunId":"run-1"},"parameters":{"context":{"candidateHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","configurationHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","policyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","specHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"headCommit":"commit-2","kind":"ci_observation","pullRequestId":"pr-1","repositoryId":"repo-1","requiredChecks":["build","test"]},"schemaVersion":1}';
const digest3 = "d719d561823a7a05cec5ce180af9ac42fd14fe2a4acc39e728cbf69a84b0ff39";

const payload4 = {
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
  registryHosts: [],
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
};
const canonical4 =
  '{"access":{"origins":[{"environmentId":"env-1","projectId":"project-1","threadId":"thread-1"}]},"checks":[{"argv":["npm","test"],"configHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","cwd":".","id":"check-1"}],"environmentId":"env-1","exclusions":[],"implementation":{"model":"example-model","providerInstanceId":"codex"},"items":[{"acceptance":"Tests pass","checkIds":["check-1"],"criterionIds":["criterion-1"],"files":[{"kind":"modify","path":"src/a.ts"}],"goal":"Implement behavior","id":"item-1"}],"limits":{"deadline":"2026-12-01T00:00:00Z","maxActiveWriters":1,"maxAttempts":3,"maxRepairRounds":1},"permissionCeiling":{"implementation":{"effectivePolicyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","kind":"configured"},"reviewer":{"effectivePolicyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","kind":"native_read_only"}},"plan":{"planId":"plan-1","revisionHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","revisionId":"revision-1","revisionSchemaVersion":1,"sourcePacketHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"projectId":"project-1","provisioning":[],"publication":{"baseRef":"main","kind":"draft_pr","requiredCi":["build"]},"registryHosts":[],"repository":{"baseCommit":"0123456789012345678901234567890123456789","root":"/workspace/repo"},"requirements":[{"criteria":[{"id":"criterion-1","text":"Observable result"}],"id":"req-1","text":"Behavior"}],"resources":{"brokerBufferBytes":1,"brokerMaxConnections":1,"brokerMaxPendingRequests":1,"brokerRequestWallSeconds":1,"cpuQuotaMicrosPer100ms":1,"environmentConcurrency":1,"freeDiskReserveBytes":1,"memoryBytes":1,"perOperationWallSeconds":1,"pidsMax":1,"rawEvidenceBytes":134217728,"stdoutStderrBytes":1048576,"structuredReviewBytes":262144,"writableBytes":1},"reviewer":{"model":"example-model","providerInstanceId":"codex"},"schemaVersion":1,"sourceDocument":{"document":{"brief":"Example","environmentId":"env-1","linkedThreadIds":[],"planId":"plan-1","projectId":"project-1","requirements":[{"criteria":[{"id":"criterion-1","text":"Observable result"}],"id":"req-1","sourceStatementIds":[],"text":"Behavior"}],"revisionId":"revision-1","schemaVersion":1,"sourceStatements":[],"sources":[],"title":"Example"},"sourcePacketHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}';
const digest4 = "0325e0d8eed5c56485b66bebcc34c09adcaab8cbbe4ee8a5329ea53cfef0952a";

const asOperation = (value: unknown) => value as WorkflowOperationV1;
const asSpec = (value: unknown) => value as WorkflowRunSpecV1;

describe("WorkflowCanonicalHash", () => {
  it.each([
    [payload1, canonical1, digest1, "operation"],
    [payload2, canonical2, digest2, "operation"],
    [payload3, canonical3, digest3, "operation"],
    [payload4, canonical4, digest4, "spec"],
  ])("matches canonical vector %#", (payload, canonical, digest, kind) => {
    expect(stableStringify(payload)).toBe(canonical);
    expect(
      kind === "operation"
        ? WorkflowCanonicalHash.operation(asOperation(payload))
        : WorkflowCanonicalHash.runSpec(asSpec(payload)),
    ).toBe(digest);
  });

  it("verifies fingerprints while excluding mutable operation fields", () => {
    const operation = {
      ...payload1,
      fingerprint: digest1,
      status: "pending",
      controlGeneration: 1,
    };
    expect(WorkflowCanonicalHash.verifyOperation(asOperation(operation))).toBe(true);
    expect(
      WorkflowCanonicalHash.verifyOperation(
        asOperation({ ...operation, status: "settled", controlGeneration: 2 }),
      ),
    ).toBe(true);
    expect(
      WorkflowCanonicalHash.verifyOperation(asOperation({ ...payload2, fingerprint: digest1 })),
    ).toBe(false);
    expect(
      WorkflowCanonicalHash.verifyOperation(asOperation({ ...payload2, fingerprint: digest2 })),
    ).toBe(true);
    expect(
      WorkflowCanonicalHash.verifyOperation(asOperation({ ...payload3, fingerprint: digest1 })),
    ).toBe(false);
    const reversed = Object.fromEntries(Object.entries(payload1).reverse());
    expect(WorkflowCanonicalHash.operation(asOperation(reversed))).toBe(digest1);
  });

  it("accepts shared references without cycles", () => {
    const shared = { value: 1.5 };
    expect(() =>
      WorkflowCanonicalHash.runSpec(asSpec({ first: shared, second: shared })),
    ).not.toThrow();
  });

  it("rejects unsupported values without invoking accessors", () => {
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    const sparse = new Array(1);
    const symbol = { [Symbol("x")]: 1 };
    const hidden = Object.defineProperty({}, "x", { value: 1 });
    let invoked = false;
    const accessor = Object.defineProperty({}, "x", {
      enumerable: true,
      get() {
        invoked = true;
        return 1;
      },
    });
    const badValues = [
      undefined,
      () => {},
      Symbol("x"),
      1n,
      NaN,
      Infinity,
      -Infinity,
      Number.MAX_SAFE_INTEGER + 1,
      sparse,
      cycle,
      new Map(),
      accessor,
      symbol,
      hidden,
      Object.assign([], { extra: 1 }),
    ];
    for (const value of badValues) {
      expect(() => WorkflowCanonicalHash.runSpec(asSpec({ nested: value }))).toThrow(
        "Invalid canonical hash payload",
      );
    }
    expect(invoked).toBe(false);
    expect(() =>
      WorkflowCanonicalHash.verifyOperation(
        asOperation({ ...payload1, parameters: accessor, fingerprint: digest1 }),
      ),
    ).toThrow("Invalid canonical hash payload");
    expect(invoked).toBe(false);
  });
});
