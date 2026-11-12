import { describe, expect, it } from "@effect/vitest";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { decodeWorkflowRunSpecV1, WorkflowRunSpecV1 } from "./workflowRuns.ts";

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
