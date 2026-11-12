import { describe, expect, it } from "@effect/vitest";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  decodeDevelopmentPlanDocumentV1,
  decodePlanningSessionV1,
  decodePlanningPassV1,
  PlanningPassV1,
  PlanningSessionV1,
} from "./developmentPlans.ts";

const example = () => ({
  schemaVersion: 1,
  planId: "plan-1",
  revisionId: "revision-1",
  environmentId: "environment-1",
  projectId: "project-1",
  title: "Example plan",
  brief: "Keep source separate from interpretation",
  linkedThreadIds: ["thread-1"],
  sources: [
    {
      id: "source-1",
      kind: "thread_message",
      environmentId: "environment-1",
      projectId: "project-1",
      threadId: "thread-1",
      messageId: "message-1",
      capturedAt: "2026-11-10T00:00:00.000Z",
      attribution: "user",
      bytesBase64: "WA==",
      byteLength: 1,
      sha256: "4b68ab3847feda7d6c62c1fbcbeebfa35eab7351ed5e78f4ddadea5df64b8015",
    },
  ],
  sourceStatements: [
    { id: "statement-1", sourceId: "source-1", startByte: 0, endByte: 1, text: "X" },
  ],
  requirements: [
    {
      id: "requirement-1",
      text: "Interpret X as the requested example behavior",
      sourceStatementIds: ["statement-1"],
      criteria: [{ id: "criterion-1", text: "Observable example behavior" }],
    },
  ],
});

type Fixture = ReturnType<typeof example>;
const changed = (f: (doc: Fixture) => void) => {
  const doc = example();
  f(doc);
  return doc;
};
const cases: { name: string; input: unknown; reason?: string }[] = [
  { name: "example roundtrip", input: example() },
  {
    name: "empty draft",
    input: changed((d) => {
      d.linkedThreadIds = [];
      d.sources = [];
      d.sourceStatements = [];
      d.requirements = [];
    }),
  },
  {
    name: "imported source retains origin",
    input: changed((d) => {
      const { threadId: _threadId, messageId: _messageId, ...source } = d.sources[0]!;
      d.sources = [
        {
          ...source,
          kind: "imported_document",
          documentId: "doc-1",
        } as unknown as (typeof d.sources)[number],
      ];
    }),
  },
  {
    name: "unsupported version",
    input: changed((d) => {
      d.schemaVersion = 2;
    }),
    reason: "unsupported version",
  },
  {
    name: "missing required field",
    input: changed((d) => {
      Reflect.deleteProperty(d, "title");
    }),
    reason: "malformed field",
  },
  { name: "non-object input", input: null, reason: "malformed field" },
  { name: "unknown root field", input: { ...example(), extra: 1 }, reason: "unknown field" },
  {
    name: "unknown nested field",
    input: changed((d) => {
      Object.assign(d.requirements[0]!.criteria[0]!, { extra: 1 });
    }),
    reason: "unknown field",
  },
  ...(["sources", "sourceStatements", "requirements"] as const).map((key) => ({
    name: `duplicate ${key}`,
    input: changed((d) => {
      d[key].push(d[key][0]! as never);
    }),
    reason: "ambiguous identity",
  })),
  {
    name: "duplicate criterion across requirements",
    input: changed((d) => {
      d.requirements.push({ ...d.requirements[0]!, id: "other" });
    }),
    reason: "ambiguous identity",
  },
  {
    name: "duplicate references",
    input: changed((d) => {
      d.requirements[0]!.sourceStatementIds.push("statement-1");
    }),
    reason: "ambiguous identity",
  },
  {
    name: "duplicate linked threads",
    input: changed((d) => {
      d.linkedThreadIds.push("thread-1");
    }),
    reason: "ambiguous identity",
  },
  {
    name: "missing source",
    input: changed((d) => {
      d.sourceStatements[0]!.sourceId = "missing";
    }),
    reason: "dangling reference",
  },
  {
    name: "missing statement",
    input: changed((d) => {
      d.requirements[0]!.sourceStatementIds = ["missing"];
    }),
    reason: "dangling reference",
  },
  ...[2, -1, 0.5, Number.MAX_SAFE_INTEGER + 1].map((n) => ({
    name: `invalid span ${n}`,
    input: changed((d) => {
      d.sourceStatements[0]!.endByte = n;
    }),
    reason: "invalid byte span",
  })),
  ...["invalid", "WA", "WB=="].map((n) => ({
    name: `invalid base64 ${n}`,
    input: changed((d) => {
      d.sources[0]!.bytesBase64 = n;
    }),
    reason: "source identity mismatch",
  })),
  {
    name: "wrong byte length",
    input: changed((d) => {
      d.sources[0]!.byteLength = 2;
    }),
    reason: "source identity mismatch",
  },
  {
    name: "wrong hash",
    input: changed((d) => {
      d.sources[0]!.sha256 = "0".repeat(64);
    }),
    reason: "source identity mismatch",
  },
  {
    name: "invalid timestamp",
    input: changed((d) => {
      d.sources[0]!.capturedAt = "2026-02-30T00:00:00Z";
    }),
    reason: "source identity mismatch",
  },
  {
    name: "different selected text",
    input: changed((d) => {
      d.sourceStatements[0]!.text = "x";
    }),
    reason: "text differs from original selected bytes",
  },
  {
    name: "malformed selected UTF-8",
    input: changed((d) => {
      d.sources[0]!.bytesBase64 = "gA==";
      d.sources[0]!.sha256 = "76be8b528d0075f7aae98d6fa57a6d3c83ae480a8469e668d7b0af968995ac71";
    }),
    reason: "text differs from original selected bytes",
  },
  {
    name: "thread with document id",
    input: changed((d) => {
      Object.assign(d.sources[0]!, { documentId: "doc" });
    }),
    reason: "mixed discriminant shape",
  },
  {
    name: "import with message id",
    input: changed((d) => {
      Object.assign(d.sources[0]!, { kind: "imported_document", documentId: "doc" });
    }),
    reason: "mixed discriminant shape",
  },
];

describe("development plan external document", () => {
  it.effect.each(cases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodeDevelopmentPlanDocumentV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else expect(result).toMatchObject({ _tag: "Success", success: input });
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );
});

const sessionExample = () => ({
  schemaVersion: 1,
  id: "session-1",
  environmentId: "env-1",
  projectId: "project-1",
  planId: "plan-1",
  origin: { kind: "human", id: "user-1" },
  request: "Review this plan",
  mode: "ordinary",
  allowedRoles: ["review"],
  allowedProviders: [{ providerInstanceId: "codex", model: "example-model" }],
  limits: { maxPasses: 3, maxRepairRounds: 1, deadline: "2026-12-01T00:00:00Z" },
  consumed: { passes: 1, repairRounds: 0 },
  controlGeneration: 1,
  recordVersion: 1,
  status: "active",
  createdAt: "2026-11-11T00:00:00Z",
  access: { origins: [{ environmentId: "env-1", projectId: "project-1", threadId: "thread-1" }] },
});
const changedSession = (f: (session: ReturnType<typeof sessionExample>) => void) => {
  const session = sessionExample();
  f(session);
  return session;
};
const sessionCases: { name: string; input: unknown; reason?: string }[] = [
  { name: "example roundtrip", input: sessionExample() },
  {
    name: "exhausted allowance needs attention",
    input: changedSession((s) => {
      s.status = "needs_attention";
      s.consumed = { passes: 3, repairRounds: 1 };
    }),
  },
  {
    name: "passes exceed limit",
    input: changedSession((s) => {
      s.consumed.passes = 4;
    }),
    reason: "invalid allowance",
  },
  {
    name: "repair rounds exceed limit",
    input: changedSession((s) => {
      s.consumed = { passes: 2, repairRounds: 2 };
    }),
    reason: "invalid allowance",
  },
  {
    name: "repair rounds exceed passes",
    input: changedSession((s) => {
      s.consumed = { passes: 0, repairRounds: 1 };
    }),
    reason: "invalid allowance",
  },
  {
    name: "created after deadline",
    input: changedSession((s) => {
      s.createdAt = "2026-12-02T00:00:00Z";
    }),
    reason: "invalid record",
  },
  {
    name: "empty roles",
    input: changedSession((s) => {
      s.allowedRoles = [];
    }),
    reason: "invalid record",
  },
  {
    name: "empty providers",
    input: changedSession((s) => {
      s.allowedProviders = [];
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate role",
    input: changedSession((s) => {
      s.allowedRoles.push("review");
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate provider",
    input: changedSession((s) => {
      s.allowedProviders.push({ ...s.allowedProviders[0]! });
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate origin",
    input: changedSession((s) => {
      s.access.origins.push({ ...s.access.origins[0]! });
    }),
    reason: "invalid record",
  },
  {
    name: "malformed deadline",
    input: changedSession((s) => {
      s.limits.deadline = "2026-02-30T00:00:00Z";
    }),
    reason: "invalid record",
  },
  {
    name: "unsafe count",
    input: changedSession((s) => {
      s.controlGeneration = Number.MAX_SAFE_INTEGER + 1;
    }),
    reason: "invalid record",
  },
  {
    name: "unknown status",
    input: changedSession((s) => {
      s.status = "unknown";
    }),
    reason: "invalid record",
  },
  { name: "extra root key", input: { ...sessionExample(), extra: true }, reason: "invalid record" },
  {
    name: "extra nested key",
    input: changedSession((s) => {
      Object.assign(s.access.origins[0]!, { extra: true });
    }),
    reason: "invalid record",
  },
];

const encodePlanningSession = Schema.encodeEffect(PlanningSessionV1);

describe("planning session", () => {
  it.effect.each(sessionCases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodePlanningSessionV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success") {
          const encoded = yield* encodePlanningSession(result.success);
          expect(encoded).toEqual(input);
        }
      }
    }),
  );
});

const passHash = "a".repeat(64);
const passReference = () => ({ kind: "source", id: "artifact-1", sha256: passHash });
const passManifest = () => ({
  sources: [passReference()],
  references: [],
  additionalContext: [],
  access: { origins: [{ environmentId: "env-1", projectId: "project-1", threadId: "thread-1" }] },
});
const passExample = () => ({
  schemaVersion: 1,
  id: "pass-1",
  planningSessionId: "session-1",
  role: "review",
  provider: { providerInstanceId: "codex", model: "example-model" },
  binding: {
    planId: "plan-1",
    revisionId: "revision-1",
    revisionSchemaVersion: 1,
    revisionHash: passHash,
    sourcePacketHash: passHash,
  },
  operationId: "op-1",
  inputManifest: passManifest(),
  status: "settled",
  native: { threadId: "thread-2", runId: "run-1", terminalMessageId: "message-1" },
  report: {
    kind: "review",
    verdict: "pass",
    assessment: {
      sourcePacketHash: passHash,
      revisionHash: passHash,
      inventory: [
        {
          id: "inv-1",
          text: "Requested behavior",
          sourceRefs: [passReference()],
          requirementIds: ["req-1"],
          disposition: "mapped",
        },
      ],
    },
    findings: [] as {
      id: string;
      severity: string;
      description: string;
      affectedObjectIds: string[];
      evidence: ReturnType<typeof passReference>[];
      proposedRemedy: string;
    }[],
  },
  access: passManifest().access,
});
const changedPass = (f: (pass: ReturnType<typeof passExample>) => void) => {
  const pass = passExample();
  f(pass);
  return pass;
};
const passCases: { name: string; input: unknown; reason?: string }[] = [
  { name: "review preserves source scope", input: passExample() },
  {
    name: "readback preserves actual inputs",
    input: {
      ...passExample(),
      role: "readback",
      report: {
        kind: "readback",
        buildDescription: "Build",
        inferredIntent: "Intent",
        discrepancies: [],
        questions: [],
        actualInputs: { ...passManifest(), additionalContext: [passReference()] },
      },
    },
  },
  {
    name: "cancelled without report",
    input: (() => {
      const { report: _report, ...pass } = passExample();
      return { ...pass, status: "cancelled" };
    })(),
  },
  {
    name: "author report",
    input: {
      ...passExample(),
      role: "author",
      report: { kind: "author", summary: "Summary", outputs: [passReference()] },
    },
  },
  {
    name: "repair defer with trigger",
    input: {
      ...passExample(),
      role: "repair",
      report: {
        kind: "repair",
        summary: "Summary",
        dispositions: [
          {
            reportId: "pass-2",
            findingId: "finding-1",
            decision: "defer",
            rationale: "Later",
            affectedObjectIds: [],
            deferTrigger: "Trigger",
          },
        ],
      },
    },
  },
  {
    name: "conflicting dispositions for one finding",
    input: (() => {
      const disposition = {
        reportId: "pass-2",
        findingId: "finding-1",
        decision: "accept",
        rationale: "Fix",
        affectedObjectIds: [],
      };
      return {
        ...passExample(),
        role: "repair",
        report: {
          kind: "repair",
          summary: "Summary",
          dispositions: [disposition, { ...disposition, decision: "reject" }],
        },
      };
    })(),
    reason: "malformed report",
  },
  {
    name: "role mismatch",
    input: changedPass((p) => {
      p.role = "repair";
    }),
    reason: "role or binding mismatch",
  },
  ...(["revisionHash", "sourcePacketHash"] as const).map((key) => ({
    name: `mismatched ${key}`,
    input: changedPass((p) => {
      p.report.assessment[key] = "b".repeat(64);
    }),
    reason: "role or binding mismatch",
  })),
  {
    name: "defer without trigger",
    input: {
      ...passExample(),
      role: "repair",
      report: {
        kind: "repair",
        summary: "Summary",
        dispositions: [
          {
            reportId: "pass-2",
            findingId: "finding-1",
            decision: "defer",
            rationale: "Later",
            affectedObjectIds: [],
          },
        ],
      },
    },
    reason: "malformed report",
  },
  {
    name: "trigger on accepted disposition",
    input: {
      ...passExample(),
      role: "repair",
      report: {
        kind: "repair",
        summary: "Summary",
        dispositions: [
          {
            reportId: "pass-2",
            findingId: "finding-1",
            decision: "accept",
            rationale: "Yes",
            affectedObjectIds: [],
            deferTrigger: "Later",
          },
        ],
      },
    },
    reason: "malformed report",
  },
  {
    name: "duplicate finding id",
    input: changedPass((p) => {
      const finding = {
        id: "finding-1",
        severity: "blocking",
        description: "Problem",
        affectedObjectIds: [],
        evidence: [],
        proposedRemedy: "Fix",
      };
      p.report.findings = [finding, { ...finding }];
    }),
    reason: "malformed report",
  },
  {
    name: "duplicate inventory id",
    input: changedPass((p) => {
      p.report.assessment.inventory.push({ ...p.report.assessment.inventory[0]! });
    }),
    reason: "malformed report",
  },
  {
    name: "mapped without requirement",
    input: changedPass((p) => {
      p.report.assessment.inventory[0]!.requirementIds = [];
    }),
    reason: "malformed report",
  },
  {
    name: "inventory without source",
    input: changedPass((p) => {
      p.report.assessment.inventory[0]!.sourceRefs = [];
    }),
    reason: "malformed report",
  },
  {
    name: "report before settled",
    input: changedPass((p) => {
      p.status = "claimed";
    }),
    reason: "unbound report",
  },
  {
    name: "report without terminal message",
    input: changedPass((p) => {
      Reflect.deleteProperty(p.native, "terminalMessageId");
    }),
    reason: "unbound report",
  },
  {
    name: "invalid hash format",
    input: changedPass((p) => {
      p.binding.revisionHash = "A".repeat(64);
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate input reference",
    input: changedPass((p) => {
      p.inputManifest.sources.push(passReference());
    }),
    reason: "invalid record",
  },
  {
    name: "duplicate origin",
    input: changedPass((p) => {
      p.access.origins.push({ ...p.access.origins[0]! });
    }),
    reason: "invalid record",
  },
  {
    name: "unknown nested field",
    input: changedPass((p) => {
      Object.assign(p.binding, { extra: true });
    }),
    reason: "invalid record",
  },
  {
    name: "null optional field",
    input: { ...passExample(), native: null },
    reason: "invalid record",
  },
  {
    name: "readback missing actual inputs",
    input: {
      ...passExample(),
      role: "readback",
      report: {
        kind: "readback",
        buildDescription: "Build",
        inferredIntent: "Intent",
        discrepancies: [],
        questions: [],
      },
    },
    reason: "invalid record",
  },
];
const encodePlanningPass = Schema.encodeEffect(PlanningPassV1);
describe("planning pass", () => {
  it.effect.each(passCases)("$name", ({ input, reason }) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(decodePlanningPassV1(input));
      if (reason) expect(result).toMatchObject({ _tag: "Failure", failure: { reason } });
      else {
        expect(result).toMatchObject({ _tag: "Success", success: input });
        if (result._tag === "Success")
          expect(yield* encodePlanningPass(result.success)).toEqual(input);
      }
    }),
  );
});
