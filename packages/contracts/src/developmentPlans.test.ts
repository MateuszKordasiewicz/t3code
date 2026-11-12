import { describe, expect, it } from "@effect/vitest";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import { decodeDevelopmentPlanDocumentV1 } from "./developmentPlans.ts";

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
