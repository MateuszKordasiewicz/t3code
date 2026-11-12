import { describe, expect, it } from "@effect/vitest";
import { upgradeDevelopmentPlanDocumentV1 } from "@t3tools/contracts";
import type { DevelopmentPlanRevisionV1, RevisionEnvelope } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";

const payload1 = {
  schemaVersion: 1,
  sources: [],
  statements: [],
  selectionLimitations: [],
  access: {
    origins: [],
  },
};
const canonical1 =
  '{"access":{"origins":[]},"schemaVersion":1,"selectionLimitations":[],"sources":[],"statements":[]}';
const digest1 = "1b697c03819fb262e91d2d9cf64128c3e0a3df6712eb6fde2dadb630d83bd6be";

const payload2 = {
  schemaVersion: 1,
  planId: "plan-1",
  sourcePacketHash: "1b697c03819fb262e91d2d9cf64128c3e0a3df6712eb6fde2dadb630d83bd6be",
  brief: "Outcome",
  requirements: [],
  decisions: [],
  questions: [],
  assumptions: [],
  approach: [],
  workItems: [],
  coverageReceipts: [],
  unmappedMaterial: [],
  access: {
    origins: [],
  },
};
const canonical2 =
  '{"access":{"origins":[]},"approach":[],"assumptions":[],"brief":"Outcome","coverageReceipts":[],"decisions":[],"planId":"plan-1","questions":[],"requirements":[],"schemaVersion":1,"sourcePacketHash":"1b697c03819fb262e91d2d9cf64128c3e0a3df6712eb6fde2dadb630d83bd6be","unmappedMaterial":[],"workItems":[]}';
const digest2 = "dae534d3c8dc391af06520337b58ca8a7f2310dc6e6b4de0d72362bc7ae53f86";

const payload3 = {
  actor: {
    kind: "human",
    id: "actor-1",
  },
  destination: {
    environmentId: "env-1",
    projectId: "project-1",
  },
  method: "unlinkThread",
  input: {
    planId: "plan-1",
    expectedVersion: 1,
    threadId: "thread-1",
  },
};
const canonical3 =
  '{"actor":{"id":"actor-1","kind":"human"},"destination":{"environmentId":"env-1","projectId":"project-1"},"input":{"expectedVersion":1,"planId":"plan-1","threadId":"thread-1"},"method":"unlinkThread"}';
const digest3 = "b36ada9bbe107426582daf257cae0ed6549c11eafeb6f25d0dedda3f55df0100";

const sourcePacket = (value: unknown) => value as RevisionEnvelope["sourcePacket"];
const revision = (value: unknown) => value as DevelopmentPlanRevisionV1;

describe("DevelopmentPlanHash", () => {
  it.each([
    [payload1, canonical1, digest1, "source"],
    [payload2, canonical2, digest2, "revision"],
    [payload3, canonical3, digest3, "command"],
  ])("matches canonical vector %#", (payload, canonical, digest, kind) => {
    expect(stableStringify(payload)).toBe(canonical);
    const actual =
      kind === "source"
        ? DevelopmentPlanHash.sourcePacket(sourcePacket(payload))
        : kind === "revision"
          ? DevelopmentPlanHash.revision(revision(payload))
          : DevelopmentPlanHash.command(payload3);
    expect(actual).toBe(digest);
  });

  it("preserves source metadata and ordering while omitting only bytes", () => {
    const source = {
      id: "one",
      bytesBase64: "WA==",
      byteLength: 1,
      sha256: "hash",
      metadata: "first",
    };
    const packet = { ...payload1, sources: [source, { ...source, id: "two" }] };
    const hash = DevelopmentPlanHash.sourcePacket(sourcePacket(packet));
    expect(
      DevelopmentPlanHash.sourcePacket(
        sourcePacket({ ...packet, sources: [...packet.sources].reverse() }),
      ),
    ).not.toBe(hash);
    expect(
      DevelopmentPlanHash.sourcePacket(
        sourcePacket({
          ...packet,
          sources: [{ ...source, bytesBase64: "different" }, packet.sources[1]],
        }),
      ),
    ).toBe(hash);
    for (const field of ["id", "byteLength", "sha256", "metadata"] as const) {
      expect(
        DevelopmentPlanHash.sourcePacket(
          sourcePacket({
            ...packet,
            sources: [{ ...source, [field]: "changed" }, packet.sources[1]],
          }),
        ),
      ).not.toBe(hash);
    }
  });

  it("ignores only commandId and includes actor, destination, method and input", () => {
    const value = { ...payload3, input: { ...payload3.input, commandId: "one" } };
    const hash = DevelopmentPlanHash.command(value);
    expect(
      DevelopmentPlanHash.command({ ...value, input: { ...value.input, commandId: "two" } }),
    ).toBe(hash);
    expect(
      DevelopmentPlanHash.command({ ...value, input: { ...value.input, expectedVersion: 2 } }),
    ).not.toBe(hash);
    expect(
      DevelopmentPlanHash.command({ ...value, input: { ...value.input, threadId: "other" } }),
    ).not.toBe(hash);
    expect(
      DevelopmentPlanHash.command({ ...value, actor: { ...value.actor, id: "other" } }),
    ).not.toBe(hash);
    expect(
      DevelopmentPlanHash.command({
        ...value,
        destination: { ...value.destination, projectId: "other" },
      }),
    ).not.toBe(hash);
    expect(DevelopmentPlanHash.command({ ...value, method: "other" })).not.toBe(hash);
  });

  it("rejects invalid values without invoking accessors", () => {
    let invoked = false;
    const accessor = Object.defineProperty({}, "x", {
      enumerable: true,
      get() {
        invoked = true;
        return 1;
      },
    });
    for (const bad of [NaN, undefined, accessor]) {
      expect(() =>
        DevelopmentPlanHash.sourcePacket(sourcePacket({ ...payload1, extra: bad })),
      ).toThrow();
      expect(() => DevelopmentPlanHash.revision(revision({ ...payload2, extra: bad }))).toThrow();
      expect(() =>
        DevelopmentPlanHash.command({ ...payload3, input: { ...payload3.input, extra: bad } }),
      ).toThrow();
    }
    expect(invoked).toBe(false);
  });

  it.effect("upgrades the V1 example using the source packet hasher", () =>
    Effect.gen(function* () {
      const example = {
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
      };
      const { native } = yield* upgradeDevelopmentPlanDocumentV1({
        bytes: new TextEncoder().encode(JSON.stringify(example)),
        hashSourcePacket: DevelopmentPlanHash.sourcePacket,
      });
      expect(native.revision.sourcePacketHash).toBe(
        DevelopmentPlanHash.sourcePacket(native.sourcePacket),
      );
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );
});
