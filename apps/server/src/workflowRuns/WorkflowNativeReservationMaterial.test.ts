import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import {
  decodeNativeReservationLaunch,
  decodeNativeReservationMaterial,
} from "./WorkflowNativeReservationMaterial.ts";

const launch = {
  commandId: "command-1",
  threadId: "thread-1",
  projectId: "project-1",
  title: "Work",
  modelSelection: {
    instanceId: "codex",
    model: "model",
    options: [{ id: "effort", value: "high" }],
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  workspaceStrategy: { type: "root", branch: "main" },
  initialMessage: { messageId: "message-1", text: "Do the work", attachments: [] },
  createdBy: "agent",
  creationSource: "mcp",
};
const session = {
  schemaVersion: 1,
  id: "session",
  environmentId: "environment",
  projectId: "project-1",
  planId: "plan",
  origin: { kind: "human", id: "human" },
  request: "Plan the work",
  mode: "ordinary",
  allowedRoles: ["author"],
  allowedProviders: [{ providerInstanceId: "codex", model: "model" }],
  limits: { maxPasses: 2, maxRepairRounds: 1, deadline: "2026-12-01T00:00:00Z" },
  consumed: { passes: 1, repairRounds: 0 },
  controlGeneration: 0,
  recordVersion: 1,
  status: "active",
  createdAt: "2026-11-01T00:00:00Z",
  access: { origins: [] },
};
const material = {
  version: 1,
  intentId: "intent",
  receiptId: "receipt",
  intentFingerprint: "a".repeat(64),
  launch,
  planningSession: session,
};
const decodeLaunch = decodeNativeReservationLaunch;
const decodeMaterial = decodeNativeReservationMaterial;
const withMessage = (message: object) => ({
  ...launch,
  initialMessage: { ...launch.initialMessage, ...message },
});
const file = {
  type: "file",
  id: "thread-1-123e4567-e89b-12d3-a456-426614174000-txt",
  name: "a.txt",
  mimeType: "text/plain",
  sizeBytes: 1,
};

describe("native reservation material", () => {
  it.effect("retains complete launch data for every workspace and independent origin fields", () =>
    Effect.gen(function* () {
      const full = {
        ...launch,
        reuseExistingThread: false,
        generateTitle: true,
        importedNativeThread: {
          ref: { driver: "codex", nativeId: "native", strength: "strong" },
          metadata: {
            title: null,
            updatedAt: "2026-11-01T00:00:00Z",
            forkHeadMessageId: "native-message",
            modelSelection: launch.modelSelection,
            itemIdentityVersion: 2,
            claudeAttachmentBudget: { bytesUsed: 1, pagesUsed: 2 },
          },
        },
        initialMessage: {
          ...launch.initialMessage,
          scheduledTaskId: "schedule",
          senderThreadId: "sender",
          attachments: [file],
          context: {
            version: 1,
            records: [
              { version: 1, contextId: "c1", kind: "skill", label: "Skill", name: "review" },
              {
                version: 1,
                contextId: "c2",
                kind: "future-kind",
                label: "Future",
                payload: { nested: [1, true, null, { extra: "value" }] },
              },
            ],
          },
        },
        linkOrigin: { sessionId: "link", label: "Link", access: "full-access" },
        delegatedFrom: { environmentId: "remote", threadId: "parent", title: "Parent" },
      };
      for (const workspaceStrategy of [
        launch.workspaceStrategy,
        { type: "existing_worktree", worktreePath: "/work", branch: "feature" },
        { type: "worktree", baseRef: "main", branch: "branch", startFromOrigin: true },
      ]) {
        const input = { ...full, workspaceStrategy };
        const result = yield* decodeLaunch(input);
        expect(result.launch).toEqual(input);
        expect(result.launchHash).toBe(
          WorkflowCanonicalHash.tagged("t3.workflow-native-reservation-launch.v1", input),
        );
      }
      const delegatedOnly = { ...launch, delegatedFrom: full.delegatedFrom };
      const linkOnly = { ...launch, linkOrigin: full.linkOrigin };
      expect((yield* decodeLaunch(delegatedOnly)).launch).toEqual(delegatedOnly);
      expect((yield* decodeLaunch(linkOnly)).launch).toEqual(linkOnly);
      expect((yield* decodeLaunch(launch)).launch).toEqual(launch);
      const reordered = {
        ...launch,
        modelSelection: {
          options: launch.modelSelection.options,
          model: "model",
          instanceId: "codex",
        },
      };
      expect((yield* decodeLaunch(reordered)).launchHash).toBe(
        (yield* decodeLaunch(launch)).launchHash,
      );
    }),
  );

  it.effect("rejects lossy, excess and transformed launch forms", () =>
    Effect.gen(function* () {
      const invalid = [
        { ...launch, threadId: undefined },
        { ...launch, initialMessage: { text: "x", attachments: [] } },
        ...["", "   ", " native "].map((nativeId) => ({
          ...launch,
          importedNativeThread: { ref: { driver: "codex", nativeId, strength: "strong" } },
        })),
        { ...launch, surprise: true },
        { ...launch, workspaceStrategy: { type: "root", extra: 1 } },
        { ...launch, title: " Work " },
        { ...launch, modelSelection: { provider: "codex", model: "model" } },
        {
          ...launch,
          modelSelection: { instanceId: "codex", model: "model", options: { effort: "high" } },
        },
        {
          ...launch,
          importedNativeThread: {
            ref: { driver: "codex", nativeId: "x", strength: "strong" },
            metadata: { modelSelection: { provider: "codex", model: "model" } },
          },
        },
        withMessage({
          context: { version: 1, records: [{ version: 1, contextId: "bad", kind: "image" }] },
        }),
        withMessage({
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "c",
                kind: "skill",
                label: "Skill",
                name: "review",
                extra: true,
              },
            ],
          },
        }),
        withMessage({
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "c",
                kind: "future-kind",
                label: "Future",
                payload: "x".repeat(64_001),
              },
            ],
          },
        }),
      ];
      for (const input of invalid) yield* Effect.flip(decodeLaunch(input));
    }),
  );

  it.effect("preflights unsafe inputs before accessing or transforming them", () =>
    Effect.gen(function* () {
      let accessed = false;
      const getter = {
        ...launch,
        get extra() {
          accessed = true;
          return 1;
        },
      };
      const cyclic: Record<string, unknown> = {};
      cyclic.self = cyclic;
      for (const input of [
        getter,
        { ...launch, extra: cyclic },
        { ...launch, extra: new Map() },
        { ...launch, extra: NaN },
        { ...launch, extra: Number.MAX_SAFE_INTEGER + 1 },
        { ...launch, extra: undefined },
      ]) {
        yield* Effect.flip(decodeLaunch(input));
      }
      expect(accessed).toBe(false);
      let materialAccessed = false;
      const unsafeMaterial = {
        ...material,
        get extra() {
          materialAccessed = true;
          return 1;
        },
      };
      yield* Effect.flip(decodeMaterial(unsafeMaterial));
      expect(materialAccessed).toBe(false);
    }),
  );

  it.effect("enforces attachment aggregate limits and the exact pending-id parser", () =>
    Effect.gen(function* () {
      expect(
        (yield* decodeLaunch(withMessage({ attachments: [file] }))).launch.initialMessage
          .attachments,
      ).toEqual([file]);
      yield* Effect.flip(
        decodeLaunch(
          withMessage({
            attachments: [{ ...file, id: "pending-123e4567-e89b-12d3-a456-426614174000-txt" }],
          }),
        ),
      );
      expect(
        (yield* decodeLaunch(withMessage({ attachments: [{ ...file, id: "pending-looking" }] })))
          .launch.initialMessage.attachments,
      ).toHaveLength(1);
      yield* Effect.flip(
        decodeLaunch(withMessage({ attachments: Array.from({ length: 101 }, () => file) })),
      );
      yield* Effect.flip(
        decodeLaunch(
          withMessage({
            attachments: Array.from({ length: 9 }, (_, i) => ({
              type: "image",
              id: `image-${i}`,
              name: "a.png",
              mimeType: "image/png",
              sizeBytes: 10 * 1024 * 1024,
            })),
          }),
        ),
      );
    }),
  );

  it.effect("validates semantic sessions and strict material, and binds both hashes", () =>
    Effect.gen(function* () {
      const result = yield* decodeMaterial(material);
      expect(result.material).toEqual(material);
      expect(result.launchHash).toBe((yield* decodeLaunch(launch)).launchHash);
      expect(result.materialHash).toBe(
        WorkflowCanonicalHash.tagged("t3.workflow-native-reservation-material.v1", material),
      );
      const reordered = {
        planningSession: session,
        launch,
        intentFingerprint: material.intentFingerprint,
        receiptId: "receipt",
        intentId: "intent",
        version: 1,
      };
      expect((yield* decodeMaterial(reordered)).materialHash).toBe(result.materialHash);
      const changedLaunch = yield* decodeMaterial({
        ...material,
        launch: { ...launch, title: "Other" },
      });
      expect(changedLaunch.launchHash).not.toBe(result.launchHash);
      expect(changedLaunch.materialHash).not.toBe(result.materialHash);
      for (const change of [
        { intentId: "other" },
        { receiptId: "other" },
        { intentFingerprint: "b".repeat(64) },
        { planningSession: null },
      ]) {
        const changed = yield* decodeMaterial({ ...material, ...change });
        expect(changed.launchHash).toBe(result.launchHash);
        expect(changed.materialHash).not.toBe(result.materialHash);
      }
      for (const planningSession of [
        { ...session, consumed: { passes: 3, repairRounds: 0 } },
        { ...session, allowedRoles: ["author", "author"] },
        {
          ...session,
          allowedProviders: [session.allowedProviders[0], session.allowedProviders[0]],
        },
        { ...session, createdAt: "not-a-time" },
        { ...session, limits: { ...session.limits, deadline: "2020-01-01T00:00:00Z" } },
        { ...session, extra: true },
      ])
        yield* Effect.flip(decodeMaterial({ ...material, planningSession }));
      yield* Effect.flip(decodeMaterial({ ...material, extra: true }));
      yield* Effect.flip(decodeMaterial({ ...material, intentFingerprint: "bad" }));
    }),
  );
  it.effect("retains versioned authority material without changing historical hashes", () =>
    Effect.gen(function* () {
      const authority = {
        version: 1,
        kind: "local-rpc-session",
        provenance: "verified-session",
        environmentId: "environment",
        sessionId: "grant-session",
        subject: "human",
        method: "bearer-access-token",
        observedScopes: ["orchestration:read", "orchestration:operate"],
        expiresAt: "2026-12-01T00:00:00.000Z",
        runtimeModeCeiling: null,
        peerLink: false,
        proofKeyThumbprint: null,
      };
      const input = { ...material, version: 2, authority };
      const prior = yield* decodeMaterial(material);
      const current = yield* decodeMaterial(input);
      expect(current.material).toEqual(input);
      expect(current.materialHash).toBe(
        WorkflowCanonicalHash.tagged("t3.workflow-native-reservation-material.v2", input),
      );
      expect(current.authorityHash).toBe(
        WorkflowCanonicalHash.tagged("t3.workflow-execution-authority.v1", authority),
      );
      expect(current.launchHash).toBe(prior.launchHash);
      expect((yield* decodeMaterial(material)).materialHash).toBe(prior.materialHash);
      for (const bad of [
        { ...input, authority: { ...authority, token: "forbidden" } },
        { ...input, authority: { ...authority, runtimeModeCeiling: "invalid" } },
        { ...input, authority: { ...authority, observedScopes: ["unknown-scope"] } },
        { ...input, version: 3 },
      ])
        yield* Effect.flip(decodeMaterial(bad));
    }),
  );
});
