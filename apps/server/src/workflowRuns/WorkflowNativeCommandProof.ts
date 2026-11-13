import {
  CommandId,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  ProjectId,
  Sha256Hex,
  ThreadId,
} from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import { ServerConfig } from "../config.ts";
import { buildInitialMessageCommand } from "../orchestration-v2/ThreadLaunchCommands.ts";
import type {
  ThreadLaunchInput,
  ThreadLaunchInitialMessage,
} from "../orchestration-v2/ThreadLaunchService.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import * as Derivation from "./WorkflowNativeLaunchDerivation.ts";
import { assertMatchesDerived, snapshotNativeInput } from "./WorkflowNativeCommandSnapshot.ts";
import * as Store from "./WorkflowRunStore.ts";
import { readNativeEvidenceStrict } from "./WorkflowNativeDispatchEvidence.ts";

const Normalization = Schema.Struct({
  version: Schema.Literal(1),
  project: Schema.Struct({ projectId: ProjectId, workspaceRoot: Schema.NonEmptyString }),
  scratchRoot: Schema.NonEmptyString,
  workspace: Schema.Struct({
    requested: OrchestrationV2ThreadLaunchWorkspaceStrategy,
    effective: OrchestrationV2ThreadLaunchWorkspaceStrategy,
    reason: Schema.Literal("unchanged"),
  }),
  reuse: Schema.Struct({
    requested: Schema.Boolean,
    claimKind: Schema.Literals(["create", "metadata-update"]),
  }),
  attachments: Schema.Struct({ kind: Schema.Literal("none") }),
});
const Proof = Schema.Struct({
  version: Schema.Literal(1),
  intentId: Schema.NonEmptyString,
  intentFingerprint: Sha256Hex,
  materialHash: Sha256Hex,
  launchHash: Sha256Hex,
  normalization: Normalization,
  normalizationHash: Sha256Hex,
  phase: Schema.Literals(["claim", "message"]),
  commandId: CommandId,
  commandType: Schema.Literals(["thread.create", "thread.metadata.update", "message.dispatch"]),
  threadId: ThreadId,
  actualCommandHash: Sha256Hex,
});
const decodeProof = Schema.decodeUnknownSync(Proof);
const encodeProof = Schema.encodeSync(Proof);
const decodeProofJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

export class NativeCommandProofError extends Schema.TaggedError<NativeCommandProofError>()(
  "NativeCommandProofError",
  { reason: Schema.Literals(["invalid", "conflict", "integrity", "persistence"]) },
) {}

export function encodeNativeCommandProof(value: unknown) {
  try {
    WorkflowCanonicalHash.assertCanonical(value);
    const original = stableStringify(value);
    const owned: unknown = JSON.parse(original);
    const decoded = decodeProof(owned, { onExcessProperty: "error" });
    const encoded = encodeProof(decoded, { onExcessProperty: "error" });
    WorkflowCanonicalHash.assertCanonical(decoded);
    WorkflowCanonicalHash.assertCanonical(encoded);
    if (stableStringify(decoded) !== original || stableStringify(encoded) !== original)
      throw new Error();
    const n = decoded.normalization;
    if (
      stableStringify(n.workspace.requested) !== stableStringify(n.workspace.effective) ||
      n.reuse.claimKind !== (n.reuse.requested ? "metadata-update" : "create") ||
      decoded.normalizationHash !==
        WorkflowCanonicalHash.tagged("t3.workflow-native-normalization.v1", n) ||
      (decoded.phase === "message"
        ? decoded.commandType !== "message.dispatch"
        : decoded.commandType !== (n.reuse.requested ? "thread.metadata.update" : "thread.create"))
    )
      throw new Error();
    return Object.freeze({
      proofJson: original,
      proofHash: WorkflowCanonicalHash.tagged("t3.workflow-native-command-proof.v1", decoded),
    });
  } catch {
    throw new NativeCommandProofError({ reason: "invalid" });
  }
}

export function makeNativeCommandProofVerifier(
  sql: SqlClient.SqlClient,
  config: ServerConfig["Service"],
  path: Path.Path,
) {
  const ownedConfig = { ...config, baseDir: config.baseDir };
  return Effect.gen(function* () {
    const store = yield* Store.make().pipe(Effect.provideService(SqlClient.SqlClient, sql));
    const derivation = yield* Derivation.make.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.provideService(Store.WorkflowRunStore, store),
      Effect.provideService(ServerConfig, ownedConfig),
      Effect.provideService(Path.Path, path),
    );
    const derive = Effect.fn("WorkflowNativeCommandProof.derive")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      const derived = yield* derivation
        .deriveInTransaction({ intentId: snapshot.hint.intentId })
        .pipe(
          Effect.mapError(
            (error) =>
              new NativeCommandProofError({
                reason: SqlError.isSqlError(error)
                  ? "persistence"
                  : error.reason === "stale"
                    ? "conflict"
                    : error.reason,
              }),
          ),
        );
      yield* Effect.try({
        try: () => assertMatchesDerived(snapshot, derived),
        catch: () => new NativeCommandProofError({ reason: "conflict" }),
      });
      return derived;
    });
    type Derived = Effect.Success<ReturnType<typeof derive>>;
    const candidate = (
      derived: Derived,
      phase: "claim" | "message",
      retained?: typeof Normalization.Type,
    ) => {
      const launch = derived.reservation.material.launch;
      const command =
        phase === "claim"
          ? derived.claimCommand
          : retained
            ? buildInitialMessageCommand(
                launch as ThreadLaunchInput,
                launch.initialMessage as ThreadLaunchInitialMessage,
                CommandId.make(`${launch.commandId}:initial-message`),
                launch.threadId,
                launch.initialMessage.messageId,
                retained.workspace.effective,
              )
            : derived.messageCommand;
      const snapshot = snapshotNativeInput(command, {
        intentId: derived.reservation.intentId,
        phase,
      });
      return encodeNativeCommandProof({
        version: 1,
        intentId: derived.reservation.intentId,
        intentFingerprint: derived.reservation.fingerprint,
        materialHash: derived.reservation.materialHash,
        launchHash: derived.reservation.launchHash,
        normalization: derived.normalization,
        normalizationHash: derived.normalizationHash,
        phase,
        commandId: command.commandId,
        commandType: command.type,
        threadId: command.threadId,
        actualCommandHash: snapshot.commandHash,
      });
    };
    const prepare = Effect.fn("WorkflowNativeCommandProof.prepareInTransaction")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      const derived = yield* derive(snapshot);
      return yield* Effect.try({
        try: () => candidate(derived, snapshot.hint.phase),
        catch: () => new NativeCommandProofError({ reason: "invalid" }),
      });
    });
    const read = Effect.fn("WorkflowNativeCommandProof.readStoredInTransaction")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new NativeCommandProofError({ reason: "integrity" });
      const derived = yield* derive(snapshot);
      const claimId = derived.claimCommand.commandId;
      const messageId = derived.messageCommand.commandId;
      const intentId = derived.reservation.intentId;
      const proofs = yield* sql<{
        command_id: string;
        intent_id: string;
        phase: string;
        proof_json: string;
        proof_hash: string;
      }>`SELECT command_id, intent_id, phase, proof_json, proof_hash FROM workflow_native_command_proofs WHERE intent_id = ${intentId} OR command_id = ${claimId} OR command_id = ${messageId}`.pipe(
        Effect.mapError(() => new NativeCommandProofError({ reason: "persistence" })),
      );
      const receipts = yield* sql<{
        command_id: string;
        aggregate_kind: string;
        aggregate_id: string;
      }>`SELECT command_id, aggregate_kind, aggregate_id FROM orchestration_command_receipts WHERE command_id = ${claimId} OR command_id = ${messageId}`.pipe(
        Effect.mapError(() => new NativeCommandProofError({ reason: "persistence" })),
      );
      if (
        proofs.some((row) => row.intent_id !== intentId) ||
        receipts.some(
          (row) =>
            row.aggregate_kind !== "thread" || row.aggregate_id !== derived.claimCommand.threadId,
        )
      )
        return yield* new NativeCommandProofError({ reason: "conflict" });
      if (proofs.some((row) => row.command_id !== claimId && row.command_id !== messageId))
        return yield* new NativeCommandProofError({ reason: "integrity" });
      const claimProof = proofs.filter((row) => row.command_id === claimId);
      const messageProof = proofs.filter((row) => row.command_id === messageId);
      const claimReceipt = receipts.filter((row) => row.command_id === claimId);
      const messageReceipt = receipts.filter((row) => row.command_id === messageId);
      if ([claimProof, messageProof, claimReceipt, messageReceipt].some((rows) => rows.length > 1))
        return yield* new NativeCommandProofError({ reason: "integrity" });
      if (
        !claimProof.length &&
        !claimReceipt.length &&
        !messageProof.length &&
        !messageReceipt.length
      )
        return snapshot.hint.phase === "claim"
          ? { kind: "absent" as const }
          : yield* new NativeCommandProofError({ reason: "integrity" });
      if (
        !claimProof.length ||
        !claimReceipt.length ||
        Boolean(messageProof.length) !== Boolean(messageReceipt.length)
      )
        return yield* new NativeCommandProofError({ reason: "integrity" });
      const verify = Effect.fn("WorkflowNativeCommandProof.verifyPhase")(function* (
        phase: "claim" | "message",
        row: (typeof proofs)[number],
        retained?: typeof Normalization.Type,
      ) {
        const parsed = yield* Effect.try({
          try: () => {
            const value = decodeProofJson(row.proof_json);
            const encoded = encodeNativeCommandProof(value);
            const proof = decodeProof(value, { onExcessProperty: "error" });
            if (
              encoded.proofJson !== row.proof_json ||
              encoded.proofHash !== row.proof_hash ||
              row.phase !== phase ||
              row.command_id !== proof.commandId ||
              row.intent_id !== proof.intentId ||
              proof.phase !== phase
            )
              throw new Error();
            return proof;
          },
          catch: () => new NativeCommandProofError({ reason: "integrity" }),
        });
        if (
          stableStringify(parsed.normalization) !== stableStringify(derived.normalization) ||
          parsed.normalizationHash !== derived.normalizationHash
        )
          return yield* new NativeCommandProofError({ reason: "conflict" });
        const expected = yield* Effect.try({
          try: () => candidate(derived, phase, retained),
          catch: () => new NativeCommandProofError({ reason: "integrity" }),
        });
        if (row.proof_json !== expected.proofJson || row.proof_hash !== expected.proofHash)
          return yield* new NativeCommandProofError({ reason: "integrity" });
        const evidence = yield* readNativeEvidenceStrict(sql, {
          phase,
          claimType: derived.claimCommand.type,
          launchCommandId: claimId,
          threadId: derived.claimCommand.threadId,
          projectId: derived.reservation.material.launch.projectId,
          messageId: derived.reservation.material.launch.initialMessage.messageId,
        }).pipe(Effect.mapError(() => new NativeCommandProofError({ reason: "persistence" })));
        if (evidence.kind !== "verified")
          return yield* new NativeCommandProofError({ reason: "integrity" });
        return { output: { ...expected, ...evidence }, normalization: parsed.normalization };
      });
      const claim = yield* verify("claim", claimProof[0]!);
      const message = messageProof[0]
        ? yield* verify("message", messageProof[0], claim.normalization)
        : undefined;
      const firstMessageSequence = message?.output.events[0]?.sequence;
      if (
        message &&
        (firstMessageSequence === undefined || firstMessageSequence <= claim.output.receiptSequence)
      )
        return yield* new NativeCommandProofError({ reason: "integrity" });
      if (snapshot.hint.phase === "claim") return claim.output;
      return message?.output ?? { kind: "absent" as const };
    });
    return {
      prepareInTransaction(command: unknown, hint: unknown) {
        try {
          return prepare(snapshotNativeInput(command, hint));
        } catch {
          return Effect.fail(new NativeCommandProofError({ reason: "invalid" }));
        }
      },
      readStoredInTransaction(command: unknown, hint: unknown) {
        try {
          return read(snapshotNativeInput(command, hint));
        } catch {
          return Effect.fail(new NativeCommandProofError({ reason: "invalid" }));
        }
      },
    };
  });
}
