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
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import { ServerConfig } from "../config.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import * as Derivation from "./WorkflowNativeLaunchDerivation.ts";
import {
  assertMatchesDerived,
  NativeCommandSnapshotError,
  snapshotNativeInput,
} from "./WorkflowNativeCommandSnapshot.ts";
import * as Store from "./WorkflowRunStore.ts";

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
    const prepare = Effect.fn("WorkflowNativeCommandProof.prepareInTransaction")(function* (
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
      return yield* Effect.try({
        try: () => {
          assertMatchesDerived(snapshot, derived);
          return encodeNativeCommandProof({
            version: 1,
            intentId: derived.reservation.intentId,
            intentFingerprint: derived.reservation.fingerprint,
            materialHash: derived.reservation.materialHash,
            launchHash: derived.reservation.launchHash,
            normalization: derived.normalization,
            normalizationHash: derived.normalizationHash,
            phase: snapshot.hint.phase,
            commandId: snapshot.command.commandId,
            commandType: snapshot.command.type,
            threadId: snapshot.command.threadId,
            actualCommandHash: snapshot.commandHash,
          });
        },
        catch: (error) =>
          error instanceof NativeCommandSnapshotError
            ? new NativeCommandProofError({ reason: error.reason })
            : new NativeCommandProofError({ reason: "invalid" }),
      });
    });
    return {
      prepareInTransaction(command: unknown, hint: unknown) {
        try {
          return prepare(snapshotNativeInput(command, hint));
        } catch {
          return Effect.fail(new NativeCommandProofError({ reason: "invalid" }));
        }
      },
    };
  });
}
