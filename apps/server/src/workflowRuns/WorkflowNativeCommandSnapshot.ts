import { OrchestrationV2Command } from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Schema from "effect/Schema";

import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";

const commandTag = "t3.workflow-native-command.v1";
const decodeCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const encodeCommand = Schema.encodeSync(OrchestrationV2Command);
type NativeCommand = Extract<
  OrchestrationV2Command,
  { type: "thread.create" | "thread.metadata.update" | "message.dispatch" }
>;
type Hint = { readonly intentId: string; readonly phase: "claim" | "message" };
type Derived = {
  readonly reservation: { readonly intentId: string };
  readonly claimCommand: NativeCommand;
  readonly messageCommand: NativeCommand;
  readonly expectedClaimCommandHash: string;
  readonly expectedMessageCommandHash: string;
};

export interface OwnedNativeInput {
  readonly command: NativeCommand;
  readonly hint: Hint;
  readonly canonicalCommand: string;
  readonly commandHash: string;
}

// @effect-diagnostics-next-line extendsNativeError:off -- this synchronous boundary throws rather than using an Effect failure channel.
export class NativeCommandSnapshotError extends Error {
  readonly reason: "invalid" | "conflict";
  constructor(reason: "invalid" | "conflict") {
    super(`Native command snapshot ${reason}`);
    this.name = "NativeCommandSnapshotError";
    this.reason = reason;
  }
}

function freeze(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  for (const child of Object.values(value)) freeze(child);
  Object.freeze(value);
}

export function snapshotNativeInput(command: unknown, hint: unknown): OwnedNativeInput {
  try {
    WorkflowCanonicalHash.assertCanonical(command);
    WorkflowCanonicalHash.assertCanonical(hint);
    const canonicalCommand = stableStringify(command);
    const ownedCommand: unknown = JSON.parse(canonicalCommand);
    const ownedHint: unknown = JSON.parse(stableStringify(hint));
    if (
      typeof ownedHint !== "object" ||
      ownedHint === null ||
      Array.isArray(ownedHint) ||
      Object.keys(ownedHint).length !== 2 ||
      !("intentId" in ownedHint) ||
      !("phase" in ownedHint) ||
      typeof ownedHint.intentId !== "string" ||
      !ownedHint.intentId.trim() ||
      (ownedHint.phase !== "claim" && ownedHint.phase !== "message")
    )
      throw new Error();
    const decoded = decodeCommand(ownedCommand, {
      onExcessProperty: "error",
    });
    const encoded = encodeCommand(decoded, {
      onExcessProperty: "error",
    });
    WorkflowCanonicalHash.assertCanonical(decoded);
    WorkflowCanonicalHash.assertCanonical(encoded);
    if (
      stableStringify(decoded) !== canonicalCommand ||
      stableStringify(encoded) !== canonicalCommand
    )
      throw new Error();
    if (
      ownedHint.phase === "claim"
        ? decoded.type !== "thread.create" && decoded.type !== "thread.metadata.update"
        : decoded.type !== "message.dispatch"
    )
      throw new Error();
    const result: OwnedNativeInput = {
      command: decoded as NativeCommand,
      hint: { intentId: ownedHint.intentId, phase: ownedHint.phase },
      canonicalCommand,
      commandHash: WorkflowCanonicalHash.tagged(commandTag, decoded),
    };
    freeze(result);
    return result;
  } catch {
    throw new NativeCommandSnapshotError("invalid");
  }
}

export function assertMatchesDerived(snapshot: OwnedNativeInput, derived: Derived): void {
  try {
    const actual = snapshotNativeInput(snapshot.command, snapshot.hint);
    if (
      actual.canonicalCommand !== snapshot.canonicalCommand ||
      actual.commandHash !== snapshot.commandHash ||
      actual.hint.intentId !== derived.reservation.intentId
    )
      throw new Error();
    const expected = snapshotNativeInput(
      actual.hint.phase === "claim" ? derived.claimCommand : derived.messageCommand,
      actual.hint,
    );
    const originalHash =
      actual.hint.phase === "claim"
        ? derived.expectedClaimCommandHash
        : derived.expectedMessageCommandHash;
    if (
      expected.commandHash !== originalHash ||
      expected.commandHash !== actual.commandHash ||
      expected.canonicalCommand !== actual.canonicalCommand
    )
      throw new Error();
  } catch {
    throw new NativeCommandSnapshotError("conflict");
  }
}
