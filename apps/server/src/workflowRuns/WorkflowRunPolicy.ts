import {
  isWorkflowTimestampAfter,
  type WorkflowOperationV1,
  type WorkflowRunV1,
} from "@t3tools/contracts";

type Command = {
  readonly kind: "pause" | "stop" | "deadline" | "custodySettled" | "resume" | "observeUnknown";
  readonly commandId: string;
  readonly fingerprint: string;
  readonly expectedVersion: number;
  readonly expectedGeneration: number;
  readonly expectedSpecHash: string;
};

type HeldOperation = Pick<WorkflowOperationV1, "id" | "fingerprint" | "status"> & {
  readonly provablyUnstarted: boolean;
};

type Facts = {
  readonly now: string;
  readonly authority: boolean;
  readonly commandReceipt?: {
    readonly commandId: string;
    readonly fingerprint: string;
    readonly receiptId: string;
  };
  readonly custody: "none" | "active" | "unknown";
  readonly activeWriterCount: number;
  readonly heldOperations: readonly HeldOperation[];
  readonly capabilitiesCurrent: boolean;
  readonly compatibilityCurrent: boolean;
  readonly fingerprintCurrent: boolean;
  readonly originGrantCurrent: boolean;
};

type Action =
  | { readonly kind: "hold_unstarted" }
  | { readonly kind: "cancel_owned" }
  | {
      readonly kind: "rearm";
      readonly operationId: WorkflowOperationV1["id"];
      readonly fingerprint: WorkflowOperationV1["fingerprint"];
    };

type Reason =
  | "stale"
  | "integrity"
  | "invalid_state"
  | "unauthorized"
  | "deadline"
  | "exhausted"
  | "custody"
  | "freshness"
  | "ordering"
  | "evidence"
  | "invalid_amendment";
type Decision =
  | { readonly kind: "accepted"; readonly run: WorkflowRunV1; readonly actions: readonly Action[] }
  | { readonly kind: "replay"; readonly receiptId: string }
  | { readonly kind: "rejected"; readonly reason: Reason };

const decide = ({
  run,
  command,
  facts,
}: {
  readonly run: WorkflowRunV1;
  readonly command: Command;
  readonly facts: Facts;
}): Decision => {
  const reject = (reason: Reason): Decision => ({ kind: "rejected", reason });
  const receipt = facts.commandReceipt;
  if (receipt?.commandId === command.commandId) {
    return receipt.fingerprint === command.fingerprint
      ? { kind: "replay", receiptId: receipt.receiptId }
      : reject("integrity");
  }
  if (
    command.expectedVersion !== run.recordVersion ||
    command.expectedGeneration !== run.controlGeneration ||
    command.expectedSpecHash !== run.spec.ref.sha256
  )
    return reject("stale");

  const accept = (
    status: WorkflowRunV1["status"],
    generation: number,
    actions: readonly Action[],
  ): Decision => ({
    kind: "accepted",
    run: { ...run, status, controlGeneration: generation, recordVersion: run.recordVersion + 1 },
    actions,
  });
  const terminal = run.status === "stopped" || run.status === "completed";
  switch (command.kind) {
    case "pause": {
      if (run.status !== "active") return reject("invalid_state");
      if (!facts.authority) return reject("unauthorized");
      const status =
        facts.custody === "none"
          ? "paused"
          : facts.custody === "active"
            ? "pausing"
            : "needs_attention";
      return accept(status, run.controlGeneration + 1, [{ kind: "hold_unstarted" }]);
    }
    case "stop":
    case "deadline": {
      if (terminal || (command.kind === "stop" && run.status === "stopping"))
        return reject("invalid_state");
      if (command.kind === "stop" && !facts.authority) return reject("unauthorized");
      if (
        command.kind === "deadline" &&
        isWorkflowTimestampAfter(run.allowance.deadline, facts.now)
      )
        return reject("deadline");
      return accept(facts.custody === "none" ? "stopped" : "stopping", run.controlGeneration + 1, [
        { kind: "hold_unstarted" },
        { kind: "cancel_owned" },
      ]);
    }
    case "custodySettled":
      if (run.status !== "pausing" && run.status !== "stopping") return reject("invalid_state");
      if (facts.custody !== "none") return reject("custody");
      return accept(run.status === "pausing" ? "paused" : "stopped", run.controlGeneration, []);
    case "observeUnknown":
      if (terminal) return reject("invalid_state");
      if (facts.custody !== "unknown") return reject("custody");
      return accept(
        run.status === "stopping" ? "stopping" : "needs_attention",
        run.controlGeneration,
        [],
      );
    case "resume":
      if (run.status !== "paused" && run.status !== "needs_attention")
        return reject("invalid_state");
      if (!facts.authority) return reject("unauthorized");
      if (!isWorkflowTimestampAfter(run.allowance.deadline, facts.now)) return reject("deadline");
      if (run.allowance.attemptsConsumed >= run.allowance.maxAttempts) return reject("exhausted");
      if (
        facts.custody !== "none" ||
        facts.activeWriterCount !== 0 ||
        facts.heldOperations.some(
          (op) =>
            op.status === "claimed" ||
            op.status === "cancelled" ||
            op.status === "unknown" ||
            (op.status === "pending" && !op.provablyUnstarted),
        )
      )
        return reject("custody");
      if (
        !facts.capabilitiesCurrent ||
        !facts.compatibilityCurrent ||
        !facts.fingerprintCurrent ||
        !facts.originGrantCurrent
      )
        return reject("freshness");
      return accept(
        "active",
        run.controlGeneration + 1,
        facts.heldOperations
          .filter((op) => op.status === "pending" && op.provablyUnstarted)
          .map((op) => ({ kind: "rearm", operationId: op.id, fingerprint: op.fingerprint })),
      );
  }
};

export const WorkflowRunPolicy = { decide };
