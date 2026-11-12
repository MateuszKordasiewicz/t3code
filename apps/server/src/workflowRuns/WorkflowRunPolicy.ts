import {
  isWorkflowTimestampAfter,
  type WorkflowOperationV1,
  type WorkflowRunV1,
} from "@t3tools/contracts";

type CommonCommand = {
  readonly commandId: string;
  readonly fingerprint: string;
  readonly expectedVersion: number;
  readonly expectedGeneration: number;
  readonly expectedSpecHash: string;
};

type Binding = {
  readonly specHash: string;
  readonly itemId?: string;
  readonly attemptId?: string;
  readonly candidateHash: string;
  readonly configurationHash: string;
  readonly policyHash: string;
};

type Command = CommonCommand &
  (
    | {
        readonly kind:
          | "pause"
          | "stop"
          | "deadline"
          | "custodySettled"
          | "resume"
          | "observeUnknown";
      }
    | {
        readonly kind: "admitAttempt";
        readonly operationId: WorkflowOperationV1["id"];
        readonly operationFingerprint: string;
        readonly attemptId: string;
        readonly itemId: string;
        readonly repair: boolean;
      }
    | { readonly kind: "advance"; readonly binding: Binding; readonly finalCombined: boolean }
    | { readonly kind: "complete" }
    | {
        readonly kind: "approveAmendment";
        readonly proposal: {
          readonly proposalHash: string;
          readonly previousSpecHash: string;
          readonly newSpecRef: WorkflowRunV1["spec"]["ref"];
          readonly newPlanBinding: WorkflowRunV1["spec"]["plan"];
          readonly limits: Pick<
            WorkflowRunV1["allowance"],
            "maxAttempts" | "maxRepairRounds" | "maxActiveWriters" | "deadline"
          >;
          readonly affectedEvidence: readonly string[] | "unknown";
        };
        readonly actor: { readonly kind: "human" | "agent" | "system"; readonly id: string };
        readonly approvedProposalHash: string;
      }
  );

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
  readonly operationReceipt?: {
    readonly operationId: WorkflowOperationV1["id"];
    readonly fingerprint: string;
    readonly receiptId: string;
  };
  readonly orderedItemIds?: readonly string[];
  readonly acceptedItemIds?: readonly string[];
  readonly predecessorAccepted?: boolean;
  readonly knownAttemptIds?: readonly string[];
  readonly knownOperationIds?: readonly WorkflowOperationV1["id"][];
  readonly currentAttemptState?:
    | "none"
    | "active"
    | "settled_failed"
    | "settled_rejected"
    | "settled_succeeded"
    | "unknown";
  readonly currentAttemptItemId?: string;
  readonly currentBinding?: Binding;
  readonly acceptance?: {
    readonly binding: Binding;
    readonly verdict: "accepted" | "rejected" | "inconclusive";
    readonly evidenceAvailable: boolean;
    readonly independent: boolean;
    readonly finalCombined: boolean;
  };
  readonly finalAccepted?: boolean;
  readonly publicationSettled?: boolean;
  readonly requiredCiPassed?: boolean;
  readonly publishedHead?: string;
  readonly ciHead?: string;
  readonly amendmentApproval?: {
    readonly actor: { readonly kind: "human" | "agent" | "system"; readonly id: string };
    readonly approvedProposalHash: string;
    readonly runId: WorkflowRunV1["id"];
    readonly recordVersion: number;
    readonly previousSpecHash: string;
    readonly newSpecHash: string;
  };
  readonly implementerId?: string;
  readonly custody: "none" | "active" | "unknown";
  readonly activeWriterCount: number;
  readonly heldOperations: readonly HeldOperation[];
  readonly capabilitiesCurrent: boolean;
  readonly compatibilityCurrent: boolean;
  readonly fingerprintCurrent: boolean;
  readonly originGrantCurrent: boolean;
};

type Action =
  | {
      readonly kind: "invalidate_evidence";
      readonly scope: "all" | "selected";
      readonly evidenceIds: readonly string[];
    }
  | { readonly kind: "hold_unstarted" }
  | { readonly kind: "cancel_owned" }
  | {
      readonly kind: "reserve_attempt";
      readonly operationId: WorkflowOperationV1["id"];
      readonly fingerprint: string;
      readonly attemptId: string;
      readonly itemId: string;
      readonly repair: boolean;
    }
  | { readonly kind: "accept_item" | "accept_final"; readonly binding: Binding }
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
    command.kind === "admitAttempt" &&
    facts.operationReceipt?.operationId === command.operationId
  ) {
    return facts.operationReceipt.fingerprint === command.operationFingerprint
      ? { kind: "replay", receiptId: facts.operationReceipt.receiptId }
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
    allowance = run.allowance,
  ): Decision => ({
    kind: "accepted",
    run: {
      ...run,
      status,
      controlGeneration: generation,
      recordVersion: run.recordVersion + 1,
      allowance,
    },
    actions,
  });
  const ordered = facts.orderedItemIds;
  const acceptedIds = facts.acceptedItemIds;
  const validPrefix =
    ordered !== undefined &&
    ordered.length > 0 &&
    new Set(ordered).size === ordered.length &&
    acceptedIds !== undefined &&
    acceptedIds.length <= ordered.length &&
    acceptedIds.every((id, i) => id === ordered[i]);
  const sameBinding = (a: Binding, b: Binding) =>
    a.specHash === b.specHash &&
    a.itemId === b.itemId &&
    a.attemptId === b.attemptId &&
    a.candidateHash === b.candidateHash &&
    a.configurationHash === b.configurationHash &&
    a.policyHash === b.policyHash;
  const terminal = run.status === "stopped" || run.status === "completed";
  switch (command.kind) {
    case "approveAmendment": {
      if (run.status !== "active" && run.status !== "paused" && run.status !== "needs_attention")
        return reject("invalid_state");
      const approval = facts.amendmentApproval;
      if (
        !facts.authority ||
        !facts.implementerId ||
        !approval ||
        command.actor.kind !== "human" ||
        !command.actor.id ||
        command.actor.id === facts.implementerId ||
        approval.actor.kind !== "human" ||
        approval.actor.id !== command.actor.id ||
        approval.runId !== run.id ||
        approval.recordVersion !== run.recordVersion ||
        approval.approvedProposalHash !== command.proposal.proposalHash ||
        command.approvedProposalHash !== command.proposal.proposalHash ||
        approval.previousSpecHash !== run.spec.ref.sha256 ||
        approval.newSpecHash !== command.proposal.newSpecRef.sha256
      )
        return reject("unauthorized");
      if (!isWorkflowTimestampAfter(run.allowance.deadline, facts.now)) return reject("deadline");
      if (facts.custody !== "none" || facts.activeWriterCount !== 0) return reject("custody");
      const { proposal } = command;
      const { limits } = proposal;
      if (
        proposal.previousSpecHash !== run.spec.ref.sha256 ||
        !Number.isSafeInteger(limits.maxAttempts) ||
        limits.maxAttempts <= 0 ||
        limits.maxAttempts < run.allowance.attemptsConsumed ||
        !Number.isSafeInteger(limits.maxRepairRounds) ||
        limits.maxRepairRounds < 0 ||
        limits.maxRepairRounds < run.allowance.repairRoundsConsumed ||
        limits.maxRepairRounds > limits.maxAttempts ||
        limits.maxActiveWriters !== 1 ||
        limits.deadline !== run.allowance.deadline ||
        !Number.isSafeInteger(run.amendmentSequence + 1) ||
        !Number.isSafeInteger(run.controlGeneration + 1)
      )
        return reject("invalid_amendment");
      return {
        kind: "accepted",
        run: {
          ...run,
          recordVersion: run.recordVersion + 1,
          controlGeneration: run.controlGeneration + 1,
          amendmentSequence: run.amendmentSequence + 1,
          priorSpecHash: run.spec.ref.sha256,
          spec: { ref: proposal.newSpecRef, plan: proposal.newPlanBinding },
          allowance: { ...run.allowance, ...limits },
        },
        actions: [
          {
            kind: "invalidate_evidence",
            scope: proposal.affectedEvidence === "unknown" ? "all" : "selected",
            evidenceIds:
              proposal.affectedEvidence === "unknown" ? [] : [...proposal.affectedEvidence],
          },
          { kind: "hold_unstarted" },
        ],
      };
    }
    case "admitAttempt": {
      if (run.status !== "active") return reject("invalid_state");
      if (!facts.authority) return reject("unauthorized");
      if (!isWorkflowTimestampAfter(run.allowance.deadline, facts.now)) return reject("deadline");
      if (
        run.allowance.attemptsConsumed >= run.allowance.maxAttempts ||
        !Number.isSafeInteger(run.allowance.attemptsConsumed + 1) ||
        (command.repair &&
          (run.allowance.repairRoundsConsumed >= run.allowance.maxRepairRounds ||
            !Number.isSafeInteger(run.allowance.repairRoundsConsumed + 1)))
      )
        return reject("exhausted");
      if (
        facts.custody === "unknown" ||
        facts.activeWriterCount !== 0 ||
        facts.currentAttemptState === "active" ||
        facts.currentAttemptState === "unknown"
      )
        return reject("custody");
      if (
        !facts.capabilitiesCurrent ||
        !facts.compatibilityCurrent ||
        !facts.fingerprintCurrent ||
        !facts.originGrantCurrent
      )
        return reject("freshness");
      if (
        !validPrefix ||
        !facts.predecessorAccepted ||
        ordered[acceptedIds.length] !== command.itemId
      )
        return reject("ordering");
      if (
        !facts.knownAttemptIds ||
        !facts.knownOperationIds ||
        facts.knownAttemptIds.includes(command.attemptId) ||
        facts.knownOperationIds.includes(command.operationId)
      )
        return reject("integrity");
      if (
        command.repair
          ? facts.currentAttemptItemId !== command.itemId ||
            (facts.currentAttemptState !== "settled_failed" &&
              facts.currentAttemptState !== "settled_rejected")
          : facts.currentAttemptState !== "none"
      )
        return reject("ordering");
      return accept(
        "active",
        run.controlGeneration,
        [
          {
            kind: "reserve_attempt",
            operationId: command.operationId,
            fingerprint: command.operationFingerprint,
            attemptId: command.attemptId,
            itemId: command.itemId,
            repair: command.repair,
          },
        ],
        {
          ...run.allowance,
          attemptsConsumed: run.allowance.attemptsConsumed + 1,
          repairRoundsConsumed: run.allowance.repairRoundsConsumed + (command.repair ? 1 : 0),
        },
      );
    }
    case "advance": {
      if (run.status !== "active" || (command.finalCombined && facts.finalAccepted))
        return reject("invalid_state");
      if (!facts.authority) return reject("unauthorized");
      if (!isWorkflowTimestampAfter(run.allowance.deadline, facts.now)) return reject("deadline");
      if (facts.custody !== "none") return reject("custody");
      if (
        !validPrefix ||
        (command.finalCombined
          ? acceptedIds.length !== ordered.length
          : ordered[acceptedIds.length] !== command.binding.itemId)
      )
        return reject("ordering");
      const evidence = facts.acceptance;
      if (
        !facts.currentBinding ||
        !evidence ||
        (command.finalCombined && facts.finalAccepted !== false) ||
        (command.finalCombined
          ? command.binding.itemId !== undefined || command.binding.attemptId !== undefined
          : command.binding.itemId === undefined || command.binding.attemptId === undefined) ||
        command.binding.specHash !== run.spec.ref.sha256 ||
        !sameBinding(command.binding, facts.currentBinding) ||
        !sameBinding(command.binding, evidence.binding) ||
        evidence.finalCombined !== command.finalCombined ||
        evidence.verdict !== "accepted" ||
        !evidence.evidenceAvailable ||
        !evidence.independent
      )
        return reject("evidence");
      return accept("active", run.controlGeneration, [
        { kind: command.finalCombined ? "accept_final" : "accept_item", binding: command.binding },
      ]);
    }
    case "complete":
      if (run.status !== "active") return reject("invalid_state");
      if (!facts.authority) return reject("unauthorized");
      if (facts.custody !== "none") return reject("custody");
      if (
        !facts.finalAccepted ||
        !facts.publicationSettled ||
        !facts.requiredCiPassed ||
        !facts.publishedHead ||
        facts.publishedHead !== facts.ciHead
      )
        return reject("evidence");
      return accept("completed", run.controlGeneration, []);
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
