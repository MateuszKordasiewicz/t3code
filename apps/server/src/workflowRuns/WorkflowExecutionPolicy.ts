import {
  isWorkflowTimestamp,
  isWorkflowTimestampAfter,
  isWorkflowUint,
  type PlanningSessionV1,
  type WorkflowOperationV1,
  type WorkflowRunV1,
} from "@t3tools/contracts";

type OwnerKind = WorkflowOperationV1["owner"]["kind"];
type Identity = {
  readonly ownerKind: OwnerKind;
  readonly ownerId: string;
  readonly operationId: string;
  readonly threadId: string;
  readonly messageId: string;
  readonly nativeRunId: string;
};
type NativeStartFacts = {
  readonly ownership:
    | { readonly kind: "ordinary" }
    | { readonly kind: "ambiguous" }
    | {
        readonly kind: "owned";
        readonly ownerKind: OwnerKind;
        readonly ownerId: string;
        readonly operationId: string;
        readonly operationStatus: WorkflowOperationV1["status"];
        readonly operationGeneration: number;
        readonly ownerGeneration: number;
        readonly ownerStatus: WorkflowRunV1["status"] | PlanningSessionV1["status"];
        readonly binding: {
          readonly threadId: string;
          readonly messageId: string;
          readonly nativeRunId?: string;
        };
        readonly selected: Identity;
        readonly deadline: string;
        readonly now: string;
        readonly cancelRequested: boolean;
        readonly liveGrant: "allowed" | "denied" | "unavailable";
      };
};
type NativeStartDecision =
  | { readonly kind: "allow"; readonly scope: "ordinary" | "owned" }
  | {
      readonly kind: "deny";
      readonly reason:
        | "ambiguous"
        | "unbound"
        | "identity"
        | "stale_generation"
        | "owner_inactive"
        | "operation_ineligible"
        | "deadline"
        | "authority"
        | "cancel_requested";
    };

const present = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

export const decideNativeStart = ({ ownership }: NativeStartFacts): NativeStartDecision => {
  const deny = (
    reason: Extract<NativeStartDecision, { kind: "deny" }>["reason"],
  ): NativeStartDecision => ({ kind: "deny", reason });
  if (ownership?.kind === "ordinary") return { kind: "allow", scope: "ordinary" };
  if (ownership?.kind !== "owned") return deny("ambiguous");
  const { binding, selected } = ownership;
  if (
    !present(ownership.ownerKind) ||
    !present(ownership.ownerId) ||
    !present(ownership.operationId) ||
    !present(binding?.threadId) ||
    !present(binding?.messageId) ||
    !present(binding?.nativeRunId) ||
    !present(selected?.ownerKind) ||
    !present(selected?.ownerId) ||
    !present(selected?.operationId) ||
    !present(selected?.threadId) ||
    !present(selected?.messageId) ||
    !present(selected?.nativeRunId)
  )
    return deny("unbound");
  if (ownership.ownerKind !== "workflow_run" && ownership.ownerKind !== "planning_session")
    return deny("ambiguous");
  if (
    selected.ownerKind !== ownership.ownerKind ||
    selected.ownerId !== ownership.ownerId ||
    selected.operationId !== ownership.operationId ||
    selected.threadId !== binding.threadId ||
    selected.messageId !== binding.messageId ||
    selected.nativeRunId !== binding.nativeRunId
  )
    return deny("identity");
  if (
    !isWorkflowUint(ownership.operationGeneration) ||
    !isWorkflowUint(ownership.ownerGeneration) ||
    ownership.operationGeneration !== ownership.ownerGeneration
  )
    return deny("stale_generation");
  if (ownership.ownerStatus !== "active") return deny("owner_inactive");
  if (ownership.operationStatus !== "claimed") return deny("operation_ineligible");
  if (
    typeof ownership.deadline !== "string" ||
    typeof ownership.now !== "string" ||
    !isWorkflowTimestamp(ownership.deadline) ||
    !isWorkflowTimestamp(ownership.now) ||
    !isWorkflowTimestampAfter(ownership.deadline, ownership.now)
  )
    return deny("deadline");
  if (ownership.cancelRequested !== false) return deny("cancel_requested");
  if (ownership.liveGrant !== "allowed") return deny("authority");
  return { kind: "allow", scope: "owned" };
};
