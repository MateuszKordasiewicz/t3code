import {
  ChatAttachment,
  CommandId,
  decodePlanningSessionV1,
  getProviderAttachmentLimitError,
  MessageId,
  ModelSelection,
  OrchestrationMessageContext,
  OrchestrationV2Actor,
  OrchestrationV2CreationSource,
  OrchestrationV2DelegatedFrom,
  OrchestrationV2LinkOrigin,
  OrchestrationV2ProviderThreadNativeMetadata,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  PlanningSessionV1,
  ProjectId,
  ProviderDriverKind,
  ProviderInteractionMode,
  RuntimeMode,
  ScheduledTaskId,
  Sha256Hex,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
} from "../attachmentStore.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";

const Launch = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  reuseExistingThread: Schema.optional(Schema.Boolean),
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  generateTitle: Schema.optional(Schema.Boolean),
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy,
  initialMessage: Schema.Struct({
    messageId: MessageId,
    scheduledTaskId: Schema.optional(ScheduledTaskId),
    senderThreadId: Schema.optional(ThreadId),
    text: Schema.String,
    attachments: Schema.Array(ChatAttachment),
    context: Schema.optional(OrchestrationMessageContext),
  }),
  importedNativeThread: Schema.optional(
    Schema.Struct({
      ref: Schema.Struct({
        driver: ProviderDriverKind,
        nativeId: TrimmedNonEmptyString,
        strength: Schema.Literal("strong"),
      }),
      metadata: Schema.optional(OrchestrationV2ProviderThreadNativeMetadata),
    }),
  ),
  createdBy: OrchestrationV2Actor,
  creationSource: OrchestrationV2CreationSource,
  linkOrigin: Schema.optional(OrchestrationV2LinkOrigin),
  delegatedFrom: Schema.optional(OrchestrationV2DelegatedFrom),
});

const Material = Schema.Struct({
  version: Schema.Literal(1),
  intentId: Schema.NonEmptyString,
  receiptId: Schema.NonEmptyString,
  intentFingerprint: Sha256Hex,
  launch: Launch,
  planningSession: Schema.NullOr(PlanningSessionV1),
});

const decodeLaunch = Schema.decodeUnknownEffect(Launch);
const encodeLaunch = Schema.encodeEffect(Launch);
const decodeMaterial = Schema.decodeUnknownEffect(Material);
const encodeMaterial = Schema.encodeEffect(Material);
class InvalidNativeReservationMaterial extends Schema.TaggedError<InvalidNativeReservationMaterial>()(
  "InvalidNativeReservationMaterial",
  {},
) {}
const invalid = () => new InvalidNativeReservationMaterial();

const canonical = (value: unknown) =>
  Effect.try({
    try: () => WorkflowCanonicalHash.assertCanonical(value),
    catch: invalid,
  });
const fixedPoint = (input: unknown, decoded: unknown, encoded: unknown) =>
  Effect.try({
    try: () => {
      WorkflowCanonicalHash.assertCanonical(decoded);
      WorkflowCanonicalHash.assertCanonical(encoded);
      const original = stableStringify(input);
      if (original !== stableStringify(decoded) || original !== stableStringify(encoded))
        throw invalid();
    },
    catch: invalid,
  });

export const decodeNativeReservationLaunch = Effect.fn(
  "WorkflowNativeReservationMaterial.decodeLaunch",
)(function* (input: unknown) {
  yield* canonical(input);
  const launch = yield* decodeLaunch(input, { onExcessProperty: "error" });
  const encoded = yield* encodeLaunch(launch);
  yield* fixedPoint(input, launch, encoded);
  const attachments = launch.initialMessage.attachments;
  if (
    getProviderAttachmentLimitError(attachments) ||
    attachments.some(
      (attachment) =>
        parseThreadSegmentFromAttachmentId(attachment.id) === PENDING_ATTACHMENT_THREAD_SEGMENT,
    )
  )
    return yield* invalid();
  return {
    launch,
    launchHash: WorkflowCanonicalHash.tagged("t3.workflow-native-reservation-launch.v1", launch),
  };
});

export const decodeNativeReservationMaterial = Effect.fn(
  "WorkflowNativeReservationMaterial.decodeMaterial",
)(function* (input: unknown) {
  yield* canonical(input);
  const material = yield* decodeMaterial(input, { onExcessProperty: "error" });
  const encoded = yield* encodeMaterial(material);
  yield* fixedPoint(input, material, encoded);
  if (material.planningSession !== null) yield* decodePlanningSessionV1(material.planningSession);
  const { launchHash } = yield* decodeNativeReservationLaunch(material.launch);
  return {
    material,
    launchHash,
    materialHash: WorkflowCanonicalHash.tagged(
      "t3.workflow-native-reservation-material.v1",
      material,
    ),
  };
});
