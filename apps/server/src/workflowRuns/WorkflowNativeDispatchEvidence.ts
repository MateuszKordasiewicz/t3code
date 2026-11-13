import {
  EventId,
  ThreadId,
  RunId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  RawEventId,
  OrchestrationV2AppThreadJson,
  OrchestrationV2RunJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2DomainEventJson,
  isKnownOrchestrationV2EventType,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { CommandReceiptV2 } from "../orchestration-v2/CommandReceiptStore.ts";

const decodeReceipt = Schema.decodeUnknownEffect(
  CommandReceiptV2.mapFields((fields) => ({
    ...fields,
    acceptedAt: Schema.DateTimeUtcFromString,
  })),
);
const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeMessage = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson);
const decodeEnvelope = Schema.decodeUnknownEffect(
  Schema.Struct({
    id: EventId,
    threadId: ThreadId,
    occurredAt: Schema.DateTimeUtcFromString,
    runId: Schema.optional(RunId),
    nodeId: Schema.optional(NodeId),
    driver: Schema.optional(ProviderDriverKind),
    providerInstanceId: Schema.optional(ProviderInstanceId),
    rawEventId: Schema.optional(RawEventId),
  }),
);
const decodeMetadata = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      applicationEventVersion: Schema.optional(Schema.Literal(2)),
      runId: Schema.optional(RunId),
      nodeId: Schema.optional(NodeId),
      driver: Schema.optional(ProviderDriverKind),
      providerInstanceId: Schema.optional(ProviderInstanceId),
      rawEventId: Schema.optional(RawEventId),
    }),
  ),
);
const conflict = { kind: "conflict" } as const;
const missing = { kind: "missing" } as const;
const positiveSequence = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0;

type RetainedIds = {
  readonly launchCommandId: string;
  readonly threadId: string;
  readonly messageId: string;
  readonly projectId: string;
};
type EventRow = {
  readonly sequence: number;
  readonly event_id: string;
  readonly aggregate_kind: string;
  readonly stream_id: string;
  readonly stream_version: number;
  readonly event_type: string;
  readonly occurred_at: string;
  readonly command_id: string;
  readonly payload_json: string;
  readonly metadata_json: string;
  readonly application_event_version: number;
};

// Caller must establish a writable workflow transaction; this client's tag does not prove writer mode.
export const readNativeDispatchEvidence = Effect.fn("WorkflowNativeDispatchEvidence.read")(
  function* (sql: SqlClient.SqlClient, ids: RetainedIds) {
    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService))) return conflict;
    if (
      ![ids?.launchCommandId, ids?.threadId, ids?.messageId, ids?.projectId].every(
        (id) => typeof id === "string" && id.length > 0,
      )
    )
      return conflict;
    const commandId = `${ids.launchCommandId}:initial-message`;
    const receipts = yield* sql<{
      aggregate_kind: string;
      aggregate_id: string;
      accepted_at: string;
      result_sequence: number;
      status: string;
      error: string | null;
      command_type: string;
    }>`SELECT aggregate_kind, aggregate_id, accepted_at, result_sequence, status, error, command_type FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    const receipt = receipts[0];
    if (receipt === undefined) return missing;
    yield* decodeReceipt({
      commandId,
      threadId: receipt.aggregate_id,
      commandType: receipt.command_type,
      acceptedAt: receipt.accepted_at,
      resultSequence: receipt.result_sequence,
      status: receipt.status,
      error: receipt.error,
    });
    if (
      receipts.length !== 1 ||
      receipt.aggregate_kind !== "thread" ||
      receipt.aggregate_id !== ids.threadId ||
      receipt.status !== "accepted" ||
      receipt.error !== null ||
      receipt.command_type !== "message.dispatch" ||
      !positiveSequence(receipt.result_sequence)
    )
      return conflict;
    const rows =
      yield* sql<EventRow>`SELECT sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, payload_json, metadata_json, application_event_version FROM orchestration_events WHERE command_id = ${commandId} ORDER BY sequence`;
    let selectedRun: Extract<OrchestrationV2DomainEventJson, { type: "run.created" }> | undefined;
    let selectedMessage:
      | Extract<OrchestrationV2DomainEventJson, { type: "message.updated" }>
      | undefined;
    let lastSequence = 0;
    for (const row of rows) {
      if (
        !positiveSequence(row.sequence) ||
        row.sequence <= lastSequence ||
        row.sequence > receipt.result_sequence ||
        !(Number.isSafeInteger(row.stream_version) && row.stream_version >= 0) ||
        row.aggregate_kind !== "thread" ||
        row.application_event_version !== 2 ||
        row.command_id !== commandId ||
        !isKnownOrchestrationV2EventType(row.event_type)
      )
        return conflict;
      lastSequence = row.sequence;
      const metadata = yield* decodeMetadata(row.metadata_json);
      yield* decodeEnvelope({
        id: row.event_id,
        threadId: row.stream_id,
        occurredAt: row.occurred_at,
        ...metadata,
      });
      if (row.event_type !== "run.created" && row.event_type !== "message.updated") continue;
      const payload = yield* decodeJson(row.payload_json);
      if (
        row.event_type === "message.updated" &&
        (typeof payload !== "object" || payload === null || !("id" in payload))
      )
        return conflict;
      if (row.event_type === "message.updated" && (payload as { id: unknown }).id !== ids.messageId)
        continue;
      const event = yield* decodeEvent({
        id: row.event_id,
        threadId: row.stream_id,
        occurredAt: row.occurred_at,
        type: row.event_type,
        payload,
        ...metadata,
      });
      if (event.type === "run.created") {
        if (selectedRun !== undefined) return conflict;
        selectedRun = event;
      } else if (event.type === "message.updated") {
        if (selectedMessage !== undefined) return conflict;
        selectedMessage = event;
      } else return conflict;
    }
    if (
      selectedRun !== undefined &&
      (selectedRun.threadId !== ids.threadId ||
        selectedRun.payload.threadId !== ids.threadId ||
        selectedRun.payload.userMessageId !== ids.messageId ||
        (selectedRun.runId !== undefined && selectedRun.runId !== selectedRun.payload.id))
    )
      return conflict;
    if (
      selectedMessage !== undefined &&
      (selectedMessage.threadId !== ids.threadId ||
        selectedMessage.payload.threadId !== ids.threadId ||
        selectedMessage.payload.id !== ids.messageId ||
        selectedMessage.payload.role !== "user")
    )
      return conflict;
    if (
      selectedRun !== undefined &&
      selectedMessage !== undefined &&
      (selectedMessage.payload.runId !== selectedRun.payload.id ||
        (selectedMessage.runId !== undefined && selectedMessage.runId !== selectedRun.payload.id))
    )
      return conflict;
    if (
      lastSequence < receipt.result_sequence ||
      selectedRun === undefined ||
      selectedMessage === undefined
    )
      return missing;
    const actualRunId = selectedRun.payload.id;
    const projects = yield* sql<{
      project_id: string;
    }>`SELECT project_id FROM projection_projects WHERE project_id = ${ids.projectId}`;
    const threads = yield* sql<{
      thread_id: string;
      project_id: string;
      payload_json: string;
    }>`SELECT thread_id, project_id, payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${ids.threadId}`;
    const runs = yield* sql<{
      run_id: string;
      thread_id: string;
      payload_json: string;
    }>`SELECT run_id, thread_id, payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${actualRunId}`;
    const messages = yield* sql<{
      message_id: string;
      thread_id: string;
      run_id: string | null;
      role: string;
      payload_json: string;
    }>`SELECT message_id, thread_id, run_id, role, payload_json FROM orchestration_v2_projection_messages WHERE message_id = ${ids.messageId}`;
    const project = projects[0];
    const threadRow = threads[0];
    const runRow = runs[0];
    const messageRow = messages[0];
    if (project !== undefined && project.project_id !== ids.projectId) return conflict;
    if (threadRow !== undefined) {
      const thread = yield* decodeThread(threadRow.payload_json);
      if (
        threadRow.thread_id !== ids.threadId ||
        threadRow.project_id !== ids.projectId ||
        thread.id !== ids.threadId ||
        thread.projectId !== ids.projectId
      )
        return conflict;
    }
    if (runRow !== undefined) {
      const run = yield* decodeRun(runRow.payload_json);
      if (
        runRow.run_id !== actualRunId ||
        runRow.thread_id !== ids.threadId ||
        run.id !== actualRunId ||
        run.threadId !== ids.threadId ||
        run.userMessageId !== ids.messageId
      )
        return conflict;
    }
    if (messageRow !== undefined) {
      const message = yield* decodeMessage(messageRow.payload_json);
      if (
        messageRow.message_id !== ids.messageId ||
        messageRow.thread_id !== ids.threadId ||
        messageRow.run_id !== actualRunId ||
        messageRow.role !== "user" ||
        message.id !== ids.messageId ||
        message.threadId !== ids.threadId ||
        message.runId !== actualRunId ||
        message.role !== "user"
      )
        return conflict;
    }
    if (!project || !threadRow || !runRow || !messageRow) return missing;
    return {
      kind: "verified",
      actualT3RunId: actualRunId,
      receiptSequence: receipt.result_sequence,
    } as const;
  },
  Effect.orElseSucceed(() => conflict),
);
