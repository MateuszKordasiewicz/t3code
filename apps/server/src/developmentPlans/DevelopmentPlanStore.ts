import {
  decodeDevelopmentPlanRevisionV1,
  type DevelopmentPlanRevisionV1,
  type RevisionEnvelope,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";

const encode = (value: unknown) => JSON.stringify(value);
const parse = (value: string): unknown => JSON.parse(value);
export class DevelopmentPlanStoreError extends Schema.TaggedError<DevelopmentPlanStoreError>()(
  "DevelopmentPlanStoreError",
  {
    reason: Schema.Literals(["not-found", "conflict", "invalid", "integrity", "storage"]),
    currentVersion: Schema.optionalKey(Schema.Number),
  },
) {}
const isStoreError = Schema.is(DevelopmentPlanStoreError);
const error = (reason: "not-found" | "invalid" | "integrity" | "storage") =>
  new DevelopmentPlanStoreError({ reason });
const conflict = (currentVersion: number) =>
  new DevelopmentPlanStoreError({ reason: "conflict", currentVersion });
const Summary = Schema.Struct({
  planId: Schema.String,
  environmentId: Schema.String,
  projectId: Schema.String,
  title: Schema.String,
  currentRevisionId: Schema.String,
  version: Schema.Number,
  createdAt: Schema.String,
  linkedThreadIds: Schema.Array(Schema.String),
});
const decodeSummary = Schema.decodeUnknownEffect(Summary);
type PlanSummary = typeof Summary.Type;
type MutationContext = {
  readonly actor: { readonly kind: "human" | "agent" | "system"; readonly id: string };
  readonly destination: { readonly environmentId: string; readonly projectId: string };
};
type Content = {
  readonly planId: string;
  readonly revisionId: string;
  readonly revision: DevelopmentPlanRevisionV1;
  readonly packet: RevisionEnvelope["sourcePacket"];
  readonly createdAt: string;
  readonly commandId: string;
};
type Create = Content & {
  readonly environmentId: string;
  readonly projectId: string;
  readonly title: string;
};
type Append = Content & { readonly expectedVersion: number };
type Link = {
  readonly planId: string;
  readonly expectedVersion: number;
  readonly threadId: string;
  readonly commandId: string;
};
type Row = {
  plan_id: string;
  environment_id: string;
  project_id: string;
  title: string;
  current_revision_id: string | null;
  version: number;
  created_at: string;
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const digest = yield* Digest.Digest;
  const inTransaction = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    sql
      .withTransaction(work)
      .pipe(Effect.mapError((e) => (isStoreError(e) ? e : error("storage"))));
  const summary = Effect.fn("DevelopmentPlanStore.summary")(function* (row: Row) {
    if (!row.current_revision_id) return yield* error("integrity");
    const threads = yield* sql<{
      thread_id: string;
    }>`SELECT thread_id FROM development_plan_threads WHERE plan_id = ${row.plan_id} ORDER BY thread_id`;
    return {
      planId: row.plan_id,
      environmentId: row.environment_id,
      projectId: row.project_id,
      title: row.title,
      currentRevisionId: row.current_revision_id,
      version: row.version,
      createdAt: row.created_at,
      linkedThreadIds: threads.map((t) => t.thread_id),
    };
  });
  const findPlan = Effect.fn("DevelopmentPlanStore.findPlan")(function* (planId: string) {
    const rows = yield* sql<Row>`SELECT * FROM development_plans WHERE plan_id = ${planId}`;
    if (!rows[0]) return yield* error("not-found");
    return rows[0];
  });
  // createdAt is server-assigned, so retries of one command must fingerprint identically.
  const fingerprint = (method: string, input: object, context: MutationContext) =>
    Effect.try({
      try: () => {
        const { createdAt: _createdAt, ...stable } = input as Record<string, unknown>;
        return DevelopmentPlanHash.command({ ...context, method, input: stable });
      },
      catch: () => error("invalid"),
    });
  const receipt = Effect.fn("DevelopmentPlanStore.receipt")(function* (
    commandId: string,
    planId: string,
    hash: string,
  ) {
    const rows = yield* sql<{
      plan_id: string;
      fingerprint: string;
      result_json: string;
    }>`SELECT * FROM development_plan_commands WHERE command_id = ${commandId}`;
    const row = rows[0];
    if (!row) return undefined;
    if (row.plan_id !== planId || row.fingerprint !== hash) return yield* error("integrity");
    const raw = yield* Effect.try({
      try: () => parse(row.result_json),
      catch: () => error("integrity"),
    });
    const plan = yield* decodeSummary(raw).pipe(Effect.mapError(() => error("integrity")));
    return { kind: "replay" as const, plan };
  });
  const saveReceipt = Effect.fn("DevelopmentPlanStore.saveReceipt")(function* (
    commandId: string,
    planId: string,
    hash: string,
  ) {
    const plan = yield* summary(yield* findPlan(planId));
    yield* sql`INSERT INTO development_plan_commands VALUES (${commandId}, ${planId}, ${hash}, ${encode(plan)})`;
    return { kind: "applied" as const, plan };
  });
  const validate = Effect.fn("DevelopmentPlanStore.validate")(function* (input: Content) {
    const hashes = yield* Effect.try({
      try: () => ({
        sourcePacketHash: DevelopmentPlanHash.sourcePacket(input.packet),
        revisionHash: DevelopmentPlanHash.revision(input.revision),
      }),
      catch: () => error("invalid"),
    });
    if (
      input.revision.planId !== input.planId ||
      input.revision.sourcePacketHash !== hashes.sourcePacketHash
    )
      return yield* error("invalid");
    const envelope = yield* decodeDevelopmentPlanRevisionV1({
      planId: input.planId,
      revisionId: input.revisionId,
      ...hashes,
      revision: input.revision,
      sourcePacket: input.packet,
      createdAt: input.createdAt,
    }).pipe(
      Effect.provideService(Digest.Digest, digest),
      Effect.mapError(() => error("invalid")),
    );
    return envelope;
  });
  const insertRevision = Effect.fn("DevelopmentPlanStore.insertRevision")(function* (
    envelope: RevisionEnvelope,
  ) {
    const packetJson = encode(envelope.sourcePacket);
    const packets = yield* sql<{
      packet_json: string;
    }>`SELECT packet_json FROM development_plan_source_packets WHERE plan_id = ${envelope.planId} AND source_packet_hash = ${envelope.sourcePacketHash}`;
    if (packets[0] && packets[0].packet_json !== packetJson) return yield* error("integrity");
    if (!packets[0])
      yield* sql`INSERT INTO development_plan_source_packets VALUES (${envelope.planId}, ${envelope.sourcePacketHash}, ${packetJson})`;
    const ids = yield* sql<{
      revision_id: string;
    }>`INSERT INTO workflow_revision_identities VALUES (${envelope.planId}, ${envelope.revisionId}, ${envelope.revisionHash}, ${envelope.sourcePacketHash}) ON CONFLICT DO NOTHING RETURNING revision_id`;
    if (!ids.length) return yield* error("integrity");
    yield* sql`INSERT INTO development_plan_revisions VALUES (${envelope.planId}, ${envelope.revisionId}, ${envelope.revisionHash}, ${envelope.sourcePacketHash}, ${encode(envelope.revision)}, ${envelope.createdAt})`;
  });
  const create = Effect.fn("DevelopmentPlanStore.create")(function* (
    input: Create,
    context: MutationContext,
  ) {
    const envelope = yield* validate(input);
    const hash = yield* fingerprint("create", input, context);
    return yield* inTransaction(
      Effect.gen(function* () {
        const prior = yield* receipt(input.commandId, input.planId, hash);
        if (prior) return prior;
        const ids = yield* sql<{
          plan_id: string;
        }>`INSERT INTO workflow_plan_identities VALUES (${input.planId}) ON CONFLICT DO NOTHING RETURNING plan_id`;
        if (!ids.length) {
          const rows =
            yield* sql<Row>`SELECT * FROM development_plans WHERE plan_id = ${input.planId}`;
          return yield* rows[0] ? conflict(rows[0].version) : error("integrity");
        }
        yield* sql`INSERT INTO development_plans VALUES (${input.planId}, ${input.environmentId}, ${input.projectId}, ${input.title}, NULL, 0, ${input.createdAt})`;
        yield* insertRevision(envelope);
        yield* sql`UPDATE development_plans SET current_revision_id = ${input.revisionId} WHERE plan_id = ${input.planId}`;
        return yield* saveReceipt(input.commandId, input.planId, hash);
      }),
    );
  });
  const append = Effect.fn("DevelopmentPlanStore.append")(function* (
    input: Append,
    context: MutationContext,
  ) {
    const envelope = yield* validate(input);
    const hash = yield* fingerprint("append", input, context);
    return yield* inTransaction(
      Effect.gen(function* () {
        const prior = yield* receipt(input.commandId, input.planId, hash);
        if (prior) return prior;
        const row = yield* findPlan(input.planId);
        if (row.version !== input.expectedVersion) return yield* conflict(row.version);
        yield* insertRevision(envelope);
        const updated = yield* sql<{
          version: number;
        }>`UPDATE development_plans SET current_revision_id = ${input.revisionId}, version = version + 1 WHERE plan_id = ${input.planId} AND version = ${input.expectedVersion} RETURNING version`;
        if (!updated.length) return yield* conflict((yield* findPlan(input.planId)).version);
        return yield* saveReceipt(input.commandId, input.planId, hash);
      }),
    );
  });
  const changeLink = Effect.fn("DevelopmentPlanStore.changeLink")(function* (
    method: "linkThread" | "unlinkThread",
    input: Link,
    context: MutationContext,
  ) {
    const hash = yield* fingerprint(method, input, context);
    return yield* inTransaction(
      Effect.gen(function* () {
        const prior = yield* receipt(input.commandId, input.planId, hash);
        if (prior) return prior;
        const row = yield* findPlan(input.planId);
        if (row.version !== input.expectedVersion) return yield* conflict(row.version);
        const changed =
          method === "linkThread"
            ? yield* sql<{
                thread_id: string;
              }>`INSERT INTO development_plan_threads VALUES (${input.planId}, ${input.threadId}) ON CONFLICT DO NOTHING RETURNING thread_id`
            : yield* sql<{
                thread_id: string;
              }>`DELETE FROM development_plan_threads WHERE plan_id = ${input.planId} AND thread_id = ${input.threadId} RETURNING thread_id`;
        if (!changed.length) return yield* error("invalid");
        const updated = yield* sql<{
          version: number;
        }>`UPDATE development_plans SET version = version + 1 WHERE plan_id = ${input.planId} AND version = ${input.expectedVersion} RETURNING version`;
        if (!updated.length) return yield* conflict((yield* findPlan(input.planId)).version);
        return yield* saveReceipt(input.commandId, input.planId, hash);
      }),
    );
  });
  const linkThread = Effect.fn("DevelopmentPlanStore.linkThread")(
    (input: Link, context: MutationContext) => changeLink("linkThread", input, context),
  );
  const unlinkThread = Effect.fn("DevelopmentPlanStore.unlinkThread")(
    (input: Link, context: MutationContext) => changeLink("unlinkThread", input, context),
  );
  const get = Effect.fn("DevelopmentPlanStore.get")(function* (input: {
    readonly planId: string;
    readonly revisionId?: string;
  }) {
    return yield* inTransaction(
      Effect.gen(function* () {
        const row = yield* findPlan(input.planId);
        const plan = yield* summary(row);
        const revisionId = input.revisionId ?? plan.currentRevisionId;
        const rows = yield* sql<{
          revision_hash: string;
          source_packet_hash: string;
          revision_json: string;
          packet_json: string | null;
          created_at: string;
          identity_hash: string | null;
          identity_source_hash: string | null;
        }>`SELECT r.revision_hash, r.source_packet_hash, r.revision_json, r.created_at, p.packet_json, i.revision_hash AS identity_hash, i.source_packet_hash AS identity_source_hash FROM development_plan_revisions r LEFT JOIN development_plan_source_packets p ON p.plan_id = r.plan_id AND p.source_packet_hash = r.source_packet_hash LEFT JOIN workflow_revision_identities i ON i.plan_id = r.plan_id AND i.revision_id = r.revision_id WHERE r.plan_id = ${input.planId} AND r.revision_id = ${revisionId}`;
        const r = rows[0];
        if (!r) return yield* error(input.revisionId ? "not-found" : "integrity");
        if (r.packet_json === null || r.identity_hash === null) return yield* error("integrity");
        const packetJson = r.packet_json;
        const content = yield* Effect.try({
          try: () => ({ revision: parse(r.revision_json), sourcePacket: parse(packetJson) }),
          catch: () => error("integrity"),
        });
        const envelope = yield* decodeDevelopmentPlanRevisionV1({
          planId: input.planId,
          revisionId,
          revisionHash: r.revision_hash,
          sourcePacketHash: r.source_packet_hash,
          ...content,
          createdAt: r.created_at,
        }).pipe(
          Effect.provideService(Digest.Digest, digest),
          Effect.mapError(() => error("integrity")),
        );
        const hashes = yield* Effect.try({
          try: () => ({
            revision: DevelopmentPlanHash.revision(envelope.revision),
            source: DevelopmentPlanHash.sourcePacket(envelope.sourcePacket),
          }),
          catch: () => error("integrity"),
        });
        if (
          hashes.revision !== r.revision_hash ||
          hashes.source !== r.source_packet_hash ||
          r.identity_hash !== r.revision_hash ||
          r.identity_source_hash !== r.source_packet_hash
        )
          return yield* error("integrity");
        return {
          plan,
          revisionId,
          revisionHash: r.revision_hash,
          revision: envelope.revision,
          sourcePacket: envelope.sourcePacket,
          createdAt: r.created_at,
        };
      }),
    );
  });
  const getObject = Effect.fn("DevelopmentPlanStore.getObject")(function* (input: {
    readonly planId: string;
    readonly revisionId: string;
    readonly family:
      | "requirements"
      | "decisions"
      | "questions"
      | "assumptions"
      | "approach"
      | "workItems";
    readonly id: string;
  }) {
    const snapshot = yield* get(input);
    const object = snapshot.revision[input.family].find((o) => o.id === input.id);
    if (!object) return yield* error("not-found");
    return object;
  });
  const list = Effect.fn("DevelopmentPlanStore.list")(function* (input: {
    readonly projectId: string;
    readonly cursor?: string;
    readonly limit: number;
  }) {
    return yield* inTransaction(
      Effect.gen(function* () {
        const limit = Math.max(1, Math.min(100, Math.trunc(input.limit) || 1));
        const rows =
          yield* sql<Row>`SELECT * FROM development_plans WHERE project_id = ${input.projectId} AND plan_id > ${input.cursor ?? ""} ORDER BY plan_id LIMIT ${limit + 1}`;
        const plans: PlanSummary[] = yield* Effect.forEach(rows.slice(0, limit), summary);
        return {
          plans,
          ...(rows.length > limit ? { nextCursor: plans[plans.length - 1]!.planId } : {}),
        };
      }),
    );
  });
  return { create, append, linkThread, unlinkThread, get, getObject, list };
});
export class DevelopmentPlanStore extends Context.Service<
  DevelopmentPlanStore,
  Effect.Success<typeof make>
>()("t3/developmentPlans/DevelopmentPlanStore") {}
export const layer = Layer.effect(DevelopmentPlanStore, make);
