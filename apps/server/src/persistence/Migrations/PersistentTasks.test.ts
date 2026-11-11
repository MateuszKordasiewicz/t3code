import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const db = NodeSqliteClient.layer({ filename: ":memory:" });

it.effect("creates the task foundation on a fresh database and reruns without changes", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const migrations = yield* runMigrations();
    assert.deepInclude(migrations, [81, "WorkTaskFoundation"]);
    const tables = yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'work_task_%' ORDER BY name
    `;
    assert.deepStrictEqual(
      tables.map((row) => row.name),
      ["work_task_accepted_definitions", "work_task_projections", "work_task_thread_links"],
    );
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("upgrades predecessor rows and events without changing their bytes or order", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 80 });
    yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES ('p', 'Project', '/a', '[ 1, 2 ]', 't', 't')`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads
      (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json)
      VALUES ('thread', 'p', 'Thread', 'codex', 'full-access', 'default', 't', 't', '{ "lineage": [1, 2] }')`;
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
      VALUES ('e1', 'project', 'p', 1, 'project.created', 't', 'client', '{ "one": 1 }', '{}'),
             ('e2', 'thread', 'thread', 1, 'thread.created', 't', 'client', '{ "two": 2 }', '{}')`;
    const projects = yield* sql`SELECT * FROM projection_projects ORDER BY project_id`;
    const threads =
      yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`;
    const events = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
    assert.deepStrictEqual(yield* runMigrations(), [[81, "WorkTaskFoundation"]]);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM projection_projects ORDER BY project_id`,
      projects,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`,
      threads,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
      events,
    );
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("rolls back a failed migration and succeeds on rerun", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 80 });
    yield* sql`CREATE TABLE work_task_accepted_definitions (sentinel TEXT)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    const created = yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_projections'`;
    const recorded =
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 81`;
    assert.deepStrictEqual(created, []);
    assert.deepStrictEqual(recorded, []);
    yield* sql`DROP TABLE work_task_accepted_definitions`;
    assert.deepStrictEqual(yield* runMigrations(), [[81, "WorkTaskFoundation"]]);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("enforces identities while retaining link and definition history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES
      ('a', 'p1', 1, 2, 3, 4, '{}', 't', 't'), ('b', 'p2', 1, 1, 1, 1, '{}', 't', 't')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_projections VALUES
      ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_projections VALUES
      ('bad', NULL, 1, 1, 1, 0, '{}', 't', 't')`),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_accepted_definitions VALUES
      ('missing', 1, 1, '{}', '{}', 'bad', 't')`),
      ),
    );
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES
      ('a', 1, 4, '{ "revision": 1 }', '{"type":"user"}', 'def1', 't'),
      ('a', 2, 5, '{ "revision": 2 }', '{"type":"user"}', 'def2', 't')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_accepted_definitions VALUES
      ('a', 1, 4, '{}', '{}', 'def3', 't')`),
      ),
    );
    yield* sql`INSERT INTO work_task_thread_links
      (link_event_id, task_id, thread_id, linked_at, linked_attribution_json)
      VALUES ('l1', 'a', 'thread', 't', '{"actor":"one"}'), ('l2', 'b', 'thread', 't', '{"actor":"two"}'),
             ('l3', 'a', 'other-thread', 't', '{}')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_thread_links
      (link_event_id, task_id, thread_id, linked_at, linked_attribution_json)
      VALUES ('l4', 'a', 'thread', 't', '{}')`),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`UPDATE work_task_thread_links SET unlink_event_id = 'u1' WHERE link_event_id = 'l1'`,
        ),
      ),
    );
    yield* sql`UPDATE work_task_thread_links SET unlink_event_id = 'u1', unlinked_at = 'later',
      unlinked_attribution_json = '{"actor":"three"}' WHERE link_event_id = 'l1'`;
    yield* sql`INSERT INTO work_task_thread_links
      (link_event_id, task_id, thread_id, linked_at, linked_attribution_json)
      VALUES ('l4', 'a', 'thread', 'new', '{"actor":"four"}')`;
    const links =
      yield* sql`SELECT link_event_id, linked_attribution_json, unlink_event_id, unlinked_attribution_json
      FROM work_task_thread_links WHERE task_id = 'a' AND thread_id = 'thread' ORDER BY linked_at`;
    assert.deepStrictEqual(links, [
      {
        link_event_id: "l4",
        linked_attribution_json: '{"actor":"four"}',
        unlink_event_id: null,
        unlinked_attribution_json: null,
      },
      {
        link_event_id: "l1",
        linked_attribution_json: '{"actor":"one"}',
        unlink_event_id: "u1",
        unlinked_attribution_json: '{"actor":"three"}',
      },
    ]);
    const revisions =
      yield* sql`SELECT definition_revision, action_policy_revision FROM work_task_accepted_definitions ORDER BY definition_revision`;
    assert.deepStrictEqual(revisions, [
      { definition_revision: 1, action_policy_revision: 4 },
      { definition_revision: 2, action_policy_revision: 5 },
    ]);
  }).pipe(Effect.provide(db)),
);
