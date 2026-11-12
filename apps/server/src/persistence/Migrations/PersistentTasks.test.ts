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
    assert.deepInclude(migrations, [82, "WorkTaskManagerCustody"]);
    assert.deepInclude(migrations, [83, "WorkTaskValidation"]);
    assert.deepInclude(migrations, [84, "WorkTaskActionReviews"]);
    assert.deepInclude(migrations, [85, "WorkTaskCurrentState"]);
    assert.deepInclude(migrations, [86, "WorkTaskWaits"]);
    assert.deepInclude(migrations, [87, "WorkTaskResults"]);
    assert.deepInclude(migrations, [88, "WorkTaskClaims"]);
    assert.deepInclude(migrations, [89, "WorkTaskValidationIdentity"]);
    const tables = yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'work_task_%' ORDER BY name
    `;
    assert.deepStrictEqual(
      tables.map((row) => row.name),
      [
        "work_task_accepted_definitions",
        "work_task_action_review_decisions",
        "work_task_action_review_releases",
        "work_task_action_reviews",
        "work_task_allowances",
        "work_task_claims",
        "work_task_current_state",
        "work_task_manager_bindings",
        "work_task_projections",
        "work_task_results",
        "work_task_thread_links",
        "work_task_validation_attempts",
        "work_task_validation_identity",
        "work_task_view_fence",
        "work_task_waits",
        "work_task_wake_reservations",
        "work_task_wake_sources",
      ],
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
    assert.deepStrictEqual(yield* runMigrations(), [
      [81, "WorkTaskFoundation"],
      [82, "WorkTaskManagerCustody"],
      [83, "WorkTaskValidation"],
      [84, "WorkTaskActionReviews"],
      [85, "WorkTaskCurrentState"],
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
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
    assert.deepStrictEqual(yield* runMigrations(), [
      [81, "WorkTaskFoundation"],
      [82, "WorkTaskManagerCustody"],
      [83, "WorkTaskValidation"],
      [84, "WorkTaskActionReviews"],
      [85, "WorkTaskCurrentState"],
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
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

it.effect(
  "preserves foundation history through custody upgrade and rolls back a failed attempt",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 81 });
      yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{ "x": 1 }', 't', 't')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{ "x": 1 }', '{}', 'e', 't')`;
      yield* sql`INSERT INTO work_task_thread_links VALUES ('l', 'a', 'thread', 't', '{}', NULL, NULL, NULL)`;
      const tasks = yield* sql`SELECT * FROM work_task_projections`;
      const definitions = yield* sql`SELECT * FROM work_task_accepted_definitions`;
      const links = yield* sql`SELECT * FROM work_task_thread_links`;
      yield* sql`CREATE TABLE work_task_wake_reservations (sentinel TEXT)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_manager_bindings'`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 82`,
        [],
      );
      yield* sql`DROP TABLE work_task_wake_reservations`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [82, "WorkTaskManagerCustody"],
        [83, "WorkTaskValidation"],
        [84, "WorkTaskActionReviews"],
        [85, "WorkTaskCurrentState"],
        [86, "WorkTaskWaits"],
        [87, "WorkTaskResults"],
        [88, "WorkTaskClaims"],
        [89, "WorkTaskValidationIdentity"],
      ]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_projections`, tasks);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_definitions`, definitions);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_thread_links`, links);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(db)),
);

it.effect("constrains manager custody, source identity and finite wake reservations", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES
      ('a', NULL, 1, 1, 1, 1, '{}', 't', 't'), ('b', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager1', 't', NULL)`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_manager_bindings VALUES ('a', 2, 'manager2', 't', NULL)`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_manager_bindings VALUES ('b', 1, 'manager1', 't', NULL)`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_manager_bindings VALUES ('b', 0, 'bad', 't', NULL)`,
        ),
      ),
    );
    yield* sql`UPDATE work_task_manager_bindings SET retired_at = 'later' WHERE task_id = 'a'`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 2, 'manager2', 'later', NULL)`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'again', 't', 't')`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(yield* Effect.exit(sql`INSERT INTO work_task_allowances VALUES ('a', 0, 0)`)),
    );
    assert.isTrue(
      Exit.isFailure(yield* Effect.exit(sql`INSERT INTO work_task_allowances VALUES ('a', 2, 3)`)),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(sql`INSERT INTO work_task_allowances VALUES ('a', 2.5, 0)`),
      ),
    );
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 2, 2)`;
    yield* sql`INSERT INTO work_task_wake_reservations VALUES ('r1', 'a', 2, 'c1', 'm1', 'run1', 1, 2, 1, 't', NULL)`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 2, 'c2', NULL, NULL, 2, 1, 0, 't', NULL)`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 3, 'c2', NULL, NULL, 2, 1, 0, 't', 't')`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 2, 'c1', NULL, NULL, 2, 1, 0, 't', 't')`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 2, 'c2', NULL, NULL, 1, 1, 0, 't', 't')`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 2, 'c2', NULL, NULL, 2, 1, 2, 't', 't')`,
        ),
      ),
    );
    yield* sql`UPDATE work_task_wake_reservations SET claimed_at = 'later' WHERE reservation_id = 'r1'`;
    yield* sql`INSERT INTO work_task_wake_reservations VALUES ('r2', 'a', 2, 'c2', NULL, NULL, 2, 1, 0, 't', NULL)`;
    yield* sql`INSERT INTO work_task_wake_sources VALUES ('a', 'event', 'e1', 'r1', 'result')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_sources VALUES ('a', 'event', 'e1', 'r2', NULL)`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_wake_sources VALUES ('b', 'event', 'e2', 'r1', NULL)`,
        ),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT generation, thread_id, retired_at FROM work_task_manager_bindings WHERE task_id = 'a' ORDER BY generation`,
      [
        { generation: 1, thread_id: "manager1", retired_at: "later" },
        { generation: 2, thread_id: "manager2", retired_at: null },
      ],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT configured_cap, consumed FROM work_task_allowances WHERE task_id = 'a'`,
      [{ configured_cap: 2, consumed: 2 }],
    );
  }).pipe(Effect.provide(db)),
);

it.effect("preserves task custody through validation upgrade and rolls back migration 83", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 82 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{"task":1}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{"definition":1}', '{}', 'def', 't')`;
    yield* sql`INSERT INTO work_task_thread_links VALUES ('link', 'a', 'thread', 't', '{}', NULL, NULL, NULL)`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 2, 1)`;
    yield* sql`INSERT INTO work_task_wake_reservations VALUES ('r', 'a', 1, 'c', 'm', 'run', 1, 1, 0, 't', 't')`;
    yield* sql`INSERT INTO work_task_wake_sources VALUES ('a', 'event', 'source', 'r', 'result')`;
    const tasks = yield* sql`SELECT * FROM work_task_projections`;
    const definitions = yield* sql`SELECT * FROM work_task_accepted_definitions`;
    const links = yield* sql`SELECT * FROM work_task_thread_links`;
    const managers = yield* sql`SELECT * FROM work_task_manager_bindings`;
    const allowances = yield* sql`SELECT * FROM work_task_allowances`;
    const reservations = yield* sql`SELECT * FROM work_task_wake_reservations`;
    const sources = yield* sql`SELECT * FROM work_task_wake_sources`;
    yield* sql`CREATE INDEX work_task_validation_task_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_validation_attempts'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 83`,
      [],
    );
    yield* sql`DROP INDEX work_task_validation_task_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [83, "WorkTaskValidation"],
      [84, "WorkTaskActionReviews"],
      [85, "WorkTaskCurrentState"],
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_projections`, tasks);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_definitions`, definitions);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_thread_links`, links);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_manager_bindings`, managers);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, allowances);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_wake_reservations`, reservations);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_wake_sources`, sources);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "binds distinct validation attempts to definitions, managers, evidence and run provenance",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO work_task_projections VALUES
      ('a', NULL, 1, 1, 1, 1, '{}', 't', 't'), ('b', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'def', 't')`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
      yield* sql`INSERT INTO work_task_validation_attempts VALUES
      ('v1', 'a', 1, 1, 1, 'evidence', 'watermark', '{"provider":"codex","model":"validator"}', 'run1', 'result', '{"outcome":"pass"}', 't', 't', 't'),
      ('v2', 'a', 1, 1, 1, 'evidence', 'watermark', '{"provider":"claude","model":"validator"}', NULL, 'reserved', NULL, 't', NULL, NULL)`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v1', 'a', 1, 1, 1, 'other', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'a', 1, 1, 1, 'e', 'w', '{}', 'run1', 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'a', 2, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'b', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'a', 1, 2, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'a', 1, 1, 0, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`INSERT INTO work_task_validation_attempts VALUES
      ('v3', 'a', 1, 1, 1, NULL, 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE work_task_validation_attempts SET state = 'result' WHERE attempt_id = 'v2'`,
          ),
        ),
      );
      yield* sql`UPDATE work_task_validation_attempts SET state = 'started', started_at = 'later', run_id = 'run2' WHERE attempt_id = 'v2'`;
      yield* sql`UPDATE work_task_validation_attempts SET state = 'uncertain', finished_at = 'later' WHERE attempt_id = 'v2'`;
      assert.deepStrictEqual(
        yield* sql`SELECT attempt_id, evidence_id, validator_json, run_id, state, outcome_json
      FROM work_task_validation_attempts ORDER BY attempt_id`,
        [
          {
            attempt_id: "v1",
            evidence_id: "evidence",
            validator_json: '{"provider":"codex","model":"validator"}',
            run_id: "run1",
            state: "result",
            outcome_json: '{"outcome":"pass"}',
          },
          {
            attempt_id: "v2",
            evidence_id: "evidence",
            validator_json: '{"provider":"claude","model":"validator"}',
            run_id: "run2",
            state: "uncertain",
            outcome_json: null,
          },
        ],
      );
    }).pipe(Effect.provide(db)),
);

it.effect("upgrades validation data atomically to action reviews and preserves prior records", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 83 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'def', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('v', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`;
    yield* sql`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
      VALUES ('event', 'project', 'p', 1, 'project.created', 't', 'client', '{ "original": true }', '{}')`;
    const tasks = yield* sql`SELECT * FROM work_task_projections`;
    const definitions = yield* sql`SELECT * FROM work_task_accepted_definitions`;
    const managers = yield* sql`SELECT * FROM work_task_manager_bindings`;
    const validations = yield* sql`SELECT * FROM work_task_validation_attempts`;
    const events = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
    yield* sql`CREATE INDEX work_task_action_reviews_task_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_action_reviews'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 84`,
      [],
    );
    yield* sql`DROP INDEX work_task_action_reviews_task_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [84, "WorkTaskActionReviews"],
      [85, "WorkTaskCurrentState"],
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_projections`, tasks);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_definitions`, definitions);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_manager_bindings`, managers);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_attempts`, validations);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
      events,
    );
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "binds immutable reviews, human decisions and once-only releases to exact provenance",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'def', 't')`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
      yield* sql`INSERT INTO work_task_action_reviews VALUES
      ('review', 'a', 1, 1, 'manager', 'session', 'run', 'command', 'request', 'rule', 1, 1,
       'send', 'label', 'target', '{"content":"full payload","artifacts":[{"uri":"artifact","hash":"hash"}]}', 'fingerprint', 1, 't')`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT OR REPLACE INTO work_task_action_reviews SELECT * FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE work_task_action_reviews SET fingerprint = 'other' WHERE review_id = 'review'`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`DELETE FROM work_task_action_reviews WHERE review_id = 'review'`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'duplicate', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, command_id, 'request2', rule_id, definition_revision, action_policy_revision, action, label, target, preview_json, fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'wrong', task_id, revision, manager_generation, 'foreign', provider_session_id, run_id, 'cmd2', 'req2', rule_id, definition_revision, action_policy_revision, action, label, target, preview_json, fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'wrong', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'cmd2', 'req2', rule_id, definition_revision, 2, action, label, target, preview_json, fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'wrong', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'cmd2', 'req2', rule_id, definition_revision, action_policy_revision, action, label, target, '{"content":"x"}', fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'wrong', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'cmd2', 'req2', rule_id, definition_revision, action_policy_revision, action, label, target, json_object('content', printf('%.*c', 32768, 'x'), 'artifacts', json('[]')), fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_reviews SELECT 'wrong', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'cmd2', 'req2', rule_id, definition_revision, action_policy_revision, action, label, target, '{"content":"x","artifacts":[{"uri":"x"}]}', fingerprint, created_sequence, created_at FROM work_task_action_reviews`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_decisions VALUES ('review', 'a', 1, 1, 'wrong', 1, 'approve', 'human', 'auth-session', NULL, 2, 't')`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_decisions VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'approve', '', 'auth-session', NULL, 2, 't')`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_releases VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'continuation', 't')`,
          ),
        ),
      );
      yield* sql`INSERT INTO work_task_action_review_decisions VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'approve', 'human', 'auth-session', NULL, 2, 't')`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_decisions VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'reject', 'human', 'auth-session', NULL, 3, 't')`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT OR REPLACE INTO work_task_action_review_decisions SELECT * FROM work_task_action_review_decisions`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE work_task_action_review_decisions SET decision = 'reject'`),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_releases VALUES ('review', 'a', 1, 1, 'fingerprint', 2, 'continuation', 't')`,
          ),
        ),
      );
      yield* sql`INSERT INTO work_task_action_review_releases VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'continuation', 't')`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_action_review_releases VALUES ('review', 'a', 1, 1, 'fingerprint', 1, 'another', 't')`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT OR REPLACE INTO work_task_action_review_releases SELECT * FROM work_task_action_review_releases`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(sql`DELETE FROM work_task_action_review_releases`)),
      );
      for (const key of ["uri", "hash"]) {
        for (const text of ["u\0" + "x".repeat(2000), "😀".repeat(1001)]) {
          const preview = JSON.stringify({
            content: "x",
            artifacts: [{ uri: "u", hash: "h", [key]: text }],
          });
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sql`INSERT INTO work_task_action_reviews SELECT 'bounds', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'bounds-cmd', 'bounds-req', rule_id, definition_revision, action_policy_revision, action, label, target, ${preview}, fingerprint, created_sequence, created_at FROM work_task_action_reviews WHERE review_id = 'review'`,
              ),
            ),
          );
        }
        for (const text of ["u\0" + "x".repeat(1998), "😀".repeat(1000)]) {
          const preview = JSON.stringify({
            content: "x",
            artifacts: [{ uri: "u", hash: "h", [key]: text }],
          });
          yield* sql`SAVEPOINT artifact_bounds`;
          yield* sql`INSERT INTO work_task_action_reviews SELECT 'bounds', task_id, revision, manager_generation, manager_thread_id, provider_session_id, run_id, 'bounds-cmd', 'bounds-req', rule_id, definition_revision, action_policy_revision, action, label, target, ${preview}, fingerprint, created_sequence, created_at FROM work_task_action_reviews WHERE review_id = 'review'`;
          yield* sql`ROLLBACK TO artifact_bounds`;
          yield* sql`RELEASE artifact_bounds`;
        }
      }
      assert.deepStrictEqual(yield* sql`SELECT preview_json FROM work_task_action_reviews`, [
        {
          preview_json: '{"content":"full payload","artifacts":[{"uri":"artifact","hash":"hash"}]}',
        },
      ]);
    }).pipe(Effect.provide(db)),
);

it.effect("upgrades existing tasks without inventing state and rolls back migration 85", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 84 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{ "original": true }', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'event', 't')`;
    const tasks = yield* sql`SELECT * FROM work_task_projections`;
    const definitions = yield* sql`SELECT * FROM work_task_accepted_definitions`;
    yield* sql`CREATE INDEX work_task_projections_updated_desc_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name IN ('work_task_current_state', 'work_task_view_fence')`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 85`,
      [],
    );
    yield* sql`DROP INDEX work_task_projections_updated_desc_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [85, "WorkTaskCurrentState"],
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_projections`, tasks);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_definitions`, definitions);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_current_state`, []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_view_fence`, [
      { singleton_id: 1, generation: 0, high_water_sequence: 0 },
    ]);
    yield* sql`INSERT INTO work_task_current_state (task_id) VALUES ('a')`;
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_current_state`, [
      {
        task_id: "a",
        configuration_json: null,
        desired_mode: null,
        execution_mode: null,
        manager_thread_id: null,
        manager_generation: null,
        attention_json: null,
        completion: null,
        archived: null,
        last_applied_sequence: null,
      },
    ]);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("constrains current state and the singleton fence and supports indexed lists", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_current_state VALUES ('a', '{"allowanceOverride":null}', 'active', 'running', 'manager', 1, '{"state":"none"}', 'incomplete', 0, 0)`;
    const invalid = [
      sql`UPDATE work_task_current_state SET manager_generation = 0`,
      sql`UPDATE work_task_current_state SET manager_generation = 9007199254740992`,
      sql`UPDATE work_task_current_state SET manager_thread_id = NULL`,
      sql`UPDATE work_task_current_state SET last_applied_sequence = -1`,
      sql`UPDATE work_task_current_state SET last_applied_sequence = 1.5`,
      sql`UPDATE work_task_current_state SET desired_mode = 'unknown'`,
      sql`UPDATE work_task_current_state SET archived = 2`,
      sql`UPDATE work_task_current_state SET configuration_json = 'invalid'`,
      sql`INSERT INTO work_task_current_state (task_id) VALUES ('missing')`,
      sql`INSERT INTO work_task_view_fence VALUES (2, 0, 0)`,
      sql`UPDATE work_task_view_fence SET generation = -1`,
      sql`UPDATE work_task_view_fence SET high_water_sequence = 9007199254740992`,
      sql`UPDATE work_task_view_fence SET generation = 1.5`,
    ];
    for (const statement of invalid) assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    const globalPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT task_id FROM work_task_projections WHERE (updated_at, task_id) < ('t', 'z') ORDER BY updated_at DESC, task_id DESC LIMIT 50`;
    const projectPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT task_id FROM work_task_projections WHERE project_id = 'p' AND (updated_at, task_id) < ('t', 'z') ORDER BY updated_at DESC, task_id DESC LIMIT 50`;
    assert.isTrue(
      globalPlan.some((row) => row.detail.includes("work_task_projections_updated_desc_idx")),
    );
    assert.isTrue(
      projectPlan.some((row) =>
        row.detail.includes("work_task_projections_project_updated_desc_idx"),
      ),
    );
    assert.isFalse(
      [...globalPlan, ...projectPlan].some((row) => row.detail.includes("TEMP B-TREE")),
    );
  }).pipe(Effect.provide(db)),
);

it.effect("upgrades waits atomically while preserving task state and link provenance", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 85 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't'), ('b', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_current_state (task_id) VALUES ('a')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_thread_links VALUES ('l1', 'a', 'thread', 't', '{"by":"user"}', 'u1', 'later', '{"by":"user"}'), ('l2', 'a', 'thread', 'later', '{}', NULL, NULL, NULL), ('l3', 'b', 'thread', 't', '{}', NULL, NULL, NULL)`;
    const links = yield* sql`SELECT * FROM work_task_thread_links ORDER BY link_event_id`;
    const state = yield* sql`SELECT * FROM work_task_current_state`;
    const fence = yield* sql`SELECT * FROM work_task_view_fence`;
    yield* sql`CREATE INDEX work_task_thread_links_reverse_page_idx ON work_task_thread_links(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_waits'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 86`,
      [],
    );
    yield* sql`DROP INDEX work_task_thread_links_reverse_page_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [86, "WorkTaskWaits"],
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM work_task_thread_links ORDER BY link_event_id`,
      links,
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_current_state`, state);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_view_fence`, fence);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_waits`, []);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("constrains attributable waits and supports bounded membership keysets", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    const targets = [
      '{"type":"source","sourceId":"source"}',
      '{"type":"run","threadId":"thread","runId":"run"}',
      '{"type":"result","result":{"type":"artifact","uri":"u","hash":"h"}}',
      '{"type":"result","result":{"type":"source_revision","repository":"r","revision":"v"}}',
      '{"type":"result","result":{"type":"run","threadId":"t","runId":"r"}}',
    ];
    for (const [index, target] of targets.entries()) {
      yield* sql`INSERT INTO work_task_waits VALUES ('a', ${String(index)}, 1, ${target}, 'registered', 1, 't')`;
    }
    for (const text of ["\0x", "\0"]) {
      const target = JSON.stringify({ type: "source", sourceId: text });
      yield* sql`INSERT INTO work_task_waits VALUES ('a', ${text}, 1, ${target}, 'registered', 1, 't')`;
      yield* sql`UPDATE work_task_waits SET wait_id = ${text + "y"}, target_json = ${JSON.stringify({ type: "source", sourceId: text + "y" })} WHERE wait_id = ${text}`;
    }
    for (const whitespace of [
      "\t",
      "\r\n",
      "\v\f",
      "\u00a0",
      "\u1680",
      "\u2000\u200a",
      "\u2028\u2029",
      "\u202f",
      "\u205f",
      "\u3000",
      "\ufeff",
    ]) {
      const target = JSON.stringify({ type: "source", sourceId: whitespace });
      for (const statement of [
        sql`INSERT INTO work_task_waits VALUES ('a', ${whitespace}, 1, ${targets[0]}, 'registered', 1, 't')`,
        sql`INSERT INTO work_task_waits VALUES ('a', 'blank', 1, ${target}, 'registered', 1, 't')`,
        sql`UPDATE work_task_waits SET wait_id = ${whitespace} WHERE wait_id = '0'`,
        sql`UPDATE work_task_waits SET target_json = ${target} WHERE wait_id = '0'`,
      ])
        assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    }
    const invalid = [
      sql`INSERT INTO work_task_waits VALUES ('a', '0', 1, ${targets[0]}, 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 2, ${targets[0]}, 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', '', 1, ${targets[0]}, 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, '{"type":"run","threadId":"t"}', 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, 'invalid', 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, '{}', 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, '{"type":"unknown"}', 'registered', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, ${targets[0]}, 'unknown', 1, 't')`,
      sql`INSERT INTO work_task_waits VALUES ('a', 'other', 1, ${targets[0]}, 'registered', -1, 't')`,
      sql`UPDATE work_task_waits SET target_json = ${JSON.stringify({ type: "source", sourceId: "😀".repeat(1001) })} WHERE wait_id = '0'`,
      sql`UPDATE work_task_waits SET wait_id = ${"x\0" + "x".repeat(1999)} WHERE wait_id = '0'`,
    ];
    for (const statement of invalid) assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    yield* sql`UPDATE work_task_waits SET target_json = ${JSON.stringify({ type: "source", sourceId: "😀".repeat(1000) })}, state = 'cleared', updated_sequence = 2 WHERE wait_id = '0'`;
    const waitPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT wait_id FROM work_task_waits WHERE task_id = 'a' AND state = 'registered' AND wait_id > '0' ORDER BY wait_id LIMIT 50`;
    const linkPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT link_event_id FROM work_task_thread_links WHERE task_id = 'a' AND unlink_event_id IS NULL AND (linked_at, link_event_id) < ('t', 'z') ORDER BY linked_at DESC, link_event_id DESC LIMIT 50`;
    const reversePlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT task_id FROM work_task_thread_links WHERE thread_id = 'thread' AND unlink_event_id IS NULL AND task_id > 'a' ORDER BY task_id LIMIT 50`;
    assert.isTrue(waitPlan.some((row) => row.detail.includes("work_task_waits_state_idx")));
    assert.isTrue(
      linkPlan.some((row) => row.detail.includes("work_task_thread_links_task_page_idx")),
    );
    assert.isTrue(
      reversePlan.some((row) => row.detail.includes("work_task_thread_links_reverse_page_idx")),
    );
    assert.isFalse(
      [...waitPlan, ...linkPlan, ...reversePlan].some((row) => row.detail.includes("TEMP B-TREE")),
    );
  }).pipe(Effect.provide(db)),
);

it.effect("upgrades result history atomically without changing predecessor data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 86 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_current_state (task_id) VALUES ('a')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_thread_links VALUES ('l', 'a', 'thread', 't', '{}', 'u', 'later', '{}')`;
    yield* sql`INSERT INTO work_task_waits VALUES ('a', 'w', 1, '{"type":"source","sourceId":"s"}', 'registered', 2, 't')`;
    yield* sql`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json) VALUES ('e', 'thread', 'thread', 1, 'thread.created', 't', 'client', '{}', '{}'), ('task-event', 'task', 'a', 1, 'task.created', 't', 'client', '{ \"task\": true }', '{}')`;
    const state = yield* sql`SELECT * FROM work_task_current_state`;
    const links = yield* sql`SELECT * FROM work_task_thread_links`;
    const waits = yield* sql`SELECT * FROM work_task_waits`;
    const fence = yield* sql`SELECT * FROM work_task_view_fence`;
    const events = yield* sql`SELECT * FROM orchestration_events`;
    yield* sql`CREATE INDEX work_task_results_history_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_results'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 87`,
      [],
    );
    yield* sql`DROP INDEX work_task_results_history_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [87, "WorkTaskResults"],
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_current_state`, state);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_thread_links`, links);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_waits`, waits);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_view_fence`, fence);
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_events`, events);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("enforces result provenance and contract bounds with indexed history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    const references = [
      { type: "run", threadId: "thread", runId: "run" },
      { type: "artifact", uri: "u", hash: "h" },
      { type: "source_revision", repository: "r", revision: "v" },
    ];
    const attribution = [
      { type: "server" },
      { type: "user", sessionId: "session", subject: "user" },
      { type: "agent", threadId: "thread", providerSessionId: "session", runId: "run" },
      {
        type: "manager",
        threadId: "manager",
        providerSessionId: "session",
        runId: "run",
        generation: 1,
      },
    ];
    for (const [i, caller] of attribution.entries()) {
      const thread = i === 0 ? null : "thread";
      yield* sql`INSERT INTO work_task_results VALUES (${`e${i}`}, 'a', ${`s${i}`}, ${thread}, ${JSON.stringify(caller)}, 'summary', ${JSON.stringify(references)}, 5, 't')`;
    }
    const batch = Array.from({ length: 32 }, () => ({
      type: "artifact",
      uri: "😀".repeat(1000),
      hash: "x".repeat(2000),
    }));
    yield* sql`INSERT INTO work_task_results VALUES ('large', 'a', ${"\0"}, NULL, '{"type":"server"}', ${"😀".repeat(1000)}, ${JSON.stringify(batch)}, 6, 't')`;
    for (const whitespace of [
      "\t",
      "\r\n",
      "\v\f",
      "\u00a0",
      "\u1680",
      "\u2000\u200a",
      "\u2028\u2029",
      "\u202f",
      "\u205f",
      "\u3000",
      "\ufeff",
    ]) {
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE work_task_results SET summary = ${whitespace} WHERE event_id = 'e0'`,
          ),
        ),
      );
    }
    yield* sql`UPDATE work_task_results SET results_json = ${JSON.stringify([{ type: "run", threadId: " \0 ", runId: "r" }])}, summary = ${" \0 "} WHERE event_id = 'e0'`;
    yield* sql`INSERT INTO work_task_results VALUES (${"i".repeat(2001)}, 'a', 's', NULL, '{"type":"server"}', 'ok', '[]', 7, 't')`;
    const invalid = [
      sql`INSERT INTO work_task_results VALUES (${"\u3000"}, 'a', 's', NULL, '{"type":"server"}', 'ok', '[]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('e0', 'a', 's', NULL, '{"type":"server"}', 'ok', '[]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('missing', 'missing', 's', NULL, '{"type":"server"}', 'ok', '[]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('bad', 'a', 's', NULL, '{"type":"user","sessionId":"s"}', 'ok', '[]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('bad', 'a', 's', NULL, '{"type":"manager","threadId":"t","providerSessionId":"s","runId":"r","generation":0}', 'ok', '[]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('bad', 'a', 's', NULL, '{"type":"server"}', 'ok', '[{"type":"run","threadId":"t"}]', 1, 't')`,
      sql`INSERT INTO work_task_results VALUES ('bad', 'a', 's', NULL, '{"type":"server"}', 'ok', ${JSON.stringify([...batch, batch[0]])}, 1, 't')`,
      sql`UPDATE work_task_results SET summary = ${"😀".repeat(1001)} WHERE event_id = 'e0'`,
      sql`UPDATE work_task_results SET source_id = ${"x\0" + "x".repeat(1999)} WHERE event_id = 'e0'`,
      sql`UPDATE work_task_results SET results_json = ${JSON.stringify([{ type: "artifact", uri: "u", hash: "\u3000" }])} WHERE event_id = 'e0'`,
      sql`UPDATE work_task_results SET attribution_json = ${JSON.stringify({ type: "user", sessionId: "s", subject: "\ufeff" })} WHERE event_id = 'e0'`,
      sql`UPDATE work_task_results SET contributor_thread_id = ${"\t\u00a0"} WHERE event_id = 'e0'`,
      sql`UPDATE work_task_results SET sequence = -1 WHERE event_id = 'e0'`,
    ];
    for (const statement of invalid) assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    const rows = yield* sql<{
      event_id: string;
    }>`SELECT event_id FROM work_task_results WHERE task_id = 'a' AND (sequence, event_id) < (6, 'large') ORDER BY sequence DESC, event_id DESC LIMIT 50`;
    assert.deepStrictEqual(
      rows.map((row) => row.event_id),
      ["e3", "e2", "e1", "e0"],
    );
    const plan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT event_id FROM work_task_results WHERE task_id = 'a' AND (sequence, event_id) < (6, 'large') ORDER BY sequence DESC, event_id DESC LIMIT 50`;
    assert.isTrue(plan.some((row) => row.detail.includes("work_task_results_history_idx")));
    assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
  }).pipe(Effect.provide(db)),
);

it.effect("upgrades claim history atomically and preserves prior task and ordinary data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 87 });
    yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at) VALUES ('p', 'Project', '/p', '[]', 't', 't')`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json) VALUES ('thread', 'p', 'Thread', 'codex', 'full-access', 'default', 't', 't', '{}')`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'accepted', 't')`;
    yield* sql`INSERT INTO work_task_current_state (task_id) VALUES ('a')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_thread_links VALUES ('link', 'a', 'thread', 't', '{}', 'unlink', 'later', '{}')`;
    yield* sql`INSERT INTO work_task_waits VALUES ('a', 'wait', 1, '{"type":"source","sourceId":"s"}', 'registered', 2, 't')`;
    yield* sql`INSERT INTO work_task_results VALUES ('result', 'a', 'source', NULL, '{"type":"server"}', 'summary', '[]', 3, 't')`;
    yield* sql`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json) VALUES ('ordinary', 'thread', 'thread', 1, 'thread.created', 't', 'client', '{ "ordinary": true }', '{}'), ('task-event', 'task', 'a', 1, 'task.created', 't', 'client', '{ "task": true }', '{}')`;
    const tables = [
      "projection_projects",
      "orchestration_v2_projection_threads",
      "orchestration_events",
      "work_task_projections",
      "work_task_accepted_definitions",
      "work_task_current_state",
      "work_task_manager_bindings",
      "work_task_view_fence",
      "work_task_thread_links",
      "work_task_waits",
      "work_task_results",
    ];
    const before = yield* Effect.forEach(tables, (name) => sql.unsafe(`SELECT * FROM ${name}`));
    yield* sql`CREATE INDEX work_task_claims_history_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_claims'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 88`,
      [],
    );
    yield* sql`DROP INDEX work_task_claims_history_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [88, "WorkTaskClaims"],
      [89, "WorkTaskValidationIdentity"],
    ]);
    assert.deepStrictEqual(
      yield* Effect.forEach(tables, (name) => sql.unsafe(`SELECT * FROM ${name}`)),
      before,
    );
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("constrains claim identity, evidence, bindings and indexed history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', 'p', 1, 1, 1, 1, '{}', 't', 't'), ('b', 'p', 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'accepted', 't'), ('b', 1, 1, '{}', '{}', 'accepted-b', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL), ('b', 1, 'other', 't', NULL)`;
    const references = [
      { type: "run", threadId: "thread", runId: "run" },
      { type: "artifact", uri: "u", hash: "h" },
      { type: "source_revision", repository: "r", revision: "v" },
    ];
    const batch = Array.from({ length: 32 }, () => ({
      type: "artifact",
      uri: "😀".repeat(1000),
      hash: "x".repeat(2000),
    }));
    yield* sql`INSERT INTO work_task_claims VALUES ('event1', 'a', 'claim-z', 'evidence1', 'manager', 1, 1, 1, 0, '{"type":"server"}', ${"😀".repeat(1000)}, ${JSON.stringify(batch)}, 5, 't')`;
    yield* sql`INSERT INTO work_task_claims VALUES ('event2', 'a', 'claim-a', 'evidence2', 'manager', 1, 1, 1, 2, '{"type":"manager","threadId":"manager","providerSessionId":"s","runId":"r","generation":1}', ${" \0 "}, ${JSON.stringify(references)}, 5, 't')`;
    yield* sql`INSERT INTO work_task_claims VALUES ('event-b', 'b', 'claim1', 'evidence1', 'other', 1, 1, 1, 0, '{"type":"server"}', 'summary', ${JSON.stringify(references)}, 1, 't')`;
    const invalid = [
      sql`INSERT INTO work_task_claims SELECT event_id, task_id, 'new', 'new', manager_thread_id, manager_generation, definition_revision, completion_policy_revision, observed_work_sequence, attribution_json, summary, evidence_json, sequence, recorded_at FROM work_task_claims WHERE event_id = 'event1'`,
      sql`INSERT INTO work_task_claims SELECT 'new', task_id, claim_id, 'new', manager_thread_id, manager_generation, definition_revision, completion_policy_revision, observed_work_sequence, attribution_json, summary, evidence_json, sequence, recorded_at FROM work_task_claims WHERE event_id = 'event1'`,
      sql`INSERT INTO work_task_claims SELECT 'new', task_id, 'new', evidence_id, manager_thread_id, manager_generation, definition_revision, completion_policy_revision, observed_work_sequence, attribution_json, summary, evidence_json, sequence, recorded_at FROM work_task_claims WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET task_id = 'b' WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET definition_revision = 2 WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET manager_generation = 2 WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET manager_thread_id = 'other' WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET completion_policy_revision = 0 WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET observed_work_sequence = -1 WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET sequence = -1 WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET claim_id = ${"x\0" + "x".repeat(1999)} WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET evidence_id = ${"😀".repeat(1001)} WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET summary = ${"😀".repeat(1001)} WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET evidence_json = '[]' WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET evidence_json = ${JSON.stringify([...batch, batch[0]])} WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET evidence_json = '[{"type":"run","threadId":"t"}]' WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET evidence_json = ${JSON.stringify([{ type: "artifact", uri: "u", hash: "\ufeff" }])} WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET attribution_json = '{"type":"user","sessionId":"s"}' WHERE event_id = 'event1'`,
      sql`UPDATE work_task_claims SET attribution_json = ${JSON.stringify({ type: "user", sessionId: "s", subject: "\u3000" })} WHERE event_id = 'event1'`,
    ];
    for (const statement of invalid) assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    for (const whitespace of [
      "\t",
      "\r\n",
      "\v\f",
      "\u00a0",
      "\u1680",
      "\u2000\u200a",
      "\u2028\u2029",
      "\u202f",
      "\u205f",
      "\u3000",
      "\ufeff",
    ]) {
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE work_task_claims SET summary = ${whitespace} WHERE event_id = 'event1'`,
          ),
        ),
      );
    }
    yield* sql`UPDATE work_task_claims SET evidence_json = ${JSON.stringify([{ type: "run", threadId: " \0 ", runId: "r" }])} WHERE event_id = 'event2'`;
    const rows = yield* sql<{
      claim_id: string;
    }>`SELECT claim_id FROM work_task_claims WHERE task_id = 'a' AND (sequence, event_id) < (6, 'z') ORDER BY sequence DESC, event_id DESC LIMIT 50`;
    assert.deepStrictEqual(
      rows.map((row) => row.claim_id),
      ["claim-a", "claim-z"],
    );
    const plan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT claim_id FROM work_task_claims WHERE task_id = 'a' AND (sequence, event_id) < (6, 'z') ORDER BY sequence DESC, event_id DESC LIMIT 50`;
    assert.isTrue(plan.some((row) => row.detail.includes("work_task_claims_history_idx")));
    assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves predecessor attempts and rolls back validation identity upgrade before retry",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 88 });
      yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'def', 't')`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
      yield* sql`INSERT INTO work_task_validation_attempts VALUES
      ('reserved', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, '1', NULL, NULL),
      ('started', 'a', 1, 1, 1, 'e', 'w', '{}', 'r1', 'started', NULL, '2', '2', NULL),
      ('result', 'a', 1, 1, 1, 'e', 'w', '{}', 'r2', 'result', '{}', '3', '3', '3'),
      ('uncertain', 'a', 1, 1, 1, 'e', 'w', '{}', 'r3', 'uncertain', NULL, '4', '4', '4')`;
      const attempts = yield* sql`SELECT * FROM work_task_validation_attempts ORDER BY attempt_id`;
      yield* sql`CREATE INDEX work_task_validation_state_history_idx ON work_task_projections(task_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_validation_identity'`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 89`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_validation_attempts ORDER BY attempt_id`,
        attempts,
      );
      yield* sql`DROP INDEX work_task_validation_state_history_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [[89, "WorkTaskValidationIdentity"]]);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_validation_attempts ORDER BY attempt_id`,
        attempts,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_identity`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(db)),
);

it.effect(
  "binds validation identity to matching attempts and claims and indexes stored state keys",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't'), ('b', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'd1', 't'), ('a', 2, 1, '{}', '{}', 'd2', 't'), ('b', 1, 1, '{}', '{}', 'd3', 't')`;
      yield* sql`INSERT INTO work_task_allowances VALUES ('a', 5, 0), ('b', 5, 0)`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'm1', 't', 't'), ('a', 2, 'm2', 't', NULL), ('b', 1, 'mb', 't', NULL)`;
      yield* sql`INSERT INTO work_task_claims VALUES
      ('c1', 'a', 'claim', 'e', 'm1', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't'),
      ('c2', 'a', 'other', 'other-e', 'm2', 2, 2, 2, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 2, 't'),
      ('c3', 'b', 'claim', 'e', 'mb', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`;
      yield* sql`INSERT INTO work_task_validation_attempts VALUES
      ('v1', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, '1', NULL, NULL),
      ('v2', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, '2026-11-11T00:00:00Z', NULL, NULL),
      ('v3', 'a', 2, 2, 2, 'other-e', 'w', '{}', NULL, 'reserved', NULL, '2026-11-11T00:00:00.1Z', NULL, NULL),
      ('vb', 'b', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, '2', NULL, NULL)`;
      yield* sql`DELETE FROM work_task_allowances WHERE task_id = 'b'`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_validation_identity VALUES ('vb', 'b', 'claim', 1)`,
          ),
        ),
      );
      yield* sql`INSERT INTO work_task_allowances VALUES ('b', 5, 0)`;
      yield* sql`INSERT INTO work_task_validation_identity VALUES ('v1', 'a', 'claim', 1), ('v3', 'a', 'other', 9007199254740991), ('vb', 'b', 'claim', 1)`;
      const invalid = [
        sql`INSERT OR REPLACE INTO work_task_validation_attempts VALUES ('v1', 'a', 2, 1, 1, 'changed', 'w', '{}', NULL, 'reserved', NULL, '1', NULL, NULL)`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('c1', 'a', 'claim', 'changed', 'm1', 1, 2, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('new-event', 'a', 'claim', 'changed', 'm1', 1, 2, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('new-event', 'a', 'new-claim', 'e', 'm1', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a', 1, 'changed', 't', 't')`,
        sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('b', 2, 'm1', 't', 't')`,
        sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a', 3, 'new-current', 't', NULL)`,
        sql`INSERT INTO work_task_validation_identity VALUES (NULL, 'a', 'claim', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', NULL, 'claim', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', NULL, 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('missing', 'a', 'claim', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'missing', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'b', 'claim', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'other', 2)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 1)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 0)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 1.5)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 9007199254740992)`,
        sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', NULL)`,
        sql`UPDATE work_task_validation_identity SET claim_id = 'other' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_identity SET attempt_id = 'missing' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_identity SET allowance_unit = 0 WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET attempt_id = 'new' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET task_id = 'b' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET definition_revision = 2 WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET manager_generation = 2 WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET completion_policy_revision = 2 WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET evidence_id = 'other-e' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_validation_attempts SET work_watermark = 'new' WHERE attempt_id = 'v1'`,
        sql`UPDATE work_task_claims SET task_id = 'b' WHERE event_id = 'c1'`,
        sql`UPDATE work_task_claims SET claim_id = 'new' WHERE event_id = 'c1'`,
        sql`UPDATE work_task_claims SET evidence_id = 'new' WHERE event_id = 'c1'`,
        sql`UPDATE work_task_claims SET definition_revision = 2 WHERE event_id = 'c1'`,
        sql`UPDATE work_task_claims SET manager_generation = 2, manager_thread_id = 'm2' WHERE event_id = 'c1'`,
        sql`UPDATE work_task_claims SET completion_policy_revision = 2 WHERE event_id = 'c1'`,
        sql`UPDATE work_task_manager_bindings SET thread_id = 'new' WHERE task_id = 'a' AND generation = 1`,
        sql`DELETE FROM work_task_validation_attempts WHERE attempt_id = 'v1'`,
        sql`DELETE FROM work_task_claims WHERE event_id = 'c1'`,
        sql`DELETE FROM work_task_allowances WHERE task_id = 'a'`,
      ];
      for (const statement of invalid) assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
      for (const [column, value] of [
        ["definition_revision", 2],
        ["manager_generation", 2],
        ["completion_policy_revision", 2],
        ["evidence_id", "other-e"],
      ] as const) {
        yield* sql.unsafe(
          `UPDATE work_task_validation_attempts SET ${column} = ? WHERE attempt_id = 'v2'`,
          [value],
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 2)`,
            ),
          ),
        );
        yield* sql.unsafe(
          `UPDATE work_task_validation_attempts SET ${column} = ? WHERE attempt_id = 'v2'`,
          [column === "evidence_id" ? "e" : 1],
        );
      }
      yield* sql`UPDATE work_task_validation_attempts SET state = 'started', started_at = 't', run_id = 'r' WHERE attempt_id = 'v1'`;
      yield* sql`UPDATE work_task_claims SET summary = 'updated' WHERE event_id = 'c1'`;
      yield* sql`INSERT INTO work_task_validation_identity VALUES ('v2', 'a', 'claim', 2)`;
      // Raw timestamp keys support lexical lookup, not chronological history.
      const rows = yield* sql<{
        attempt_id: string;
      }>`SELECT attempt_id FROM work_task_validation_attempts WHERE task_id = 'a' AND state = 'reserved' AND (created_at, attempt_id) < ('2026-11-12T00:00:00Z', 'z') ORDER BY created_at DESC, attempt_id DESC LIMIT 50`;
      assert.deepStrictEqual(
        rows.map((row) => row.attempt_id),
        ["v2", "v3"],
      );
      const plan = yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT attempt_id FROM work_task_validation_attempts WHERE task_id = 'a' AND state = 'reserved' AND (created_at, attempt_id) < ('2026-11-12T00:00:00Z', 'z') ORDER BY created_at DESC, attempt_id DESC LIMIT 50`;
      assert.isTrue(
        plan.some((row) => row.detail.includes("work_task_validation_state_history_idx")),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
    }).pipe(Effect.provide(db)),
);

it.effect("applies validation identity contract bounds without truncating NUL or UTF-16 text", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'd', 't')`;
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 5, 0)`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'm', 't', NULL)`;
    yield* sql`INSERT INTO work_task_claims VALUES ('c', 'a', 'claim', 'e', 'm', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`;
    for (const [attempt, watermark, valid] of [
      ["max", "😀".repeat(1000), true],
      ["nul", " \0 ", true],
      ["padded-max", " " + "x".repeat(2000) + " ", true],
      ["over-utf16", "😀".repeat(1001), false],
      ["over-nul", "x\0" + "x".repeat(1999), false],
      ["blank", "\t\u00a0\u1680\u2000\u200a\u2028\u2029\u202f\u205f\u3000\ufeff", false],
      ["😀".repeat(1001), "w", false],
      ["\u3000", "w", false],
      [" \0 ", "w", true],
    ] as const) {
      yield* sql`INSERT INTO work_task_validation_attempts VALUES (${attempt}, 'a', 1, 1, 1, 'e', ${watermark}, '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`;
      const result = yield* Effect.exit(
        sql`INSERT INTO work_task_validation_identity VALUES (${attempt}, 'a', 'claim', ${valid ? (attempt === "max" ? 1 : attempt === "nul" ? 2 : attempt === "padded-max" ? 3 : 4) : 5})`,
      );
      assert.strictEqual(Exit.isSuccess(result), valid);
    }
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('numeric', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, 't', NULL, NULL)`;
    for (const statement of [
      sql`UPDATE work_task_validation_attempts SET definition_revision = 9007199254740992 WHERE attempt_id = 'numeric'`,
      sql`UPDATE work_task_validation_attempts SET completion_policy_revision = 9007199254740992 WHERE attempt_id = 'numeric'`,
      sql`UPDATE work_task_validation_attempts SET manager_generation = 9007199254740992 WHERE attempt_id = 'numeric'`,
    ]) {
      // Parent constraints may reject the update before the companion is inserted.
      const updated = yield* Effect.exit(statement);
      if (Exit.isSuccess(updated)) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql`INSERT INTO work_task_validation_identity VALUES ('numeric', 'a', 'claim', 6)`,
            ),
          ),
        );
        yield* sql`UPDATE work_task_validation_attempts SET definition_revision = 1, completion_policy_revision = 1, manager_generation = 1 WHERE attempt_id = 'numeric'`;
      }
    }
  }).pipe(Effect.provide(db)),
);
