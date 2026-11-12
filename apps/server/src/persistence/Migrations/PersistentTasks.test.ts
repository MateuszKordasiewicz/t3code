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
        "work_task_current_state",
        "work_task_manager_bindings",
        "work_task_projections",
        "work_task_thread_links",
        "work_task_validation_attempts",
        "work_task_view_fence",
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
    assert.deepStrictEqual(yield* runMigrations(), [[85, "WorkTaskCurrentState"]]);
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
