import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import {
  WorkTaskPersistedEvent,
  WorkTaskPersistedAcceptedRunOwnershipEvent,
  WorkTaskPersistedCoordinationEvent,
  WorkTaskGraphEdgeRecord,
  WorkTaskGraphProvenance,
  WorkTaskGroupControlEvent,
  WorkTaskMembershipEventBatch,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { runMigrations } from "../Migrations.ts";

const db = NodeSqliteClient.layer({ filename: ":memory:" });
const decodePersistedEvent = Schema.decodeUnknownSync(WorkTaskPersistedEvent);

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
    assert.deepInclude(migrations, [90, "WorkTaskValidationReplay"]);
    assert.deepInclude(migrations, [91, "WorkTaskReviewOutcomes"]);
    assert.deepInclude(migrations, [92, "WorkTaskSharedManagerBindings"]);
    assert.deepInclude(migrations, [93, "WorkTaskDependencies"]);
    assert.deepInclude(migrations, [94, "WorkTaskManagerGroups"]);
    assert.deepInclude(migrations, [95, "WorkTaskManagerMemberships"]);
    assert.deepInclude(migrations, [96, "WorkTaskDirtyReconciliation"]);
    assert.deepInclude(migrations, [97, "WorkTaskAcceptedRuns"]);
    assert.deepInclude(migrations, [98, "WorkTaskAcceptedRunObservations"]);
    assert.deepInclude(migrations, [99, "WorkTaskManagerClaims"]);
    assert.deepInclude(migrations, [100, "WorkTaskOwnershipReleaseBridge"]);
    const tables = yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'work_task_%' ORDER BY name
    `;
    assert.deepStrictEqual(
      tables.map((row) => row.name),
      [
        "work_task_accepted_definitions",
        "work_task_accepted_run_debits",
        "work_task_accepted_run_references",
        "work_task_accepted_runs",
        "work_task_action_review_decisions",
        "work_task_action_review_releases",
        "work_task_action_reviews",
        "work_task_allowances",
        "work_task_claim_references",
        "work_task_claim_releases",
        "work_task_claim_uncertainties",
        "work_task_claims",
        "work_task_current_state",
        "work_task_dirty_events",
        "work_task_executable_claims",
        "work_task_graph_edges",
        "work_task_group_managers",
        "work_task_group_memberships",
        "work_task_manager_bindings",
        "work_task_manager_groups",
        "work_task_membership_companions",
        "work_task_projections",
        "work_task_ready_events",
        "work_task_ready_references",
        "work_task_ready_state",
        "work_task_reconciliation_events",
        "work_task_reconciliation_visits",
        "work_task_results",
        "work_task_review_events",
        "work_task_review_outcomes",
        "work_task_review_rowids",
        "work_task_review_status",
        "work_task_run_observation_references",
        "work_task_run_ownership_observations",
        "work_task_run_ownership_release_bridges",
        "work_task_thread_links",
        "work_task_validation_attempts",
        "work_task_validation_identity",
        "work_task_validation_instant",
        "work_task_validation_replay",
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
        [90, "WorkTaskValidationReplay"],
        [91, "WorkTaskReviewOutcomes"],
        [92, "WorkTaskSharedManagerBindings"],
        [93, "WorkTaskDependencies"],
        [94, "WorkTaskManagerGroups"],
        [95, "WorkTaskManagerMemberships"],
        [96, "WorkTaskDirtyReconciliation"],
        [97, "WorkTaskAcceptedRuns"],
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
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
          sql`INSERT INTO work_task_manager_bindings VALUES ('a', 2, 'manager1', 't', 't')`,
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
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
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 89 }), [
        [89, "WorkTaskValidationIdentity"],
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_validation_attempts ORDER BY attempt_id`,
        attempts,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_identity`, []);
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 89 }), []);
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
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 3, 'm3', 't', 't'), ('a', 4, 'm4', 't', 't')`;
      yield* sql`INSERT INTO work_task_claims VALUES ('free-event', 'a', 'free-claim', 'free-evidence', 'm3', 3, 2, 2, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 3, 't')`;
      yield* sql`INSERT INTO work_task_validation_attempts VALUES ('free-attempt', 'a', 2, 3, 2, 'free-evidence', 'w', '{}', 'free-run', 'started', NULL, 't', 't', NULL)`;
      const invalid = [
        sql`UPDATE OR REPLACE work_task_validation_attempts SET attempt_id = 'v1' WHERE attempt_id = 'free-attempt'`,
        sql`UPDATE OR REPLACE work_task_claims SET claim_id = 'claim' WHERE event_id = 'free-event'`,
        sql`UPDATE OR REPLACE work_task_claims SET event_id = 'c1', claim_id = 'claim' WHERE event_id = 'free-event'`,
        sql`UPDATE OR REPLACE work_task_claims SET event_id = 'c1' WHERE event_id = 'free-event'`,
        sql`UPDATE OR REPLACE work_task_claims SET evidence_id = 'e' WHERE event_id = 'free-event'`,
        sql`UPDATE OR REPLACE work_task_manager_bindings SET generation = 1 WHERE task_id = 'a' AND generation = 4`,
        sql`UPDATE OR REPLACE work_task_manager_bindings SET thread_id = 'm1' WHERE task_id = 'a' AND generation = 4`,
        sql`UPDATE OR REPLACE work_task_manager_bindings SET retired_at = NULL WHERE task_id = 'a' AND generation = 4`,
        sql`INSERT OR REPLACE INTO work_task_validation_attempts VALUES ('v1', 'a', 2, 1, 1, 'changed', 'w', '{}', NULL, 'reserved', NULL, '1', NULL, NULL)`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('c1', 'a', 'claim', 'changed', 'm1', 1, 2, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('new-event', 'a', 'claim', 'changed', 'm1', 1, 2, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_claims VALUES ('new-event', 'a', 'new-claim', 'e', 'm1', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`,
        sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a', 1, 'changed', 't', 't')`,
        sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a', 5, 'm1', 't', 't')`,
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

it.effect("preserves legacy validation rows and rolls replay migration back before retry", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 89 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'd', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'm', 't', NULL)`;
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 1, 0)`;
    yield* sql`INSERT INTO work_task_claims VALUES ('c', 'a', 'claim', 'e', 'm', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`;
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('legacy', 'a', 1, 1, 1, 'e', 'w', '{}', NULL, 'reserved', NULL, '2026-11-11T00:00:00.000001+01:00', NULL, NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('legacy', 'a', 'claim', 1)`;
    const before = yield* sql`SELECT * FROM work_task_validation_attempts`;
    const claims = yield* sql`SELECT * FROM work_task_claims`;
    const identities = yield* sql`SELECT * FROM work_task_validation_identity`;
    yield* sql`CREATE INDEX work_task_validation_chronological_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_validation_replay'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 90`,
      [],
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_attempts`, before);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_claims`, claims);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_identity`, identities);
    yield* sql`DROP INDEX work_task_validation_chronological_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [90, "WorkTaskValidationReplay"],
      [91, "WorkTaskReviewOutcomes"],
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_attempts`, before);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_claims`, claims);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_identity`, identities);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_replay`, []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_validation_instant`, []);
    assert.deepStrictEqual(yield* runMigrations(), []);
  }).pipe(Effect.provide(db)),
);

it.effect("stores validation phases and indexes sequence and lossless chronological keys", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'd', 't')`;
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 10, 0)`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'm', 't', NULL)`;
    yield* sql`INSERT INTO work_task_claims VALUES ('claim-event', 'a', 'claim', 'e', 'm', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 1, 't')`;
    const validator = {
      projectId: "p",
      modelSelection: { instanceId: "codex", model: "m" },
      workspace: { type: "root" },
    };
    const validatorJson = JSON.stringify(validator);
    const time = "2026-11-11T00:00:00Z";
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('v', 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('v', 'a', 'claim', 1)`;
    const common = {
      taskId: "a",
      attemptId: "v",
      commandId: "cmd",
      attribution: { type: "server" },
      definitionRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
      managerGeneration: 1,
      claimId: "claim",
      evidenceId: "e",
      workWatermark: "w",
      validator,
      allowanceUnit: 1,
      createdAt: time,
      occurredAt: time,
    };
    const reserved = { ...common, id: "ev1", type: "task.validation-reserved" };
    const started = {
      ...common,
      id: "ev2",
      type: "task.validation-started",
      runId: "run",
      startedAt: time,
    };
    const result = {
      ...started,
      id: "ev3",
      type: "task.validation-result",
      finishedAt: time,
      outcome: "pass",
      findings: [
        {
          criterionId: "c",
          outcome: "pass",
          reason: "yes",
          evidence: [{ type: "artifact", uri: "file", hash: "hash" }],
        },
      ],
    };
    const uncertain = {
      ...started,
      id: "ev4",
      type: "task.validation-uncertain",
      finishedAt: time,
      reason: "lost",
    };
    const record = <T extends { id: string; attemptId: string }>(event: T, sequence: number) => {
      const payload = JSON.stringify(event);
      decodePersistedEvent({
        event,
        context: { sequence, eventId: event.id, occurredAt: time, commandId: "cmd" },
      });
      return sql`INSERT INTO work_task_validation_replay VALUES (${event.id}, ${event.attemptId}, 'a', ${sequence}, 'cmd', ${time}, ${payload})`;
    };
    yield* record(reserved, 1);
    yield* sql`INSERT INTO work_task_validation_instant VALUES ('v', 'a', 'reserved', 1794355200, '')`;
    yield* sql`UPDATE work_task_validation_attempts SET state = 'started', run_id = 'run', started_at = ${time} WHERE attempt_id = 'v'`;
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM work_task_validation_instant WHERE attempt_id = 'v'`,
      [],
    );
    yield* record(started, 2);
    yield* sql`UPDATE work_task_validation_attempts SET state = 'result', finished_at = ${time}, outcome_json = '{}' WHERE attempt_id = 'v'`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(record(result, 3))));
    yield* sql`UPDATE work_task_validation_attempts SET outcome_json = '{"outcome":"fail","findings":[]}' WHERE attempt_id = 'v'`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(record(result, 3))));
    const outcome = { outcome: result.outcome, findings: result.findings };
    yield* sql`UPDATE work_task_validation_attempts SET outcome_json = ${JSON.stringify(outcome)} WHERE attempt_id = 'v'`;
    yield* record(result, 3);
    const recordResult = Effect.fn("PersistentTasks.recordResult")(function* (
      event: typeof result & { reason?: string },
      sequence: number,
    ) {
      const attemptId = event.id;
      const runId = `run-${event.id}`;
      const allowanceUnit = sequence + 20;
      const eventOutcome = JSON.stringify({
        outcome: event.outcome,
        findings: event.findings,
        reason: event.reason,
      });
      yield* sql`INSERT INTO work_task_validation_attempts VALUES (${attemptId}, 'a', 1, 1, 1, 'e', 'w', ${JSON.stringify(event.validator)}, ${runId}, 'result', ${eventOutcome}, ${time}, ${time}, ${time})`;
      yield* sql`INSERT INTO work_task_validation_identity VALUES (${attemptId}, 'a', 'claim', ${allowanceUnit})`;
      yield* record({ ...event, attemptId, runId, allowanceUnit }, sequence);
    });
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('u', 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('u', 'a', 'claim', 2)`;
    yield* record({ ...reserved, id: "ev4", attemptId: "u", allowanceUnit: 2 }, 4);
    yield* sql`UPDATE work_task_validation_attempts SET state = 'started', run_id = 'run-u', started_at = ${time} WHERE attempt_id = 'u'`;
    yield* record({ ...started, id: "ev5", attemptId: "u", allowanceUnit: 2, runId: "run-u" }, 5);
    yield* sql`UPDATE work_task_validation_attempts SET state = 'uncertain', finished_at = ${time} WHERE attempt_id = 'u'`;
    yield* record({ ...uncertain, id: "ev6", attemptId: "u", allowanceUnit: 2, runId: "run-u" }, 6);
    const bad = [
      { ...reserved, id: "bad", runId: "run" },
      { ...result, id: "bad", findings: [] },
      { ...result, id: "bad", outcome: "error", findings: [] },
      { ...result, id: "bad", findings: [{ ...result.findings[0], reason: " " }] },
      { ...uncertain, id: "bad", reason: null },
      { ...reserved, id: "bad", claimId: "other" },
      { ...started, id: "bad", runId: "another-run" },
      { ...reserved, id: "bad", definitionRevision: 1.5 },
      { ...result, id: "bad", reason: "😀".repeat(1001) },
      { ...result, id: "bad", findings: [result.findings[0], result.findings[0]] },
      { ...result, id: "bad", findings: Array(65).fill(result.findings[0]) },
      { ...reserved, id: "bad", validator: { ...validator, workspace: { type: "other" } } },
    ];
    for (const event of bad) {
      const payload = JSON.stringify(event);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_validation_replay VALUES ('bad', 'v', 'a', 9, 'cmd', ${time}, ${payload})`,
          ),
        ),
      );
    }
    yield* recordResult({ ...result, id: "nul", reason: " \0 " }, 7);
    yield* recordResult({ ...result, id: "utf16", reason: "😀".repeat(1000) }, 8);
    const manyFindings = Array.from({ length: 64 }, (_, i) => ({
      criterionId: `criterion-${i}`,
      outcome: "inconclusive" as const,
      reason: "reason",
      evidence: Array.from({ length: 32 }, () => ({
        type: "artifact" as const,
        uri: "file",
        hash: "hash",
      })),
    }));
    yield* recordResult(
      { ...result, id: "large-findings", outcome: "inconclusive", findings: manyFindings },
      9,
    );
    for (const [index, [instanceId, valid]] of [
      ["x".repeat(64), true],
      ["a/b", false],
      ["x".repeat(65), false],
    ].entries()) {
      const attemptId = `slug-${index}`;
      const selected = {
        ...validator,
        modelSelection: { ...validator.modelSelection, instanceId },
      };
      yield* sql`INSERT INTO work_task_validation_attempts VALUES (${attemptId}, 'a', 1, 1, 1, 'e', 'w', ${JSON.stringify(selected)}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
      yield* sql`INSERT INTO work_task_validation_identity VALUES (${attemptId}, 'a', 'claim', ${100 + index})`;
      const payload = JSON.stringify({
        ...reserved,
        id: attemptId,
        attemptId,
        validator: selected,
        allowanceUnit: 100 + index,
      });
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`INSERT INTO work_task_validation_replay VALUES (${attemptId}, ${attemptId}, 'a', 0, 'cmd', ${time}, ${payload})`,
          ),
        ),
        valid,
      );
      if (valid) yield* sql`DELETE FROM work_task_validation_replay WHERE event_id = ${attemptId}`;
    }
    const duplicateFindings = [
      result.findings[0],
      { ...result.findings[0], criterionId: " \t c \u3000" },
    ];
    const duplicateOutcome = JSON.stringify({ outcome: "pass", findings: duplicateFindings });
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('trimmed-findings', 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, 'trimmed-run', 'result', ${duplicateOutcome}, ${time}, ${time}, ${time})`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('trimmed-findings', 'a', 'claim', 110)`;
    const duplicateEvent = {
      ...result,
      id: "trimmed-findings-event",
      attemptId: "trimmed-findings",
      runId: "trimmed-run",
      allowanceUnit: 110,
      findings: duplicateFindings,
    };
    assert.throws(() =>
      decodePersistedEvent({
        event: duplicateEvent,
        context: { sequence: 20, eventId: duplicateEvent.id, occurredAt: time, commandId: "cmd" },
      }),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_validation_replay VALUES ('trimmed-findings-event', 'trimmed-findings', 'a', 20, 'cmd', ${time}, ${JSON.stringify(duplicateEvent)})`,
        ),
      ),
    );
    const reorderedValidator = {
      workspace: validator.workspace,
      modelSelection: validator.modelSelection,
      projectId: validator.projectId,
    };
    yield* recordResult({ ...result, id: "reordered", validator: reorderedValidator }, 10);
    const emptyModel = { ...validator, modelSelection: { ...validator.modelSelection, model: "" } };
    const maxValidator = {
      ...validator,
      modelSelection: {
        ...validator.modelSelection,
        model: "x".repeat(16000 - JSON.stringify(emptyModel).length),
      },
    };
    assert.strictEqual(new TextEncoder().encode(JSON.stringify(maxValidator)).length, 16000);
    const maxValidatorJson = JSON.stringify(maxValidator);
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('max-validator', 'a', 1, 1, 1, 'e', 'w', ${maxValidatorJson}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('max-validator', 'a', 'claim', 3)`;
    yield* record(
      {
        ...reserved,
        id: "max-validator-event",
        attemptId: "max-validator",
        allowanceUnit: 3,
        validator: maxValidator,
      },
      11,
    );
    const overValidator = {
      ...maxValidator,
      modelSelection: {
        ...maxValidator.modelSelection,
        model: maxValidator.modelSelection.model + "x",
      },
    };
    const overValidatorJson = JSON.stringify(overValidator);
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('over-validator', 'a', 1, 1, 1, 'e', 'w', ${overValidatorJson}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('over-validator', 'a', 'claim', 4)`;
    const overEvent = JSON.stringify({
      ...reserved,
      id: "over-validator-event",
      attemptId: "over-validator",
      allowanceUnit: 4,
      validator: overValidator,
    });
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_validation_replay VALUES ('over-validator-event', 'over-validator', 'a', 12, 'cmd', ${time}, ${overEvent})`,
        ),
      ),
    );
    assert.isTrue(
      Exit.isFailure(yield* Effect.exit(record({ ...reserved, id: "duplicate-sequence" }, 11))),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(record({ ...reserved, id: "missing", attemptId: "missing" }, 12)),
      ),
    );
    for (const statement of [
      sql`UPDATE work_task_validation_attempts SET outcome_json = '{"outcome":"fail","findings":[]}' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_attempts SET created_at = 'changed' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_attempts SET validator_json = '{}' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_attempts SET run_id = 'other' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_attempts SET started_at = 'other' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_attempts SET finished_at = 'other' WHERE attempt_id = 'v'`,
      sql`UPDATE work_task_validation_identity SET allowance_unit = 3 WHERE attempt_id = 'v'`,
      sql`INSERT OR REPLACE INTO work_task_validation_identity VALUES ('v', 'a', 'claim', 3)`,
      sql`DELETE FROM work_task_validation_identity WHERE attempt_id = 'v'`,
    ])
      assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    for (const [attemptId, unit] of [
      ["\ud800", 5],
      ["\ufffd", 6],
    ] as const) {
      const encodedId = JSON.stringify(attemptId);
      yield* sql`INSERT INTO work_task_validation_attempts VALUES (json_extract(${encodedId}, '$'), 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, NULL, 'reserved', NULL, ${time}, NULL, NULL)`;
      yield* sql`INSERT INTO work_task_validation_identity VALUES (json_extract(${encodedId}, '$'), 'a', 'claim', ${unit})`;
      const event = { ...reserved, id: `unicode-${unit}`, attemptId, allowanceUnit: unit };
      decodePersistedEvent({
        event,
        context: { sequence: unit + 7, eventId: event.id, occurredAt: time, commandId: "cmd" },
      });
      const payload = JSON.stringify(event);
      yield* sql`INSERT INTO work_task_validation_replay VALUES (${event.id}, json_extract(${encodedId}, '$'), 'a', ${unit + 7}, 'cmd', ${time}, ${payload})`;
    }
    const unicode = yield* sql<{
      encoded: string;
      event_json: string;
    }>`SELECT hex(attempt_id) AS encoded, event_json FROM work_task_validation_replay WHERE event_id IN ('unicode-5', 'unicode-6') ORDER BY event_id`;
    assert.deepStrictEqual(
      unicode.map((row) => row.encoded),
      ["EDA080", "EFBFBD"],
    );
    assert.deepStrictEqual(
      unicode.map((row) => JSON.parse(row.event_json).attemptId),
      ["\ud800", "\ufffd"],
    );
    const firstHistory = yield* sql<{
      event_id: string;
      attempt_hex: string;
      sequence: number;
    }>`SELECT event_id, hex(attempt_id) AS attempt_hex, sequence FROM work_task_validation_replay WHERE task_id = 'a' ORDER BY sequence DESC, attempt_id DESC LIMIT 2`;
    assert.deepStrictEqual(
      firstHistory.map((row) => row.event_id),
      ["unicode-6", "unicode-5"],
    );
    const historyCursor = firstHistory[1]!;
    const nextHistory = yield* sql<{
      event_id: string;
    }>`SELECT event_id FROM work_task_validation_replay WHERE task_id = 'a' AND (sequence, attempt_id) < (${historyCursor.sequence}, CAST(unhex(${historyCursor.attempt_hex}) AS TEXT)) ORDER BY sequence DESC, attempt_id DESC LIMIT 2`;
    assert.deepStrictEqual(
      nextHistory.map((row) => row.event_id),
      ["max-validator-event", "reordered"],
    );
    const historyPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT attempt_id FROM work_task_validation_replay WHERE task_id = 'a' AND (sequence, attempt_id) < (${historyCursor.sequence}, CAST(unhex(${historyCursor.attempt_hex}) AS TEXT)) ORDER BY sequence DESC, attempt_id DESC LIMIT 50`;
    assert.isTrue(
      historyPlan.some((row) => row.detail.includes("work_task_validation_replay_history_idx")),
    );
    assert.isFalse(historyPlan.some((row) => row.detail.includes("TEMP B-TREE")));

    const maxPrecision = "1" + "0".repeat(1977) + "1";
    const timestamps = [
      ["zero", "2026-11-11T00:00:00Z"],
      ["equal-a", "2026-11-11T01:00:00.1000+01:00"],
      ["equal-b", "2026-11-11T00:00:00.1Z"],
      ["small", "2026-11-11T00:00:00.000000000000000000001Z"],
      ["large", "2026-11-11T00:00:00.100000000000000000001Z"],
      ["full-precision", `2026-11-11T00:00:00.${maxPrecision}Z`],
    ] as const;
    for (const [id, raw] of timestamps) {
      const seconds = Math.floor(Date.parse(raw.replace(/\.\d+/, "")) / 1000);
      const fraction = (raw.match(/\.(\d+)/)?.[1] ?? "").replace(/0+$/, "");
      yield* sql`INSERT INTO work_task_validation_attempts VALUES (${id}, 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, NULL, 'reserved', NULL, ${raw}, NULL, NULL)`;
      yield* sql`INSERT INTO work_task_validation_instant VALUES (${id}, 'a', 'reserved', ${seconds}, ${fraction})`;
    }
    for (const fraction of ["10", "1x", "1\0x"]) {
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_validation_instant VALUES ('v', 'a', 'result', 0, ${fraction})`,
          ),
        ),
      );
    }
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_validation_instant VALUES ('v', 'a', 'reserved', 0, '')`,
        ),
      ),
    );
    assert.strictEqual(`2026-11-11T00:00:00.${maxPrecision}Z`.length, 2000);
    const rows = yield* sql<{
      attempt_id: string;
      utc_seconds: number;
      fraction: string;
    }>`SELECT attempt_id, utc_seconds, fraction FROM work_task_validation_instant WHERE task_id = 'a' AND state = 'reserved' ORDER BY utc_seconds DESC, fraction DESC, attempt_id DESC LIMIT 3`;
    assert.deepStrictEqual(
      rows.map((row) => row.attempt_id),
      ["large", "full-precision", "equal-b"],
    );
    const cursor = rows[2]!;
    const next = yield* sql<{
      attempt_id: string;
    }>`SELECT attempt_id FROM work_task_validation_instant WHERE task_id = 'a' AND state = 'reserved' AND (utc_seconds, fraction, attempt_id) < (${cursor.utc_seconds}, ${cursor.fraction}, ${cursor.attempt_id}) ORDER BY utc_seconds DESC, fraction DESC, attempt_id DESC LIMIT 3`;
    assert.deepStrictEqual(
      next.map((row) => row.attempt_id),
      ["equal-a", "small", "zero"],
    );
    const wholeSeconds = Math.floor(Date.parse(time) / 1000);
    for (const attemptId of ["\ud800", "\ufffd"]) {
      const encodedId = JSON.stringify(attemptId);
      yield* sql`INSERT INTO work_task_validation_instant VALUES (json_extract(${encodedId}, '$'), 'a', 'reserved', ${wholeSeconds}, '')`;
    }
    const unicodePage = yield* sql<{
      attempt_hex: string;
    }>`SELECT hex(attempt_id) AS attempt_hex FROM work_task_validation_instant WHERE task_id = 'a' AND state = 'reserved' AND utc_seconds = ${wholeSeconds} AND fraction = '' ORDER BY utc_seconds DESC, fraction DESC, attempt_id DESC LIMIT 2`;
    assert.deepStrictEqual(
      unicodePage.map((row) => row.attempt_hex),
      ["EFBFBD", "EDA080"],
    );
    const unicodeCursor = unicodePage[1]!;
    const unicodeNext = yield* sql<{
      attempt_hex: string;
    }>`SELECT hex(attempt_id) AS attempt_hex FROM work_task_validation_instant WHERE task_id = 'a' AND state = 'reserved' AND (utc_seconds, fraction, attempt_id) < (${wholeSeconds}, '', CAST(unhex(${unicodeCursor.attempt_hex}) AS TEXT)) ORDER BY utc_seconds DESC, fraction DESC, attempt_id DESC LIMIT 2`;
    assert.deepStrictEqual(
      unicodeNext.map((row) => row.attempt_hex),
      ["7A65726F"],
    );
    const plan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT attempt_id FROM work_task_validation_instant WHERE task_id = 'a' AND state = 'reserved' AND (utc_seconds, fraction, attempt_id) < (${wholeSeconds}, '', CAST(unhex(${unicodeCursor.attempt_hex}) AS TEXT)) ORDER BY utc_seconds DESC, fraction DESC, attempt_id DESC LIMIT 50`;
    assert.isTrue(
      plan.some((row) => row.detail.includes("work_task_validation_chronological_idx")),
    );
    assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT OR REPLACE INTO work_task_validation_attempts VALUES ('zero', 'a', 1, 1, 1, 'e', 'w', ${validatorJson}, 'legacy-run', 'started', NULL, ${time}, ${time}, NULL)`,
        ),
      ),
    );
    yield* sql`UPDATE work_task_validation_attempts SET state = 'started', run_id = 'legacy-run', started_at = ${time} WHERE attempt_id = 'zero'`;
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM work_task_validation_instant WHERE attempt_id = 'zero'`,
      [],
    );
    yield* sql`INSERT INTO work_task_validation_instant VALUES ('zero', 'a', 'started', ${wholeSeconds}, '')`;
    assert.deepStrictEqual(
      (yield* sql<{
        state: string;
      }>`SELECT state FROM work_task_validation_instant WHERE attempt_id = 'zero'`).map(
        (row) => row.state,
      ),
      ["started"],
    );
  }).pipe(Effect.provide(db)),
);

it.effect("preserves review evidence on upgrade and binds append-only outcomes and status", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const reject = Effect.fn("PersistentTasks.reject")(function* (
      statement: Effect.Effect<unknown, SqlError>,
    ) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
    });
    yield* runMigrations({ toMigrationInclusive: 90 });
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`INSERT INTO work_task_projections VALUES ('a', NULL, 1, 1, 1, 1, '{}', 't', 't')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a', 1, 1, '{}', '{}', 'def', 't')`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a', 1, 'manager', 't', NULL)`;
    yield* sql`INSERT INTO work_task_action_reviews (rowid,review_id,task_id,revision,manager_generation,manager_thread_id,provider_session_id,run_id,command_id,runtime_request_id,rule_id,definition_revision,action_policy_revision,action,label,target,preview_json,fingerprint,created_sequence,created_at) VALUES
      (-1,'review', 'a', 1, 1, 'manager', 'session', 'run', 'command', 'request', 'rule', 1, 1,
       'send', 'label', 'target', '{"content":"payload","artifacts":[]}', 'fingerprint', 1, '2026-11-12T00:00:00Z')`;
    yield* sql`INSERT INTO work_task_action_review_decisions (rowid,review_id,task_id,review_revision,action_policy_revision,fingerprint,manager_generation,decision,human_subject,human_session_id,feedback,resolved_sequence,resolved_at) VALUES (-1,'review', 'a', 1, 1, 'fingerprint', 1, 'reject', 'human', 'session', 'feedback', 2, 't')`;
    yield* sql`INSERT INTO work_task_action_review_releases (rowid,review_id,task_id,review_revision,action_policy_revision,fingerprint,manager_generation,continuation_id,created_at) VALUES (-1,'review', 'a', 1, 1, 'fingerprint', 1, 'continuation', 't')`;
    const reviews = yield* sql`SELECT * FROM work_task_action_reviews`;
    const decisions = yield* sql`SELECT * FROM work_task_action_review_decisions`;
    const releases = yield* sql`SELECT * FROM work_task_action_review_releases`;
    yield* sql`CREATE INDEX work_task_reviews_history_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name = 'work_task_review_outcomes'`,
      [],
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 91`,
      [],
    );
    yield* sql`DROP INDEX work_task_reviews_history_idx`;
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 91 }), [
      [91, "WorkTaskReviewOutcomes"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_action_reviews`, reviews);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_action_review_decisions`, decisions);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_action_review_releases`, releases);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_review_outcomes`, []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_review_status`, []);
    const binding = {
      taskId: "a",
      reviewId: "review",
      reviewRevision: 1,
      definitionRevision: 1,
      actionPolicyRevision: 1,
      managerGeneration: 1,
      managerThreadId: "manager",
      providerSessionId: "session",
      runtimeRequestId: "request",
      runId: "run",
      ruleId: "rule",
      payloadIdentity: { action: "send", target: "target", fingerprint: "fingerprint" },
    };
    const base = {
      id: "event",
      taskId: "a",
      occurredAt: "2026-11-12T00:00:00Z",
      attribution: { type: "server" },
      definitionRevision: 2,
      actionPolicyRevision: 2,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      type: "task.action-review-invalidated",
      cause: "policy_changed",
      reason: "changed",
      binding,
    };
    const event = JSON.stringify(base);
    const decisionCollision = JSON.stringify({ ...base, id: "decision-sequence-collision" });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('decision-sequence-collision','a','review',2,'2026-11-12T00:00:00Z',NULL,${decisionCollision})`,
    );

    assert.isDefined(
      decodePersistedEvent({
        event: base,
        context: {
          sequence: 3,
          eventId: "event",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: null,
        },
      }),
    );
    yield* sql`INSERT INTO work_task_review_outcomes VALUES ('event', 'a', 'review', 3, '2026-11-12T00:00:00Z', NULL, ${event})`;
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes (rowid,event_id,task_id,review_id,sequence,occurred_at,command_id,event_json) SELECT rowid,'rowid-replacement',task_id,review_id,4,occurred_at,command_id,json_set(event_json,'$.id','rowid-replacement') FROM work_task_review_outcomes WHERE event_id = 'event'`,
    );
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes SELECT * FROM work_task_review_outcomes`,
    );
    yield* reject(sql`UPDATE work_task_review_outcomes SET sequence = 4`);
    yield* reject(sql`DELETE FROM work_task_review_outcomes`);
    for (const invalid of [
      { ...base, id: "bad", binding: { ...binding, runtimeRequestId: "other" } },
      {
        ...base,
        id: "bad",
        binding: {
          ...binding,
          payloadIdentity: { ...binding.payloadIdentity, fingerprint: "other" },
        },
      },
      { ...base, id: "bad", reason: "\u00a0\ufeff" },
      { ...base, id: "bad", reason: "😀".repeat(1001) },
      { ...base, id: "bad", reason: "\u0000" + "x".repeat(2000) },
      { ...base, id: "bad", binding: { ...binding, reviewRevision: "1" } },
      { ...base, id: "bad", binding: { ...binding, runtimeRequestId: 123 } },
      { ...base, id: "bad", cause: "timeout" },
    ]) {
      const value = JSON.stringify(invalid);
      yield* reject(
        sql`INSERT INTO work_task_review_outcomes VALUES ('bad', 'a', 'review', 4, '2026-11-12T00:00:00Z', NULL, ${value})`,
      );
    }
    const requested = JSON.stringify({
      id: "requested",
      taskId: "a",
      occurredAt: "2026-11-12T00:00:00Z",
      attribution: { type: "server" },
      type: "task.action-review-requested",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      review: {
        taskId: "a",
        reviewId: "review",
        runtimeRequestId: "request",
        continuationId: "continuation",
        createdSequence: 1,
        createdAt: "2026-11-12T00:00:00Z",
        revision: 1,
        managerGeneration: 1,
        managerThreadId: "manager",
        providerSessionId: "session",
        runId: "run",
        ruleId: "rule",
        definitionRevision: 1,
        actionPolicyRevision: 1,
        action: "send",
        label: "label",
        target: "target",
        fingerprint: "fingerprint",
        state: { type: "pending" },
        preview: { content: "payload", artifacts: [] },
      },
    });
    assert.isDefined(
      decodePersistedEvent({
        event: JSON.parse(requested),
        context: {
          sequence: 1,
          eventId: "requested",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: "command",
        },
      }),
    );
    for (const occurredAt of ["not-a-timestamp", "\0", "2026-11-13T00:00:00Z"]) {
      const badTime = { ...JSON.parse(requested), occurredAt };
      assert.throws(() =>
        decodePersistedEvent({
          event: badTime,
          context: { sequence: 1, eventId: "requested", occurredAt, commandId: "command" },
        }),
      );
      yield* reject(
        sql`INSERT INTO work_task_review_events VALUES ('requested','a','review',1,${occurredAt},'command',${JSON.stringify(badTime)})`,
      );
    }
    const wrongContinuation = JSON.stringify({
      ...JSON.parse(requested),
      review: { ...JSON.parse(requested).review, continuationId: "reserved-other" },
    });
    yield* reject(
      sql`INSERT INTO work_task_review_events VALUES ('requested','a','review',1,'2026-11-12T00:00:00Z','command',${wrongContinuation})`,
    );
    yield* sql`INSERT INTO work_task_allowances VALUES ('a', 10, 0)`;
    yield* sql`INSERT INTO work_task_claims VALUES ('validation-claim', 'a', 'validation-claim', 'evidence', 'manager', 1, 1, 1, 0, '{"type":"server"}', 's', '[{"type":"run","threadId":"t","runId":"r"}]', 100, 't')`;
    const validator = {
      projectId: "p",
      modelSelection: { instanceId: "codex", model: "m" },
      workspace: { type: "root" },
    };
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('validation','a',1,1,1,'evidence','w',${JSON.stringify(validator)},NULL,'reserved',NULL,'2026-11-12T00:00:00Z',NULL,NULL)`;
    yield* sql`INSERT INTO work_task_validation_identity VALUES ('validation','a','validation-claim',1)`;
    const validation = decodePersistedEvent({
      event: {
        taskId: "a",
        occurredAt: base.occurredAt,
        attribution: { type: "server" },
        lifecycleRevision: 1,
        completionPolicyRevision: 1,
        id: "requested",
        type: "task.validation-reserved",
        definitionRevision: 1,
        actionPolicyRevision: 1,
        attemptId: "validation",
        commandId: "validation-command",
        managerGeneration: 1,
        claimId: "validation-claim",
        evidenceId: "evidence",
        workWatermark: "w",
        validator,
        allowanceUnit: 1,
        createdAt: base.occurredAt,
      },
      context: {
        sequence: 1,
        eventId: "requested",
        occurredAt: base.occurredAt,
        commandId: "validation-command",
      },
    }).event;
    yield* sql`INSERT INTO work_task_validation_replay VALUES ('requested','validation','a',1,'validation-command',${base.occurredAt},${JSON.stringify(validation)})`;
    yield* reject(
      sql`INSERT INTO work_task_review_events VALUES ('requested','a','review',1,${base.occurredAt},'command',${requested})`,
    );
    yield* sql`DELETE FROM work_task_validation_replay WHERE event_id = 'requested'`;
    yield* sql`INSERT INTO work_task_review_events VALUES ('requested', 'a', 'review', 1, '2026-11-12T00:00:00Z', 'command', ${requested})`;
    yield* reject(
      sql`INSERT INTO work_task_validation_replay VALUES ('requested','validation','a',1,'validation-command',${base.occurredAt},${JSON.stringify(validation)})`,
    );
    const otherValidation = JSON.stringify({ ...validation, id: "validation-other" });
    yield* sql`INSERT INTO work_task_validation_replay VALUES ('validation-other','validation','a',99,'validation-command',${base.occurredAt},${otherValidation})`;
    yield* reject(
      sql`UPDATE work_task_validation_replay SET event_id='requested',sequence=1,event_json=${JSON.stringify(validation)} WHERE event_id='validation-other'`,
    );

    const decided = JSON.stringify({
      id: "decision-event",
      taskId: "a",
      occurredAt: "2026-11-12T00:00:00Z",
      attribution: { type: "server" },
      type: "task.action-review-result-recorded",
      reviewId: "review",
      reviewRevision: 1,
      managerGeneration: 1,
      managerThreadId: "manager",
      runId: "run",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      payloadIdentity: binding.payloadIdentity,
      decision: "reject",
      human: { subject: "human", sessionId: "session" },
      feedback: "feedback",
    });
    assert.isDefined(
      decodePersistedEvent({
        event: JSON.parse(decided),
        context: {
          sequence: 2,
          eventId: "decision-event",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: null,
        },
      }),
    );
    yield* sql`INSERT INTO work_task_review_events VALUES ('decision-event', 'a', 'review', 2, '2026-11-12T00:00:00Z', ${"c".repeat(2001)}, ${decided})`;
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_events SELECT * FROM work_task_review_events WHERE event_id = 'requested'`,
    );
    yield* reject(sql`UPDATE work_task_review_events SET command_id = 'other'`);
    yield* reject(sql`DELETE FROM work_task_review_events`);
    const collision = JSON.stringify({ ...base, id: "requested" });
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes VALUES ('requested', 'a', 'review', 6, '2026-11-12T00:00:00Z', NULL, ${collision})`,
    );
    for (const [index, reason] of ["😀".repeat(1000), "a\u0000b"].entries()) {
      const id = `valid-bound-${index}`;
      const value = JSON.stringify({ ...base, id, reason });
      yield* sql`INSERT INTO work_task_review_outcomes VALUES (${id}, 'a', 'review', ${10 + index}, '2026-11-12T00:00:00Z', NULL, ${value})`;
    }
    for (const [sequence, id, expectedHex] of [
      [12, "id\u0000tail", "6964007461696C"],
      [13, "\ud800", "EDA080"],
    ] as const) {
      const value = JSON.stringify({ ...base, id });
      yield* sql`INSERT INTO work_task_review_outcomes SELECT json_extract(${value}, '$.id'), 'a', 'review', ${sequence}, '2026-11-12T00:00:00Z', NULL, ${value}`;
      assert.strictEqual(
        (yield* sql<{
          readonly id: string;
        }>`SELECT hex(event_id) AS id FROM work_task_review_outcomes WHERE sequence = ${sequence}`)[0]
          ?.id,
        expectedHex,
      );
    }
    const wrongDecisionEvent = JSON.stringify({
      ...base,
      id: "wrong-decision",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      type: "task.action-review-continuation-released",
      purpose: "feedback",
      decision: "reject",
      continuationId: "continuation",
      decisionEventId: "other-decision",
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('wrong-decision', 'a', 'review', 9, '2026-11-12T00:00:00Z', NULL, ${wrongDecisionEvent})`,
    );
    const release = JSON.stringify({
      ...base,
      id: "release-event",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      type: "task.action-review-continuation-released",
      purpose: "feedback",
      decision: "reject",
      continuationId: "continuation",
      decisionEventId: "decision-event",
    });
    assert.isDefined(
      decodePersistedEvent({
        event: JSON.parse(release),
        context: {
          sequence: 4,
          eventId: "release-event",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: null,
        },
      }),
    );
    yield* sql`INSERT INTO work_task_review_outcomes VALUES ('release-event', 'a', 'review', 4, '2026-11-12T00:00:00Z', NULL, ${release})`;
    const duplicateRelease = JSON.stringify({ ...JSON.parse(release), id: "duplicate-release" });
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes VALUES ('duplicate-release', 'a', 'review', 15, '2026-11-12T00:00:00Z', NULL, ${duplicateRelease})`,
    );
    const wrongPurpose = JSON.stringify({
      ...JSON.parse(release),
      id: "wrong",
      purpose: "approved_action",
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('wrong', 'a', 'review', 5, '2026-11-12T00:00:00Z', NULL, ${wrongPurpose})`,
    );
    const cancelled = JSON.stringify({
      ...base,
      id: "cancel",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      type: "task.action-review-cancelled",
      source: {
        type: "task.archive",
        commandId: "archive",
        human: { subject: "human", sessionId: "session" },
      },
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('cancel', 'a', 'review', 5, '2026-11-12T00:00:00Z', NULL, ${cancelled})`,
    );
    assert.isDefined(
      decodePersistedEvent({
        event: JSON.parse(cancelled),
        context: {
          sequence: 5,
          eventId: "cancel",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: "archive",
        },
      }),
    );
    yield* sql`INSERT INTO work_task_review_outcomes VALUES ('cancel', 'a', 'review', 5, '2026-11-12T00:00:00Z', 'archive', ${cancelled})`;
    yield* sql`INSERT INTO work_task_action_reviews SELECT 'review2', task_id, revision, manager_generation,
      manager_thread_id, provider_session_id, run_id, 'command2', '123', rule_id,
      definition_revision, action_policy_revision, action, label, target, preview_json, fingerprint, 20, created_at
      FROM work_task_action_reviews WHERE review_id = 'review'`;
    for (const [command, request] of [
      ["command", "new-request"],
      ["new-command", "request"],
    ] as const) {
      yield* reject(sql`INSERT OR REPLACE INTO work_task_action_reviews SELECT 'review3', task_id, revision, manager_generation,
        manager_thread_id, provider_session_id, run_id, ${command}, ${request}, rule_id, definition_revision,
        action_policy_revision, action, label, target, preview_json, fingerprint, 23, created_at
        FROM work_task_action_reviews WHERE review_id = 'review'`);
    }
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_action_reviews (rowid,review_id,task_id,revision,manager_generation,manager_thread_id,provider_session_id,run_id,command_id,runtime_request_id,rule_id,definition_revision,action_policy_revision,action,label,target,preview_json,fingerprint,created_sequence,created_at) SELECT rowid,'rowid-review',task_id,revision,manager_generation,manager_thread_id,provider_session_id,run_id,'rowid-command','rowid-request',rule_id,definition_revision,action_policy_revision,action,label,target,preview_json,fingerprint,60,created_at FROM work_task_action_reviews WHERE review_id='review2'`,
    );
    const duplicateRequested = JSON.stringify({
      ...JSON.parse(requested),
      id: "requested2",
      review: {
        ...JSON.parse(requested).review,
        reviewId: "review2",
        runtimeRequestId: "123",
        createdSequence: 20,
      },
    });
    yield* reject(
      sql`INSERT INTO work_task_review_events VALUES ('requested2','a','review2',20,${base.occurredAt},'command2',${duplicateRequested})`,
    );
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_action_review_decisions (rowid,review_id,task_id,review_revision,action_policy_revision,fingerprint,manager_generation,decision,human_subject,human_session_id,feedback,resolved_sequence,resolved_at) SELECT rowid,'review2',task_id,review_revision,action_policy_revision,fingerprint,manager_generation,'approve',human_subject,human_session_id,NULL,21,resolved_at FROM work_task_action_review_decisions WHERE review_id='review'`,
    );
    yield* sql`INSERT INTO work_task_action_review_decisions VALUES ('review2', 'a', 1, 1, 'fingerprint', 1, 'approve', 'human', 'session', NULL, 21, 't')`;
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_action_review_releases VALUES ('review2', 'a', 1, 1, 'fingerprint', 1, 'continuation', 't')`,
    );
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_action_review_releases (rowid,review_id,task_id,review_revision,action_policy_revision,fingerprint,manager_generation,continuation_id,created_at) SELECT rowid,'review2',task_id,review_revision,action_policy_revision,fingerprint,manager_generation,'fresh-continuation',created_at FROM work_task_action_review_releases WHERE review_id='review'`,
    );
    yield* sql`INSERT INTO work_task_action_review_releases VALUES ('review2', 'a', 1, 1, 'fingerprint', 1, 'continuation2', 't')`;
    const coerced = JSON.stringify({
      ...base,
      id: "coerced",
      binding: { ...binding, reviewId: "review2", runtimeRequestId: 123 },
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('coerced', 'a', 'review2', 26, '2026-11-12T00:00:00Z', NULL, ${coerced})`,
    );
    const approved = JSON.stringify({
      ...base,
      id: "approved-event",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      binding: { ...binding, reviewId: "review2", runtimeRequestId: "123" },
      type: "task.action-review-continuation-released",
      purpose: "approved_action",
      decision: "approve",
      continuationId: "continuation2",
      decisionEventId: "actual-decision",
    });
    assert.isDefined(
      decodePersistedEvent({
        event: JSON.parse(approved),
        context: {
          sequence: 22,
          eventId: "approved-event",
          occurredAt: "2026-11-12T00:00:00Z",
          commandId: null,
        },
      }),
    );
    const absentDecision = JSON.stringify({
      ...JSON.parse(approved),
      decisionEventId: "nonexistent",
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('approved-event','a','review2',22,${base.occurredAt},NULL,${absentDecision})`,
    );
    const borrowedDecision = JSON.stringify({
      ...JSON.parse(approved),
      id: "borrowed",
      decisionEventId: "decision-event",
    });
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes VALUES ('borrowed', 'a', 'review2', 24, '2026-11-12T00:00:00Z', NULL, ${borrowedDecision})`,
    );
    const approvedAsFeedback = JSON.stringify({
      ...JSON.parse(approved),
      id: "bad-purpose",
      purpose: "feedback",
    });
    yield* reject(
      sql`INSERT INTO work_task_review_outcomes VALUES ('bad-purpose', 'a', 'review2', 24, '2026-11-12T00:00:00Z', NULL, ${approvedAsFeedback})`,
    );
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO work_task_review_outcomes VALUES ('approved-event', 'a', 'review2', 22, '2026-11-12T00:00:00Z', NULL, ${approved})`;
        const actualDecision = JSON.stringify({
          ...JSON.parse(decided),
          id: "actual-decision",
          reviewId: "review2",
          decision: "approve",
          feedback: undefined,
        });
        yield* sql`INSERT INTO work_task_review_events VALUES ('actual-decision', 'a', 'review2', 21, '2026-11-12T00:00:00Z', NULL, ${actualDecision})`;
        yield* sql`INSERT INTO work_task_review_status VALUES ('review2', 'a', 'approved', 21)`;
      }),
    );
    const stolenDecision = JSON.stringify({ ...base, id: "actual-decision" });
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_outcomes VALUES ('actual-decision', 'a', 'review', 25, '2026-11-12T00:00:00Z', NULL, ${stolenDecision})`,
    );
    yield* reject(sql`INSERT INTO work_task_review_status VALUES ('missing', 'a', 'pending', 1)`);
    yield* reject(sql`INSERT INTO work_task_review_status VALUES ('review', 'a', 'pending', 1)`);
    yield* sql`INSERT INTO work_task_review_status VALUES ('review', 'a', 'invalidated', 13)`;
    const latest = JSON.stringify({ ...base, id: "latest" });
    yield* sql`INSERT INTO work_task_review_outcomes VALUES ('latest','a','review',14,${base.occurredAt},${"c".repeat(2001)},${latest})`;
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM work_task_review_status WHERE review_id='review'`,
      [],
    );
    yield* sql`INSERT INTO work_task_review_status VALUES ('review','a','invalidated',14)`;

    yield* reject(
      sql`UPDATE OR REPLACE work_task_review_status SET review_id = 'review2' WHERE review_id = 'review'`,
    );
    assert.strictEqual((yield* sql`SELECT * FROM work_task_review_status`).length, 2);
    yield* reject(
      sql`UPDATE work_task_review_status SET state = 'rejected', sequence = 2 WHERE review_id = 'review'`,
    );
    yield* reject(sql`UPDATE work_task_review_status SET state = 'approved', sequence = 2`);
    yield* reject(
      sql`UPDATE work_task_review_status SET state = 'invalidated', sequence = 3 WHERE review_id = 'review'`,
    );
    yield* reject(
      sql`UPDATE work_task_review_status SET state = 'cancelled', sequence = 5 WHERE review_id = 'review'`,
    );
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_review_status VALUES ('review', 'a', 'pending', 1)`,
    );
    yield* reject(sql`UPDATE work_task_review_status SET task_id = 'other'`);
    yield* sql`INSERT INTO work_task_action_reviews SELECT 'review-changes', task_id, revision, manager_generation,
      manager_thread_id, provider_session_id, run_id, 'command-changes', 'request-changes', rule_id,
      definition_revision, action_policy_revision, action, label, target, preview_json, fingerprint, 30, created_at
      FROM work_task_action_reviews WHERE review_id = 'review'`;
    yield* sql`INSERT INTO work_task_action_review_decisions VALUES ('review-changes', 'a', 1, 1, 'fingerprint', 1, 'request_changes', 'human', 'session', NULL, 31, 't')`;
    yield* sql`INSERT INTO work_task_action_review_releases VALUES ('review-changes', 'a', 1, 1, 'fingerprint', 1, 'continuation-changes', 't')`;
    const changes = JSON.stringify({
      ...base,
      id: "changes-event",
      definitionRevision: 1,
      actionPolicyRevision: 1,
      binding: { ...binding, reviewId: "review-changes", runtimeRequestId: "request-changes" },
      type: "task.action-review-continuation-released",
      purpose: "feedback",
      decision: "request_changes",
      continuationId: "continuation-changes",
      decisionEventId: "changes-decision",
    });
    const changesDecision = JSON.stringify({
      ...JSON.parse(decided),
      id: "changes-decision",
      reviewId: "review-changes",
      decision: "request_changes",
      feedback: undefined,
    });
    yield* sql`INSERT INTO work_task_review_events VALUES ('changes-decision', 'a', 'review-changes', 31, '2026-11-12T00:00:00Z', NULL, ${changesDecision})`;
    yield* sql`INSERT INTO work_task_review_outcomes VALUES ('changes-event', 'a', 'review-changes', 32, '2026-11-12T00:00:00Z', NULL, ${changes})`;
    yield* sql`INSERT INTO work_task_review_status VALUES ('review-changes', 'a', 'changes_requested', 31)`;
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM work_task_action_review_decisions WHERE review_id = 'review'`,
      decisions,
    );
    const longId = "e".repeat(2001);
    yield* sql`INSERT INTO work_task_action_reviews SELECT 'long-review',task_id,revision,manager_generation,manager_thread_id,provider_session_id,run_id,'long-command','long-request',rule_id,definition_revision,action_policy_revision,action,label,target,preview_json,fingerprint,40,created_at FROM work_task_action_reviews WHERE review_id='review'`;
    const longRequested = {
      ...JSON.parse(requested),
      id: longId,
      occurredAt: "2026-11-12T01:00:00.0000000+01:00",
      review: {
        ...JSON.parse(requested).review,
        reviewId: "long-review",
        runtimeRequestId: "long-request",
        createdSequence: 40,
        continuationId: undefined,
      },
    };
    assert.isDefined(
      decodePersistedEvent({
        event: longRequested,
        context: {
          sequence: 40,
          eventId: longId,
          occurredAt: longRequested.occurredAt,
          commandId: "long-command",
        },
      }),
    );
    yield* sql`INSERT INTO work_task_review_events VALUES (${longId},'a','long-review',40,${longRequested.occurredAt},'long-command',${JSON.stringify(longRequested)})`;
    for (const [index, preview] of [
      { content: "\u00a0\ufeff", artifacts: [] },
      { content: "valid", artifacts: [{ uri: "\t\ufeff", hash: "hash" }] },
    ].entries()) {
      const reviewId = `blank-${index}`;
      const command = `blank-command-${index}`;
      const request = `blank-request-${index}`;
      yield* sql`INSERT INTO work_task_action_reviews SELECT ${reviewId},task_id,revision,manager_generation,manager_thread_id,provider_session_id,run_id,${command},${request},rule_id,definition_revision,action_policy_revision,action,label,target,${JSON.stringify(preview)},fingerprint,${50 + index},created_at FROM work_task_action_reviews WHERE review_id='review'`;
      const badPreview = {
        ...JSON.parse(requested),
        id: reviewId,
        review: {
          ...JSON.parse(requested).review,
          reviewId,
          runtimeRequestId: request,
          createdSequence: 50 + index,
          continuationId: undefined,
          preview,
        },
      };
      assert.throws(() =>
        decodePersistedEvent({
          event: badPreview,
          context: {
            sequence: 50 + index,
            eventId: reviewId,
            occurredAt: base.occurredAt,
            commandId: command,
          },
        }),
      );
      yield* reject(
        sql`INSERT INTO work_task_review_events VALUES (${reviewId},'a',${reviewId},${50 + index},${base.occurredAt},${command},${JSON.stringify(badPreview)})`,
      );
    }
    const latestPlan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN SELECT max(seq) FROM (
      SELECT created_sequence AS seq FROM work_task_action_reviews WHERE review_id = 'review'
      UNION ALL SELECT resolved_sequence FROM work_task_action_review_decisions WHERE review_id = 'review'
      UNION ALL SELECT sequence FROM work_task_review_outcomes WHERE review_id = 'review' AND json_extract(event_json,'$.type') IN ('task.action-review-invalidated','task.action-review-cancelled'))`;
    assert.isTrue(
      latestPlan.some((row) =>
        row.detail.includes(
          "SEARCH work_task_review_outcomes USING INDEX work_task_review_outcomes_review_idx",
        ),
      ),
    );
    assert.isFalse(latestPlan.some((row) => row.detail.includes("SCAN work_task_review_outcomes")));
    const terminalPlan = yield* sql<{
      detail: string;
    }>`EXPLAIN QUERY PLAN SELECT 1 FROM work_task_review_outcomes WHERE review_id = 'review' AND sequence = 14 AND json_extract(event_json,'$.type') = 'task.action-review-invalidated'`;
    assert.isTrue(
      terminalPlan.some((row) =>
        row.detail.includes(
          "SEARCH work_task_review_outcomes USING INDEX work_task_review_outcomes_review_idx",
        ),
      ),
    );
    const statusPlan = yield* sql<{
      readonly detail: string;
    }>`EXPLAIN QUERY PLAN SELECT * FROM work_task_review_status WHERE task_id = 'a' AND state = 'cancelled' ORDER BY sequence DESC, review_id DESC LIMIT 10`;
    assert.isTrue(
      statusPlan.some((row) => row.detail.includes("work_task_review_status_page_idx")),
    );
    const reviewsPlan = yield* sql<{
      readonly detail: string;
    }>`EXPLAIN QUERY PLAN SELECT * FROM work_task_action_reviews WHERE task_id = 'a' ORDER BY created_sequence DESC, review_id DESC LIMIT 10`;
    assert.isTrue(reviewsPlan.some((row) => row.detail.includes("work_task_reviews_history_idx")));
    const historyPlan = yield* sql<{
      readonly detail: string;
    }>`EXPLAIN QUERY PLAN SELECT * FROM work_task_review_outcomes WHERE task_id = 'a' ORDER BY sequence DESC, review_id DESC LIMIT 10`;
    assert.isTrue(historyPlan.some((row) => row.detail.includes("INDEX")));
    assert.isFalse(historyPlan.some((row) => row.detail.includes("TEMP B-TREE")));
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 91 }), []);
    yield* sql`INSERT INTO work_task_waits VALUES ('a','migration-wait',1,'{"type":"source","sourceId":"source"}','registered',101,'t')`;
    const tables = yield* sql<{
      readonly name: string;
    }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'work_task_%' ORDER BY name`;
    const snapshot = Effect.fn("PersistentTasks.snapshotBindingsUpgrade")(function* () {
      const rows: Record<string, readonly string[]> = {};
      for (const table of tables) {
        const data = yield* sql.unsafe(`SELECT * FROM "${table.name.replaceAll('"', '""')}"`);
        rows[table.name] = data.map((row) => JSON.stringify(row)).toSorted();
      }
      return rows;
    });
    const before = yield* snapshot();
    const bindingSchema =
      yield* sql`SELECT sql FROM sqlite_master WHERE name = 'work_task_manager_bindings'`;
    yield* sql`CREATE INDEX work_task_manager_thread_history_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(yield* snapshot(), before);
    assert.deepStrictEqual(
      yield* sql`SELECT sql FROM sqlite_master WHERE name = 'work_task_manager_bindings'`,
      bindingSchema,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 92`,
      [],
    );
    yield* sql`DROP INDEX work_task_manager_thread_history_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [92, "WorkTaskSharedManagerBindings"],
      [93, "WorkTaskDependencies"],
      [94, "WorkTaskManagerGroups"],
      [95, "WorkTaskManagerMemberships"],
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
    ]);
    assert.deepStrictEqual(yield* snapshot(), before);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* snapshot(), before);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "shares manager threads across tasks while preserving task-local binding identities",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 91 });
      yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','1','1'),('b',NULL,1,1,1,1,'{}','1','1'),('c',NULL,1,1,1,1,'{}','1','1')`;
      const encoded = JSON.stringify("shared\u0000😀\ud800");
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a',1,json_extract(${encoded},'$'),'bound',NULL),('a',2,'retired','old','retired'),('a',3,'free','old','retired')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',1,1,'{}','{}','definition','1')`;
      yield* sql`INSERT INTO work_task_claims VALUES ('legacy-claim','a','claim','e','retired',2,1,1,0,'{"type":"server"}','legacy','[{"type":"run","threadId":"t","runId":"r"}]',1,'1')`;
      const bytes =
        yield* sql`SELECT task_id,generation,hex(thread_id) AS thread,hex(bound_at) AS bound,hex(retired_at) AS retired FROM work_task_manager_bindings ORDER BY task_id,generation`;
      yield* runMigrations();
      assert.deepStrictEqual(
        yield* sql`SELECT task_id,generation,hex(thread_id) AS thread,hex(bound_at) AS bound,hex(retired_at) AS retired FROM work_task_manager_bindings ORDER BY task_id,generation`,
        bytes,
      );
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('b',1,json_extract(${encoded},'$'),'bound',NULL)`;
      assert.lengthOf(
        yield* sql`SELECT * FROM work_task_manager_bindings WHERE thread_id = json_extract(${encoded},'$')`,
        2,
      );
      for (const recursive of [0, 1]) {
        yield* sql.unsafe(`PRAGMA recursive_triggers = ${recursive}`);
        const before =
          yield* sql`SELECT * FROM work_task_manager_bindings ORDER BY task_id,generation`;
        const rejected = [
          sql`UPDATE work_task_manager_bindings SET thread_id = 'changed' WHERE task_id = 'a' AND generation = 2`,
          sql`UPDATE OR REPLACE work_task_manager_bindings SET generation = 2 WHERE task_id = 'a' AND generation = 3`,
          sql`UPDATE OR REPLACE work_task_manager_bindings SET thread_id = 'retired' WHERE task_id = 'a' AND generation = 3`,
          sql`UPDATE OR REPLACE work_task_manager_bindings SET generation = 1,thread_id = 'retired' WHERE task_id = 'a' AND generation = 3`,
          sql`UPDATE OR REPLACE work_task_manager_bindings SET task_id = 'b',generation = 1 WHERE task_id = 'a' AND generation = 3`,
          sql`UPDATE OR REPLACE work_task_manager_bindings SET retired_at = NULL WHERE task_id = 'a' AND generation = 3`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a',2,'changed','old','retired')`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a',4,'retired','old','retired')`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('a',4,'new-current','1',NULL)`,
          sql`INSERT OR IGNORE INTO work_task_manager_bindings VALUES ('a',2,'changed','old','retired')`,
          sql`INSERT INTO work_task_manager_bindings VALUES ('a',4,json_extract(${encoded},'$'),'1','retired')`,
          sql`DELETE FROM work_task_manager_bindings WHERE task_id = 'a' AND generation = 2`,
          sql`DELETE FROM work_task_manager_bindings WHERE task_id = 'a' AND generation = 3`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings (rowid,task_id,generation,thread_id,bound_at) VALUES (1,'c',1,'fresh','1')`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings (oid,task_id,generation,thread_id,bound_at) VALUES (1,'c',1,'fresh','1')`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings (_rowid_,task_id,generation,thread_id,bound_at) VALUES (1,'c',1,'fresh','1')`,
          sql`INSERT OR REPLACE INTO work_task_manager_bindings VALUES ('c',1,'fresh','1',NULL),('a',2,'changed','1','retired')`,
          sql`UPDATE work_task_manager_bindings SET generation = 0 WHERE task_id = 'a' AND generation = 3`,
          sql`INSERT INTO work_task_manager_bindings VALUES ('missing',1,'missing','1',NULL)`,
        ];
        for (const statement of rejected) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM work_task_manager_bindings ORDER BY task_id,generation`,
            before,
          );
        }
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      }
      yield* sql`UPDATE work_task_manager_bindings SET retired_at = 'retired' WHERE task_id = 'a' AND generation = 1`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a',4,'new-manager','next',NULL),('c',1,json_extract(${encoded},'$'),'next',NULL)`;
      const members = yield* sql<{
        readonly task_id: string;
      }>`SELECT task_id FROM work_task_manager_bindings WHERE thread_id = json_extract(${encoded},'$') AND retired_at IS NULL ORDER BY task_id,generation LIMIT 1`;
      assert.deepStrictEqual(
        members.map((row) => row.task_id),
        ["b"],
      );
      const nextMembers = yield* sql<{
        readonly task_id: string;
      }>`SELECT task_id FROM work_task_manager_bindings WHERE thread_id = json_extract(${encoded},'$') AND retired_at IS NULL AND (task_id,generation) > ('b',1) ORDER BY task_id,generation LIMIT 1`;
      assert.deepStrictEqual(
        nextMembers.map((row) => row.task_id),
        ["c"],
      );
      const activePlan = yield* sql<{
        readonly detail: string;
      }>`EXPLAIN QUERY PLAN SELECT task_id,generation FROM work_task_manager_bindings WHERE thread_id = json_extract(${encoded},'$') AND retired_at IS NULL AND (task_id,generation) > ('a',1) ORDER BY task_id,generation LIMIT 50`;
      assert.isTrue(
        activePlan.some((row) => row.detail.includes("work_task_manager_active_thread_idx")),
      );
      assert.isFalse(activePlan.some((row) => row.detail.includes("TEMP B-TREE")));
      const historyPlan = yield* sql<{
        readonly detail: string;
      }>`EXPLAIN QUERY PLAN SELECT task_id,generation FROM work_task_manager_bindings WHERE thread_id = json_extract(${encoded},'$') AND (task_id,generation) > ('a',1) ORDER BY task_id,generation LIMIT 50`;
      assert.isTrue(
        historyPlan.some((row) => row.detail.includes("work_task_manager_thread_history_idx")),
      );
      assert.isFalse(historyPlan.some((row) => row.detail.includes("TEMP B-TREE")));
      assert.lengthOf(
        yield* sql`SELECT * FROM work_task_claims WHERE manager_thread_id = 'retired' AND manager_generation = 2`,
        1,
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

const decodeGraphEdge = Schema.decodeUnknownSync(WorkTaskGraphEdgeRecord);
const decodeGraphProvenance = Schema.decodeUnknownSync(WorkTaskGraphProvenance);
const graphProvenance = (eventId: string) => ({
  eventId,
  commandId: "command",
  occurredAt: "2026-11-15T00:00:00.0001Z",
  adoptedBy: { subject: "owner", sessionId: "session" },
});
const graphEdge = (edgeId: string, revision: number, dependent = "a", blocker = "b") => ({
  edgeId,
  dependent: { environmentId: "env", taskId: dependent },
  blocker: { environmentId: "env", taskId: blocker },
  createdGraphRevision: revision,
  provenance: graphProvenance(`adopt-${edgeId}`),
});
const insertGraphJson = Effect.fn("PersistentTasks.insertGraphJson")(function* (
  encoded: string,
  sequence: number,
  mode: "insert" | "replace" | "ignore" = "insert",
) {
  const sql = yield* SqlClient.SqlClient;
  const verb = { insert: "INSERT", replace: "INSERT OR REPLACE", ignore: "INSERT OR IGNORE" }[mode];
  yield* sql.unsafe(
    `WITH input(value) AS (VALUES (?)) ${verb} INTO work_task_graph_edges
    (environment_id,edge_id,dependent_task_id,blocker_task_id,created_graph_revision,adopted_event_id,adopted_sequence,edge_json)
    SELECT json_extract(value,'$.dependent.environmentId'),json_extract(value,'$.edgeId'),json_extract(value,'$.dependent.taskId'),json_extract(value,'$.blocker.taskId'),json_extract(value,'$.createdGraphRevision'),json_extract(value,'$.provenance.eventId'),?,value FROM input`,
    [encoded, sequence],
  );
});
const graphRevision = Effect.fn("PersistentTasks.graphRevision")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ revision: number }>`SELECT max(
    coalesce((SELECT max(created_graph_revision) FROM work_task_graph_edges WHERE environment_id='env'),0),
    coalesce((SELECT max(removed_graph_revision) FROM work_task_graph_edges WHERE environment_id='env' AND removed_graph_revision IS NOT NULL),0)
  ) AS revision`;
  return rows[0]!.revision;
});

it.effect("keeps native blocker history canonical, immutable and indexed", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','1','1'),('b',NULL,1,1,1,1,'{}','1','1'),('c',NULL,1,1,1,1,'{}','1','1')`;
    assert.strictEqual(yield* graphRevision(), 0);
    for (const [id, revision, dependent, blocker] of [
      ["one", 1, "a", "b"],
      ["two", 2, "c", "b"],
      ["three", 3, "a", "c"],
    ] as const)
      yield* insertGraphJson(
        JSON.stringify(decodeGraphEdge(graphEdge(id, revision, dependent, blocker))),
        revision * 10,
      );
    const removed = JSON.stringify(decodeGraphProvenance(graphProvenance("remove-three")));
    yield* sql`UPDATE work_task_graph_edges SET removed_graph_revision=4, removed_event_id='remove-three',removed_sequence=40,removal_provenance_json=${removed} WHERE edge_id='three'`;
    yield* insertGraphJson(JSON.stringify(decodeGraphEdge(graphEdge("four", 5, "a", "c"))), 50);
    const normalized = decodeGraphEdge({
      ...graphEdge("padded", 6, "b", "c"),
      edgeId: " padded ",
      provenance: {
        ...graphProvenance("adopt-padded"),
        adoptedBy: { subject: " owner ", sessionId: " session " },
      },
    });
    yield* insertGraphJson(JSON.stringify(normalized), 60);
    assert.strictEqual(yield* graphRevision(), 6);
    const canonical = yield* sql<{
      edge_json: string;
    }>`SELECT edge_json FROM work_task_graph_edges WHERE edge_id='padded'`;
    assert.deepStrictEqual(JSON.parse(canonical[0]!.edge_json), normalized);
    const reject = Effect.fn("PersistentTasks.rejectGraph")(function* (
      effect: Effect.Effect<unknown, SqlError, SqlClient.SqlClient>,
    ) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
      assert.strictEqual(yield* graphRevision(), 6);
    });
    for (const edge of [
      graphEdge("bad", 8, "b", "a"),
      graphEdge("bad", 6, "b", "a"),
      graphEdge("bad", Number.MAX_SAFE_INTEGER + 1, "b", "a"),
      graphEdge("bad", 7, "a", "a"),
      graphEdge("bad", 7, "missing", "a"),
      {
        ...graphEdge("bad", 7, "b", "a"),
        dependent: { environmentId: "foreign", taskId: "b" },
        createdGraphRevision: 1,
      },
      {
        ...graphEdge("bad", 7, "b", "a"),
        provenance: {
          ...graphProvenance("bad"),
          adoptedBy: { subject: "\t\ufeff", sessionId: "session" },
        },
      },
      { ...graphEdge("bad", 7, "b", "a"), edgeId: "😀".repeat(1001) },
      { ...graphEdge("bad", 7, "b", "a"), edgeId: " noncanonical " },
      {
        ...graphEdge("bad", 7, "b", "a"),
        provenance: { ...graphProvenance("bad"), occurredAt: "2026-02-30T00:00:00Z" },
      },
      {
        ...graphEdge("bad", 7, "b", "a"),
        provenance: { ...graphProvenance("bad"), occurredAt: "2026-11-15T00:00:00Z\0suffix" },
      },
      {
        ...graphEdge("bad", 7, "b", "a"),
        provenance: { ...graphProvenance("bad"), adoptedBy: { subject: "owner" } },
      },
      { ...graphEdge("bad", 7, "b", "a"), extra: "discarded-on-wire" },
    ])
      yield* reject(insertGraphJson(JSON.stringify(edge), 70));
    const validNext = JSON.stringify(graphEdge("bad", 7, "b", "a"));
    for (const duplicate of [
      validNext.replace('"edgeId":"bad"', '"edgeId":"bad","edgeId":"other"'),
      validNext.replace('"subject":"owner"', '"subject":"owner","subject":"other"'),
      validNext.replace('"eventId":"adopt-bad"', '"eventId":"adopt-bad","eventId":"other"'),
    ])
      yield* reject(insertGraphJson(duplicate, 70));
    yield* reject(insertGraphJson(validNext, 59));
    const otherEdge = decodeGraphEdge({
      ...graphEdge("other", 1),
      dependent: { environmentId: "other-env", taskId: "a" },
      blocker: { environmentId: "other-env", taskId: "b" },
    });
    yield* insertGraphJson(JSON.stringify(otherEdge), 100);
    yield* sql`UPDATE work_task_graph_edges SET removed_graph_revision=2,removed_event_id='remove-other',removed_sequence=110,removal_provenance_json=${JSON.stringify(graphProvenance("remove-other"))} WHERE environment_id='other-env'`;
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      const beforeReplacement =
        yield* sql`SELECT * FROM work_task_graph_edges ORDER BY environment_id,edge_id`;
      for (const mode of ["replace", "ignore"] as const) {
        yield* reject(
          insertGraphJson(JSON.stringify(graphEdge("fresh-active-pair", 7, "a", "b")), 70, mode),
        );
        yield* reject(
          insertGraphJson(
            JSON.stringify({
              ...graphEdge("one", 7, "b", "a"),
              provenance: graphProvenance("fresh-replacement"),
            }),
            70,
            mode,
          ),
        );
        yield* reject(
          insertGraphJson(
            JSON.stringify({
              ...graphEdge("replacement", 7, "b", "a"),
              provenance: graphProvenance("adopt-one"),
            }),
            70,
            mode,
          ),
        );
        yield* reject(
          insertGraphJson(
            JSON.stringify({
              ...graphEdge("replacement", 7, "b", "a"),
              provenance: graphProvenance("remove-other"),
            }),
            70,
            mode,
          ),
        );
        yield* reject(insertGraphJson(validNext, 100, mode));
        yield* reject(insertGraphJson(validNext, 110, mode));
      }
      yield* reject(
        sql`UPDATE OR REPLACE work_task_graph_edges SET removed_graph_revision=7,removed_event_id='remove-other',removed_sequence=120,removal_provenance_json=${JSON.stringify(graphProvenance("remove-other"))} WHERE edge_id='two'`,
      );
      yield* reject(
        sql`UPDATE work_task_graph_edges SET removed_graph_revision=7,removed_event_id='multi-remove',removed_sequence=70,removal_provenance_json=${JSON.stringify(graphProvenance("multi-remove"))} WHERE environment_id='env' AND edge_id IN ('one','two')`,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_graph_edges ORDER BY environment_id,edge_id`,
        beforeReplacement,
      );
      yield* reject(
        sql`INSERT OR REPLACE INTO work_task_graph_edges SELECT * FROM work_task_graph_edges WHERE edge_id='one'`,
      );
      yield* reject(
        sql`INSERT OR IGNORE INTO work_task_graph_edges SELECT * FROM work_task_graph_edges WHERE edge_id='one'`,
      );
      yield* reject(
        sql`UPDATE OR REPLACE work_task_graph_edges SET edge_id='one' WHERE edge_id='two'`,
      );
      yield* reject(
        sql`UPDATE OR REPLACE work_task_graph_edges SET removed_graph_revision=7,removed_event_id='adopt-one',removed_sequence=70,removal_provenance_json=${JSON.stringify(graphProvenance("adopt-one"))} WHERE edge_id='two'`,
      );
      yield* reject(
        sql`UPDATE work_task_graph_edges SET removed_graph_revision=NULL,removed_event_id=NULL,removed_sequence=NULL,removal_provenance_json=NULL WHERE edge_id='three'`,
      );
      yield* reject(sql`DELETE FROM work_task_graph_edges WHERE edge_id='one'`);
    }
    const lone = decodeGraphEdge({
      ...graphEdge("\0😀\ud800", 7, "b", "a"),
      provenance: {
        ...graphProvenance("adopt-lone"),
        adoptedBy: { subject: "😀".repeat(1000), sessionId: "session" },
      },
    });
    yield* insertGraphJson(JSON.stringify(lone), 70);
    const readback = yield* sql<{
      edge_json: string;
      identity: string;
    }>`SELECT edge_json,hex(edge_id) AS identity FROM work_task_graph_edges WHERE edge_id=json_extract(${JSON.stringify(lone.edgeId)},'$')`;
    assert.deepStrictEqual(JSON.parse(readback[0]!.edge_json), lone);
    assert.strictEqual(readback[0]!.identity, "00F09F9880EDA080");
    const reverse = yield* sql<{
      edge_json: string;
    }>`SELECT edge_json FROM work_task_graph_edges WHERE environment_id='env' AND blocker_task_id='b' AND removed_graph_revision IS NULL AND (dependent_task_id,edge_id)>(json_extract(${JSON.stringify("a")},'$'),json_extract(${JSON.stringify("one")},'$')) ORDER BY dependent_task_id,edge_id LIMIT 50`;
    assert.deepStrictEqual(
      reverse.map((row) => JSON.parse(row.edge_json).edgeId),
      ["two"],
    );
    const plans = [
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT edge_json FROM work_task_graph_edges WHERE environment_id='env' AND dependent_task_id='a' AND removed_graph_revision IS NULL AND (blocker_task_id,edge_id)>('','') ORDER BY blocker_task_id,edge_id LIMIT 50`,
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT edge_json FROM work_task_graph_edges WHERE environment_id='env' AND blocker_task_id='b' AND removed_graph_revision IS NULL AND (dependent_task_id,edge_id)>('','') ORDER BY dependent_task_id,edge_id LIMIT 50`,
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT max(created_graph_revision) FROM work_task_graph_edges WHERE environment_id='env'`,
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT max(removed_graph_revision) FROM work_task_graph_edges WHERE environment_id='env' AND removed_graph_revision IS NOT NULL`,
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT adopted_sequence FROM work_task_graph_edges WHERE environment_id='env' ORDER BY created_graph_revision DESC LIMIT 1`,
      yield* sql<{
        detail: string;
      }>`EXPLAIN QUERY PLAN SELECT removed_sequence FROM work_task_graph_edges WHERE environment_id='env' AND removed_graph_revision IS NOT NULL ORDER BY removed_graph_revision DESC LIMIT 1`,
    ];
    for (const [index, name] of [
      "forward",
      "reverse",
      "created_revision",
      "removed_revision",
      "created_revision",
      "removed_revision",
    ].entries()) {
      assert.isTrue(
        plans[index]!.some((row) => row.detail.includes(`work_task_graph_edges_${name}_idx`)),
      );
      assert.isFalse(plans[index]!.some((row) => row.detail.includes("TEMP B-TREE")));
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.strictEqual(yield* graphRevision(), 7);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "rolls native graph migration and ledger back before preserving predecessor rows on retry",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 92 });
      yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{"bytes":"before"}','raw-created','raw-updated')`;
      yield* sql`UPDATE work_task_view_fence SET generation=9,high_water_sequence=17`;
      const tables = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`;
      const snapshot = Effect.fn("PersistentTasks.snapshotGraphUpgrade")(function* () {
        const rows: Record<string, readonly string[]> = {};
        for (const table of tables) {
          const name = table.name.replaceAll('"', '""');
          const columns = yield* sql.unsafe<{ name: string }>(`PRAGMA table_info("${name}")`);
          const expressions = columns.map((column) => {
            const c = '"' + column.name.replaceAll('"', '""') + '"';
            return `typeof(${c})||':'||coalesce(hex(CAST(${c} AS BLOB)),'NULL') AS ${c}`;
          });
          const data = yield* sql.unsafe(`SELECT ${expressions.join(",")} FROM "${name}"`);
          rows[table.name] = data.map((row) => JSON.stringify(row)).toSorted();
        }
        return rows;
      });
      const before = yield* snapshot();
      yield* sql`CREATE INDEX work_task_graph_edges_reverse_idx ON work_task_projections(task_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='work_task_graph_edges'`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=93`,
        [],
      );
      assert.deepStrictEqual(yield* snapshot(), before);
      yield* sql`DROP INDEX work_task_graph_edges_reverse_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [93, "WorkTaskDependencies"],
        [94, "WorkTaskManagerGroups"],
        [95, "WorkTaskManagerMemberships"],
        [96, "WorkTaskDirtyReconciliation"],
        [97, "WorkTaskAcceptedRuns"],
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* snapshot();
      assert.deepStrictEqual(
        { ...after, effect_sql_migrations: before.effect_sql_migrations },
        { ...before },
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_graph_edges`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(db)),
);

const decodeGroupControlEvent = Schema.decodeUnknownSync(WorkTaskGroupControlEvent);
const emptyGroupControl = (groupId = "group") => ({
  group: { environmentId: "env", groupId },
  controlRevision: 0,
  membershipRevision: 0,
  highestManagerGeneration: 0,
  currentManager: null,
  dispatchHold: "none",
});
const groupContext = (id: string) => ({
  id,
  environmentId: "env",
  commandId: `command-${id}`,
  occurredAt: "2026-11-15T00:00:00.0001Z",
  performedBy: { subject: "owner", sessionId: "session" },
});
const writeGroupControl = Effect.fn("PersistentTasks.writeGroupControl")(function* (
  event: WorkTaskGroupControlEvent,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const encoded = JSON.stringify(event);
  if (event.type === "task-group.created") {
    yield* sql`INSERT INTO work_task_manager_groups
      (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json)
      SELECT json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),0,0,0,NULL,NULL,'none',json_extract(value,'$.id'),${sequence},value,json_extract(value,'$.id'),${sequence},value FROM (SELECT ${encoded} AS value)`;
  } else {
    yield* sql`UPDATE work_task_manager_groups SET control_revision=json_extract(${encoded},'$.control.controlRevision'),membership_revision=json_extract(${encoded},'$.control.membershipRevision'),highest_manager_generation=json_extract(${encoded},'$.control.highestManagerGeneration'),current_manager_generation=json_extract(${encoded},'$.control.currentManager.managerGeneration'),current_manager_thread_id=json_extract(${encoded},'$.control.currentManager.threadId'),dispatch_hold=json_extract(${encoded},'$.control.dispatchHold'),control_event_id=json_extract(${encoded},'$.id'),control_sequence=${sequence},control_event_json=${encoded} WHERE environment_id=json_extract(${encoded},'$.environmentId') AND group_id=json_extract(${encoded},'$.control.group.groupId')`;
  }
});
const bindGroupManager = Effect.fn("PersistentTasks.bindGroupManager")(function* (
  event: WorkTaskGroupControlEvent,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const manager = event.control.currentManager;
  assert.isNotNull(manager);
  const encoded = JSON.stringify(event);
  yield* sql`INSERT INTO work_task_group_managers (environment_id,group_id,generation,manager_thread_id,bound_event_id,bound_sequence,bound_event_json,current_generation,current_manager_thread_id)
    SELECT json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),json_extract(value,'$.control.currentManager.managerGeneration'),json_extract(value,'$.control.currentManager.threadId'),json_extract(value,'$.id'),${sequence},value,json_extract(value,'$.control.currentManager.managerGeneration'),json_extract(value,'$.control.currentManager.threadId') FROM (SELECT ${encoded} AS value)`;
});
const retireGroupManager = Effect.fn("PersistentTasks.retireGroupManager")(function* (
  event: WorkTaskGroupControlEvent,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  assert.strictEqual(event.type, "task-group.manager-changed");
  if (event.type !== "task-group.manager-changed") throw new Error("Expected manager change");
  const previous = event.previousControl.currentManager!;
  yield* sql`UPDATE work_task_group_managers SET retired_event_id=${event.id},retired_sequence=${sequence},retired_event_json=${JSON.stringify(event)},current_generation=NULL,current_manager_thread_id=NULL
    WHERE environment_id=${event.environmentId} AND group_id=${event.control.group.groupId} AND generation=${previous.managerGeneration}`;
});
it.effect("keeps group current-manager links commit-consistent and replacement-safe", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const initial = emptyGroupControl();
    const created = decodeGroupControlEvent({
      ...groupContext("created"),
      type: "task-group.created",
      control: initial,
    });
    yield* writeGroupControl(created, 1);
    const bound = decodeGroupControlEvent({
      ...groupContext("bound"),
      type: "task-group.manager-changed",
      previousControl: initial,
      control: {
        ...initial,
        controlRevision: 1,
        highestManagerGeneration: 1,
        currentManager: { managerGeneration: 1, threadId: "manager" },
      },
    });
    yield* sql`BEGIN`;
    yield* writeGroupControl(bound, 2);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
    yield* sql`ROLLBACK`;
    assert.strictEqual(
      (yield* sql<{
        control_revision: number;
      }>`SELECT control_revision FROM work_task_manager_groups`)[0]!.control_revision,
      0,
    );
    yield* sql`BEGIN`;
    yield* writeGroupControl(bound, 2);
    yield* bindGroupManager(bound, 2);
    yield* sql`COMMIT`;
    let current = bound.control;
    for (const [index, bindFirst] of [true, false].entries()) {
      const replacement = decodeGroupControlEvent({
        ...groupContext(`replacement-${index}`),
        type: "task-group.manager-changed",
        previousControl: current,
        control: {
          ...current,
          controlRevision: current.controlRevision + 1,
          highestManagerGeneration: current.highestManagerGeneration + 1,
          currentManager: {
            managerGeneration: current.highestManagerGeneration + 1,
            threadId: `manager-${index}`,
          },
        },
      });
      yield* sql`BEGIN`;
      yield* writeGroupControl(replacement, index + 3);
      const prematureHold = decodeGroupControlEvent({
        ...groupContext(`premature-${index}`),
        type: "task-group.hold-changed",
        previousControl: replacement.control,
        control: {
          ...replacement.control,
          controlRevision: replacement.control.controlRevision + 1,
          dispatchHold: "paused",
        },
      });
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(writeGroupControl(prematureHold, 20 + index))),
      );
      if (bindFirst) {
        yield* bindGroupManager(replacement, index + 3);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
        yield* retireGroupManager(replacement, index + 3);
      } else {
        yield* retireGroupManager(replacement, index + 3);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
        yield* bindGroupManager(replacement, index + 3);
      }
      yield* sql`COMMIT`;
      current = replacement.control;
    }
    const paused = decodeGroupControlEvent({
      ...groupContext("paused"),
      type: "task-group.hold-changed",
      previousControl: current,
      control: { ...current, controlRevision: current.controlRevision + 1, dispatchHold: "paused" },
    });
    yield* writeGroupControl(paused, 5);
    assert.strictEqual(
      (yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM work_task_group_managers WHERE retired_event_id IS NOT NULL`)[0]!
        .count,
      2,
    );
    yield* sql`INSERT INTO work_task_projections VALUES ('task',NULL,1,1,1,1,'{}','1','1')`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT INTO work_task_manager_bindings VALUES ('task',1,'manager-1','1',NULL)`,
        ),
      ),
    );
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('task',1,'legacy','1',NULL)`;
    const legacyConflict = decodeGroupControlEvent({
      ...groupContext("legacy-conflict"),
      type: "task-group.manager-changed",
      previousControl: paused.control,
      control: {
        ...paused.control,
        controlRevision: paused.control.controlRevision + 1,
        highestManagerGeneration: 4,
        currentManager: { managerGeneration: 4, threadId: "legacy" },
      },
    });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(writeGroupControl(legacyConflict, 6))));
    for (const malformed of [
      { ...created, id: "bad", extra: true },
      { ...created, id: "bad", performedBy: { subject: "\t\ufeff", sessionId: "session" } },
      { ...created, id: "bad", occurredAt: "2026-02-30T00:00:00Z" },
      { ...created, id: "bad", occurredAt: "2026-11-15T00:00:00Z\0suffix" },
      {
        ...created,
        id: "bad",
        control: {
          ...created.control,
          group: { environmentId: "env", groupId: "bad" },
          unknown: 1,
        },
      },
    ]) {
      const encoded = JSON.stringify(malformed);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT INTO work_task_manager_groups (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json) VALUES ('env','bad',0,0,0,NULL,NULL,'none','bad',30,${encoded},'bad',30,${encoded})`,
          ),
        ),
      );
    }
    for (const [id, sequence] of [
      ["nul\0group", 40],
      ["😀".repeat(1000), 41],
      ["\ud800", 42],
    ] as const) {
      const accepted = decodeGroupControlEvent({
        ...groupContext(`unicode-${sequence}`),
        type: "task-group.created",
        control: emptyGroupControl(id),
      });
      const encoded = JSON.stringify(accepted);
      yield* sql`WITH input(value) AS (VALUES (${encoded})) INSERT INTO work_task_manager_groups (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json) SELECT json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),0,0,0,NULL,NULL,'none',json_extract(value,'$.id'),${sequence},value,json_extract(value,'$.id'),${sequence},value FROM input`;
      const read = yield* sql<{
        control_event_json: string;
      }>`SELECT control_event_json FROM work_task_manager_groups WHERE created_sequence=${sequence}`;
      assert.deepStrictEqual(JSON.parse(read[0]!.control_event_json), accepted);
    }
    const empty = decodeGroupControlEvent({
      ...groupContext("empty"),
      type: "task-group.created",
      control: emptyGroupControl("empty"),
    });
    yield* writeGroupControl(empty, 50);
    const unicode = decodeGroupControlEvent({
      ...groupContext("unicode-manager"),
      type: "task-group.manager-changed",
      previousControl: emptyGroupControl("unicode-bound"),
      control: {
        ...emptyGroupControl("unicode-bound"),
        controlRevision: 1,
        highestManagerGeneration: 1,
        currentManager: { managerGeneration: 1, threadId: "\ud800-manager" },
      },
    });
    yield* writeGroupControl(
      decodeGroupControlEvent({
        ...groupContext("unicode-bound-created"),
        type: "task-group.created",
        control: emptyGroupControl("unicode-bound"),
      }),
      49,
    );
    yield* sql`BEGIN`;
    yield* writeGroupControl(unicode, 51);
    yield* bindGroupManager(unicode, 51);
    yield* sql`COMMIT`;
    const unicodeHistory = yield* sql<{
      bound_event_json: string;
    }>`SELECT bound_event_json FROM work_task_group_managers WHERE bound_sequence=51`;
    assert.deepStrictEqual(JSON.parse(unicodeHistory[0]!.bound_event_json), unicode);
    for (const [query, index] of [
      [
        "SELECT * FROM work_task_manager_groups WHERE current_manager_thread_id='manager-1'",
        "work_task_group_current_thread_idx",
      ],
      [
        "SELECT max(generation) FROM work_task_group_managers WHERE environment_id='env' AND group_id='group'",
        "sqlite_autoindex_work_task_group_managers_2",
      ],
      [
        "SELECT group_id,generation FROM work_task_group_managers WHERE environment_id='env' AND manager_thread_id='manager' ORDER BY group_id,generation",
        "work_task_group_manager_thread_history_idx",
      ],
    ]) {
      const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(
        plan.some((row) => row.detail.includes(index!)),
        JSON.stringify(plan),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      const fresh = JSON.stringify(
        decodeGroupControlEvent({
          ...groupContext(`fresh-${recursive}`),
          type: "task-group.created",
          control: emptyGroupControl("empty"),
        }),
      );
      for (const verb of ["INSERT OR REPLACE", "INSERT OR IGNORE"]) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql.unsafe(
                `${verb} INTO work_task_manager_groups (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json) VALUES ('env','empty',0,0,0,NULL,NULL,'none',?,60,?,?,60,?)`,
                [`fresh-${recursive}`, fresh, `fresh-${recursive}`, fresh],
              ),
            ),
          ),
        );
      }

      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT OR REPLACE INTO work_task_manager_groups SELECT * FROM work_task_manager_groups`,
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`INSERT OR REPLACE INTO work_task_group_managers SELECT * FROM work_task_group_managers`,
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`DELETE FROM work_task_group_managers`)));
    }
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves populated predecessor storage through group migration rollback and retry",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 93 });
      yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('p','Project','/a','[ 1, 2 ]','raw','raw')`;
      yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES ('thread','p','Thread','codex','full-access','default','raw','raw','{ "lineage": [1, 2] }')`;
      yield* sql`INSERT INTO orchestration_events (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json) VALUES ('event','thread','thread',1,'thread.created','raw','client','{ "bytes": 1 }','{}')`;
      yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{ "raw": 1 }','raw','raw'),('b',NULL,1,1,1,1,'{}','raw','raw')`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a',1,'legacy','raw',NULL)`;
      yield* insertGraphJson(JSON.stringify(decodeGraphEdge(graphEdge("preserved", 1))), 90);
      const tables = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`;
      const capture = Effect.fn("PersistentTasks.captureGroupUpgrade")(function* () {
        const result: Record<string, readonly string[]> = {};
        for (const table of tables) {
          const name = table.name.replaceAll('"', '""');
          const columns = yield* sql.unsafe<{ name: string }>(`PRAGMA table_info("${name}")`);
          const expressions = columns.map((column) => {
            const key = '"' + column.name.replaceAll('"', '""') + '"';
            return `typeof(${key})||':'||coalesce(hex(CAST(${key} AS BLOB)),'NULL') AS ${key}`;
          });
          const rows = yield* sql.unsafe(`SELECT ${expressions.join(",")} FROM "${name}"`);
          result[table.name] = rows.map((row) => JSON.stringify(row)).toSorted();
        }
        return result;
      });
      const before = yield* capture();
      yield* sql`CREATE INDEX work_task_group_current_thread_idx ON work_task_projections(task_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name IN ('work_task_manager_groups','work_task_group_managers')`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=94`,
        [],
      );
      assert.deepStrictEqual(yield* capture(), before);
      yield* sql`DROP INDEX work_task_group_current_thread_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [94, "WorkTaskManagerGroups"],
        [95, "WorkTaskManagerMemberships"],
        [96, "WorkTaskDirtyReconciliation"],
        [97, "WorkTaskAcceptedRuns"],
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      assert.deepStrictEqual(
        { ...after, effect_sql_migrations: before.effect_sql_migrations! },
        before,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_manager_groups`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_group_managers`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

const decodeMembershipBatch = Schema.decodeUnknownSync(WorkTaskMembershipEventBatch);
const membershipJoin = () => {
  const task = { environmentId: "env", taskId: "a" };
  const previousControl = emptyGroupControl();
  const control = { ...previousControl, membershipRevision: 1 };
  const context = groupContext("membership-join");
  const previousSnapshot = {
    task,
    highestMembershipGeneration: 0,
    currentMembership: null,
    taskRevisions: {
      definitionRevision: 1,
      acceptedDefinitionRevision: 1,
      lifecycleRevision: 1,
      actionPolicyRevision: 1,
      completionPolicyRevision: 1,
    },
  };
  const joined = {
    task,
    group: control.group,
    membershipGeneration: 1,
    acceptedDefinitionRevisionAtJoin: 1,
    joinedMembershipRevision: 1,
    joined: {
      eventId: context.id,
      commandId: context.commandId,
      occurredAt: context.occurredAt,
      performedBy: context.performedBy,
    },
    state: "current",
  };
  return decodeMembershipBatch({
    event: {
      ...context,
      type: "task.membership-changed",
      previousSnapshot,
      snapshot: { ...previousSnapshot, highestMembershipGeneration: 1, currentMembership: joined },
      retiredMembership: null,
      groupTransitions: [{ companionEventId: "membership-companion", previousControl, control }],
    },
    companions: [
      {
        ...context,
        id: "membership-companion",
        type: "task-group.membership-changed",
        rootEventId: context.id,
        task,
        previousControl,
        control,
      },
    ],
  });
};
const selectMembershipGroup = Effect.fn("PersistentTasks.selectMembershipGroup")(function* (
  event: WorkTaskGroupControlEvent,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const encoded = JSON.stringify(event);
  yield* sql`UPDATE work_task_manager_groups SET membership_revision=json_extract(${encoded},'$.control.membershipRevision'),control_event_id=json_extract(${encoded},'$.id'),control_sequence=${sequence},control_event_json=${encoded},latest_membership_event_id=json_extract(${encoded},'$.id') WHERE environment_id=json_extract(${encoded},'$.environmentId') AND group_id=json_extract(${encoded},'$.control.group.groupId')`;
});
const insertMembershipCompanion = Effect.fn("PersistentTasks.insertMembershipCompanion")(function* (
  batch: WorkTaskMembershipEventBatch,
  index: number,
  rootSequence: number,
  sequence: number,
  rawRoot?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const root = batch.event;
  const event = batch.companions[index]!;
  const destination = root.snapshot.currentMembership;
  const join = destination?.group.groupId === event.control.group.groupId;
  const generation = join
    ? destination!.membershipGeneration
    : root.retiredMembership!.membershipGeneration;
  const peer = batch.companions.find((entry) => entry.id !== event.id)?.id ?? null;
  yield* sql`INSERT INTO work_task_membership_companions
    (event_id,sequence,environment_id,group_id,task_id,membership_generation,role,root_event_id,root_sequence,root_event_json,event_json,joined_generation,retired_generation,peer_event_id)
    SELECT json_extract(value,'$.id'),${sequence},json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),json_extract(value,'$.task.taskId'),${generation},${join ? "join" : "retire"},json_extract(value,'$.rootEventId'),${rootSequence},${rawRoot ?? JSON.stringify(root)},value,${join ? generation : null},${join ? null : generation},${peer} FROM (SELECT ${JSON.stringify(event)} value)`;
});
const writeMembershipHistory = Effect.fn("PersistentTasks.writeMembershipHistory")(function* (
  batch: WorkTaskMembershipEventBatch,
) {
  const sql = yield* SqlClient.SqlClient;
  const retired = batch.event.retiredMembership;
  if (retired !== null) {
    const companion = batch.companions.find(
      (entry) => entry.control.group.groupId === retired.group.groupId,
    )!;
    yield* sql`UPDATE work_task_group_memberships SET retired_membership_revision=${retired.retiredMembershipRevision},retired_event_id=${retired.retired.eventId},retired_record_json=${JSON.stringify(retired)},retired_companion_id=${companion.id}
      WHERE task_id=${retired.task.taskId} AND membership_generation=${retired.membershipGeneration}`;
  }
  const joined = batch.event.snapshot.currentMembership;
  if (joined !== null) {
    const companion = batch.companions.find(
      (entry) => entry.control.group.groupId === joined.group.groupId,
    )!;
    yield* sql`INSERT INTO work_task_group_memberships
      (task_id,membership_generation,environment_id,group_id,accepted_definition_revision,joined_membership_revision,joined_event_id,joined_record_json,joined_companion_id)
      SELECT json_extract(value,'$.task.taskId'),json_extract(value,'$.membershipGeneration'),json_extract(value,'$.task.environmentId'),json_extract(value,'$.group.groupId'),json_extract(value,'$.acceptedDefinitionRevisionAtJoin'),json_extract(value,'$.joinedMembershipRevision'),json_extract(value,'$.joined.eventId'),value,${companion.id} FROM (SELECT ${JSON.stringify(joined)} value)`;
  }
});
const membershipTransition = (
  previousSnapshot: WorkTaskMembershipEventBatch["event"]["snapshot"],
  controls: readonly WorkTaskGroupControlEvent["control"][],
  destinationGroup: string | null,
  id: string,
  occurredAt = "2026-11-15T00:00:00.0002Z",
) => {
  const context = { ...groupContext(id), occurredAt };
  const provenance = {
    eventId: id,
    commandId: context.commandId,
    occurredAt,
    performedBy: context.performedBy,
  };
  const source = previousSnapshot.currentMembership;
  const groups = [source?.group.groupId, destinationGroup].filter(
    (value): value is string => value !== undefined && value !== null,
  );
  const transitions = groups.map((groupId) => {
    const previousControl = controls.find((entry) => entry.group.groupId === groupId)!;
    return {
      companionEventId: `${id}-${groupId}`,
      previousControl,
      control: { ...previousControl, membershipRevision: previousControl.membershipRevision + 1 },
    };
  });
  const destination =
    destinationGroup === null
      ? null
      : transitions.find((entry) => entry.control.group.groupId === destinationGroup)!;
  const joined =
    destination === null
      ? null
      : {
          task: previousSnapshot.task,
          group: destination.control.group,
          membershipGeneration: previousSnapshot.highestMembershipGeneration + 1,
          acceptedDefinitionRevisionAtJoin:
            previousSnapshot.taskRevisions.acceptedDefinitionRevision,
          joinedMembershipRevision: destination.control.membershipRevision,
          joined: provenance,
          state: "current",
        };
  return decodeMembershipBatch({
    event: {
      ...context,
      type: "task.membership-changed",
      previousSnapshot,
      snapshot: {
        ...previousSnapshot,
        highestMembershipGeneration:
          previousSnapshot.highestMembershipGeneration + (joined === null ? 0 : 1),
        currentMembership: joined,
      },
      retiredMembership:
        source === null
          ? null
          : {
              ...source,
              state: "retired",
              retiredMembershipRevision: transitions.find(
                (entry) => entry.control.group.groupId === source.group.groupId,
              )!.control.membershipRevision,
              retired: provenance,
            },
      groupTransitions: transitions,
    },
    companions: transitions.map((entry) => ({
      ...context,
      id: entry.companionEventId,
      type: "task-group.membership-changed",
      rootEventId: id,
      task: previousSnapshot.task,
      previousControl: entry.previousControl,
      control: entry.control,
    })),
  });
};
const applyMembership = Effect.fn("PersistentTasks.applyMembership")(function* (
  batch: WorkTaskMembershipEventBatch,
  sequence: number,
  reverse = false,
) {
  for (const [index, companion] of batch.companions.entries())
    yield* selectMembershipGroup(companion, sequence + index + 1);
  const indexes = batch.companions.map((_, index) => index);
  for (const index of reverse ? indexes.toReversed() : indexes)
    yield* insertMembershipCompanion(batch, index, sequence, sequence + index + 1);
  yield* writeMembershipHistory(batch);
});
it.effect("commits complete group membership and rejects incomplete deferred history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','raw','raw')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',1,1,'{}','{}','accepted','raw')`;
    yield* writeGroupControl(
      decodeGroupControlEvent({
        ...groupContext("membership-group"),
        type: "task-group.created",
        control: emptyGroupControl(),
      }),
      100,
    );
    const batch = membershipJoin();
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      yield* sql`BEGIN`;
      yield* selectMembershipGroup(batch.companions[0]!, 102);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
      yield* sql`ROLLBACK`;
      yield* sql`BEGIN`;
      yield* selectMembershipGroup(batch.companions[0]!, 102);
      yield* insertMembershipCompanion(batch, 0, 101, 102);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
      yield* sql`ROLLBACK`;
    }
    for (const [timestamp, rootId] of [
      ["0000-01-01T00:00:00+23:59", "nul\0identity"],
      ["9999-12-31T23:59:59.000000001-23:59", "😀".repeat(1000)],
      ["2026-11-15T00:00:00." + "0".repeat(1979) + "Z", "\ud800"],
    ]) {
      const encoded = JSON.stringify(batch)
        .replaceAll(JSON.stringify(batch.event.occurredAt), JSON.stringify(timestamp))
        .replaceAll(JSON.stringify(batch.event.id), JSON.stringify(rootId));
      const variant = decodeMembershipBatch(JSON.parse(encoded));
      yield* sql`BEGIN`;
      yield* applyMembership(variant, 101);
      const removal = membershipTransition(
        variant.event.snapshot,
        [variant.companions[0]!.control],
        null,
        "boundary-retirement",
        timestamp,
      );
      yield* applyMembership(removal, 103);
      const stored = yield* sql<{
        root_event_json: string;
      }>`SELECT root_event_json FROM work_task_membership_companions WHERE sequence=102`;
      assert.deepStrictEqual(JSON.parse(stored[0]!.root_event_json), variant.event);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      yield* sql`ROLLBACK`;
    }
    const encodedRoot = JSON.stringify(batch.event);
    for (const [path, value] of [
      ["$.snapshot.taskRevisions.definitionRevision", 2],
      ["$.previousSnapshot.taskRevisions.definitionRevision", 2],
      ["$.snapshot.taskRevisions.acceptedDefinitionRevision", null],
      ["$.snapshot.highestMembershipGeneration", 2],
      ["$.previousSnapshot.highestMembershipGeneration", 1],
      ["$.snapshot.currentMembership.acceptedDefinitionRevisionAtJoin", 2],
      ["$.snapshot.currentMembership.membershipGeneration", 0],
      ["$.snapshot.currentMembership.joinedMembershipRevision", 2],
      ["$.snapshot.currentMembership.joined.commandId", "different"],
      ["$.snapshot.currentMembership.joined.performedBy.subject", "different"],
      ["$.groupTransitions[0].companionEventId", "different"],
      ["$.groupTransitions[0].control.controlRevision", 1],
      ["$.groupTransitions[0].control.group.environmentId", "other"],
      ["$.occurredAt", "not-a-time"],
      ["$.occurredAt", "2026-02-30T00:00:00Z"],
      ["$.performedBy.subject", "\t"],
      ["$.performedBy.sessionId", "x".repeat(2001)],
      ["$.extra", "unexpected"],
      ["$.snapshot.currentMembership.state", "retired"],
    ] as const) {
      const changed = yield* sql<{
        value: string;
      }>`SELECT json_set(${encodedRoot},${path},json(${JSON.stringify(value)})) value`;
      yield* sql`BEGIN`;
      yield* selectMembershipGroup(batch.companions[0]!, 102);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(insertMembershipCompanion(batch, 0, 101, 102, changed[0]!.value)),
        ),
        path,
      );
      yield* sql`ROLLBACK`;
    }
    for (const changed of [
      encodedRoot.replace("{", '{"id":"duplicate",'),
      encodedRoot.replace('"groupTransitions":[', '"groupTransitions":[{},'),
    ]) {
      yield* sql`BEGIN`;
      yield* selectMembershipGroup(batch.companions[0]!, 102);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(insertMembershipCompanion(batch, 0, 101, 102, changed))),
      );
      yield* sql`ROLLBACK`;
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* selectMembershipGroup(batch.companions[0]!, 102);
        yield* insertMembershipCompanion(batch, 0, 101, 102);
        yield* writeMembershipHistory(batch);
      }),
    );
    const rows = yield* sql<{
      joined_record_json: string;
    }>`SELECT joined_record_json FROM work_task_group_memberships WHERE task_id='a'`;
    assert.deepStrictEqual(
      JSON.parse(rows[0]!.joined_record_json),
      batch.event.snapshot.currentMembership,
    );
    const reject = Effect.fn("PersistentTasks.rejectMembershipWrite")(function* (query: string) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(query))), query);
    });
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      for (const query of [
        "INSERT OR REPLACE INTO work_task_group_memberships SELECT * FROM work_task_group_memberships",
        "INSERT OR REPLACE INTO work_task_membership_companions SELECT * FROM work_task_membership_companions",
        "DELETE FROM work_task_group_memberships",
        "DELETE FROM work_task_membership_companions",
        "UPDATE work_task_group_memberships SET group_id='other'",
        "UPDATE work_task_membership_companions SET root_sequence=999",
        "INSERT INTO work_task_group_memberships(rowid) VALUES(1)",
        "INSERT INTO work_task_membership_companions(rowid) VALUES(1)",
        "INSERT OR REPLACE INTO work_task_accepted_definitions VALUES ('a',1,1,'changed','{}','accepted','raw')",
        "INSERT OR REPLACE INTO work_task_accepted_definitions VALUES ('a',2,1,'changed','{}','accepted','raw')",
        "INSERT OR REPLACE INTO work_task_projections VALUES ('a',NULL,2,1,1,1,'changed','raw','raw')",
        "INSERT OR REPLACE INTO work_task_manager_groups SELECT * FROM work_task_manager_groups",
      ])
        yield* reject(query);
    }
    yield* sql`INSERT INTO work_task_projections VALUES ('b',NULL,1,1,1,1,'{}','raw','raw')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('b',1,1,'{}','{}','other-accepted','raw')`;
    const otherCreated = decodeGroupControlEvent({
      ...groupContext("other-membership-group"),
      type: "task-group.created",
      control: emptyGroupControl("other"),
    });
    yield* writeGroupControl(otherCreated, 103);
    for (const query of [
      "UPDATE OR REPLACE work_task_projections SET task_id='a' WHERE task_id='b'",
      "UPDATE OR REPLACE work_task_accepted_definitions SET accepted_event_id='accepted' WHERE task_id='b'",
      "UPDATE OR REPLACE work_task_accepted_definitions SET task_id='a' WHERE task_id='b'",
      "UPDATE OR REPLACE work_task_manager_groups SET group_id='group' WHERE group_id='other'",
    ])
      yield* reject(query);
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',2,1,'{}','{}','accepted-new','raw')`;
    yield* sql`UPDATE work_task_projections SET definition_revision=2 WHERE task_id='a'`;
    const newerSnapshot = {
      ...batch.event.snapshot,
      taskRevisions: {
        ...batch.event.snapshot.taskRevisions,
        definitionRevision: 2,
        acceptedDefinitionRevision: 2,
      },
    };
    const removed = membershipTransition(
      newerSnapshot,
      [batch.companions[0]!.control],
      null,
      "membership-remove",
    );
    yield* sql.withTransaction(applyMembership(removed, 110));
    const rejoined = membershipTransition(
      removed.event.snapshot,
      [removed.companions[0]!.control],
      "group",
      "membership-rejoin",
    );
    yield* sql.withTransaction(applyMembership(rejoined, 120));
    const moved = membershipTransition(
      rejoined.event.snapshot,
      [rejoined.companions[0]!.control, otherCreated.control],
      "other",
      "membership-move",
      "2026-11-15T01:00:00.0002+01:00",
    );
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      for (const reverse of [false, true]) {
        yield* sql`BEGIN`;
        for (const [index, companion] of moved.companions.entries())
          yield* selectMembershipGroup(companion, 131 + index);
        const first = reverse ? 1 : 0;
        yield* insertMembershipCompanion(moved, first, 130, 131 + first);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
        yield* sql`ROLLBACK`;
        yield* sql`BEGIN`;
        for (const [index, companion] of moved.companions.entries())
          yield* selectMembershipGroup(companion, 131 + index);
        for (const index of reverse ? [1, 0] : [0, 1])
          yield* insertMembershipCompanion(moved, index, 130, 131 + index);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
        yield* sql`ROLLBACK`;
        yield* sql`BEGIN`;
        yield* applyMembership(moved, 130, reverse);
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        yield* sql`ROLLBACK`;
      }
    }
    yield* sql.withTransaction(applyMembership(moved, 130, true));
    assert.deepStrictEqual(
      (yield* sql`SELECT membership_generation,accepted_definition_revision,retired_event_id FROM work_task_group_memberships WHERE task_id='a' ORDER BY membership_generation`).map(
        (row) => ({ ...row }),
      ),
      [
        {
          membership_generation: 1,
          accepted_definition_revision: 1,
          retired_event_id: "membership-remove",
        },
        {
          membership_generation: 2,
          accepted_definition_revision: 2,
          retired_event_id: "membership-move",
        },
        { membership_generation: 3, accepted_definition_revision: 2, retired_event_id: null },
      ],
    );
    const currentControl = moved.companions.find(
      (entry) => entry.control.group.groupId === "other",
    )!.control;
    const bound = decodeGroupControlEvent({
      ...groupContext("member-manager-bind"),
      type: "task-group.manager-changed",
      previousControl: currentControl,
      control: {
        ...currentControl,
        controlRevision: 1,
        highestManagerGeneration: 1,
        currentManager: { threadId: "member-manager", managerGeneration: 1 },
      },
    });
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* writeGroupControl(bound, 140);
        yield* bindGroupManager(bound, 140);
      }),
    );
    const replaced = decodeGroupControlEvent({
      ...groupContext("member-manager-replace"),
      type: "task-group.manager-changed",
      previousControl: bound.control,
      control: {
        ...bound.control,
        controlRevision: 2,
        highestManagerGeneration: 2,
        currentManager: { threadId: "member-manager-new", managerGeneration: 2 },
      },
    });
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* writeGroupControl(replaced, 141);
        yield* retireGroupManager(replaced, 141);
        yield* bindGroupManager(replaced, 141);
      }),
    );
    const paused = decodeGroupControlEvent({
      ...groupContext("member-manager-pause"),
      type: "task-group.hold-changed",
      previousControl: replaced.control,
      control: { ...replaced.control, controlRevision: 3, dispatchHold: "paused" },
    });
    yield* writeGroupControl(paused, 142);
    assert.strictEqual(
      (yield* sql<{
        latest_membership_event_id: string;
      }>`SELECT latest_membership_event_id FROM work_task_manager_groups WHERE group_id='other'`)[0]!
        .latest_membership_event_id,
      moved.companions[1]!.id,
    );
    const pendingRemoval = membershipTransition(
      moved.event.snapshot,
      [paused.control],
      null,
      "pending-removal",
    );
    const abandoned = decodeGroupControlEvent({
      ...groupContext("abandon-removal"),
      type: "task-group.hold-changed",
      previousControl: pendingRemoval.companions[0]!.control,
      control: {
        ...pendingRemoval.companions[0]!.control,
        controlRevision: 4,
        dispatchHold: "none",
      },
    });
    yield* sql`BEGIN`;
    yield* selectMembershipGroup(pendingRemoval.companions[0]!, 151);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(writeGroupControl(abandoned, 152))));
    yield* insertMembershipCompanion(pendingRemoval, 0, 150, 151);
    yield* writeGroupControl(abandoned, 152);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
    yield* sql`ROLLBACK`;
    for (const [query, index] of [
      [
        "SELECT task_id,membership_generation FROM work_task_group_memberships WHERE environment_id='env' AND group_id='other' AND retired_event_id IS NULL ORDER BY task_id",
        "work_task_group_members_current_idx",
      ],
      [
        "SELECT group_id,membership_generation FROM work_task_group_memberships WHERE task_id='a' AND retired_event_id IS NULL",
        "work_task_membership_current_idx",
      ],
      [
        "SELECT max(membership_generation) FROM work_task_group_memberships WHERE task_id='a'",
        "work_task_membership_history_idx",
      ],
    ]) {
      const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(
        plan.some((row) => row.detail.includes(index!)),
        JSON.stringify(plan),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
    }
    const triggers = yield* sql<{
      sql: string;
    }>`SELECT sql FROM sqlite_master WHERE type='trigger' AND name='work_task_membership_companion_insert'`;
    const peerPredicate = triggers[0]!.sql.match(
      /FROM work_task_membership_companions p WHERE (.*?)\n        AND/s,
    )?.[1];
    assert.isDefined(peerPredicate);
    const boundPredicate = peerPredicate!
      .replaceAll("NEW.peer_event_id", "'peer'")
      .replaceAll("NEW.event_id", "'event'");
    const peerPlan = yield* sql.unsafe<{ detail: string }>(
      `EXPLAIN QUERY PLAN SELECT 1 FROM work_task_membership_companions p WHERE ${boundPredicate}`,
    );
    assert.isTrue(
      peerPlan.some((row) => row.detail.includes("work_task_membership_peer_idx")),
      JSON.stringify(peerPlan),
    );
    assert.isTrue(
      peerPlan.some((row) => row.detail.includes("PRIMARY KEY")),
      JSON.stringify(peerPlan),
    );
    assert.isFalse(
      peerPlan.some((row) => row.detail.includes("SCAN")),
      JSON.stringify(peerPlan),
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves populated group history and restores the closed stage on failed membership upgrade",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 94 });
      yield* sql`INSERT INTO work_task_projections VALUES ('legacy-task',NULL,1,1,1,1,'{ "raw": 1 }','raw','raw')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('legacy-task',1,1,'{ "raw": 1 }','{}','legacy-accept','raw')`;
      yield* sql`INSERT INTO work_task_manager_bindings VALUES ('legacy-task',1,'legacy-manager','raw',NULL)`;
      const created = decodeGroupControlEvent({
        ...groupContext("upgrade-group"),
        type: "task-group.created",
        control: emptyGroupControl(),
      });
      yield* writeGroupControl(created, 500);
      const bound = decodeGroupControlEvent({
        ...groupContext("upgrade-bound"),
        type: "task-group.manager-changed",
        previousControl: created.control,
        control: {
          ...created.control,
          controlRevision: 1,
          highestManagerGeneration: 1,
          currentManager: { threadId: "group-manager", managerGeneration: 1 },
        },
      });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writeGroupControl(bound, 501);
          yield* bindGroupManager(bound, 501);
        }),
      );
      const tables = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`;
      const columns: Record<string, string> = {};
      for (const table of tables) {
        const fields = yield* sql.unsafe<{ name: string }>(
          `PRAGMA table_info("${table.name.replaceAll('"', '""')}")`,
        );
        columns[table.name] = fields
          .map((field) => {
            const name = '"' + field.name.replaceAll('"', '""') + '"';
            return `typeof(${name})||':'||coalesce(hex(CAST(${name} AS BLOB)),'NULL') AS ${name}`;
          })
          .join(",");
      }
      const capture = Effect.fn("PersistentTasks.captureMembershipUpgrade")(function* () {
        const result: Record<string, readonly string[]> = {};
        for (const table of tables) {
          const rows = yield* sql.unsafe(
            `SELECT ${columns[table.name]} FROM "${table.name.replaceAll('"', '""')}"`,
          );
          result[table.name] = rows.map((row) => JSON.stringify(row)).toSorted();
        }
        return result;
      });
      const before = yield* capture();
      yield* sql`CREATE TRIGGER work_task_group_membership_initial BEFORE INSERT ON work_task_manager_groups BEGIN SELECT 1; END`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* capture(), before);
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name IN ('work_task_group_memberships','work_task_membership_companions')`,
        [],
      );
      assert.strictEqual(
        (yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'work_task_group_membership_closed_%'`)
          .length,
        2,
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(sql`UPDATE work_task_manager_groups SET membership_revision=1`),
        ),
      );
      yield* sql`DROP TRIGGER work_task_group_membership_initial`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [95, "WorkTaskManagerMemberships"],
        [96, "WorkTaskDirtyReconciliation"],
        [97, "WorkTaskAcceptedRuns"],
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      assert.deepStrictEqual(
        { ...after, effect_sql_migrations: before.effect_sql_migrations! },
        before,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_group_memberships`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_membership_companions`, []);
      assert.isNull(
        (yield* sql<{
          latest_membership_event_id: string | null;
        }>`SELECT latest_membership_event_id FROM work_task_manager_groups`)[0]!
          .latest_membership_event_id,
      );
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

const dirtyEvent = (id: string, taskId = "a", sourceEventId = "source", sourceSequence = 1) => ({
  id,
  environmentId: "env",
  occurredAt: "2026-11-15T00:00:00Z",
  commandId: `command-${id}`,
  attribution: { type: "server" },
  type: "task.dirty-source-recorded",
  sourceEventId,
  sourceSequence,
  task: { environmentId: "env", taskId },
  membership: null,
  previousDirtyGeneration: 0,
  dirtyGeneration: 1,
  acknowledgedGeneration: 0,
});
const scanEvent = (id: string) => ({
  id,
  environmentId: "env",
  occurredAt: "2026-11-15T00:00:00Z",
  commandId: `command-${id}`,
  attribution: { type: "server" },
  type: "task.reconciliation-progressed",
  completed: false,
  scan: {
    kind: "graph_dependents",
    sourceEventId: "source",
    sourceSequence: 1,
    blocker: { environmentId: "env", taskId: "b" },
    capturedRevision: 0,
  },
  observedRevision: 0,
  previousPosition: null,
  position: { dependentTaskId: "a", edgeId: "edge" },
  visited: [{ environmentId: "env", taskId: "a" }],
});
const decodeCoordination = Schema.decodeUnknownEffect(WorkTaskPersistedCoordinationEvent);
const insertCoordination = Effect.fn("PersistentTasks.insertCoordination")(function* (
  event: Record<string, unknown>,
  sequence: number,
  decode = true,
  replace = false,
) {
  const sql = yield* SqlClient.SqlClient;
  const decoded = decode
    ? (yield* decodeCoordination({
        event,
        context: {
          sequence,
          eventId: event.id,
          commandId: event.commandId,
          occurredAt: event.occurredAt,
        },
      })).event
    : event;
  const table =
    event.type === "task.reconciliation-progressed"
      ? "work_task_reconciliation_events"
      : "work_task_dirty_events";
  yield* sql.unsafe(
    `INSERT ${replace ? "OR REPLACE" : ""} INTO ${table}(event_id,sequence,event_json) SELECT json_extract(value,'$.id'),?,value FROM (SELECT ? value)`,
    [sequence, JSON.stringify(decoded)],
  );
});
it.effect("retains dirty generations and commits scan progress only with every dirty visit", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','raw','raw'),('b',NULL,1,1,1,1,'{}','raw','raw'),('c',NULL,1,1,1,1,'{}','raw','raw')`;
    const source = dirtyEvent("dirty-a");
    const progress = scanEvent("progress");
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      yield* sql`BEGIN`;
      yield* insertCoordination(progress, 3);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
      yield* sql`ROLLBACK`;
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_reconciliation_events`, []);
      for (const progressFirst of [true, false]) {
        yield* sql`BEGIN`;
        if (progressFirst) yield* insertCoordination(progress, 3);
        yield* insertCoordination(source, 2);
        if (!progressFirst) yield* insertCoordination(progress, 3);
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        for (const table of [
          "work_task_dirty_events",
          "work_task_reconciliation_events",
          "work_task_reconciliation_visits",
        ]) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(sql.unsafe(`UPDATE ${table} SET event_id='rewritten'`)),
            ),
          );
          assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(`DELETE FROM ${table}`))));
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sql.unsafe(`INSERT OR REPLACE INTO ${table} SELECT * FROM ${table}`),
              ),
            ),
          );
        }
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(insertCoordination({ ...source, id: "replacement" }, 2, true, true)),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              insertCoordination(
                { ...source, id: "duplicate", previousDirtyGeneration: 1, dirtyGeneration: 2 },
                4,
              ),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              insertCoordination(
                {
                  ...progress,
                  id: "restart",
                  scan: { ...progress.scan, capturedRevision: 1 },
                  observedRevision: 1,
                },
                4,
              ),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(insertCoordination({ ...progress, id: "stale" }, 4))),
        );
        yield* insertCoordination(
          {
            ...progress,
            id: "finished",
            previousPosition: progress.position,
            position: progress.position,
            visited: [],
            completed: true,
          },
          4,
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              insertCoordination(
                {
                  ...progress,
                  id: "resurrected",
                  previousPosition: progress.position,
                  position: progress.position,
                  visited: [],
                  completed: true,
                },
                5,
              ),
            ),
          ),
        );
        yield* sql`ROLLBACK`;
      }
    }
    yield* insertCoordination(source, 2);
    yield* insertCoordination(
      {
        ...dirtyEvent("dirty-a-2", "a", "source2", 3),
        previousDirtyGeneration: 1,
        dirtyGeneration: 2,
      },
      4,
    );
    const { previousDirtyGeneration: _previous, ...common } = source;
    const ack = {
      ...common,
      id: "ack",
      type: "task.dirty-acknowledged",
      previousAcknowledgedGeneration: 0,
      acknowledgedGeneration: 1,
      observedDirtyGeneration: 1,
      dirtyGeneration: 2,
    };
    yield* insertCoordination(ack, 5);
    assert.isTrue(
      Exit.isFailure(yield* Effect.exit(insertCoordination({ ...ack, id: "stale-ack" }, 6))),
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          insertCoordination(
            {
              ...ack,
              id: "future-ack",
              previousAcknowledgedGeneration: 1,
              acknowledgedGeneration: 2,
              observedDirtyGeneration: 1,
            },
            6,
            false,
          ),
        ),
      ),
    );
    yield* insertCoordination(
      {
        ...dirtyEvent("third", "a", "source3", 6),
        previousDirtyGeneration: 2,
        dirtyGeneration: 3,
        acknowledgedGeneration: 1,
      },
      7,
    );
    assert.deepStrictEqual(
      (yield* sql`SELECT dirty_generation,acknowledged_generation FROM work_task_dirty_events WHERE task_id='a' ORDER BY sequence DESC LIMIT 1`).map(
        (row) => ({ ...row }),
      ),
      [{ dirty_generation: 3, acknowledged_generation: 1 }],
    );
    const emptyAck = {
      ...ack,
      id: "empty-ack",
      task: { environmentId: "env", taskId: "c" },
      acknowledgedGeneration: 0,
      observedDirtyGeneration: 0,
      dirtyGeneration: 0,
    };
    yield* insertCoordination(emptyAck, 8);
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          insertCoordination(
            {
              ...dirtyEvent("wrong-env", "c", "other-source", 9),
              environmentId: "other",
              task: { environmentId: "other", taskId: "c" },
            },
            10,
          ),
        ),
      ),
    );
    for (const [index, value] of ["\0x", "😀".repeat(1000), "\ud800"].entries()) {
      yield* sql`BEGIN`;
      yield* insertCoordination(
        {
          ...dirtyEvent(value, "b", `unicode-source-${index}`, 10),
          commandId: "unicode-command",
          occurredAt: "0000-01-01T00:00:00.000000001+23:59",
        },
        11,
      );
      assert.strictEqual(
        (yield* sql<{
          value: string;
        }>`SELECT hex(event_id) value FROM work_task_dirty_events WHERE task_id='b'`)[0]!.value,
        index === 2 ? "EDA080" : Buffer.from(value).toString("hex").toUpperCase(),
      );
      yield* sql`ROLLBACK`;
    }
    for (const patch of [
      { id: " " },
      { id: "x".repeat(2001) },
      { sourceSequence: 11 },
      { sourceEventId: "invalid" },
      { attribution: { type: "human" } },
      { occurredAt: "invalid" },
      { previousDirtyGeneration: 0.5 },
      { membership: {} },
      { task: { environmentId: "other", taskId: "b" } },
    ]) {
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            insertCoordination(
              { ...dirtyEvent("invalid", "b", "source-invalid", 10), ...patch },
              11,
              false,
            ),
          ),
        ),
      );
    }
    yield* sql`BEGIN`;
    yield* insertCoordination(progress, 12);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    yield* sql`COMMIT`;
    for (const query of [
      "SELECT event_json FROM work_task_dirty_events WHERE task_id='a' ORDER BY sequence DESC LIMIT 1",
      "SELECT event_json FROM work_task_reconciliation_events WHERE job_key='key' ORDER BY sequence DESC LIMIT 1",
      "SELECT 1 FROM work_task_dirty_events WHERE source_id='source' AND source_sequence<>1",
      "SELECT 1 FROM work_task_dirty_events WHERE source_sequence=1 AND source_id<>'source'",
      "SELECT 1 FROM work_task_reconciliation_visits WHERE environment_id='env' AND task_id='a' AND source_id='source' AND source_sequence=1",
    ]) {
      const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(plan.some((row) => row.detail.includes("SEARCH")));
      assert.isFalse(plan.some((row) => /SCAN |TEMP B-TREE/.test(row.detail)));
    }
    yield* sql`BEGIN`;
    const visited = Array.from({ length: 100 }, (_, index) => ({
      environmentId: "env",
      taskId: `v${String(index).padStart(3, "0")}`,
    }));
    for (const [index, task] of visited.entries()) {
      yield* sql`INSERT INTO work_task_projections VALUES (${task.taskId},NULL,1,1,1,1,'{}','raw','raw')`;
      yield* insertCoordination(
        dirtyEvent(`batch-${index}`, task.taskId, "batch-source", 400),
        401 + index,
      );
    }
    const batchScan = {
      ...progress,
      id: "batch-scan",
      scan: { ...progress.scan, sourceEventId: "batch-source", sourceSequence: 400 },
      position: { dependentTaskId: "v099", edgeId: "last-edge" },
      visited,
    };
    for (const patch of [
      { visited: [...visited, visited[0]] },
      { visited: [visited[1], visited[0]], position: { dependentTaskId: "v000", edgeId: "edge" } },
      { visited: [{ environmentId: "other", taskId: "v099" }] },
      { previousPosition: { dependentTaskId: "z", edgeId: "edge" } },
      { scan: { ...batchScan.scan, sourceSequence: 600 } },
      { observedRevision: -1 },
      { completed: undefined, unknown: false },
    ])
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(insertCoordination({ ...batchScan, ...patch }, 600, false)),
        ),
      );
    yield* insertCoordination(batchScan, 600);
    assert.strictEqual(
      (yield* sql<{
        n: number;
      }>`SELECT count(*) n FROM work_task_reconciliation_visits WHERE event_id='batch-scan'`)[0]!.n,
      100,
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    yield* sql`ROLLBACK`;
    const installed = yield* sql<{
      sql: string;
    }>`SELECT sql FROM sqlite_master WHERE name IN ('work_task_dirty_valid','work_task_reconciliation_valid')`;
    for (const trigger of installed) {
      const queries = [
        ...trigger.sql.matchAll(
          /SELECT event_json FROM work_task_(?:dirty|reconciliation)_events WHERE (?:task_id=NEW.task_id|job_key=NEW.job_key) ORDER BY sequence DESC LIMIT 1/g,
        ),
      ];
      assert.isNotEmpty(queries);
      for (const match of queries) {
        const plan = yield* sql.unsafe<{ detail: string }>(
          "EXPLAIN QUERY PLAN " +
            match[0].replaceAll("NEW.task_id", "'a'").replaceAll("NEW.job_key", "'job'"),
        );
        assert.isTrue(plan.some((row) => row.detail.includes("SEARCH")));
        assert.isFalse(plan.some((row) => /SCAN |TEMP B-TREE/.test(row.detail)));
      }
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect("preserves dirty scan membership observations and rolls back migration96", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 95 });
    yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','raw','raw'),('b',NULL,1,1,1,1,'{}','raw','raw')`;
    yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',1,1,'{}','{}','accepted','raw')`;
    yield* writeGroupControl(
      decodeGroupControlEvent({
        ...groupContext("group"),
        type: "task-group.created",
        control: emptyGroupControl(),
      }),
      100,
    );
    const batch = membershipJoin();
    yield* sql.withTransaction(applyMembership(batch, 101));
    const before = yield* sql`SELECT * FROM work_task_group_memberships`;
    yield* sql`CREATE TABLE work_task_reconciliation_visits(collision TEXT)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name IN ('work_task_dirty_events','work_task_reconciliation_events')`,
      [],
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_group_memberships`, before);
    yield* sql`DROP TABLE work_task_reconciliation_visits`;
    assert.deepStrictEqual(yield* runMigrations(), [
      [96, "WorkTaskDirtyReconciliation"],
      [97, "WorkTaskAcceptedRuns"],
      [98, "WorkTaskAcceptedRunObservations"],
      [99, "WorkTaskManagerClaims"],
      [100, "WorkTaskOwnershipReleaseBridge"],
      [101, "WorkTaskReadyState"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_group_memberships`, before);
    const member = batch.event.snapshot.currentMembership!;
    const source = {
      ...dirtyEvent("member-dirty"),
      membership: { group: member.group, membershipGeneration: member.membershipGeneration },
    };
    const event = {
      ...scanEvent("member-scan"),
      scan: {
        kind: "group_members",
        sourceEventId: "source",
        sourceSequence: 1,
        group: member.group,
        capturedRevision: 1,
      },
      observedRevision: 1,
      position: { taskId: "a" },
      visited: [{ task: member.task, membershipGeneration: member.membershipGeneration }],
    };
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* insertCoordination(event, 201);
        yield* insertCoordination(source, 200);
      }),
    );
    const removed = membershipTransition(
      batch.event.snapshot,
      batch.companions.map((row) => row.control),
      null,
      "remove",
    );
    yield* sql.withTransaction(applyMembership(removed, 202));
    yield* insertCoordination(
      {
        ...source,
        id: "after-retirement",
        sourceEventId: "later-source",
        sourceSequence: 204,
        previousDirtyGeneration: 1,
        dirtyGeneration: 2,
      },
      205,
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          insertCoordination(
            {
              ...event,
              id: "bad-member",
              scan: { ...event.scan, sourceEventId: "new-source", sourceSequence: 206 },
              visited: [{ task: member.task, membershipGeneration: 99 }],
            },
            207,
          ),
        ),
      ),
    );
    yield* insertCoordination(
      {
        ...event,
        id: "finished-member",
        previousPosition: event.position,
        visited: [],
        completed: true,
      },
      208,
    );
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          sql`INSERT OR REPLACE INTO work_task_reconciliation_visits VALUES ('member-scan','env','a','source',1,1)`,
        ),
      ),
    );
    const captured = yield* sql`SELECT * FROM work_task_dirty_events`;
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_dirty_events`, captured);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

const acceptedRunPaths = {
  run_id: "ownership.run.runId",
  message_id: "ownership.run.messageId",
  environment_id: "environmentId",
  task_id: "ownership.run.task.taskId",
  group_id: "ownership.run.group.groupId",
  manager_generation: "ownership.run.manager.managerGeneration",
  manager_thread_id: "ownership.run.manager.threadId",
  membership_generation: "ownership.run.membershipGeneration",
  project_id: "ownership.run.projectId",
  provider_instance_id: "ownership.run.providerInstanceId",
  definition_revision: "ownership.run.taskRevisions.definitionRevision",
  accepted_definition_revision: "ownership.run.taskRevisions.acceptedDefinitionRevision",
  lifecycle_revision: "ownership.run.taskRevisions.lifecycleRevision",
  completion_policy_revision: "ownership.run.taskRevisions.completionPolicyRevision",
  action_policy_revision: "ownership.run.taskRevisions.actionPolicyRevision",
  graph_revision: "ownership.run.graphRevision",
  control_revision: "ownership.run.controlRevision",
  membership_revision: "ownership.run.membershipRevision",
  recorded_event_id: "id",
  recorded_sequence: "ownership.recorded.sequence",
  recorded_command_id: "commandId",
  recorded_at: "occurredAt",
  native_run_event_id: "ownership.nativeAcceptance.runEvent.eventId",
  native_run_sequence: "ownership.nativeAcceptance.runEvent.sequence",
  native_message_event_id: "ownership.nativeAcceptance.messageEvent.eventId",
  native_message_sequence: "ownership.nativeAcceptance.messageEvent.sequence",
  native_receipt_command_id: "ownership.nativeAcceptance.receipt.commandId",
  native_receipt_result_sequence: "ownership.nativeAcceptance.receipt.resultSequence",
  origin_type: "ownership.origin.type",
  reservation_id: "ownership.origin.reservationId",
  reservation_command_id: "ownership.origin.reservationCommandId",
  allowance_unit: "ownership.origin.allowanceUnit",
  debit_event_id: "ownership.origin.debitEvent.eventId",
  debit_sequence: "ownership.origin.debitEvent.sequence",
};
const insertAcceptedRun = Effect.fn("PersistentTasks.insertAcceptedRun")(function* (
  event: unknown,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_accepted_runs (${Object.keys(acceptedRunPaths).join(",")},event_json)
    SELECT ${Object.values(acceptedRunPaths)
      .map((p) => `json_extract(value,'$.${p}')`)
      .join(",")},value FROM (SELECT ? AS value)`,
    [JSON.stringify(event)],
  );
});
const decodeAcceptedOwnership = Schema.decodeSync(WorkTaskPersistedAcceptedRunOwnershipEvent);
const decodeAcceptedOwnershipEffect = Schema.decodeUnknownEffect(
  WorkTaskPersistedAcceptedRunOwnershipEvent,
);
const acceptedOwnershipFixture = (suffix = "one", offset = 0) => {
  const occurredAt = "2026-11-15T00:00:01.0009Z";
  const recorded = {
    eventId: `owner-${suffix}`,
    sequence: 1000 + offset,
    commandId: `record-${suffix}`,
    occurredAt,
  };
  const decoded = decodeAcceptedOwnership({
    context: recorded,
    event: {
      id: recorded.eventId,
      type: "task.run-ownership-accepted",
      environmentId: "env",
      commandId: recorded.commandId,
      occurredAt,
      attribution: { type: "server" },
      ownership: {
        recorded,
        run: {
          version: 2,
          task: { environmentId: "env", taskId: "a" },
          group: { environmentId: "env", groupId: "group" },
          manager: { managerGeneration: 1, threadId: "manager" },
          membershipGeneration: 1,
          membershipRevision: 1,
          controlRevision: 1,
          graphRevision: 0,
          projectId: "project",
          providerInstanceId: "codex",
          runId: `run-${suffix}`,
          messageId: `message-${suffix}`,
          taskRevisions: {
            definitionRevision: 3,
            acceptedDefinitionRevision: 2,
            lifecycleRevision: 1,
            completionPolicyRevision: 2,
            actionPolicyRevision: 3,
          },
        },
        nativeAcceptance: {
          receipt: {
            commandId: `native-${suffix}`,
            threadId: "manager",
            commandType: "thread.message.send",
            acceptedAt: "2026-11-15T00:00:01.0001Z",
            resultSequence: 202 + offset,
            status: "accepted",
            error: null,
          },
          runEvent: {
            type: "run.created",
            eventId: `native-run-${suffix}`,
            sequence: 201 + offset,
          },
          messageEvent: {
            type: "message.updated",
            eventId: `native-message-${suffix}`,
            sequence: 202 + offset,
          },
          runStatus: "queued",
        },
        sourceEventId: "shared-source",
        sourceSequence: 100,
        observedWorkSequence: 100,
        prerequisites: [],
        origin: {
          type: "initial_start",
          startCommandId: "start-command",
          startEventId: "shared-source",
          startSequence: 100,
        },
      },
    },
  }).event;
  if (decoded.type !== "task.run-ownership-accepted")
    throw new Error("Expected accepted ownership");
  return decoded;
};
const seedAcceptedOwnership = Effect.fn("PersistentTasks.seedAcceptedOwnership")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO work_task_projections VALUES ('a',NULL,1,1,1,1,'{}','raw','raw')`;
  yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',1,1,'{}','{}','definition-one','raw')`;
  yield* writeGroupControl(
    decodeGroupControlEvent({
      ...groupContext("owner-group"),
      type: "task-group.created",
      control: emptyGroupControl(),
    }),
    10,
  );
  const join = membershipJoin();
  yield* sql.withTransaction(applyMembership(join, 11));
  const bound = decodeGroupControlEvent({
    ...groupContext("owner-manager"),
    type: "task-group.manager-changed",
    previousControl: join.companions[0]!.control,
    control: {
      ...join.companions[0]!.control,
      controlRevision: 1,
      highestManagerGeneration: 1,
      currentManager: { managerGeneration: 1, threadId: "manager" },
    },
  });
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* writeGroupControl(bound, 13);
      yield* bindGroupManager(bound, 13);
    }),
  );
  yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',2,1,'{}','{}','definition-two','raw')`;
  yield* sql`UPDATE work_task_projections SET definition_revision=3,completion_policy_revision=2,action_policy_revision=3 WHERE task_id='a'`;
  return { bound, join };
});
const automaticOwnershipFixture = (suffix: string, offset: number, unit = 1) => {
  const base = acceptedOwnershipFixture(suffix, offset);
  const sequence = 203 + offset;
  const commandId = `reservation-command-${suffix}`;
  const revisions = base.ownership.run.taskRevisions;
  const debit = decodePersistedEvent({
    event: {
      id: `debit-${suffix}`,
      type: "task.allowance-debited",
      taskId: "a",
      commandId,
      occurredAt: base.occurredAt,
      definitionRevision: revisions.definitionRevision,
      lifecycleRevision: revisions.lifecycleRevision,
      completionPolicyRevision: revisions.completionPolicyRevision,
      actionPolicyRevision: revisions.actionPolicyRevision,
      attribution: { type: "server" },
      previous: { type: "configured", cap: unit, consumed: unit - 1 },
      resulting: { type: "configured", cap: unit, consumed: unit },
      allowanceUnit: unit,
    },
    context: { eventId: `debit-${suffix}`, sequence, commandId, occurredAt: base.occurredAt },
  });
  const event = decodeAcceptedOwnership({
    context: base.ownership.recorded,
    event: {
      ...base,
      ownership: {
        ...base.ownership,
        origin: {
          type: "automatic_followup",
          reservationId: `reservation-${suffix}`,
          reservationCommandId: commandId,
          allowanceUnit: unit,
          observedDirtyGeneration: 3,
          acknowledgedGeneration: 1,
          pending: {
            sources: [{ kind: "result", sourceId: "pending", resultRef: null }],
            overflowThroughGeneration: 3,
          },
          debitEvent: {
            type: "task.allowance-debited",
            eventId: debit.event.id,
            sequence,
            commandId,
            taskId: "a",
            definitionRevision: revisions.definitionRevision,
            lifecycleRevision: revisions.lifecycleRevision,
            completionPolicyRevision: revisions.completionPolicyRevision,
            actionPolicyRevision: revisions.actionPolicyRevision,
            allowanceUnit: unit,
          },
        },
      },
    },
  }).event;
  if (event.type !== "task.run-ownership-accepted") throw new Error("Expected accepted ownership");
  return { event, debit };
};
const insertAcceptedDebit = Effect.fn("PersistentTasks.insertAcceptedDebit")(function* (
  runId: string,
  event: unknown,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO work_task_accepted_run_debits(event_id,run_id,sequence,task_id,command_id,allowance_unit,definition_revision,lifecycle_revision,completion_policy_revision,action_policy_revision,occurred_at,event_json)
    SELECT json_extract(value,'$.id'),json_extract(${JSON.stringify(runId)},'$'),${sequence},json_extract(value,'$.taskId'),json_extract(value,'$.commandId'),json_extract(value,'$.allowanceUnit'),
      json_extract(value,'$.definitionRevision'),json_extract(value,'$.lifecycleRevision'),json_extract(value,'$.completionPolicyRevision'),json_extract(value,'$.actionPolicyRevision'),json_extract(value,'$.occurredAt'),value FROM (SELECT ${JSON.stringify(event)} value)`;
});
it.effect(
  "retains accepted run ownership without physical launch or another allowance charge",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* seedAcceptedOwnership();
      const event = acceptedOwnershipFixture();
      yield* insertAcceptedRun(event);
      assert.deepStrictEqual(
        JSON.parse(
          (yield* sql<{
            event_json: string;
          }>`SELECT event_json FROM work_task_accepted_runs WHERE run_id='run-one'`)[0]!.event_json,
        ),
        event,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_run_debits`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
      for (const [index, reverse] of [false, true].entries()) {
        const automatic = automaticOwnershipFixture(`auto-${index}`, 20 + index * 20, index + 1);
        const owner = insertAcceptedRun(automatic.event);
        const debit = insertAcceptedDebit(
          automatic.event.ownership.run.runId,
          automatic.debit.event,
          automatic.debit.context.sequence,
        );
        yield* sql`BEGIN`;
        yield* reverse ? debit : owner;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql`COMMIT`)));
        yield* sql`ROLLBACK`;
        yield* sql`BEGIN`;
        yield* reverse ? debit : owner;
        yield* reverse ? owner : debit;
        yield* sql`COMMIT`;
      }
      assert.strictEqual(
        (yield* sql<{ count: number }>`SELECT count(*) count FROM work_task_accepted_runs`)[0]!
          .count,
        3,
      );
      const unicode = acceptedOwnershipFixture("\0😀\ud800", 60);
      yield* insertAcceptedRun(unicode);
      assert.deepStrictEqual(
        JSON.parse(
          (yield* sql<{
            event_json: string;
          }>`SELECT event_json FROM work_task_accepted_runs WHERE recorded_sequence=1060`)[0]!
            .event_json,
        ),
        unicode,
      );
      const batch = automaticOwnershipFixture("bounded", 80, 3);
      const pending = {
        sources: Array.from({ length: 32 }, (_, i) => ({
          kind: "result",
          sourceId: `source-${i}`,
          resultRef: null,
        })),
        overflowThroughGeneration: 3,
      };
      const prerequisites = Array.from({ length: 64 }, (_, i) => ({
        edgeId: `edge-${i}`,
        blocker: { environmentId: "env", taskId: `blocker-${i}` },
        completionEventId: `completion-${i}`,
        completionSequence: i,
      }));
      const bounded = {
        ...batch.event,
        ownership: {
          ...batch.event.ownership,
          prerequisites,
          origin: { ...batch.event.ownership.origin, pending },
        },
      };
      yield* decodeAcceptedOwnershipEffect({
        event: bounded,
        context: bounded.ownership.recorded,
      });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertAcceptedRun(bounded);
          yield* insertAcceptedDebit(
            bounded.ownership.run.runId,
            batch.debit.event,
            batch.debit.context.sequence,
          );
        }),
      );
      assert.strictEqual(
        (yield* sql<{
          count: number;
        }>`SELECT count(*) count FROM work_task_accepted_run_references WHERE accepted_event_id='owner-bounded'`)[0]!
          .count,
        69,
      );
      for (const reverse of [false, true]) {
        const unknownBase = acceptedOwnershipFixture("unknown", 100);
        const unknown = {
          ...unknownBase,
          ownership: {
            ...unknownBase.ownership,
            sourceEventId: "future-native",
            sourceSequence: 400,
          },
        };
        const knownBase = acceptedOwnershipFixture("known", 200);
        const known = {
          ...knownBase,
          ownership: {
            ...knownBase.ownership,
            nativeAcceptance: {
              ...knownBase.ownership.nativeAcceptance,
              runEvent: { type: "run.created", eventId: "future-native", sequence: 400 },
            },
          },
        };
        yield* sql`BEGIN`;
        yield* insertAcceptedRun(reverse ? known : unknown);
        yield* insertAcceptedRun(reverse ? unknown : known);
        const conflictBase = acceptedOwnershipFixture("conflict", 300);
        for (const ownership of [
          { ...conflictBase.ownership, sourceEventId: "future-native", sourceSequence: 401 },
          { ...conflictBase.ownership, sourceEventId: "different", sourceSequence: 400 },
          {
            ...conflictBase.ownership,
            prerequisites: [
              {
                edgeId: "wrong-kind",
                blocker: { environmentId: "env", taskId: "other" },
                completionEventId: "future-native",
                completionSequence: 400,
              },
            ],
          },
        ])
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(insertAcceptedRun({ ...conflictBase, ownership }))),
          );
        yield* sql`ROLLBACK`;
      }
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

it.effect("rejects contradictory accepted ownership and deferred legacy interleavings", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const reject = Effect.fn("PersistentTasks.rejectAcceptedWrite")(function* (
      effect: Effect.Effect<unknown, SqlError, SqlClient.SqlClient>,
    ) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
    });
    const base = acceptedOwnershipFixture();
    const changes: ReadonlyArray<readonly [string, unknown]> = [
      ["$.id", "x".repeat(2001)],
      ["$.occurredAt", "2026-02-30T00:00:00Z"],
      ["$.attribution.type", "user"],
      ["$.ownership.run.version", 3],
      ["$.ownership.run.providerInstanceId", "a/b"],
      ["$.ownership.run.providerInstanceId", "a\0b"],
      ["$.ownership.run.providerInstanceId", "a".repeat(65)],
      ["$.ownership.run.membershipGeneration", 0],
      ["$.ownership.run.manager.managerGeneration", 0],
      ["$.ownership.run.task.environmentId", "other"],
      ["$.ownership.run.taskRevisions.acceptedDefinitionRevision", 4],
      ["$.ownership.run.effectId", "fabricated"],
      ["$.ownership.run.runId", "\t\ufeff"],
      ["$.ownership.sourceSequence", 1000],
      ["$.ownership.observedWorkSequence", 1001],
      ["$.ownership.recorded.commandId", "wrong"],
      ["$.ownership.nativeAcceptance.receipt.threadId", "other"],
      ["$.ownership.nativeAcceptance.receipt.resultSequence", 200],
      ["$.ownership.nativeAcceptance.receipt.acceptedAt", "no"],
      ["$.ownership.nativeAcceptance.receipt.error", "error"],
      ["$.ownership.nativeAcceptance.runEvent.eventId", "native-message-one"],
      ["$.ownership.nativeAcceptance.runEvent.sequence", 202],
      [
        "$.ownership.prerequisites",
        [
          {
            edgeId: "e",
            blocker: { environmentId: "env", taskId: "a" },
            completionEventId: "done",
            completionSequence: 5,
          },
        ],
      ],
      ["$.ownership.origin.allowanceUnit", 1],
    ];
    for (const [path, value] of changes) {
      const [row] = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(base)},${path},json(${JSON.stringify(value)})) value`;
      const variant: unknown = JSON.parse(row!.value);
      yield* reject(insertAcceptedRun(variant));
    }
    yield* insertAcceptedRun(base);
    const automatic = automaticOwnershipFixture("guarded", 20);
    const debit = insertAcceptedDebit(
      automatic.event.ownership.run.runId,
      automatic.debit.event,
      automatic.debit.context.sequence,
    );
    for (const [path, value] of [
      [
        "$.ownership.origin.pending.sources",
        Array.from({ length: 33 }, (_, i) => ({ kind: "r", sourceId: String(i), resultRef: null })),
      ],
      [
        "$.ownership.origin.pending.sources",
        [
          { kind: "r", sourceId: "same", resultRef: null },
          { kind: "r", sourceId: "same", resultRef: "different" },
        ],
      ],
      ["$.ownership.origin.pending.sources", [{ kind: "r", sourceId: "missing", extra: null }]],
      ["$.ownership.origin.pending.overflowThroughGeneration", 4],
      ["$.ownership.origin.debitEvent.allowanceUnit", 2],
      ["$.ownership.origin.debitEvent.allowanceUnit", true],
      ["$.ownership.origin.debitEvent.taskId", "other"],
    ] as ReadonlyArray<readonly [string, unknown]>) {
      const [row] = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(automatic.event)},${path},json(${JSON.stringify(value)})) value`;
      const malformed: unknown = JSON.parse(row!.value);
      yield* reject(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertAcceptedRun(malformed);
            yield* debit;
          }),
        ),
      );
    }
    const owner = insertAcceptedRun(automatic.event);
    const origin = automatic.event.ownership.origin;
    if (origin.type !== "automatic_followup") throw new Error("Expected automatic origin");
    yield* sql`INSERT INTO work_task_manager_bindings VALUES ('a',1,'legacy-manager','raw',NULL)`;
    yield* sql`INSERT INTO work_task_allowances VALUES ('a',20,1)`;
    yield* sql`INSERT INTO work_task_claims VALUES ('legacy-claim','a','legacy-claim','evidence','legacy-manager',1,1,1,0,'{"type":"server"}','summary','[{"type":"artifact","uri":"u","hash":"h"}]',2,'raw')`;
    yield* sql`INSERT INTO work_task_validation_attempts VALUES ('attempt','a',1,1,1,'evidence','watermark','{}',NULL,'reserved',NULL,'raw',NULL,NULL)`;
    const legacyWake = sql`INSERT INTO work_task_wake_reservations VALUES ('legacy-reservation','a',1,'legacy-command',NULL,NULL,1,1,0,'raw',NULL)`;
    const legacyValidation = sql`INSERT INTO work_task_validation_identity VALUES ('attempt','a','legacy-claim',1)`;
    for (const recursive of [0, 1]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      for (const legacy of [legacyWake, legacyValidation]) {
        for (const first of [owner, debit]) {
          yield* sql`BEGIN`;
          yield* first;
          yield* reject(legacy);
          yield* sql`ROLLBACK`;
          yield* sql`BEGIN`;
          yield* legacy;
          yield* reject(first);
          yield* sql`ROLLBACK`;
        }
      }
      yield* sql`BEGIN`;
      yield* owner;
      yield* sql`INSERT INTO work_task_wake_reservations VALUES ('unrelated','a',1,'unrelated',NULL,NULL,2,1,0,'raw',NULL)`;
      for (const set of [
        "allowance_unit=1",
        "run_id='run-guarded'",
        "message_id='message-guarded'",
        "reservation_id='reservation-guarded'",
        "command_id='reservation-command-guarded'",
      ])
        yield* reject(
          sql.unsafe(
            `UPDATE OR REPLACE work_task_wake_reservations SET ${set} WHERE reservation_id='unrelated'`,
          ),
        );
      yield* sql`ROLLBACK`;
      yield* sql`BEGIN`;
      yield* debit;
      yield* sql`INSERT INTO work_task_validation_identity VALUES ('attempt','a','legacy-claim',2)`;
      yield* reject(
        sql`UPDATE OR REPLACE work_task_validation_identity SET allowance_unit=1 WHERE attempt_id='attempt'`,
      );
      yield* sql`ROLLBACK`;
      for (const table of ["work_task_accepted_runs", "work_task_accepted_run_references"]) {
        yield* reject(sql.unsafe(`INSERT OR REPLACE INTO ${table} SELECT * FROM ${table}`));
        yield* reject(
          sql.unsafe(
            `UPDATE ${table} SET ${table.endsWith("references") ? "sequence=sequence" : "recorded_sequence=recorded_sequence"}`,
          ),
        );
        yield* reject(sql.unsafe(`DELETE FROM ${table}`));
      }
      yield* reject(
        sql`UPDATE work_task_accepted_definitions SET definition_json='{"changed":true}' WHERE task_id='a' AND definition_revision=2`,
      );
      yield* reject(
        sql`INSERT OR REPLACE INTO work_task_accepted_definitions VALUES ('a',2,1,'{}','{}','new-definition','raw')`,
      );
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('a',4,1,'{}','{}','unrelated-definition','raw')`;
      yield* reject(
        sql`UPDATE OR REPLACE work_task_accepted_definitions SET definition_revision=2 WHERE definition_revision=4`,
      );
      yield* reject(
        sql`UPDATE OR REPLACE work_task_accepted_definitions SET accepted_event_id='definition-two' WHERE definition_revision=4`,
      );
      yield* sql`DELETE FROM work_task_accepted_definitions WHERE definition_revision=4`;
    }
    for (const key of [
      "commandId",
      "definitionRevision",
      "lifecycleRevision",
      "completionPolicyRevision",
      "actionPolicyRevision",
    ] as const) {
      const value = key === "commandId" ? "other" : 9;
      const wrong = { ...automatic.debit.event, [key]: value };
      yield* sql`BEGIN`;
      yield* owner;
      const inserted = yield* Effect.exit(
        insertAcceptedDebit(
          automatic.event.ownership.run.runId,
          wrong,
          automatic.debit.context.sequence,
        ),
      );
      if (Exit.isSuccess(inserted)) yield* reject(sql`COMMIT`);
      yield* sql`ROLLBACK`;
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* debit;
        yield* owner;
      }),
    );
    const before = yield* sql`SELECT * FROM work_task_accepted_run_debits`;
    yield* reject(
      sql`INSERT OR REPLACE INTO work_task_accepted_run_debits SELECT * FROM work_task_accepted_run_debits`,
    );
    yield* reject(sql`UPDATE work_task_accepted_run_debits SET sequence=sequence`);
    yield* reject(sql`DELETE FROM work_task_accepted_run_debits`);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_accepted_run_debits`, before);
    assert.deepStrictEqual(yield* sql`SELECT configured_cap,consumed FROM work_task_allowances`, [
      { configured_cap: 20, consumed: 1 },
    ]);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves accepted ownership through migration retry and manager retirement with indexed reads",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 96 });
      const { bound, join } = yield* seedAcceptedOwnership();
      yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at,deleted_at) VALUES ('ordinary','Ordinary','/untouched','[ 1 ]','raw','raw',NULL)`;
      const names = (yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).map(
        (r) => r.name,
      );
      const capture = Effect.fn("PersistentTasks.captureAcceptedPredecessor")(function* () {
        return Object.fromEntries(
          yield* Effect.forEach(names, (name) =>
            sql.unsafe(`SELECT * FROM "${name}"`).pipe(Effect.map((rows) => [name, rows] as const)),
          ),
        );
      });
      const before = yield* capture();
      yield* sql`CREATE INDEX work_task_accepted_thread_idx ON work_task_projections(task_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* capture(), before);
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='work_task_accepted_runs'`,
        [],
      );
      yield* sql`DROP INDEX work_task_accepted_thread_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [97, "WorkTaskAcceptedRuns"],
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      for (const name of names.filter((name) => name !== "effect_sql_migrations"))
        assert.deepStrictEqual(after[name], before[name]);
      const first = acceptedOwnershipFixture();
      const second = acceptedOwnershipFixture("two", 20);
      yield* insertAcceptedRun(first);
      yield* insertAcceptedRun(second);
      const retained = yield* sql`SELECT * FROM work_task_accepted_runs ORDER BY run_id`;
      const retired = membershipTransition(
        { ...join.event.snapshot, taskRevisions: first.ownership.run.taskRevisions },
        [bound.control],
        null,
        "retire-owner-member",
      );
      yield* sql.withTransaction(applyMembership(retired, 5000));
      const previousControl = retired.companions[0]!.control;
      const stopped = decodeGroupControlEvent({
        ...groupContext("retire-owner-manager"),
        occurredAt: retired.event.occurredAt,
        type: "task-group.manager-changed",
        previousControl,
        control: {
          ...previousControl,
          controlRevision: previousControl.controlRevision + 1,
          currentManager: null,
        },
      });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writeGroupControl(stopped, 5002);
          yield* retireGroupManager(stopped, 5002);
        }),
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_accepted_runs ORDER BY run_id`,
        retained,
      );
      for (const query of [
        "SELECT event_json FROM work_task_accepted_runs WHERE run_id='run-one'",
        "SELECT run_id FROM work_task_accepted_runs WHERE task_id='a' AND (recorded_sequence,run_id)<(2000,'z') ORDER BY recorded_sequence DESC,run_id LIMIT 50",
        "SELECT run_id FROM work_task_accepted_runs WHERE environment_id='env' AND group_id='group' ORDER BY recorded_sequence,run_id LIMIT 50",
        "SELECT run_id FROM work_task_accepted_runs WHERE manager_thread_id='manager' ORDER BY run_id LIMIT 50",
        "SELECT event_id FROM work_task_accepted_run_references WHERE event_id='shared-source' AND sequence=100",
        "SELECT event_id FROM work_task_accepted_run_references WHERE sequence=100 AND event_id='shared-source'",
      ]) {
        const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
          .map((r) => r.detail)
          .join(" ");
        assert.include(plan, "SEARCH");
        assert.notInclude(plan, "TEMP B-TREE");
      }
      for (const operation of ["insert", "update"]) {
        const [trigger] = yield* sql<{
          sql: string;
        }>`SELECT sql FROM sqlite_master WHERE type='trigger' AND name=${`work_task_wake_reservations_accepted_${operation}`}`;
        const predicate = trigger!.sql.split("SELECT CASE WHEN ")[1]!.split(" THEN RAISE")[0]!;
        const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN SELECT ${predicate}
          FROM (SELECT 'a' task_id,1 allowance_unit,'r' run_id,'m' message_id,'c' command_id,'v' reservation_id) NEW`))
          .map((row) => row.detail)
          .join(" ");
        assert.include(plan, "work_task_accepted_debit_command_idx");
        assert.notInclude(plan, "SCAN work_task_accepted_run_debits");
      }
      assert.deepStrictEqual(
        (yield* sql<{
          run_id: string;
        }>`SELECT run_id FROM work_task_accepted_runs WHERE task_id='a' ORDER BY recorded_sequence DESC,run_id LIMIT 1`).map(
          (r) => r.run_id,
        ),
        ["run-two"],
      );
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

const retainedOwnership = (owner: ReturnType<typeof acceptedOwnershipFixture>) => {
  const { run, recorded, nativeAcceptance, origin } = owner.ownership;
  return {
    run,
    recorded,
    nativeRunEvent: nativeAcceptance.runEvent,
    origin:
      origin.type === "initial_start"
        ? origin
        : {
            type: origin.type,
            reservationId: origin.reservationId,
            reservationCommandId: origin.reservationCommandId,
            allowanceUnit: origin.allowanceUnit,
            debitEvent: origin.debitEvent,
          },
  };
};
const ownershipObservation = (
  owner = acceptedOwnershipFixture(),
  id = "uncertain",
  sequence = 2000,
) => {
  const context = {
    eventId: id,
    sequence,
    commandId: `observe-${id}`,
    occurredAt: "2026-11-16T00:00:00.0001Z",
  };
  const decoded = decodeAcceptedOwnership({
    context,
    event: {
      id,
      type: "task.run-ownership-uncertain",
      environmentId: owner.environmentId,
      commandId: context.commandId,
      occurredAt: context.occurredAt,
      attribution: { type: "server" },
      ownership: retainedOwnership(owner),
      sourceEventId: owner.ownership.sourceEventId,
      sourceSequence: owner.ownership.sourceSequence,
      reason: "Native response not yet reconciled",
    },
  });
  if (decoded.event.type !== "task.run-ownership-uncertain")
    throw new Error("Expected uncertain ownership");
  return { event: decoded.event, context: decoded.context };
};
const ownershipTermination = (
  owner = acceptedOwnershipFixture(),
  id = "terminated",
  sequence = 2002,
) => {
  const { context } = ownershipObservation(owner, id, sequence);
  const decoded = decodeAcceptedOwnership({
    context,
    event: {
      id,
      type: "task.run-ownership-terminated",
      environmentId: owner.environmentId,
      commandId: context.commandId,
      occurredAt: context.occurredAt,
      attribution: { type: "server" },
      ownership: retainedOwnership(owner),
      observedSequence: 500,
      terminationEvidence: {
        kind: "before_executable_claim",
        terminalRunEvent: { type: "run.updated", eventId: "native-terminal", sequence: 500 },
        terminalStatus: "cancelled",
        settlementCommandId: "settlement",
      },
    },
  });
  if (decoded.event.type !== "task.run-ownership-terminated")
    throw new Error("Expected terminated ownership");
  return { event: decoded.event, context: decoded.context };
};
const rejectOwnershipWrite = Effect.fn("PersistentTasks.rejectOwnershipWrite")(function* (
  effect: Effect.Effect<unknown, SqlError, SqlClient.SqlClient>,
  detail?: string,
) {
  assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)), detail);
});
const observationPaths = {
  event_id: "id",
  run_id: "ownership.run.runId",
  environment_id: "environmentId",
  task_id: "ownership.run.task.taskId",
  group_id: "ownership.run.group.groupId",
  manager_thread_id: "ownership.run.manager.threadId",
  kind: "type",
  command_id: "commandId",
  occurred_at: "occurredAt",
  source_event_id: "sourceEventId",
  source_sequence: "sourceSequence",
  observed_sequence: "observedSequence",
  terminal_event_id: "terminationEvidence.terminalRunEvent.eventId",
  terminal_sequence: "terminationEvidence.terminalRunEvent.sequence",
  terminal_status: "terminationEvidence.terminalStatus",
  settlement_command_id: "terminationEvidence.settlementCommandId",
};
const insertOwnershipObservation = Effect.fn("PersistentTasks.insertOwnershipObservation")(
  function* (event: unknown, sequence: number) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(
      `INSERT INTO work_task_run_ownership_observations (${Object.keys(observationPaths).join(",")},sequence,event_json)
    SELECT ${Object.values(observationPaths)
      .map((p) => `json_extract(value,'$.${p}')`)
      .join(",")},?,value FROM (SELECT ? value)`,
      [sequence, JSON.stringify(event)],
    );
  },
);
it.effect("retains monotonic ownership observations without changing accepted debit evidence", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const observed = ownershipObservation(owner);
    const reference = retainedOwnership(owner);
    yield* insertOwnershipObservation(
      {
        ...observed.event,
        ownership: {
          origin: reference.origin,
          nativeRunEvent: reference.nativeRunEvent,
          recorded: reference.recorded,
          run: reference.run,
        },
      },
      observed.context.sequence,
    );
    assert.deepStrictEqual(
      (yield* sql<{
        event_json: string;
      }>`SELECT event_json FROM work_task_run_ownership_observations`).map((r) =>
        JSON.parse(r.event_json),
      ),
      [observed.event],
    );
    const again = ownershipObservation(owner, "uncertain-again", 2001);
    yield* insertOwnershipObservation(again.event, again.context.sequence);
    const terminal = ownershipTermination(owner);
    yield* insertOwnershipObservation(terminal.event, terminal.context.sequence);
    const late = ownershipObservation(owner, "late", 2003);
    yield* rejectOwnershipWrite(insertOwnershipObservation(late.event, late.context.sequence));
    const unicodeOwner = acceptedOwnershipFixture("unicode\0😀\ud800", 40);
    yield* insertAcceptedRun(unicodeOwner);
    const unicodeObservation = ownershipObservation(unicodeOwner, "uncertain\0😀\ud800", 2150);
    yield* insertOwnershipObservation(
      unicodeObservation.event,
      unicodeObservation.context.sequence,
    );
    const [unicodeRow] = yield* sql<{
      event_json: string;
    }>`SELECT event_json FROM work_task_run_ownership_observations WHERE run_id=json_extract(${JSON.stringify(unicodeOwner.ownership.run.runId)},'$')`;
    assert.deepStrictEqual(JSON.parse(unicodeRow!.event_json), unicodeObservation.event);
    for (const debitFirst of [false, true]) {
      const automatic = automaticOwnershipFixture(
        `observed-${debitFirst}`,
        debitFirst ? 80 : 60,
        debitFirst ? 2 : 1,
      );
      const next = ownershipObservation(
        automatic.event,
        `automatic-${debitFirst}`,
        debitFirst ? 2201 : 2200,
      );
      const debit = insertAcceptedDebit(
        automatic.event.ownership.run.runId,
        automatic.debit.event,
        automatic.debit.context.sequence,
      );
      const insertOwner = insertAcceptedRun(automatic.event);
      const insertObservation = insertOwnershipObservation(next.event, next.context.sequence);
      yield* sql`BEGIN`;
      yield* insertOwner;
      yield* insertObservation;
      yield* rejectOwnershipWrite(sql`COMMIT`);
      yield* sql`ROLLBACK`;
      assert.deepStrictEqual(
        yield* sql`SELECT event_id FROM work_task_run_ownership_observations WHERE event_id=${next.event.id}`,
        [],
      );
      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (debitFirst) yield* debit;
          yield* insertOwner;
          yield* insertObservation;
          if (!debitFirst) yield* debit;
        }),
      );
    }
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    assert.equal((yield* sql`SELECT event_id FROM work_task_accepted_run_debits`).length, 2);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "rejects ownership observation contradictions and preserves shared source references",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* seedAcceptedOwnership();
      const owner = acceptedOwnershipFixture();
      const second = acceptedOwnershipFixture("second", 20);
      const third = acceptedOwnershipFixture("third", 40);
      for (const value of [owner, second, third]) yield* insertAcceptedRun(value);
      const observation = ownershipObservation(owner);
      for (const [path, value] of [
        ["$.id", "x".repeat(2001)],
        ["$.attribution.type", "human"],
        ["$.environmentId", "other"],
        ["$.occurredAt", "2026-02-30T00:00:00Z"],
        ["$.sourceSequence", 2000],
        ["$.sourceSequence", true],
        ["$.reason", "\t\ufeff"],
        ["$.ownership.run.runId", second.ownership.run.runId],
        ["$.ownership.run.messageId", "different"],
        ["$.ownership.run.taskRevisions.definitionRevision", 4],
        ["$.ownership.run.taskRevisions.acceptedDefinitionRevision", 1],
        ["$.ownership.run.taskRevisions.actionPolicyRevision", true],
        ["$.ownership.recorded.commandId", "different"],
        ["$.ownership.nativeRunEvent.sequence", 200],
        ["$.ownership.origin.startCommandId", "different"],
        ["$.observedSequence", null],
      ] as ReadonlyArray<readonly [string, unknown]>) {
        const [row] = yield* sql<{
          value: string;
        }>`SELECT json_set(${JSON.stringify(observation.event)},${path},json(${JSON.stringify(value)})) value`;
        const malformed: unknown = JSON.parse(row!.value);
        yield* rejectOwnershipWrite(
          insertOwnershipObservation(malformed, observation.context.sequence),
          `${path}: ${JSON.stringify(value)}`,
        );
      }
      yield* insertOwnershipObservation(observation.event, observation.context.sequence);
      for (const sequence of [1999, 2000]) {
        const older = ownershipObservation(owner, `older-${sequence}`, sequence);
        yield* rejectOwnershipWrite(insertOwnershipObservation(older.event, sequence));
      }
      const terminal = ownershipTermination(owner);
      for (const [path, value] of [
        ["$.terminationEvidence.terminalStatus", "completed"],
        ["$.terminationEvidence.terminalRunEvent.type", "run.created"],
        ["$.terminationEvidence.terminalRunEvent.sequence", 200],
        ["$.terminationEvidence.terminalRunEvent.sequence", true],
        ["$.terminationEvidence.settlementCommandId", "\u00a0"],
        ["$.terminationEvidence.workClaimId", null],
        ["$.observedSequence", 2002],
        ["$.observedSequence", 499],
        ["$.reason", "extra"],
      ] as ReadonlyArray<readonly [string, unknown]>) {
        const [row] = yield* sql<{
          value: string;
        }>`SELECT json_set(${JSON.stringify(terminal.event)},${path},json(${JSON.stringify(value)})) value`;
        const malformed: unknown = JSON.parse(row!.value);
        yield* rejectOwnershipWrite(
          insertOwnershipObservation(malformed, terminal.context.sequence),
          `${path}: ${JSON.stringify(value)}`,
        );
      }
      const afterClaim = {
        ...terminal.event,
        observedSequence: 600,
        terminationEvidence: {
          kind: "after_executable_claim",
          terminalRunEvent: { type: "run.updated", eventId: "native-terminal", sequence: 500 },
          terminalStatus: "completed",
          workClaimId: "physical-claim",
          workReleaseEvent: {
            type: "task-group.work-released",
            eventId: "physical-release",
            sequence: 600,
          },
        },
      };
      yield* decodeAcceptedOwnershipEffect({ event: afterClaim, context: terminal.context });
      yield* rejectOwnershipWrite(
        insertOwnershipObservation(afterClaim, terminal.context.sequence),
      );
      for (const bFirst of [false, true]) {
        const future = acceptedOwnershipFixture("future", 200);
        const source = {
          ...ownershipObservation(owner, `future-${bFirst}`, 2100).event,
          sourceEventId: future.ownership.nativeAcceptance.runEvent.eventId,
          sourceSequence: future.ownership.nativeAcceptance.runEvent.sequence,
        };
        yield* sql`BEGIN`;
        if (bFirst) {
          yield* insertOwnershipObservation(source, 2100);
          const wrong = {
            ...future,
            ownership: {
              ...future.ownership,
              nativeAcceptance: {
                ...future.ownership.nativeAcceptance,
                runEvent: { ...future.ownership.nativeAcceptance.runEvent, sequence: 400 },
              },
            },
          };
          yield* rejectOwnershipWrite(insertAcceptedRun(wrong));
          yield* insertAcceptedRun(future);
        } else {
          yield* insertAcceptedRun(future);
          yield* rejectOwnershipWrite(
            insertOwnershipObservation({ ...source, sourceSequence: 400 }, 2100),
          );
          yield* insertOwnershipObservation(source, 2100);
        }
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        yield* sql`ROLLBACK`;
      }
      yield* insertOwnershipObservation(terminal.event, terminal.context.sequence);
      for (const [value, sequence] of [
        [second, 2200],
        [third, 2300],
      ] as const) {
        const shared = {
          ...ownershipObservation(value, `shared-${sequence}`, sequence).event,
          sourceEventId: "native-terminal",
          sourceSequence: 500,
        };
        yield* insertOwnershipObservation(shared, sequence);
      }
      const reused = ownershipTermination(second, "reused-terminal", 2400);
      yield* rejectOwnershipWrite(
        insertOwnershipObservation(reused.event, reused.context.sequence),
      );
      const before =
        yield* sql`SELECT * FROM work_task_run_ownership_observations ORDER BY event_id`;
      for (const recursive of [0, 1]) {
        yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
        yield* rejectOwnershipWrite(sql`INSERT OR REPLACE INTO work_task_run_ownership_observations
        SELECT 'replacement',run_id,environment_id,task_id,group_id,manager_thread_id,kind,command_id,occurred_at,source_event_id,source_sequence,observed_sequence,terminal_event_id,terminal_sequence,terminal_status,settlement_command_id,2500,json_set(event_json,'$.id','replacement'),NULL FROM work_task_run_ownership_observations WHERE event_id='terminated'`);
        for (const table of [
          "work_task_run_ownership_observations",
          "work_task_run_observation_references",
        ]) {
          yield* rejectOwnershipWrite(
            sql.unsafe(`INSERT OR REPLACE INTO ${table} SELECT * FROM ${table}`),
          );
          yield* rejectOwnershipWrite(
            sql.unsafe(`UPDATE OR REPLACE ${table} SET event_id=event_id`),
          );
          yield* rejectOwnershipWrite(sql.unsafe(`DELETE FROM ${table}`));
        }
        yield* rejectOwnershipWrite(
          sql`INSERT INTO work_task_run_observation_references VALUES ('uncertain','fabricated',0,'source',1,NULL,NULL)`,
        );
      }
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_run_ownership_observations ORDER BY event_id`,
        before,
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves ownership history through observation upgrade, journal deletion and retired-manager queries",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 97 });
      const { bound, join } = yield* seedAcceptedOwnership();
      const owner = acceptedOwnershipFixture();
      const automatic = automaticOwnershipFixture("retained", 20);
      yield* insertAcceptedRun(owner);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertAcceptedRun(automatic.event);
          yield* insertAcceptedDebit(
            automatic.event.ownership.run.runId,
            automatic.debit.event,
            automatic.debit.context.sequence,
          );
        }),
      );
      yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json)
      VALUES (500,'native-terminal','thread','manager',1,'run.updated','2026-11-16T00:00:00Z','server','{"id":"run-one","status":"cancelled"}','{}'),
      (501,'ordinary-preserved','thread','ordinary',1,'thread.created','raw','client','{ "preserved": true }','{}')`;
      const names = (yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).map(
        (r) => r.name,
      );
      const capture = Effect.fn("PersistentTasks.captureObservationPredecessor")(function* () {
        return Object.fromEntries(
          yield* Effect.forEach(names, (name) =>
            sql.unsafe(`SELECT * FROM "${name}"`).pipe(Effect.map((rows) => [name, rows] as const)),
          ),
        );
      });
      const before = yield* capture();
      yield* sql`CREATE INDEX work_task_run_observation_task_idx ON work_task_accepted_runs(task_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* capture(), before);
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='work_task_run_ownership_observations'`,
        [],
      );
      yield* sql`DROP INDEX work_task_run_observation_task_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [98, "WorkTaskAcceptedRunObservations"],
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      for (const name of names.filter((name) => name !== "effect_sql_migrations"))
        assert.deepStrictEqual(after[name], before[name]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_run_ownership_observations`, []);
      const uncertainty = ownershipObservation(owner);
      const terminal = ownershipTermination(owner);
      const pending = ownershipObservation(automatic.event, "pending", 2200);
      for (const value of [uncertainty, terminal, pending])
        yield* insertOwnershipObservation(value.event, value.context.sequence);
      const retained =
        yield* sql`SELECT * FROM work_task_run_ownership_observations ORDER BY event_id`;
      yield* sql`DELETE FROM orchestration_events WHERE event_id='native-terminal'`;
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_events WHERE event_id='ordinary-preserved'`).length,
        1,
      );
      const retired = membershipTransition(
        { ...join.event.snapshot, taskRevisions: owner.ownership.run.taskRevisions },
        [bound.control],
        null,
        "retire-observed-member",
      );
      yield* sql.withTransaction(applyMembership(retired, 5000));
      const previousControl = retired.companions[0]!.control;
      const stopped = decodeGroupControlEvent({
        ...groupContext("retire-observed-manager"),
        occurredAt: retired.event.occurredAt,
        type: "task-group.manager-changed",
        previousControl,
        control: {
          ...previousControl,
          controlRevision: previousControl.controlRevision + 1,
          currentManager: null,
        },
      });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writeGroupControl(stopped, 5002);
          yield* retireGroupManager(stopped, 5002);
        }),
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_run_ownership_observations ORDER BY event_id`,
        retained,
      );
      const unclosed = `NOT EXISTS(SELECT 1 FROM work_task_run_ownership_observations o WHERE o.run_id=a.run_id AND o.kind='task.run-ownership-terminated')`;
      for (const query of [
        "SELECT a.run_id,(SELECT event_id FROM work_task_run_ownership_observations WHERE run_id=a.run_id ORDER BY sequence DESC LIMIT 1) FROM work_task_accepted_runs a WHERE a.run_id='run-one'",
        `SELECT a.run_id FROM work_task_accepted_runs a WHERE a.task_id='a' AND ${unclosed} ORDER BY a.recorded_sequence DESC,a.run_id LIMIT 50`,
        `SELECT a.run_id FROM work_task_accepted_runs a WHERE a.environment_id='env' AND a.group_id='group' AND ${unclosed} ORDER BY a.recorded_sequence,a.run_id LIMIT 50`,
        `SELECT a.run_id FROM work_task_accepted_runs a WHERE a.manager_thread_id='manager' AND ${unclosed} ORDER BY a.run_id LIMIT 50`,
        "SELECT event_id FROM work_task_run_ownership_observations WHERE task_id='a' AND (sequence,event_id)<(3000,'z') ORDER BY sequence DESC,event_id LIMIT 50",
      ]) {
        const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
          .map((r) => r.detail)
          .join(" ");
        assert.include(plan, "SEARCH");
        assert.notInclude(plan, "TEMP B-TREE");
      }
      assert.deepStrictEqual(
        (yield* sql.unsafe<{ run_id: string }>(
          `SELECT a.run_id FROM work_task_accepted_runs a WHERE a.manager_thread_id='manager' AND ${unclosed} ORDER BY a.run_id`,
        )).map((r) => r.run_id),
        [automatic.event.ownership.run.runId],
      );
      for (const name of [
        "work_task_accepted_reference_observation",
        "work_task_run_observation_reference_valid",
      ]) {
        const [trigger] = yield* sql<{
          sql: string;
        }>`SELECT sql FROM sqlite_master WHERE name=${name} AND type='trigger'`;
        const predicate = trigger!.sql.split("SELECT CASE WHEN ").at(-1)!.split(" THEN RAISE")[0]!;
        const plan = (yield* sql.unsafe<{ detail: string }>(
          `EXPLAIN QUERY PLAN SELECT ${predicate} FROM (SELECT 'source' event_id,100 sequence,'run.updated' known_kind,'a' known_task_id) NEW`,
        ))
          .map((r) => r.detail)
          .join(" ");
        assert.include(plan, "SEARCH r USING INDEX");
        assert.notInclude(plan, "SCAN r");
      }
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    }).pipe(Effect.provide(db)),
);

const decodePhysical = Schema.decodeUnknownSync(WorkTaskPersistedCoordinationEvent);
const physicalClaimPaths = {
  work_claim_id: "claim.binding.workClaimId",
  run_id: "claim.binding.runId",
  environment_id: "environmentId",
  task_id: "claim.binding.task.taskId",
  group_id: "claim.binding.group.groupId",
  manager_generation: "claim.binding.manager.managerGeneration",
  manager_thread_id: "claim.binding.manager.threadId",
  membership_generation: "claim.binding.membershipGeneration",
  accepted_event_id: "id",
  accepted_sequence: "claim.acceptedSequence",
  effect_id: "claim.binding.effectId",
  accepted_definition_revision: "claim.binding.taskRevisions.acceptedDefinitionRevision",
};
const insertPhysicalClaim = Effect.fn("PersistentTasks.insertPhysicalClaim")(function* (
  event: unknown,
  release: string | null = null,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_executable_claims (${Object.keys(physicalClaimPaths).join(",")},event_json,release_event_id)
    SELECT ${Object.values(physicalClaimPaths)
      .map((path) => `json_extract(value,'$.${path}')`)
      .join(",")},value,? FROM (SELECT ? AS value)`,
    [release, JSON.stringify(event)],
  );
});
const physicalClaimFixture = (
  owner: ReturnType<typeof acceptedOwnershipFixture>,
  suffix = "one",
  sequence = 3000,
  membership = membershipJoin().event.snapshot,
) => {
  const occurredAt = owner.occurredAt;
  const binding = {
    ...owner.ownership.run,
    workClaimId: `physical-${suffix}`,
    commandId: `activate-${suffix}`,
    effectId: `effect-${suffix}`,
  };
  const origin = owner.ownership.origin;
  const decoded = decodePhysical({
    context: { eventId: `claim-${suffix}`, sequence, commandId: binding.commandId, occurredAt },
    event: {
      id: `claim-${suffix}`,
      type: "task-group.work-accepted",
      environmentId: owner.environmentId,
      commandId: binding.commandId,
      occurredAt,
      attribution: { type: "server" },
      previousActiveClaimId: null,
      claim: {
        binding,
        acceptedEventId: `claim-${suffix}`,
        acceptedSequence: sequence,
        acceptedAt: occurredAt,
        sourceEventId: owner.id,
        sourceSequence: owner.ownership.recorded.sequence,
        observedWorkSequence: owner.ownership.recorded.sequence,
        prerequisites: [],
        launchOrigin:
          origin.type === "initial_start"
            ? origin
            : {
                type: origin.type,
                reservationId: origin.reservationId,
                reservationCommandId: origin.reservationCommandId,
                allowanceUnit: origin.allowanceUnit,
                observedDirtyGeneration: origin.observedDirtyGeneration,
                acknowledgedGeneration: origin.acknowledgedGeneration,
              },
      },
      control: {
        ...emptyGroupControl(),
        membershipRevision: binding.membershipRevision,
        controlRevision: 1,
        highestManagerGeneration: 1,
        currentManager: binding.manager,
        dispatchHold: "none",
      },
      membership: { ...membership, taskRevisions: binding.taskRevisions },
    },
  }).event;
  if (decoded.type !== "task-group.work-accepted")
    throw new Error("Expected physical work acceptance");
  return decoded;
};
const retainedPhysicalClaim = (event: ReturnType<typeof physicalClaimFixture>) => ({
  binding: event.claim.binding,
  acceptedEventId: event.id,
  acceptedSequence: event.claim.acceptedSequence,
  acceptedAt: event.occurredAt,
});
const physicalReleaseFixture = (
  claim: ReturnType<typeof physicalClaimFixture>,
  suffix = "one",
  sequence = 4000,
  settled = false,
) => {
  const terminalRunEvent = {
    type: "run.updated" as const,
    eventId: `terminal-${suffix}`,
    sequence: sequence - 2,
  };
  const terminalAttemptEvent = {
    type: "run-attempt.updated" as const,
    eventId: `attempt-terminal-${suffix}`,
    sequence: sequence - 3,
  };
  const occurredAt = claim.occurredAt;
  const event = decodePhysical({
    context: { eventId: `release-${suffix}`, sequence, commandId: `settle-${suffix}`, occurredAt },
    event: {
      id: `release-${suffix}`,
      type: "task-group.work-released",
      environmentId: claim.environmentId,
      commandId: `settle-${suffix}`,
      occurredAt,
      attribution: { type: "server" },
      claim: retainedPhysicalClaim(claim),
      observedSequence: sequence - 1,
      releaseEvidence: settled
        ? {
            type: "run_settled",
            terminalRunEvent,
            attempt: {
              attemptId: `attempt-${suffix}`,
              providerThreadId: "provider-thread",
              providerTurnId: null,
              terminalAttemptEvent,
            },
          }
        : { type: "not_started", terminalRunEvent, settlementCommandId: `settle-${suffix}` },
    },
  }).event;
  if (event.type !== "task-group.work-released") throw new Error("Expected release");
  const native = {
    run: {
      ...terminalRunEvent,
      runId: claim.claim.binding.runId,
      threadId: claim.claim.binding.manager.threadId,
      status: settled ? "completed" : "cancelled",
      occurredAt,
    },
    ...(settled
      ? {
          attempt: {
            ...terminalAttemptEvent,
            runId: claim.claim.binding.runId,
            attemptId: `attempt-${suffix}`,
            providerThreadId: "provider-thread",
            providerTurnId: null,
            status: "completed",
            occurredAt,
          },
        }
      : {}),
  };
  return { event, native, sequence };
};
const insertPhysicalRelease = Effect.fn("PersistentTasks.insertPhysicalRelease")(function* (
  release: ReturnType<typeof physicalReleaseFixture>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_claim_releases(event_id,sequence,work_claim_id,run_id,effect_id,terminal_event_id,terminal_sequence,event_json,native_json)
    SELECT json_extract(value,'$.id'),?,json_extract(value,'$.claim.binding.workClaimId'),json_extract(value,'$.claim.binding.runId'),json_extract(value,'$.claim.binding.effectId'),
      json_extract(value,'$.releaseEvidence.terminalRunEvent.eventId'),json_extract(value,'$.releaseEvidence.terminalRunEvent.sequence'),value,? FROM(SELECT ? value)`,
    [release.sequence, JSON.stringify(release.native), JSON.stringify(release.event)],
  );
});
const closePhysicalClaim = Effect.fn("PersistentTasks.closePhysicalClaim")(function* (
  release: ReturnType<typeof physicalReleaseFixture>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE work_task_executable_claims SET release_event_id=json_extract(${JSON.stringify(release.event.id)},'$') WHERE work_claim_id=json_extract(${JSON.stringify(release.event.claim.binding.workClaimId)},'$')`;
});
const rejectPhysicalRelease = Effect.fn("PersistentTasks.rejectPhysicalRelease")(function* (
  release: ReturnType<typeof physicalReleaseFixture>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`BEGIN`;
  yield* closePhysicalClaim(release);
  const result = yield* Effect.exit(insertPhysicalRelease(release));
  yield* sql`ROLLBACK`;
  assert.isTrue(Exit.isFailure(result));
});
const physicalUncertainty = (
  claim: ReturnType<typeof physicalClaimFixture>,
  id: string,
  sequence: number,
) =>
  decodePhysical({
    context: { eventId: id, sequence, commandId: id, occurredAt: claim.occurredAt },
    event: {
      id,
      type: "task-group.work-uncertain",
      commandId: id,
      occurredAt: claim.occurredAt,
      environmentId: claim.environmentId,
      attribution: { type: "server" },
      claim: retainedPhysicalClaim(claim),
      sourceEventId: claim.id,
      sourceSequence: claim.claim.acceptedSequence,
      reason: "Launch response unknown",
    },
  }).event;
const insertPhysicalUncertainty = Effect.fn("PersistentTasks.insertPhysicalUncertainty")(function* (
  event: unknown,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_claim_uncertainties(event_id,sequence,work_claim_id,event_json) SELECT json_extract(value,'$.id'),?,json_extract(value,'$.claim.binding.workClaimId'),value FROM(SELECT ? value)`,
    [sequence, JSON.stringify(event)],
  );
});
it.effect("keeps physical task custody occupied until a real release closes it", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const claim = physicalClaimFixture(owner);
    yield* insertPhysicalClaim(claim);
    const rows = yield* sql<{
      event_json: string;
    }>`SELECT event_json FROM work_task_executable_claims WHERE release_event_id IS NULL`;
    assert.deepStrictEqual(
      rows.map((row) => JSON.parse(row.event_json)),
      [claim],
    );
    yield* rejectOwnershipWrite(insertPhysicalClaim(physicalClaimFixture(owner, "second", 3001)));
    yield* insertPhysicalUncertainty(physicalUncertainty(claim, "uncertain", 3001), 3001);
    yield* rejectOwnershipWrite(insertPhysicalClaim(physicalClaimFixture(owner, "third", 3002)));
    const release = physicalReleaseFixture(claim);
    for (const write of [insertPhysicalRelease(release), closePhysicalClaim(release)]) {
      yield* sql`BEGIN`;
      yield* write;
      yield* rejectOwnershipWrite(sql`COMMIT`);
      yield* sql`ROLLBACK`;
      assert.lengthOf(
        yield* sql`SELECT * FROM work_task_executable_claims WHERE release_event_id IS NULL`,
        1,
      );
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* insertPhysicalRelease(release);
        yield* closePhysicalClaim(release);
      }),
    );
    yield* rejectOwnershipWrite(
      insertPhysicalUncertainty(physicalUncertainty(claim, "too-late", 4001), 4001),
    );
    yield* rejectOwnershipWrite(insertPhysicalClaim(physicalClaimFixture(owner, "overlap", 3500)));
    const next = physicalClaimFixture(owner, "next", 4500);
    yield* insertPhysicalClaim(next);
    const settled = physicalReleaseFixture(next, "next", 5000, true);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(settled);
        yield* insertPhysicalRelease(settled);
      }),
    );
    const terminal = ownershipTermination(owner, "wrong-before", 6000);
    yield* rejectOwnershipWrite(insertOwnershipObservation(terminal.event, 6000));
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect("rejects malformed physical evidence and immutable identity replacements", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const claim = physicalClaimFixture(owner);
    for (const [path, value] of [
      ["$.claim.binding.version", 3],
      ["$.claim.binding.providerInstanceId", "a/b"],
      ["$.claim.binding.runId", "missing"],
      ["$.claim.binding.task.taskId", "other"],
      ["$.claim.binding.manager.threadId", "wrong"],
      ["$.claim.binding.membershipGeneration", 2],
      ["$.claim.binding.taskRevisions.acceptedDefinitionRevision", 4],
      ["$.claim.binding.controlRevision", true],
      ["$.claim.binding.effectId", "\t"],
      ["$.claim.acceptedSequence", 1000],
      ["$.claim.sourceSequence", 3001],
      ["$.control.dispatchHold", "paused"],
      ["$.control.highestManagerGeneration", 2],
      ["$.membership.currentMembership.joinedMembershipRevision", 2],
      ["$.claim.launchOrigin.startCommandId", "other"],
      ["$.attribution.type", "manager"],
      ["$.occurredAt", "bad"],
      ["$.previousActiveClaimId", "old"],
      ["$.claim.extra", 1],
      ["$.claim.prerequisites", null],
    ] as const) {
      const rows = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(claim)},${path},json(${JSON.stringify(value)})) value`;
      yield* rejectOwnershipWrite(insertPhysicalClaim(JSON.parse(rows[0]!.value)));
    }
    yield* rejectOwnershipWrite(
      insertPhysicalClaim({ ...claim, id: "1", claim: { ...claim.claim, acceptedEventId: 1 } }),
    );
    yield* insertPhysicalClaim(claim);
    const release = physicalReleaseFixture(claim);
    for (const [path, value] of [
      ["$.run.runId", "wrong"],
      ["$.run.threadId", "other"],
      ["$.run.sequence", 3997],
      ["$.run.status", "running"],
      ["$.run.occurredAt", "bad"],
      ["$.run.eventId", null],
      ["$.extra", 1],
    ] as const) {
      const rows = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(release.native)},${path},json(${JSON.stringify(value)})) value`;
      yield* rejectPhysicalRelease({ ...release, native: JSON.parse(rows[0]!.value) });
    }
    const settled = physicalReleaseFixture(claim, "settled", 4000, true);
    for (const [path, value] of [
      ["$.attempt.runId", "wrong"],
      ["$.attempt.attemptId", "wrong"],
      ["$.attempt.providerThreadId", "wrong"],
      ["$.attempt.providerTurnId", "wrong"],
      ["$.attempt.status", "running"],
      ["$.attempt", null],
    ] as const) {
      const rows = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(settled.native)},${path},json(${JSON.stringify(value)})) value`;
      yield* rejectPhysicalRelease({ ...settled, native: JSON.parse(rows[0]!.value) });
    }
    for (const witness of ["attempt", "interrupt"] as const) {
      const event = JSON.parse(JSON.stringify(settled.event));
      const native = JSON.parse(JSON.stringify(settled.native));
      if (witness === "attempt") {
        event.releaseEvidence.attempt.terminalAttemptEvent.sequence = 2500;
        native.attempt.sequence = 2500;
      } else {
        const ref = {
          type: "turn-item.updated",
          eventId: "interrupt",
          sequence: 2501,
          turnItemId: "interrupt-item",
        };
        event.releaseEvidence.interruptResult = ref;
        native.interrupt = {
          ...ref,
          runId: claim.claim.binding.runId,
          threadId: claim.claim.binding.manager.threadId,
          itemType: "run_interrupt_result",
          occurredAt: claim.occurredAt,
        };
      }
      assert.throws(() =>
        decodePhysical({
          event,
          context: {
            eventId: event.id,
            sequence: settled.sequence,
            commandId: event.commandId,
            occurredAt: event.occurredAt,
          },
        }),
      );
      yield* rejectPhysicalRelease({ ...settled, event, native });
    }
    yield* sql`BEGIN`;
    yield* closePhysicalClaim(settled);
    yield* insertPhysicalRelease({
      ...settled,
      native: { ...settled.native, run: { ...settled.native.run, status: "rolled_back" } },
    });
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    yield* sql`ROLLBACK`;
    for (const recursive of ["OFF", "ON"]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      for (const operation of ["INSERT OR REPLACE", "INSERT OR IGNORE"]) {
        yield* rejectOwnershipWrite(
          sql.unsafe(
            `${operation} INTO work_task_executable_claims SELECT * FROM work_task_executable_claims`,
          ),
        );
      }
      yield* rejectOwnershipWrite(
        sql`UPDATE OR REPLACE work_task_executable_claims SET work_claim_id='other'`,
      );
      yield* rejectOwnershipWrite(sql`DELETE FROM work_task_executable_claims`);
      yield* rejectOwnershipWrite(
        sql`INSERT OR REPLACE INTO work_task_executable_claims(rowid) VALUES(1)`,
      );
      yield* rejectOwnershipWrite(
        sql`UPDATE work_task_accepted_definitions SET definition_json='{"changed":true}' WHERE task_id='a' AND definition_revision=2`,
      );
      yield* rejectOwnershipWrite(
        sql`INSERT OR REPLACE INTO work_task_claim_references SELECT * FROM work_task_claim_references`,
      );
      yield* rejectOwnershipWrite(
        sql`INSERT INTO work_task_claim_references VALUES('claim-one','claim-one',NULL,NULL,'forged',0,'fake',1,NULL,NULL)`,
      );
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(release);
        yield* insertPhysicalRelease(release);
      }),
    );
    yield* rejectOwnershipWrite(sql`UPDATE work_task_executable_claims SET release_event_id=NULL`);
    yield* rejectOwnershipWrite(
      sql`INSERT OR REPLACE INTO work_task_claim_releases SELECT * FROM work_task_claim_releases`,
    );
    yield* rejectOwnershipWrite(
      sql`UPDATE OR REPLACE work_task_claim_releases SET event_id='other'`,
    );
    yield* rejectOwnershipWrite(sql`DELETE FROM work_task_claim_releases`);
    yield* rejectOwnershipWrite(
      insertPhysicalClaim(physicalClaimFixture(owner, "reverse", 2999), "missing"),
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves physical custody through upgrade retirement rollback and indexed history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 98 });
      const { bound, join } = yield* seedAcceptedOwnership();
      const owner = acceptedOwnershipFixture();
      yield* insertAcceptedRun(owner);
      const names = (yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).map(
        (r) => r.name,
      );
      const capture = Effect.fn("PersistentTasks.capturePhysicalPredecessor")(function* () {
        return Object.fromEntries(
          yield* Effect.forEach(names, (name) =>
            sql.unsafe(`SELECT * FROM "${name}"`).pipe(Effect.map((rows) => [name, rows] as const)),
          ),
        );
      });
      const before = yield* capture();
      yield* sql`CREATE INDEX work_task_claim_run_idx ON work_task_accepted_runs(run_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* capture(), before);
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='work_task_executable_claims'`,
        [],
      );
      yield* sql`DROP INDEX work_task_claim_run_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [99, "WorkTaskManagerClaims"],
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      for (const name of names.filter((n) => n !== "effect_sql_migrations"))
        assert.deepStrictEqual(after[name], before[name]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_executable_claims`, []);
      const claim = physicalClaimFixture(owner);
      yield* insertPhysicalClaim(claim);
      const retired = membershipTransition(
        { ...join.event.snapshot, taskRevisions: owner.ownership.run.taskRevisions },
        [bound.control],
        null,
        "retire-physical-member",
      );
      yield* sql.withTransaction(applyMembership(retired, 3100));
      const previousControl = retired.companions[0]!.control;
      const stopped = decodeGroupControlEvent({
        ...groupContext("retire-physical-manager"),
        occurredAt: retired.event.occurredAt,
        type: "task-group.manager-changed",
        previousControl,
        control: {
          ...previousControl,
          controlRevision: previousControl.controlRevision + 1,
          currentManager: null,
        },
      });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* writeGroupControl(stopped, 3102);
          yield* retireGroupManager(stopped, 3102);
        }),
      );
      yield* rejectOwnershipWrite(
        insertPhysicalClaim(physicalClaimFixture(owner, "retired-overlap", 3200)),
      );
      assert.lengthOf(
        yield* sql`SELECT * FROM work_task_executable_claims WHERE release_event_id IS NULL`,
        1,
      );
      for (const query of [
        "SELECT work_claim_id FROM work_task_executable_claims WHERE environment_id='env' AND group_id='group' AND release_event_id IS NULL",
        "SELECT work_claim_id FROM work_task_executable_claims WHERE task_id='a' AND release_event_id IS NULL",
        "SELECT work_claim_id FROM work_task_executable_claims WHERE manager_thread_id='manager' AND release_event_id IS NULL",
        "SELECT work_claim_id FROM work_task_executable_claims WHERE run_id='run-one' ORDER BY accepted_sequence DESC",
        "SELECT event_id FROM work_task_claim_uncertainties WHERE work_claim_id='physical-one' ORDER BY sequence DESC LIMIT 1",
        "SELECT p.value FROM work_task_executable_claims c,json_each(c.event_json,'$.claim.prerequisites') p WHERE c.work_claim_id='physical-one'",
        "SELECT * FROM work_task_graph_edges WHERE environment_id='env' AND edge_id='edge'",
      ]) {
        const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
          .map((r) => r.detail)
          .join("\n");
        assert.include(plan, "SEARCH");
        assert.notInclude(plan, "TEMP B-TREE");
        assert.notMatch(plan, /SCAN (c|work_task_)/);
      }
      for (const name of ["group", "task", "thread"]) {
        const text = (yield* sql<{
          sql: string;
        }>`SELECT sql FROM sqlite_master WHERE name=${`work_task_claim_${name}_order`}`)[0]!.sql;
        const predicate = text.slice(text.indexOf("SELECT CASE"), text.indexOf("THEN RAISE"));
        const query = predicate
          .slice("SELECT CASE WHEN ".length)
          .replaceAll("NEW.environment_id", "'env'")
          .replaceAll("NEW.group_id", "'group'")
          .replaceAll("NEW.task_id", "'a'")
          .replaceAll("NEW.manager_thread_id", "'manager'")
          .replaceAll("NEW.accepted_sequence", "5000");
        const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN SELECT ${query}`))
          .map((r) => r.detail)
          .join("\n");
        assert.include(plan, `work_task_claim_${name}_history_idx`);
        assert.notInclude(plan, "TEMP B-TREE");
      }
      const release = physicalReleaseFixture(claim);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertPhysicalRelease(release);
          yield* closePhysicalClaim(release);
        }),
      );
      const retained = yield* sql`SELECT * FROM work_task_executable_claims`;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_executable_claims`, retained);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

it.effect("binds bounded physical prerequisites and retains the original automatic charge", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const automatic = automaticOwnershipFixture("physical", 10, 1);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* insertAcceptedRun(automatic.event);
        yield* insertAcceptedDebit(
          automatic.event.ownership.run.runId,
          automatic.debit.event,
          automatic.debit.context.sequence,
        );
      }),
    );
    const base = physicalClaimFixture(automatic.event, "automatic");
    const prerequisites = [];
    for (let index = 1; index <= 64; index++) {
      const task = `blocker-${index}`;
      yield* sql`INSERT INTO work_task_projections VALUES(${task},NULL,1,1,1,1,'{}','raw','raw')`;
      yield* insertGraphJson(
        JSON.stringify(decodeGraphEdge(graphEdge(`edge-${index}`, index, "a", task))),
        1500 + index,
      );
      prerequisites.push({
        edgeId: `edge-${index}`,
        blocker: { environmentId: "env", taskId: task },
        completionEventId: `completed-${index}`,
        completionSequence: 2000 + index,
      });
    }
    const candidate = {
      ...base,
      claim: {
        ...base.claim,
        binding: { ...base.claim.binding, graphRevision: 64 },
        prerequisites,
      },
    };
    const full = decodePhysical({
      event: candidate,
      context: {
        eventId: candidate.id,
        sequence: candidate.claim.acceptedSequence,
        commandId: candidate.commandId,
        occurredAt: candidate.occurredAt,
      },
    }).event;
    if (full.type !== "task-group.work-accepted")
      throw new Error("Expected accepted physical work");
    yield* rejectOwnershipWrite(
      insertPhysicalClaim({
        ...full,
        claim: { ...full.claim, launchOrigin: { ...full.claim.launchOrigin, allowanceUnit: true } },
      }),
    );
    for (const witnesses of [
      [...prerequisites, prerequisites[0]!],
      [prerequisites[0]!, prerequisites[0]!],
      [{ ...prerequisites[0]!, edgeId: "missing" }],
      [{ ...prerequisites[0]!, blocker: { environmentId: "other", taskId: "blocker-1" } }],
    ])
      yield* rejectOwnershipWrite(
        insertPhysicalClaim({ ...full, claim: { ...full.claim, prerequisites: witnesses } }),
      );
    yield* insertPhysicalClaim(full);
    assert.strictEqual(
      (yield* sql<{
        n: number;
      }>`SELECT count(*) n FROM work_task_claim_references WHERE reference_role='prerequisite'`)[0]!
        .n,
      64,
    );
    assert.lengthOf(yield* sql`SELECT * FROM work_task_accepted_run_debits`, 1);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    yield* sql`INSERT INTO work_task_allowances VALUES('a',1,1)`;
    yield* sql`INSERT INTO work_task_manager_bindings VALUES('a',1,'legacy-manager','raw',NULL)`;
    yield* rejectOwnershipWrite(
      sql`INSERT INTO work_task_wake_reservations VALUES('legacy','a',1,'legacy-command','legacy-message','legacy-run',1,1,0,'raw',NULL)`,
    );
    const release = physicalReleaseFixture(full, "automatic", 4000);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(release);
        yield* insertPhysicalRelease(release);
      }),
    );
    const removed = decodeGraphProvenance(graphProvenance("remove-edge-1"));
    yield* sql`UPDATE work_task_graph_edges SET removed_graph_revision=65,removed_event_id=${removed.eventId},removed_sequence=4500,removal_provenance_json=${JSON.stringify(removed)} WHERE edge_id='edge-1'`;
    const later = physicalClaimFixture(automatic.event, "later", 5000);
    assert.deepStrictEqual(
      (yield* sql`SELECT configured_cap,consumed FROM work_task_allowances`).map((row) => ({
        ...row,
      })),
      [{ configured_cap: 1, consumed: 1 }],
    );
    yield* rejectOwnershipWrite(
      insertPhysicalClaim({
        ...later,
        claim: {
          ...later.claim,
          binding: { ...later.claim.binding, graphRevision: 65 },
          prerequisites: [prerequisites[0]!],
        },
      }),
    );
    for (const reverse of [false, true]) {
      yield* sql`BEGIN`;
      const future = acceptedOwnershipFixture("future", 300);
      const observed = {
        ...physicalUncertainty(later, "future-observed", 5001),
        sourceEventId: "native-run-future",
        sourceSequence: 501,
      };
      yield* insertPhysicalClaim(later);
      if (reverse) yield* insertAcceptedRun(future);
      yield* insertPhysicalUncertainty(observed, 5001);
      if (!reverse) yield* insertAcceptedRun(future);
      const bad = acceptedOwnershipFixture("wrong-future", 400);
      yield* rejectOwnershipWrite(
        insertAcceptedRun({
          ...bad,
          ownership: {
            ...bad.ownership,
            nativeAcceptance: {
              ...bad.ownership.nativeAcceptance,
              runEvent: {
                ...bad.ownership.nativeAcceptance.runEvent,
                eventId: "native-run-future",
                sequence: 502,
              },
            },
          },
        }),
      );
      yield* rejectOwnershipWrite(
        insertPhysicalUncertainty({ ...observed, id: "bad-source", sourceSequence: 502 }, 5002),
      );
      yield* sql`ROLLBACK`;
    }
    const terminalOwner = acceptedOwnershipFixture("already-terminal", 50);
    yield* insertAcceptedRun(terminalOwner);
    const terminal = ownershipTermination(terminalOwner, "already-ended", 6000);
    yield* insertOwnershipObservation(terminal.event, 6000);
    yield* rejectOwnershipWrite(
      insertPhysicalClaim(physicalClaimFixture(terminalOwner, "too-late", 7000)),
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect("serializes physical slot contenders across SQLite connections", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-physical-" });
    const filename = path.join(directory, "state.sqlite");
    const owner = acceptedOwnershipFixture();
    const claim = physicalClaimFixture(owner);
    yield* Effect.gen(function* () {
      yield* runMigrations();
      yield* seedAcceptedOwnership();
      yield* insertAcceptedRun(owner);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    using first = new NodeSqlite.DatabaseSync(filename);
    using second = new NodeSqlite.DatabaseSync(filename);
    first.exec("PRAGMA foreign_keys=ON");
    second.exec("PRAGMA foreign_keys=ON");
    const statement = `INSERT INTO work_task_executable_claims (${Object.keys(physicalClaimPaths).join(",")},event_json)
      SELECT ${Object.values(physicalClaimPaths)
        .map((path) => `json_extract(value,'$.${path}')`)
        .join(",")},value FROM(SELECT ? value)`;
    first.exec("BEGIN IMMEDIATE");
    first.prepare(statement).run(JSON.stringify(claim));
    assert.throws(
      () =>
        second
          .prepare(statement)
          .run(JSON.stringify(physicalClaimFixture(owner, "contender", 3001))),
      /locked/,
    );
    first.exec("COMMIT");
    assert.throws(
      () =>
        second
          .prepare(statement)
          .run(JSON.stringify(physicalClaimFixture(owner, "contender", 3001))),
      /occupied|conflict|UNIQUE/,
    );
    assert.strictEqual(
      second
        .prepare(
          "SELECT count(*) n FROM work_task_executable_claims WHERE release_event_id IS NULL",
        )
        .get()!.n,
      1,
    );
    assert.deepStrictEqual(second.prepare("PRAGMA foreign_key_check").all(), []);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const afterOwnershipTermination = (
  owner: ReturnType<typeof acceptedOwnershipFixture>,
  release: ReturnType<typeof physicalReleaseFixture>,
  id = "after-terminated",
  sequence = 5000,
) => {
  const { context } = ownershipObservation(owner, id, sequence);
  const event = decodeAcceptedOwnership({
    context,
    event: {
      id,
      type: "task.run-ownership-terminated",
      environmentId: owner.environmentId,
      commandId: context.commandId,
      occurredAt: context.occurredAt,
      attribution: { type: "server" },
      ownership: retainedOwnership(owner),
      observedSequence: release.sequence,
      terminationEvidence: {
        kind: "after_executable_claim",
        terminalRunEvent: release.event.releaseEvidence.terminalRunEvent,
        terminalStatus:
          release.event.releaseEvidence.type === "not_started" ? "cancelled" : "completed",
        workClaimId: release.event.claim.binding.workClaimId,
        workReleaseEvent: {
          type: "task-group.work-released",
          eventId: release.event.id,
          sequence: release.sequence,
        },
      },
    },
  }).event;
  if (event.type !== "task.run-ownership-terminated") throw new Error("Expected termination");
  return { event, sequence };
};
const insertAfterOwnership = Effect.fn("PersistentTasks.insertAfterOwnership")(function* (
  value: ReturnType<typeof afterOwnershipTermination>,
  companion: string | null = value.event.id,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_run_ownership_observations (${Object.keys(observationPaths).join(",")},sequence,event_json,after_release_companion_id)
    SELECT ${Object.values(observationPaths)
      .map((p) => `json_extract(value,'$.${p}')`)
      .join(",")},?,value,? FROM(SELECT ? value)`,
    [value.sequence, companion, JSON.stringify(value.event)],
  );
});
const insertOwnershipBridge = Effect.fn("PersistentTasks.insertOwnershipBridge")(function* (
  value: ReturnType<typeof afterOwnershipTermination>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_run_ownership_release_bridges
    SELECT json_extract(value,'$.id'),json_extract(value,'$.terminationEvidence.workClaimId'),json_extract(value,'$.terminationEvidence.workReleaseEvent.eventId'),
      json_extract(value,'$.ownership.run.runId'),json_extract(value,'$.terminationEvidence.terminalRunEvent.eventId'),json_extract(value,'$.terminationEvidence.terminalRunEvent.sequence') FROM(SELECT ? value)`,
    [JSON.stringify(value.event)],
  );
});
it.effect(
  "terminates accepted ownership through its latest resolved physical claim in both orders",
  () =>
    Effect.gen(function* () {
      for (const bridgeFirst of [false, true]) {
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* runMigrations();
          yield* seedAcceptedOwnership();
          const owner = acceptedOwnershipFixture();
          yield* insertAcceptedRun(owner);
          const claim = physicalClaimFixture(owner);
          yield* insertPhysicalClaim(claim);
          const release = physicalReleaseFixture(claim);
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* closePhysicalClaim(release);
              yield* insertPhysicalRelease(release);
            }),
          );
          const termination = afterOwnershipTermination(owner, release);
          yield* sql`BEGIN`;
          if (bridgeFirst) yield* insertOwnershipBridge(termination);
          yield* insertAfterOwnership(termination);
          if (!bridgeFirst) yield* insertOwnershipBridge(termination);
          yield* sql`COMMIT`;
          assert.strictEqual(
            (yield* sql`SELECT * FROM work_task_run_ownership_release_bridges`).length,
            1,
          );
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
          yield* rejectOwnershipWrite(
            insertPhysicalClaim(physicalClaimFixture(owner, "after-terminal", 6000)),
          );
        }).pipe(Effect.provide(db));
      }
    }),
);

it.effect("rejects stale custody and incomplete ownership release companions", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const first = physicalClaimFixture(owner);
    yield* insertPhysicalClaim(first);
    const firstRelease = physicalReleaseFixture(first);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(firstRelease);
        yield* insertPhysicalRelease(firstRelease);
      }),
    );
    const stale = afterOwnershipTermination(owner, firstRelease, "stale-owner", 7000);
    const second = physicalClaimFixture(owner, "second", 4500);
    yield* insertPhysicalClaim(second);
    const release = physicalReleaseFixture(second, "second", 6000, true);
    const latest = afterOwnershipTermination(owner, release, "latest-owner", 7000);
    for (const value of [stale, latest]) {
      yield* sql`BEGIN`;
      yield* rejectOwnershipWrite(insertAfterOwnership(value));
      yield* rejectOwnershipWrite(insertOwnershipBridge(value));
      yield* sql`ROLLBACK`;
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(release);
        yield* insertPhysicalRelease(release);
      }),
    );
    yield* sql`BEGIN`;
    yield* rejectOwnershipWrite(insertAfterOwnership(stale));
    yield* rejectOwnershipWrite(insertOwnershipBridge(stale));
    yield* sql`ROLLBACK`;
    for (const write of [insertAfterOwnership(latest), insertOwnershipBridge(latest)]) {
      yield* sql`BEGIN`;
      yield* write;
      yield* rejectOwnershipWrite(sql`COMMIT`);
      yield* sql`ROLLBACK`;
      assert.strictEqual(
        (yield* sql`SELECT * FROM work_task_run_ownership_release_bridges`).length,
        0,
      );
    }
    const uncertain = ownershipObservation(owner, latest.event.id, latest.sequence);
    for (const bridgeFirst of [false, true]) {
      yield* sql`BEGIN`;
      if (bridgeFirst) {
        yield* insertOwnershipBridge(latest);
        yield* rejectOwnershipWrite(
          insertOwnershipObservation(uncertain.event, uncertain.context.sequence),
        );
      } else {
        yield* insertOwnershipObservation(uncertain.event, uncertain.context.sequence);
        yield* rejectOwnershipWrite(insertOwnershipBridge(latest));
      }
      yield* sql`ROLLBACK`;
    }
    for (const [path, value] of [
      ["$.terminationEvidence.workClaimId", first.claim.binding.workClaimId],
      ["$.terminationEvidence.workReleaseEvent.eventId", firstRelease.event.id],
      ["$.terminationEvidence.workReleaseEvent.sequence", 6001],
      ["$.terminationEvidence.terminalStatus", "failed"],
      ["$.terminationEvidence.terminalRunEvent.eventId", "unrelated"],
      ["$.terminationEvidence.workReleaseEvent.sequence", true],
      ["$.terminationEvidence.extra", 1],
    ] as const) {
      const rows = yield* sql<{
        value: string;
      }>`SELECT json_set(${JSON.stringify(latest.event)},${path},json(${JSON.stringify(value)})) value`;
      yield* sql`BEGIN`;
      yield* insertOwnershipBridge(latest);
      yield* rejectOwnershipWrite(
        insertAfterOwnership({ ...latest, event: JSON.parse(rows[0]!.value) }),
      );
      yield* sql`ROLLBACK`;
    }
    const bridgeValues = [
      latest.event.id,
      second.claim.binding.workClaimId,
      release.event.id,
      second.claim.binding.runId,
      release.event.releaseEvidence.terminalRunEvent.eventId,
      release.event.releaseEvidence.terminalRunEvent.sequence,
    ];
    for (let index = 0; index < bridgeValues.length; index++) {
      const values = bridgeValues.map((v, i) => (i === index ? null : v));
      yield* sql`BEGIN`;
      yield* rejectOwnershipWrite(
        sql.unsafe(
          "INSERT INTO work_task_run_ownership_release_bridges VALUES(?,?,?,?,?,?)",
          values,
        ),
      );
      yield* sql`ROLLBACK`;
    }
    yield* sql`BEGIN`;
    yield* insertOwnershipBridge(latest);
    yield* rejectOwnershipWrite(insertAfterOwnership(latest, null));
    yield* insertAfterOwnership(latest);
    yield* sql`COMMIT`;
    for (const recursive of ["OFF", "ON"]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive}`);
      yield* rejectOwnershipWrite(
        sql`INSERT OR REPLACE INTO work_task_run_ownership_release_bridges SELECT * FROM work_task_run_ownership_release_bridges`,
      );
      yield* rejectOwnershipWrite(
        sql`UPDATE OR REPLACE work_task_run_ownership_release_bridges SET observation_event_id='replacement'`,
      );
      yield* rejectOwnershipWrite(sql`DELETE FROM work_task_run_ownership_release_bridges`);
      yield* rejectOwnershipWrite(
        sql`INSERT INTO work_task_run_ownership_release_bridges(rowid) VALUES(1)`,
      );
      yield* rejectOwnershipWrite(
        sql`UPDATE work_task_run_ownership_observations SET after_release_companion_id=NULL`,
      );
    }
    const references = yield* sql<{
      event_id: string;
    }>`SELECT event_id FROM work_task_run_observation_references WHERE reference_role='work_release'`;
    assert.deepStrictEqual(
      references.map((r) => r.event_id),
      [release.event.id],
    );
    for (const query of [
      "SELECT work_claim_id FROM work_task_executable_claims WHERE run_id='run-owner' ORDER BY accepted_sequence DESC LIMIT 1",
      "SELECT 1 FROM work_task_executable_claims u LEFT JOIN work_task_claim_releases r ON r.event_id=u.release_event_id AND r.work_claim_id=u.work_claim_id AND r.run_id=u.run_id AND r.effect_id=u.effect_id WHERE u.run_id='run-owner' AND r.event_id IS NULL",
      "SELECT * FROM work_task_run_ownership_release_bridges WHERE work_release_event_id='release-second'",
    ]) {
      const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      assert.isTrue(plan.some((r) => r.detail.includes("SEARCH")));
      assert.isFalse(plan.some((r) => /SCAN|TEMP B-TREE/.test(r.detail)));
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "upgrades ownership bridges without rewriting prior evidence and rolls back failed DDL",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 99 });
      yield* seedAcceptedOwnership();
      const owner = acceptedOwnershipFixture();
      yield* insertAcceptedRun(owner);
      const uncertain = ownershipObservation(owner);
      yield* insertOwnershipObservation(uncertain.event, uncertain.context.sequence);
      const claim = physicalClaimFixture(owner);
      yield* insertPhysicalClaim(claim);
      const release = physicalReleaseFixture(claim);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* closePhysicalClaim(release);
          yield* insertPhysicalRelease(release);
        }),
      );
      const tables = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`;
      const projections = yield* Effect.forEach(tables, ({ name }) =>
        Effect.gen(function* () {
          const columns = yield* sql<{
            name: string;
          }>`SELECT name FROM pragma_table_info(${name}) ORDER BY cid`;
          return {
            name,
            query: `SELECT ${columns.map((c) => `"${c.name}"`).join(",")} FROM "${name}"`,
          };
        }),
      );
      const capture = Effect.fn("PersistentTasks.captureOwnershipBridgePredecessor")(function* () {
        return Object.fromEntries(
          yield* Effect.forEach(projections, ({ name, query }) =>
            sql.unsafe(query).pipe(Effect.map((rows) => [name, rows] as const)),
          ),
        );
      });
      const before = yield* capture();
      yield* sql`CREATE INDEX work_task_ownership_bridge_release_idx ON work_task_claim_releases(event_id)`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* capture(), before);
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name='work_task_run_ownership_release_bridges'`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM pragma_table_info('work_task_run_ownership_observations') WHERE name='after_release_companion_id'`,
        [],
      );
      yield* sql`DROP INDEX work_task_ownership_bridge_release_idx`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [100, "WorkTaskOwnershipReleaseBridge"],
        [101, "WorkTaskReadyState"],
      ]);
      const after = yield* capture();
      for (const { name } of tables.filter((r) => r.name !== "effect_sql_migrations"))
        assert.deepStrictEqual(after[name], before[name]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_run_ownership_release_bridges`, []);
      assert.isNull(
        (yield* sql<{
          after_release_companion_id: string | null;
        }>`SELECT after_release_companion_id FROM work_task_run_ownership_observations`)[0]!
          .after_release_companion_id,
      );
      const termination = afterOwnershipTermination(owner, release);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertAfterOwnership(termination);
          yield* insertOwnershipBridge(termination);
        }),
      );
      const completed = yield* sql`SELECT * FROM work_task_run_ownership_release_bridges`;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM work_task_run_ownership_release_bridges`,
        completed,
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

const readyFixture = (sequence: number, previous: number | null = null) => {
  const owner = acceptedOwnershipFixture();
  const occurredAt = owner.occurredAt;
  const event = decodePhysical({
    context: { eventId: `ready-${sequence}`, sequence, commandId: `ready-${sequence}`, occurredAt },
    event: {
      id: `ready-${sequence}`,
      type: "task.ready-sequence-updated",
      environmentId: "env",
      commandId: `ready-${sequence}`,
      occurredAt,
      attribution: { type: "server" },
      task: { environmentId: "env", taskId: "a" },
      group: { environmentId: "env", groupId: "group" },
      membershipGeneration: 1,
      sourceEventId: owner.id,
      sourceSequence: owner.ownership.recorded.sequence,
      basis: {
        taskRevisions: owner.ownership.run.taskRevisions,
        graphRevision: 0,
        controlRevision: 1,
        membershipRevision: 1,
        managerGeneration: 1,
      },
      kind: "qualified",
      previousOldestReadySequence: previous,
      oldestReadySequence: previous ?? sequence,
    },
  }).event;
  if (event.type !== "task.ready-sequence-updated") throw new Error("Expected ready event");
  return event;
};
const insertReady = Effect.fn("PersistentTasks.insertReady")(function* (
  event: unknown,
  sequence: number,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_ready_events(event_id,sequence,event_json)
    SELECT json_extract(value,'$.id'),?,value FROM(SELECT ? value)`,
    [sequence, JSON.stringify(event)],
  );
});
it.effect("retains earned ready age and derives exact latest task membership state", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    yield* insertAcceptedRun(acceptedOwnershipFixture());
    yield* insertReady(readyFixture(2000), 2000);
    yield* insertReady(readyFixture(2001, 2000), 2001);
    const rows =
      yield* sql`SELECT oldest_ready_sequence,latest_sequence FROM work_task_ready_state`;
    assert.deepStrictEqual(
      rows.map((r) => ({ ...r })),
      [{ oldest_ready_sequence: 2000, latest_sequence: 2001 }],
    );
    yield* rejectOwnershipWrite(insertReady(readyFixture(2002), 2002));
  }).pipe(Effect.provide(db)),
);

const decodeReady = (event: unknown, sequence: number) => {
  const value = event as ReturnType<typeof readyFixture>;
  return decodePhysical({
    event,
    context: {
      eventId: value.id,
      sequence,
      commandId: value.commandId,
      occurredAt: value.occurredAt,
    },
  }).event;
};
const settledReady = (release: ReturnType<typeof physicalReleaseFixture>, sequence: number) =>
  decodeReady(
    {
      ...readyFixture(sequence),
      kind: "settled",
      claim: release.event.claim,
      releaseEventId: release.event.id,
      releaseSequence: release.sequence,
      sourceEventId: release.event.id,
      sourceSequence: release.sequence,
    },
    sequence,
  );
it.effect("consumes and settles ready epochs without erasing pending custody or rebilling", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    yield* insertReady(readyFixture(2000), 2000);
    for (let i = 1; i <= 32; i++) yield* insertReady(readyFixture(2000 + i, 2000), 2000 + i);
    const refresh = readyFixture(2100, 2000);
    yield* insertReady(
      decodeReady(
        {
          ...refresh,
          basis: {
            ...refresh.basis,
            taskRevisions: { ...refresh.basis.taskRevisions, definitionRevision: 4 },
          },
        },
        2100,
      ),
      2100,
    );
    assert.strictEqual(
      (yield* sql`SELECT oldest_ready_sequence FROM work_task_ready_state`)[0]!
        .oldest_ready_sequence,
      2000,
    );
    const originalClaim = physicalClaimFixture(owner);
    const claim = {
      ...originalClaim,
      id: "1" as typeof originalClaim.id,
      claim: { ...originalClaim.claim, acceptedEventId: "1" as typeof originalClaim.id },
    };
    yield* insertPhysicalClaim(claim);
    const accepted = decodeReady(
      {
        ...readyFixture(3001, 2000),
        kind: "accepted",
        oldestReadySequence: null,
        claim: retainedPhysicalClaim(claim),
        sourceEventId: claim.id,
        sourceSequence: 3000,
      },
      3001,
    );
    for (const badClaim of [
      { ...retainedPhysicalClaim(claim), acceptedSequence: "3000" },
      { ...retainedPhysicalClaim(claim), acceptedEventId: 1 },
      { ...retainedPhysicalClaim(claim), acceptedEventId: true },
      { ...retainedPhysicalClaim(claim), acceptedAt: 123 },
    ])
      yield* rejectOwnershipWrite(insertReady({ ...accepted, claim: badClaim }, 3001));
    yield* insertReady(accepted, 3001);
    yield* rejectOwnershipWrite(insertReady(readyFixture(3100), 3100));
    yield* insertReady(
      decodeReady(
        {
          ...readyFixture(3101),
          kind: "disqualified",
          oldestReadySequence: null,
          reason: "task_pause",
        },
        3101,
      ),
      3101,
    );
    yield* rejectOwnershipWrite(insertReady(readyFixture(3102), 3102));
    const release = physicalReleaseFixture(claim);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(release);
        yield* insertPhysicalRelease(release);
      }),
    );
    for (const badClaim of [
      { ...release.event.claim, acceptedSequence: "3000" },
      { ...release.event.claim, acceptedEventId: 1 },
      { ...release.event.claim, acceptedEventId: true },
    ])
      yield* rejectOwnershipWrite(
        insertReady({ ...settledReady(release, 4001), claim: badClaim }, 4001),
      );
    yield* insertReady(settledReady(release, 4001), 4001);
    const expected = yield* sql`SELECT * FROM work_task_ready_state`;
    assert.strictEqual(expected[0]!.oldest_ready_sequence, 4001);
    assert.isNull(expected[0]!.pending_claim_id);
    assert.strictEqual(expected[0]!.last_served_claim_id, claim.claim.binding.workClaimId);
    for (const query of [
      "UPDATE work_task_ready_state SET oldest_ready_sequence=2000",
      "UPDATE work_task_ready_state SET pending_claim_id='physical-one'",
      "UPDATE work_task_ready_state SET last_served_claim_id=NULL",
      "UPDATE work_task_ready_state SET latest_event_id='ready-2000',latest_sequence=2000",
      "INSERT OR REPLACE INTO work_task_ready_state SELECT * FROM work_task_ready_state",
      "DELETE FROM work_task_ready_state",
      "INSERT OR REPLACE INTO work_task_ready_events(event_id,sequence,event_json) SELECT event_id,sequence,event_json FROM work_task_ready_events",
      "DELETE FROM work_task_ready_references",
    ])
      yield* rejectOwnershipWrite(sql.unsafe(query));
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_ready_state`, expected);
    yield* insertReady(
      decodeReady(
        {
          ...readyFixture(4002, 4001),
          kind: "disqualified",
          oldestReadySequence: null,
          reason: "done",
        },
        4002,
      ),
      4002,
    );
    yield* rejectOwnershipWrite(insertReady(settledReady(release, 4003), 4003));
    yield* insertReady(readyFixture(4004), 4004);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_allowances`, []);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    for (const query of [
      "SELECT * FROM work_task_ready_state WHERE environment_id='env' AND group_id='group' AND oldest_ready_sequence IS NOT NULL ORDER BY oldest_ready_sequence,task_id,membership_generation LIMIT 50",
      "SELECT event_id FROM work_task_ready_events WHERE task_id='a' AND membership_generation=1 AND kind='accepted' ORDER BY sequence DESC LIMIT 1",
      "SELECT event_id FROM work_task_ready_events WHERE release_event_id='release-one' AND kind='settled'",
      "SELECT claim_id FROM work_task_ready_events WHERE task_id='a' AND membership_generation=1 AND kind IN ('accepted','settled') ORDER BY sequence DESC LIMIT 1",
      "SELECT event_id FROM work_task_ready_events WHERE task_id='a' AND sequence>3000",
    ]) {
      const plan = (yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
        .map((r) => r.detail)
        .join(" ");
      assert.include(plan, "SEARCH");
      assert.notInclude(plan, "TEMP B-TREE");
      assert.notInclude(plan, "SCAN work_task_ready");
    }
    for (const name of ["work_task_ready_physical_reverse", "work_task_ready_state_update"]) {
      const trigger = (yield* sql<{
        sql: string;
      }>`SELECT sql FROM sqlite_master WHERE name=${name}`)[0]!;
      const predicate = trigger.sql.split("SELECT CASE WHEN ")[1]!.split(" THEN RAISE")[0]!;
      const from = name.includes("physical")
        ? "work_task_executable_claims"
        : "work_task_ready_state";
      const plan = (yield* sql.unsafe<{ detail: string }>(
        `EXPLAIN QUERY PLAN SELECT ${predicate} FROM ${from} NEW${name.includes("state") ? " CROSS JOIN work_task_ready_state OLD" : ""}`,
      ))
        .map((r) => r.detail)
        .join(" ");
      assert.include(plan, "SEARCH");
      assert.notMatch(plan, /SCAN [ers]\b/, plan);
      assert.include(
        plan,
        name.includes("state") ? "work_task_ready_accepted_idx" : "work_task_ready_task_idx",
      );
    }
  }).pipe(Effect.provide(db)),
);
it.effect("bootstraps only real settled claims and rejects reverse ready chronology", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const first = physicalClaimFixture(owner);
    yield* insertPhysicalClaim(first);
    const release = physicalReleaseFixture(first);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* insertPhysicalRelease(release);
        yield* closePhysicalClaim(release);
      }),
    );
    yield* insertReady(settledReady(release, 5000), 5000);
    const later = physicalClaimFixture(owner, "later", 4500);
    yield* rejectOwnershipWrite(insertPhysicalClaim(later));
    const valid = physicalClaimFixture(owner, "future", 6000);
    yield* insertPhysicalClaim(valid);
    yield* rejectOwnershipWrite(insertReady(readyFixture(6001, 5000), 6001));
    const before = yield* sql`SELECT * FROM work_task_ready_state`;
    for (const patch of [
      {
        sequence: 5001,
        event: {
          ...readyFixture(5001, 5000),
          sourceEventId: release.event.id,
          sourceSequence: 4001,
        },
      },
      { sequence: 5001, event: { ...readyFixture(5001, 5000), id: release.event.id } },
      {
        sequence: 5001,
        event: {
          ...readyFixture(5001, 5000),
          basis: { ...readyFixture(5001).basis, managerGeneration: null },
        },
      },
      { sequence: 5001, event: { ...readyFixture(5001, 5000), membershipGeneration: 2 } },
      { sequence: 5001, event: { ...readyFixture(5001, 5000), occurredAt: "invalid" } },
      { sequence: 5001, event: { ...readyFixture(5001, 5000), sourceSequence: true } },
    ])
      yield* rejectOwnershipWrite(insertReady(patch.event, patch.sequence));
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_ready_state`, before);
    const unusual = decodeReady(
      {
        ...readyFixture(5200, 5000),
        id: "ready-\0😀\ud800",
        commandId: "ready-command-\0😀\ud800",
        unknownField: "stripped",
      },
      5200,
    );
    yield* insertReady(unusual, 5200);
    assert.strictEqual(
      (yield* sql`SELECT hex(latest_event_id) encoded FROM work_task_ready_state`)[0]!.encoded,
      (yield* sql`SELECT hex(json_extract(${JSON.stringify(unusual)},'$.id')) encoded`)[0]!.encoded,
    );
    for (const changed of [
      { sourceEventId: "\t" },
      { id: "x".repeat(2001) },
      { unknownField: "not decoded" },
      { oldestReadySequence: null },
      { previousOldestReadySequence: 4999 },
    ])
      yield* rejectOwnershipWrite(insertReady({ ...readyFixture(5300, 5000), ...changed }, 5300));
    yield* rejectOwnershipWrite(
      sql`UPDATE work_task_accepted_definitions SET definition_json='{"changed":true}' WHERE task_id='a' AND definition_revision=2`,
    );
    yield* rejectOwnershipWrite(
      sql`INSERT OR REPLACE INTO work_task_accepted_definitions SELECT * FROM work_task_accepted_definitions WHERE task_id='a' AND definition_revision=2`,
    );
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves ready age across shared holds and keeps moved membership pending evidence local",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const seed = yield* seedAcceptedOwnership();
      const owner = acceptedOwnershipFixture();
      yield* insertAcceptedRun(owner);
      yield* insertReady(readyFixture(2000), 2000);
      const claim = physicalClaimFixture(owner);
      yield* insertPhysicalClaim(claim);
      yield* insertReady(
        decodeReady(
          {
            ...readyFixture(3001, 2000),
            kind: "accepted",
            oldestReadySequence: null,
            claim: retainedPhysicalClaim(claim),
            sourceEventId: claim.id,
            sourceSequence: 3000,
          },
          3001,
        ),
        3001,
      );
      const removed = membershipTransition(
        { ...seed.join.event.snapshot, taskRevisions: owner.ownership.run.taskRevisions },
        [seed.bound.control],
        null,
        "ready-remove",
      );
      yield* sql.withTransaction(applyMembership(removed, 3100));
      const rejoined = membershipTransition(
        removed.event.snapshot,
        [removed.companions[0]!.control],
        "group",
        "ready-rejoin",
      );
      yield* sql.withTransaction(applyMembership(rejoined, 3200));
      const fresh = (sequence: number, previous: number | null = null) =>
        decodeReady(
          {
            ...readyFixture(sequence, previous),
            membershipGeneration: 2,
            basis: { ...readyFixture(sequence).basis, membershipRevision: 3 },
          },
          sequence,
        );
      yield* rejectOwnershipWrite(insertReady(fresh(3300), 3300));
      const release = physicalReleaseFixture(claim);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* closePhysicalClaim(release);
          yield* insertPhysicalRelease(release);
        }),
      );
      yield* insertReady(fresh(4100), 4100);
      const retained =
        yield* sql`SELECT membership_generation,oldest_ready_sequence,pending_claim_id FROM work_task_ready_state ORDER BY membership_generation`;
      assert.deepStrictEqual(
        retained.map((r) => ({ ...r })),
        [
          {
            membership_generation: 1,
            oldest_ready_sequence: null,
            pending_claim_id: claim.claim.binding.workClaimId,
          },
          { membership_generation: 2, oldest_ready_sequence: 4100, pending_claim_id: null },
        ],
      );
      yield* sql`INSERT INTO work_task_projections VALUES ('b',NULL,3,1,2,3,'{}','raw','raw')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('b',1,1,'{}','{}','b-definition-one','raw')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('b',2,1,'{}','{}','b-definition-two','raw')`;
      const bJoin = membershipTransition(
        {
          ...membershipJoin().event.previousSnapshot,
          taskRevisions: owner.ownership.run.taskRevisions,
          task: {
            environmentId: owner.environmentId,
            taskId: "b" as typeof owner.ownership.run.task.taskId,
          },
        },
        [rejoined.companions[0]!.control],
        "group",
        "b-join",
      );
      yield* sql.withTransaction(applyMembership(bJoin, 4200));
      const bBase = acceptedOwnershipFixture("b", 3300);
      const bOwner = decodeAcceptedOwnership({
        event: {
          ...bBase,
          ownership: {
            ...bBase.ownership,
            sourceEventId: "b-start",
            sourceSequence: 150,
            observedWorkSequence: 150,
            origin: {
              type: "initial_start",
              startCommandId: "b-start-command",
              startEventId: "b-start",
              startSequence: 150,
            },
            nativeAcceptance: {
              ...bBase.ownership.nativeAcceptance,
              runEvent: { ...bBase.ownership.nativeAcceptance.runEvent, sequence: 4251 },
              messageEvent: { ...bBase.ownership.nativeAcceptance.messageEvent, sequence: 4252 },
              receipt: { ...bBase.ownership.nativeAcceptance.receipt, resultSequence: 4252 },
            },
            run: { ...bBase.ownership.run, task: bJoin.event.snapshot.task, membershipRevision: 4 },
          },
        },
        context: bBase.ownership.recorded,
      }).event;
      if (bOwner.type !== "task.run-ownership-accepted") throw new Error("Expected B ownership");
      yield* insertAcceptedRun(bOwner);
      const bClaim = physicalClaimFixture(bOwner, "b", 4400, bJoin.event.snapshot);
      yield* insertPhysicalClaim(bClaim);
      yield* insertReady(fresh(4500, 4100), 4500);
      const bReady = decodeReady(
        {
          ...readyFixture(4350),
          id: "b-ready",
          commandId: "b-ready",
          task: bJoin.event.snapshot.task,
          sourceEventId: bOwner.id,
          sourceSequence: 4300,
          basis: { ...readyFixture(4350).basis, membershipRevision: 4 },
        },
        4350,
      );
      yield* insertReady(bReady, 4350);
      const selected = yield* sql<{
        task_id: string;
      }>`SELECT s.task_id FROM work_task_ready_state s JOIN work_task_group_memberships m ON m.task_id=s.task_id AND m.membership_generation=s.membership_generation
      WHERE s.environment_id='env' AND s.group_id='group' AND s.oldest_ready_sequence IS NOT NULL AND m.retired_event_id IS NULL ORDER BY s.oldest_ready_sequence,s.task_id,s.membership_generation`;
      assert.deepStrictEqual(
        selected.map((r) => r.task_id),
        ["a", "b"],
      );
      yield* insertCoordination(dirtyEvent("ready-dirty", "a", bOwner.id, 4300), 4600);
      const scan = {
        ...scanEvent("ready-scan"),
        scan: {
          kind: "group_members",
          sourceEventId: bOwner.id,
          sourceSequence: 4300,
          group: { environmentId: "env", groupId: "group" },
          capturedRevision: 4,
        },
        observedRevision: 4,
        visited: [{ task: { environmentId: "env", taskId: "a" }, membershipGeneration: 2 }],
        position: { taskId: "a" },
      };
      yield* insertCoordination(scan, 4603);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(insertCoordination({ ...scan, id: "stale-ready-scan" }, 4604)),
        ),
      );
      const dirty = dirtyEvent("ready-dirty", "a", bOwner.id, 4300);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(insertCoordination({ ...dirty, id: "duplicate-dirty" }, 4601)),
        ),
      );
      assert.strictEqual(
        (yield* sql`SELECT oldest_ready_sequence FROM work_task_ready_state WHERE task_id='a' AND membership_generation=2`)[0]!
          .oldest_ready_sequence,
        4100,
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            insertCoordination(
              { ...dirtyEvent("bad-alias"), sourceEventId: "b-ready", sourceSequence: 4351 },
              4602,
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);
it.effect("upgrades ready storage preserving every predecessor row and rolls back DDL", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 100 });
    yield* seedAcceptedOwnership();
    const owner = acceptedOwnershipFixture();
    yield* insertAcceptedRun(owner);
    const claim = physicalClaimFixture(owner);
    yield* insertPhysicalClaim(claim);
    const release = physicalReleaseFixture(claim);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* closePhysicalClaim(release);
        yield* insertPhysicalRelease(release);
      }),
    );
    const tables = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`;
    const capture = Effect.fn("PersistentTasks.captureReadyPredecessor")(function* () {
      return Object.fromEntries(
        yield* Effect.forEach(tables, ({ name }) =>
          sql.unsafe(`SELECT * FROM "${name}"`).pipe(Effect.map((rows) => [name, rows] as const)),
        ),
      );
    });
    const before = yield* capture();
    yield* sql`CREATE INDEX work_task_ready_latest_idx ON work_task_projections(task_id)`;
    assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
    assert.deepStrictEqual(yield* capture(), before);
    assert.deepStrictEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name='work_task_ready_events'`,
      [],
    );
    yield* sql`DROP INDEX work_task_ready_latest_idx`;
    assert.deepStrictEqual(yield* runMigrations(), [[101, "WorkTaskReadyState"]]);
    const after = yield* capture();
    for (const { name } of tables.filter((t) => t.name !== "effect_sql_migrations"))
      assert.deepStrictEqual(after[name], before[name]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_ready_events`, []);
    yield* insertReady(settledReady(release, 5000), 5000);
    const state = yield* sql`SELECT * FROM work_task_ready_state`;
    assert.deepStrictEqual(yield* runMigrations(), []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM work_task_ready_state`, state);
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);
