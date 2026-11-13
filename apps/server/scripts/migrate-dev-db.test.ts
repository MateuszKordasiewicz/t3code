import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../src/persistence/Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrateDevDb } from "./migrate-dev-db.ts";

const withDatabase = <A, E>(
  databasePath: string,
  effect: Effect.Effect<A, E, SqlClient.SqlClient>,
) => effect.pipe(Effect.provide(NodeSqliteClient.layer({ filename: databasePath })));

const createFixtureSource = Effect.fn("createMigrateDevDbFixtureSource")(function* (
  baseDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const stateDir = path.join(baseDir, "userdata");
  const databasePath = path.join(stateDir, "statev2.sqlite");
  yield* fs.makeDirectory(stateDir, { recursive: true });
  yield* withDatabase(
    databasePath,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();

      yield* sql`INSERT INTO projection_projects
        (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
        VALUES
        ('project-kept', 'Kept', '/tmp/kept', '[]', '2026-08-01', '2026-08-01', NULL),
        ('project-deleted', 'Deleted', '/tmp/deleted', '[]', '2026-08-01', '2026-08-02', '2026-08-02')`;

      const forkPayload =
        '{"lineage":{"parentThreadId":"stopped-thread","relationshipToParent":"fork","rootThreadId":"stopped-thread"}}';
      const subagentPayload =
        '{"lineage":{"parentThreadId":"subagent-parent","relationshipToParent":"subagent","rootThreadId":"subagent-parent"},"forkedFrom":{"type":"node","nodeId":"node-1"}}';
      const watchedPayload = JSON.stringify({
        pullRequests: [
          {
            host: "github.com",
            repository: "owner/repo",
            number: 7,
            url: "https://github.com/owner/repo/pull/7",
            source: "agent",
            linkedAt: "2026-08-01T00:00:00.000Z",
            snapshot: null,
            stack: null,
            watch: {
              startedAt: "2026-08-01T00:00:00.000Z",
              headSha: null,
              failedChecks: [],
              passed: false,
              passedChecks: [],
              remarksThrough: "2026-08-01T00:00:00.000Z",
              remarkIds: [],
              conflicting: false,
              wakes: 0,
            },
          },
        ],
      });
      const threads = [
        ["stopped-thread", "project-kept", "completed", watchedPayload, "2026-08-01"],
        ["fork-thread", "project-kept", "completed", forkPayload, "2026-08-02"],
        ["running-thread", "project-kept", "running", "{}", "2026-08-05"],
        ["settled-thread", "project-kept", "completed", '{"settledAt":"2026-08-01"}', "2026-08-05"],
        [
          "archived-thread",
          "project-kept",
          "completed",
          '{"archivedAt":"2026-08-01"}',
          "2026-08-05",
        ],
        [
          "limit-thread",
          "project-kept",
          "completed",
          '{"limitRecovery":{"autoResume":true}}',
          "2026-08-05",
        ],
        ["subagent-parent", "project-kept", "completed", "{}", "2026-08-05"],
        ["subagent-child", "project-kept", "completed", subagentPayload, "2026-08-05"],
        ["deleted-project-thread", "project-deleted", "completed", "{}", "2026-08-05"],
      ] as const;
      for (const [threadId, projectId, runStatus, payload, updatedAt] of threads) {
        yield* sql`INSERT INTO orchestration_v2_projection_threads
          (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode, created_at, updated_at, payload_json)
          VALUES (${threadId}, ${projectId}, ${threadId}, 'codex', 'full-access', 'default', '2026-08-01', ${updatedAt}, ${payload})`;
        yield* sql`INSERT INTO orchestration_v2_projection_runs
          (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
          VALUES (${`run-${threadId}`}, ${threadId}, 1, 'codex', ${runStatus}, '2026-08-01', '{}')`;
        yield* sql`INSERT INTO orchestration_events
          (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
          VALUES (${`event-${threadId}`}, 'thread', ${threadId}, 0, 'thread.created', '2026-08-01', 'user', '{}', '{}')`;
      }
      yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions
        (provider_session_id, thread_id, provider, status, updated_at, payload_json)
        VALUES ('session-shared', 'running-thread', 'codex', 'stopped', '2026-08-01', '{}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings
        (provider_session_id, thread_id)
        VALUES ('session-shared', 'running-thread'), ('session-shared', 'stopped-thread')`;
      yield* sql`INSERT INTO orchestration_v2_projection_context_transfers
        (context_transfer_id, source_thread_id, target_thread_id, type, status, updated_at, payload_json)
        VALUES ('transfer-1', 'archived-thread', 'stopped-thread', 'provider_handoff', 'completed', '2026-08-01', '{}')`;
      yield* sql`INSERT INTO scheduled_tasks
        (task_id, title, prompt, enabled, schedule_json, project_id, workspace_strategy_json,
          model_selection_json, runtime_mode, interaction_mode, created_by, creation_source,
          created_at, updated_at, last_run_status, run_count)
        VALUES ('task-1', 'Nightly', 'Run it', 1, '{}', 'project-kept', '{}', '{}',
          'full-access', 'default', 'user', 'user', '2026-08-01', '2026-08-01', 'never', 0)`;
      yield* sql`INSERT INTO scheduled_task_webhook_replay_keys
        (task_id, replay_key, delivery_id, expires_at)
        VALUES ('task-1', 'replay-1', 'delivery-1', 1900000000000)`;
      yield* sql`INSERT INTO scheduled_task_webhook_pending
        (delivery_id, task_id, coalesce_key, due_at, task_json, request_json, delivery_ids_json)
        VALUES ('delivery-1', 'task-1', 'key-1', 1900000000000, '{}', '{}', '[]')`;
      yield* sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at)
        VALUES ('session-1', 'user', '[]', 'pairing', '2026-08-01', '2027-08-01')`;
      yield* sql`INSERT INTO peer_environment_links
        (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
        VALUES ('environment-box', 'peer-link-box', 'Box', '["https://box.example"]', 'auto',
          '2026-08-01', '2027-08-01')`;
    }),
  );
  return databasePath;
});

it.layer(NodeServices.layer)("migrate-dev-db", (it) => {
  it.effect("keeps stopped and settled thread families and clears pending work", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-src-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-dest-" });
      const source = yield* createFixtureSource(sourceDir);

      const result = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
        { sharedHome: sourceDir },
      );

      assert.equal(result.databasePath, path.join(destDir, "userdata", "statev2.sqlite"));
      const kept = yield* withDatabase(
        result.databasePath,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const threads = yield* sql<{ thread_id: string }>`
            SELECT thread_id FROM orchestration_v2_projection_threads ORDER BY thread_id`;
          const events = yield* sql<{ stream_id: string }>`
            SELECT stream_id FROM orchestration_events ORDER BY stream_id`;
          const sessions = yield* sql<{ provider_session_id: string }>`
            SELECT provider_session_id FROM orchestration_v2_projection_provider_sessions`;
          const [pullRequest] = yield* sql<{ url: string | null; watch: string | null }>`
            SELECT
              json_extract(payload_json, '$.pullRequests[0].url') AS url,
              json_extract(payload_json, '$.pullRequests[0].watch') AS watch
            FROM orchestration_v2_projection_threads WHERE thread_id = 'stopped-thread'`;
          const [leftovers] = yield* sql<{
            auth: number;
            tasks: number;
            webhooks: number;
            transfers: number;
            links: number;
          }>`
            SELECT
              (SELECT COUNT(*) FROM auth_sessions) AS auth,
              (SELECT COUNT(*) FROM scheduled_tasks) AS tasks,
              (SELECT COUNT(*) FROM scheduled_task_webhook_replay_keys) +
                (SELECT COUNT(*) FROM scheduled_task_webhook_pending) AS webhooks,
              (SELECT COUNT(*) FROM orchestration_v2_projection_context_transfers) AS transfers,
              (SELECT COUNT(*) FROM peer_environment_links) AS links`;
          return { threads, events, sessions, pullRequest, leftovers };
        }),
      );
      assert.deepStrictEqual(
        kept.threads.map((row) => row.thread_id),
        ["fork-thread", "settled-thread", "stopped-thread"],
      );
      assert.deepStrictEqual(
        kept.events.map((row) => row.stream_id),
        ["fork-thread", "settled-thread", "stopped-thread"],
      );
      assert.deepStrictEqual(
        kept.sessions.map((row) => row.provider_session_id),
        ["session-shared"],
      );
      assert.deepStrictEqual(
        kept.pullRequest,
        { url: "https://github.com/owner/repo/pull/7", watch: null },
        "copied pull request links keep their history but stop being watched",
      );
      assert.deepStrictEqual(kept.leftovers, {
        auth: 0,
        tasks: 0,
        webhooks: 0,
        transfers: 0,
        links: 0,
      });
    }),
  );

  it.effect("never copies an archived thread, and new events append after the source's", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-slice-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-slice-dest-" });
      const source = yield* createFixtureSource(sourceDir);
      const [sourceSequence] = yield* withDatabase(
        source,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ seq: number }>`
            SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'`;
        }),
      );

      const result = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 100 },
        { sharedHome: sourceDir },
      );

      const copied = yield* withDatabase(
        result.databasePath,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const [archivedRows] = yield* sql<{ count: number }>`
            SELECT
              (SELECT COUNT(*) FROM orchestration_v2_projection_runs WHERE thread_id = 'archived-thread')
              + (SELECT COUNT(*) FROM orchestration_events WHERE stream_id = 'archived-thread')
              AS count`;
          const [sequence] = yield* sql<{ seq: number }>`
            SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'`;
          return { archivedRows: archivedRows?.count, sequence: sequence?.seq };
        }),
      );
      assert.equal(copied.archivedRows, 0);
      assert.equal(copied.sequence, sourceSequence?.seq);
    }),
  );

  it.effect("skips a thread family larger than the per-project limit", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-family-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-family-dest-" });
      const source = yield* createFixtureSource(sourceDir);

      // stopped-thread and its fork are 2 threads; settled-thread is 1.
      const result = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 1 },
        { sharedHome: sourceDir },
      );
      const threads = yield* withDatabase(
        result.databasePath,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ thread_id: string }>`
            SELECT thread_id FROM orchestration_v2_projection_threads`;
        }),
      );
      assert.deepStrictEqual(
        threads.map((row) => row.thread_id),
        ["settled-thread"],
      );
    }),
  );

  it.effect("upgrades a source from before the V2 thread tables", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-v1-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-v1-dest-" });
      const stateDir = path.join(sourceDir, "userdata");
      const source = path.join(stateDir, "statev2.sqlite");
      yield* fs.makeDirectory(stateDir, { recursive: true });
      yield* withDatabase(source, runMigrations({ toMigrationInclusive: 54 }));

      const result = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 10 },
        { sharedHome: sourceDir },
      );
      assert.include(result.executedMigrations, "55_OrchestrationV2");
    }),
  );

  it.effect("seeds from a source that recorded another migration below V2", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-local-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-local-dest-" });
      const source = yield* createFixtureSource(sourceDir);
      yield* withDatabase(
        source,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE effect_sql_migrations SET name = 'SiteLocalMigration'
            WHERE migration_id = 51`;
        }),
      );

      const result = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
        { sharedHome: sourceDir },
      );
      const threads = yield* withDatabase(
        result.databasePath,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql<{ thread_id: string }>`
            SELECT thread_id FROM orchestration_v2_projection_threads ORDER BY thread_id`;
        }),
      );
      assert.deepStrictEqual(
        threads.map((row) => row.thread_id),
        ["fork-thread", "settled-thread", "stopped-thread"],
      );
    }),
  );

  it.effect("refuses a source whose V2 migration history clashes with this checkout", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-clash-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-clash-dest-" });
      const source = yield* createFixtureSource(sourceDir);
      yield* withDatabase(
        source,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE effect_sql_migrations SET name = 'OtherBuildMigration'
            WHERE migration_id = 60`;
        }),
      );

      const destination = yield* createFixtureSource(destDir);
      const sourceBefore = yield* fs.readFile(source);
      const destinationBefore = yield* fs.readFile(destination);

      const error = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
        { sharedHome: sourceDir },
      ).pipe(Effect.flip);
      assert.equal(error._tag, "MigrateDevDbPhaseError");
      if (error._tag === "MigrateDevDbPhaseError") {
        assert.equal(error.phase, "migrate");
        assert.include(String(error.cause), "60_ThreadSnapshotWindowIndexes");
      }
      assert.deepStrictEqual(yield* fs.readFile(source), sourceBefore);
      assert.deepStrictEqual(yield* fs.readFile(destination), destinationBefore);
    }),
  );

  it.effect("refuses while a dev server holds the destination", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-busy-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-busy-dest-" });
      const source = yield* createFixtureSource(sourceDir);
      const stateDir = path.join(destDir, "userdata");
      yield* fs.makeDirectory(stateDir, { recursive: true });
      yield* fs.writeFileString(
        path.join(stateDir, "server-runtime.json"),
        `{"version":1,"pid":${process.pid}}`,
      );

      const error = yield* runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 10 },
        { sharedHome: sourceDir },
      ).pipe(Effect.flip);
      assert.equal(error._tag, "MigrateDevDbServerRunningError");
      if (error._tag === "MigrateDevDbServerRunningError") {
        assert.equal(error.pid, process.pid);
      }
    }),
  );

  it.effect("refuses a source that resolves to a destination path", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sharedDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-overlap-" });
      const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-overlap-dest-" });
      const leftoverSnapshot = path.join(destDir, "userdata", "statev2.sqlite.migrate-dev-db-tmp");
      yield* fs.makeDirectory(path.dirname(leftoverSnapshot), { recursive: true });
      yield* fs.writeFileString(leftoverSnapshot, "not a real db");

      const error = yield* runMigrateDevDb(
        { baseDir: destDir, source: leftoverSnapshot, projects: 5, threadsPerProject: 10 },
        { sharedHome: sharedDir },
      ).pipe(Effect.flip);
      assert.equal(error._tag, "MigrateDevDbSourceIsDestinationError");
      assert.equal(yield* fs.exists(leftoverSnapshot), true);
    }),
  );

  it.effect("refuses to rebuild the shared home", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "migrate-dev-db-shared-" });
      const source = yield* createFixtureSource(sourceDir);

      const error = yield* runMigrateDevDb(
        { baseDir: sourceDir, source, projects: 5, threadsPerProject: 10 },
        { sharedHome: sourceDir },
      ).pipe(Effect.flip);
      assert.equal(error._tag, "MigrateDevDbSharedHomeError");
    }),
  );
});

const operationalWorkflowTables = [
  "workflow_runs",
  "workflow_planning_sessions",
  "workflow_operations",
  "workflow_planning_passes",
  "workflow_attempts",
  "workflow_role_results",
  "workflow_acceptances",
  "workflow_command_receipts",
  "workflow_native_launch_intents",
  "workflow_native_intent_receipts",
  "workflow_native_reservation_material",
  "workflow_native_command_proofs",
  "workflow_planning_observations",
  "workflow_planning_dispositions",
  "workflow_planning_settlements",
  "orchestration_v2_submission_transports",
  "orchestration_v2_submission_attempts",
  "orchestration_v2_preparation_jobs",
  "test_preparation_job_child",
] as const;
const planContentTables = [
  "workflow_plan_identities",
  "workflow_revision_identities",
  "development_plans",
  "development_plan_source_packets",
  "development_plan_revisions",
  "development_plan_threads",
  "development_plan_commands",
] as const;

const seedOperationalWorkflow = Effect.fn("test.seedOperationalWorkflowForDevCopy")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const hash = "a".repeat(64);
  const at = "2026-11-13T00:00:00.000Z";
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('seed-plan')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('seed-plan','seed-revision',${hash},${hash})`;
  yield* sql`INSERT INTO development_plans VALUES ('seed-plan','environment-kept','project-kept','Preserved plan',NULL,1,${at})`;
  yield* sql`INSERT INTO development_plan_source_packets VALUES ('seed-plan',${hash},'{"source":"retained"}')`;
  yield* sql`INSERT INTO development_plan_revisions VALUES ('seed-plan','seed-revision',${hash},${hash},'{"revision":"retained"}',${at})`;
  yield* sql`INSERT INTO development_plan_threads VALUES ('seed-plan','stopped-thread')`;
  yield* sql`INSERT INTO development_plan_commands VALUES ('seed-plan-command','seed-plan',${hash},'{"plan":"retained"}')`;
  yield* sql`INSERT INTO workflow_runs VALUES ('seed-run','seed-plan','seed-revision',${hash},${hash},${hash},'active',0,0,'{}')`;
  yield* sql`INSERT INTO workflow_planning_sessions VALUES ('seed-session','seed-plan',NULL,'active',0,0,'{}')`;
  yield* sql`INSERT INTO workflow_operations(id,workflow_run_id,kind,fingerprint,status,generation,record_json) VALUES ('seed-work-operation','seed-run','stage_launch',${hash},'claimed',0,'{}')`;
  yield* sql`INSERT INTO workflow_operations(id,planning_session_id,kind,fingerprint,status,generation,record_json) VALUES ('seed-plan-operation','seed-session','planning_pass',${hash},'claimed',0,'{}')`;
  yield* sql`INSERT INTO workflow_attempts VALUES ('seed-work-attempt','seed-run','seed-work-operation','seed-item',0)`;
  yield* sql`INSERT INTO workflow_role_results VALUES ('seed-result','seed-run','seed-work-operation','{}',0)`;
  yield* sql`INSERT INTO workflow_acceptances VALUES ('seed-run','seed-acceptance','{}',0)`;
  yield* sql`INSERT INTO workflow_command_receipts VALUES ('seed-work-command','seed-run',NULL,${hash},'seed-work-receipt','seed-work-operation','{}')`;
  yield* sql`INSERT INTO workflow_planning_passes VALUES ('seed-pass','seed-session','seed-plan','seed-revision',${hash},${hash},'seed-plan-operation','stopped-thread','run-stopped-thread','{}')`;
  yield* sql`INSERT INTO workflow_planning_observations VALUES ('seed-pass','seed-plan-operation','stopped-thread','run-stopped-thread','seed-terminal',${hash},'{}')`;
  yield* sql`INSERT INTO workflow_planning_dispositions VALUES ('seed-pass','seed-finding','seed-pass','{}')`;
  yield* sql`INSERT INTO workflow_planning_settlements VALUES ('seed-plan-operation',${hash},'{}')`;
  yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,workflow_run_id,operation_id,attempt_id,item_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state) VALUES ('seed-work-intent','seed-run','seed-work-operation','seed-work-attempt','seed-item',1,0,'writer','environment-kept','project-kept','seed-launch','stopped-thread','seed-message',${hash},'{}',0,'reserved')`;
  yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,planning_session_id,operation_id,pass_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state) VALUES ('seed-plan-intent','seed-session','seed-plan-operation','seed-pass',0,0,'planner','environment-kept','project-kept','seed-plan-launch','stopped-thread','seed-plan-message',${hash},'{}',0,'reserved')`;
  for (const intent of ["seed-work-intent", "seed-plan-intent"]) {
    yield* sql`INSERT INTO workflow_native_intent_receipts VALUES (${intent + "-command"},${intent + "-receipt"},${intent},${hash},'{}')`;
    yield* sql`INSERT INTO workflow_native_reservation_material VALUES (${intent},'{}',${hash})`;
  }
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES ('seed-native-command','thread','stopped-thread',${at},1,'accepted')`;
  yield* sql`INSERT INTO workflow_native_command_proofs VALUES ('seed-native-command','seed-work-intent','claim','{}',${hash})`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json) VALUES ('seed-native-attempt','stopped-thread','run-stopped-thread',1,'seed-root','codex','seed-provider-thread','completed','{}')`;
  for (const id of ["unbound", "settled"]) {
    const unit = `t3code-codex-seed-${id}.scope`;
    yield* sql`INSERT INTO orchestration_v2_submission_transports(format_version,transport_id,environment_id,project_id,opening_thread_id,driver,provider_instance_id,provider_session_id,host_boot_id,scope_namespace,proposed_unit,creation_context_json,creation_context_hash,creation_input_hash,created_at) VALUES (1,${id},'environment-kept','project-kept','stopped-thread','codex','seed-instance','session-shared','seed-host-boot','app-t3code-agents-main.slice',${unit},'{}',${hash},${hash},${at})`;
    if (id === "settled") {
      const binding = {
        unit,
        invocationId: "1".repeat(32),
        controlGroup: `/fixture/${unit}`,
        pid: 101,
        pidStartTicks: "9007199254740993",
      };
      yield* sql`UPDATE orchestration_v2_submission_transports SET binding_json=${JSON.stringify(binding)},binding_hash=${hash},bound_at=${at} WHERE transport_id=${id}`;
      yield* sql`INSERT INTO orchestration_v2_submission_attempts(format_version,attempt_id,transport_id,environment_id,project_id,thread_id,run_id,run_ordinal,root_node_id,provider_turn_ordinal,driver,provider_instance_id,provider_session_id,provider_thread_id,native_thread_id,method,selection_json,selection_hash,input_hash,frame_hash,request_id_json,created_at) VALUES (1,'seed-native-attempt',${id},'environment-kept','project-kept','stopped-thread','run-stopped-thread',1,'seed-root',1,'codex','seed-instance','session-shared','seed-provider-thread','seed-native-thread','turn/start','{}',${hash},${hash},${hash},'1',${at})`;
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome='unknown',local_reason='sink',local_observed_at=${at} WHERE attempt_id='seed-native-attempt'`;
      yield* sql`UPDATE orchestration_v2_submission_transports SET settlement_json=${JSON.stringify({ binding, observation: "removed" })},settlement_hash=${hash},settled_at=${at} WHERE transport_id=${id}`;
    }
  }
  for (const [id, kind, parent, state] of [
    ["prep-active", "launch", null, "active"],
    ["prep-quiescing", "setup", "prep-active", "quiescing"],
    ["prep-unknown", "title", null, "unknown"],
    ["prep-settled", "release", null, "settled"],
  ] as const) {
    yield* sql`INSERT INTO orchestration_v2_preparation_jobs(format_version,job_id,entry_key,kind,environment_id,project_id,thread_id,originating_launch_job_id,server_boot_id,request_identity_json,request_identity_hash,created_at) VALUES (1,${id},${id},${kind},'environment-kept','project-kept','stopped-thread',${parent},'seed-server-boot','{}',${hash},${at})`;
    if (state === "settled")
      yield* sql`UPDATE orchestration_v2_preparation_jobs SET state='settled',record_version=1,code_closed_at=${at},settlement_json=${JSON.stringify({ v: 1, jobId: id, entryKey: id, priorVersion: 0, kind: "code_only" })},settlement_hash=${hash},settled_at=${at} WHERE job_id=${id}`;
    else if (state !== "active")
      yield* sql`UPDATE orchestration_v2_preparation_jobs SET state=${state},record_version=1 WHERE job_id=${id}`;
  }
  yield* sql`CREATE TABLE test_preparation_job_child(id TEXT PRIMARY KEY,job_id TEXT NOT NULL REFERENCES orchestration_v2_preparation_jobs(job_id))`;
  yield* sql`INSERT INTO test_preparation_job_child VALUES ('copy-descendant','prep-active')`;
});

const readWorkflowCopyState = Effect.fn("test.readWorkflowCopyState")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const operational = yield* Effect.forEach(
    operationalWorkflowTables,
    (table) => sql`SELECT * FROM ${sql(table)}`,
  );
  const content = yield* Effect.forEach(
    planContentTables,
    (table) => sql`SELECT * FROM ${sql(table)}`,
  );
  const schema =
    yield* sql`SELECT type,name,sql FROM sqlite_master WHERE tbl_name IN ('orchestration_v2_submission_transports','orchestration_v2_submission_attempts','orchestration_v2_preparation_jobs','workflow_native_reservation_material','test_preparation_job_child') OR name LIKE 'submission_%' OR name LIKE 'orchestration_v2_submission_%_guard' ORDER BY name`;
  const history = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
  return { operational, content, schema, history };
});

it.effect("copies plan content without retaining operational workflow or physical custody", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-workflow-custody-src-" });
    const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-workflow-custody-dest-" });
    const source = yield* createFixtureSource(sourceDir);
    yield* withDatabase(source, seedOperationalWorkflow());
    const before = yield* withDatabase(source, readWorkflowCopyState());
    assert.isTrue(before.operational.every((rows) => rows.length > 0));
    assert.isTrue(before.content.every((rows) => rows.length > 0));
    const sourceBytes = yield* fs.readFile(source);
    const result = yield* runMigrateDevDb(
      { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
      { sharedHome: sourceDir },
    );
    assert.deepStrictEqual(yield* fs.readFile(source), sourceBytes);
    assert.deepStrictEqual(yield* withDatabase(source, readWorkflowCopyState()), before);
    const copy = yield* withDatabase(result.databasePath, readWorkflowCopyState());
    assert.isTrue(
      copy.operational.every((rows) => rows.length === 0),
      "dev copy must contain no operational workflow or process custody",
    );
    assert.deepStrictEqual(copy.content, before.content);
    assert.deepStrictEqual(copy.schema, before.schema);
    assert.deepStrictEqual(copy.history, before.history);
    const threads = yield* withDatabase(
      result.databasePath,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return yield* sql<{
          readonly thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads ORDER BY thread_id`;
      }),
    );
    assert.deepStrictEqual(
      threads.map((row) => row.thread_id),
      ["fork-thread", "settled-thread", "stopped-thread"],
    );
    assert.deepStrictEqual(result.executedMigrations, []);
    assert.strictEqual(result.databasePath, path.join(destDir, "userdata", "statev2.sqlite"));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("keeps a published plan's current revision when omitting its execution owners", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-published-plan-src-" });
    const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-published-plan-dest-" });
    const source = yield* createFixtureSource(sourceDir);
    yield* withDatabase(
      source,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* seedOperationalWorkflow();
        yield* sql`UPDATE development_plans SET current_revision_id='seed-revision' WHERE plan_id='seed-plan'`;
      }),
    );
    const before = yield* withDatabase(source, readWorkflowCopyState());
    const bytes = yield* fs.readFile(source);
    const result = yield* runMigrateDevDb(
      { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
      { sharedHome: sourceDir },
    );
    const copy = yield* withDatabase(result.databasePath, readWorkflowCopyState());
    assert.deepStrictEqual(copy.content, before.content);
    assert.isTrue(copy.operational.every((rows) => rows.length === 0));
    assert.deepStrictEqual(yield* fs.readFile(source), bytes);
    assert.deepStrictEqual(yield* withDatabase(source, readWorkflowCopyState()), before);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("rejects a dangling retained plan reference at the destination copy commit", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const sourceDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-dangling-plan-src-" });
    const destDir = yield* fs.makeTempDirectoryScoped({ prefix: "dev-dangling-plan-dest-" });
    const source = yield* createFixtureSource(sourceDir);
    yield* withDatabase(
      source,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql`INSERT INTO development_plan_commands(command_id,plan_id,fingerprint,result_json) VALUES ('dangling-command','missing-plan',${"a".repeat(64)},'{}')`;
      }),
    );
    const before = yield* fs.readFile(source);
    const exit = yield* Effect.exit(
      runMigrateDevDb(
        { baseDir: destDir, source, projects: 5, threadsPerProject: 2 },
        { sharedHome: sourceDir },
      ),
    );
    assert.isTrue(Exit.isFailure(exit));
    assert.include(String(exit), "FOREIGN KEY constraint failed");
    assert.deepStrictEqual(yield* fs.readFile(source), before);
    assert.isFalse(yield* fs.exists(path.join(destDir, "userdata", "statev2.sqlite")));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
