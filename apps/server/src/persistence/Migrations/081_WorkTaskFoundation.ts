import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_projections (
      task_id TEXT PRIMARY KEY NOT NULL,
      project_id TEXT,
      definition_revision INTEGER NOT NULL CHECK (definition_revision > 0),
      lifecycle_revision INTEGER NOT NULL CHECK (lifecycle_revision > 0),
      completion_policy_revision INTEGER NOT NULL CHECK (completion_policy_revision > 0),
      action_policy_revision INTEGER NOT NULL CHECK (action_policy_revision > 0),
      definition_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`CREATE INDEX work_task_projections_project_updated_idx ON work_task_projections(project_id, updated_at, task_id)`;
  yield* sql`
    CREATE TABLE work_task_accepted_definitions (
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      definition_revision INTEGER NOT NULL CHECK (definition_revision > 0),
      action_policy_revision INTEGER NOT NULL CHECK (action_policy_revision > 0),
      definition_json TEXT NOT NULL,
      attribution_json TEXT NOT NULL,
      accepted_event_id TEXT NOT NULL UNIQUE,
      accepted_at TEXT NOT NULL,
      PRIMARY KEY (task_id, definition_revision)
    )
  `;
  yield* sql`
    CREATE TABLE work_task_thread_links (
      link_event_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      thread_id TEXT NOT NULL,
      linked_at TEXT NOT NULL,
      linked_attribution_json TEXT NOT NULL,
      unlink_event_id TEXT UNIQUE,
      unlinked_at TEXT,
      unlinked_attribution_json TEXT,
      CHECK (
        (unlink_event_id IS NULL AND unlinked_at IS NULL AND unlinked_attribution_json IS NULL)
        OR (unlink_event_id IS NOT NULL AND unlinked_at IS NOT NULL AND unlinked_attribution_json IS NOT NULL)
      )
    )
  `;
  yield* sql`CREATE UNIQUE INDEX work_task_thread_links_active_idx ON work_task_thread_links(task_id, thread_id) WHERE unlink_event_id IS NULL`;
  yield* sql`CREATE INDEX work_task_thread_links_thread_idx ON work_task_thread_links(thread_id, task_id)`;
});
