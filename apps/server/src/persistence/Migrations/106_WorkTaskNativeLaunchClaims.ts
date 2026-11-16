import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE work_task_native_launch_claims (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_native_launch_intents(command_id),
    environment_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
    group_id TEXT NOT NULL,
    manager_thread_id TEXT NOT NULL,
    manager_generation INTEGER NOT NULL,
    membership_generation INTEGER NOT NULL,
    definition_revision INTEGER NOT NULL,
    lifecycle_revision INTEGER NOT NULL,
    allowance_unit INTEGER NOT NULL CHECK(allowance_unit > 0),
    claimed_at TEXT NOT NULL,
    FOREIGN KEY(environment_id,group_id,manager_generation,manager_thread_id)
      REFERENCES work_task_group_managers(environment_id,group_id,generation,manager_thread_id),
    FOREIGN KEY(task_id,membership_generation)
      REFERENCES work_task_group_memberships(task_id,membership_generation),
    FOREIGN KEY(task_id,definition_revision)
      REFERENCES work_task_accepted_definitions(task_id,definition_revision),
    UNIQUE(task_id,allowance_unit)
  ) WITHOUT ROWID`;
  yield* sql`CREATE UNIQUE INDEX work_task_native_claim_group_idx
    ON work_task_native_launch_claims(environment_id,group_id)`;
  yield* sql`CREATE UNIQUE INDEX work_task_native_claim_thread_idx
    ON work_task_native_launch_claims(environment_id,manager_thread_id)`;
  yield* sql`CREATE TRIGGER work_task_native_claim_immutable_update BEFORE UPDATE ON work_task_native_launch_claims
    BEGIN SELECT RAISE(ABORT,'immutable native launch claim'); END`;
  yield* sql`CREATE TRIGGER work_task_native_claim_immutable_delete BEFORE DELETE ON work_task_native_launch_claims
    BEGIN SELECT RAISE(ABORT,'immutable native launch claim'); END`;
  yield* sql`CREATE TRIGGER work_task_native_launch_fence BEFORE INSERT ON orchestration_v2_events
    WHEN NEW.event_type='run.created' AND EXISTS(
      SELECT 1 FROM work_task_native_launch_intents i WHERE i.command_id=NEW.command_id)
    BEGIN
      SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM work_task_native_launch_intents i
        JOIN work_task_native_launch_claims c ON c.command_id=i.command_id
        JOIN work_task_manager_groups g ON g.environment_id=i.environment_id AND g.group_id=i.group_id
        JOIN work_task_group_memberships m ON m.task_id=c.task_id AND m.membership_generation=c.membership_generation
        JOIN work_task_projections t ON t.task_id=c.task_id
        JOIN work_task_current_state s ON s.task_id=c.task_id
        JOIN work_task_allowances a ON a.task_id=c.task_id
        WHERE i.command_id=NEW.command_id AND i.manager_thread_id=NEW.thread_id
          AND i.message_id=json_extract(NEW.payload_json,'$.payload.userMessageId')
          AND NEW.run_id=json_extract(NEW.payload_json,'$.payload.id')
          AND c.environment_id=i.environment_id AND c.task_id=i.task_id AND c.group_id=i.group_id
          AND c.manager_thread_id=i.manager_thread_id AND c.manager_generation=i.manager_generation
          AND g.current_manager_generation=i.manager_generation
          AND g.current_manager_thread_id=i.manager_thread_id AND g.dispatch_hold='none'
          AND m.retired_event_id IS NULL AND m.environment_id=i.environment_id AND m.group_id=i.group_id
          AND t.definition_revision=c.definition_revision AND t.lifecycle_revision=c.lifecycle_revision
          AND s.desired_mode='active' AND s.execution_mode='idle'
          AND a.consumed<a.configured_cap AND a.consumed+1=c.allowance_unit
      ) THEN RAISE(ABORT,'native Task launch fenced') END;
    END`;
});
