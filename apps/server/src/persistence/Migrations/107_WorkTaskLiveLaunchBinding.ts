import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE work_task_native_live_bindings (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_native_launch_intents(command_id),
    run_id TEXT NOT NULL UNIQUE,
    run_event_id TEXT NOT NULL UNIQUE,
    run_sequence INTEGER NOT NULL UNIQUE REFERENCES orchestration_events(sequence),
    receipt_sequence INTEGER NOT NULL
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE work_task_native_live_terminals (
    event_id TEXT PRIMARY KEY NOT NULL,
    command_id TEXT NOT NULL REFERENCES work_task_native_live_bindings(command_id),
    sequence INTEGER NOT NULL UNIQUE REFERENCES orchestration_events(sequence),
    status TEXT NOT NULL CHECK(status IN('completed','interrupted','failed','cancelled','rolled_back'))
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_native_live_terminal_command_idx
    ON work_task_native_live_terminals(command_id,sequence DESC)`;
  for (const table of ["work_task_native_live_bindings", "work_task_native_live_terminals"])
    for (const operation of ["UPDATE", "DELETE"])
      yield* sql.unsafe(`CREATE TRIGGER ${table}_${operation.toLowerCase()} BEFORE ${operation} ON ${table}
        BEGIN SELECT RAISE(ABORT,'immutable native launch evidence'); END`);
  yield* sql`CREATE TRIGGER work_task_native_live_launch_fence BEFORE INSERT ON orchestration_events
    WHEN NEW.aggregate_kind='thread' AND NEW.application_event_version=2 AND NEW.event_type='run.created'
      AND EXISTS(SELECT 1 FROM work_task_native_launch_intents i WHERE i.command_id=NEW.command_id)
    BEGIN
      SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM work_task_native_launch_intents i
        JOIN work_task_native_launch_claims c ON c.command_id=i.command_id
        JOIN work_task_manager_groups g ON g.environment_id=i.environment_id AND g.group_id=i.group_id
        JOIN work_task_group_memberships m ON m.task_id=c.task_id AND m.membership_generation=c.membership_generation
        JOIN work_task_projections t ON t.task_id=c.task_id
        JOIN work_task_current_state s ON s.task_id=c.task_id
        JOIN work_task_allowances a ON a.task_id=c.task_id
        WHERE i.command_id=NEW.command_id AND i.manager_thread_id=NEW.stream_id
          AND i.message_id=json_extract(NEW.payload_json,'$.userMessageId')
          AND json_type(NEW.payload_json,'$.nativeTaskLaunch')='object'
          AND (SELECT count(*) FROM json_each(NEW.payload_json,'$.nativeTaskLaunch'))=12
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.commandId')=i.command_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.environmentId')=i.environment_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.taskId')=i.task_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.groupId')=i.group_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.managerThreadId')=i.manager_thread_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.managerGeneration')=i.manager_generation
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.membershipGeneration')=c.membership_generation
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.definitionRevision')=c.definition_revision
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.lifecycleRevision')=c.lifecycle_revision
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.allowanceUnit')=c.allowance_unit
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.messageId')=i.message_id
          AND json_extract(NEW.payload_json,'$.nativeTaskLaunch.claimedAt')=c.claimed_at
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
  yield* sql`CREATE TRIGGER work_task_native_provider_start_fence BEFORE INSERT ON orchestration_v2_effect_outbox
    WHEN NEW.effect_type='provider-turn.start'
      AND EXISTS(SELECT 1 FROM orchestration_v2_projection_runs r
        WHERE r.run_id=json_extract(NEW.payload_json,'$.runId')
          AND json_type(r.payload_json,'$.nativeTaskLaunch')='object')
      AND NOT EXISTS(SELECT 1 FROM work_task_executable_claims c
        WHERE c.run_id=json_extract(NEW.payload_json,'$.runId') AND c.release_event_id IS NULL)
    BEGIN SELECT RAISE(ABORT,'native Task run has no accepted work claim'); END`;
});
