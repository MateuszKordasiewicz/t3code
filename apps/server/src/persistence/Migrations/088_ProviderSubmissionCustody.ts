import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE orchestration_v2_submission_transports (
 format_version INTEGER NOT NULL CHECK(format_version=1),
 transport_id TEXT PRIMARY KEY NOT NULL CHECK(length(transport_id)>0),
 environment_id TEXT NOT NULL CHECK(length(environment_id)>0), project_id TEXT NOT NULL CHECK(length(project_id)>0), opening_thread_id TEXT NOT NULL CHECK(length(opening_thread_id)>0),
 driver TEXT NOT NULL CHECK(length(driver)>0), provider_instance_id TEXT NOT NULL CHECK(length(provider_instance_id)>0), provider_session_id TEXT NOT NULL CHECK(length(provider_session_id)>0),
 host_boot_id TEXT NOT NULL CHECK(length(host_boot_id)>0), scope_namespace TEXT NOT NULL CHECK(length(scope_namespace)>0), proposed_unit TEXT NOT NULL UNIQUE CHECK(length(proposed_unit)>0),
 creation_context_json TEXT NOT NULL CHECK(json_valid(creation_context_json) AND json_type(creation_context_json)='object'),
 creation_context_hash TEXT NOT NULL CHECK(length(creation_context_hash)=64 AND creation_context_hash NOT GLOB '*[^0-9a-f]*'),
 creation_input_hash TEXT NOT NULL CHECK(length(creation_input_hash)=64 AND creation_input_hash NOT GLOB '*[^0-9a-f]*'),
 created_at TEXT NOT NULL CHECK(length(created_at)>0),
 binding_json TEXT, binding_hash TEXT, bound_at TEXT,
 settlement_json TEXT, settlement_hash TEXT, settled_at TEXT,
 CHECK((binding_json IS NULL AND binding_hash IS NULL AND bound_at IS NULL) OR
       (binding_json IS NOT NULL AND binding_hash IS NOT NULL AND bound_at IS NOT NULL AND length(bound_at)>0 AND
        json_valid(binding_json) AND json_type(binding_json)='object' AND length(binding_hash)=64 AND binding_hash NOT GLOB '*[^0-9a-f]*' AND
        COALESCE(json_extract(binding_json,'$.unit')=proposed_unit,0))),
 CHECK((settlement_json IS NULL AND settlement_hash IS NULL AND settled_at IS NULL) OR
       (settlement_json IS NOT NULL AND settlement_hash IS NOT NULL AND settled_at IS NOT NULL AND length(settled_at)>0 AND
        binding_json IS NOT NULL AND json_valid(settlement_json) AND json_type(settlement_json)='object' AND length(settlement_hash)=64 AND settlement_hash NOT GLOB '*[^0-9a-f]*' AND
        COALESCE(json_type(settlement_json,'$.binding')='object' AND
                 json_extract(settlement_json,'$.observation') IN ('empty','removed') AND json_extract(settlement_json,'$.binding.unit')=json_extract(binding_json,'$.unit') AND json_extract(settlement_json,'$.binding.invocationId')=json_extract(binding_json,'$.invocationId') AND json_extract(settlement_json,'$.binding.controlGroup')=json_extract(binding_json,'$.controlGroup') AND json_extract(settlement_json,'$.binding.pid')=json_extract(binding_json,'$.pid') AND json_extract(settlement_json,'$.binding.pidStartTicks')=json_extract(binding_json,'$.pidStartTicks'),0)))
) WITHOUT ROWID`;
  yield* sql`CREATE TABLE orchestration_v2_submission_attempts (
 format_version INTEGER NOT NULL CHECK(format_version=1),
 attempt_id TEXT PRIMARY KEY NOT NULL CHECK(length(attempt_id)>0),
 transport_id TEXT NOT NULL REFERENCES orchestration_v2_submission_transports(transport_id),
 environment_id TEXT NOT NULL CHECK(length(environment_id)>0), project_id TEXT NOT NULL CHECK(length(project_id)>0), thread_id TEXT NOT NULL CHECK(length(thread_id)>0), run_id TEXT NOT NULL CHECK(length(run_id)>0),
 run_ordinal INTEGER NOT NULL CHECK(typeof(run_ordinal)='integer' AND run_ordinal BETWEEN 1 AND 9007199254740991),
 root_node_id TEXT NOT NULL CHECK(length(root_node_id)>0),
 provider_turn_ordinal INTEGER NOT NULL CHECK(typeof(provider_turn_ordinal)='integer' AND provider_turn_ordinal BETWEEN 1 AND 9007199254740991),
 driver TEXT NOT NULL CHECK(length(driver)>0), provider_instance_id TEXT NOT NULL CHECK(length(provider_instance_id)>0), provider_session_id TEXT NOT NULL CHECK(length(provider_session_id)>0),
 provider_thread_id TEXT NOT NULL CHECK(length(provider_thread_id)>0), native_thread_id TEXT NOT NULL CHECK(length(native_thread_id)>0),
 method TEXT NOT NULL CHECK(method='turn/start'),
 selection_json TEXT NOT NULL CHECK(json_valid(selection_json) AND json_type(selection_json)='object'),
 selection_hash TEXT NOT NULL CHECK(length(selection_hash)=64 AND selection_hash NOT GLOB '*[^0-9a-f]*'),
 input_hash TEXT NOT NULL CHECK(length(input_hash)=64 AND input_hash NOT GLOB '*[^0-9a-f]*'),
 frame_hash TEXT NOT NULL CHECK(length(frame_hash)=64 AND frame_hash NOT GLOB '*[^0-9a-f]*'),
 request_id_json TEXT NOT NULL CHECK(json_valid(request_id_json) AND json_type(request_id_json) IN ('integer','text')),
 created_at TEXT NOT NULL CHECK(length(created_at)>0),
 local_outcome TEXT, local_reason TEXT, local_observed_at TEXT,
 CHECK((local_outcome IS NULL AND local_reason IS NULL AND local_observed_at IS NULL) OR
       (local_outcome IS NOT NULL AND local_reason IS NOT NULL AND local_observed_at IS NOT NULL AND length(local_observed_at)>0 AND
        local_outcome IN ('not_submitted','unknown','submitted') AND local_reason IN ('completed','invalid_frame','queue_limit','output_blocked','reentrant','frame_limit','invalid_timeout','continuation','wrapper','sink','timeout','interrupted','outcome_hook') AND
        ((local_outcome='submitted' AND local_reason='completed') OR (local_outcome<>'submitted' AND local_reason<>'completed'))))
) WITHOUT ROWID`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_transports_insert_guard BEFORE INSERT ON orchestration_v2_submission_transports
 WHEN (NEW.binding_json IS NOT NULL OR NEW.binding_hash IS NOT NULL OR NEW.bound_at IS NOT NULL OR NEW.settlement_json IS NOT NULL OR NEW.settlement_hash IS NOT NULL OR NEW.settled_at IS NOT NULL) OR EXISTS(SELECT 1 FROM orchestration_v2_submission_transports WHERE transport_id=NEW.transport_id OR proposed_unit=NEW.proposed_unit)
 BEGIN SELECT RAISE(ABORT,'submission fence requires fresh immutable identity'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_transports_update_guard BEFORE UPDATE ON orchestration_v2_submission_transports
 WHEN NEW.format_version IS NOT OLD.format_version OR NEW.transport_id IS NOT OLD.transport_id OR NEW.environment_id IS NOT OLD.environment_id OR NEW.project_id IS NOT OLD.project_id OR NEW.opening_thread_id IS NOT OLD.opening_thread_id OR NEW.driver IS NOT OLD.driver OR NEW.provider_instance_id IS NOT OLD.provider_instance_id OR NEW.provider_session_id IS NOT OLD.provider_session_id OR NEW.host_boot_id IS NOT OLD.host_boot_id OR NEW.scope_namespace IS NOT OLD.scope_namespace OR NEW.proposed_unit IS NOT OLD.proposed_unit OR NEW.creation_context_json IS NOT OLD.creation_context_json OR NEW.creation_context_hash IS NOT OLD.creation_context_hash OR NEW.creation_input_hash IS NOT OLD.creation_input_hash OR NEW.created_at IS NOT OLD.created_at OR (OLD.binding_json IS NOT NULL AND NEW.binding_json IS NOT OLD.binding_json) OR (OLD.binding_hash IS NOT NULL AND NEW.binding_hash IS NOT OLD.binding_hash) OR (OLD.bound_at IS NOT NULL AND NEW.bound_at IS NOT OLD.bound_at) OR (OLD.settlement_json IS NOT NULL AND NEW.settlement_json IS NOT OLD.settlement_json) OR (OLD.settlement_hash IS NOT NULL AND NEW.settlement_hash IS NOT OLD.settlement_hash) OR (OLD.settled_at IS NOT NULL AND NEW.settled_at IS NOT OLD.settled_at) OR (OLD.settlement_json IS NULL AND NEW.settlement_json IS NOT NULL AND OLD.binding_json IS NULL)
 BEGIN SELECT RAISE(ABORT,'submission fence identity or evidence is immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_transports_delete_guard BEFORE DELETE ON orchestration_v2_submission_transports
 BEGIN SELECT RAISE(ABORT,'submission fence tombstone is permanent'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_attempts_insert_guard BEFORE INSERT ON orchestration_v2_submission_attempts
 WHEN (NEW.local_outcome IS NOT NULL OR NEW.local_reason IS NOT NULL OR NEW.local_observed_at IS NOT NULL) OR EXISTS(SELECT 1 FROM orchestration_v2_submission_attempts WHERE attempt_id=NEW.attempt_id)
 OR NOT EXISTS(SELECT 1 FROM orchestration_v2_submission_transports t WHERE t.transport_id=NEW.transport_id AND t.environment_id=NEW.environment_id AND t.driver=NEW.driver AND t.provider_instance_id=NEW.provider_instance_id AND t.provider_session_id=NEW.provider_session_id AND t.binding_json IS NOT NULL AND t.settlement_json IS NULL)
 BEGIN SELECT RAISE(ABORT,'submission fence requires fresh immutable identity'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_attempts_update_guard BEFORE UPDATE ON orchestration_v2_submission_attempts
 WHEN NEW.format_version IS NOT OLD.format_version OR NEW.attempt_id IS NOT OLD.attempt_id OR NEW.transport_id IS NOT OLD.transport_id OR NEW.environment_id IS NOT OLD.environment_id OR NEW.project_id IS NOT OLD.project_id OR NEW.thread_id IS NOT OLD.thread_id OR NEW.run_id IS NOT OLD.run_id OR NEW.run_ordinal IS NOT OLD.run_ordinal OR NEW.root_node_id IS NOT OLD.root_node_id OR NEW.provider_turn_ordinal IS NOT OLD.provider_turn_ordinal OR NEW.driver IS NOT OLD.driver OR NEW.provider_instance_id IS NOT OLD.provider_instance_id OR NEW.provider_session_id IS NOT OLD.provider_session_id OR NEW.provider_thread_id IS NOT OLD.provider_thread_id OR NEW.native_thread_id IS NOT OLD.native_thread_id OR NEW.method IS NOT OLD.method OR NEW.selection_json IS NOT OLD.selection_json OR NEW.selection_hash IS NOT OLD.selection_hash OR NEW.input_hash IS NOT OLD.input_hash OR NEW.frame_hash IS NOT OLD.frame_hash OR NEW.request_id_json IS NOT OLD.request_id_json OR NEW.created_at IS NOT OLD.created_at OR (OLD.local_outcome IS NOT NULL AND NEW.local_outcome IS NOT OLD.local_outcome) OR (OLD.local_reason IS NOT NULL AND NEW.local_reason IS NOT OLD.local_reason) OR (OLD.local_observed_at IS NOT NULL AND NEW.local_observed_at IS NOT OLD.local_observed_at)
 BEGIN SELECT RAISE(ABORT,'submission fence identity or evidence is immutable'); END`;
  yield* sql`CREATE TRIGGER orchestration_v2_submission_attempts_delete_guard BEFORE DELETE ON orchestration_v2_submission_attempts
 BEGIN SELECT RAISE(ABORT,'submission fence tombstone is permanent'); END`;
  yield* sql`CREATE INDEX submission_transports_thread ON orchestration_v2_submission_transports(environment_id,opening_thread_id) WHERE settlement_json IS NULL`;
  yield* sql`CREATE INDEX submission_transports_session ON orchestration_v2_submission_transports(environment_id,provider_session_id) WHERE settlement_json IS NULL`;
  yield* sql`CREATE INDEX submission_transports_instance ON orchestration_v2_submission_transports(environment_id,driver,provider_instance_id) WHERE settlement_json IS NULL`;
  yield* sql`CREATE INDEX submission_attempts_thread ON orchestration_v2_submission_attempts(environment_id,thread_id) WHERE local_outcome IS NULL OR local_outcome<>'not_submitted'`;
  yield* sql`CREATE INDEX submission_attempts_provider_thread ON orchestration_v2_submission_attempts(environment_id,provider_thread_id) WHERE local_outcome IS NULL OR local_outcome<>'not_submitted'`;
  yield* sql`CREATE INDEX submission_attempts_native ON orchestration_v2_submission_attempts(environment_id,driver,provider_instance_id,native_thread_id) WHERE local_outcome IS NULL OR local_outcome<>'not_submitted'`;
  yield* sql`CREATE INDEX submission_attempts_session ON orchestration_v2_submission_attempts(environment_id,provider_session_id) WHERE local_outcome IS NULL OR local_outcome<>'not_submitted'`;
  yield* sql`CREATE INDEX submission_attempts_transport ON orchestration_v2_submission_attempts(transport_id)`;
});
