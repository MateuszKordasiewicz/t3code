import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_preparation_jobs (
 format_version INTEGER NOT NULL CHECK(format_version=1),
 job_id TEXT PRIMARY KEY NOT NULL CHECK(length(job_id)>0),
 entry_key TEXT NOT NULL UNIQUE CHECK(length(entry_key)>0),
 kind TEXT NOT NULL CHECK(kind IN ('launch','setup','title','release')),
 environment_id TEXT NOT NULL CHECK(length(environment_id)>0),
 project_id TEXT NOT NULL CHECK(length(project_id)>0),
 thread_id TEXT NOT NULL CHECK(length(thread_id)>0),
 run_id TEXT CHECK(run_id IS NULL OR length(run_id)>0),
 originating_launch_job_id TEXT REFERENCES orchestration_v2_preparation_jobs(job_id),
 server_boot_id TEXT NOT NULL CHECK(length(server_boot_id)>0),
 request_identity_json TEXT NOT NULL CHECK(json_valid(request_identity_json) AND json_type(request_identity_json)='object'),
 request_identity_hash TEXT NOT NULL CHECK(length(request_identity_hash)=64 AND request_identity_hash NOT GLOB '*[^0-9a-f]*'),
 created_at TEXT NOT NULL CHECK(length(created_at)>0),
 record_version INTEGER NOT NULL DEFAULT 0 CHECK(typeof(record_version)='integer' AND record_version BETWEEN 0 AND 9007199254740991),
 state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','quiescing','unknown','settled')),
 process_exposed INTEGER NOT NULL DEFAULT 0 CHECK(typeof(process_exposed)='integer' AND process_exposed IN (0,1)),
 code_closed_at TEXT CHECK(code_closed_at IS NULL OR length(code_closed_at)>0),
 settlement_json TEXT, settlement_hash TEXT, settled_at TEXT,
 CHECK(typeof(job_id)='text' AND typeof(entry_key)='text' AND typeof(kind)='text' AND
       typeof(environment_id)='text' AND typeof(project_id)='text' AND typeof(thread_id)='text' AND
       typeof(server_boot_id)='text' AND typeof(request_identity_json)='text' AND typeof(request_identity_hash)='text' AND typeof(created_at)='text' AND typeof(state)='text'),
 CHECK((run_id IS NULL OR typeof(run_id)='text') AND
       (originating_launch_job_id IS NULL OR typeof(originating_launch_job_id)='text') AND
       (code_closed_at IS NULL OR typeof(code_closed_at)='text') AND
       (settlement_json IS NULL OR typeof(settlement_json)='text') AND
       (settlement_hash IS NULL OR typeof(settlement_hash)='text') AND
       (settled_at IS NULL OR typeof(settled_at)='text')),
 CHECK(originating_launch_job_id IS NULL OR kind='setup'),
 CHECK((state='settled')=(settlement_json IS NOT NULL)),
 CHECK((settlement_json IS NULL AND settlement_hash IS NULL AND settled_at IS NULL) OR
       (settlement_json IS NOT NULL AND settlement_hash IS NOT NULL AND settled_at IS NOT NULL AND length(settled_at)>0 AND code_closed_at IS NOT NULL AND
        json_valid(settlement_json) AND json_type(settlement_json)='object' AND length(settlement_hash)=64 AND settlement_hash NOT GLOB '*[^0-9a-f]*' AND
        COALESCE(json_type(settlement_json,'$.v')='integer' AND json_extract(settlement_json,'$.v')=1 AND
                 json_type(settlement_json,'$.jobId')='text' AND json_extract(settlement_json,'$.jobId')=job_id AND
                 json_type(settlement_json,'$.entryKey')='text' AND json_extract(settlement_json,'$.entryKey')=entry_key AND
                 json_type(settlement_json,'$.priorVersion')='integer' AND json_extract(settlement_json,'$.priorVersion') BETWEEN 0 AND 9007199254740991 AND
                 json_extract(settlement_json,'$.kind') IN ('code_only','owned_resources') AND
                 (process_exposed=0 OR json_extract(settlement_json,'$.kind')='owned_resources'),0)))
) WITHOUT ROWID`;
  yield* sql`CREATE TRIGGER preparation_jobs_insert_guard BEFORE INSERT ON orchestration_v2_preparation_jobs
 WHEN NEW.record_version<>0 OR NEW.state<>'active' OR NEW.process_exposed<>0 OR NEW.code_closed_at IS NOT NULL OR NEW.settlement_json IS NOT NULL OR NEW.settlement_hash IS NOT NULL OR NEW.settled_at IS NOT NULL
 OR EXISTS(SELECT 1 FROM orchestration_v2_preparation_jobs WHERE job_id=NEW.job_id OR entry_key=NEW.entry_key)
 OR (NEW.originating_launch_job_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM orchestration_v2_preparation_jobs p WHERE p.job_id=NEW.originating_launch_job_id AND p.kind='launch' AND p.environment_id=NEW.environment_id AND p.project_id=NEW.project_id AND p.thread_id=NEW.thread_id))
 BEGIN SELECT RAISE(ABORT,'preparation job requires fresh immutable identity'); END`;
  yield* sql`CREATE TRIGGER preparation_jobs_update_guard BEFORE UPDATE ON orchestration_v2_preparation_jobs
 WHEN OLD.state='settled' OR
 NEW.format_version IS NOT OLD.format_version OR NEW.job_id IS NOT OLD.job_id OR NEW.entry_key IS NOT OLD.entry_key OR NEW.kind IS NOT OLD.kind OR
 NEW.environment_id IS NOT OLD.environment_id OR NEW.project_id IS NOT OLD.project_id OR NEW.thread_id IS NOT OLD.thread_id OR NEW.run_id IS NOT OLD.run_id OR
 NEW.originating_launch_job_id IS NOT OLD.originating_launch_job_id OR NEW.server_boot_id IS NOT OLD.server_boot_id OR NEW.request_identity_json IS NOT OLD.request_identity_json OR NEW.request_identity_hash IS NOT OLD.request_identity_hash OR NEW.created_at IS NOT OLD.created_at OR
 NEW.record_version<>OLD.record_version+1 OR NEW.process_exposed<OLD.process_exposed OR (OLD.code_closed_at IS NOT NULL AND NEW.code_closed_at IS NOT OLD.code_closed_at) OR
 NOT ((OLD.state='active' AND NEW.state IN ('active','quiescing','unknown','settled')) OR
      (OLD.state='quiescing' AND NEW.state IN ('quiescing','unknown','settled')) OR
      (OLD.state='unknown' AND NEW.state IN ('unknown','settled'))) OR
 (NEW.state='settled' AND json_extract(NEW.settlement_json,'$.priorVersion') IS NOT OLD.record_version)
 BEGIN SELECT RAISE(ABORT,'preparation job identity and evidence are monotonic'); END`;
  yield* sql`CREATE TRIGGER preparation_jobs_delete_guard BEFORE DELETE ON orchestration_v2_preparation_jobs
 BEGIN SELECT RAISE(ABORT,'preparation job history is permanent'); END`;
  yield* sql`CREATE INDEX preparation_jobs_thread_history ON orchestration_v2_preparation_jobs(environment_id,thread_id,job_id)`;
  yield* sql`CREATE INDEX preparation_jobs_origin_history ON orchestration_v2_preparation_jobs(originating_launch_job_id,job_id)`;
});
