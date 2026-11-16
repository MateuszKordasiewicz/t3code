import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { canonicalTextSql, validTimestampSql } from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const safe = (value: string, minimum = 0) =>
    `(typeof(${value})='integer' AND ${value} BETWEEN ${minimum} AND 9007199254740991)`;
  const text = (value: string) => canonicalTextSql(value);
  const optionalText = (value: string) => `(${value} IS NULL OR ${text(value)})`;
  const guard = (name: string, table: string, operation: string, valid: string) =>
    sql.unsafe(`CREATE TRIGGER ${name} BEFORE ${operation} ON ${table}
      WHEN (${valid}) IS NOT TRUE BEGIN SELECT RAISE(ABORT,'invalid wake evidence'); END`);
  yield* sql.unsafe(`CREATE TABLE work_task_wake_state (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_projections(task_id),
    dirty_generation INTEGER NOT NULL CHECK(${safe("dirty_generation")}),
    acknowledged_generation INTEGER NOT NULL CHECK(${safe("acknowledged_generation")} AND acknowledged_generation<=dirty_generation),
    overflow_through_generation INTEGER CHECK(overflow_through_generation IS NULL OR
      (${safe("overflow_through_generation", 1)} AND overflow_through_generation>acknowledged_generation AND overflow_through_generation<=dirty_generation))
  ) WITHOUT ROWID`);
  yield* sql.unsafe(`CREATE TABLE work_task_wake_source_state (
    task_id TEXT NOT NULL REFERENCES work_task_wake_state(task_id),
    source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL UNIQUE,
    source_sequence INTEGER NOT NULL UNIQUE CHECK(${safe("source_sequence")}),
    dirty_generation INTEGER NOT NULL CHECK(${safe("dirty_generation", 1)}),
    retained_pending INTEGER NOT NULL CHECK(typeof(retained_pending)='integer' AND retained_pending IN(0,1)),
    original_reservation_id TEXT,
    original_result_ref TEXT,
    PRIMARY KEY(task_id,source_kind,source_id), UNIQUE(task_id,dirty_generation),
    FOREIGN KEY(task_id,source_kind,source_id) REFERENCES work_task_wake_sources(task_id,source_kind,source_id)
  ) WITHOUT ROWID`);
  yield* sql`CREATE UNIQUE INDEX work_task_wake_parent_claim_tuple_idx ON work_task_wake_reservations(reservation_id,task_id,message_id,run_id,claimed_at)`;
  yield* sql.unsafe(`CREATE TABLE work_task_wake_phase_state (
    event_id TEXT PRIMARY KEY NOT NULL,
    sequence INTEGER NOT NULL UNIQUE CHECK(${safe("sequence")}),
    task_id TEXT NOT NULL REFERENCES work_task_wake_state(task_id),
    reservation_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN('reserved','claimed','uncertain','acknowledged')),
    dirty_generation INTEGER NOT NULL CHECK(${safe("dirty_generation", 1)}),
    acknowledged_generation INTEGER NOT NULL CHECK(${safe("acknowledged_generation")} AND acknowledged_generation<=dirty_generation),
    previous_acknowledged_generation INTEGER,
    observed_dirty_generation INTEGER,
    message_id TEXT, run_id TEXT, first_claimed_at TEXT,
    CHECK((kind='claimed' AND typeof(message_id)='text' AND typeof(run_id)='text' AND ${validTimestampSql("first_claimed_at")})
      OR (kind<>'claimed' AND message_id IS NULL AND run_id IS NULL AND first_claimed_at IS NULL)),
    CHECK((kind='acknowledged' AND ${safe("previous_acknowledged_generation")} AND ${safe("observed_dirty_generation", 1)}
      AND previous_acknowledged_generation<=acknowledged_generation AND acknowledged_generation<=observed_dirty_generation AND observed_dirty_generation<=dirty_generation)
      OR (kind<>'acknowledged' AND previous_acknowledged_generation IS NULL AND observed_dirty_generation IS NULL)),
    FOREIGN KEY(task_id,reservation_id) REFERENCES work_task_wake_reservations(task_id,reservation_id),
    FOREIGN KEY(reservation_id,task_id,message_id,run_id,first_claimed_at)
      REFERENCES work_task_wake_reservations(reservation_id,task_id,message_id,run_id,claimed_at) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`);
  for (const statement of [
    "CREATE INDEX work_task_wake_retained_idx ON work_task_wake_source_state(task_id,dirty_generation,source_kind,source_id) WHERE retained_pending=1",
    "CREATE INDEX work_task_wake_overflow_idx ON work_task_wake_source_state(task_id,dirty_generation DESC) WHERE retained_pending=0",
    "CREATE INDEX work_task_wake_source_sequence_idx ON work_task_wake_source_state(task_id,source_sequence DESC)",
    "CREATE UNIQUE INDEX work_task_wake_reserved_idx ON work_task_wake_phase_state(reservation_id) WHERE kind='reserved'",
    "CREATE UNIQUE INDEX work_task_wake_uncertain_idx ON work_task_wake_phase_state(reservation_id) WHERE kind='uncertain'",
    "CREATE INDEX work_task_wake_phase_latest_idx ON work_task_wake_phase_state(reservation_id,kind,sequence DESC)",
    "CREATE INDEX work_task_wake_ack_max_idx ON work_task_wake_phase_state(task_id,acknowledged_generation DESC) WHERE kind='acknowledged'",
    "CREATE INDEX work_task_wake_phase_task_sequence_idx ON work_task_wake_phase_state(task_id,sequence DESC)",
  ])
    yield* sql.unsafe(statement);
  const dirty = (task: string) =>
    `COALESCE((SELECT dirty_generation FROM work_task_wake_source_state WHERE task_id=${task} ORDER BY dirty_generation DESC LIMIT 1),0)`;
  const acknowledged = (task: string) =>
    `COALESCE((SELECT acknowledged_generation FROM work_task_wake_phase_state WHERE task_id=${task} AND kind='acknowledged' ORDER BY acknowledged_generation DESC LIMIT 1),0)`;
  const overflow = (task: string, ack: string) =>
    `(SELECT dirty_generation FROM work_task_wake_source_state WHERE task_id=${task} AND retained_pending=0 AND dirty_generation>${ack} ORDER BY dirty_generation DESC LIMIT 1)`;
  const afterHeads = (
    sequence: string,
  ) => `${sequence}>COALESCE((SELECT source_sequence FROM work_task_wake_source_state
    WHERE task_id=NEW.task_id ORDER BY source_sequence DESC LIMIT 1),-1)
    AND ${sequence}>COALESCE((SELECT sequence FROM work_task_wake_phase_state WHERE task_id=NEW.task_id ORDER BY sequence DESC LIMIT 1),-1)`;
  const phase = (kind: string, column: string, order = "DESC") =>
    `(SELECT ${column} FROM work_task_wake_phase_state WHERE reservation_id=NEW.reservation_id AND kind='${kind}' ORDER BY sequence ${order} LIMIT 1)`;
  const state = (column: string) =>
    `(SELECT ${column} FROM work_task_wake_state WHERE task_id=NEW.task_id)`;
  yield* guard(
    "work_task_wake_state_insert",
    "work_task_wake_state",
    "INSERT",
    `
    ${text("NEW.task_id")} AND NOT EXISTS(SELECT 1 FROM work_task_wake_state WHERE task_id=NEW.task_id)
    AND NEW.dirty_generation=0 AND NEW.acknowledged_generation=0 AND NEW.overflow_through_generation IS NULL
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_source_state WHERE task_id=NEW.task_id)
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE task_id=NEW.task_id)`,
  );
  yield* guard(
    "work_task_wake_state_update",
    "work_task_wake_state",
    "UPDATE",
    `
    NEW.task_id IS OLD.task_id AND NEW.dirty_generation=${dirty("NEW.task_id")}
    AND NEW.acknowledged_generation=${acknowledged("NEW.task_id")}
    AND NEW.overflow_through_generation IS ${overflow("NEW.task_id", "NEW.acknowledged_generation")}`,
  );
  yield* guard("work_task_wake_state_delete", "work_task_wake_state", "DELETE", "0");
  yield* guard(
    "work_task_wake_source_insert",
    "work_task_wake_source_state",
    "INSERT",
    `
    ${text("NEW.source_kind")} AND ${text("NEW.source_id")} AND ${text("NEW.source_event_id")}
    AND ${optionalText("NEW.original_reservation_id")} AND ${optionalText("NEW.original_result_ref")}
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_source_state WHERE (task_id=NEW.task_id AND source_kind=NEW.source_kind AND source_id=NEW.source_id)
      OR source_event_id=NEW.source_event_id OR source_sequence=NEW.source_sequence OR (task_id=NEW.task_id AND dirty_generation=NEW.dirty_generation))
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE event_id=NEW.source_event_id OR sequence=NEW.source_sequence)
    AND ${afterHeads("NEW.source_sequence")}
    AND EXISTS(SELECT 1 FROM work_task_wake_sources WHERE task_id=NEW.task_id AND source_kind=NEW.source_kind AND source_id=NEW.source_id
      AND reservation_id IS NEW.original_reservation_id AND result_ref IS NEW.original_result_ref)
    AND NEW.dirty_generation=${state("dirty_generation")}+1
    AND NEW.retained_pending=((SELECT count(*) FROM work_task_wake_source_state WHERE task_id=NEW.task_id AND retained_pending=1)<32)
    AND (NEW.original_reservation_id IS NULL OR EXISTS(SELECT 1 FROM work_task_wake_phase_state
      WHERE reservation_id=NEW.original_reservation_id AND task_id=NEW.task_id AND kind='reserved' AND sequence<NEW.source_sequence))`,
  );
  yield* guard(
    "work_task_wake_source_update",
    "work_task_wake_source_state",
    "UPDATE",
    `
    NEW.task_id IS OLD.task_id AND NEW.source_kind IS OLD.source_kind AND NEW.source_id IS OLD.source_id
    AND NEW.source_event_id IS OLD.source_event_id AND NEW.source_sequence IS OLD.source_sequence
    AND NEW.dirty_generation IS OLD.dirty_generation AND NEW.original_reservation_id IS OLD.original_reservation_id
    AND NEW.original_result_ref IS OLD.original_result_ref AND OLD.retained_pending=1 AND NEW.retained_pending=0
    AND NEW.dirty_generation<=${state("acknowledged_generation")}`,
  );
  yield* guard("work_task_wake_source_delete", "work_task_wake_source_state", "DELETE", "0");
  yield* sql.unsafe(`CREATE TRIGGER work_task_wake_source_derive AFTER INSERT ON work_task_wake_source_state BEGIN
    UPDATE work_task_wake_state SET dirty_generation=${dirty("NEW.task_id")},
      overflow_through_generation=${overflow("NEW.task_id", "acknowledged_generation")} WHERE task_id=NEW.task_id; END`);
  yield* guard(
    "work_task_wake_phase_insert",
    "work_task_wake_phase_state",
    "INSERT",
    `
    ${text("NEW.event_id")} AND ${text("NEW.reservation_id")}
    AND (NEW.kind<>'claimed' OR (${text("NEW.message_id")} AND ${text("NEW.run_id")}))
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE event_id=NEW.event_id OR sequence=NEW.sequence)
    AND NOT EXISTS(SELECT 1 FROM work_task_wake_source_state WHERE source_event_id=NEW.event_id OR source_sequence=NEW.sequence)
    AND ${afterHeads("NEW.sequence")} AND NEW.dirty_generation=${state("dirty_generation")}
    AND EXISTS(SELECT 1 FROM work_task_wake_reservations WHERE reservation_id=NEW.reservation_id AND task_id=NEW.task_id)
    AND CASE NEW.kind
      WHEN 'reserved' THEN ${phase("reserved", "event_id")} IS NULL
        AND NEW.acknowledged_generation=${state("acknowledged_generation")}
        AND EXISTS(SELECT 1 FROM work_task_wake_reservations p WHERE p.reservation_id=NEW.reservation_id AND p.task_id=NEW.task_id
          AND p.message_id IS NULL AND p.run_id IS NULL AND p.claimed_at IS NULL
          AND p.dirty_generation=NEW.dirty_generation AND p.acknowledged_generation=NEW.acknowledged_generation
          AND ${safe("p.manager_generation", 1)} AND ${safe("p.allowance_unit", 1)} AND ${text("p.command_id")} AND ${validTimestampSql("p.created_at")})
      WHEN 'claimed' THEN ${phase("reserved", "event_id")} IS NOT NULL AND ${phase("acknowledged", "event_id")} IS NULL
        AND NEW.acknowledged_generation=${state("acknowledged_generation")}
        AND (${phase("claimed", "event_id")} IS NULL OR (
          NEW.dirty_generation>${phase("claimed", "dirty_generation")}
          AND NEW.message_id IS ${phase("claimed", "message_id", "ASC")}
          AND NEW.run_id IS ${phase("claimed", "run_id", "ASC")}
          AND NEW.first_claimed_at IS ${phase("claimed", "first_claimed_at", "ASC")}))
      WHEN 'uncertain' THEN ${phase("reserved", "event_id")} IS NOT NULL AND ${phase("uncertain", "event_id")} IS NULL
        AND ${phase("acknowledged", "event_id")} IS NULL AND NEW.acknowledged_generation=${state("acknowledged_generation")}
      WHEN 'acknowledged' THEN ${phase("claimed", "event_id")} IS NOT NULL
        AND NEW.previous_acknowledged_generation=${state("acknowledged_generation")}
        AND NEW.acknowledged_generation>NEW.previous_acknowledged_generation
        AND NEW.observed_dirty_generation=${phase("claimed", "dirty_generation")}
      ELSE 0 END`,
  );
  yield* guard("work_task_wake_phase_update", "work_task_wake_phase_state", "UPDATE", "0");
  yield* guard("work_task_wake_phase_delete", "work_task_wake_phase_state", "DELETE", "0");
  yield* sql.unsafe(`CREATE TRIGGER work_task_wake_ack_derive AFTER INSERT ON work_task_wake_phase_state
    WHEN NEW.kind='acknowledged' BEGIN
      UPDATE work_task_wake_state SET acknowledged_generation=${acknowledged("NEW.task_id")},
        overflow_through_generation=${overflow("NEW.task_id", "NEW.acknowledged_generation")} WHERE task_id=NEW.task_id;
      UPDATE work_task_wake_source_state SET retained_pending=0 WHERE task_id=NEW.task_id AND retained_pending=1 AND dirty_generation<=NEW.acknowledged_generation;
    END`);
  const sourceOwner = (row: string) => `EXISTS(SELECT 1 FROM work_task_wake_source_state
    WHERE task_id=${row}.task_id AND source_kind=${row}.source_kind AND source_id=${row}.source_id)`;
  yield* guard(
    "work_task_wake_parent_source_insert",
    "work_task_wake_sources",
    "INSERT",
    `NOT ${sourceOwner("NEW")}`,
  );
  yield* guard(
    "work_task_wake_parent_source_delete",
    "work_task_wake_sources",
    "DELETE",
    `NOT ${sourceOwner("OLD")}`,
  );
  yield* guard(
    "work_task_wake_parent_source_update",
    "work_task_wake_sources",
    "UPDATE",
    `
    (NOT ${sourceOwner("OLD")} OR (NEW.task_id IS OLD.task_id AND NEW.source_kind IS OLD.source_kind
      AND NEW.source_id IS OLD.source_id AND NEW.reservation_id IS OLD.reservation_id AND NEW.result_ref IS OLD.result_ref))
    AND (NOT ${sourceOwner("NEW")} OR (NEW.task_id IS OLD.task_id AND NEW.source_kind IS OLD.source_kind AND NEW.source_id IS OLD.source_id))`,
  );
  const reservationOwner = (row: string) =>
    `EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE reservation_id=${row}.reservation_id AND kind='reserved')`;
  const collisions = (exceptOld: boolean) => `EXISTS(SELECT 1 FROM work_task_wake_reservations p
    WHERE ${exceptOld ? "p.rowid<>OLD.rowid AND" : ""}
      (p.reservation_id=NEW.reservation_id OR p.command_id=NEW.command_id OR (p.task_id=NEW.task_id AND p.allowance_unit=NEW.allowance_unit)
        OR (NEW.message_id IS NOT NULL AND p.message_id=NEW.message_id) OR (NEW.run_id IS NOT NULL AND p.run_id=NEW.run_id))
      AND EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE reservation_id=p.reservation_id AND kind='reserved'))`;
  yield* guard(
    "work_task_wake_parent_reservation_insert",
    "work_task_wake_reservations",
    "INSERT",
    `NOT ${collisions(false)}`,
  );
  yield* guard(
    "work_task_wake_parent_reservation_delete",
    "work_task_wake_reservations",
    "DELETE",
    `NOT ${reservationOwner("OLD")}`,
  );
  yield* guard(
    "work_task_wake_parent_reservation_update",
    "work_task_wake_reservations",
    "UPDATE",
    `
    NOT ${collisions(true)} AND (NOT ${reservationOwner("OLD")} OR (
      NEW.reservation_id IS OLD.reservation_id AND NEW.task_id IS OLD.task_id AND NEW.manager_generation IS OLD.manager_generation
      AND NEW.command_id IS OLD.command_id AND NEW.allowance_unit IS OLD.allowance_unit
      AND NEW.dirty_generation IS OLD.dirty_generation AND NEW.acknowledged_generation IS OLD.acknowledged_generation AND NEW.created_at IS OLD.created_at
      AND ((NEW.message_id IS OLD.message_id AND NEW.run_id IS OLD.run_id AND NEW.claimed_at IS OLD.claimed_at)
        OR (OLD.message_id IS NULL AND OLD.run_id IS NULL AND OLD.claimed_at IS NULL
          AND EXISTS(SELECT 1 FROM work_task_wake_phase_state WHERE reservation_id=OLD.reservation_id AND kind='claimed'
            AND message_id IS NEW.message_id AND run_id IS NEW.run_id AND first_claimed_at IS NEW.claimed_at)))))`,
  );
});
