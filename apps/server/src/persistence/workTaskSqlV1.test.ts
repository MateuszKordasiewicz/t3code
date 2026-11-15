import { assert, it } from "@effect/vitest";
import {
  WorkTaskManagerGroupId,
  WorkTaskManagerGroupControlSnapshot,
  WorkTaskMembershipProvenance,
  WorkTaskMembershipRecord,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  canonicalTextSql,
  canonicalEvidenceJsonSql,
  structuralJsonEqualSql,
  validTimestampSql,
  timestampAtOrBeforeSql,
} from "./workTaskSqlV1.ts";

const decodeGroupId = Schema.decodeUnknownSync(WorkTaskManagerGroupId);
const canonicalGroupId = (value: unknown) => {
  try {
    return decodeGroupId(value) === value;
  } catch {
    return false;
  }
};
const decodeControl = Schema.decodeUnknownSync(WorkTaskManagerGroupControlSnapshot);
const decodeProvenance = Schema.decodeUnknownSync(WorkTaskMembershipProvenance);
const isMembership = Schema.is(WorkTaskMembershipRecord);
const db = NodeSqliteClient.layer({ filename: ":memory:" });
const predicateResult = Effect.fn("WorkTaskSqlV1.predicateResult")(function* (
  predicate: string,
  left: unknown,
  right?: unknown,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql.unsafe<{ result: number }>(
    `WITH input(encoded) AS (VALUES (?)), fields AS (SELECT json_extract(encoded,'$.left') AS lhs,json_extract(encoded,'$.right') AS rhs FROM input) SELECT (${predicate}) AS result FROM fields`,
    [JSON.stringify({ left, right })],
  );
  return rows[0]!.result;
});
const provenance = (time: unknown) => ({
  eventId: "event",
  commandId: "command",
  occurredAt: time,
  performedBy: { subject: "owner", sessionId: "session" },
});
const canonicalTime = (time: unknown) => {
  try {
    return decodeProvenance(provenance(time)).occurredAt === time;
  } catch {
    return false;
  }
};
const orderedMembership = (left: unknown, right: unknown) =>
  isMembership({
    task: { environmentId: "env", taskId: "task" },
    group: { environmentId: "env", groupId: "group" },
    membershipGeneration: 1,
    acceptedDefinitionRevisionAtJoin: 1,
    joinedMembershipRevision: 1,
    joined: provenance(left),
    state: "retired",
    retiredMembershipRevision: 2,
    retired: provenance(right),
  });

it.effect("applies canonical text bounds to real SQLite inserts and updates", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE evidence_text(value)`;
    for (const operation of ["INSERT", "UPDATE"]) {
      yield* sql.unsafe(`CREATE TRIGGER evidence_text_${operation.toLowerCase()} BEFORE ${operation} ON evidence_text
        WHEN NOT (${canonicalTextSql("NEW.value")}) BEGIN SELECT RAISE(ABORT,'invalid canonical text'); END`);
    }
    yield* sql`INSERT INTO evidence_text VALUES ('seed')`;
    for (const value of [
      "canonical",
      "\0x",
      "x\0y",
      "\ud800",
      "\udfff",
      "\ud800x\udc00",
      "\u0085",
      "\u200b",
      "x".repeat(2000),
      "😀".repeat(1000),
      "\ud800".repeat(2000),
      "",
      " ",
      "\t\ufeff",
      " padded ",
      "\0x ",
      " \0x",
      "\u00a0x",
      "x\u3000",
      "x".repeat(2001),
      "😀".repeat(1001),
      null,
      0,
      true,
    ]) {
      const accepted = canonicalGroupId(value);
      assert.strictEqual(yield* predicateResult(canonicalTextSql("lhs"), value), Number(accepted));
      const encoded = JSON.stringify({ value });
      const before = yield* sql`SELECT rowid,hex(value) AS bytes FROM evidence_text ORDER BY rowid`;
      const insert = yield* Effect.exit(
        sql`INSERT INTO evidence_text SELECT json_extract(${encoded},'$.value')`,
      );
      assert.strictEqual(Exit.isSuccess(insert), accepted);
      const update = yield* Effect.exit(
        sql`UPDATE evidence_text SET value=json_extract(${encoded},'$.value') WHERE rowid=1`,
      );
      assert.strictEqual(Exit.isSuccess(update), accepted);
      if (!accepted)
        assert.deepStrictEqual(
          yield* sql`SELECT rowid,hex(value) AS bytes FROM evidence_text ORDER BY rowid`,
          before,
        );
    }
    const normalized = decodeGroupId(" \ufeffwire-padding \t");
    assert.strictEqual(yield* predicateResult(canonicalTextSql("lhs"), normalized), 1);
    const blob = yield* sql.unsafe<{ result: number }>(
      `SELECT (${canonicalTextSql("value")}) AS result FROM (SELECT X'41' AS value)`,
    );
    assert.strictEqual(blob[0]!.result, 0);
  }).pipe(Effect.provide(db)),
);

it.effect("rejects malformed, duplicate and noncanonical JSON evidence in installed triggers", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE evidence_json(value)`;
    for (const operation of ["INSERT", "UPDATE"]) {
      yield* sql.unsafe(`CREATE TRIGGER evidence_json_${operation.toLowerCase()} BEFORE ${operation} ON evidence_json
        WHEN NOT (${canonicalEvidenceJsonSql("NEW.value")}) BEGIN SELECT RAISE(ABORT,'invalid canonical evidence'); END`);
    }
    yield* sql`INSERT INTO evidence_json VALUES ('{}')`;
    const cases = [
      [JSON.stringify({ a: "x", b: ["\0x", "\ud800", "😀"] }), true],
      [JSON.stringify({ a: { key: "one" }, b: { key: "two" } }), true],
      [JSON.stringify({ value: "x".repeat(2000) }), true],
      [JSON.stringify({ value: "😀".repeat(1000) }), true],
      ["null", true],
      ["1", true],
      ["[]", true],
      ["{}", true],
      ["{", false],
      ["undefined", false],
      ['{"a":1,}', false],
      ['{"a":"one","a":"two"}', false],
      ['{"a":{"key":"one","key":"two"}}', false],
      [String.raw`{"\ud800":"one","\ud800":"two"}`, false],
      [JSON.stringify({ value: "" }), false],
      [JSON.stringify({ value: "\t\ufeff" }), false],
      [JSON.stringify({ value: " padded " }), false],
      [JSON.stringify({ value: "x".repeat(2001) }), false],
      [JSON.stringify({ value: "😀".repeat(1001) }), false],
      [null, false],
      [17, false],
    ] as const;
    for (const [value, accepted] of cases) {
      assert.strictEqual(
        yield* predicateResult(canonicalEvidenceJsonSql("lhs"), value),
        Number(accepted),
      );
      const before = yield* sql`SELECT rowid,hex(value) AS bytes FROM evidence_json ORDER BY rowid`;
      const encoded = JSON.stringify({ value });
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`INSERT INTO evidence_json SELECT json_extract(${encoded},'$.value')`,
          ),
        ),
        accepted,
      );
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`UPDATE evidence_json SET value=json_extract(${encoded},'$.value') WHERE rowid=1`,
          ),
        ),
        accepted,
      );
      if (!accepted)
        assert.deepStrictEqual(
          yield* sql`SELECT rowid,hex(value) AS bytes FROM evidence_json ORDER BY rowid`,
          before,
        );
    }
  }).pipe(Effect.provide(db)),
);

it.effect("compares JSON tree structure, exact keys and SQLite numeric values", () =>
  Effect.gen(function* () {
    const pairs = [
      ['{"a":1,"b":[2,{"x":"y"}]}', '{"b":[2,{"x":"y"}],"a":1}', true],
      ['{"a":{"x":1},"b":{"x":2}}', '{"b":{"x":2},"a":{"x":1}}', true],
      ['{"a":{"x":1},"b":{"x":2}}', '{"b":{"x":1},"a":{"x":2}}', false],
      ['{"a.b":1}', '{"a":{"b":1}}', false],
      ['{"a[0]":1}', '{"a":[1]}', false],
      [
        JSON.stringify({ "\0key": { x: "\ud800" }, other: 1 }),
        JSON.stringify({ other: 1, "\0key": { x: "\ud800" } }),
        true,
      ],
      [JSON.stringify({ "a\0b": 1 }), JSON.stringify({ a: 1 }), false],
      [JSON.stringify({ "\ud800": 1 }), JSON.stringify({ "\udfff": 1 }), false],
      [
        JSON.stringify({ "\ud800": "\udfff", a: true }),
        JSON.stringify({ a: true, "\ud800": "\udfff" }),
        true,
      ],
      ["[1,2]", "[2,1]", false],
      ["[1]", '{"0":1}', false],
      ['{"a":null}', "{}", false],
      ["null", "null", true],
      ["1", "true", false],
      ["1", '"1"', false],
      ["0", "null", false],
      ["1", "1.0", true],
      ["0", "-0.0", true],
      ["10000", "1e4", true],
      ["9007199254740991", "9007199254740991.0", true],
      ["9007199254740992", "9007199254740993", false],
      ["9007199254740993", "9007199254740992.0", false],
      ['{"a":1,"a":2}', '{"a":1,"a":2}', false],
      ["{", "{", false],
      [null, null, false],
      ['""', '""', true],
    ] as const;
    for (const [left, right, equal] of pairs) {
      assert.strictEqual(
        yield* predicateResult(structuralJsonEqualSql("lhs", "rhs"), left, right),
        Number(equal),
        `${left} vs ${right}`,
      );
      assert.strictEqual(
        yield* predicateResult(structuralJsonEqualSql("lhs", "rhs"), right, left),
        Number(equal),
      );
    }
    const control = {
      group: { environmentId: "env", groupId: "group" },
      controlRevision: -0,
      membershipRevision: 0,
      highestManagerGeneration: 0,
      currentManager: null,
      dispatchHold: "none",
    };
    const decoded = decodeControl(control);
    assert.strictEqual(
      yield* predicateResult(
        structuralJsonEqualSql("lhs", "rhs"),
        JSON.stringify(decoded),
        JSON.stringify({ ...decoded, controlRevision: 0 }),
      ),
      1,
    );
    assert.throws(() => decodeControl({ ...control, controlRevision: 9007199254740992 }));
  }).pipe(Effect.provide(db)),
);

it.effect("validates timestamps and orders full-precision membership instants in SQLite", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(`CREATE TABLE timestamp_values(value CHECK (${validTimestampSql("value")}))`);
    yield* sql`CREATE TABLE intervals(start_value,end_value)`;
    for (const operation of ["INSERT", "UPDATE"]) {
      yield* sql.unsafe(`CREATE TRIGGER intervals_${operation.toLowerCase()} BEFORE ${operation} ON intervals
        WHEN NOT (${timestampAtOrBeforeSql("NEW.start_value", "NEW.end_value")}) BEGIN SELECT RAISE(ABORT,'invalid interval'); END`);
    }
    yield* sql`INSERT INTO intervals VALUES ('2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`;
    const maximum = "2026-01-01T00:00:00." + "0".repeat(1979) + "Z";
    const times = [
      "0000-01-01T00:00:00Z",
      "0000-02-29T23:59:59+23:59",
      "0000-03-01T00:00:00-23:59",
      "9999-12-31T23:59:59-23:59",
      "2000-02-29T12:00:00Z",
      "1900-02-28T00:00:00Z",
      "1969-12-31T23:59:59.9999Z",
      "1970-01-01T00:00:00Z",
      "2026-01-01T00:00:00Z",
      "2026-01-01T00:00:00.1Z",
      "2026-01-01T00:00:00.1000Z",
      "2026-01-01T00:00:00.0001Z",
      "2026-01-01T00:00:00.0009Z",
      "2026-01-01T23:59:00+23:59",
      "2025-12-31T00:01:00-23:59",
      maximum,
      "1900-02-29T00:00:00Z",
      "2026-02-30T00:00:00Z",
      "2026-13-01T00:00:00Z",
      "2026-01-00T00:00:00Z",
      "2026-01-01T24:00:00Z",
      "2026-01-01T00:00:60Z",
      "2026-01-01T00:00:00+24:00",
      "2026-01-01T00:00:00-01:60",
      "2026-01-01T00:00:00.Z",
      "2026-01-01T00:00:00z",
      "2026-01-01T00:00:00Z\0suffix",
      "２０２６-01-01T00:00:00Z",
      maximum.replace("Z", "0Z"),
      null,
      "invalid",
      "",
      1,
    ];
    for (const value of times) {
      const accepted = canonicalTime(value);
      assert.strictEqual(
        yield* predicateResult(validTimestampSql("lhs"), value),
        Number(accepted),
        String(value).slice(0, 50),
      );
      const encoded = JSON.stringify({ value });
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`INSERT INTO timestamp_values SELECT json_extract(${encoded},'$.value')`,
          ),
        ),
        accepted,
      );
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`UPDATE timestamp_values SET value=json_extract(${encoded},'$.value') WHERE rowid=1`,
          ),
        ),
        accepted,
      );
    }
    const valid = times.filter(canonicalTime);
    const ordering = timestampAtOrBeforeSql("lhs", "rhs");
    for (const left of valid)
      for (const right of valid) {
        const accepted = orderedMembership(left, right);
        assert.strictEqual(
          yield* predicateResult(ordering, left, right),
          Number(accepted),
          `${String(left).slice(0, 35)} <= ${String(right).slice(0, 35)}`,
        );
      }
    for (const [left, right] of [
      ["2026-01-01T00:00:00.0009Z", "2026-01-01T00:00:00.0001Z"],
      ["2026-01-01T00:00:00.1Z", "2026-01-01T00:00:00.1000Z"],
      [
        "2026-01-01T00:00:00." + "0".repeat(1900) + "1Z",
        "2026-01-01T00:00:00." + "0".repeat(1900) + "2Z",
      ],
      ["invalid", "2026-01-01T00:00:00Z"],
      [null, null],
    ]) {
      const accepted = orderedMembership(left, right);
      const encoded = JSON.stringify({ left, right });
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`INSERT INTO intervals SELECT json_extract(${encoded},'$.left'),json_extract(${encoded},'$.right')`,
          ),
        ),
        accepted,
      );
      assert.strictEqual(
        Exit.isSuccess(
          yield* Effect.exit(
            sql`UPDATE intervals SET start_value=json_extract(${encoded},'$.left'),end_value=json_extract(${encoded},'$.right') WHERE rowid=1`,
          ),
        ),
        accepted,
      );
    }
  }).pipe(Effect.provide(db)),
);
