import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export const THREAD_TRIAGE_NOTE_MAX_LENGTH = 120;

export const ThreadTriageNote = TrimmedNonEmptyString.check(
  Schema.isMaxLength(THREAD_TRIAGE_NOTE_MAX_LENGTH),
  Schema.makeFilter((value) =>
    /[\p{Cc}]/u.test(value) ? "A thread note must be a single line of text." : true,
  ),
);
export type ThreadTriageNote = typeof ThreadTriageNote.Type;

/** Who acts next on a stopped thread. Snoozed and settled are their own thread states. */
export const ThreadTriageLabel = Schema.Struct({
  kind: Schema.Literal("my-move"),
  note: Schema.NullOr(ThreadTriageNote),
});
export type ThreadTriageLabel = typeof ThreadTriageLabel.Type;

/** Labels read back from storage or a peer. Lenient so a later label kind never fails a whole thread. */
export const StoredThreadTriageLabel = Schema.NullOr(ThreadTriageLabel).pipe(
  Schema.catchDecoding(() => Effect.succeedSome(null)),
);
