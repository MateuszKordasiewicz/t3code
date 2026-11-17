/**
 * Beads (`bd`) keeps a repository's issues in a database inside the checkout, which agents read
 * and write with the CLI. This tracker only reads it, through the same CLI, from the project's
 * directory: bd finds the database there, including from a git worktree of the repository.
 */
import * as Cache from "effect/Cache";
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import {
  IssueTrackerKind,
  type IssueActor,
  type IssueCapabilities,
  type IssueComment,
  type IssueState,
  type IssueViewerPermissions,
} from "@t3tools/contracts";
import {
  IssueTrackerError,
  type IssueProjectDiscovery,
  type IssueTrackerApi,
  type TrackerIssueDetail,
  type TrackerIssueSummary,
} from "@t3tools/source-control-core/server/IssueTracker";
import * as SourceControlHost from "@t3tools/source-control-core/server/SourceControlHost";
import { decodeJsonResult } from "@t3tools/shared/schemaJson";

export const KIND = IssueTrackerKind.make("beads");
/** Not a network host: every beads database is told apart by its scope alone. */
export const HOST = "beads";
const URL_PROTOCOL = "beads:";

const CAPABILITIES: IssueCapabilities = {
  create: false,
  edit: false,
  comment: false,
  labels: false,
  assignees: false,
  reactions: false,
  templates: false,
  linkedPullRequests: false,
  events: false,
  search: true,
};

const VIEWER_PERMISSIONS: IssueViewerPermissions = {
  setState: false,
  comment: false,
  edit: false,
  labels: false,
  assignees: false,
};

const TIMEOUT_MS = 15_000;
/** A listing is read whole and paged here; refusing an oversized one beats truncating it. */
const MAX_OUTPUT_BYTES = 64 * 1024 * 1024;
const DISCOVERY_TTL = Duration.seconds(30);
const BUSY_RETRY = { times: 4, schedule: Schedule.spaced("250 millis") };
const CURSOR = /^[1-9]\d{0,6}$/;
const BUSY_DETAIL =
  "The beads database is busy: another bd process is using it. Try again in a moment.";

const optionalEnv = (name: string) =>
  Config.String(name).pipe(
    Config.withDefault(""),
    Effect.map((value) => value.trim()),
    Effect.orElseSucceed(() => ""),
  );

const BeadContext = Schema.Struct({
  project_id: Schema.String,
  repo_root: Schema.optional(Schema.String),
});

/**
 * The repository's folder name, for people, and the database's own id, so two folders with one
 * name stay apart. bd names the main checkout's root even from a worktree, so worktrees share it.
 */
export function databaseScope(context: typeof BeadContext.Type): string {
  const id = context.project_id.trim().toLowerCase().replaceAll("-", "").slice(0, 8);
  const folder = (context.repo_root ?? "")
    .split(/[\\/]/)
    .filter((part) => part.length > 0)
    .at(-1)
    ?.toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return folder ? `${folder}-${id}` : id;
}

const BeadRow = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  status: Schema.String,
  priority: Schema.optional(Schema.Number),
  issue_type: Schema.optional(Schema.String),
  assignee: Schema.optional(Schema.String),
  created_by: Schema.optional(Schema.String),
  created_at: Schema.String,
  updated_at: Schema.String,
  closed_at: Schema.optional(Schema.String),
  close_reason: Schema.optional(Schema.String),
  labels: Schema.optional(Schema.Array(Schema.String)),
  comment_count: Schema.optional(Schema.Number),
  parent: Schema.optional(Schema.String),
});
type BeadRow = typeof BeadRow.Type;

const BeadRelation = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  status: Schema.String,
  dependency_type: Schema.optional(Schema.String),
});
type BeadRelation = typeof BeadRelation.Type;

const BeadDetail = Schema.Struct({
  ...BeadRow.fields,
  description: Schema.optional(Schema.String),
  acceptance_criteria: Schema.optional(Schema.String),
  design: Schema.optional(Schema.String),
  notes: Schema.optional(Schema.String),
  dependencies: Schema.optional(Schema.Array(BeadRelation)),
  dependents: Schema.optional(Schema.Array(BeadRelation)),
  comments: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        author: Schema.optional(Schema.String),
        text: Schema.String,
        created_at: Schema.String,
      }),
    ),
  ),
});
type BeadDetail = typeof BeadDetail.Type;

const BeadFailure = Schema.Struct({ error: Schema.String });

const decodeContext = decodeJsonResult(BeadContext);
const decodeRows = decodeJsonResult(Schema.Array(BeadRow));
const decodeDetail = decodeJsonResult(Schema.Array(BeadDetail));
const decodeFailure = decodeJsonResult(BeadFailure);

/** An issue link that names its database, so two repositories' equal keys never meet. */
export function issueUrl(scope: string, key: string): string {
  return `beads://${encodeURIComponent(scope)}/${encodeURIComponent(key)}`;
}

export function parseIssueUrl(url: URL) {
  if (url.protocol !== URL_PROTOCOL) return null;
  const key = decodeURIComponent(url.pathname.replace(/^\/+/, ""));
  const scope = decodeURIComponent(url.hostname).toLowerCase();
  if (scope.length === 0 || key.length === 0 || key.includes("/")) return null;
  return { host: HOST, scope, key };
}

const STATE_NAMES: Record<string, string> = {
  open: "Open",
  in_progress: "In progress",
  blocked: "Blocked",
  deferred: "Deferred",
  closed: "Closed",
};

function stateName(status: string): string {
  const named = STATE_NAMES[status];
  if (named !== undefined) return named;
  const words = status.replaceAll("_", " ").trim();
  return words.length === 0 ? "Unknown" : words[0]!.toUpperCase() + words.slice(1);
}

function toState(status: string): IssueState {
  return { category: status === "closed" ? "closed" : "open", name: stateName(status) };
}

function toActor(login: string | undefined): IssueActor | null {
  const trimmed = login?.trim();
  return trimmed ? { login: trimmed, name: null, avatarUrl: null } : null;
}

export function toSummary(scope: string, row: BeadRow): TrackerIssueSummary {
  const assignee = toActor(row.assignee);
  return {
    key: row.id,
    displayKey: row.id,
    title: row.title.trim() || row.id,
    url: issueUrl(scope, row.id),
    author: toActor(row.created_by),
    state: toState(row.status),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    closedAt: row.closed_at ?? null,
    assignees: assignee === null ? [] : [assignee],
    labels: (row.labels ?? []).map((name) => ({ name, color: null })),
    commentCount: row.comment_count ?? 0,
  };
}

function relationLine(relation: BeadRelation): string {
  return `${relation.id} · ${relation.title} (${stateName(relation.status).toLowerCase()})`;
}

function relationList(label: string, relations: ReadonlyArray<BeadRelation>): string[] {
  if (relations.length === 0) return [];
  return [`- ${label}:`, ...relations.map((relation) => `  - ${relationLine(relation)}`)];
}

/**
 * The generic issue has no fields for a bead's type, priority or graph, so they are written out
 * as text after the bead's own writing.
 */
export function detailBody(bead: BeadDetail): string {
  const dependencies = bead.dependencies ?? [];
  const dependents = bead.dependents ?? [];
  const parent = dependencies.filter((relation) => relation.dependency_type === "parent-child");
  const needs = dependencies.filter((relation) => relation.dependency_type !== "parent-child");
  const children = dependents.filter((relation) => relation.dependency_type === "parent-child");
  const neededBy = dependents.filter((relation) => relation.dependency_type !== "parent-child");
  const details = [
    `- Type: ${bead.issue_type ?? "unknown"}`,
    `- Priority: ${bead.priority === undefined ? "none" : `P${bead.priority}`}`,
    `- Status: ${stateName(bead.status)}`,
    ...(bead.close_reason?.trim() ? [`- Close reason: ${bead.close_reason.trim()}`] : []),
    ...(parent.length > 0
      ? parent.map((relation) => `- Parent: ${relationLine(relation)}`)
      : bead.parent
        ? [`- Parent: ${bead.parent}`]
        : []),
    ...relationList("Depends on", needs),
    ...relationList("Children", children),
    ...relationList("Required by", neededBy),
  ];
  const sections = [
    bead.description?.trim() ?? "",
    ...(
      [
        ["Acceptance criteria", bead.acceptance_criteria],
        ["Design", bead.design],
        ["Notes", bead.notes],
      ] as const
    ).flatMap(([heading, text]) => (text?.trim() ? [`## ${heading}\n\n${text.trim()}`] : [])),
    `## Details\n\n${details.join("\n")}`,
  ];
  return sections.filter((section) => section.length > 0).join("\n\n");
}

function toComments(bead: BeadDetail): IssueComment[] {
  return (bead.comments ?? []).map((comment) => ({
    id: comment.id,
    author: toActor(comment.author),
    body: comment.text,
    createdAt: comment.created_at,
    url: null,
  }));
}

/** The process's own choice of database would read one repository's beads for every project. */
const BD_ENV = { BEADS_DIR: undefined, BEADS_DB: undefined } as const;

export const make: Effect.Effect<IssueTrackerApi, never, SourceControlHost.SourceControlHost> =
  Effect.gen(function* () {
    const host = yield* SourceControlHost.SourceControlHost;

    const failure = (
      operation: string,
      reason: IssueTrackerError["reason"],
      detail: string,
      cause?: unknown,
    ) => new IssueTrackerError({ tracker: KIND, operation, reason, detail, cause });

    /** bd answers a failure with `{ "error": ... }` on stdout and a non-zero exit. */
    const bd = (operation: string, cwd: string, args: ReadonlyArray<string>) =>
      host.process
        .run({
          operation: `BeadsIssueTracker.${operation}`,
          command: "bd",
          args: [...args, "--json", "--readonly"],
          cwd,
          env: BD_ENV,
          allowNonZeroExit: true,
          timeoutMs: TIMEOUT_MS,
          maxOutputBytes: MAX_OUTPUT_BYTES,
          outputMode: "error",
        })
        .pipe(
          Effect.mapError((error) => {
            switch (error._tag) {
              case "VcsProcessSpawnError":
                return failure(
                  operation,
                  "missing-tool",
                  "bd (beads) was not found on the T3 server's PATH.",
                  error,
                );
              case "VcsProcessTimeoutError":
                return failure(
                  operation,
                  "failed",
                  `bd did not answer within ${TIMEOUT_MS / 1000} seconds; the beads database may be busy.`,
                  error,
                );
              case "VcsProcessOutputLimitError":
                return failure(operation, "failed", "bd answered with too much output.", error);
              default:
                return failure(operation, "failed", "bd could not be run.", error);
            }
          }),
          Effect.flatMap((output) => {
            if (output.exitCode === 0) return Effect.succeed(output.stdout);
            const decoded = decodeFailure(output.stdout);
            const message = Result.isSuccess(decoded)
              ? decoded.success.error
              : output.stderr.trim() || "bd failed.";
            if (/workspace gate busy|database is locked|lock.*held|retry when/i.test(message)) {
              return Effect.fail(failure(operation, "failed", BUSY_DETAIL));
            }
            if (/not found|no issues found/i.test(message)) {
              return Effect.fail(failure(operation, "not-found", message));
            }
            return Effect.fail(failure(operation, "failed", `bd failed: ${message}`));
          }),
          Effect.retry({
            ...BUSY_RETRY,
            while: (error) => error.detail === BUSY_DETAIL,
          }),
        );

    const read = <A>(
      operation: string,
      cwd: string,
      args: ReadonlyArray<string>,
      decode: (raw: string) => Result.Result<A, unknown>,
    ) =>
      bd(operation, cwd, args).pipe(
        Effect.flatMap((raw) => {
          const decoded = decode(raw);
          return Result.isSuccess(decoded)
            ? Effect.succeed(decoded.success)
            : Effect.fail(
                failure(
                  operation,
                  "failed",
                  "bd returned output this version cannot read.",
                  decoded.failure,
                ),
              );
        }),
      );

    const discovery = yield* Cache.make({
      capacity: 256,
      timeToLive: DISCOVERY_TTL,
      lookup: (cwd: string) =>
        read("discoverProject", cwd, ["context"], decodeContext).pipe(
          Effect.map((context): IssueProjectDiscovery => ({
            host: HOST,
            scope: databaseScope(context),
          })),
          // Most projects have no beads, and most machines no bd: neither is a failure here.
          Effect.catchIf(
            (error) => error.reason === "missing-tool" || error.reason === "not-found",
            () => Effect.succeed(null),
          ),
          Effect.catchIf(
            (error) => /no \.beads directory|no beads database/i.test(error.detail),
            () => Effect.succeed(null),
          ),
        ),
    });

    /** A project lends its directory to a scope only when bd finds that very database there. */
    const requireScope = (operation: string, cwd: string, scope: string) =>
      Cache.get(discovery, cwd).pipe(
        Effect.flatMap((found) =>
          found !== null && found.scope === scope.trim().toLowerCase()
            ? Effect.void
            : Effect.fail(
                failure(operation, "not-found", "This project does not hold that beads database."),
              ),
        ),
      );

    const getViewer: IssueTrackerApi["getViewer"] = Effect.fn("BeadsIssueTracker.getViewer")(
      function* (input) {
        // bd's own order for the actor it records.
        const fromEnv = yield* optionalEnv("BEADS_ACTOR");
        if (fromEnv) return fromEnv;
        const gitName = yield* host.git
          .readConfigValue(input.cwd, "user.name")
          .pipe(Effect.orElseSucceed(() => null));
        if (gitName?.trim()) return gitName.trim();
        return (yield* optionalEnv("USER")) || (yield* optionalEnv("USERNAME")) || "unknown";
      },
    );

    const listIssues: IssueTrackerApi["listIssues"] = Effect.fn("BeadsIssueTracker.listIssues")(
      function* (input) {
        if (input.cursor !== undefined && !CURSOR.test(input.cursor)) {
          return yield* failure("listIssues", "failed", "The beads page cursor is not valid.");
        }
        yield* requireScope("listIssues", input.cwd, input.scope);
        const offset = input.cursor === undefined ? 0 : Number(input.cursor);
        const assignee = input.involvement === "assigned" ? input.viewer : input.assignee;
        const rows = yield* read(
          "listIssues",
          input.cwd,
          [
            "list",
            "--brief",
            "--flat",
            "--limit",
            "0",
            ...(input.state === "all" ? ["--all"] : []),
            ...(input.state === "closed" ? ["--status", "closed"] : []),
            ...(input.labels ?? []).flatMap((label) => ["--label", label]),
            ...(assignee === undefined ? [] : ["--assignee", assignee]),
          ],
          decodeRows,
        );
        const query = input.query?.trim().toLowerCase();
        const field = input.sort === "created" ? "created_at" : "updated_at";
        const matching = rows
          .filter((row) => input.involvement !== "authored" || row.created_by === input.viewer)
          .filter(
            (row) =>
              !query ||
              row.id.toLowerCase().includes(query) ||
              row.title.toLowerCase().includes(query),
          )
          .toSorted((a, b) => b[field].localeCompare(a[field]) || a.id.localeCompare(b.id));
        const page = matching.slice(offset, offset + input.limit);
        const next = offset + input.limit;
        return {
          items: page.map((row) => toSummary(input.scope, row)),
          nextCursor: next < matching.length ? String(next) : null,
        };
      },
    );

    const getIssue: IssueTrackerApi["getIssue"] = Effect.fn("BeadsIssueTracker.getIssue")(
      function* (input) {
        yield* requireScope("getIssue", input.cwd, input.scope);
        const [bead] = yield* read(
          "getIssue",
          input.cwd,
          ["show", "--include-comments", "--include-dependents", "--id", input.key],
          decodeDetail,
        );
        if (bead === undefined || bead.id !== input.key) {
          return yield* failure("getIssue", "not-found", `Bead ${input.key} was not found.`);
        }
        return {
          ...toSummary(input.scope, bead),
          body: detailBody(bead),
          viewer: null,
          viewerPermissions: VIEWER_PERMISSIONS,
          states: [],
          comments: toComments(bead),
          commentsCursor: null,
          events: [],
          linkedPullRequests: [],
        } satisfies TrackerIssueDetail;
      },
    );

    return {
      kind: KIND,
      capabilities: CAPABILITIES,
      discoverProject: (input) => Cache.get(discovery, input.cwd),
      parseIssueUrl,
      getViewer,
      listIssues,
      getIssue,
      // A bead's comments all arrive with it, so no page ever follows.
      getComments: () => Effect.succeed({ comments: [], nextCursor: null }),
    } satisfies IssueTrackerApi;
  });
