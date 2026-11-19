import * as Schema from "effect/Schema";

import type { IssueGraphGroupBy } from "./issueGraph.logic";

/**
 * Collapsed graph rows, oldest first: project ids, and region ids per grouping (a region id only
 * means something within one grouping).
 */
export type CollapsedIssueGraphRows = Readonly<
  Record<IssueGraphGroupBy | "projects", ReadonlyArray<string>>
>;

/** Per list. The oldest collapse is forgotten first, so rows that no longer exist age out. */
export const MAX_COLLAPSED_ISSUE_GRAPH_ROWS = 100;

const STORAGE_KEY = "t3.issues.graphCollapsedBands";

const NO_COLLAPSED_ROWS: CollapsedIssueGraphRows = { projects: [], tree: [], type: [], status: [] };

const RowIds = Schema.optional(Schema.Array(Schema.String));
const decodeStored = Schema.decodeUnknownOption(
  Schema.Struct({ projects: RowIds, tree: RowIds, type: RowIds, status: RowIds }),
);

export function decodeCollapsedIssueGraphRows(value: unknown): CollapsedIssueGraphRows {
  const decoded = decodeStored(value);
  if (decoded._tag === "None") return NO_COLLAPSED_ROWS;
  const keep = (ids: ReadonlyArray<string> | undefined) =>
    (ids ?? []).slice(-MAX_COLLAPSED_ISSUE_GRAPH_ROWS);
  return {
    projects: keep(decoded.value.projects),
    tree: keep(decoded.value.tree),
    type: keep(decoded.value.type),
    status: keep(decoded.value.status),
  };
}

export function toggleCollapsedIssueGraphRow(
  rows: CollapsedIssueGraphRows,
  list: keyof CollapsedIssueGraphRows,
  rowId: string,
): CollapsedIssueGraphRows {
  const ids = rows[list];
  const next = ids.includes(rowId)
    ? ids.filter((id) => id !== rowId)
    : [...ids, rowId].slice(-MAX_COLLAPSED_ISSUE_GRAPH_ROWS);
  return { ...rows, [list]: next };
}

function storage(): Pick<Storage, "getItem" | "setItem"> | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

export function readCollapsedIssueGraphRows(): CollapsedIssueGraphRows {
  try {
    const raw = storage()?.getItem(STORAGE_KEY);
    return raw ? decodeCollapsedIssueGraphRows(JSON.parse(raw)) : NO_COLLAPSED_ROWS;
  } catch {
    return NO_COLLAPSED_ROWS;
  }
}

export function writeCollapsedIssueGraphRows(rows: CollapsedIssueGraphRows): void {
  try {
    storage()?.setItem(STORAGE_KEY, JSON.stringify(rows));
  } catch {
    // Storage can be full or forbidden; rows then reset on the next visit.
  }
}
