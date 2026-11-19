import * as Schema from "effect/Schema";

import type { IssueGraphGroupBy } from "./issueGraph.logic";

/** Collapsed band ids per grouping, oldest first. Band ids only mean something within one grouping. */
export type CollapsedIssueGraphBands = Readonly<Record<IssueGraphGroupBy, ReadonlyArray<string>>>;

/** Per grouping. The oldest collapse is forgotten first, so bands that no longer exist age out. */
export const MAX_COLLAPSED_ISSUE_GRAPH_BANDS = 100;

const STORAGE_KEY = "t3.issues.graphCollapsedBands";

const NO_COLLAPSED_BANDS: CollapsedIssueGraphBands = { tree: [], type: [], status: [] };

const BandIds = Schema.Array(Schema.String.check(Schema.isMaxLength(1000)));
const decodeStored = Schema.decodeUnknownOption(
  Schema.Struct({
    tree: Schema.optional(BandIds),
    type: Schema.optional(BandIds),
    status: Schema.optional(BandIds),
  }),
);

export function decodeCollapsedIssueGraphBands(value: unknown): CollapsedIssueGraphBands {
  const decoded = decodeStored(value);
  if (decoded._tag === "None") return NO_COLLAPSED_BANDS;
  const keep = (ids: ReadonlyArray<string> | undefined) =>
    (ids ?? []).slice(-MAX_COLLAPSED_ISSUE_GRAPH_BANDS);
  return {
    tree: keep(decoded.value.tree),
    type: keep(decoded.value.type),
    status: keep(decoded.value.status),
  };
}

export function toggleCollapsedIssueGraphBand(
  bands: CollapsedIssueGraphBands,
  groupBy: IssueGraphGroupBy,
  bandId: string,
): CollapsedIssueGraphBands {
  const ids = bands[groupBy];
  const next = ids.includes(bandId)
    ? ids.filter((id) => id !== bandId)
    : [...ids, bandId].slice(-MAX_COLLAPSED_ISSUE_GRAPH_BANDS);
  return { ...bands, [groupBy]: next };
}

function storage(): Pick<Storage, "getItem" | "setItem"> | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

export function readCollapsedIssueGraphBands(): CollapsedIssueGraphBands {
  try {
    const raw = storage()?.getItem(STORAGE_KEY);
    return raw ? decodeCollapsedIssueGraphBands(JSON.parse(raw)) : NO_COLLAPSED_BANDS;
  } catch {
    return NO_COLLAPSED_BANDS;
  }
}

export function writeCollapsedIssueGraphBands(bands: CollapsedIssueGraphBands): void {
  try {
    storage()?.setItem(STORAGE_KEY, JSON.stringify(bands));
  } catch {
    // Storage can be full or forbidden; bands then reset on the next visit.
  }
}
