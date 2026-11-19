import { describe, expect, it } from "vite-plus/test";

import {
  decodeCollapsedIssueGraphBands,
  MAX_COLLAPSED_ISSUE_GRAPH_BANDS,
  toggleCollapsedIssueGraphBand,
} from "./issueGraphCollapsedBands";

const empty = { tree: [], type: [], status: [] };

describe("collapsed issue graph bands", () => {
  it("decodes partial stores and ignores anything malformed", () => {
    expect(decodeCollapsedIssueGraphBands({ status: ["status:closed"] })).toEqual({
      ...empty,
      status: ["status:closed"],
    });
    expect(decodeCollapsedIssueGraphBands(null)).toEqual(empty);
    expect(decodeCollapsedIssueGraphBands({ tree: "tree:x" })).toEqual(empty);
    expect(decodeCollapsedIssueGraphBands({ tree: ["x".repeat(1001)] })).toEqual(empty);
    const long = Array.from({ length: 150 }, (_, i) => `tree:${i}`);
    expect(decodeCollapsedIssueGraphBands({ tree: long }).tree).toEqual(
      long.slice(-MAX_COLLAPSED_ISSUE_GRAPH_BANDS),
    );
  });

  it("toggles one grouping's band and forgets the oldest past the limit", () => {
    const collapsed = toggleCollapsedIssueGraphBand(empty, "status", "status:closed");
    expect(collapsed).toEqual({ ...empty, status: ["status:closed"] });
    expect(toggleCollapsedIssueGraphBand(collapsed, "status", "status:closed")).toEqual(empty);
    expect(toggleCollapsedIssueGraphBand(collapsed, "tree", "status:closed")).toEqual({
      ...collapsed,
      tree: ["status:closed"],
    });

    let full = empty as Parameters<typeof toggleCollapsedIssueGraphBand>[0];
    for (let i = 0; i <= MAX_COLLAPSED_ISSUE_GRAPH_BANDS; i++)
      full = toggleCollapsedIssueGraphBand(full, "tree", `tree:${i}`);
    expect(full.tree).toHaveLength(MAX_COLLAPSED_ISSUE_GRAPH_BANDS);
    expect(full.tree[0]).toBe("tree:1");
    expect(full.tree.at(-1)).toBe(`tree:${MAX_COLLAPSED_ISSUE_GRAPH_BANDS}`);
  });
});
