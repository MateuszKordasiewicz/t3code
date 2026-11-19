import { describe, expect, it } from "vite-plus/test";

import {
  decodeCollapsedIssueGraphRows,
  MAX_COLLAPSED_ISSUE_GRAPH_ROWS,
  toggleCollapsedIssueGraphRow,
} from "./issueGraphCollapsedRows";

const empty = { projects: [], tree: [], type: [], status: [] };

describe("collapsed issue graph rows", () => {
  it("decodes partial stores and ignores anything malformed", () => {
    expect(decodeCollapsedIssueGraphRows({ status: ["status:closed"] })).toEqual({
      ...empty,
      status: ["status:closed"],
    });
    // A store written before project rows existed keeps its regions.
    expect(decodeCollapsedIssueGraphRows({ tree: ["a"], type: [], status: [] })).toEqual({
      ...empty,
      tree: ["a"],
    });
    expect(decodeCollapsedIssueGraphRows(null)).toEqual(empty);
    expect(decodeCollapsedIssueGraphRows({ tree: "tree:x" })).toEqual(empty);
    expect(decodeCollapsedIssueGraphRows({ projects: [1] })).toEqual(empty);
    const long = Array.from({ length: 150 }, (_, i) => `env:${i}`);
    expect(decodeCollapsedIssueGraphRows({ projects: long }).projects).toEqual(
      long.slice(-MAX_COLLAPSED_ISSUE_GRAPH_ROWS),
    );
  });

  it("toggles one list's row and forgets the oldest past the limit", () => {
    const collapsed = toggleCollapsedIssueGraphRow(empty, "status", "status:closed");
    expect(collapsed).toEqual({ ...empty, status: ["status:closed"] });
    expect(toggleCollapsedIssueGraphRow(collapsed, "status", "status:closed")).toEqual(empty);
    expect(toggleCollapsedIssueGraphRow(collapsed, "projects", "env:p")).toEqual({
      ...collapsed,
      projects: ["env:p"],
    });

    let full = empty as Parameters<typeof toggleCollapsedIssueGraphRow>[0];
    for (let i = 0; i <= MAX_COLLAPSED_ISSUE_GRAPH_ROWS; i++)
      full = toggleCollapsedIssueGraphRow(full, "tree", `tree:${i}`);
    expect(full.tree).toHaveLength(MAX_COLLAPSED_ISSUE_GRAPH_ROWS);
    expect(full.tree[0]).toBe("tree:1");
    expect(full.tree.at(-1)).toBe(`tree:${MAX_COLLAPSED_ISSUE_GRAPH_ROWS}`);
  });
});
