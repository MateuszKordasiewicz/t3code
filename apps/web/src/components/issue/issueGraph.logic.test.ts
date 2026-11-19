import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { EnvironmentId, IssueTrackerKind, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { layoutIssueGraph, type IssueGraphLayout } from "./issueGraph.logic";

type Overrides = Partial<
  Pick<
    EnvironmentIssueEntry,
    "parentKey" | "dependsOnKeys" | "issueType" | "priority" | "updatedAt"
  >
> & { readonly status?: string; readonly closed?: boolean };

function issue(key: string, overrides: Overrides = {}): EnvironmentIssueEntry {
  const { status, closed, ...fields } = overrides;
  return {
    environmentId: EnvironmentId.make("env-1"),
    projectId: ProjectId.make("project-1"),
    projectTitle: "T3",
    tracker: IssueTrackerKind.make("beads"),
    host: "local",
    scope: "t3",
    key,
    displayKey: key,
    title: `Issue ${key}`,
    url: `beads://${key}`,
    author: null,
    state: { category: closed ? "closed" : "open", name: status ?? (closed ? "closed" : "open") },
    createdAt: "2026-11-01T00:00:00.000Z",
    updatedAt: "2026-11-01T00:00:00.000Z",
    closedAt: null,
    assignees: [],
    labels: [],
    commentCount: 0,
    ...fields,
  };
}

const nodeId = (key: string) => `env-1:project-1\u0000local\u0000t3\u0000${key}`;

function clusterKeys(layout: IssueGraphLayout) {
  return layout.clusters.map((cluster) => ({
    kind: cluster.kind,
    label: cluster.label,
    keys: cluster.nodeIds
      .map((id) => layout.nodes.find((node) => node.id === id)!.entry.key)
      .toSorted(),
  }));
}

function nodeOf(layout: IssueGraphLayout, key: string) {
  return layout.nodes.find((node) => node.entry.key === key)!;
}

describe("layoutIssueGraph", () => {
  it("clusters epic trees, keys hidden-parent siblings by the missing key, and groups loose issues by type", () => {
    const layout = layoutIssueGraph(
      [
        issue("bug-1", { issueType: "bug" }),
        issue("epic-a", { issueType: "epic", priority: 2 }),
        issue("a.1", { parentKey: "epic-a", issueType: "feature" }),
        issue("a.1.1", { parentKey: "a.1", issueType: "task" }),
        issue("h.1", { parentKey: "epic-hidden", issueType: "feature", priority: 0 }),
        issue("h.2", { parentKey: "epic-hidden", issueType: "feature" }),
        issue("chore-1", { issueType: "chore" }),
        issue("bug-2", { issueType: "bug" }),
        issue("gh-1"),
        // Same issue read twice: drawn once.
        issue("bug-2", { issueType: "bug" }),
      ],
      "tree",
    );

    expect(clusterKeys(layout)).toEqual([
      { kind: "tree", label: "epic-hidden", keys: ["h.1", "h.2"] },
      { kind: "tree", label: "epic-a", keys: ["a.1", "a.1.1", "epic-a"] },
      { kind: "loose", label: "bug", keys: ["bug-1", "bug-2"] },
      { kind: "loose", label: "chore", keys: ["chore-1"] },
      { kind: "loose", label: "", keys: ["gh-1"] },
    ]);
    expect(layout.clusters[0]!.rootNodeId).toBeNull();
    expect(layout.clusters[1]!.rootNodeId).toBe(nodeId("epic-a"));
    expect(layout.nodes).toHaveLength(9);
    // Bands stack top to bottom without overlapping.
    for (let i = 1; i < layout.clusters.length; i++) {
      const above = layout.clusters[i - 1]!;
      expect(layout.clusters[i]!.y).toBeGreaterThan(above.y + above.height);
    }
  });

  it("draws parent and blocks edges between present issues and drops the rest", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("one", { parentKey: "epic" }),
        issue("two", { parentKey: "epic", dependsOnKeys: ["one", "gone"] }),
        issue("loose", { dependsOnKeys: ["two"] }),
      ],
      "tree",
    );

    expect(layout.edges.map(({ kind, from, to }) => ({ kind, from, to }))).toEqual(
      expect.arrayContaining([
        { kind: "blocks", from: nodeId("one"), to: nodeId("two") },
        { kind: "blocks", from: nodeId("two"), to: nodeId("loose") },
        { kind: "parent", from: nodeId("epic"), to: nodeId("one") },
        { kind: "parent", from: nodeId("epic"), to: nodeId("two") },
      ]),
    );
    expect(layout.edges).toHaveLength(4);
    for (const edge of layout.edges) expect(edge.path).toMatch(/^M [\d.]+ [\d.]+ C /);
  });

  it("ranks blockers left of what they block and survives dependency cycles", () => {
    const chain = layoutIssueGraph(
      [
        issue("c", { issueType: "task", dependsOnKeys: ["b"] }),
        issue("b", { issueType: "task", dependsOnKeys: ["a"] }),
        issue("a", { issueType: "task" }),
        issue("side", { issueType: "task", dependsOnKeys: ["a"] }),
      ],
      "type",
    );
    expect(nodeOf(chain, "a").x).toBeLessThan(nodeOf(chain, "b").x);
    expect(nodeOf(chain, "b").x).toBeLessThan(nodeOf(chain, "c").x);
    expect(nodeOf(chain, "side").x).toBe(nodeOf(chain, "b").x);

    const cycle = [
      issue("x", { dependsOnKeys: ["z"], parentKey: "y" }),
      issue("y", { dependsOnKeys: ["x"], parentKey: "z" }),
      issue("z", { dependsOnKeys: ["y"], parentKey: "y" }),
    ];
    const first = layoutIssueGraph(cycle, "tree");
    expect(first.nodes).toHaveLength(3);
    expect(new Set(first.nodes.map((node) => node.x)).size).toBe(3);
    expect(layoutIssueGraph(cycle.toReversed(), "tree")).toEqual(first);
  });

  it("groups by type and by status in a stable order", () => {
    const entries = [
      issue("1", { issueType: "chore", status: "in_progress" }),
      issue("2", { issueType: "zeta", closed: true }),
      issue("3", { issueType: "bug", status: "blocked" }),
      issue("4", { status: "open" }),
      issue("5", { issueType: "epic", status: "open" }),
      issue("6", { issueType: "alpha" }),
    ];
    expect(layoutIssueGraph(entries, "type").clusters.map((c) => [c.kind, c.label])).toEqual([
      ["type", "epic"],
      ["type", "bug"],
      ["type", "chore"],
      ["type", "alpha"],
      ["type", "zeta"],
      ["type", ""],
    ]);
    const grid = layoutIssueGraph(
      entries.map((entry) => ({ ...entry, issueType: "task" })),
      "type",
    ).nodes;
    expect({
      columns: new Set(grid.map((node) => node.x)).size,
      rows: new Set(grid.map((node) => node.y)).size,
    }).toEqual({ columns: 4, rows: 2 });
    expect(layoutIssueGraph(entries, "status").clusters.map((c) => c.label)).toEqual([
      "open",
      "in_progress",
      "blocked",
      "closed",
    ]);
  });
});
