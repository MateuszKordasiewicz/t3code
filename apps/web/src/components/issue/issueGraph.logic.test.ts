import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { EnvironmentId, IssueTrackerKind, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { layoutIssueGraph, type IssueGraphProject } from "./issueGraph.logic";

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

type Layout = ReadonlyArray<IssueGraphProject>;

const regionsOf = (layout: Layout) => layout.flatMap((project) => project.regions);
const nodesOf = (layout: Layout) => regionsOf(layout).flatMap((region) => region.nodes);
const edgesOf = (layout: Layout) => regionsOf(layout).flatMap((region) => region.edges);

function regionKeys(layout: Layout) {
  return regionsOf(layout).map((region) => ({
    kind: region.kind,
    label: region.label,
    keys: region.nodes.map((node) => node.entry.key).toSorted(),
  }));
}

function nodeOf(layout: Layout, key: string) {
  return nodesOf(layout).find((node) => node.entry.key === key)!;
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
        // An epic with every slice filtered out still gets its own band.
        issue("epic-solo", { issueType: "epic" }),
        // Same issue read twice: drawn once.
        issue("bug-2", { issueType: "bug" }),
      ],
      "tree",
    );

    expect(regionKeys(layout)).toEqual([
      { kind: "tree", label: "epic-hidden", keys: ["h.1", "h.2"] },
      { kind: "tree", label: "epic-a", keys: ["a.1", "a.1.1", "epic-a"] },
      { kind: "tree", label: "epic-solo", keys: ["epic-solo"] },
      { kind: "loose", label: "bug", keys: ["bug-1", "bug-2"] },
      { kind: "loose", label: "chore", keys: ["chore-1"] },
      { kind: "loose", label: "", keys: ["gh-1"] },
    ]);
    const [hidden, epicA] = regionsOf(layout);
    expect(hidden!.root).toBeNull();
    expect(epicA!.root?.key).toBe("epic-a");
    expect(nodesOf(layout)).toHaveLength(10);
  });

  it("draws parent and blocks edges inside a region, toned by the blocker, and labels the rest on the card", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("one", { parentKey: "epic" }),
        issue("two", { parentKey: "epic", dependsOnKeys: ["one", "gone", "shipped"] }),
        issue("fixed", { parentKey: "epic", closed: true }),
        issue("three", { parentKey: "epic", dependsOnKeys: ["fixed", "three"] }),
        issue("loose", { dependsOnKeys: ["two", "gone"] }),
        issue("shipped", { issueType: "bug", closed: true }),
        issue("later", { issueType: "task", dependsOnKeys: ["shipped"] }),
      ],
      "tree",
    );

    expect(
      edgesOf(layout).map((edge) => ({
        kind: edge.kind,
        from: edge.from,
        to: edge.to,
        blockerOpen: edge.kind === "blocks" ? edge.blockerOpen : null,
      })),
    ).toEqual(
      expect.arrayContaining([
        { kind: "blocks", from: nodeId("one"), to: nodeId("two"), blockerOpen: true },
        { kind: "blocks", from: nodeId("fixed"), to: nodeId("three"), blockerOpen: false },
        { kind: "parent", from: nodeId("epic"), to: nodeId("one"), blockerOpen: null },
        { kind: "parent", from: nodeId("epic"), to: nodeId("two"), blockerOpen: null },
        { kind: "parent", from: nodeId("epic"), to: nodeId("fixed"), blockerOpen: null },
        { kind: "parent", from: nodeId("epic"), to: nodeId("three"), blockerOpen: null },
      ]),
    );
    // two -> loose crosses into the loose region, which scrolls on its own: not drawn.
    expect(edgesOf(layout)).toHaveLength(6);
    for (const edge of edgesOf(layout)) {
      expect(edge.path).toMatch(edge.kind === "blocks" ? /^M [\d.]+ [\d.]+ C / : /^M [^C]+$/);
    }

    // Blockers with no arrow show on the card: amber if one is open, grey only when all are
    // known closed, and neutral when one is not in the list, as its state is unknown.
    expect(
      Object.fromEntries(nodesOf(layout).map((node) => [node.entry.key, node.externalBlockers])),
    ).toEqual({
      epic: null,
      one: null,
      two: { keys: ["gone", "shipped"], tone: "unknown" },
      fixed: null,
      three: null,
      loose: { keys: ["two", "gone"], tone: "open" },
      shipped: null,
      later: { keys: ["shipped"], tone: "closed" },
    });
  });

  it("draws sub-tasks as a tree: below the parent, joined by orthogonal connectors that pass no card", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("a", { parentKey: "epic" }),
        // Blocked by its sibling, so it sits two columns right of its parent.
        issue("b", { parentKey: "epic", dependsOnKeys: ["a"] }),
        issue("c", { parentKey: "a" }),
        issue("d", { parentKey: "epic" }),
      ],
      "tree",
    );
    const nodes = nodesOf(layout);
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const tree = edgesOf(layout).filter((edge) => edge.kind === "parent");
    expect(tree).toHaveLength(4);
    for (const edge of tree) {
      const from = byId.get(edge.from)!;
      const to = byId.get(edge.to)!;
      expect(to.x).toBeGreaterThan(from.x);
      expect(to.y).toBeGreaterThan(from.y + from.height);

      const [start, ...steps] = edge.path.match(/[MHV] [^MHV]+/g)!.map((step) => step.trim());
      const [sx, sy] = start!.slice(2).split(" ").map(Number);
      expect(start).toMatch(/^M /);
      // Leaves the parent's bottom edge and ends on the child's left edge, at its middle.
      expect(sy).toBe(from.y + from.height);
      expect(sx).toBeGreaterThan(from.x);
      expect(sx).toBeLessThan(from.x + from.width);
      const points = [{ x: sx!, y: sy! }];
      for (const step of steps) {
        const last = points[points.length - 1]!;
        const value = Number(step.slice(2));
        points.push(step.startsWith("H") ? { x: value, y: last.y } : { x: last.x, y: value });
      }
      expect(points[points.length - 1]).toEqual({ x: to.x, y: to.y + to.height / 2 });
      for (let i = 1; i < points.length; i++) {
        const [p, q] = [points[i - 1]!, points[i]!];
        for (const node of nodes) {
          const crossesX = Math.max(p.x, q.x) > node.x && Math.min(p.x, q.x) < node.x + node.width;
          const crossesY = Math.max(p.y, q.y) > node.y && Math.min(p.y, q.y) < node.y + node.height;
          expect(crossesX && crossesY, `${edge.id} passes behind ${node.entry.key}`).toBe(false);
        }
      }
    }
    // Siblings in the next column share the parent's drop and trunk: `M … V … H trunkX`.
    const trunk = (key: string) => {
      const path = tree.find((edge) => edge.to === nodeId(key))!.path;
      return path.slice(0, path.indexOf(" V ", path.indexOf(" H ")));
    };
    expect(trunk("a")).toBe(trunk("d"));
  });

  it("ranks blockers left of what they block and survives dependency cycles", () => {
    const chain = layoutIssueGraph(
      [
        issue("c", { issueType: "task", dependsOnKeys: ["b", "a"] }),
        issue("b", { issueType: "task", dependsOnKeys: ["a"] }),
        issue("a", { issueType: "task" }),
        issue("side", { issueType: "task", dependsOnKeys: ["a"] }),
      ],
      "type",
    );
    expect(nodeOf(chain, "a").x).toBeLessThan(nodeOf(chain, "b").x);
    expect(nodeOf(chain, "b").x).toBeLessThan(nodeOf(chain, "c").x);
    expect(nodeOf(chain, "side").x).toBe(nodeOf(chain, "b").x);
    // a -> c skips b's column: it turns inside the column gaps and crosses above b's card.
    const skip = edgesOf(chain).find(
      (edge) => edge.from === nodeId("a") && edge.to === nodeId("c"),
    )!;
    const b = nodeOf(chain, "b");
    const [, riseEndX, crossY, crossEndX] = skip.path
      .match(/, ([\d.-]+) ([\d.-]+) L ([\d.-]+) [\d.-]+ C/)!
      .map(Number);
    expect(riseEndX).toBeLessThanOrEqual(b.x);
    expect(crossEndX).toBeGreaterThanOrEqual(b.x + b.width);
    expect(crossY).toBeLessThan(b.y);

    const cycle = [
      issue("x", { dependsOnKeys: ["z"], parentKey: "y" }),
      issue("y", { dependsOnKeys: ["x"], parentKey: "z" }),
      issue("z", { dependsOnKeys: ["y"], parentKey: "y" }),
    ];
    const first = layoutIssueGraph(cycle, "tree");
    const firstNodes = nodesOf(first);
    expect(firstNodes).toHaveLength(3);
    expect(new Set(firstNodes.map((node) => node.x)).size).toBe(3);
    // A backward edge crosses in the row gap too, above the card it passes, inside the region.
    const back = edgesOf(first).find((edge) => {
      const from = firstNodes.find((node) => node.id === edge.from)!;
      const to = firstNodes.find((node) => node.id === edge.to)!;
      return to.x < from.x - from.width;
    })!;
    const backY = Number(back.path.match(/ L [\d.-]+ ([\d.-]+) C/)![1]);
    expect(backY).toBeLessThan(Math.min(...firstNodes.map((node) => node.y)));
    expect(backY).toBeGreaterThan(0);
    expect(layoutIssueGraph(cycle, "tree")).toEqual(first);
  });

  it("sizes each region to its cards plus padding, and widens it for an edge past the last column", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("one", { parentKey: "epic" }),
        issue("two", { parentKey: "epic" }),
        issue("bug-1", { issueType: "bug" }),
      ],
      "tree",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    const [tree, loose] = regionsOf(layout);
    // epic in column 0; one and two stacked below it in column 1: three rows of 40 with 12 gaps.
    expect(tree!.height).toBe(12 + 40 + 12 + 40 + 12 + 40 + 12);
    expect(Math.min(...tree!.nodes.map((node) => node.y))).toBe(12);
    expect(Math.max(...tree!.nodes.map((node) => node.y + node.height))).toBe(tree!.height - 12);
    expect(tree!.width).toBe(12 + 100 + 64 + 100 + 12);
    expect(loose).toMatchObject({ height: 12 + 40 + 12, width: 12 + 100 + 12, edges: [] });

    // A card with a blocked-by line is taller, and so is its row.
    const labelled = regionsOf(
      layoutIssueGraph(
        [
          issue("bug-1", { issueType: "bug", dependsOnKeys: ["elsewhere"] }),
          issue("bug-2", { issueType: "bug" }),
        ],
        "tree",
        { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
      ),
    )[0]!;
    expect(labelled.nodes.map((node) => node.height)).toEqual([56, 40]);
    expect(labelled.height).toBe(12 + 56 + 12);

    // A backward edge leaves the last column to its right and enters the first from its left;
    // the region makes room on both sides.
    const cycle = regionsOf(
      layoutIssueGraph(
        [
          issue("x", { dependsOnKeys: ["z"] }),
          issue("y", { dependsOnKeys: ["x"] }),
          issue("z", { dependsOnKeys: ["y"] }),
        ],
        "tree",
        { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
      ),
    )[0]!;
    const cardsRight = Math.max(...cycle.nodes.map((node) => node.x + node.width));
    const pathRight = Math.max(
      ...cycle.edges.flatMap((edge) =>
        [...edge.path.matchAll(/(-?[\d.]+) -?[\d.]+/g)].map((match) => Number(match[1])),
      ),
    );
    const pathLeft = Math.min(
      ...cycle.edges.flatMap((edge) =>
        [...edge.path.matchAll(/(-?[\d.]+) -?[\d.]+/g)].map((match) => Number(match[1])),
      ),
    );
    expect(pathRight).toBeGreaterThan(cardsRight);
    expect(cycle.width).toBe(pathRight + 12);
    expect(pathLeft).toBe(12);
  });

  it("groups regions under one row per project, keeping a shared scope in the project that listed it first", () => {
    const inProject = (entry: EnvironmentIssueEntry, project: string, title: string) => ({
      ...entry,
      projectId: ProjectId.make(project),
      projectTitle: title,
    });
    const github = (entry: EnvironmentIssueEntry) => ({
      ...entry,
      tracker: IssueTrackerKind.make("github"),
      host: "github.com",
      scope: "owner/repo",
    });
    const layout = layoutIssueGraph(
      [
        inProject({ ...issue("bug-1", { issueType: "bug" }), scope: "zed" }, "zed", "zed"),
        inProject(github(issue("7")), "zed", "zed"),
        inProject(issue("epic", { issueType: "epic" }), "fork", "Fork"),
        // The t3 beads scope and the GitHub repo were listed first by Fork and zed.
        inProject(issue("child", { parentKey: "epic" }), "app", "app"),
        inProject({ ...issue("bug-2", { issueType: "bug" }), scope: "app" }, "app", "app"),
        inProject(github(issue("8")), "app", "app"),
      ],
      "tree",
    );
    expect(
      layout.map((project) => ({
        id: project.id,
        title: project.title,
        issueCount: project.issueCount,
        regions: project.regions.map((region) => [
          region.id,
          region.nodes.map((node) => node.entry.key),
        ]),
      })),
    ).toEqual([
      {
        id: "env-1:app",
        title: "app",
        issueCount: 1,
        regions: [["env-1:app:loose:bug", ["bug-2"]]],
      },
      {
        id: "env-1:fork",
        title: "Fork",
        issueCount: 2,
        regions: [["env-1:fork:tree:env-1:fork\u0000local\u0000t3\u0000epic", ["epic", "child"]]],
      },
      {
        id: "env-1:zed",
        title: "zed",
        issueCount: 3,
        regions: [
          ["env-1:zed:loose:bug", ["bug-1"]],
          ["env-1:zed:loose:", ["7", "8"]],
        ],
      },
    ]);
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
    expect(regionsOf(layoutIssueGraph(entries, "type")).map((c) => [c.kind, c.label])).toEqual([
      ["type", "epic"],
      ["type", "bug"],
      ["type", "chore"],
      ["type", "alpha"],
      ["type", "zeta"],
      ["type", ""],
    ]);
    const grid = nodesOf(
      layoutIssueGraph(
        entries.map((entry) => ({ ...entry, issueType: "task" })),
        "type",
      ),
    );
    expect({
      columns: new Set(grid.map((node) => node.x)).size,
      rows: new Set(grid.map((node) => node.y)).size,
    }).toEqual({ columns: 4, rows: 2 });
    // Unlinked grid columns sit close together; only ranked columns leave room for edges.
    const gridXs = [...new Set(grid.map((node) => node.x))].toSorted((a, b) => a - b);
    expect(gridXs[1]! - gridXs[0]!).toBe(grid[0]!.width + 16);
    expect(regionsOf(layoutIssueGraph(entries, "status")).map((c) => c.label)).toEqual([
      "open",
      "in_progress",
      "blocked",
      "closed",
    ]);
  });
});
