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

/** `M x1 y1 H … Q gx … V … Q gx y2 … H x2`: an arc out to the gutter at gx and back. */
function arcOf(path: string) {
  expect(path).toMatch(/^M [\d.-]+ [\d.-]+ H [\d.-]+ Q .+ V .+ Q .+ H [\d.-]+$/);
  const n = path.match(/-?[\d.]+/g)!.map(Number);
  return {
    x1: n[0]!,
    y1: n[1]!,
    gutter: n[3]!,
    top: Math.min(n[1]!, n[9]!),
    bottom: Math.max(n[1]!, n[9]!),
    y2: n[9]!,
    x2: n[12]!,
  };
}

/** `M x top V bottom H x2`: a trunk at x from the parent's bottom, then a tick into the child. */
function treeOf(path: string) {
  const match = path.match(/^M ([\d.-]+) ([\d.-]+) V ([\d.-]+) H ([\d.-]+)$/);
  expect(match, path).not.toBeNull();
  const [, x, top, bottom, end] = match!.map(Number);
  return { x: x!, top: top!, bottom: bottom!, end: end! };
}

/** Every arc end on each card's right edge keeps more than an arrowhead from the next. */
function expectSpacedEnds(layout: Layout) {
  const byId = new Map(nodesOf(layout).map((node) => [node.id, node]));
  const ends = new Map<string, Array<number>>();
  for (const edge of edgesOf(layout).filter((e) => e.kind === "blocks")) {
    const arc = arcOf(edge.path);
    const [from, to] = [byId.get(edge.from)!, byId.get(edge.to)!];
    expect(arc.x1).toBe(from.x + from.width);
    expect(arc.x2).toBe(to.x + to.width);
    ends.set(from.id, [...(ends.get(from.id) ?? []), arc.y1]);
    ends.set(to.id, [...(ends.get(to.id) ?? []), arc.y2]);
  }
  for (const [id, ys] of ends) {
    const node = byId.get(id)!;
    const sorted = ys.toSorted((a, b) => a - b);
    expect(sorted[0]).toBeGreaterThan(node.y);
    expect(sorted[sorted.length - 1]).toBeLessThan(node.y + node.height);
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i]! - sorted[i - 1]!).toBeGreaterThanOrEqual(10);
    }
  }
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
      if (edge.kind === "blocks") arcOf(edge.path);
      else treeOf(edge.path);
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

  it("lists linked issues depth-first, each sub-task indented under its parent and joined by a trunk on the left", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("b", { parentKey: "epic" }),
        issue("a", { parentKey: "epic", priority: 1 }),
        issue("b1", { parentKey: "b" }),
        issue("a1", { parentKey: "a" }),
        issue("a2", { parentKey: "a" }),
        issue("a1x", { parentKey: "a1" }),
      ],
      "tree",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    const region = regionsOf(layout)[0]!;
    expect(region.nodes.map((node) => [node.entry.key, node.x])).toEqual([
      ["epic", 12],
      ["a", 36],
      ["a1", 60],
      ["a1x", 84],
      ["a2", 60],
      ["b", 36],
      ["b1", 60],
    ]);
    const ys = region.nodes.map((node) => node.y);
    expect(ys).toEqual(ys.toSorted((a, b) => a - b));
    expect(new Set(ys).size).toBe(ys.length);
    // One card per row of 40, 12 apart, inside 12 of padding; nothing scrolls sideways.
    expect(region.height).toBe(12 + 7 * 40 + 6 * 12 + 12);
    expect(region.width).toBe(84 + 100 + 12);

    const byId = new Map(region.nodes.map((node) => [node.id, node]));
    const trunks = region.edges.map((edge) => {
      expect(edge.kind).toBe("parent");
      const [from, to] = [byId.get(edge.from)!, byId.get(edge.to)!];
      const tree = treeOf(edge.path);
      // Under the parent's left edge, left of every card below it, ticking into the child.
      expect(tree.x).toBeGreaterThan(from.x);
      expect(tree.x).toBeLessThan(to.x);
      expect(tree.top).toBe(from.y + from.height);
      expect(tree.bottom).toBeGreaterThan(to.y);
      expect(tree.bottom).toBeLessThan(to.y + to.height);
      expect(tree.end).toBe(to.x);
      return { parent: edge.from, ...tree };
    });
    expect(trunks).toHaveLength(6);
    // Siblings share their parent's trunk; trunks of different parents never overlap.
    for (const a of trunks) {
      for (const b of trunks) {
        if (a.parent === b.parent) expect(a.x).toBe(b.x);
        else if (a.x === b.x) expect(a.bottom < b.top || b.bottom < a.top).toBe(true);
      }
    }
  });

  it("puts a blocker above the card it blocks, and lays a cycle out the same way every time", () => {
    const siblings = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("urgent", { parentKey: "epic", priority: 0, dependsOnKeys: ["first"] }),
        issue("first", { parentKey: "epic", priority: 3 }),
        issue("other", { parentKey: "epic", priority: 1 }),
      ],
      "tree",
    );
    expect(regionsOf(siblings)[0]!.nodes.map((node) => node.entry.key)).toEqual([
      "epic",
      "other",
      "first",
      "urgent",
    ]);

    const cycle = [
      issue("x", { dependsOnKeys: ["z"], parentKey: "y" }),
      issue("y", { dependsOnKeys: ["x"], parentKey: "z" }),
      issue("z", { dependsOnKeys: ["y"], parentKey: "y" }),
      issue("p", { dependsOnKeys: ["q"] }),
      issue("q", { dependsOnKeys: ["p"] }),
    ];
    const first = layoutIssueGraph(cycle, "tree");
    expect(nodesOf(first)).toHaveLength(5);
    for (const region of regionsOf(first)) {
      expect(new Set(region.nodes.map((node) => node.y)).size).toBe(region.nodes.length);
    }
    expect(layoutIssueGraph(cycle, "tree")).toEqual(first);
    expectSpacedEnds(first);
  });

  it("nests arcs in the right gutter by span, so overlapping spans never share a segment", () => {
    const layout = layoutIssueGraph(
      [
        issue("closed", { issueType: "task", closed: true }),
        issue("open", { issueType: "task" }),
        issue("mid", { issueType: "task", dependsOnKeys: ["closed"] }),
        issue("target", { issueType: "task", dependsOnKeys: ["closed", "open", "mid"] }),
        issue("side", { issueType: "task", dependsOnKeys: ["open"] }),
        issue("alone", { issueType: "task" }),
      ],
      "type",
    );
    const region = regionsOf(layout)[0]!;
    const nodes = region.nodes;
    const cardsRight = Math.max(...nodes.map((node) => node.x + node.width));
    const arcs = region.edges.map((edge) => ({ edge, ...arcOf(edge.path) }));
    expect(arcs).toHaveLength(5);
    for (const a of arcs) {
      expect(a.gutter).toBeGreaterThan(cardsRight);
      expect(region.width).toBeGreaterThanOrEqual(a.gutter + 12);
      for (const b of arcs) {
        if (a === b || a.bottom < b.top || b.bottom < a.top) continue;
        expect(a.gutter, `${a.edge.id} and ${b.edge.id}`).not.toBe(b.gutter);
        // A span inside another nests inside its arc.
        if (b.top <= a.top && a.bottom <= b.bottom) expect(a.gutter).toBeLessThan(b.gutter);
      }
    }
    expectSpacedEnds(layout);
    // The unlinked card waits in the grid below the list.
    const alone = nodeOf(layout, "alone");
    expect(alone.y).toBeGreaterThan(
      Math.max(...nodes.filter((node) => node !== alone).map((node) => node.y + node.height)),
    );
  });

  it("grows a card's row to keep many arc ends apart, and a blocked-by line's row", () => {
    const blockers = Array.from({ length: 8 }, (_, i) => issue(`b${i}`, { issueType: "task" }));
    const layout = layoutIssueGraph(
      [
        ...blockers,
        issue("hub", { issueType: "task", dependsOnKeys: blockers.map((entry) => entry.key) }),
        issue("bug-1", { issueType: "bug", dependsOnKeys: ["elsewhere"] }),
      ],
      "type",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    expect(nodeOf(layout, "hub").height).toBe(7 * 12 + 2 * 8);
    expect(nodeOf(layout, "b0").height).toBe(40);
    expectSpacedEnds(layout);
    expect(nodeOf(layout, "bug-1").height).toBe(56);
    const bugs = regionsOf(layout).find((region) => region.label === "bug")!;
    expect(bugs.height).toBe(12 + 56 + 12);
  });

  it("lays out a dense region of 200 issues and about 3,800 links in one list", () => {
    const entries = Array.from({ length: 200 }, (_, i) =>
      issue(`t${i}`, {
        issueType: "task",
        dependsOnKeys: Array.from({ length: Math.min(i, 20) }, (_, k) => `t${i - k - 1}`),
      }),
    );
    const layout = layoutIssueGraph(entries, "type");
    const region = regionsOf(layout)[0]!;
    expect(region.nodes).toHaveLength(200);
    expect(region.edges).toHaveLength(3790);
    const y = new Map(region.nodes.map((node) => [node.id, node.y]));
    expect(new Set(y.values()).size).toBe(200);
    for (const edge of region.edges) expect(y.get(edge.from)!).toBeLessThan(y.get(edge.to)!);
    expectSpacedEnds(layout);
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
