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

interface Point {
  readonly x: number;
  readonly y: number;
}
interface Segment {
  readonly from: Point;
  readonly to: Point;
}

/** The legs of a path made only of M, H and V commands. */
function orthogonalSegments(path: string): Array<Segment> {
  expect(path).toMatch(/^M [^CLQAS]+$/);
  const [start, ...steps] = path.match(/[MHV] [^MHV]+/g)!.map((step) => step.trim());
  const [x, y] = start!.slice(2).split(" ").map(Number);
  let at: Point = { x: x!, y: y! };
  return steps.map((step) => {
    const value = Number(step.slice(2));
    const next = step.startsWith("H") ? { x: value, y: at.y } : { x: at.x, y: value };
    const segment = { from: at, to: next };
    at = next;
    return segment;
  });
}

/** Two collinear legs that share more than a point. */
function overlaps(a: Segment, b: Segment): boolean {
  const span = (s: Segment, axis: "x" | "y") =>
    [Math.min(s.from[axis], s.to[axis]), Math.max(s.from[axis], s.to[axis])] as const;
  for (const [along, across] of [
    ["x", "y"],
    ["y", "x"],
  ] as const) {
    if (a.from[across] !== a.to[across] || b.from[across] !== b.to[across]) continue;
    if (a.from[across] !== b.from[across]) continue;
    const [a0, a1] = span(a, along);
    const [b0, b1] = span(b, along);
    if (Math.min(a1, b1) - Math.max(a0, b0) > 0) return true;
  }
  return false;
}

function arrowEnd(path: string): Point {
  const numbers = path.match(/-?[\d.]+/g)!.map(Number);
  return { x: numbers[numbers.length - 2]!, y: numbers[numbers.length - 1]! };
}

/** The straight crossing of a blocks curve that skips a column: `…, x1 y L x2 y C …`. */
function arrowCrossing(path: string): Segment | null {
  const match = path.match(/, ([\d.-]+) ([\d.-]+) L ([\d.-]+) ([\d.-]+) C/);
  if (!match) return null;
  const [, x1, y1, x2, y2] = match.map(Number);
  return { from: { x: x1!, y: y1! }, to: { x: x2!, y: y2! } };
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
        issue("a1", { parentKey: "a" }),
        issue("a2", { parentKey: "a" }),
        issue("b", { parentKey: "epic" }),
        issue("b1", { parentKey: "b" }),
        issue("c", { parentKey: "epic" }),
        // Blocked by a sibling, so each sits two columns right of its parent.
        issue("far1", { parentKey: "epic", dependsOnKeys: ["a"] }),
        issue("far2", { parentKey: "epic", dependsOnKeys: ["c"] }),
      ],
      "tree",
    );
    const nodes = nodesOf(layout);
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const tree = edgesOf(layout).filter((edge) => edge.kind === "parent");
    expect(tree).toHaveLength(8);
    const legs = new Map<string, Array<Segment>>();
    for (const edge of tree) {
      const from = byId.get(edge.from)!;
      const to = byId.get(edge.to)!;
      expect(to.x).toBeGreaterThan(from.x);
      expect(to.y).toBeGreaterThan(from.y + from.height);

      const segments = orthogonalSegments(edge.path);
      legs.set(edge.id, segments);
      const start = segments[0]!.from;
      const end = segments[segments.length - 1]!.to;
      // Leaves the parent's bottom edge and ends on the child's left edge.
      expect(start.y).toBe(from.y + from.height);
      expect(start.x).toBeGreaterThan(from.x);
      expect(start.x).toBeLessThan(from.x + from.width);
      expect(end.x).toBe(to.x);
      expect(end.y).toBeGreaterThan(to.y);
      expect(end.y).toBeLessThan(to.y + to.height);
      for (const { from: p, to: q } of segments) {
        for (const node of nodes) {
          const overX = Math.max(p.x, q.x) > node.x && Math.min(p.x, q.x) < node.x + node.width;
          const overY = Math.max(p.y, q.y) > node.y && Math.min(p.y, q.y) < node.y + node.height;
          expect(overX && overY, `${edge.id} passes behind ${node.entry.key}`).toBe(false);
          // Crossing a column, it keeps a row gap clear of the cards there, so it never reads
          // as a link between two of them.
          if (
            p.y === q.y &&
            Math.min(p.x, q.x) <= node.x &&
            Math.max(p.x, q.x) >= node.x + node.width
          ) {
            const clearance = Math.max(node.y - p.y, p.y - (node.y + node.height));
            expect(clearance, `${edge.id} hugs ${node.entry.key}`).toBeGreaterThanOrEqual(16);
          }
        }
      }
    }
    // Different parents never share a leg, so one branch cannot look attached to another.
    for (const a of tree) {
      for (const b of tree) {
        if (a.from === b.from) continue;
        for (const p of legs.get(a.id)!) {
          for (const q of legs.get(b.id)!) {
            expect(overlaps(p, q), `${a.id} shares a leg with ${b.id}`).toBe(false);
          }
        }
      }
    }
    // Siblings in the next column share the parent's drop and trunk: `M … V … H trunkX`.
    const trunk = (key: string) => {
      const path = tree.find((edge) => edge.to === nodeId(key))!.path;
      return path.slice(0, path.indexOf(" V ", path.indexOf(" H ")));
    };
    expect(trunk("a1")).toBe(trunk("a2"));

    // A child that blocks its own parent still sits right of and below it; the dependency is the
    // link drawn backward.
    const loop = layoutIssueGraph(
      [
        issue("kid", { parentKey: "boss", priority: 0 }),
        issue("boss", { issueType: "epic", dependsOnKeys: ["kid"] }),
      ],
      "tree",
    );
    const [kid, boss] = [nodeOf(loop, "kid"), nodeOf(loop, "boss")];
    expect(kid.x).toBeGreaterThan(boss.x);
    expect(kid.y).toBeGreaterThan(boss.y);
    const back = edgesOf(loop).find((edge) => edge.kind === "blocks")!;
    expect(back.path).toMatch(/ L /);
    expect(arrowEnd(back.path).x).toBe(boss.x);
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

    // Arrows into one card enter at their own points, on their own tracks, so an open arrow
    // never hides a closed one.
    const fan = layoutIssueGraph(
      [
        issue("closed", { issueType: "task", closed: true }),
        issue("open", { issueType: "task" }),
        issue("mid", { issueType: "task", dependsOnKeys: ["closed"] }),
        issue("target", { issueType: "task", dependsOnKeys: ["closed", "open", "mid"] }),
      ],
      "type",
    );
    const target = nodeOf(fan, "target");
    const into = edgesOf(fan).filter((edge) => edge.to === nodeId("target"));
    expect(into).toHaveLength(3);
    const ends = into.map((edge) => arrowEnd(edge.path));
    for (const end of ends) {
      expect(end.x).toBe(target.x);
      expect(end.y).toBeGreaterThan(target.y);
      expect(end.y).toBeLessThan(target.y + target.height);
    }
    expect(new Set(ends.map((end) => end.y)).size).toBe(3);
    const crossings = into.flatMap((edge) => arrowCrossing(edge.path) ?? []);
    expect(crossings).toHaveLength(2);
    expect(overlaps(crossings[0]!, crossings[1]!)).toBe(false);
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
    // epic in column 0; one and two stacked below it in column 1: three rows of 40 with 16 gaps,
    // inside 16 of padding.
    expect(tree!.height).toBe(16 + 40 + 16 + 40 + 16 + 40 + 16);
    expect(Math.min(...tree!.nodes.map((node) => node.y))).toBe(16);
    expect(Math.max(...tree!.nodes.map((node) => node.y + node.height))).toBe(tree!.height - 16);
    expect(tree!.width).toBe(16 + 100 + 64 + 100 + 16);
    expect(loose).toMatchObject({ height: 16 + 40 + 16, width: 16 + 100 + 16, edges: [] });

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
    expect(labelled.height).toBe(16 + 56 + 16);

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
    expect(cycle.width).toBe(pathRight + 16);
    expect(pathLeft).toBe(16);
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
