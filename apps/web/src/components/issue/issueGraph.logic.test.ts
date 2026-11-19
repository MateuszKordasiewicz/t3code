import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { EnvironmentId, IssueTrackerKind, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { layoutIssueGraph, type IssueGraphNode, type IssueGraphProject } from "./issueGraph.logic";

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

const NUMBER = String.raw`-?[\d.]+`;

/** `M x y H x2`: a straight chain arrow. */
function chainOf(path: string) {
  const match = path.match(new RegExp(`^M (${NUMBER}) (${NUMBER}) H (${NUMBER})$`));
  expect(match, path).not.toBeNull();
  const [, x1, y, x2] = match!.map(Number);
  return { x1: x1!, y: y!, x2: x2! };
}

/**
 * `M x1 y1 H … Q x … V … Q x y2 … H x2`: out to a vertical at x and back in. A bent arrow ends on
 * the blocked card's left edge, an arc on its right edge.
 */
function legsOf(path: string) {
  const shape = new RegExp(`^M ${NUMBER} ${NUMBER} H ${NUMBER} Q .+ V ${NUMBER} Q .+ H ${NUMBER}$`);
  expect(path).toMatch(shape);
  const n = path.match(new RegExp(NUMBER, "g"))!.map(Number);
  return {
    x1: n[0]!,
    y1: n[1]!,
    x: n[3]!,
    y2: n[9]!,
    x2: n[12]!,
    top: Math.min(n[1]!, n[9]!),
    bottom: Math.max(n[1]!, n[9]!),
  };
}

/** `M x top V bottom H x2`: a trunk at x from the parent's bottom, then a tick into the child. */
function treeOf(path: string) {
  const match = path.match(new RegExp(`^M (${NUMBER}) (${NUMBER}) V (${NUMBER}) H (${NUMBER})$`));
  expect(match, path).not.toBeNull();
  const [, x, top, bottom, end] = match!.map(Number);
  return { x: x!, top: top!, bottom: bottom!, end: end! };
}

function endsOf(path: string) {
  const n = path.match(new RegExp(NUMBER, "g"))!.map(Number);
  const last = path.match(/H (-?[\d.]+)$/)!;
  return { start: { x: n[0]!, y: n[1]! }, end: { x: Number(last[1]), y: n[n.length - 2]! } };
}

/** Every link end on each card edge keeps more than an arrowhead from the next. */
function expectSpacedEnds(layout: Layout) {
  const byId = new Map(nodesOf(layout).map((node) => [node.id, node]));
  const ends = new Map<string, Array<number>>();
  const add = (key: string, y: number) => ends.set(key, [...(ends.get(key) ?? []), y]);
  for (const edge of edgesOf(layout).filter((e) => e.kind === "blocks")) {
    const [from, to] = [byId.get(edge.from)!, byId.get(edge.to)!];
    const { start, end } = endsOf(edge.path);
    expect(start.x).toBe(from.x + from.width);
    expect([to.x, to.x + to.width]).toContain(end.x);
    add(`${from.id} right`, start.y);
    add(`${to.id} ${end.x === to.x ? "left" : "right"}`, end.y);
  }
  for (const [key, ys] of ends) {
    const node = byId.get(key.slice(0, key.lastIndexOf(" ")))!;
    const sorted = ys.toSorted((a, b) => a - b);
    expect(sorted[0]).toBeGreaterThan(node.y);
    expect(sorted[sorted.length - 1]).toBeLessThan(node.y + node.height);
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i]! - sorted[i - 1]!, key).toBeGreaterThanOrEqual(10);
    }
  }
}

/** Cards grouped by the row they sit on, top to bottom. */
function rowsOf(nodes: ReadonlyArray<IssueGraphNode>) {
  const rows = new Map<number, Array<IssueGraphNode>>();
  for (const node of nodes) rows.set(node.y, [...(rows.get(node.y) ?? []), node]);
  return [...rows.values()];
}

const edgeBetween = (layout: Layout, from: string, to: string) =>
  edgesOf(layout).find((edge) => edge.from === nodeId(from) && edge.to === nodeId(to));

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
        { kind: "parent", from: nodeId("epic"), to: nodeId("fixed"), blockerOpen: null },
      ]),
    );
    // two and three follow their blockers on the same row, so no tree tick runs into them; two ->
    // loose crosses into the loose region, which scrolls on its own: not drawn.
    expect(edgesOf(layout)).toHaveLength(4);
    for (const edge of edgesOf(layout)) {
      if (edge.kind === "blocks") chainOf(edge.path);
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

  it("chains blockers to the right on one row, with straight arrows and no tree tick into followers", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("d13", { parentKey: "epic" }),
        issue("d11", { parentKey: "epic" }),
        issue("d12", { parentKey: "epic", dependsOnKeys: ["d11"] }),
        issue("d3", { parentKey: "epic" }),
        issue("d4", { parentKey: "epic", dependsOnKeys: ["d3"] }),
        issue("d5", { parentKey: "epic", dependsOnKeys: ["d4"] }),
        issue("d9", { parentKey: "epic" }),
      ],
      "tree",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    const region = regionsOf(layout)[0]!;
    // [epic] / ├─[d13] / ├─[d11]──▶[d12] / ├─[d3]──▶[d4]──▶[d5] / └─[d9]
    expect(region.nodes.map((node) => [node.entry.key, node.x, node.y])).toEqual([
      ["epic", 12, 12],
      ["d13", 36, 64],
      ["d11", 36, 116],
      ["d12", 168, 116],
      ["d3", 36, 168],
      ["d4", 168, 168],
      ["d5", 300, 168],
      ["d9", 36, 220],
    ]);
    expect(region.height).toBe(12 + 5 * 40 + 4 * 12 + 12);
    expect(region.width).toBe(300 + 100 + 12);
    for (const [from, to] of [
      ["d11", "d12"],
      ["d3", "d4"],
      ["d4", "d5"],
    ] as const) {
      const [a, b] = [nodeOf(layout, from), nodeOf(layout, to)];
      expect(chainOf(edgeBetween(layout, from, to)!.path)).toEqual({
        x1: a.x + a.width,
        y: a.y + a.height / 2,
        x2: b.x,
      });
    }
    // Ticks only into the cards that start a row, off the epic's trunk on the left.
    const ticks = region.edges.filter((edge) => edge.kind === "parent");
    expect(ticks.map((edge) => edge.to).toSorted()).toEqual(
      ["d11", "d13", "d3", "d9"].map(nodeId).toSorted(),
    );
    for (const edge of ticks) {
      const tree = treeOf(edge.path);
      expect(tree.x).toBe(12 + 12);
      expect(tree.end).toBe(36);
    }
  });

  it("fans out and in through the gap before the blocked card, one track per bent arrow", () => {
    const fanOut = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("b", { parentKey: "epic" }),
        issue("x", { parentKey: "epic", dependsOnKeys: ["b"] }),
        issue("y", { parentKey: "epic", dependsOnKeys: ["b"] }),
      ],
      "tree",
    );
    const [epic, b, x, y] = ["epic", "b", "x", "y"].map((key) => nodeOf(fanOut, key));
    expect(x!.y).toBe(b!.y);
    // y gets its own row, lined up under x.
    expect(y!.x).toBe(x!.x);
    expect(y!.y).toBeGreaterThan(b!.y + b!.height);
    expect(edgeBetween(fanOut, "epic", "x")).toBeUndefined();
    const out = legsOf(edgeBetween(fanOut, "b", "y")!.path);
    expect(out.x1).toBe(b!.x + b!.width);
    expect(out.y1).toBeGreaterThan(b!.y + b!.height / 2);
    expect(out.x).toBeGreaterThan(b!.x + b!.width);
    expect(out.x).toBeLessThan(y!.x);
    expect(out.x2).toBe(y!.x);
    // A row that starts further right still hangs off its parent's trunk: the tick runs along
    // the row into y's left edge, below the bent arrow's entry.
    const tick = treeOf(edgeBetween(fanOut, "epic", "y")!.path);
    expect(tick).toMatchObject({ x: epic!.x + 12, end: y!.x });
    expect(tick.bottom).toBeGreaterThan(out.y2);
    expect(tick.bottom).toBeLessThan(y!.y + y!.height);
    expectSpacedEnds(fanOut);

    // With a sub-task under x, the arrow to y falls back to y's label, and y keeps its tick. A
    // third sibling's arrow would cross y's tick, so it falls back to its label too.
    const crowded = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("b", { parentKey: "epic" }),
        issue("x", { parentKey: "epic", dependsOnKeys: ["b"] }),
        issue("x1", { parentKey: "x" }),
        issue("y", { parentKey: "epic", dependsOnKeys: ["b"] }),
      ],
      "tree",
    );
    expect(nodeOf(crowded, "y").externalBlockers?.keys).toEqual(["b"]);
    expect(treeOf(edgeBetween(crowded, "epic", "y")!.path).end).toBe(nodeOf(crowded, "y").x);
    const three = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("b", { parentKey: "epic" }),
        issue("x", { parentKey: "epic", dependsOnKeys: ["b"] }),
        issue("y", { parentKey: "epic", dependsOnKeys: ["b"] }),
        issue("z", { parentKey: "epic", dependsOnKeys: ["b"] }),
      ],
      "tree",
    );
    const ticks = edgesOf(three)
      .filter((edge) => edge.kind === "parent")
      .map((edge) => treeOf(edge.path));
    // b, y and z start rows; x follows b on its row.
    expect(ticks).toHaveLength(3);
    for (const edge of edgesOf(three).filter((e) => e.kind === "blocks" && e.path.includes("Q"))) {
      const bend = legsOf(edge.path);
      for (const t of ticks) {
        const crosses =
          t.x < bend.x && bend.x < t.end && bend.top < t.bottom && t.bottom < bend.bottom;
        expect(crosses, `${edge.id} crosses a tick`).toBe(false);
      }
    }
    expect(nodeOf(three, "z").externalBlockers?.keys).toEqual(["b"]);

    const fanIn = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        ...["p1", "p2", "p3", "p4", "p5"].map((key) => issue(key, { parentKey: "epic" })),
        issue("r", { parentKey: "epic", dependsOnKeys: ["p1", "p2", "p3", "p4", "p5"] }),
      ],
      "tree",
    );
    const r = nodeOf(fanIn, "r");
    const p5 = nodeOf(fanIn, "p5");
    // r follows the blocker last in row order; the others bend in above the chain arrow.
    expect(r.y).toBe(p5.y);
    expect(chainOf(edgeBetween(fanIn, "p5", "r")!.path).y).toBe(r.y + r.height / 2);
    const bends = ["p1", "p2", "p3", "p4"].map((key) => legsOf(edgeBetween(fanIn, key, "r")!.path));
    for (const bend of bends) {
      expect(bend.x).toBeGreaterThan(p5.x + p5.width);
      expect(bend.x).toBeLessThan(r.x);
      expect(bend.x2).toBe(r.x);
      expect(bend.y2).toBeLessThan(r.y + r.height / 2);
    }
    // Each its own track; four tracks need more than the usual 32px gap.
    expect(new Set(bends.map((bend) => bend.x)).size).toBe(4);
    expect(r.x - (p5.x + p5.width)).toBeGreaterThan(32);
    expectSpacedEnds(fanIn);
  });

  it("draws a cousin dependency as a right-hand arc, and leaves the row right of its ends free", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("a", { parentKey: "epic" }),
        issue("a1", { parentKey: "a" }),
        issue("a2", { parentKey: "a", dependsOnKeys: ["a1"] }),
        issue("b", { parentKey: "epic" }),
        issue("b1", { parentKey: "b", dependsOnKeys: ["a1"] }),
      ],
      "tree",
    );
    const region = regionsOf(layout)[0]!;
    const cardsRight = Math.max(...region.nodes.map((node) => node.x + node.width));
    const [a1, a2, b1] = ["a1", "a2", "b1"].map((key) => nodeOf(layout, key));
    const arc = legsOf(edgeBetween(layout, "a1", "b1")!.path);
    expect(arc.x1).toBe(a1!.x + a1!.width);
    expect(arc.x2).toBe(b1!.x + b1!.width);
    expect(arc.x).toBeGreaterThan(cardsRight);
    expect(region.width).toBeGreaterThanOrEqual(arc.x + 12);
    // a1's right edge carries the arc, so a2 takes its own row and bends in.
    expect(a2!.y).toBeGreaterThan(a1!.y);
    expect(legsOf(edgeBetween(layout, "a1", "a2")!.path).x2).toBe(a2!.x);
    expectSpacedEnds(layout);
  });

  it("indents a follower's own sub-tasks from its x, under a trunk from its bottom", () => {
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("a", { parentKey: "epic" }),
        issue("b", { parentKey: "epic", dependsOnKeys: ["a"] }),
        issue("b1", { parentKey: "b" }),
        issue("b2", { parentKey: "b" }),
        issue("c", { parentKey: "epic" }),
      ],
      "tree",
    );
    const region = regionsOf(layout)[0]!;
    expect(region.nodes.map((node) => node.entry.key)).toEqual(["epic", "a", "b", "b1", "b2", "c"]);
    const b = nodeOf(layout, "b");
    expect(b.y).toBe(nodeOf(layout, "a").y);
    for (const key of ["b1", "b2"]) {
      const child = nodeOf(layout, key);
      expect(child.x).toBe(b.x + 24);
      const tree = treeOf(edgeBetween(layout, "b", key)!.path);
      expect(tree).toMatchObject({ x: b.x + 12, top: b.y + b.height, end: child.x });
    }
    // Trunks of different parents never overlap.
    const trunks = region.edges.filter((edge) => edge.kind === "parent");
    for (const one of trunks) {
      for (const other of trunks) {
        if (one.from === other.from) continue;
        const [p, q] = [treeOf(one.path), treeOf(other.path)];
        if (p.x === q.x) expect(p.bottom < q.top || q.bottom < p.top).toBe(true);
      }
    }
  });

  it("wraps a chain at six cards, and lays a cycle out the same way every time", () => {
    const chain = layoutIssueGraph(
      Array.from({ length: 8 }, (_, i) =>
        issue(`t${i}`, { issueType: "task", dependsOnKeys: i === 0 ? [] : [`t${i - 1}`] }),
      ),
      "type",
    );
    expect(rowsOf(nodesOf(chain)).map((row) => row.map((node) => node.entry.key))).toEqual([
      ["t0", "t1", "t2", "t3", "t4", "t5"],
      ["t6", "t7"],
    ]);
    // The new row starts back at the left, names the blocker it wrapped from, and chains on.
    expect(nodeOf(chain, "t6").x).toBe(nodeOf(chain, "t0").x);
    expect(nodeOf(chain, "t6").externalBlockers).toEqual({ keys: ["t5"], tone: "open" });
    expect(chainOf(edgeBetween(chain, "t6", "t7")!.path).x2).toBe(nodeOf(chain, "t7").x);

    const cycle = [
      issue("x", { dependsOnKeys: ["z"], parentKey: "y" }),
      issue("y", { dependsOnKeys: ["x"], parentKey: "z" }),
      issue("z", { dependsOnKeys: ["y"], parentKey: "y" }),
      issue("p", { dependsOnKeys: ["q"] }),
      issue("q", { dependsOnKeys: ["p"] }),
    ];
    const first = layoutIssueGraph(cycle, "tree");
    expect(nodesOf(first)).toHaveLength(5);
    expect(layoutIssueGraph(cycle, "tree")).toEqual(first);
    expectSpacedEnds(first);

    // a and b are each other's parent. The loop's earliest card, a, tops the tree, as the region's
    // header says; only b -> a, the link closing the loop, is cut, and a keeps c.
    const parentLoop = layoutIssueGraph(
      [
        issue("c", { parentKey: "a", priority: 0 }),
        issue("a", { parentKey: "b", priority: 1 }),
        issue("b", { parentKey: "a", priority: 2 }),
      ],
      "tree",
    );
    const loopRegion = regionsOf(parentLoop)[0]!;
    expect(loopRegion.root?.key).toBe("a");
    expect(loopRegion.nodes.map((node) => [node.entry.key, node.x])).toEqual([
      ["a", 12],
      ["c", 36],
      ["b", 36],
    ]);
    expect(
      loopRegion.edges
        .map((edge) => `${edge.from.split("\u0000").pop()}>${edge.to.split("\u0000").pop()}`)
        .toSorted(),
    ).toEqual(["a>b", "a>c"]);
  });

  it("grows a card's row to keep many arc ends apart, and puts unlinked cards in the grid below", () => {
    const sources = ["a0", "a1", "a2", "a3", "a4", "a5"];
    const layout = layoutIssueGraph(
      [
        issue("epic", { issueType: "epic" }),
        issue("a", { parentKey: "epic" }),
        ...sources.map((key) => issue(key, { parentKey: "a" })),
        issue("b", { parentKey: "epic" }),
        issue("hub", { parentKey: "b", dependsOnKeys: sources }),
      ],
      "tree",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    // Six arcs from cousins end on hub's right edge, 12 apart: it grows past its 40px height.
    const hub = nodeOf(layout, "hub");
    expect(
      edgesOf(layout).filter((edge) => edge.kind === "blocks" && edge.to === hub.id),
    ).toHaveLength(6);
    expect(hub.externalBlockers).toBeNull();
    expect(hub.height).toBe(5 * 12 + 2 * 8);
    expectSpacedEnds(layout);

    const grid = layoutIssueGraph(
      [
        issue("t0", { issueType: "task" }),
        issue("t1", { issueType: "task", dependsOnKeys: ["t0"] }),
        issue("alone", { issueType: "task" }),
        issue("bug-1", { issueType: "bug", dependsOnKeys: ["elsewhere"] }),
      ],
      "type",
      { nodeWidth: 100, nodeHeight: 40, labelHeight: 16 },
    );
    const tasks = regionsOf(grid).find((region) => region.label === "task")!;
    const alone = nodeOf(grid, "alone");
    expect(tasks.nodes).toContain(alone);
    expect(alone.y).toBeGreaterThan(
      Math.max(...tasks.nodes.filter((node) => node !== alone).map((n) => n.y + n.height)),
    );
    expect(nodeOf(grid, "bug-1").height).toBe(56);
    expect(regionsOf(grid).find((region) => region.label === "bug")!.height).toBe(12 + 56 + 12);
  });

  it("keeps a dense region of 200 issues and about 3,800 links bounded, and accounts for every link", () => {
    const entries = Array.from({ length: 200 }, (_, i) =>
      issue(`t${i}`, {
        issueType: "task",
        dependsOnKeys: Array.from({ length: Math.min(i, 20) }, (_, k) => `t${i - k - 1}`),
      }),
    );
    const layout = layoutIssueGraph(entries, "type");
    const region = regionsOf(layout)[0]!;
    expect(region.nodes).toHaveLength(200);
    // Every dependency is drawn or named on the blocked card.
    const drawn = new Set(region.edges.map((edge) => `${edge.from}>${edge.to}`));
    let links = 0;
    for (const entry of entries) {
      const node = nodeOf(layout, entry.key);
      for (const key of entry.dependsOnKeys ?? []) {
        links++;
        const listed = node.externalBlockers?.keys.includes(key) ?? false;
        expect(drawn.has(`${nodeId(key)}>${node.id}`) !== listed, `${key} -> ${entry.key}`).toBe(
          true,
        );
      }
    }
    expect(links).toBe(3790);
    for (const row of rowsOf(region.nodes)) {
      expect(row.length).toBeLessThanOrEqual(6);
    }
    const cardsRight = Math.max(...region.nodes.map((node) => node.x + node.width));
    expect(cardsRight).toBeLessThanOrEqual(12 + 6 * 240 + 5 * 56);
    expectSpacedEnds(layout);

    // Dense links between two sibling sub-trees are all cousin arcs: the gutter keeps at most
    // eight levels and the rest are named on their cards.
    const cousins = [
      issue("epic", { issueType: "epic" }),
      issue("a", { parentKey: "epic" }),
      issue("b", { parentKey: "epic" }),
      ...Array.from({ length: 99 }, (_, i) => issue(`a${i}`, { parentKey: "a" })),
      ...Array.from({ length: 99 }, (_, j) =>
        issue(`b${j}`, {
          parentKey: "b",
          dependsOnKeys: Array.from({ length: 38 }, (_, k) => `a${(j + k) % 99}`),
        }),
      ),
    ];
    const dense = layoutIssueGraph(cousins, "tree");
    const tree = regionsOf(dense)[0]!;
    expect(tree.nodes).toHaveLength(201);
    const right = Math.max(...tree.nodes.map((node) => node.x + node.width));
    expect(tree.width).toBeLessThanOrEqual(right + 16 + 7 * 8 + 12);
    const drawnLinks = new Set(tree.edges.map((edge) => `${edge.from}>${edge.to}`));
    let cousinLinks = 0;
    for (const entry of cousins) {
      const node = nodeOf(dense, entry.key);
      for (const key of entry.dependsOnKeys ?? []) {
        cousinLinks++;
        const named = node.externalBlockers?.keys.includes(key) ?? false;
        expect(
          drawnLinks.has(`${nodeId(key)}>${node.id}`) !== named,
          `${key} -> ${entry.key}`,
        ).toBe(true);
      }
    }
    expect(cousinLinks).toBe(3762);
    expectSpacedEnds(dense);
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
