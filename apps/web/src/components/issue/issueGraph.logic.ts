import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { issueEntryKey } from "@t3tools/client-runtime/state/issues";

export type IssueGraphGroupBy = "tree" | "type" | "status";

/** Blockers of a card that have no arrow to it, because they sit outside its region. */
export interface IssueGraphExternalBlockers {
  /** Display keys; the raw key for a blocker that is not in the list. */
  readonly keys: ReadonlyArray<string>;
  /** open: one is known open. closed: all are known closed. unknown: none open, some not listed. */
  readonly tone: "open" | "closed" | "unknown";
}

export interface IssueGraphNode {
  readonly id: string;
  readonly entry: EnvironmentIssueEntry;
  readonly x: number;
  readonly y: number;
  readonly width: number;
  /** Taller by the size's labelHeight with a blocked-by line, or to space many arc ends. */
  readonly height: number;
  readonly externalBlockers: IssueGraphExternalBlockers | null;
}

interface IssueGraphEdgeBase {
  readonly id: string;
  readonly from: string;
  readonly to: string;
  /** SVG path `d` attribute. */
  readonly path: string;
}

export type IssueGraphEdge =
  /** Parent to child: a folder-tree line from the parent's trunk into the child's left edge. */
  | (IssueGraphEdgeBase & { readonly kind: "parent" })
  /** Blocker to blocked issue: an arc through the right-hand gutter ending in an arrowhead. */
  | (IssueGraphEdgeBase & { readonly kind: "blocks"; readonly blockerOpen: boolean });

export interface IssueGraphRegion {
  /** Unique across projects, so a region's collapse is remembered for its own project only. */
  readonly id: string;
  readonly kind: "tree" | "loose" | "type" | "status";
  /** Raw value: root displayKey or missing parent key, the issue type, or the state name. */
  readonly label: string;
  /** The root issue of a tree; null when that root is not among the entries. */
  readonly root: EnvironmentIssueEntry | null;
  readonly issueCount: number;
  /** The body under the region's header; node and edge coordinates are relative to it. */
  readonly width: number;
  readonly height: number;
  readonly nodes: ReadonlyArray<IssueGraphNode>;
  /** Only links inside the region: each region scrolls sideways on its own. */
  readonly edges: ReadonlyArray<IssueGraphEdge>;
}

export interface IssueGraphProject {
  /** `environmentId:projectId`. */
  readonly id: string;
  readonly title: string;
  readonly issueCount: number;
  readonly regions: ReadonlyArray<IssueGraphRegion>;
}

export interface IssueGraphSize {
  readonly nodeWidth: number;
  readonly nodeHeight: number;
  /** Extra height of a card that shows a line of external blockers. */
  readonly labelHeight: number;
}

const DEFAULT_SIZE: IssueGraphSize = { nodeWidth: 240, nodeHeight: 56, labelHeight: 20 };
const REGION_PADDING = 12;
const ROW_GAP = 12;
const GRID_COLUMNS = 4;
const GRID_GAP = 16;
/** Each level of the sub-task tree moves a card this far right. */
const INDENT = 24;
// A parent's trunk runs this far in from its left edge, so it stays left of every card below it.
const TRUNK_INSET = 12;
// A sub-task's tick enters beside its first line, as in a folder tree.
const TREE_ENTRY = 16;
// Arcs leave the widest card by GUTTER_GAP before their first level; levels sit LEVEL_GAP apart.
const GUTTER_GAP = 16;
const LEVEL_GAP = 8;
// Arc ends on one card sit further apart than an 8px arrowhead, and clear of its corners.
const ENDPOINT_GAP = 12;
const ENDPOINT_MARGIN = 8;
const ARC_RADIUS = 4;

const TYPE_ORDER = ["epic", "feature", "bug", "task", "chore"];
const OPEN_STATUS_ORDER = [
  "backlog",
  "todo",
  "open",
  "in progress",
  "in review",
  "blocked",
  "deferred",
];
const collator = new Intl.Collator("en", { numeric: true });

type ClusterKind = IssueGraphRegion["kind"];

interface Item {
  readonly id: string;
  readonly entry: EnvironmentIssueEntry;
  /** Position in the global priority-then-input order, used for every tie-break. */
  order: number;
}

interface PendingCluster {
  readonly id: string;
  readonly kind: ClusterKind;
  readonly label: string;
  readonly rootNodeId: string | null;
  readonly members: Array<number>;
}

interface RawEdge {
  readonly id: string;
  readonly from: number;
  readonly to: number;
  readonly kind: "parent" | "blocks";
}

/** A resolved dependsOnKeys entry; index is undefined when the blocker is not in the list. */
interface Blocker {
  readonly key: string;
  readonly index: number | undefined;
}

/** Keys in parentKey and dependsOnKeys name issues of the same environment and tracker scope. */
function scopeRef(entry: EnvironmentIssueEntry, key: string): string {
  return `${entry.environmentId}\u0000${entry.tracker}\u0000${entry.host.toLowerCase()}\u0000${entry.scope.toLowerCase()}\u0000${key}`;
}

function priorityOf(entry: EnvironmentIssueEntry): number {
  return entry.priority ?? Number.POSITIVE_INFINITY;
}

function typeRank(type: string): [number, string] {
  if (type === "") return [TYPE_ORDER.length + 1, ""];
  const known = TYPE_ORDER.indexOf(type.toLowerCase());
  return known === -1 ? [TYPE_ORDER.length, type] : [known, ""];
}

function compareTypes(a: string, b: string): number {
  const [ra, na] = typeRank(a);
  const [rb, nb] = typeRank(b);
  return ra - rb || collator.compare(na, nb);
}

function statusRank(category: "open" | "closed", name: string): number {
  if (category === "closed") return OPEN_STATUS_ORDER.length + 1;
  const known = OPEN_STATUS_ORDER.indexOf(name.toLowerCase().replace(/[_-]+/g, " "));
  return known === -1 ? OPEN_STATUS_ORDER.length : known;
}

function compareIds(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Issues grouped into one row per project, each holding the regions of the chosen grouping. Every
 * issue of one tracker scope sits in the project that listed that scope first, so parent and
 * blocking links never cross projects.
 */
export function layoutIssueGraph(
  entries: ReadonlyArray<EnvironmentIssueEntry>,
  groupBy: IssueGraphGroupBy,
  size: IssueGraphSize = DEFAULT_SIZE,
): ReadonlyArray<IssueGraphProject> {
  // One node per issue: the same issue read through two projects of one scope is drawn once.
  const items: Array<Item> = [];
  const seenRefs = new Set<string>();
  const seenIds = new Set<string>();
  const projectOfScope = new Map<string, { id: string; title: string }>();
  const projectItems = new Map<string, { title: string; items: Array<Item> }>();
  for (const entry of entries) {
    const ref = scopeRef(entry, entry.key);
    const id = `${entry.environmentId}:${issueEntryKey(entry)}`;
    if (seenRefs.has(ref) || seenIds.has(id)) continue;
    seenRefs.add(ref);
    seenIds.add(id);
    const scope = scopeRef(entry, "");
    let project = projectOfScope.get(scope);
    if (project === undefined) {
      project = { id: `${entry.environmentId}:${entry.projectId}`, title: entry.projectTitle };
      projectOfScope.set(scope, project);
    }
    const item: Item = { id, entry, order: 0 };
    items.push(item);
    let group = projectItems.get(project.id);
    if (group === undefined) {
      group = { title: project.title, items: [] };
      projectItems.set(project.id, group);
    }
    group.items.push(item);
  }
  // Within a priority, the list's chosen sort order decides, so the Sort control applies here too.
  items
    .map((item, index) => ({ item, index }))
    .toSorted((a, b) => priorityOf(a.item.entry) - priorityOf(b.item.entry) || a.index - b.index)
    .forEach(({ item }, order) => {
      item.order = order;
    });

  return [...projectItems]
    .toSorted(([idA, a], [idB, b]) => collator.compare(a.title, b.title) || compareIds(idA, idB))
    .map(([id, { title, items: members }]) => {
      const byRef = new Map(
        members.map((item, index) => [scopeRef(item.entry, item.entry.key), index]),
      );
      const edges = collectEdges(members, byRef);
      const blockers = members.map((item, index) => resolveBlockers(item.entry, index, byRef));
      const clusters =
        groupBy === "tree" ? treeClusters(members, byRef) : attributeClusters(members, groupBy);
      return {
        id,
        title,
        issueCount: members.length,
        regions: clusters.map((cluster) =>
          placeRegion(id, members, edges, blockers, cluster, size),
        ),
      };
    });
}

function collectEdges(
  items: ReadonlyArray<Item>,
  byRef: ReadonlyMap<string, number>,
): Array<RawEdge> {
  const edges = new Map<string, RawEdge>();
  const add = (from: number | undefined, to: number, kind: RawEdge["kind"]) => {
    if (from === undefined || from === to) return;
    const id = `${kind}:${items[from]!.id}->${items[to]!.id}`;
    if (!edges.has(id)) edges.set(id, { id, from, to, kind });
  };
  items.forEach(({ entry }, index) => {
    if (entry.parentKey !== undefined)
      add(byRef.get(scopeRef(entry, entry.parentKey)), index, "parent");
    for (const key of entry.dependsOnKeys ?? [])
      add(byRef.get(scopeRef(entry, key)), index, "blocks");
  });
  return [...edges.values()].toSorted((a, b) => compareIds(a.id, b.id));
}

function resolveBlockers(
  entry: EnvironmentIssueEntry,
  self: number,
  byRef: ReadonlyMap<string, number>,
): Array<Blocker> {
  const blockers = new Map<string, Blocker>();
  for (const key of entry.dependsOnKeys ?? []) {
    const index = byRef.get(scopeRef(entry, key));
    if (index !== self && !blockers.has(key)) blockers.set(key, { key, index });
  }
  return [...blockers.values()];
}

function externalBlockersOf(
  items: ReadonlyArray<Item>,
  blockers: ReadonlyArray<Blocker>,
  memberSet: ReadonlySet<number>,
): IssueGraphExternalBlockers | null {
  const outside = blockers.filter(({ index }) => index === undefined || !memberSet.has(index));
  if (outside.length === 0) return null;
  const categories = outside.map(({ index }) =>
    index === undefined ? undefined : items[index]!.entry.state.category,
  );
  return {
    keys: outside.map(({ key, index }) =>
      index === undefined ? key : items[index]!.entry.displayKey,
    ),
    tone: categories.includes("open")
      ? "open"
      : categories.every((category) => category === "closed")
        ? "closed"
        : "unknown",
  };
}

type Root =
  | { readonly kind: "node"; readonly index: number }
  | { readonly kind: "missing"; readonly ref: string; readonly key: string };

function treeClusters(
  items: ReadonlyArray<Item>,
  byRef: ReadonlyMap<string, number>,
): Array<PendingCluster> {
  const parentOf = items.map(({ entry }): Root | null => {
    if (entry.parentKey === undefined) return null;
    const ref = scopeRef(entry, entry.parentKey);
    const index = byRef.get(ref);
    if (index === undefined) return { kind: "missing", ref, key: entry.parentKey };
    return items[index]!.entry === entry ? null : { kind: "node", index };
  });
  const hasChildren = new Set<number>();
  for (const parent of parentOf) if (parent?.kind === "node") hasChildren.add(parent.index);

  const roots: Array<Root | undefined> = Array.from({ length: items.length });
  for (let start = 0; start < items.length; start++) {
    const path: Array<number> = [];
    const onPath = new Map<number, number>();
    let current = start;
    let root: Root;
    for (;;) {
      const known = roots[current];
      if (known !== undefined) {
        root = known;
        break;
      }
      const cycleStart = onPath.get(current);
      if (cycleStart !== undefined) {
        // A parent cycle has no top; its earliest member in the global order stands in for one.
        const cycle = path.slice(cycleStart);
        const top = cycle.reduce((best, index) =>
          items[index]!.order < items[best]!.order ? index : best,
        );
        root = { kind: "node", index: top };
        break;
      }
      onPath.set(current, path.length);
      path.push(current);
      const parent = parentOf[current];
      if (parent == null) {
        root = { kind: "node", index: current };
        break;
      }
      if (parent.kind === "missing") {
        root = parent;
        break;
      }
      current = parent.index;
    }
    for (const index of path) roots[index] = root;
  }

  const clusters = new Map<string, PendingCluster>();
  const sortKey = new Map<
    string,
    { group: number; priority: number; updated: number; type: string }
  >();
  items.forEach(({ entry }, index) => {
    const root = roots[index]!;
    let cluster: Omit<PendingCluster, "members">;
    let group = 0;
    if (root.kind === "missing") {
      cluster = { id: `tree:missing:${root.ref}`, kind: "tree", label: root.key, rootNodeId: null };
    } else if (
      root.index === index &&
      parentOf[index] === null &&
      !hasChildren.has(index) &&
      entry.issueType !== "epic"
    ) {
      const type = entry.issueType ?? "";
      cluster = { id: `loose:${type}`, kind: "loose", label: type, rootNodeId: null };
      group = 1;
    } else {
      const rootItem = items[root.index]!;
      cluster = {
        id: `tree:${rootItem.id}`,
        kind: "tree",
        label: rootItem.entry.displayKey,
        rootNodeId: rootItem.id,
      };
    }
    let target = clusters.get(cluster.id);
    if (target === undefined) {
      target = { ...cluster, members: [] };
      clusters.set(cluster.id, target);
    }
    target.members.push(index);
    const rootPriority =
      root.kind === "node" ? priorityOf(items[root.index]!.entry) : priorityOf(entry);
    const updated = Date.parse(entry.updatedAt) || 0;
    const previous = sortKey.get(cluster.id);
    sortKey.set(cluster.id, {
      group,
      // A tree whose root is hidden ranks by its most urgent visible member.
      priority:
        root.kind === "node"
          ? rootPriority
          : Math.min(previous?.priority ?? rootPriority, rootPriority),
      updated: Math.max(previous?.updated ?? updated, updated),
      type: cluster.label,
    });
  });

  return [...clusters.values()].toSorted((a, b) => {
    const ka = sortKey.get(a.id)!;
    const kb = sortKey.get(b.id)!;
    if (ka.group !== kb.group) return ka.group - kb.group;
    if (ka.group === 1) return compareTypes(ka.type, kb.type);
    return ka.priority - kb.priority || kb.updated - ka.updated || compareIds(a.id, b.id);
  });
}

function attributeClusters(
  items: ReadonlyArray<Item>,
  groupBy: "type" | "status",
): Array<PendingCluster> {
  const clusters = new Map<string, PendingCluster & { category: "open" | "closed" }>();
  items.forEach(({ entry }, index) => {
    const label = groupBy === "type" ? (entry.issueType ?? "") : entry.state.name;
    const id = `${groupBy}:${label}`;
    let cluster = clusters.get(id);
    if (cluster === undefined) {
      cluster = {
        id,
        kind: groupBy,
        label,
        rootNodeId: null,
        members: [],
        category: entry.state.category,
      };
      clusters.set(id, cluster);
    }
    if (entry.state.category === "open") cluster.category = "open";
    cluster.members.push(index);
  });
  return [...clusters.values()].toSorted((a, b) =>
    groupBy === "type"
      ? compareTypes(a.label, b.label)
      : statusRank(a.category, a.label) - statusRank(b.category, b.label) ||
        collator.compare(a.label, b.label),
  );
}

/** Blockers above what they block where the links allow, else the current order. */
function orderSiblings(
  group: ReadonlyArray<number>,
  blocks: ReadonlyMap<number, ReadonlyArray<number>>,
  order: (index: number) => number,
): Array<number> {
  const pending = group.toSorted((a, b) => order(a) - order(b));
  const members = new Set(group);
  const waiting = new Map<number, number>(group.map((index) => [index, 0]));
  for (const index of group) {
    for (const next of blocks.get(index) ?? []) {
      if (members.has(next)) waiting.set(next, waiting.get(next)! + 1);
    }
  }
  const ordered: Array<number> = [];
  while (pending.length > 0) {
    // In a cycle nothing is free; the earliest card goes next and the rest follow.
    const free = pending.findIndex((index) => waiting.get(index) === 0);
    const [next] = pending.splice(Math.max(free, 0), 1);
    ordered.push(next!);
    for (const blocked of blocks.get(next!) ?? []) {
      if (members.has(blocked)) waiting.set(blocked, waiting.get(blocked)! - 1);
    }
  }
  return ordered;
}

/**
 * Linked issues form an indented list: each parent, then its sub-tasks in depth-first order, joined
 * by folder-tree lines on the left. Blocking links are arcs on the right, nested by span in a
 * gutter past the widest card. Issues with no link in the region fill a grid below the list.
 */
function placeRegion(
  projectId: string,
  items: ReadonlyArray<Item>,
  edges: ReadonlyArray<RawEdge>,
  blockers: ReadonlyArray<ReadonlyArray<Blocker>>,
  cluster: PendingCluster,
  size: IssueGraphSize,
): IssueGraphRegion {
  const { nodeWidth, nodeHeight, labelHeight } = size;
  const order = (index: number) => items[index]!.order;
  const memberSet = new Set(cluster.members);
  const regionEdges = edges.filter((edge) => memberSet.has(edge.from) && memberSet.has(edge.to));
  const linked = new Set<number>();
  const parentOf = new Map<number, number>();
  const blocks = new Map<number, Array<number>>();
  for (const edge of regionEdges) {
    linked.add(edge.from).add(edge.to);
    if (edge.kind === "parent") {
      if (!parentOf.has(edge.to)) parentOf.set(edge.to, edge.from);
    } else {
      let next = blocks.get(edge.from);
      if (next === undefined) blocks.set(edge.from, (next = []));
      next.push(edge.to);
    }
  }
  const listed = [...linked].toSorted((a, b) => order(a) - order(b));
  const childrenOf = new Map<number, Array<number>>();
  for (const index of listed) {
    const parent = parentOf.get(index);
    if (parent === undefined) continue;
    let children = childrenOf.get(parent);
    if (children === undefined) childrenOf.set(parent, (children = []));
    children.push(index);
  }

  // Depth-first pre-order. A parent cycle has no top; its earliest card starts it at depth 0.
  const rows: Array<number> = [];
  const depth = new Map<number, number>();
  const treeParent = new Map<number, number>();
  const visit = (root: number) => {
    const stack = [{ index: root, level: 0, parent: -1 }];
    while (stack.length > 0) {
      const { index, level, parent } = stack.pop()!;
      if (depth.has(index)) continue;
      depth.set(index, level);
      if (parent !== -1) treeParent.set(index, parent);
      rows.push(index);
      const children = orderSiblings(childrenOf.get(index) ?? [], blocks, order);
      for (let i = children.length - 1; i >= 0; i--) {
        if (!depth.has(children[i]!))
          stack.push({ index: children[i]!, level: level + 1, parent: index });
      }
    }
  };
  const roots = listed.filter((index) => !parentOf.has(index));
  for (const root of orderSiblings(roots, blocks, order)) visit(root);
  for (const index of listed) if (!depth.has(index)) visit(index);
  const rowOf = new Map(rows.map((index, row) => [index, row]));

  // Arcs nest by span: the shortest takes the innermost free level, so arcs on one level never
  // share a row and arcs on different levels never share a gutter segment.
  const arcs = regionEdges
    .filter((edge) => edge.kind === "blocks")
    .map((edge) => {
      const [a, b] = [rowOf.get(edge.from)!, rowOf.get(edge.to)!];
      return { edge, low: Math.min(a, b), high: Math.max(a, b), level: 0 };
    })
    .toSorted(
      (a, b) =>
        a.high - a.low - (b.high - b.low) || a.low - b.low || compareIds(a.edge.id, b.edge.id),
    );
  // Each row keeps a bitset of the levels its arcs use; an arc takes the lowest level free in
  // every row of its span.
  let words = 1;
  let levelCount = 0;
  let used = Array.from({ length: rows.length }, () => new Uint32Array(words));
  const seen = new Uint32Array(Math.ceil(arcs.length / 32) + 1);
  for (const arc of arcs) {
    const active = Math.ceil(levelCount / 32);
    seen.fill(0, 0, active);
    for (let row = arc.low; row <= arc.high; row++) {
      const bits = used[row]!;
      for (let word = 0; word < active; word++) seen[word]! |= bits[word]!;
    }
    let level = active * 32;
    for (let word = 0; word < active; word++) {
      const free = ~seen[word]! >>> 0;
      if (free !== 0) {
        level = word * 32 + 31 - Math.clz32(free & -free);
        break;
      }
    }
    if (level >= words * 32) {
      words *= 2;
      used = used.map((bits) => {
        const grown = new Uint32Array(words);
        grown.set(bits);
        return grown;
      });
    }
    for (let row = arc.low; row <= arc.high; row++) used[row]![level >> 5]! |= 1 << (level & 31);
    levelCount = Math.max(levelCount, level + 1);
    arc.level = level;
  }

  // Ends on one card: arcs going up above arcs going down; inner arcs nearer the far side, so
  // no arc's leg crosses another arc of the same card.
  const ends = new Map<
    number,
    Array<{ readonly arc: (typeof arcs)[number]; readonly up: boolean }>
  >();
  for (const arc of arcs) {
    for (const [index, other] of [
      [arc.edge.from, arc.edge.to],
      [arc.edge.to, arc.edge.from],
    ] as const) {
      let list = ends.get(index);
      if (list === undefined) ends.set(index, (list = []));
      list.push({ arc, up: rowOf.get(other)! < rowOf.get(index)! });
    }
  }
  for (const list of ends.values()) {
    list.sort(
      (a, b) =>
        Number(a.up ? 0 : 1) - Number(b.up ? 0 : 1) ||
        (a.up ? a.arc.level - b.arc.level : b.arc.level - a.arc.level) ||
        compareIds(a.arc.edge.id, b.arc.edge.id),
    );
  }

  const external = new Map(
    cluster.members.map((index) => [index, externalBlockersOf(items, blockers[index]!, memberSet)]),
  );
  const cardHeight = (index: number) =>
    Math.max(
      nodeHeight + (external.get(index) === null ? 0 : labelHeight),
      ((ends.get(index)?.length ?? 1) - 1) * ENDPOINT_GAP + 2 * ENDPOINT_MARGIN,
    );
  const nodeAt = new Map<number, IssueGraphNode>();
  const place = (index: number, x: number, y: number, height: number) => {
    const node: IssueGraphNode = {
      id: items[index]!.id,
      entry: items[index]!.entry,
      x,
      y,
      width: nodeWidth,
      height,
      externalBlockers: external.get(index)!,
    };
    nodeAt.set(index, node);
    return node;
  };
  let bottom = REGION_PADDING - ROW_GAP;
  for (const index of rows) {
    const height = cardHeight(index);
    place(index, REGION_PADDING + depth.get(index)! * INDENT, bottom + ROW_GAP, height);
    bottom += ROW_GAP + height;
  }
  const listRight = Math.max(0, ...rows.map((index) => nodeAt.get(index)!.x + nodeWidth));
  const gutterX = (level: number) => listRight + GUTTER_GAP + level * LEVEL_GAP;

  // Issues with no link inside the region fill a grid below the list.
  const loose = cluster.members
    .filter((index) => !linked.has(index))
    .toSorted((a, b) => order(a) - order(b));
  for (let start = 0; start < loose.length; start += GRID_COLUMNS) {
    const line = loose.slice(start, start + GRID_COLUMNS);
    const height = Math.max(...line.map(cardHeight));
    line.forEach((index, column) => {
      place(
        index,
        REGION_PADDING + column * (nodeWidth + GRID_GAP),
        bottom + ROW_GAP,
        cardHeight(index),
      );
    });
    bottom += ROW_GAP + height;
  }

  const endY = (index: number, arc: (typeof arcs)[number]) => {
    const node = nodeAt.get(index)!;
    const list = ends.get(index)!;
    const position = list.findIndex((end) => end.arc === arc);
    return node.y + node.height / 2 + (position - (list.length - 1) / 2) * ENDPOINT_GAP;
  };
  const arcPath = new Map(
    arcs.map((arc) => {
      const [from, to] = [nodeAt.get(arc.edge.from)!, nodeAt.get(arc.edge.to)!];
      const [y1, y2] = [endY(arc.edge.from, arc), endY(arc.edge.to, arc)];
      const x = gutterX(arc.level);
      const turn = Math.sign(y2 - y1) * Math.min(ARC_RADIUS, Math.abs(y2 - y1) / 2);
      const radius = Math.abs(turn);
      return [
        arc.edge.id,
        `M ${from.x + from.width} ${y1} H ${x - radius} Q ${x} ${y1} ${x} ${y1 + turn} ` +
          `V ${y2 - turn} Q ${x} ${y2} ${x - radius} ${y2} H ${to.x + to.width}`,
      ] as const;
    }),
  );
  const placedEdges = regionEdges.flatMap((edge): Array<IssueGraphEdge> => {
    const from = nodeAt.get(edge.from)!;
    const to = nodeAt.get(edge.to)!;
    const base = { id: edge.id, from: from.id, to: to.id };
    if (edge.kind === "blocks") {
      const blockerOpen = items[edge.from]!.entry.state.category !== "closed";
      return [{ ...base, kind: "blocks", blockerOpen, path: arcPath.get(edge.id)! }];
    }
    // Only the link the list follows; a parent cycle's closing link is not drawn.
    if (treeParent.get(edge.to) !== edge.from) return [];
    const entry = to.y + Math.min(TREE_ENTRY, to.height / 2);
    return [
      {
        ...base,
        kind: "parent",
        path: `M ${from.x + TRUNK_INSET} ${from.y + from.height} V ${entry} H ${to.x}`,
      },
    ];
  });

  const gridRight =
    loose.length === 0
      ? 0
      : REGION_PADDING + Math.min(loose.length, GRID_COLUMNS) * (nodeWidth + GRID_GAP) - GRID_GAP;
  return {
    id: `${projectId}:${cluster.id}`,
    kind: cluster.kind,
    label: cluster.label,
    root:
      cluster.rootNodeId === null
        ? null
        : (items.find((item) => item.id === cluster.rootNodeId)?.entry ?? null),
    issueCount: cluster.members.length,
    width:
      Math.max(listRight, gridRight, levelCount === 0 ? 0 : gutterX(levelCount - 1)) +
      REGION_PADDING,
    height: bottom + REGION_PADDING,
    nodes: [...rows, ...loose].map((index) => nodeAt.get(index)!),
    edges: placedEdges,
  };
}
