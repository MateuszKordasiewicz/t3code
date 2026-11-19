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
  /** Taller by the size's labelHeight when the card shows its external blockers. */
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
  /** Parent to child: an orthogonal tree connector into the child's left edge. */
  | (IssueGraphEdgeBase & { readonly kind: "parent" })
  /** Blocker to blocked issue: a curve that ends in an arrowhead. */
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
const REGION_PADDING = 16;
const RANK_GAP = 64;
// Wide enough for three parallel arrow tracks with clearance from the cards on both sides.
const ROW_GAP = 16;
const GRID_COLUMNS = 4;
// Grid issues have no links between them, so their columns need no room for edges.
const GRID_GAP = 16;
const EDGE_BOW = 48;
// How far in from the parent's right edge a tree connector drops out of its bottom.
const TREE_DROP_INSET = 16;
// A tree connector enters the child beside its first line, as in a folder tree.
const TREE_ENTRY = 16;
// Spacing of parallel tracks: tree tracks step left from the column gap's middle; arrow tracks
// sit either side of the row gap's middle; a lane through empty cells keeps off their middle.
const TRACK_GAP = 8;
const TREE_TRACK_OFFSETS = [0, -8, -16, -24];
const ARROW_TRACK_OFFSETS = [-4, 4, 0];
const LANE_TRACK_OFFSETS = [-8, 8, -16, 16];

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

/**
 * Ranks are longest paths from the sources. Links join the ranking one by one, parent links first,
 * and a link that would close a loop is left out, so the hierarchy always reads left to right and
 * a dependency against it is drawn backward.
 */
function rankCluster(
  items: ReadonlyArray<Item>,
  members: ReadonlyArray<number>,
  edges: ReadonlyArray<RawEdge>,
): { ranks: Map<number, number>; predecessors: Map<number, Array<number>> } {
  const order = (index: number) => items[index]!.order;
  const successors = new Map<number, Array<number>>(members.map((index) => [index, []]));
  const predecessors = new Map<number, Array<number>>(members.map((index) => [index, []]));
  const reaches = (start: number, target: number) => {
    const seen = new Set<number>([start]);
    const stack = [start];
    while (stack.length > 0) {
      const node = stack.pop()!;
      if (node === target) return true;
      for (const next of successors.get(node)!) {
        if (!seen.has(next)) {
          seen.add(next);
          stack.push(next);
        }
      }
    }
    return false;
  };
  const candidates = edges.toSorted(
    (a, b) =>
      Number(a.kind === "blocks") - Number(b.kind === "blocks") ||
      order(a.from) - order(b.from) ||
      order(a.to) - order(b.to),
  );
  for (const { from, to } of candidates) {
    if (successors.get(from)!.includes(to) || reaches(to, from)) continue;
    successors.get(from)!.push(to);
    predecessors.get(to)!.push(from);
  }
  const ranks = new Map<number, number>();
  const rankOf = (node: number): number => {
    const known = ranks.get(node);
    if (known !== undefined) return known;
    const rank = Math.max(0, ...predecessors.get(node)!.map((from) => rankOf(from) + 1));
    ranks.set(node, rank);
    return rank;
  };
  for (const index of members.toSorted((a, b) => order(a) - order(b))) rankOf(index);
  return { ranks, predecessors };
}

function placeRegion(
  projectId: string,
  items: ReadonlyArray<Item>,
  edges: ReadonlyArray<RawEdge>,
  blockers: ReadonlyArray<ReadonlyArray<Blocker>>,
  cluster: PendingCluster,
  size: IssueGraphSize,
): IssueGraphRegion {
  const { nodeWidth, nodeHeight, labelHeight } = size;
  const memberSet = new Set(cluster.members);
  const regionEdges = edges.filter((edge) => memberSet.has(edge.from) && memberSet.has(edge.to));
  const linked = new Set<number>();
  const parentsOf = new Map<number, Array<number>>();
  for (const edge of regionEdges) {
    linked.add(edge.from).add(edge.to);
    if (edge.kind === "parent")
      parentsOf.set(edge.to, [...(parentsOf.get(edge.to) ?? []), edge.from]);
  }
  const { ranks, predecessors } = rankCluster(
    items,
    cluster.members.filter((index) => linked.has(index)),
    regionEdges,
  );
  const columns: Array<Array<number>> = [];
  for (const [index, rank] of ranks) (columns[rank] ??= []).push(index);
  const row = new Map<number, number>();
  const column = new Map<number, number>();
  columns.forEach((members, rank) => {
    const barycenter = new Map<number, number>();
    if (rank > 0) {
      for (const index of members) {
        const preds = predecessors.get(index)!;
        barycenter.set(index, preds.reduce((sum, from) => sum + row.get(from)!, 0) / preds.length);
      }
    }
    members.sort(
      (a, b) =>
        (barycenter.get(a) ?? 0) - (barycenter.get(b) ?? 0) || items[a]!.order - items[b]!.order,
    );
    let next = 0;
    for (const index of members) {
      // A sub-task sits below its parent, as in a folder tree, so its connector can turn down.
      for (const parent of parentsOf.get(index) ?? []) {
        if (column.has(parent)) next = Math.max(next, row.get(parent)! + 1);
      }
      row.set(index, next);
      column.set(index, rank);
      next++;
    }
  });
  // Issues with no link inside the region fill a grid beside the ranked part, not one tall column.
  const loose = cluster.members
    .filter((index) => !linked.has(index))
    .toSorted((a, b) => items[a]!.order - items[b]!.order);
  loose.forEach((index, position) => {
    row.set(index, Math.floor(position / GRID_COLUMNS));
    column.set(index, columns.length + (position % GRID_COLUMNS));
  });

  const external = new Map(
    cluster.members.map((index) => [index, externalBlockersOf(items, blockers[index]!, memberSet)]),
  );
  // Rows stay aligned across columns, so row and column gaps stay free for links to run in.
  const rowCount = Math.max(...cluster.members.map((index) => row.get(index)! + 1));
  const rowHeights = Array.from({ length: rowCount }, () => nodeHeight);
  for (const index of cluster.members) {
    if (external.get(index) !== null) rowHeights[row.get(index)!] = nodeHeight + labelHeight;
  }
  const rowTops: Array<number> = [];
  rowHeights.reduce((top, height, index) => {
    rowTops[index] = top;
    return top + height + ROW_GAP;
  }, REGION_PADDING);

  const gridColumns = Math.min(loose.length, GRID_COLUMNS);
  // A link back into the first column turns left of it, so that column moves right to make room.
  const left =
    REGION_PADDING +
    (regionEdges.some((edge) => column.get(edge.to) === 0 && column.get(edge.from)! > 0)
      ? EDGE_BOW
      : 0);
  const nodeAt = new Map<number, IssueGraphNode>();
  const nodes = [...columns.flat(), ...loose].map((index) => {
    const externalBlockers = external.get(index)!;
    const node: IssueGraphNode = {
      id: items[index]!.id,
      entry: items[index]!.entry,
      x: left + columnX(column.get(index)!, columns.length, nodeWidth),
      y: rowTops[row.get(index)!]!,
      width: nodeWidth,
      height: nodeHeight + (externalBlockers === null ? 0 : labelHeight),
      externalBlockers,
    };
    nodeAt.set(index, node);
    return node;
  });
  const lastColumn = columns.length + gridColumns - 1;
  const cardsRight =
    lastColumn < 0
      ? left + REGION_PADDING
      : left + columnX(lastColumn, columns.length, nodeWidth) + nodeWidth + REGION_PADDING;
  const routed = routeEdges({
    items,
    edges: regionEdges,
    nodeAt,
    column,
    row,
    rowTops,
    rowHeights,
    gapLeft: (index) => left + columnX(index, columns.length, nodeWidth) + nodeWidth,
  });
  return {
    id: `${projectId}:${cluster.id}`,
    kind: cluster.kind,
    label: cluster.label,
    root:
      cluster.rootNodeId === null
        ? null
        : (items.find((item) => item.id === cluster.rootNodeId)?.entry ?? null),
    issueCount: cluster.members.length,
    // A link can run past the last column; the region scrolls far enough to show it.
    width: Math.max(cardsRight, routed.maxX + REGION_PADDING),
    height: Math.max(
      REGION_PADDING * 2 +
        rowHeights.reduce((sum, height) => sum + height, 0) +
        (rowCount - 1) * ROW_GAP,
      routed.maxY + REGION_PADDING,
    ),
    nodes,
    edges: routed.edges,
  };
}

function columnX(column: number, rankedColumns: number, nodeWidth: number): number {
  if (column < rankedColumns) return column * (nodeWidth + RANK_GAP);
  return rankedColumns * (nodeWidth + RANK_GAP) + (column - rankedColumns) * (nodeWidth + GRID_GAP);
}

interface Run {
  readonly channel: string;
  readonly owner: string;
  readonly from: number;
  readonly to: number;
  /** A run that only passes through a gap takes tracks after the gap's own trunks. */
  readonly passing?: boolean;
}

/**
 * Parallel tracks inside the gaps links run in. One owner keeps one track per gap, so siblings
 * share their parent's trunk; owners whose runs overlap in a gap get different tracks.
 */
function assignTracks(runs: ReadonlyArray<Run>): (channel: string, owner: string) => number {
  const key = (channel: string, owner: string) => `${channel}\u0000${owner}`;
  const merged = new Map<string, Run>();
  for (const run of runs) {
    const previous = merged.get(key(run.channel, run.owner));
    const low = Math.min(run.from, run.to, previous?.from ?? Infinity);
    const high = Math.max(run.from, run.to, previous?.to ?? -Infinity);
    merged.set(key(run.channel, run.owner), { ...run, from: low, to: high });
  }
  const byChannel = new Map<string, Array<Run>>();
  for (const run of merged.values()) {
    byChannel.set(run.channel, [...(byChannel.get(run.channel) ?? []), run]);
  }
  const tracks = new Map<string, number>();
  for (const channelRuns of byChannel.values()) {
    const taken: Array<Array<Run>> = [];
    const sorted = channelRuns.toSorted(
      (a, b) =>
        Number(a.passing ?? false) - Number(b.passing ?? false) ||
        a.from - b.from ||
        a.to - b.to ||
        compareIds(a.owner, b.owner),
    );
    for (const run of sorted) {
      let track = 0;
      while (taken[track]?.some((other) => other.from <= run.to && run.from <= other.to)) track++;
      (taken[track] ??= []).push(run);
      tracks.set(key(run.channel, run.owner), track);
    }
  }
  return (channel, owner) => tracks.get(key(channel, owner)) ?? 0;
}

const offsetOf = (offsets: ReadonlyArray<number>, track: number) =>
  offsets[track % offsets.length]!;

function routeEdges({
  items,
  edges,
  nodeAt,
  column,
  row,
  rowTops,
  rowHeights,
  gapLeft,
}: {
  items: ReadonlyArray<Item>;
  edges: ReadonlyArray<RawEdge>;
  nodeAt: ReadonlyMap<number, IssueGraphNode>;
  column: ReadonlyMap<number, number>;
  row: ReadonlyMap<number, number>;
  rowTops: ReadonlyArray<number>;
  rowHeights: ReadonlyArray<number>;
  /** Left side of the gap to the right of a column. */
  gapLeft: (column: number) => number;
}): { edges: Array<IssueGraphEdge>; maxX: number; maxY: number } {
  const half = RANK_GAP / 2;
  const contentBottom = rowTops[rowTops.length - 1]! + rowHeights[rowHeights.length - 1]!;
  const occupied = new Set(
    [...nodeAt.keys()].map((index) => `${column.get(index)}:${row.get(index)}`),
  );
  const treeEntryY = (to: IssueGraphNode) => to.y + Math.min(TREE_ENTRY, to.height / 2);

  // Tree connectors. A child more than one column right reaches its column through a lane: the
  // first row below the parent with no card in the columns it crosses, else a lane under them all.
  const parentEdges = edges.filter((edge) => edge.kind === "parent");
  const forward = (edge: RawEdge) => column.get(edge.to)! > column.get(edge.from)!;
  const farthest = new Map<number, number>();
  for (const edge of parentEdges) {
    if (forward(edge) && column.get(edge.to)! > column.get(edge.from)! + 1) {
      farthest.set(edge.from, Math.max(farthest.get(edge.from) ?? 0, column.get(edge.to)!));
    }
  }
  const lanes = new Map<number, { channel: string; row: number | null }>();
  const laneRuns: Array<Run> = [];
  for (const [parent, last] of farthest) {
    const first = column.get(parent)! + 1;
    let laneRow: number | null = null;
    for (let r = row.get(parent)! + 1; r < rowTops.length && laneRow === null; r++) {
      let free = true;
      for (let c = first; c < last; c++) if (occupied.has(`${c}:${r}`)) free = false;
      if (free) laneRow = r;
    }
    const channel = laneRow === null ? "lane:bottom" : `lane:${laneRow}`;
    lanes.set(parent, { channel, row: laneRow });
    laneRuns.push({ channel, owner: String(parent), from: first - 1, to: last - 1 });
  }
  const laneTrack = assignTracks(laneRuns);
  const laneY = (parent: number) => {
    const lane = lanes.get(parent)!;
    const track = laneTrack(lane.channel, String(parent));
    return lane.row === null
      ? contentBottom + ROW_GAP + track * TRACK_GAP
      : rowTops[lane.row]! + rowHeights[lane.row]! / 2 + offsetOf(LANE_TRACK_OFFSETS, track);
  };
  const treeRuns: Array<Run> = [];
  for (const edge of parentEdges.filter(forward)) {
    const from = nodeAt.get(edge.from)!;
    const to = nodeAt.get(edge.to)!;
    const [start, end] = [column.get(edge.from)!, column.get(edge.to)!];
    const dropY = from.y + from.height + ROW_GAP / 2;
    const owner = String(edge.from);
    if (end === start + 1) {
      treeRuns.push({ channel: `${start}`, owner, from: dropY, to: treeEntryY(to) });
    } else {
      const lane = laneY(edge.from);
      treeRuns.push({ channel: `${start}`, owner, from: dropY, to: lane });
      // Left of the trunks there, so their entries into children never cross it.
      treeRuns.push({
        channel: `${end - 1}`,
        owner,
        from: lane,
        to: treeEntryY(to),
        passing: true,
      });
    }
  }
  const treeTrack = assignTracks(treeRuns);
  const trackX = (gap: number, owner: number) =>
    gapLeft(gap) + half + offsetOf(TREE_TRACK_OFFSETS, treeTrack(`${gap}`, String(owner)));

  // Arrows into one card spread along its left edge, below a tree connector's entry, in the
  // order of their sources, so none ends under another's arrowhead.
  const blocksEdges = edges.filter((edge) => edge.kind === "blocks");
  const entryY = new Map<string, number>();
  const hasTreeEntry = new Set(parentEdges.map((edge) => edge.to));
  const incomingOf = new Map<number, Array<RawEdge>>();
  for (const edge of blocksEdges)
    incomingOf.set(edge.to, [...(incomingOf.get(edge.to) ?? []), edge]);
  for (const [target, incoming] of incomingOf) {
    const to = nodeAt.get(target)!;
    const top = hasTreeEntry.has(target) ? treeEntryY(to) : to.y;
    const sorted = incoming.toSorted((a, b) => {
      const [fa, fb] = [nodeAt.get(a.from)!, nodeAt.get(b.from)!];
      return fa.y + fa.height / 2 - (fb.y + fb.height / 2) || fa.x - fb.x || compareIds(a.id, b.id);
    });
    sorted.forEach((edge, position) => {
      entryY.set(edge.id, top + ((to.y + to.height - top) * (position + 1)) / (sorted.length + 1));
    });
  }
  // A backward arrow, or one skipping a column, crosses in the row gap above the higher of its two
  // cards, on its own track there.
  const crossesGap = (from: IssueGraphNode, to: IssueGraphNode) =>
    to.x < from.x + from.width || to.x - (from.x + from.width) > from.width + EDGE_BOW;
  const crossingRow = (edge: RawEdge) => Math.min(row.get(edge.from)!, row.get(edge.to)!);
  const arrowTrack = assignTracks(
    blocksEdges.flatMap((edge) => {
      const [from, to] = [nodeAt.get(edge.from)!, nodeAt.get(edge.to)!];
      if (!crossesGap(from, to)) return [];
      const sx = from.x + from.width;
      return [
        {
          channel: `${crossingRow(edge)}`,
          owner: edge.id,
          from: sx + EDGE_BOW,
          to: to.x - EDGE_BOW,
        },
      ];
    }),
  );

  let maxX = 0;
  let maxY = 0;
  const routed = edges.map((edge): IssueGraphEdge => {
    const from = nodeAt.get(edge.from)!;
    const to = nodeAt.get(edge.to)!;
    const base = { id: edge.id, from: from.id, to: to.id };
    if (edge.kind === "parent") {
      const start = column.get(edge.from)!;
      const end = column.get(edge.to)!;
      const dropX = from.x + from.width - TREE_DROP_INSET;
      const bottom = from.y + from.height;
      const dropY = bottom + ROW_GAP / 2;
      const ty = treeEntryY(to);
      let legs: string;
      if (end === start + 1) {
        legs = `H ${trackX(start, edge.from)} V ${ty}`;
      } else if (end > start) {
        const lane = laneY(edge.from);
        maxY = Math.max(maxY, lane);
        legs = `H ${trackX(start, edge.from)} V ${lane} H ${trackX(end - 1, edge.from)} V ${ty}`;
      } else {
        // Behind its parent, only possible in a parent cycle: over in the row gap above it.
        legs = `H ${from.x + from.width + half} V ${to.y - ROW_GAP / 2} H ${to.x - half} V ${ty}`;
      }
      maxX = Math.max(maxX, from.x + from.width + half);
      return { ...base, kind: "parent", path: `M ${dropX} ${bottom} V ${dropY} ${legs} H ${to.x}` };
    }
    const ty = entryY.get(edge.id)!;
    const gapY = crossesGap(from, to)
      ? rowTops[crossingRow(edge)]! -
        ROW_GAP / 2 +
        offsetOf(ARROW_TRACK_OFFSETS, arrowTrack(`${crossingRow(edge)}`, edge.id))
      : null;
    const { path, maxX: right } = edgePath(from, to, ty, gapY);
    maxX = Math.max(maxX, right);
    return {
      ...base,
      kind: "blocks",
      blockerOpen: items[edge.from]!.entry.state.category !== "closed",
      path,
    };
  });
  return { edges: routed, maxX, maxY };
}

function edgePath(
  from: IssueGraphNode,
  to: IssueGraphNode,
  ty: number,
  gapY: number | null,
): { readonly path: string; readonly maxX: number } {
  const sx = from.x + from.width;
  const sy = from.y + from.height / 2;
  const tx = to.x;
  if (gapY !== null) {
    // Backward or skipping a column: rise inside the column gap, cross in the row gap above,
    // then come down, so the edge never runs behind the cards it passes.
    const bow = EDGE_BOW / 2;
    return {
      path:
        `M ${sx} ${sy} C ${sx + bow} ${sy}, ${sx + bow} ${gapY}, ${sx + EDGE_BOW} ${gapY} ` +
        `L ${tx - EDGE_BOW} ${gapY} C ${tx - bow} ${gapY}, ${tx - bow} ${ty}, ${tx} ${ty}`,
      maxX: Math.max(sx + EDGE_BOW, tx),
    };
  }
  const pull = Math.max(EDGE_BOW / 2, (tx - sx) / 2);
  return { path: `M ${sx} ${sy} C ${sx + pull} ${sy}, ${tx - pull} ${ty}, ${tx} ${ty}`, maxX: tx };
}
