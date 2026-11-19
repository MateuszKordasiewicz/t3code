import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { issueEntryKey } from "@t3tools/client-runtime/state/issues";

export type IssueGraphGroupBy = "tree" | "type" | "status";

export interface IssueGraphNode {
  readonly id: string;
  readonly entry: EnvironmentIssueEntry;
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}

export interface IssueGraphEdge {
  readonly id: string;
  readonly from: string;
  readonly to: string;
  readonly kind: "parent" | "blocks";
  /** SVG path `d` attribute. */
  readonly path: string;
}

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

const DEFAULT_SIZE = { nodeWidth: 240, nodeHeight: 56 };
const REGION_PADDING = 12;
const RANK_GAP = 64;
const ROW_GAP = 12;
const GRID_COLUMNS = 4;
const EDGE_BOW = 48;

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
  size: { nodeWidth: number; nodeHeight: number } = DEFAULT_SIZE,
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
      const clusters =
        groupBy === "tree" ? treeClusters(members, byRef) : attributeClusters(members, groupBy);
      return {
        id,
        title,
        issueCount: members.length,
        regions: clusters.map((cluster) => placeRegion(id, members, edges, cluster, size)),
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

/** Ranks are longest paths from the sources; back edges found by a DFS in global order are ignored. */
function rankCluster(
  items: ReadonlyArray<Item>,
  members: ReadonlyArray<number>,
  edges: ReadonlyArray<RawEdge>,
): { ranks: Map<number, number>; predecessors: Map<number, Array<number>> } {
  const ordered = members.toSorted((a, b) => items[a]!.order - items[b]!.order);
  const successors = new Map<number, Array<number>>(ordered.map((index) => [index, []]));
  for (const edge of edges) successors.get(edge.from)?.push(edge.to);
  for (const next of successors.values()) next.sort((a, b) => items[a]!.order - items[b]!.order);

  const state = new Map<number, "active" | "done">();
  const finished: Array<number> = [];
  const dagEdges: Array<[number, number]> = [];
  for (const start of ordered) {
    if (state.has(start)) continue;
    const stack: Array<{ node: number; next: number }> = [{ node: start, next: 0 }];
    state.set(start, "active");
    while (stack.length > 0) {
      const frame = stack[stack.length - 1]!;
      const next = successors.get(frame.node)!;
      if (frame.next < next.length) {
        const to = next[frame.next++]!;
        const seen = state.get(to);
        if (seen === "active") continue;
        dagEdges.push([frame.node, to]);
        if (seen === undefined) {
          state.set(to, "active");
          stack.push({ node: to, next: 0 });
        }
      } else {
        state.set(frame.node, "done");
        finished.push(frame.node);
        stack.pop();
      }
    }
  }

  const predecessors = new Map<number, Array<number>>(ordered.map((index) => [index, []]));
  for (const [from, to] of dagEdges) predecessors.get(to)!.push(from);
  const ranks = new Map<number, number>();
  for (let i = finished.length - 1; i >= 0; i--) {
    const node = finished[i]!;
    let rank = 0;
    for (const from of predecessors.get(node)!) rank = Math.max(rank, ranks.get(from)! + 1);
    ranks.set(node, rank);
  }
  return { ranks, predecessors };
}

function placeRegion(
  projectId: string,
  items: ReadonlyArray<Item>,
  edges: ReadonlyArray<RawEdge>,
  cluster: PendingCluster,
  size: { nodeWidth: number; nodeHeight: number },
): IssueGraphRegion {
  const { nodeWidth, nodeHeight } = size;
  const memberSet = new Set(cluster.members);
  const regionEdges = edges.filter((edge) => memberSet.has(edge.from) && memberSet.has(edge.to));
  const linked = new Set<number>();
  for (const edge of regionEdges) linked.add(edge.from).add(edge.to);
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
    members.forEach((index, position) => {
      row.set(index, position);
      column.set(index, rank);
    });
  });
  // Issues with no link inside the region fill a grid beside the ranked part, not one tall column.
  const loose = cluster.members
    .filter((index) => !linked.has(index))
    .toSorted((a, b) => items[a]!.order - items[b]!.order);
  loose.forEach((index, position) => {
    row.set(index, Math.floor(position / GRID_COLUMNS));
    column.set(index, columns.length + (position % GRID_COLUMNS));
  });

  const columnCount = columns.length + Math.min(loose.length, GRID_COLUMNS);
  const rows = Math.max(
    Math.ceil(loose.length / GRID_COLUMNS),
    ...columns.map((members) => members.length),
  );
  const nodeAt = new Map<number, IssueGraphNode>();
  const nodes = [...columns.flat(), ...loose].map((index) => {
    const node: IssueGraphNode = {
      id: items[index]!.id,
      entry: items[index]!.entry,
      x: REGION_PADDING + column.get(index)! * (nodeWidth + RANK_GAP),
      y: REGION_PADDING + row.get(index)! * (nodeHeight + ROW_GAP),
      width: nodeWidth,
      height: nodeHeight,
    };
    nodeAt.set(index, node);
    return node;
  });
  let width = REGION_PADDING * 2 + columnCount * nodeWidth + (columnCount - 1) * RANK_GAP;
  const placedEdges = regionEdges.map((edge): IssueGraphEdge => {
    const from = nodeAt.get(edge.from)!;
    const to = nodeAt.get(edge.to)!;
    const { path, maxX } = edgePath(from, to);
    // A same-column bow leaves the last column; the region scrolls far enough to show it.
    width = Math.max(width, maxX + REGION_PADDING);
    return { id: edge.id, from: from.id, to: to.id, kind: edge.kind, path };
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
    width,
    height: REGION_PADDING * 2 + rows * nodeHeight + (rows - 1) * ROW_GAP,
    nodes,
    edges: placedEdges,
  };
}

function edgePath(
  from: IssueGraphNode,
  to: IssueGraphNode,
): { readonly path: string; readonly maxX: number } {
  const sx = from.x + from.width;
  const sy = from.y + from.height / 2;
  const ty = to.y + to.height / 2;
  if (to.x === from.x) {
    // Same column: leave and enter on the right, bowing out past both nodes.
    const bow = EDGE_BOW + Math.abs(ty - sy) / 4;
    return {
      path: `M ${sx} ${sy} C ${sx + bow} ${sy}, ${sx + bow} ${ty}, ${sx} ${ty}`,
      maxX: sx + bow,
    };
  }
  const tx = to.x;
  if (tx < sx || tx - sx > from.width + EDGE_BOW) {
    // Backward or skipping a column: rise inside the column gap, cross in the row gap above,
    // then come down, so the edge never runs behind the cards it passes.
    const gapY = Math.min(from.y, to.y) - ROW_GAP / 2;
    const half = EDGE_BOW / 2;
    return {
      path:
        `M ${sx} ${sy} C ${sx + half} ${sy}, ${sx + half} ${gapY}, ${sx + EDGE_BOW} ${gapY} ` +
        `L ${tx - EDGE_BOW} ${gapY} C ${tx - half} ${gapY}, ${tx - half} ${ty}, ${tx} ${ty}`,
      maxX: Math.max(sx + EDGE_BOW, tx),
    };
  }
  const pull = Math.max(EDGE_BOW / 2, (tx - sx) / 2);
  return { path: `M ${sx} ${sy} C ${sx + pull} ${sy}, ${tx - pull} ${ty}, ${tx} ${ty}`, maxX: tx };
}
