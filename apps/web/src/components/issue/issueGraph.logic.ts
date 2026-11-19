import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { issueEntryKey } from "@t3tools/client-runtime/state/issues";

export type IssueGraphGroupBy = "tree" | "type" | "status";

/** Blockers of a card with no arrow to it: outside its region, or with no clean route here. */
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
  /**
   * Blocker to blocked issue, ending in an arrowhead: straight along a chain, bent through the gap
   * before the blocked card, or an arc through the right-hand gutter.
   */
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
// Gap between cards of one dependency chain; it widens when its bent arrows need more tracks.
const CHAIN_GAP = 32;
const TRACK_GAP = 8;
const MAX_GAP_TRACKS = 6;
/** A row holds at most this many chained cards, so a long chain wraps instead of widening. */
const MAX_CHAIN = 6;
// Arcs leave the widest card by GUTTER_GAP before their first level; levels sit LEVEL_GAP apart.
const GUTTER_GAP = 16;
const LEVEL_GAP = 8;
// Link ends on one card edge sit further apart than an 8px arrowhead, and clear of its corners.
const ENDPOINT_GAP = 12;
const ENDPOINT_MARGIN = 8;
const CORNER_RADIUS = 4;

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
  drawn: ReadonlySet<number> | undefined,
): IssueGraphExternalBlockers | null {
  const outside = blockers.filter(({ index }) => index === undefined || !drawn?.has(index));
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

type Route = "chain" | "bend" | "arc" | "label";

function pushTo<K, V>(map: Map<K, Array<V>>, key: K, value: V) {
  const list = map.get(key);
  if (list === undefined) map.set(key, [value]);
  else list.push(value);
}

/** Levels in the right-hand gutter: the shortest span takes the lowest level free in its rows. */
function assignArcLevels(
  arcs: ReadonlyArray<{ readonly low: number; readonly high: number; level: number }>,
  rowCount: number,
): number {
  let words = 1;
  let levelCount = 0;
  let used = Array.from({ length: rowCount }, () => new Uint32Array(words));
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
  return levelCount;
}

/**
 * Linked issues form an indented tree: each parent, then its sub-tasks, joined by folder-tree
 * lines on the left. A sibling an issue blocks sits to its right, on its row when it can follow it
 * in a chain, else on its own row in the next column with a bent arrow. Other blocking links are
 * arcs through a gutter on the right. Issues with no link in the region fill a grid below.
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
  const byOrder = (a: number, b: number) => order(a) - order(b);
  const memberSet = new Set(cluster.members);
  const regionEdges = edges.filter((edge) => memberSet.has(edge.from) && memberSet.has(edge.to));
  const linked = new Set<number>();
  const parentOf = new Map<number, number>();
  const blocks = new Map<number, Array<number>>();
  for (const edge of regionEdges) {
    linked.add(edge.from).add(edge.to);
    if (edge.kind === "blocks") pushTo(blocks, edge.from, edge.to);
    else if (!parentOf.has(edge.to)) parentOf.set(edge.to, edge.from);
  }
  const listed = [...linked].toSorted(byOrder);
  const childrenOf = new Map<number, Array<number>>();
  for (const index of listed) {
    const parent = parentOf.get(index);
    if (parent !== undefined) pushTo(childrenOf, parent, index);
  }

  // The tree the list follows. A parent cycle has no top; its earliest card starts at the top level.
  const treeParent = new Map<number, number>();
  const reached = new Set<number>();
  const visit = (root: number) => {
    reached.add(root);
    const stack = [root];
    while (stack.length > 0) {
      const index = stack.pop()!;
      for (const child of childrenOf.get(index) ?? []) {
        if (reached.has(child)) continue;
        reached.add(child);
        treeParent.set(child, index);
        stack.push(child);
      }
    }
  };
  for (const index of listed) if (!parentOf.has(index)) visit(index);
  for (const index of listed) if (!reached.has(index)) visit(index);
  // Sibling groups, keyed by their parent; -1 holds the top level.
  const groupOf = (index: number) => treeParent.get(index) ?? -1;
  const groups = new Map<number, Array<number>>();
  for (const index of listed) pushTo(groups, groupOf(index), index);
  const siblingRank = new Map<number, number>();
  const orderedGroups = new Map<number, Array<number>>();
  for (const [key, group] of groups) {
    const ordered = orderSiblings(group, blocks, order);
    orderedGroups.set(key, ordered);
    ordered.forEach((index, rank) => siblingRank.set(index, rank));
  }

  // A link between siblings in order can run sideways; the rest, links between cousins and links
  // closing a cycle, are arcs, and a card with an arc keeps the row to its right free for it.
  const route = new Map<RawEdge, Route>();
  const arcCards = new Set<number>();
  const sidewaysInto = new Map<number, Array<RawEdge>>();
  for (const edge of regionEdges) {
    if (edge.kind !== "blocks") continue;
    if (
      groupOf(edge.from) === groupOf(edge.to) &&
      siblingRank.get(edge.from)! < siblingRank.get(edge.to)!
    ) {
      pushTo(sidewaysInto, edge.to, edge);
    } else {
      route.set(edge, "arc");
      arcCards.add(edge.from).add(edge.to);
    }
  }

  // Each group lays out its own rows; a card's slot is its row in the group and its chain column.
  const slot = new Map<number, { readonly row: number; readonly column: number }>();
  const followerOf = new Map<number, number>();
  const rowsOf = new Map<number, Array<Array<number>>>();
  const tracksOf = new Map<number, Map<number, number>>();
  const bends: Array<RawEdge> = [];
  const hasChildren = (index: number) => groups.has(index);
  for (const [key, ordered] of orderedGroups) {
    const rows: Array<Array<number>> = [];
    const rowHasChildren: Array<boolean> = [];
    const tracks = new Map<number, number>();
    const spans: Array<readonly [number, number]> = [];
    const farthestBend = new Map<number, number>();
    for (const card of ordered) {
      const sources = sidewaysInto.get(card) ?? [];
      let lead: RawEdge | undefined;
      if (sources.length > 0) {
        // It follows the blocker last in row order, when that one is free to take it.
        lead = sources.reduce((best, edge) => {
          const [a, b] = [slot.get(edge.from)!, slot.get(best.from)!];
          return a.row > b.row || (a.row === b.row && a.column > b.column) ? edge : best;
        });
        const at = slot.get(lead.from)!;
        const free =
          !followerOf.has(lead.from) &&
          !arcCards.has(lead.from) &&
          rows[at.row]!.length < MAX_CHAIN &&
          (farthestBend.get(lead.from) ?? 0) <= at.column + 1 &&
          // Its sub-tasks would come between the rows a bent arrow already spans.
          (!hasChildren(card) || !spans.some(([low, high]) => low <= at.row && at.row < high));
        if (free) {
          slot.set(card, { row: at.row, column: at.column + 1 });
          rows[at.row]!.push(card);
          followerOf.set(lead.from, card);
          route.set(lead, "chain");
          if (hasChildren(card)) rowHasChildren[at.row] = true;
        } else {
          lead = undefined;
        }
      }
      let wrapped = false;
      if (!slot.has(card)) {
        const right = Math.max(-1, ...sources.map((edge) => slot.get(edge.from)!.column));
        wrapped = right + 1 >= MAX_CHAIN;
        slot.set(card, { row: rows.length, column: wrapped ? 0 : right + 1 });
        rows.push([card]);
        rowHasChildren.push(hasChildren(card));
      }
      const at = slot.get(card)!;
      for (const edge of sources) {
        if (edge === lead) continue;
        const from = slot.get(edge.from)!;
        const used = tracks.get(at.column) ?? 0;
        // A bent arrow runs right from its blocker into the gap before this card's column, down
        // that gap past rows of this group only, and into the card.
        const clear =
          at.column > 0 &&
          from.row < at.row &&
          from.column < at.column &&
          (from.column === at.column - 1 || !followerOf.has(edge.from)) &&
          used < MAX_GAP_TRACKS &&
          rowHasChildren.slice(from.row, at.row).every((children) => !children);
        if (clear) {
          route.set(edge, "bend");
          bends.push(edge);
          tracks.set(at.column, used + 1);
          spans.push([from.row, at.row]);
          farthestBend.set(edge.from, Math.max(farthestBend.get(edge.from) ?? 0, at.column));
        } else if (!followerOf.has(edge.from) && !wrapped) {
          // A card that wrapped to a new row keeps its right edge free to go on chaining there.
          route.set(edge, "arc");
          arcCards.add(edge.from).add(card);
        } else {
          route.set(edge, "label");
        }
      }
    }
    rowsOf.set(key, rows);
    tracksOf.set(key, tracks);
  }

  // Rows in tree order: a group's row, then the sub-tasks of its cards, rightmost card first, so
  // each trunk drops straight into its own sub-tasks and passes only cards to its right.
  const rowList: Array<Array<number>> = [];
  const emit = (key: number) => {
    for (const row of rowsOf.get(key) ?? []) {
      rowList.push(row);
      for (let i = row.length - 1; i >= 0; i--) if (groups.has(row[i]!)) emit(row[i]!);
    }
  };
  emit(-1);
  const rowOf = new Map<number, number>();
  rowList.forEach((row, index) => {
    for (const card of row) rowOf.set(card, index);
  });

  const drawnInto = new Map<number, Set<number>>();
  for (const [edge, kind] of route) {
    if (kind === "label") continue;
    let drawn = drawnInto.get(edge.to);
    if (drawn === undefined) drawnInto.set(edge.to, (drawn = new Set()));
    drawn.add(edge.from);
  }
  const external = new Map(
    cluster.members.map((index) => [
      index,
      externalBlockersOf(items, blockers[index]!, drawnInto.get(index)),
    ]),
  );

  const arcs = [...route]
    .filter(([, kind]) => kind === "arc")
    .map(([edge]) => {
      const [a, b] = [rowOf.get(edge.from)!, rowOf.get(edge.to)!];
      return { edge, low: Math.min(a, b), high: Math.max(a, b), level: 0 };
    })
    .toSorted(
      (a, b) =>
        a.high - a.low - (b.high - b.low) || a.low - b.low || compareIds(a.edge.id, b.edge.id),
    );
  const levelCount = assignArcLevels(arcs, rowList.length);
  const levelOf = new Map(arcs.map((arc) => [arc.edge, arc.level]));

  // Link ends along each card edge, top to bottom. On the right: arcs (up, inner first; then down,
  // outer first), the chain arrow, then bent arrows by target row. On the left: bent arrows by
  // source row, then the chain arrow. In that order no two links of one card cross.
  type End = { readonly edge: RawEdge; readonly anchor: boolean };
  const rightEnds = new Map<number, Array<End>>();
  const leftEnds = new Map<number, Array<End>>();
  const goesUp = (edge: RawEdge, from: number) =>
    rowOf.get(edge.from === from ? edge.to : edge.from)! < rowOf.get(from)!;
  for (const arc of arcs) {
    pushTo(rightEnds, arc.edge.from, { edge: arc.edge, anchor: false });
    pushTo(rightEnds, arc.edge.to, { edge: arc.edge, anchor: false });
  }
  for (const [card, list] of rightEnds) {
    list.sort((a, b) => {
      const [upA, upB] = [goesUp(a.edge, card), goesUp(b.edge, card)];
      const [la, lb] = [levelOf.get(a.edge)!, levelOf.get(b.edge)!];
      return (
        Number(!upA) - Number(!upB) || (upA ? la - lb : lb - la) || compareIds(a.edge.id, b.edge.id)
      );
    });
  }
  for (const [edge, kind] of route) {
    if (kind !== "chain") continue;
    pushTo(rightEnds, edge.from, { edge, anchor: true });
    pushTo(leftEnds, edge.to, { edge, anchor: true });
  }
  const byRows = (a: number, b: number, c: number, d: number) => a - b || c - d;
  for (const edge of bends.toSorted(
    (a, b) =>
      byRows(rowOf.get(a.to)!, rowOf.get(b.to)!, rowOf.get(a.from)!, rowOf.get(b.from)!) ||
      compareIds(a.id, b.id),
  )) {
    pushTo(rightEnds, edge.from, { edge, anchor: false });
  }
  for (const edge of bends.toSorted(
    (a, b) => byRows(rowOf.get(a.from)!, rowOf.get(b.from)!, 0, 0) || compareIds(a.id, b.id),
  )) {
    pushTo(leftEnds, edge.to, { edge, anchor: false });
  }
  for (const list of leftEnds.values()) list.sort((a, b) => Number(a.anchor) - Number(b.anchor));
  const spread = (list: ReadonlyArray<End>) => {
    const anchor = list.findIndex((end) => end.anchor);
    const centre = anchor === -1 ? (list.length - 1) / 2 : anchor;
    const reach = Math.max(centre, list.length - 1 - centre);
    return {
      need: 2 * reach * ENDPOINT_GAP + 2 * ENDPOINT_MARGIN,
      offsets: new Map(list.map((end, i) => [end.edge, (i - centre) * ENDPOINT_GAP])),
    };
  };
  const rightSpread = new Map([...rightEnds].map(([card, list]) => [card, spread(list)]));
  const leftSpread = new Map([...leftEnds].map(([card, list]) => [card, spread(list)]));
  const cardHeight = (index: number) =>
    Math.max(
      nodeHeight + (external.get(index) === null ? 0 : labelHeight),
      rightSpread.get(index)?.need ?? 0,
      leftSpread.get(index)?.need ?? 0,
    );

  // Columns of a group start at its parent's indent; a gap widens for its bent-arrow tracks.
  const gapWidth = (key: number, column: number) =>
    Math.max(CHAIN_GAP, ((tracksOf.get(key)?.get(column) ?? 0) + 1) * TRACK_GAP);
  const xOf = new Map<number, number>();
  const xFor = (index: number): number => {
    const known = xOf.get(index);
    if (known !== undefined) return known;
    const key = groupOf(index);
    let x = key === -1 ? REGION_PADDING : xFor(key) + INDENT;
    for (let column = 1; column <= slot.get(index)!.column; column++) {
      x += nodeWidth + gapWidth(key, column);
    }
    xOf.set(index, x);
    return x;
  };

  const nodeAt = new Map<number, IssueGraphNode>();
  const place = (index: number, x: number, y: number, height: number) => {
    nodeAt.set(index, {
      id: items[index]!.id,
      entry: items[index]!.entry,
      x,
      y,
      width: nodeWidth,
      height,
      externalBlockers: external.get(index)!,
    });
  };
  let bottom = REGION_PADDING - ROW_GAP;
  for (const row of rowList) {
    // One height per row, so a chain's arrows run straight between the middles of its cards.
    const height = Math.max(...row.map(cardHeight));
    for (const card of row) place(card, xFor(card), bottom + ROW_GAP, height);
    bottom += ROW_GAP + height;
  }
  const listRight = Math.max(0, ...listed.map((index) => nodeAt.get(index)!.x + nodeWidth));
  const gutterX = (level: number) => listRight + GUTTER_GAP + level * LEVEL_GAP;

  const loose = cluster.members.filter((index) => !linked.has(index)).toSorted(byOrder);
  for (let start = 0; start < loose.length; start += GRID_COLUMNS) {
    const line = loose.slice(start, start + GRID_COLUMNS);
    const height = Math.max(...line.map(cardHeight));
    line.forEach((index, column) => {
      const x = REGION_PADDING + column * (nodeWidth + GRID_GAP);
      place(index, x, bottom + ROW_GAP, cardHeight(index));
    });
    bottom += ROW_GAP + height;
  }

  const mid = (node: IssueGraphNode) => node.y + node.height / 2;
  const rightY = (index: number, edge: RawEdge) =>
    mid(nodeAt.get(index)!) + rightSpread.get(index)!.offsets.get(edge)!;
  const leftY = (index: number, edge: RawEdge) =>
    mid(nodeAt.get(index)!) + leftSpread.get(index)!.offsets.get(edge)!;
  // Tracks in one gap: the arrow into the earliest row sits rightmost, so none crosses another
  // arrow of the same blocker or the same blocked card.
  const trackX = new Map<RawEdge, number>();
  const byGap = new Map<string, Array<RawEdge>>();
  for (const edge of bends) pushTo(byGap, `${groupOf(edge.to)}:${slot.get(edge.to)!.column}`, edge);
  for (const gap of byGap.values()) {
    gap
      .toSorted(
        (a, b) =>
          byRows(rowOf.get(a.to)!, rowOf.get(b.to)!, rowOf.get(a.from)!, rowOf.get(b.from)!) ||
          compareIds(a.id, b.id),
      )
      .forEach((edge, track) => trackX.set(edge, nodeAt.get(edge.to)!.x - (track + 1) * TRACK_GAP));
  }

  const placedEdges: Array<IssueGraphEdge> = [];
  for (const edge of regionEdges) {
    const from = nodeAt.get(edge.from)!;
    const to = nodeAt.get(edge.to)!;
    const base = { id: edge.id, from: from.id, to: to.id };
    if (edge.kind === "parent") {
      // Only cards that start a row at their parent's indent hang off its trunk.
      if (treeParent.get(edge.to) !== edge.from || slot.get(edge.to)!.column !== 0) continue;
      const entry = to.y + Math.min(TREE_ENTRY, to.height / 2);
      const path = `M ${from.x + TRUNK_INSET} ${from.y + from.height} V ${entry} H ${to.x}`;
      placedEdges.push({ ...base, kind: "parent", path });
      continue;
    }
    const kind = route.get(edge)!;
    if (kind === "label") continue;
    const blockerOpen = items[edge.from]!.entry.state.category !== "closed";
    const sx = from.x + from.width;
    let path: string;
    if (kind === "chain") {
      path = `M ${sx} ${rightY(edge.from, edge)} H ${to.x}`;
    } else if (kind === "bend") {
      const [y1, y2] = [rightY(edge.from, edge), leftY(edge.to, edge)];
      const x = trackX.get(edge)!;
      const r = Math.min(CORNER_RADIUS, (y2 - y1) / 2, x - sx, to.x - x);
      path =
        `M ${sx} ${y1} H ${x - r} Q ${x} ${y1} ${x} ${y1 + r} ` +
        `V ${y2 - r} Q ${x} ${y2} ${x + r} ${y2} H ${to.x}`;
    } else {
      const [y1, y2] = [rightY(edge.from, edge), rightY(edge.to, edge)];
      const x = gutterX(levelOf.get(edge)!);
      const turn = Math.sign(y2 - y1) * Math.min(CORNER_RADIUS, Math.abs(y2 - y1) / 2);
      const r = Math.abs(turn);
      path =
        `M ${sx} ${y1} H ${x - r} Q ${x} ${y1} ${x} ${y1 + turn} ` +
        `V ${y2 - turn} Q ${x} ${y2} ${x - r} ${y2} H ${to.x + to.width}`;
    }
    placedEdges.push({ ...base, kind: "blocks", blockerOpen, path });
  }

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
    nodes: [...rowList.flat(), ...loose].map((index) => nodeAt.get(index)!),
    edges: placedEdges,
  };
}
