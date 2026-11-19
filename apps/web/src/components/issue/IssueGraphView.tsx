import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { msg, useGT, useMessages } from "gt-react";
import { useId } from "react";

import { cn } from "~/lib/utils";

import { PULL_REQUEST_ROW_NUMBER_CLASS } from "../pullRequest/PullRequestListRow";
import type { IssueRowTarget } from "./IssueRow";
import {
  layoutIssueGraph,
  type IssueGraphCluster,
  type IssueGraphGroupBy,
  type IssueGraphNode,
} from "./issueGraph.logic";
import { IssueStateGlyph } from "./issuePresentation";
import {
  IssueThreadStatusBoxSpacer,
  IssueThreadStatusBoxView,
  useIssueThreadStatus,
} from "./IssueThreadStatusBox";

/** The contract's most, so a whole beads project usually lands in one page. */
export const ISSUE_GRAPH_PAGE_SIZE = 200;

const TYPE_NAMES: Partial<Record<string, { readonly plural: string; readonly loose: string }>> = {
  epic: { plural: msg("Epics"), loose: msg("Loose epics") },
  feature: { plural: msg("Features"), loose: msg("Loose features") },
  bug: { plural: msg("Bugs"), loose: msg("Loose bugs") },
  task: { plural: msg("Tasks"), loose: msg("Loose tasks") },
  chore: { plural: msg("Chores"), loose: msg("Loose chores") },
};

const capitalise = (text: string) => text.charAt(0).toUpperCase() + text.slice(1);

function ClusterLabel({
  cluster,
  root,
}: {
  cluster: IssueGraphCluster;
  root: IssueGraphNode | undefined;
}) {
  const gt = useGT();
  const resolve = useMessages();
  if (cluster.kind === "tree") {
    return (
      <>
        <span
          className={cn(
            PULL_REQUEST_ROW_NUMBER_CLASS,
            "rounded border border-border bg-muted/40 px-1 leading-4",
          )}
        >
          {root?.entry.displayKey ?? cluster.label}
        </span>
        <span className="truncate text-foreground">
          {root ? root.entry.title : gt("Parent not in this list")}
        </span>
      </>
    );
  }
  const type = cluster.label.toLowerCase();
  const known = TYPE_NAMES[type];
  const text =
    cluster.kind === "status"
      ? cluster.label
      : cluster.kind === "type"
        ? known
          ? resolve(known.plural)
          : cluster.label
            ? capitalise(cluster.label)
            : gt("No type", { $context: "issue type" })
        : known
          ? resolve(known.loose)
          : cluster.label
            ? gt("Loose {type} issues", { type: cluster.label })
            : gt("Other issues");
  return <span className="truncate text-foreground">{text}</span>;
}

/**
 * The issues the list would show, laid out as clusters of cards with their parent and blocking
 * links drawn between them. Selection works as in the list.
 */
export function IssueGraphView({
  entries,
  groupBy,
  selectedId,
  onSelect,
}: {
  entries: ReadonlyArray<EnvironmentIssueEntry>;
  groupBy: IssueGraphGroupBy;
  /** `environmentId:issueEntryKey` of the issue open in the panel, the layout's node id. */
  selectedId: string | null;
  onSelect: (entry: IssueRowTarget) => void;
}) {
  const gt = useGT();
  const markerId = useId();
  const layout = layoutIssueGraph(entries, groupBy);
  if (layout.nodes.length === 0) return null;
  const nodesById = new Map(layout.nodes.map((node) => [node.id, node] as const));
  const touchesSelection = (edge: { from: string; to: string }) =>
    selectedId !== null && (edge.from === selectedId || edge.to === selectedId);
  // Edges at the selected issue are drawn last, so they sit on top.
  const edges = layout.edges.toSorted(
    (a, b) => Number(touchesSelection(a)) - Number(touchesSelection(b)),
  );

  return (
    <div
      role="group"
      aria-label={gt("Issue graph")}
      className="max-h-[calc(100dvh-10rem)] overflow-auto rounded-lg border border-border"
    >
      <div className="relative min-w-full" style={{ width: layout.width, height: layout.height }}>
        {layout.clusters.map((cluster) => (
          <section
            key={cluster.id}
            className="absolute inset-x-0 border-y border-border/50 bg-muted/25"
            style={{ top: cluster.y, height: cluster.height }}
          >
            {/* Sticky, so the label stays in view while the graph scrolls sideways. */}
            <h3 className="sticky left-0 flex h-7 w-fit max-w-full min-w-0 items-center gap-1.5 px-3 text-xs font-medium text-muted-foreground">
              <ClusterLabel
                cluster={cluster}
                root={cluster.rootNodeId === null ? undefined : nodesById.get(cluster.rootNodeId)}
              />
              <span className="shrink-0 font-normal">
                ·{" "}
                {gt("{count, plural, one {{count} issue} other {{count} issues}}", {
                  count: cluster.nodeIds.length,
                })}
              </span>
            </h3>
          </section>
        ))}
        <svg
          aria-hidden
          className="pointer-events-none absolute inset-0 overflow-visible"
          width={layout.width}
          height={layout.height}
        >
          <defs>
            {(["muted", "selected"] as const).map((tone) => (
              <marker
                key={tone}
                id={`${markerId}-${tone}`}
                viewBox="0 0 8 8"
                refX="7"
                refY="4"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path
                  d="M 0 0 L 8 4 L 0 8 z"
                  className={tone === "selected" ? "fill-primary" : "fill-muted-foreground"}
                />
              </marker>
            ))}
          </defs>
          {edges.map((edge) => {
            const selected = touchesSelection(edge);
            return (
              <path
                key={edge.id}
                d={edge.path}
                fill="none"
                strokeWidth={selected ? 2 : 1.25}
                strokeDasharray={edge.kind === "parent" ? "4 4" : undefined}
                markerEnd={
                  edge.kind === "blocks"
                    ? `url(#${markerId}-${selected ? "selected" : "muted"})`
                    : undefined
                }
                className={cn(
                  selected
                    ? "stroke-primary"
                    : edge.kind === "blocks"
                      ? "stroke-muted-foreground/70"
                      : "stroke-muted-foreground/35",
                )}
              />
            );
          })}
        </svg>
        {layout.nodes.map((node) => (
          <IssueGraphCard
            key={node.id}
            node={node}
            showType={groupBy !== "type"}
            selected={node.id === selectedId}
            onSelect={onSelect}
          />
        ))}
      </div>
    </div>
  );
}

function IssueGraphCard({
  node,
  showType,
  selected,
  onSelect,
}: {
  node: IssueGraphNode;
  showType: boolean;
  selected: boolean;
  onSelect: (entry: IssueRowTarget) => void;
}) {
  const gt = useGT();
  const { entry } = node;
  const blockers = entry.dependsOnKeys ?? [];
  const threadStatus = useIssueThreadStatus(entry.environmentId, entry);
  return (
    <div
      className="absolute"
      style={{ left: node.x, top: node.y, width: node.width, height: node.height }}
    >
      <button
        type="button"
        aria-current={selected ? "true" : undefined}
        onClick={() => onSelect(entry)}
        className={cn(
          "flex size-full flex-col justify-center gap-1 rounded-lg border bg-background px-2.5 text-left shadow-xs/5 outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
          selected
            ? "border-primary bg-accent ring-1 ring-primary ring-inset"
            : "border-border hover:bg-accent",
          entry.state.category === "closed" && !selected ? "opacity-70" : null,
        )}
      >
        <span className="flex min-w-0 items-center gap-1.5">
          <IssueStateGlyph state={entry.state} className="size-3.5" />
          <span
            className={cn(
              PULL_REQUEST_ROW_NUMBER_CLASS,
              "rounded border border-border bg-muted/40 px-1 leading-4",
            )}
          >
            {entry.displayKey}
          </span>
          <span className="min-w-0 flex-1" />
          {showType && entry.issueType ? (
            <span className="truncate text-2xs text-muted-foreground">{entry.issueType}</span>
          ) : null}
          {entry.priority === undefined ? null : (
            <span className="shrink-0 rounded bg-muted px-1 font-mono text-2xs leading-4 text-muted-foreground">
              <span aria-hidden>P{entry.priority}</span>
              <span className="sr-only">
                {gt("Priority {priority}", { priority: entry.priority })}
              </span>
            </span>
          )}
        </span>
        <span className="flex min-w-0 items-center gap-1.5">
          <span className="min-w-0 flex-1 truncate text-sm text-foreground">{entry.title}</span>
          {/* Holds the title clear of the box, which cannot sit inside this button. */}
          {threadStatus ? <IssueThreadStatusBoxSpacer box={threadStatus} /> : null}
        </span>
        {blockers.length > 0 ? (
          <span className="sr-only">{gt("Blocked by {keys}", { keys: blockers.join(", ") })}</span>
        ) : null}
      </button>
      {threadStatus ? (
        // On the title line at any text size: 1px border plus px-2.5 across, and below the 2.5rem
        // of content centred in the card's fixed height.
        <span
          className="absolute flex"
          style={{
            right: "calc(1px + 0.625rem)",
            bottom: `calc((${node.height}px - 2.5rem) / 2)`,
          }}
        >
          <IssueThreadStatusBoxView box={threadStatus} />
        </span>
      ) : null}
    </div>
  );
}
