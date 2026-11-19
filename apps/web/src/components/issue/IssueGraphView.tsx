import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { msg, useGT, useMessages } from "gt-react";
import { ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { useId, useState } from "react";

import { cn } from "~/lib/utils";

import { PULL_REQUEST_ROW_NUMBER_CLASS } from "../pullRequest/PullRequestListRow";
import type { IssueRowTarget } from "./IssueRow";
import {
  layoutIssueGraph,
  type IssueGraphGroupBy,
  type IssueGraphNode,
  type IssueGraphRegion,
} from "./issueGraph.logic";
import {
  readCollapsedIssueGraphRows,
  toggleCollapsedIssueGraphRow,
  writeCollapsedIssueGraphRows,
  type CollapsedIssueGraphRows,
} from "./issueGraphCollapsedRows";
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

function RegionLabel({ region }: { region: IssueGraphRegion }) {
  const gt = useGT();
  const resolve = useMessages();
  if (region.kind === "tree") {
    return (
      <>
        <span
          className={cn(
            PULL_REQUEST_ROW_NUMBER_CLASS,
            "rounded border border-border bg-background px-1 leading-4",
          )}
        >
          {region.root?.displayKey ?? region.label}
        </span>
        <span className="truncate text-foreground">
          {region.root ? region.root.title : gt("Parent not in this list")}
        </span>
      </>
    );
  }
  const type = region.label.toLowerCase();
  const known = TYPE_NAMES[type];
  const text =
    region.kind === "status"
      ? region.label
      : region.kind === "type"
        ? known
          ? resolve(known.plural)
          : region.label
            ? capitalise(region.label)
            : gt("No type", { $context: "issue type" })
        : known
          ? resolve(known.loose)
          : region.label
            ? gt("Loose {type} issues", { type: region.label })
            : gt("Other issues");
  return <span className="truncate text-foreground">{text}</span>;
}

function IssueCount({ count }: { count: number }) {
  const gt = useGT();
  return (
    <span className="shrink-0 text-xs font-normal text-muted-foreground">
      · {gt("{count, plural, one {{count} issue} other {{count} issues}}", { count })}
    </span>
  );
}

/** Project rows hold region rows; both are full-width disclosure rows over one 1px divider. */
const ROW_BUTTON_CLASS =
  "flex w-full min-w-0 items-center gap-1.5 border-b text-left outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset";

/**
 * The issues the list would show, as a table of project rows and, inside each, region rows of
 * cards with their parent and blocking links drawn between them. Selection works as in the list.
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
  const [collapsed, setCollapsed] = useState(readCollapsedIssueGraphRows);
  const projects = layoutIssueGraph(entries, groupBy);
  if (projects.length === 0) return null;
  const toggle = (list: keyof CollapsedIssueGraphRows, rowId: string) => {
    const next = toggleCollapsedIssueGraphRow(collapsed, list, rowId);
    setCollapsed(next);
    writeCollapsedIssueGraphRows(next);
  };

  return (
    <div role="group" aria-label={gt("Issue graph")} className="border-t border-border">
      {projects.map((project) => {
        const projectOpen = !collapsed.projects.includes(project.id);
        const Chevron = projectOpen ? ChevronDownIcon : ChevronRightIcon;
        return (
          <section key={project.id}>
            <h3>
              <button
                type="button"
                aria-expanded={projectOpen}
                onClick={() => toggle("projects", project.id)}
                className={cn(
                  ROW_BUTTON_CLASS,
                  "h-8 border-primary/25 bg-primary/8 px-2 text-sm font-semibold text-foreground hover:bg-primary/12",
                )}
              >
                <Chevron aria-hidden className="size-4 shrink-0 text-primary" />
                <span className="truncate">{project.title}</span>
                <IssueCount count={project.issueCount} />
              </button>
            </h3>
            {projectOpen
              ? project.regions.map((region) => (
                  <IssueGraphRegionRow
                    key={region.id}
                    region={region}
                    open={!collapsed[groupBy].includes(region.id)}
                    onToggle={() => toggle(groupBy, region.id)}
                    showType={groupBy !== "type"}
                    selectedId={selectedId}
                    onSelect={onSelect}
                  />
                ))
              : null}
          </section>
        );
      })}
    </div>
  );
}

function IssueGraphRegionRow({
  region,
  open,
  onToggle,
  showType,
  selectedId,
  onSelect,
}: {
  region: IssueGraphRegion;
  open: boolean;
  onToggle: () => void;
  showType: boolean;
  selectedId: string | null;
  onSelect: (entry: IssueRowTarget) => void;
}) {
  const markerId = useId();
  const Chevron = open ? ChevronDownIcon : ChevronRightIcon;
  const touchesSelection = (edge: { from: string; to: string }) =>
    selectedId !== null && (edge.from === selectedId || edge.to === selectedId);
  // Edges at the selected issue are drawn last, so they sit on top.
  const edges = region.edges.toSorted(
    (a, b) => Number(touchesSelection(a)) - Number(touchesSelection(b)),
  );
  return (
    <>
      <h4>
        {/* Indented so the region's chevron lines up under its project's title. */}
        <button
          type="button"
          aria-expanded={open}
          onClick={onToggle}
          className={cn(
            ROW_BUTTON_CLASS,
            "h-7 border-border bg-muted/50 pr-2 pl-7.5 text-xs font-medium text-muted-foreground hover:bg-muted",
          )}
        >
          <Chevron aria-hidden className="size-4 shrink-0" />
          <RegionLabel region={region} />
          <IssueCount count={region.issueCount} />
        </button>
      </h4>
      {open ? (
        // Only a region wider than the page scrolls, and only sideways: its height is its content.
        <div className="overflow-x-auto border-b border-border">
          <div className="relative" style={{ width: region.width, height: region.height }}>
            <svg
              aria-hidden
              className="pointer-events-none absolute inset-0"
              width={region.width}
              height={region.height}
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
            {region.nodes.map((node) => (
              <IssueGraphCard
                key={node.id}
                node={node}
                showType={showType}
                selected={node.id === selectedId}
                onSelect={onSelect}
              />
            ))}
          </div>
        </div>
      ) : null}
    </>
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
