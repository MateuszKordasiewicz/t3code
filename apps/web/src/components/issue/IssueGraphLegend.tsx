import { useGT } from "gt-react";

import { cn } from "~/lib/utils";

import type { IssueGraphEdge } from "./issueGraph.logic";

export type IssueGraphLinkTone = "tree" | "open" | "closed" | "selected";

/** Tree connectors draw opaque inside a half-opacity group, so siblings' shared trunk stays even. */
export const TREE_LINK_GROUP_CLASS = "opacity-50";

export const LINK_STROKE_CLASS: Record<IssueGraphLinkTone, string> = {
  tree: "stroke-muted-foreground",
  open: "stroke-warning",
  closed: "stroke-muted-foreground/70",
  selected: "stroke-primary",
};

export const ARROW_FILL_CLASS: Record<Exclude<IssueGraphLinkTone, "tree">, string> = {
  open: "fill-warning",
  closed: "fill-muted-foreground",
  selected: "fill-primary",
};

export function linkTone(edge: IssueGraphEdge, selected: boolean): IssueGraphLinkTone {
  if (selected) return "selected";
  if (edge.kind === "parent") return "tree";
  return edge.blockerOpen ? "open" : "closed";
}

function DependencyMark({ tone }: { tone: "open" | "closed" }) {
  return (
    <svg aria-hidden width="16" height="14" viewBox="0 0 16 14" className="shrink-0">
      <path
        d="M 1 2 H 13 V 10 H 6"
        fill="none"
        strokeWidth={1.25}
        className={LINK_STROKE_CLASS[tone]}
      />
      <path d="M 7 6.5 L 1 10 L 7 13.5 z" className={ARROW_FILL_CLASS[tone]} />
    </svg>
  );
}

/** Names the link marks drawn between the graph's cards. */
export function IssueGraphLegend() {
  const gt = useGT();
  const item = "flex min-w-0 items-center gap-1.5";
  return (
    // Wraps onto more lines in a narrow pane, and a long label wraps beside its mark, rather than
    // cutting off a translation.
    <div className="flex min-h-7 flex-wrap items-center gap-x-4 gap-y-1 border-b border-border px-2 py-1 text-xs text-muted-foreground">
      <span className={item}>
        <svg aria-hidden width="16" height="12" viewBox="0 0 16 12" className="shrink-0">
          <path
            d="M 3 0 V 10 H 16"
            fill="none"
            strokeWidth={1.25}
            className={cn(LINK_STROKE_CLASS.tree, TREE_LINK_GROUP_CLASS)}
          />
        </svg>
        <span className="min-w-0">
          {gt("Sub-task", { $context: "issue graph legend: parent-child link" })}
        </span>
      </span>
      <span className={item}>
        <DependencyMark tone="open" />
        <span className="min-w-0">
          {gt("Open dependency", { $context: "issue graph legend: link from an open blocker" })}
        </span>
      </span>
      <span className={item}>
        <DependencyMark tone="closed" />
        <span className="min-w-0">
          {gt("Closed dependency", { $context: "issue graph legend: link from a closed blocker" })}
        </span>
      </span>
    </div>
  );
}
