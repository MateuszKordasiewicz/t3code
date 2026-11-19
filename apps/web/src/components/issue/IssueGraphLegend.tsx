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
    <svg aria-hidden width="22" height="10" viewBox="0 0 22 10" className="shrink-0">
      <path d="M 1 5 H 15" fill="none" strokeWidth={1.25} className={LINK_STROKE_CLASS[tone]} />
      <path d="M 14 1.5 L 21 5 L 14 8.5 z" className={ARROW_FILL_CLASS[tone]} />
    </svg>
  );
}

/** Names the link marks drawn between the graph's cards. */
export function IssueGraphLegend() {
  const gt = useGT();
  const item = "flex items-center gap-1.5 whitespace-nowrap";
  return (
    // Wraps onto more lines in a narrow pane rather than cutting off a long translation.
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
        {gt("Sub-task", { $context: "issue graph legend: parent-child link" })}
      </span>
      <span className={item}>
        <DependencyMark tone="open" />
        {gt("Open dependency", { $context: "issue graph legend: link from an open blocker" })}
      </span>
      <span className={item}>
        <DependencyMark tone="closed" />
        {gt("Closed dependency", { $context: "issue graph legend: link from a closed blocker" })}
      </span>
    </div>
  );
}
