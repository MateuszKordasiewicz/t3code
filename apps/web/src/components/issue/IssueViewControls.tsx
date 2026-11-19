import { CircleDotIcon, ListIcon, NetworkIcon, ShapesIcon } from "lucide-react";
import { msg, useGT, useMessages } from "gt-react";

import { Toggle, ToggleGroup } from "~/components/ui/toggle-group";
import { PullRequestCompactFilterMenu } from "../pullRequest/PullRequestCompactFilterMenu";
import type { PullRequestFilterOption } from "../pullRequest/PullRequestListFilters";
import { resolveFilterOptions } from "../pullRequest/pullRequestListOptions";
import type { IssueGraphGroupBy } from "./issueGraph.logic";

const GROUP_OPTIONS = [
  { value: "tree", label: msg("Epic tree"), Icon: NetworkIcon },
  { value: "type", label: msg("Issue type"), Icon: ShapesIcon },
  {
    value: "status",
    label: msg("Status", { $context: "issue graph grouping" }),
    Icon: CircleDotIcon,
  },
] as const satisfies ReadonlyArray<PullRequestFilterOption<IssueGraphGroupBy>>;

/** The list or graph switch, and the graph's grouping while the graph is shown. */
export function IssueViewControls({
  view,
  group,
  onView,
  onGroup,
}: {
  view: "list" | "graph";
  group: IssueGraphGroupBy;
  onView: (view: "list" | "graph") => void;
  onGroup: (group: IssueGraphGroupBy) => void;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const groupOptions = resolveFilterOptions(GROUP_OPTIONS, resolve);
  const currentGroup = groupOptions.find((option) => option.value === group) ?? groupOptions[0];
  return (
    <>
      <ToggleGroup
        aria-label={gt("Issue view")}
        variant="outline"
        value={[view]}
        onValueChange={(value) => {
          const next = value[0];
          if (next === "list" || next === "graph") onView(next);
        }}
      >
        <Toggle aria-label={gt("Show issues as a list")} title={gt("List")} value="list">
          <ListIcon aria-hidden />
        </Toggle>
        <Toggle aria-label={gt("Show issues as a graph")} title={gt("Graph")} value="graph">
          <NetworkIcon aria-hidden />
        </Toggle>
      </ToggleGroup>
      {view === "graph" && currentGroup ? (
        <PullRequestCompactFilterMenu
          label={gt("Group by")}
          triggerIcon={<currentGroup.Icon aria-hidden className="size-4" />}
          triggerLabel={currentGroup.label}
          outlined
          value={group}
          options={groupOptions}
          onChange={onGroup}
        />
      ) : null}
    </>
  );
}
