import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import type { ProjectIconColor } from "@t3tools/contracts";

import { deriveProjectIdentity } from "../../projectIdentity";

export type IssueGraphBandProject = Pick<
  EnvironmentProject,
  "environmentId" | "id" | "title" | "projectIcon"
>;

/**
 * The colour of the issue's project icon. Emoji and favicon icons have no colour, so they use the
 * automatic colour the project would get without an icon, as the icon picker does.
 */
export function issueGraphBandColor(
  entry: EnvironmentIssueEntry,
  projects: ReadonlyArray<IssueGraphBandProject>,
): ProjectIconColor {
  const project = projects.find(
    (candidate) =>
      candidate.environmentId === entry.environmentId && candidate.id === entry.projectId,
  );
  const icon = project?.projectIcon;
  if (icon && icon.kind !== "emoji") return icon.color;
  return deriveProjectIdentity(project?.title ?? entry.projectTitle).color;
}
