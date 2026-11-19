import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { deriveProjectIdentity } from "../../projectIdentity";
import { issueGraphBandColor, type IssueGraphBandProject } from "./issueGraphBands.logic";

const environmentId = EnvironmentId.make("env-1");

function entryIn(projectId: string, projectTitle = "Listed title") {
  return {
    environmentId,
    projectId: ProjectId.make(projectId),
    projectTitle,
  } as EnvironmentIssueEntry;
}

function project(
  id: string,
  title: string,
  projectIcon: IssueGraphBandProject["projectIcon"],
): IssueGraphBandProject {
  return { environmentId, id: ProjectId.make(id), title, projectIcon };
}

describe("issueGraphBandColor", () => {
  const projects = [
    project("lucide", "Lucide", { kind: "lucide", name: "rocket", color: "teal" }),
    project("monogram", "Monogram", { kind: "monogram", text: "MO", color: "rose" }),
    project("emoji", "Emoji project", { kind: "emoji", emoji: "🦊" }),
    project("plain", "Plain project", null),
    {
      ...project("lucide", "Other env", { kind: "lucide", name: "x", color: "red" }),
      environmentId: EnvironmentId.make("env-2"),
    },
  ];

  it("uses the icon's chosen colour, else the project's automatic colour", () => {
    expect(issueGraphBandColor(entryIn("lucide"), projects)).toBe("teal");
    expect(issueGraphBandColor(entryIn("monogram"), projects)).toBe("rose");
    expect(issueGraphBandColor(entryIn("emoji"), projects)).toBe(
      deriveProjectIdentity("Emoji project").color,
    );
    expect(issueGraphBandColor(entryIn("plain"), projects)).toBe(
      deriveProjectIdentity("Plain project").color,
    );
    // A project not loaded yet falls back to the title the issue entry carries.
    expect(issueGraphBandColor(entryIn("unknown", "Listed title"), projects)).toBe(
      deriveProjectIdentity("Listed title").color,
    );
  });
});
