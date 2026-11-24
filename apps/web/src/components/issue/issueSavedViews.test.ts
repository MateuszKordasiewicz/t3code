import { describe, expect, it, vi } from "vite-plus/test";

import * as Schema from "effect/Schema";

import { EnvironmentId, ProjectId } from "@t3tools/contracts";

import {
  issueListPreferences,
  IssueListPreferencesSchema,
  readIssueListPreferences,
  writeIssueListPreferences,
} from "./issueListPreferences";
import { ISSUE_SAVED_VIEWS, issueViewMatches } from "./issueSavedViews";
import { setAllIssueProjectsShown, setIssueProjectShown } from "./issuesSearch.logic";

const decodePreferences = Schema.decodeOption(IssueListPreferencesSchema);
const decodeSavedViews = Schema.decodeSync(Schema.fromJsonString(ISSUE_SAVED_VIEWS.schema));

const preferences = {
  who: "assigned",
  status: "open",
  hideProjects: [
    [EnvironmentId.make("env-1"), ProjectId.make("project-1")],
    [EnvironmentId.make("env-1"), ProjectId.make("project-2")],
  ],
  host: "github.com",
  q: "crash",
  label: ["bug", "priority"],
  assignee: "octocat",
  order: "created",
} as const;
const view = { id: "mine", name: "Issues Assigned to Me", preferences };
const legacyPreferences = {
  who: "assigned",
  status: "open",
  issueServer: EnvironmentId.make("env-1"),
  issueProject: ProjectId.make("project-1"),
} as const;

describe("saved issue views", () => {
  it("restores a saved single-project view and matches its server as well as its project", () => {
    const stored = JSON.stringify([
      { ...view, id: "legacy", preferences: legacyPreferences },
      view,
    ]);
    const [legacy, modern] = decodeSavedViews(stored);
    expect(ISSUE_SAVED_VIEWS.normalize(legacy!.preferences)).toEqual(legacyPreferences);
    expect(issueViewMatches(legacy!, legacyPreferences)).toBe(true);
    expect(
      issueViewMatches(legacy!, { ...legacyPreferences, issueServer: EnvironmentId.make("env-2") }),
    ).toBe(false);
    expect(ISSUE_SAVED_VIEWS.normalize(modern!.preferences)).toEqual(preferences);
  });

  it("round-trips a saved inclusion list and distinguishes an empty selection from Show all", () => {
    const included = {
      who: "assigned",
      status: "open",
      showProjects: preferences.hideProjects,
      q: "crash",
    } as const;
    const stored = JSON.stringify([
      { ...view, preferences: included },
      { ...view, id: "none", preferences: { ...included, showProjects: [] } },
    ]);
    const [both, none] = decodeSavedViews(stored);
    expect(ISSUE_SAVED_VIEWS.normalize(both!.preferences)).toEqual(included);
    expect(ISSUE_SAVED_VIEWS.normalize(none!.preferences).showProjects).toEqual([]);
    expect(
      issueViewMatches(both!, { ...included, showProjects: included.showProjects.toReversed() }),
    ).toBe(true);
    expect(issueViewMatches(none!, { ...included, showProjects: undefined })).toBe(false);
    expect(
      issueViewMatches(
        { ...view, preferences: { ...legacyPreferences, q: "crash" } },
        {
          ...included,
          showProjects: [included.showProjects[0]],
        },
      ),
    ).toBe(true);
  });

  it("matches regardless of label order", () => {
    expect(issueViewMatches(view, { ...preferences, label: ["priority", "bug"], q: "crash" })).toBe(
      true,
    );
    expect(issueViewMatches(view, { ...preferences, assignee: undefined })).toBe(false);
    expect(issueViewMatches(view, { ...preferences, label: ["bug"] })).toBe(false);
    expect(
      issueViewMatches(view, {
        ...preferences,
        hideProjects: preferences.hideProjects.toReversed(),
      }),
    ).toBe(true);
    expect(issueViewMatches(view, { ...preferences, hideProjects: undefined })).toBe(false);
  });

  it("keeps a selection of every project within what it reads back", () => {
    const many = Array.from(
      { length: 1_001 },
      (_, index) => [EnvironmentId.make("env-1"), ProjectId.make(`project-${index}`)] as const,
    );
    const stored = issueListPreferences({ ...preferences, hideProjects: many });
    const restored = decodePreferences(stored);
    expect(restored._tag === "Some" && restored.value.hideProjects).toEqual(many.slice(0, 1_000));
  });

  it("tells views apart by graph grouping only while the graph is shown", () => {
    expect(issueViewMatches(view, { ...preferences, view: "graph" })).toBe(false);
    expect(issueViewMatches(view, { ...preferences, group: "status" })).toBe(true);
    const graph = { ...view, preferences: { ...preferences, view: "graph" as const } };
    expect(issueViewMatches(graph, { ...preferences, view: "graph" })).toBe(true);
    expect(issueViewMatches(graph, { ...preferences, view: "graph", group: "type" })).toBe(false);
  });
});

describe("remembered issue preferences", () => {
  it("restores the legacy project pair and gives current exclusions precedence", () => {
    const getItem = vi.fn((key: string) =>
      key === "t3.issues.preferences" ? JSON.stringify(legacyPreferences) : null,
    );
    vi.stubGlobal("window", { localStorage: { getItem } });
    try {
      expect(readIssueListPreferences()).toEqual(legacyPreferences);
      getItem.mockReturnValue(JSON.stringify({ ...legacyPreferences, hideProjects: [] }));
      expect(readIssueListPreferences()).toEqual({ who: "assigned", status: "open" });
      getItem.mockReturnValue(
        JSON.stringify({ ...legacyPreferences, hideProjects: preferences.hideProjects }),
      );
      expect(readIssueListPreferences()).toEqual({
        who: "assigned",
        status: "open",
        hideProjects: preferences.hideProjects,
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("remembers edits to a restored project, including an empty selection until Show all", () => {
    const a = { environmentId: legacyPreferences.issueServer, id: legacyPreferences.issueProject };
    const b = { environmentId: a.environmentId, id: ProjectId.make("project-2") };
    const stored = new Map([["t3.issues.preferences", JSON.stringify(legacyPreferences)]]);
    vi.stubGlobal("window", {
      localStorage: { getItem: stored.get.bind(stored), setItem: stored.set.bind(stored) },
    });
    try {
      const toggle = (project: typeof a, shown: boolean) => {
        const current = readIssueListPreferences();
        writeIssueListPreferences({ ...current, ...setIssueProjectShown(current, project, shown) });
      };
      toggle(b, true);
      expect(readIssueListPreferences()).toEqual({
        who: "assigned",
        status: "open",
        showProjects: preferences.hideProjects,
      });
      toggle(a, false);
      toggle(b, false);
      const empty = readIssueListPreferences();
      expect(empty.showProjects).toEqual([]);
      writeIssueListPreferences({ ...empty, ...setAllIssueProjectsShown([a, b], true) });
      expect(readIssueListPreferences()).toEqual({ who: "assigned", status: "open" });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
