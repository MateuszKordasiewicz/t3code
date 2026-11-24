import { describe, expect, it } from "vite-plus/test";

import { continueIssuePage, groupIssues, holdIssuePages } from "@t3tools/client-runtime/issue-list";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import {
  EnvironmentId,
  IssueTrackerKind,
  ProjectId,
  type IssueListEntry,
  type IssueListResult,
} from "@t3tools/contracts";

import { issueListPreferences } from "./issueListPreferences";
import {
  filterIssuePages,
  issueFilterProjects,
  issueListTargets,
  issueProjectVisibility,
  issuesSearchFromUrl,
  setAllIssueProjectsShown,
  setIssueProjectShown,
} from "./issuesSearch.logic";

describe("issuesSearchFromUrl", () => {
  it("keeps a legacy list project separate from the issue details link", () => {
    const legacy = {
      issueServer: "laptop",
      issueProject: "api",
      selectedEnvironmentId: "vps",
      selectedProjectId: "web",
      selectedHost: "github.com",
      tracker: "github",
      scope: "org/web",
      key: 42,
      view: "graph",
    };
    expect(issuesSearchFromUrl(legacy)).toMatchObject({ ...legacy, key: "42" });
    for (const hideProjects of [[], [["laptop", "web"]]]) {
      expect(issuesSearchFromUrl({ ...legacy, hideProjects })).toMatchObject({
        issueServer: undefined,
        issueProject: undefined,
      });
    }
    expect(issuesSearchFromUrl({ issueProject: "api" }).issueProject).toBeUndefined();
  });

  it("replaces a malformed filter instead of letting the raw value through", () => {
    // The router lays the validated keys over the raw search, so a key left out keeps its raw value.
    const raw = {
      label: true,
      order: "oldest",
      assignee: "   ",
      host: true,
      view: "board",
      group: "tree",
    };
    const search = { ...raw, ...issuesSearchFromUrl(raw) };
    expect(search.host).toBeUndefined();
    expect(search.label).toBeUndefined();
    expect(search.assignee).toBeUndefined();
    expect(search.order).toBeUndefined();
    expect(search.view).toBeUndefined();
    expect(search.group).toBeUndefined();
    expect(issuesSearchFromUrl({ label: "bug" }).label).toEqual(["bug"]);
    expect(
      issuesSearchFromUrl({
        label: ["bug", " ui "],
        order: "created",
        view: "graph",
        group: "status",
      }),
    ).toMatchObject({ label: ["bug", "ui"], order: "created", view: "graph", group: "status" });
    expect(
      issuesSearchFromUrl({ hideProjects: [["env", "web"], ["env"], "api", ["env", 3]] })
        .hideProjects,
    ).toEqual([
      ["env", "web"],
      ["env", "3"],
    ]);
  });
});

describe("issueListTargets", () => {
  it("keeps an edited explicit selection when another project is discovered", () => {
    const laptop = EnvironmentId.make("laptop");
    const vps = EnvironmentId.make("vps");
    const a = { id: ProjectId.make("a"), environmentId: laptop };
    const b = { id: ProjectId.make("b"), environmentId: laptop };
    const c = { id: ProjectId.make("c"), environmentId: laptop };
    const projects = [a, b];
    for (const view of [undefined, "graph"] as const) {
      const restored = issuesSearchFromUrl({ issueServer: laptop, issueProject: a.id, view });
      const edit = (search: typeof restored, project: typeof a, shown: boolean) =>
        issuesSearchFromUrl({
          ...issueListPreferences({ ...search, ...setIssueProjectShown(search, project, shown) }),
        });
      const search = edit(restored, b, true);
      const input = { state: search.status, limit: view === "graph" ? 200 : 50 };
      for (const inventory of [projects, [...projects, c, { ...a, environmentId: vps }]]) {
        expect(inventory.filter(issueProjectVisibility(search).isShown)).toEqual([a, b]);
        expect(issueListTargets([laptop, vps], inventory, search, input)).toEqual([
          { environmentId: laptop, input: { ...input, includeProjectIds: [a.id, b.id] } },
        ]);
      }

      const single = edit(search, b, false);
      expect(issueListTargets([laptop, vps], [], single, input)).toEqual([
        { environmentId: laptop, input: { ...input, projectId: a.id } },
      ]);
      const crossEnvironment = edit(single, { ...b, environmentId: vps }, true);
      expect(issueListTargets([laptop, vps], [], crossEnvironment, input)).toEqual([
        { environmentId: laptop, input: { ...input, projectId: a.id } },
        { environmentId: vps, input: { ...input, projectId: b.id } },
      ]);
      const empty = edit(single, a, false);
      expect(empty.showProjects).toEqual([]);
      expect(issueListTargets([laptop, vps], [a, b, c], empty, input)).toEqual([]);
      const all = issuesSearchFromUrl({ ...empty, ...setAllIssueProjectsShown(projects, true) });
      expect(all.showProjects).toBeUndefined();
      expect(issueListTargets([laptop, vps], [a, b, c], all, input)).toEqual([
        { environmentId: laptop, input },
        { environmentId: vps, input },
      ]);
    }
  });

  it("reads only the legacy project on its server before and after project discovery", () => {
    const laptop = EnvironmentId.make("laptop");
    const vps = EnvironmentId.make("vps");
    const id = ProjectId.make("api");
    const search = issuesSearchFromUrl({ issueServer: laptop, issueProject: id });
    const input = { state: "open", limit: 100 } as const;
    const other = { environmentId: vps, id };
    for (const projects of [[], [other], [other, { environmentId: laptop, id }]]) {
      expect(issueListTargets([laptop, vps], projects, search, input)).toEqual([
        { environmentId: laptop, input: { ...input, projectId: id } },
      ]);
    }
    expect(issueListTargets([vps], [other], search, input)).toEqual([]);
  });

  it("leaves hidden projects out and skips an environment with every project hidden", () => {
    const laptop = EnvironmentId.make("laptop");
    const vps = EnvironmentId.make("vps");
    const projects = [
      { environmentId: laptop, id: ProjectId.make("web") },
      { environmentId: laptop, id: ProjectId.make("api") },
      { environmentId: vps, id: ProjectId.make("docs") },
    ];
    const input = { state: "open" } as const;
    const hidden = [
      [laptop, ProjectId.make("api")],
      [laptop, ProjectId.make("removed")],
      [vps, ProjectId.make("docs")],
    ] as const;
    const search = issuesSearchFromUrl({ hideProjects: hidden });
    expect(issueListTargets([laptop, vps], projects, search, input)).toEqual([
      { environmentId: laptop, input: { state: "open", excludeProjectIds: ["api"] } },
    ]);
    // Before the project list loads, the saved choice still leaves its projects out.
    expect(issueListTargets([laptop, vps], [], search, input)).toEqual([
      { environmentId: laptop, input: { state: "open", excludeProjectIds: ["api", "removed"] } },
      { environmentId: vps, input: { state: "open", excludeProjectIds: ["docs"] } },
    ]);
    expect(issueListTargets([laptop, vps], projects, issuesSearchFromUrl({}), input)).toEqual([
      { environmentId: laptop, input },
      { environmentId: vps, input },
    ]);
    const edited = { ...search, ...setIssueProjectShown(search, projects[1]!, true) };
    expect(edited.showProjects).toBeUndefined();
    expect(edited.hideProjects).toEqual(hidden.slice(1));
    expect(
      issueProjectVisibility(edited).isShown({ id: ProjectId.make("new"), environmentId: vps }),
    ).toBe(true);
  });
});

describe("filterIssuePages", () => {
  it("keeps unselected rows and known unselected cursors out of List and Graph on older servers", () => {
    const environmentId = EnvironmentId.make("linked");
    const tracker = IssueTrackerKind.make("github");
    const host = "github.com";
    const entry = (id: string, key: string): IssueListEntry => ({
      projectId: ProjectId.make(id),
      projectTitle: id,
      tracker,
      host,
      scope: `acme/${id.toUpperCase()}`,
      key,
      displayKey: `#${key}`,
      title: `${id} issue ${key}`,
      url: `https://${host}/acme/${id}/issues/${key}`,
      author: null,
      state: { category: "open", name: "Open" },
      createdAt: "2026-11-24T00:00:00Z",
      updatedAt: "2026-11-24T00:00:00Z",
      closedAt: null,
      assignees: [],
      labels: [],
      commentCount: 0,
    });
    const a = entry("a", "1");
    const b = entry("b", "1");
    const c = entry("c", "1");
    const error = { projectId: c.projectId, projectTitle: "c", message: "Unavailable" };
    const selection = issueProjectVisibility(
      issuesSearchFromUrl({
        showProjects: [
          [environmentId, a.projectId],
          [environmentId, b.projectId],
        ],
      }),
    );
    const nextCursors = {
      "github.com acme/a": "a-next",
      "github.com acme/b": "b-next",
      "github.com acme/c": "c-next",
    };
    const scope = ({ projectId, tracker, host, scope }: IssueListEntry) => ({
      projectId,
      tracker,
      host,
      scope,
    });

    for (const scopes of [undefined, [a, b, c].map(scope)]) {
      const answer: IssueListResult = {
        entries: [a, c],
        errors: [error],
        viewers: {},
        scopes,
        nextCursors,
        trackers: [
          { tracker, host, configured: true, capabilities: null, detail: null, projectCount: 3 },
        ],
      };
      const held = holdIssuePages([[environmentId, answer]], new Map());
      const first = filterIssuePages(held, selection, undefined);
      const visible = first.get(environmentId)!;
      expect(visible.entries).toEqual([a]);
      expect(visible.errors).toEqual([]);
      expect(visible.answer.scopes).toEqual(scopes?.slice(0, 2));
      // B has no first-page rows. Its cursor must survive even without scope metadata.
      expect(visible.nextCursors).toEqual({
        "github.com acme/a": "a-next",
        "github.com acme/b": "b-next",
      });

      const next = continueIssuePage(
        held.get(environmentId)!,
        {
          ...answer,
          entries: [b, entry("c", "2")],
          nextCursors: {},
        },
        visible.nextCursors,
      );
      const shown = filterIssuePages(new Map([[environmentId, next]]), selection, undefined);
      expect(
        groupIssues(shown, "", []).flatMap(({ rows }) => rows.map(({ projectId }) => projectId)),
      ).toEqual([a.projectId, b.projectId]);
      expect(shown.get(environmentId)?.nextCursors).toEqual({});
      expect(shown.get(environmentId)?.errors).toEqual([]);
    }
  });
});

describe("issueFilterProjects", () => {
  it("keeps every project, scratch ones too, as its own choice", () => {
    const environmentId = EnvironmentId.make("env");
    const project = (id: string, isScratch = false) =>
      ({
        id: ProjectId.make(id),
        environmentId,
        title: "t3code",
        workspaceRoot: `/work/${id}`,
        repositoryIdentity: { canonicalKey: "github.com/pingdotgg/t3code" },
        isScratch,
      }) as unknown as EnvironmentProject;
    const choices = issueFilterProjects(
      [project("main"), project("worktree"), project("scratch", true)],
      new Map([[environmentId, "laptop"]]),
    );
    expect(choices.map((choice) => [choice.id, choice.title])).toEqual([
      ["main", "t3code · laptop · /work/main"],
      ["scratch", "t3code · laptop · /work/scratch"],
      ["worktree", "t3code · laptop · /work/worktree"],
    ]);
  });
});
