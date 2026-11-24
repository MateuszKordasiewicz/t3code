import type { HeldIssuePage } from "@t3tools/client-runtime/issue-list";
import { pullRequestFilterProjects } from "@t3tools/client-runtime/pull-request-project-filter";
import type { IssueListTarget } from "@t3tools/client-runtime/state/issues";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import {
  ISSUE_LIST_MAX_EXCLUDED_PROJECTS,
  ISSUE_LIST_MAX_INCLUDED_PROJECTS,
  type EnvironmentId,
  type IssueListInput,
  type ProjectId,
} from "@t3tools/contracts";

import type { IssuesSearch } from "./IssuesPage";
import {
  issueIncludedProjects,
  legacyIssueProject,
  type IssueProjectPair,
} from "./issueListPreferences";

type IssueProject = Pick<EnvironmentProject, "environmentId" | "id">;

// The router reads a bare `key=42` as a number.
const bounded = (value: unknown): string | undefined => {
  const text = typeof value === "number" ? String(value) : value;
  return typeof text === "string" && text.trim() ? text.trim().slice(0, 200) : undefined;
};

const labels = (raw: unknown): ReadonlyArray<string> | undefined => {
  const names = (Array.isArray(raw) ? raw : [raw]).flatMap((name) => bounded(name) ?? []);
  return names.length === 0 ? undefined : names.slice(0, 20);
};

const projectPairs = (raw: unknown, limit: number): ReadonlyArray<IssueProjectPair> | undefined => {
  if (!Array.isArray(raw)) return undefined;
  return raw
    .flatMap((pair) => {
      const environmentId = Array.isArray(pair) ? bounded(pair[0]) : undefined;
      const projectId = Array.isArray(pair) ? bounded(pair[1]) : undefined;
      return environmentId && projectId
        ? [[environmentId as EnvironmentId, projectId as ProjectId] as const]
        : [];
    })
    .slice(0, limit);
};

// Every optional key is an own key, even when undefined: the router lays these over the parent
// route's unvalidated search, where a key left out would keep its raw value.
export const issuesSearchFromUrl = (raw: Record<string, unknown>): IssuesSearch => {
  const showProjects = projectPairs(raw.showProjects, ISSUE_LIST_MAX_INCLUDED_PROJECTS);
  const hidden = projectPairs(raw.hideProjects, ISSUE_LIST_MAX_EXCLUDED_PROJECTS);
  const hideProjects =
    hidden?.length || (Array.isArray(raw.hideProjects) && raw.hideProjects.length === 0)
      ? hidden
      : undefined;
  const project = legacyIssueProject({
    hideProjects,
    showProjects,
    issueServer: bounded(raw.issueServer) as EnvironmentId | undefined,
    issueProject: bounded(raw.issueProject) as ProjectId | undefined,
  });
  return {
    who:
      raw.who === "assigned" || raw.who === "authored" || raw.who === "mentioned" ? raw.who : "all",
    status: raw.status === "closed" || raw.status === "all" ? raw.status : "open",
    hideProjects: showProjects === undefined && hideProjects?.length ? hideProjects : undefined,
    showProjects,
    issueServer: project?.environmentId,
    issueProject: project?.projectId,
    host: bounded(raw.host),
    q: bounded(raw.q),
    label: labels(raw.label),
    assignee: bounded(raw.assignee),
    order: raw.order === "created" ? "created" : undefined,
    view: raw.view === "graph" ? "graph" : undefined,
    group: raw.group === "type" || raw.group === "status" ? raw.group : undefined,
    selectedEnvironmentId: bounded(raw.selectedEnvironmentId) as EnvironmentId | undefined,
    selectedProjectId: bounded(raw.selectedProjectId) as ProjectId | undefined,
    selectedHost: bounded(raw.selectedHost),
    tracker: bounded(raw.tracker),
    scope: bounded(raw.scope),
    key: bounded(raw.key),
  };
};

/**
 * One choice per project: projects on one repository can read different Linear teams. Scratch
 * projects stay, since Settings can bind one to a Linear team.
 */
export const issueFilterProjects = (
  projects: ReadonlyArray<EnvironmentProject>,
  environmentLabels: ReadonlyMap<EnvironmentId, string>,
) =>
  pullRequestFilterProjects(
    projects.map((project) => ({ ...project, repositoryIdentity: null })),
    environmentLabels,
  );

/** The same key as `pullRequestProjectKey`. */
const projectKey = (project: IssueProject) => JSON.stringify([project.environmentId, project.id]);

const issueProjectKeys = (
  pairs: ReadonlyArray<IssueProjectPair> | undefined,
): ReadonlySet<string> => new Set((pairs ?? []).map((pair) => JSON.stringify(pair)));

export function issueProjectVisibility(search: IssuesSearch) {
  const included = issueIncludedProjects(search);
  const includedKeys = included === undefined ? undefined : issueProjectKeys(included);
  const hiddenKeys = issueProjectKeys(search.hideProjects);
  return {
    includedCount: includedKeys?.size,
    hiddenCount: hiddenKeys.size,
    isShown: (project: IssueProject) => {
      const key = projectKey(project);
      return includedKeys === undefined ? !hiddenKeys.has(key) : includedKeys.has(key);
    },
  };
}

export function setIssueProjectShown(search: IssuesSearch, project: IssueProject, shown: boolean) {
  const included = issueIncludedProjects(search);
  const pair = [project.environmentId, project.id] as const;
  const update = (pairs: ReadonlyArray<IssueProjectPair>, add: boolean) => {
    const others = pairs.filter(
      ([environmentId, id]) => environmentId !== project.environmentId || id !== project.id,
    );
    return add ? [...others, pair] : others;
  };
  return included === undefined
    ? { hideProjects: update(search.hideProjects ?? [], !shown) }
    : {
        showProjects: update(included, shown),
        hideProjects: undefined,
        issueServer: undefined,
        issueProject: undefined,
      };
}

export function setAllIssueProjectsShown(projects: ReadonlyArray<IssueProject>, shown: boolean) {
  return {
    showProjects: undefined,
    hideProjects: shown
      ? undefined
      : projects.map(({ environmentId, id }) => [environmentId, id] as const),
    issueServer: undefined,
    issueProject: undefined,
  };
}

export function issueListTargets(
  environmentIds: ReadonlyArray<EnvironmentId>,
  projects: ReadonlyArray<IssueProject>,
  search: IssuesSearch,
  input: IssueListInput,
): ReadonlyArray<IssueListTarget> {
  const included = issueIncludedProjects(search);
  if (included !== undefined) {
    return environmentIds.flatMap((environmentId) => {
      const ids = [
        ...new Set(included.filter(([owner]) => owner === environmentId).map(([, id]) => id)),
      ];
      const [projectId] = ids;
      if (projectId === undefined) return [];
      return [
        {
          environmentId,
          input: ids.length === 1 ? { ...input, projectId } : { ...input, includeProjectIds: ids },
        },
      ];
    });
  }

  // Before the project list loads, the saved exclusions still apply.
  const hidden = search.hideProjects;
  const keys = issueProjectKeys(hidden);
  return environmentIds.flatMap((environmentId) => {
    const own = projects.filter((project) => project.environmentId === environmentId);
    if (own.length > 0 && own.every((project) => keys.has(projectKey(project)))) return [];
    const excludeProjectIds = (
      own.length > 0
        ? own.filter((project) => keys.has(projectKey(project))).map((project) => project.id)
        : (hidden ?? [])
            .filter(([environment]) => environment === environmentId)
            .map(([, projectId]) => projectId)
    ).slice(0, ISSUE_LIST_MAX_EXCLUDED_PROJECTS);
    return [
      {
        environmentId,
        input: excludeProjectIds.length ? { ...input, excludeProjectIds } : input,
      },
    ];
  });
}

/** Older servers may ignore the project filters on both first pages and continuations. */
export function filterIssuePages(
  pages: ReadonlyMap<EnvironmentId, HeldIssuePage>,
  projects: ReturnType<typeof issueProjectVisibility>,
  host: string | undefined,
): ReadonlyMap<EnvironmentId, HeldIssuePage> {
  if (projects.includedCount === undefined && projects.hiddenCount === 0 && host === undefined)
    return pages;

  return new Map(
    [...pages].map(([environmentId, page]) => {
      const isShown = ({ projectId: id }: { readonly projectId: ProjectId }) =>
        projects.isShown({ environmentId, id });
      const scopes = page.answer.scopes?.filter(isShown);
      let nextCursors = page.nextCursors;
      if (projects.includedCount !== undefined && Object.keys(nextCursors).length > 0) {
        // Without scope metadata, keep cursors whose owner is unknown so selected rows can still load.
        const excluded = new Set(
          (page.answer.scopes ?? page.entries)
            .filter((scope) => !isShown(scope))
            .flatMap(({ host, scope }) => (host ? [`${host} ${scope.toLowerCase()}`] : [])),
        );
        nextCursors = Object.fromEntries(
          Object.entries(nextCursors).filter(([key]) => !excluded.has(key)),
        );
      }
      return [
        environmentId,
        {
          ...page,
          entries: page.entries.filter(isShown),
          errors: page.errors.filter(isShown),
          nextCursors,
          answer: {
            ...page.answer,
            scopes,
            trackers:
              host === undefined
                ? page.answer.trackers
                : page.answer.trackers.filter((tracker) => tracker.host.toLowerCase() === host),
          },
        },
      ];
    }),
  );
}
