import * as Schema from "effect/Schema";

import {
  EnvironmentId,
  ISSUE_LIST_MAX_EXCLUDED_PROJECTS,
  ISSUE_LIST_MAX_INCLUDED_PROJECTS,
  IssueInvolvement,
  IssueListState,
  ProjectId,
  type ScopedProjectRef,
} from "@t3tools/contracts";

import type { IssueGraphGroupBy } from "./issueGraph.logic";

export type IssueProjectPair = readonly [EnvironmentId, ProjectId];

/**
 * What the issues page remembers between visits; the selection in the URL is not part of it.
 * `who` and `status` rather than `involvement` and `state`: the router merges every route's
 * search into one type for `navigate`, and sharing the pull-request page's key names would widen
 * its own literal unions with this page's values.
 */
export interface IssueListPreferences {
  readonly who: IssueInvolvement;
  readonly status: IssueListState;
  /** Projects left out of the list, so a project added later shows. */
  readonly hideProjects?: ReadonlyArray<IssueProjectPair> | undefined;
  /** Only these projects are shown. An empty selection still applies. */
  readonly showProjects?: ReadonlyArray<IssueProjectPair> | undefined;
  /** A single-project filter from before the multi-project picker. */
  readonly issueServer?: EnvironmentId | undefined;
  readonly issueProject?: ProjectId | undefined;
  readonly host?: string | undefined;
  readonly q?: string | undefined;
  /** `label`, `assignee` and `order` for the same reason: the pull-request page has `labels` and `sort`. */
  readonly label?: ReadonlyArray<string> | undefined;
  readonly assignee?: string | undefined;
  readonly order?: "created" | undefined;
  /** Left out for the list; `group` is left out for the epic tree. */
  readonly view?: "graph" | undefined;
  readonly group?: Exclude<IssueGraphGroupBy, "tree"> | undefined;
}

const DEFAULT_ISSUE_LIST_PREFERENCES = {
  who: "all",
  status: "open",
} as const satisfies IssueListPreferences;

const STORAGE_KEY = "t3.issues.preferences";

const BoundedPreference = Schema.String.check(Schema.isMaxLength(200));
export const IssueListPreferencesSchema = Schema.Struct({
  who: IssueInvolvement,
  status: IssueListState,
  hideProjects: Schema.optional(
    Schema.Array(Schema.Tuple([EnvironmentId, ProjectId])).check(
      Schema.isMaxLength(ISSUE_LIST_MAX_EXCLUDED_PROJECTS),
    ),
  ),
  showProjects: Schema.optional(
    Schema.Array(Schema.Tuple([EnvironmentId, ProjectId])).check(
      Schema.isMaxLength(ISSUE_LIST_MAX_INCLUDED_PROJECTS),
    ),
  ),
  issueServer: Schema.optional(EnvironmentId),
  issueProject: Schema.optional(ProjectId),
  host: Schema.optional(BoundedPreference),
  q: Schema.optional(BoundedPreference),
  label: Schema.optional(Schema.Array(BoundedPreference).check(Schema.isMaxLength(20))),
  assignee: Schema.optional(BoundedPreference),
  order: Schema.optional(Schema.Literal("created")),
  view: Schema.optional(Schema.Literal("graph")),
  group: Schema.optional(Schema.Literals(["type", "status"])),
});
const decodePreferences = Schema.decodeUnknownOption(IssueListPreferencesSchema);

type PreferenceStorage = Pick<Storage, "getItem" | "setItem">;

function storage(): PreferenceStorage | undefined {
  return typeof window === "undefined" ? undefined : window.localStorage;
}

export function legacyIssueProject(
  search: Pick<
    IssueListPreferences,
    "hideProjects" | "showProjects" | "issueServer" | "issueProject"
  >,
): ScopedProjectRef | undefined {
  return search.showProjects === undefined &&
    search.hideProjects === undefined &&
    search.issueServer &&
    search.issueProject
    ? { environmentId: search.issueServer, projectId: search.issueProject }
    : undefined;
}

export function issueIncludedProjects(
  search: IssueListPreferences,
): ReadonlyArray<IssueProjectPair> | undefined {
  const legacy = legacyIssueProject(search);
  return search.showProjects ?? (legacy ? [[legacy.environmentId, legacy.projectId]] : undefined);
}

/** Only list controls are remembered. The selected issue remains a URL and right-panel concern. */
export function issueListPreferences(search: IssueListPreferences): IssueListPreferences {
  const project = legacyIssueProject(search);
  return {
    who: search.who,
    status: search.status,
    ...(search.showProjects !== undefined
      ? { showProjects: search.showProjects.slice(0, ISSUE_LIST_MAX_INCLUDED_PROJECTS) }
      : {}),
    ...(search.showProjects === undefined && search.hideProjects?.length
      ? { hideProjects: search.hideProjects.slice(0, ISSUE_LIST_MAX_EXCLUDED_PROJECTS) }
      : {}),
    ...(project ? { issueServer: project.environmentId, issueProject: project.projectId } : {}),
    ...(search.host ? { host: search.host } : {}),
    ...(search.q ? { q: search.q } : {}),
    ...(search.label?.length ? { label: search.label } : {}),
    ...(search.assignee ? { assignee: search.assignee } : {}),
    ...(search.order ? { order: search.order } : {}),
    ...(search.view ? { view: search.view } : {}),
    ...(search.group ? { group: search.group } : {}),
  };
}

export function readIssueListPreferences(): IssueListPreferences {
  try {
    const raw = storage()?.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_ISSUE_LIST_PREFERENCES;
    const decoded = decodePreferences(JSON.parse(raw));
    return decoded._tag === "Some"
      ? issueListPreferences(decoded.value)
      : DEFAULT_ISSUE_LIST_PREFERENCES;
  } catch {
    return DEFAULT_ISSUE_LIST_PREFERENCES;
  }
}

export function writeIssueListPreferences(preferences: IssueListPreferences): void {
  try {
    storage()?.setItem(STORAGE_KEY, JSON.stringify(issueListPreferences(preferences)));
  } catch {
    // Storage can be full or forbidden; the page still works with defaults.
  }
}
