import * as Schema from "effect/Schema";
import { msg } from "gt-react";
import { SavedViewIdentity } from "@t3tools/client-runtime/saved-views";

import type { SavedView, SavedViewsKind } from "../SavedViewsMenu";
import {
  issueIncludedProjects,
  issueListPreferences,
  IssueListPreferencesSchema,
  type IssueListPreferences,
} from "./issueListPreferences";

export function issueViewMatches(
  view: SavedView<typeof IssueListPreferencesSchema.Type>,
  preferences: IssueListPreferences,
): boolean {
  const key = (value: IssueListPreferences) => {
    const normalized = issueListPreferences(value);
    // The list ignores graph grouping, and project order does not distinguish views.
    return JSON.stringify({
      ...normalized,
      label: normalized.label?.toSorted(),
      group: normalized.view === "graph" ? normalized.group : undefined,
      hideProjects: normalized.hideProjects?.map((pair) => JSON.stringify(pair)).toSorted(),
      issueServer: undefined,
      issueProject: undefined,
      showProjects: undefined,
      included: issueIncludedProjects(normalized)
        ?.map((pair) => JSON.stringify(pair))
        .toSorted(),
    });
  };
  return key(issueListPreferences(view.preferences)) === key(preferences);
}

export const ISSUE_SAVED_VIEWS = {
  storageKey: "t3.issues.savedViews",
  schema: Schema.Array(
    Schema.Struct({ ...SavedViewIdentity.fields, preferences: IssueListPreferencesSchema }),
  ),
  empty: [],
  list: "issue",
  placeholder: msg("Issues Assigned to Me"),
  suggestedName: (preferences) =>
    preferences.who === "assigned" ? msg("Issues Assigned to Me") : "",
  normalize: issueListPreferences,
  matches: issueViewMatches,
} as const satisfies SavedViewsKind<
  IssueListPreferences,
  typeof IssueListPreferencesSchema.Type,
  unknown
>;
