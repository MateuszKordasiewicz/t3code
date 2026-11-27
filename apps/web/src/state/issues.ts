import {
  createIssueEnvironmentAtoms,
  createMergedIssueCandidatesAtoms,
  createMergedIssueListAtoms,
  type IssueCandidatesTarget,
  type IssueListTarget,
  type MergedIssueList,
} from "@t3tools/client-runtime/state/issues";
import { useAtomValue } from "@effect/atom-react";
import { environmentSessionGenerationAtom } from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId, IssueState, ThreadIssueLink } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Atom } from "effect/reactivity";
import { useEffect, useSyncExternalStore } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { useServerConfigs } from "./entities";
import {
  issueStateQueue,
  knownIssueDetailStates,
  notifyIssueDetailState,
  subscribeIssueDetailStates,
  type IssueDetailStateAtom,
} from "./issueStateQueue";
import { formatEnvironmentQueryError, useEnvironmentQuery } from "./query";

export type { IssueListTarget };

export const issueEnvironment = createIssueEnvironmentAtoms(connectionAtomRuntime, {
  detail: (target, value) =>
    Effect.sync(() => {
      notifyIssueDetailState(issueEnvironment.detail(target), value.state);
    }),
});

const linkedIssueRef = ({ projectId, tracker, host, scope, key }: ThreadIssueLink) => ({
  projectId,
  tracker,
  host,
  scope,
  key,
});

export function useLinkedIssueDetail(
  environmentId: EnvironmentId,
  link: ThreadIssueLink | undefined,
  mountScope?: "sidebar" | "thread-details" | "linked-items" | "sidebar-tooltip",
) {
  const configs = useServerConfigs();
  return useEnvironmentQuery(
    link && configs.get(environmentId)?.environment.capabilities.issues === true
      ? issueEnvironment.detail({ environmentId, input: linkedIssueRef(link) }, mountScope)
      : null,
  ).data;
}

function sidebarLinksKey(links: ReadonlyArray<ThreadIssueLink>): string {
  return links
    .map((link) => JSON.stringify([link.projectId, link.tracker, link.host, link.scope, link.key]))
    .join("\n");
}

const sidebarDetailAtomArrays = new Map<string, WeakRef<ReadonlyArray<IssueDetailStateAtom>>>();
const sidebarDetailAtomCleanup = new FinalizationRegistry<string>((cacheKey) => {
  const current = sidebarDetailAtomArrays.get(cacheKey);
  if (current !== undefined && current.deref() === undefined) {
    sidebarDetailAtomArrays.delete(cacheKey);
  }
});

function sidebarDetailAtoms(
  environmentId: EnvironmentId,
  linksKey: string,
): ReadonlyArray<IssueDetailStateAtom> | null {
  if (!linksKey) return null;
  const cacheKey = `${environmentId}\u0000${linksKey}`;
  const cached = sidebarDetailAtomArrays.get(cacheKey)?.deref();
  if (cached !== undefined) return cached;
  const atoms = linksKey.split("\n").map((entry) => {
    const [projectId, tracker, host, scope, key] = JSON.parse(entry) as [
      ThreadIssueLink["projectId"],
      ThreadIssueLink["tracker"],
      ThreadIssueLink["host"],
      ThreadIssueLink["scope"],
      ThreadIssueLink["key"],
    ];
    return issueEnvironment.detail({
      environmentId,
      input: { projectId, tracker, host, scope, key },
    });
  });
  sidebarDetailAtomArrays.set(cacheKey, new WeakRef(atoms));
  sidebarDetailAtomCleanup.register(atoms, cacheKey);
  return atoms;
}

export function selectLinkedIssueState(
  details: ReadonlyArray<IssueDetailStateAtom> | null,
): IssueState | null {
  if (details === null || details.length === 0) return null;
  let missing = false;
  let stopped: IssueState | null = null;
  let firstClosed: IssueState | null = null;
  for (const detail of details) {
    const state = knownIssueDetailStates.get(detail);
    if (state === undefined) {
      missing = true;
      continue;
    }
    if (state.category === "open") return state;
    if (stopped === null && state.closeReason && state.closeReason !== "completed") {
      stopped = state;
    }
    if (firstClosed === null) firstClosed = state;
  }
  if (missing) return null;
  return stopped ?? firstClosed;
}

const zeroSessionGenerationAtom = Atom.make(0).pipe(Atom.withLabel("issues:session-gen:empty"));

export function useSidebarLinkedIssueState(
  environmentId: EnvironmentId,
  links: ReadonlyArray<ThreadIssueLink> | undefined,
) {
  const configs = useServerConfigs();
  const enabled =
    Boolean(links?.length) && configs.get(environmentId)?.environment.capabilities.issues === true;
  const linksKey = enabled && links ? sidebarLinksKey(links) : "";
  const details = sidebarDetailAtoms(environmentId, linksKey);
  const sessionGeneration = useAtomValue(
    details === null
      ? zeroSessionGenerationAtom
      : environmentSessionGenerationAtom(connectionAtomRuntime, environmentId),
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- joins the per-environment background queue for visible sidebar links
  useEffect(() => {
    if (details === null || sessionGeneration === 0) return;
    const tickets = details.map((detail) => {
      const ticket = {};
      issueStateQueue.request(ticket, environmentId, detail, false);
      return ticket;
    });
    return () => {
      for (const ticket of tickets) issueStateQueue.drop(ticket);
    };
  }, [environmentId, details, sessionGeneration]);

  return useSyncExternalStore(
    (listener) => subscribeIssueDetailStates(details, listener),
    () => selectLinkedIssueState(details),
  );
}

const mergedIssueLists = createMergedIssueListAtoms(issueEnvironment, formatEnvironmentQueryError);

/** The same listing read across every connected environment, as the pull-request page does. */
export function useIssueList(targets: ReadonlyArray<IssueListTarget>): MergedIssueList & {
  readonly refresh: () => void;
  readonly reload: () => void;
  readonly retry: () => void;
} {
  const view = useAtomValue(mergedIssueLists.atom(targets));
  return {
    ...view,
    refresh: () => mergedIssueLists.refresh(appAtomRegistry, targets, "manual"),
    reload: () => mergedIssueLists.refresh(appAtomRegistry, targets, "background"),
    retry: () => mergedIssueLists.retry(appAtomRegistry, targets),
  };
}

/** After a write made beside the list, such as a state change in the detail panel. */
export function refreshIssueLists(environmentId: EnvironmentId) {
  mergedIssueLists.refreshEnvironment(appAtomRegistry, environmentId);
}

const mergedIssueCandidates = createMergedIssueCandidatesAtoms(
  issueEnvironment.candidates,
  formatEnvironmentQueryError,
);

/** The labels and people of every scope a list filter covers; no targets reads nothing. */
export function useIssueFilterCandidates(targets: ReadonlyArray<IssueCandidatesTarget>) {
  return useAtomValue(mergedIssueCandidates.atom(targets));
}
