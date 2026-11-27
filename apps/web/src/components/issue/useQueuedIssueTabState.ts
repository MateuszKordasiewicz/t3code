import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useState, useSyncExternalStore } from "react";

import {
  issueStateQueue,
  knownIssueDetailStates,
  rememberIssueDetailState,
  subscribeIssueDetailStates,
  type IssueDetailStateAtom,
} from "~/state/issueStateQueue";

const UNREAD = Atom.make(AsyncResult.initial<never, never>(false)).pipe(
  Atom.withLabel("issue-tab-icon:unread"),
);

/**
 * An issue tab's state for its icon. The open tab reads its detail live. Any other tab reads it
 * once, after it scrolls into the strip or is hovered or focused, one tab per environment at a time.
 */
export function useQueuedIssueTabState(
  environmentId: EnvironmentId | null,
  active: boolean,
  detail: IssueDetailStateAtom | null,
) {
  const [ticket] = useState(() => ({}));
  const [icon, setIcon] = useState<SVGSVGElement | null>(null);
  const phase = useSyncExternalStore(issueStateQueue.subscribe, () =>
    issueStateQueue.phase(ticket),
  );
  const live = useAtomValue(active && detail !== null ? detail : UNREAD);
  const liveState = AsyncResult.getOrElse(live, () => null)?.state ?? null;
  // Read through the store: a plain read of the module-level map would be memoized away.
  const knownState = useSyncExternalStore(
    (listener) => subscribeIssueDetailStates(detail === null ? null : [detail], listener),
    () => (detail === null ? null : (knownIssueDetailStates.get(detail) ?? null)),
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- leaves the module-level queue on unmount
  useEffect(() => () => issueStateQueue.drop(ticket), [ticket]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- tells the module-level queue the tab opened
  useEffect(() => {
    if (!active) return;
    issueStateQueue.skip(ticket);
    // Left before its detail arrived: wait in line like any other tab instead of staying a dot.
    return () => {
      if (detail === null || !knownIssueDetailStates.has(detail)) issueStateQueue.drop(ticket);
    };
  }, [active, detail, ticket]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- follows the tab's DOM visibility and pointer
  useEffect(() => {
    if (icon === null || environmentId === null || detail === null) return;
    if (phase === "fetching" || phase === "settled") return;
    // The icon itself hides while the tab is hovered, so watch and listen on the tab.
    const tab = icon.closest<HTMLElement>("[data-active-tab]") ?? icon;
    const jump = () => issueStateQueue.request(ticket, environmentId, detail, true);
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        issueStateQueue.request(ticket, environmentId, detail, false);
      }
    });
    observer.observe(tab);
    tab.addEventListener("pointerenter", jump);
    tab.addEventListener("focusin", jump);
    return () => {
      observer.disconnect();
      tab.removeEventListener("pointerenter", jump);
      tab.removeEventListener("focusin", jump);
    };
  }, [icon, environmentId, detail, phase, ticket]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the open tab's state for after it closes
  useEffect(() => {
    if (detail !== null) rememberIssueDetailState(detail, live);
  }, [detail, live]);

  return {
    state: liveState ?? knownState,
    iconRef: setIcon,
  };
}
