import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, IssueState } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useState, useSyncExternalStore } from "react";

import { appAtomRegistry } from "~/rpc/atomRegistry";

import { createIssueTabIconQueue, issueDetailReadStep } from "./issueTabIconQueue.logic";

type IssueDetailAtom = Atom.Atom<AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>>;

/** The last state each detail showed. Closed tabs draw this instead of holding the query open. */
const knownStates = new WeakMap<object, IssueState>();
const remember = (
  detail: object,
  result: AsyncResult.AsyncResult<{ state: IssueState }, unknown>,
) => {
  const data = AsyncResult.getOrElse(result, () => null);
  if (data !== null) knownStates.set(detail, data.state);
};

// Reads through the registry, not a component, so a tab closing mid-read still holds the slot.
const queue = createIssueTabIconQueue<IssueDetailAtom>((detail, settle) => {
  let fetched = false;
  let finished = false;
  const onResult = (result: AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>) => {
    if (finished) return;
    remember(detail, result);
    const step = issueDetailReadStep(result, fetched, Date.now());
    if (step === "done") {
      finished = true;
      queueMicrotask(() => {
        unsubscribe();
        settle(knownStates.has(detail));
      });
      return;
    }
    fetched = true;
    if (step === "fetch") appAtomRegistry.refresh(detail);
  };
  // Listen before reading: a refresh before the listener is attached would go unheard.
  const unsubscribe = appAtomRegistry.subscribe(detail, onResult);
  onResult(appAtomRegistry.get(detail));
});

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
  detail: IssueDetailAtom | null,
) {
  const [ticket] = useState(() => ({}));
  const [icon, setIcon] = useState<SVGSVGElement | null>(null);
  const phase = useSyncExternalStore(queue.subscribe, () => queue.phase(ticket));
  const live = useAtomValue(active && detail !== null ? detail : UNREAD);
  const liveState = AsyncResult.getOrElse(live, () => null)?.state ?? null;
  // Read through the store: a plain read of the module-level map would be memoized away.
  const knownState = useSyncExternalStore(queue.subscribe, () =>
    detail === null ? null : (knownStates.get(detail) ?? null),
  );

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- leaves the module-level queue on unmount
  useEffect(() => () => queue.drop(ticket), [ticket]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- tells the module-level queue the tab opened
  useEffect(() => {
    if (!active) return;
    queue.skip(ticket);
    // Left before its detail arrived: wait in line like any other tab instead of staying a dot.
    return () => {
      if (detail === null || !knownStates.has(detail)) queue.drop(ticket);
    };
  }, [active, detail, ticket]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- follows the tab's DOM visibility and pointer
  useEffect(() => {
    if (icon === null || environmentId === null || detail === null) return;
    if (phase === "fetching" || phase === "settled") return;
    // The icon itself hides while the tab is hovered, so watch and listen on the tab.
    const tab = icon.closest<HTMLElement>("[data-active-tab]") ?? icon;
    const jump = () => queue.request(ticket, environmentId, detail, true);
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        queue.request(ticket, environmentId, detail, false);
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
    if (detail !== null) remember(detail, live);
  }, [detail, live]);

  return {
    state: liveState ?? knownState,
    iconRef: setIcon,
  };
}
