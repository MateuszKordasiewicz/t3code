import type { IssueState } from "@t3tools/contracts";
import { AsyncResult, type Atom } from "effect/reactivity";

import {
  createIssueTabIconQueue,
  issueDetailReadStep,
} from "../components/issue/issueTabIconQueue.logic";
import { appAtomRegistry } from "../rpc/atomRegistry";

export type IssueDetailStateAtom = Atom.Atom<
  AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>
>;

/** The last state each detail showed. Inactive tabs and sidebar rows draw this without holding queries open. */
export const knownIssueDetailStates = new WeakMap<object, IssueState>();

const stateListenersByDetail = new WeakMap<object, Set<() => void>>();

function sameIssueState(a: IssueState | undefined, b: IssueState): boolean {
  return (
    a !== undefined &&
    a.category === b.category &&
    a.name === b.name &&
    a.closeReason === b.closeReason
  );
}

export function rememberIssueDetailState(
  detail: object,
  result: AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>,
) {
  const data = AsyncResult.getOrElse(result, () => null);
  if (data !== null) notifyIssueDetailState(detail, data.state);
}

export function subscribeIssueDetailStates(
  details: ReadonlyArray<object> | null,
  listener: () => void,
): () => void {
  if (details === null || details.length === 0) return () => {};
  for (const detail of details) {
    let listeners = stateListenersByDetail.get(detail);
    if (listeners === undefined) {
      listeners = new Set();
      stateListenersByDetail.set(detail, listeners);
    }
    listeners.add(listener);
  }
  return () => {
    for (const detail of details) {
      const listeners = stateListenersByDetail.get(detail);
      if (listeners === undefined) continue;
      listeners.delete(listener);
      if (listeners.size === 0) stateListenersByDetail.delete(detail);
    }
  };
}

export function notifyIssueDetailState(detail: object, state: IssueState): void {
  if (sameIssueState(knownIssueDetailStates.get(detail), state)) return;
  knownIssueDetailStates.set(detail, state);
  const listeners = stateListenersByDetail.get(detail);
  if (listeners === undefined) return;
  for (const listener of listeners) listener();
}

// Reads through the registry, not a component, so a row or tab unmounting mid-read still holds the slot.
export const issueStateQueue = createIssueTabIconQueue<IssueDetailStateAtom>((detail, settle) => {
  let fetched = false;
  let finished = false;
  const onResult = (result: AsyncResult.AsyncResult<{ readonly state: IssueState }, unknown>) => {
    if (finished) return;
    rememberIssueDetailState(detail, result);
    const step = issueDetailReadStep(result, fetched, Date.now());
    if (step === "done") {
      finished = true;
      queueMicrotask(() => {
        unsubscribe();
        settle(knownIssueDetailStates.has(detail));
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
