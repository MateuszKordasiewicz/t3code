import { useAtomValue } from "@effect/atom-react";
import { issueThreads } from "@t3tools/client-runtime/issue-threads";
import { visibleLinkedThreads } from "@t3tools/client-runtime/state/threads";
import { EnvironmentId, ThreadId, type IssueSummary } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";

import { useEnvironmentServerConfig } from "../../state/entities";
import { issueEnvironment } from "../../state/issues";
import { useEnvironmentQuery } from "../../state/query";
import { environmentThreadShells } from "../../state/threads";
import { usePullRequestConnected } from "../pull-requests/usePullRequestConnected";
import { issueTarget, type IssueParams } from "./issueParams";

// Keyed by environment and thread ids, so the issue screen follows only its linked threads'
// run state; quiet shells skip streaming ticks.
const linkedThreadShells = Atom.family((key: string) => {
  const [environmentId = "", ...threadIds] = key.split("\u0000");
  return Atom.make((get) =>
    threadIds.map((threadId) =>
      get(
        environmentThreadShells.quietThreadShellAtom({
          environmentId: EnvironmentId.make(environmentId),
          threadId: ThreadId.make(threadId),
        }),
      ),
    ),
  );
});

/**
 * The threads linked to an issue and which one its status button opens. A failed read, or a
 * server without `issues.linkedThreads`, counts as none.
 */
export function useIssueThreads(params: IssueParams, issue: IssueSummary | null) {
  const target = issueTarget(params);
  const connected = usePullRequestConnected(params.environmentId);
  const capabilities = useEnvironmentServerConfig(target.environmentId)?.environment.capabilities;
  const listsThreads = capabilities?.issueLinkedThreads === true;
  const relations = useEnvironmentQuery(
    issue && listsThreads
      ? issueEnvironment.linkedThreads({
          environmentId: target.environmentId,
          input: {
            tracker: target.input.tracker,
            host: target.input.host ?? issue.host,
            scope: target.input.scope,
            key: target.input.key,
          },
        })
      : null,
  );
  const data = relations.data;
  const hiddenKeys = useAtomValue(environmentThreadShells.hiddenThreadKeysAtom);
  const hidingAnywhere = useAtomValue(environmentThreadShells.anyEnvironmentHidingAtom);
  const loadedThreadRefs = useAtomValue(
    environmentThreadShells.environmentThreadRefsAtom(target.environmentId),
  );
  const rows = visibleLinkedThreads(
    data?.threads ?? [],
    target.environmentId,
    loadedThreadRefs,
    hiddenKeys,
    hidingAnywhere,
  );
  const shells = useAtomValue(
    linkedThreadShells([target.environmentId, ...rows.map((row) => row.id)].join("\u0000")),
  );
  return {
    ...issueThreads(rows, (threadId) => shells[rows.findIndex((row) => row.id === threadId)]),
    // Until then the button stays hidden rather than offering a start it may take back. Offline,
    // no answer comes, and a start waits in the outbox.
    answered:
      data !== null ||
      relations.error !== null ||
      !connected ||
      (capabilities !== undefined && !listsThreads),
  };
}
