import type { IssueLinkedThreadsResult, ThreadId } from "@t3tools/contracts";

import { isThreadWorking } from "./state/threadInbox.ts";

type LinkedThreadRow = IssueLinkedThreadsResult["threads"][number];
type LiveThread = Parameters<typeof isThreadWorking>[0] & { readonly title: string };

/** `done` covers settled and archived threads; `idle` is a finished turn nobody settled. */
export type IssueThreadStatus = "running" | "idle" | "done";

export interface IssueThread {
  readonly id: ThreadId;
  readonly projectId: LinkedThreadRow["projectId"];
  readonly title: string;
  readonly status: IssueThreadStatus;
}

export interface IssueThreads {
  /** Most recently updated first, as the server answers. */
  readonly threads: ReadonlyArray<IssueThread>;
  /** What the issue's status button opens: a running thread first, then the most recent one. */
  readonly primary: IssueThread | null;
}

/**
 * The run state of an issue's linked threads. The server answers which threads are linked, since
 * the client's shells leave out archived and trimmed ones; a loaded shell answers whether the
 * thread is running now, so the button follows a turn without refetching.
 */
export function issueThreads(
  rows: ReadonlyArray<LinkedThreadRow>,
  liveThread: (threadId: ThreadId) => LiveThread | null | undefined,
): IssueThreads {
  const threads = rows.map((row): IssueThread => {
    const live = row.archivedAt === null ? liveThread(row.id) : null;
    const status: IssueThreadStatus =
      live != null && isThreadWorking(live)
        ? "running"
        : row.archivedAt !== null || row.settled === true
          ? "done"
          : "idle";
    return { id: row.id, projectId: row.projectId, title: live?.title ?? row.title, status };
  });
  const primary = threads.find((thread) => thread.status === "running") ?? threads[0] ?? null;
  return { threads, primary };
}
