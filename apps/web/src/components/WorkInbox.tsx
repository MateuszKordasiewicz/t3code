import { useAtomValue } from "@effect/atom-react";
import { Link } from "@tanstack/react-router";
import { workInboxRows, type WorkInboxStatus } from "@t3tools/client-runtime/state/thread-inbox";
import { threadKey } from "@t3tools/client-runtime/state/entities";
import { useState } from "react";
import { useQuietThreadShells, useProjects } from "../state/entities";
import { useEnvironments } from "../state/environments";
import { liveShellEnvironmentIdsAtom } from "../state/shell";
import { buildThreadRouteParams } from "../threadRoutes";
import { Button } from "./ui/button";
import { WorkspacePageContainer } from "./WorkspacePageContainer";

const labels: Record<WorkInboxStatus, string> = {
  approval: "Needs approval",
  input: "Needs your input",
  guest: "Guest message",
  authentication: "Authentication needed",
  failed: "Run failed",
  limited: "Usage limit",
  preparing: "Preparing",
  queued: "Queued",
  running: "Running",
  waiting: "Waiting",
  "coordinator-input": "Waiting for coordinator",
  idle: "No active run",
};

export function WorkInbox() {
  const threads = useQuietThreadShells();
  const projects = useProjects();
  const { environments, isReady } = useEnvironments();
  const live = useAtomValue(liveShellEnvironmentIdsAtom);
  const [view, setView] = useState<"attention" | "activity" | "all">("attention");
  const [environment, setEnvironment] = useState("");
  const [project, setProject] = useState("");
  const [related, setRelated] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const rows = workInboxRows(threads);
  const environmentById = new Map(environments.map((entry) => [entry.environmentId, entry]));
  const projectByKey = new Map(
    projects.map((entry) => [JSON.stringify([entry.environmentId, entry.id]), entry]),
  );
  const filtered = rows.filter(
    (row) =>
      (environment === "" || row.thread.environmentId === environment) &&
      (project === "" ||
        JSON.stringify([row.thread.environmentId, row.thread.projectId]) === project) &&
      (related === null ||
        row.key === related ||
        (row.parent !== null &&
          threadKey({ environmentId: row.parent.environmentId, threadId: row.parent.id }) ===
            related)) &&
      (view === "all" ||
        (view === "attention" ? row.attention : !row.attention && row.status !== "idle")),
  );
  const pageCount = Math.max(1, Math.ceil(filtered.length / 50));
  const currentPage = Math.min(page, pageCount - 1);
  const shown = filtered.slice(currentPage * 50, (currentPage + 1) * 50);
  const unavailable = environments.filter((entry) => !live.has(entry.environmentId));
  const reset = () => setPage(0);
  return (
    <div className="h-full overflow-y-auto">
      <WorkspacePageContainer width="wide">
        <div>
          <h1 className="text-xl font-semibold">Work inbox</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Requests and observed activity. Open the original conversation to respond.
          </p>
        </div>
        {!isReady && <p role="status">Loading environments. Requests may not be loaded yet.</p>}
        {unavailable.length > 0 && (
          <div role="status" className="rounded-lg border p-4 text-sm">
            {unavailable.map((entry) => (
              <p key={entry.environmentId}>
                {entry.label}:{" "}
                {entry.connection.phase === "connected"
                  ? "loading or refreshing threads"
                  : entry.connection.phase}
                . Saved rows may be out of date.
              </p>
            ))}
          </div>
        )}
        <div className="flex flex-wrap items-center gap-2">
          {(["attention", "activity", "all"] as const).map((value) => (
            <Button
              key={value}
              variant={view === value ? "default" : "outline"}
              onClick={() => {
                setView(value);
                reset();
              }}
            >
              {value === "attention"
                ? "Needs you"
                : value === "activity"
                  ? "Activity"
                  : "All conversations"}
            </Button>
          ))}
        </div>
        <div className="flex flex-wrap gap-3">
          <label className="flex flex-col gap-1 text-sm">
            Environment
            <select
              className="rounded-md border bg-background p-2"
              value={environment}
              onChange={(event) => {
                setEnvironment(event.target.value);
                setProject("");
                reset();
              }}
            >
              <option value="">All environments</option>
              {environments.map((entry) => (
                <option key={entry.environmentId} value={entry.environmentId}>
                  {entry.label}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1 text-sm">
            Project
            <select
              className="rounded-md border bg-background p-2"
              value={project}
              onChange={(event) => {
                setProject(event.target.value);
                reset();
              }}
            >
              <option value="">All projects</option>
              {projects
                .filter((entry) => environment === "" || entry.environmentId === environment)
                .map((entry) => (
                  <option
                    key={JSON.stringify([entry.environmentId, entry.id])}
                    value={JSON.stringify([entry.environmentId, entry.id])}
                  >
                    {entry.title}
                  </option>
                ))}
            </select>
          </label>
          {related !== null && (
            <Button
              variant="outline"
              onClick={() => {
                setRelated(null);
                reset();
              }}
            >
              Clear related-thread filter
            </Button>
          )}
        </div>
        {shown.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No matching loaded conversations. Hidden threads follow your existing visibility
            settings; unavailable environments may have more work.
          </p>
        ) : (
          <ul className="divide-y rounded-lg border">
            {shown.map((row) => {
              const thread = row.thread;
              const fresh = live.has(thread.environmentId);
              return (
                <li key={row.key} className="flex flex-col gap-2 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <Link
                      className="font-medium underline-offset-4 hover:underline"
                      to="/$environmentId/$threadId"
                      params={buildThreadRouteParams({
                        environmentId: thread.environmentId,
                        threadId: thread.id,
                      })}
                    >
                      {thread.title || "Untitled conversation"}
                    </Link>
                    <span className="text-sm text-muted-foreground">
                      {fresh ? labels[row.status] : `Last observed: ${labels[row.status]}`}
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    {environmentById.get(thread.environmentId)?.label ?? "Environment unavailable"}{" "}
                    ·{" "}
                    {projectByKey.get(JSON.stringify([thread.environmentId, thread.projectId]))
                      ?.title ?? "Project unavailable"}
                    {thread.archivedAt !== null ? " · Archived" : ""}
                  </p>
                  {row.parent !== null && (
                    <div className="flex flex-wrap items-center gap-2 text-xs">
                      <span>Related to</span>
                      <Link
                        className="underline"
                        to="/$environmentId/$threadId"
                        params={buildThreadRouteParams({
                          environmentId: row.parent.environmentId,
                          threadId: row.parent.id,
                        })}
                      >
                        {row.parent.title}
                      </Link>
                      <Button
                        size="compact"
                        variant="ghost"
                        onClick={() => {
                          setRelated(
                            threadKey({
                              environmentId: row.parent!.environmentId,
                              threadId: row.parent!.id,
                            }),
                          );
                          setView("all");
                          reset();
                        }}
                      >
                        Show related
                      </Button>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        )}
        <div className="flex items-center gap-3">
          <Button
            variant="outline"
            disabled={currentPage === 0}
            onClick={() => setPage(currentPage - 1)}
          >
            Previous
          </Button>
          <span className="text-sm">
            Page {currentPage + 1} of {pageCount} · {filtered.length} loaded conversations
          </span>
          <Button
            variant="outline"
            disabled={currentPage + 1 >= pageCount}
            onClick={() => setPage(currentPage + 1)}
          >
            Next
          </Button>
        </div>
      </WorkspacePageContainer>
    </div>
  );
}
