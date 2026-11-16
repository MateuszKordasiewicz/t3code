import {
  AuthOrchestrationOperateScope,
  CommandId,
  ProjectId,
  ThreadId,
  WorkTaskCriterionId,
  WorkTaskId,
  WorkTaskManagerGroupId,
  type EnvironmentId,
  type WorkTaskInspectDraftResult,
} from "@t3tools/contracts";
import { useState } from "react";
import * as SecureRandom from "effect/crypto/SecureRandom";
import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import { useAtomCommand } from "../state/use-atom-command";
import { useEnvironmentsWithScope } from "../state/session";
import {
  nativeTaskAcceptDraft,
  nativeTaskCreateDraft,
  nativeTaskCreateInitialGroup,
  nativeTaskInspectDraft,
} from "../state/nativeTasks";
import { Button } from "./ui/button";

const newIds = () => ({
  taskId: WorkTaskId.make(SecureRandom.uuidV4Unsafe()),
  createCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  acceptCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  groupId: WorkTaskManagerGroupId.make(SecureRandom.uuidV4Unsafe()),
  groupCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
});

export function NativeTaskCreatePanel({
  projects,
  threads,
}: {
  readonly projects: ReadonlyArray<EnvironmentProject>;
  readonly threads: ReadonlyArray<EnvironmentThreadShell>;
}) {
  const writable = useEnvironmentsWithScope(projects, AuthOrchestrationOperateScope);
  const [ids, setIds] = useState(newIds);
  const [projectKey, setProjectKey] = useState("");
  const [managerThreadId, setManagerThreadId] = useState("");
  const [title, setTitle] = useState("");
  const [objective, setObjective] = useState("");
  const [condition, setCondition] = useState("");
  const [verification, setVerification] = useState("");
  const [inspectTaskId, setInspectTaskId] = useState("");
  const [current, setCurrent] = useState<WorkTaskInspectDraftResult | null>(null);
  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);
  const create = useAtomCommand(nativeTaskCreateDraft, { reportFailure: false });
  const inspect = useAtomCommand(nativeTaskInspectDraft, { reportFailure: false });
  const accept = useAtomCommand(nativeTaskAcceptDraft, { reportFailure: false });
  const createGroup = useAtomCommand(nativeTaskCreateInitialGroup, { reportFailure: false });
  const selected = projects.find(
    (project) => JSON.stringify([project.environmentId, project.id]) === projectKey,
  );
  const availableThreads = threads.filter(
    (thread) =>
      selected &&
      thread.environmentId === selected.environmentId &&
      thread.projectId === selected.id &&
      thread.archivedAt === null,
  );
  const canWrite = selected !== undefined && writable.has(selected.environmentId);
  const refresh = async (environmentId: EnvironmentId, taskId: WorkTaskId) => {
    const result = await inspect({ environmentId, input: { taskId } });
    if (result._tag === "Success") {
      setCurrent(result.value);
      setInspectTaskId(result.value.taskId);
      return result.value;
    }
    setStatus("Could not inspect this Task. Check the ID and environment, then retry.");
    return null;
  };
  const run = async (operation: () => Promise<void>) => {
    setBusy(true);
    setStatus("");
    try {
      await operation();
    } finally {
      setBusy(false);
    }
  };
  return (
    <section className="rounded-lg border p-4" aria-label="Native Task">
      <h2 className="font-semibold">Native Task</h2>
      <p className="mt-1 text-sm text-muted-foreground">
        Create a Task draft, inspect it, accept its definition, then bind a manager conversation.
      </p>
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <label className="flex flex-col gap-1 text-sm">
          Project
          <select
            className="rounded-md border bg-background p-2"
            value={projectKey}
            onChange={(event) => {
              setProjectKey(event.target.value);
              setManagerThreadId("");
              setCurrent(null);
            }}
          >
            <option value="">Choose a project</option>
            {projects
              .filter((project) => writable.has(project.environmentId))
              .map((project) => (
                <option
                  key={JSON.stringify([project.environmentId, project.id])}
                  value={JSON.stringify([project.environmentId, project.id])}
                >
                  {project.title}
                </option>
              ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-sm">
          Manager conversation
          <select
            className="rounded-md border bg-background p-2"
            value={managerThreadId}
            onChange={(event) => setManagerThreadId(event.target.value)}
          >
            <option value="">Choose a conversation</option>
            {availableThreads.map((thread) => (
              <option key={thread.id} value={thread.id}>
                {thread.title || thread.id}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-sm">
          Title
          <input
            className="rounded-md border bg-background p-2"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
          />
        </label>
        <label className="flex flex-col gap-1 text-sm">
          Objective
          <input
            className="rounded-md border bg-background p-2"
            value={objective}
            onChange={(event) => setObjective(event.target.value)}
          />
        </label>
        <label className="flex flex-col gap-1 text-sm">
          Done when
          <input
            className="rounded-md border bg-background p-2"
            value={condition}
            onChange={(event) => setCondition(event.target.value)}
          />
        </label>
        <label className="flex flex-col gap-1 text-sm">
          Verify by
          <input
            className="rounded-md border bg-background p-2"
            value={verification}
            onChange={(event) => setVerification(event.target.value)}
          />
        </label>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        <Button
          disabled={
            busy ||
            !canWrite ||
            !title.trim() ||
            !objective.trim() ||
            !condition.trim() ||
            !verification.trim()
          }
          onClick={() =>
            void run(async () => {
              if (!selected) return;
              const result = await create({
                environmentId: selected.environmentId,
                input: {
                  taskId: ids.taskId,
                  commandId: ids.createCommandId,
                  projectId: ProjectId.make(selected.id),
                  definition: {
                    title,
                    objective,
                    criteria: [
                      { id: WorkTaskCriterionId.make("outcome"), condition, verification },
                    ],
                    actionPolicy: { version: 1, rules: [] },
                  },
                },
              });
              if (result._tag === "Success") {
                await refresh(selected.environmentId, ids.taskId);
                setStatus("Draft saved. Inspect its definition before accepting.");
              } else setStatus("Could not save the draft. Retry with the same Task ID.");
            })
          }
        >
          Create draft
        </Button>
        <Button
          variant="outline"
          disabled={busy || !canWrite || !inspectTaskId.trim()}
          onClick={() =>
            void run(async () => {
              if (selected)
                await refresh(selected.environmentId, WorkTaskId.make(inspectTaskId.trim()));
            })
          }
        >
          Inspect Task
        </Button>
        <Button
          variant="outline"
          disabled={busy || !canWrite || !current || current.accepted !== null}
          onClick={() =>
            void run(async () => {
              if (!selected || !current) return;
              const result = await accept({
                environmentId: selected.environmentId,
                input: {
                  taskId: current.taskId,
                  commandId: ids.acceptCommandId,
                  expectedDefinitionRevision: current.definitionRevision,
                  expectedLifecycleRevision: current.lifecycleRevision,
                  expectedActionPolicyRevision: current.actionPolicyRevision,
                },
              });
              if (result._tag === "Success") {
                await refresh(selected.environmentId, current.taskId);
                setStatus("Definition accepted.");
              } else setStatus("Could not accept the displayed revision. Inspect it again.");
            })
          }
        >
          Accept definition
        </Button>
        <Button
          variant="outline"
          disabled={busy || !canWrite || !current?.accepted || !managerThreadId}
          onClick={() =>
            void run(async () => {
              if (!selected || !current) return;
              const result = await createGroup({
                environmentId: selected.environmentId,
                input: {
                  taskId: current.taskId,
                  groupId: ids.groupId,
                  managerThreadId: ThreadId.make(managerThreadId),
                  commandId: ids.groupCommandId,
                },
              });
              setStatus(
                result._tag === "Success"
                  ? `Manager bound at event ${result.value.sequence}. Task run is not available yet.`
                  : "Could not bind the manager. Verify the conversation and retry.",
              );
            })
          }
        >
          Bind manager
        </Button>
        <Button
          variant="ghost"
          disabled={busy}
          onClick={() => {
            setIds(newIds());
            setCurrent(null);
            setInspectTaskId("");
            setStatus("New Task identity ready.");
          }}
        >
          New Task
        </Button>
      </div>
      <label className="mt-3 flex flex-col gap-1 text-sm">
        Task ID for inspection
        <input
          className="rounded-md border bg-background p-2"
          value={inspectTaskId}
          onChange={(event) => setInspectTaskId(event.target.value)}
        />
      </label>
      {current && (
        <p className="mt-2 text-sm">
          {current.definition.title} · {current.accepted ? "Accepted" : "Draft"} · Task{" "}
          {current.taskId}
        </p>
      )}
      {status && (
        <p className="mt-2 text-sm" role="status">
          {status}
        </p>
      )}
    </section>
  );
}
