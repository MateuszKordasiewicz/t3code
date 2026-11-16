import {
  AuthOrchestrationOperateScope,
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  WorkTaskCriterionId,
  WorkTaskId,
  WorkTaskManagerGroupId,
  type EnvironmentId,
  type WorkTaskInspectDraftResult,
  type WorkTaskInspectNativeRunResult,
} from "@t3tools/contracts";
import { useState } from "react";
import * as SecureRandom from "effect/crypto/SecureRandom";
import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import { useAtomCommand } from "../state/use-atom-command";
import { useEnvironmentsWithScope } from "../state/session";
import { threadEnvironment } from "../state/threads";
import { useOrchestrationCommand } from "../state/use-orchestration-command";
import {
  nativeTaskAcceptDraft,
  nativeTaskCreateDraft,
  nativeTaskCreateInitialGroup,
  nativeTaskInspectDraft,
  nativeTaskConfigureAllowance,
  nativeTaskStartPrepared,
  nativeTaskReserveLaunch,
  nativeTaskInspectNativeRun,
} from "../state/nativeTasks";
import { Button } from "./ui/button";

const newIds = () => ({
  taskId: WorkTaskId.make(SecureRandom.uuidV4Unsafe()),
  createCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  acceptCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  groupId: WorkTaskManagerGroupId.make(SecureRandom.uuidV4Unsafe()),
  groupCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  allowanceCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  startCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  launchCommandId: CommandId.make(SecureRandom.uuidV4Unsafe()),
  messageId: MessageId.make(SecureRandom.uuidV4Unsafe()),
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
  const [groupBound, setGroupBound] = useState(false);
  const [launchLifecycleRevision, setLaunchLifecycleRevision] = useState<number | null>(null);
  const [inspectCommandId, setInspectCommandId] = useState("");
  const [runResult, setRunResult] = useState<WorkTaskInspectNativeRunResult | null>(null);
  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);
  const create = useAtomCommand(nativeTaskCreateDraft, { reportFailure: false });
  const inspect = useAtomCommand(nativeTaskInspectDraft, { reportFailure: false });
  const accept = useAtomCommand(nativeTaskAcceptDraft, { reportFailure: false });
  const createGroup = useAtomCommand(nativeTaskCreateInitialGroup, { reportFailure: false });
  const configureAllowance = useAtomCommand(nativeTaskConfigureAllowance, { reportFailure: false });
  const startPrepared = useAtomCommand(nativeTaskStartPrepared, { reportFailure: false });
  const reserveLaunch = useAtomCommand(nativeTaskReserveLaunch, { reportFailure: false });
  const inspectRun = useAtomCommand(nativeTaskInspectNativeRun, { reportFailure: false });
  const startTurn = useOrchestrationCommand(threadEnvironment.startTurn, { reportFailure: false });
  const selected = projects.find(
    (project) => JSON.stringify([project.environmentId, project.id]) === projectKey,
  );
  const availableThreads = threads.filter(
    (thread) =>
      selected &&
      thread.environmentId === selected.environmentId &&
      thread.projectId === selected.id &&
      thread.providerInstanceId === "devReplay" &&
      thread.archivedAt === null,
  );
  const manager = availableThreads.find((thread) => thread.id === managerThreadId);
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
  const refreshRun = async (environmentId: EnvironmentId, commandId: CommandId) => {
    const result = await inspectRun({ environmentId, input: { commandId } });
    if (result._tag === "Success") {
      setRunResult(result.value);
      setStatus(
        `Native run: ${result.value.state}${result.value.status ? ` (${result.value.status})` : ""}.`,
      );
    } else
      setStatus(
        "Could not reconcile the native run. Its claim remains reserved; inspect the server evidence.",
      );
  };
  return (
    <section className="rounded-lg border p-4" aria-label="Native Task">
      <h2 className="font-semibold">Native Task</h2>
      <p className="mt-1 text-sm text-muted-foreground">
        Create a Task, bind a Replay (dev) conversation, run one turn, and inspect its result.
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
              setGroupBound(false);
              setRunResult(null);
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
            <option value="">Choose a Replay (dev) conversation</option>
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
                  ? `Manager bound at event ${result.value.sequence}. Ready for one Replay (dev) turn.`
                  : "Could not bind the manager. Verify the conversation and retry.",
              );
              if (result._tag === "Success") {
                setGroupBound(true);
                setLaunchLifecycleRevision(current.lifecycleRevision);
              }
            })
          }
        >
          Bind manager
        </Button>
        <Button
          disabled={
            busy ||
            !canWrite ||
            !groupBound ||
            !manager ||
            !current?.accepted ||
            launchLifecycleRevision === null
          }
          onClick={() =>
            void run(async () => {
              if (!selected || !current || !manager || launchLifecycleRevision === null) return;
              const environmentId = selected.environmentId;
              const allowance = await configureAllowance({
                environmentId,
                input: {
                  taskId: current.taskId,
                  commandId: ids.allowanceCommandId,
                  expectedLifecycleRevision: launchLifecycleRevision,
                  cap: 1,
                },
              });
              if (allowance._tag !== "Success") {
                setStatus("Could not configure the one-turn allowance. The Task was not launched.");
                return;
              }
              const started = await startPrepared({
                environmentId,
                input: {
                  taskId: current.taskId,
                  commandId: ids.startCommandId,
                  expectedLifecycleRevision: launchLifecycleRevision,
                  groupId: ids.groupId,
                  managerThreadId: manager.id,
                },
              });
              if (started._tag !== "Success") {
                setStatus("Could not start the prepared Task. The Task was not launched.");
                return;
              }
              const reserved = await reserveLaunch({
                environmentId,
                input: {
                  taskId: current.taskId,
                  groupId: ids.groupId,
                  managerThreadId: manager.id,
                  commandId: ids.launchCommandId,
                  messageId: ids.messageId,
                },
              });
              if (reserved._tag !== "Success") {
                setStatus("Could not reserve native custody. No provider turn was sent.");
                return;
              }
              setInspectCommandId(ids.launchCommandId);
              const text = `${current.definition.objective}\n\nDone when: ${current.definition.criteria.map((item) => item.condition).join("; ")}\nVerify by: ${current.definition.criteria.map((item) => item.verification).join("; ")}`;
              const sent = await startTurn({
                environmentId,
                input: {
                  threadId: manager.id,
                  commandId: ids.launchCommandId,
                  message: { messageId: ids.messageId, role: "user", text, attachments: [] },
                  modelSelection: manager.modelSelection,
                  runtimeMode: manager.runtimeMode,
                  interactionMode: manager.interactionMode,
                  dispatchMode: "start",
                },
              });
              if (sent._tag !== "Success") {
                setStatus(
                  "Dispatch did not confirm. The reservation is retained; inspect this command before retrying.",
                );
                return;
              }
              await refreshRun(environmentId, ids.launchCommandId);
            })
          }
        >
          Run one Replay turn
        </Button>
        <Button
          variant="outline"
          disabled={busy || !canWrite || !inspectCommandId.trim()}
          onClick={() =>
            void run(async () => {
              if (selected)
                await refreshRun(selected.environmentId, CommandId.make(inspectCommandId.trim()));
            })
          }
        >
          Inspect run
        </Button>
        <Button
          variant="ghost"
          disabled={busy}
          onClick={() => {
            setIds(newIds());
            setCurrent(null);
            setInspectTaskId("");
            setInspectCommandId("");
            setGroupBound(false);
            setLaunchLifecycleRevision(null);
            setRunResult(null);
            setStatus("New Task identity ready.");
          }}
        >
          New Task
        </Button>
      </div>
      <label className="mt-3 flex flex-col gap-1 text-sm">
        Run command ID for recovery
        <input
          className="rounded-md border bg-background p-2"
          value={inspectCommandId}
          onChange={(event) => setInspectCommandId(event.target.value)}
        />
      </label>
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
      {runResult && (
        <p className="mt-2 text-sm">
          Run {runResult.runId ?? "pending"} · {runResult.state}
          {runResult.releaseEventId ? ` · released at ${runResult.releaseEventId}` : ""}
          {selected && manager && runResult.runId && (
            <>
              {" · "}
              <a
                className="underline"
                href={`/${encodeURIComponent(selected.environmentId)}/${encodeURIComponent(manager.id)}`}
              >
                Open conversation result
              </a>
            </>
          )}
        </p>
      )}
      {runResult?.state === "released" && (
        <p className="mt-1 text-sm text-muted-foreground">
          The native run settled and its claim was released. Review the conversation before treating
          the Task outcome as complete.
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
