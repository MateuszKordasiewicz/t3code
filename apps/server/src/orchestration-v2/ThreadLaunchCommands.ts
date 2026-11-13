import {
  type CommandId,
  type MessageId,
  type OrchestrationV2ServerCommand,
  type ThreadId,
} from "@t3tools/contracts";
import type {
  ThreadLaunchInitialMessage,
  ThreadLaunchInput,
  ThreadLaunchWorkspaceStrategy,
} from "./ThreadLaunchService.ts";

export function buildClaimCommand(
  input: ThreadLaunchInput,
  threadId: ThreadId,
  workspaceStrategy: ThreadLaunchWorkspaceStrategy,
): Extract<OrchestrationV2ServerCommand, { type: "thread.create" | "thread.metadata.update" }> {
  const initialBranch = workspaceStrategy.branch ?? null;
  const initialWorktreePath =
    workspaceStrategy.type === "existing_worktree" ? workspaceStrategy.worktreePath : null;
  return input.reuseExistingThread === true
    ? {
        type: "thread.metadata.update",
        commandId: input.commandId,
        threadId,
        expectedEmpty: true,
      }
    : {
        type: "thread.create",
        commandId: input.commandId,
        threadId,
        projectId: input.projectId,
        title: input.title,
        modelSelection: input.modelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: initialBranch,
        worktreePath: initialWorktreePath,
        ...(input.importedNativeThread === undefined
          ? {}
          : { importedNativeThread: input.importedNativeThread }),
        ...(input.linkOrigin === undefined ? {} : { linkOrigin: input.linkOrigin }),
        ...(input.delegatedFrom === undefined ? {} : { delegatedFrom: input.delegatedFrom }),
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      };
}

export function buildInitialMessageCommand(
  input: ThreadLaunchInput,
  initialMessage: ThreadLaunchInitialMessage,
  commandId: CommandId,
  threadId: ThreadId,
  messageId: MessageId,
  workspaceStrategy: ThreadLaunchWorkspaceStrategy,
): Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }> {
  return {
    type: "message.dispatch",
    commandId,
    threadId,
    messageId,
    text: initialMessage.text,
    ...(initialMessage.scheduledTaskId === undefined
      ? {}
      : { scheduledTaskId: initialMessage.scheduledTaskId }),
    ...(initialMessage.senderThreadId === undefined
      ? {}
      : { senderThreadId: initialMessage.senderThreadId }),
    attachments: initialMessage.attachments,
    ...(initialMessage.context ? { context: initialMessage.context } : {}),
    ...(input.generateTitle === true ? { titleSeed: input.title } : {}),
    modelSelection: input.modelSelection,
    dispatchMode: { type: "defer_start", workspaceStrategy },
    createdBy: input.createdBy,
    creationSource: input.creationSource,
  };
}
