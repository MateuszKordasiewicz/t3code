import { WS_METHODS } from "@t3tools/contracts";
import { createEnvironmentRpcCommand } from "@t3tools/client-runtime/state/runtime";
import { connectionAtomRuntime } from "../connection/runtime";

export const nativeTaskCreateDraft = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:create-draft",
  tag: WS_METHODS.workTasksCreateDraft,
});
export const nativeTaskInspectDraft = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:inspect-draft",
  tag: WS_METHODS.workTasksInspectDraft,
});
export const nativeTaskAcceptDraft = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:accept-draft",
  tag: WS_METHODS.workTasksAcceptDraft,
});
export const nativeTaskCreateInitialGroup = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:create-initial-group",
  tag: WS_METHODS.workTasksCreateInitialGroup,
});
