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
export const nativeTaskConfigureAllowance = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:configure-allowance",
  tag: WS_METHODS.workTasksConfigureNativeAllowance,
});
export const nativeTaskStartPrepared = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:start-prepared",
  tag: WS_METHODS.workTasksStartPrepared,
});
export const nativeTaskReserveLaunch = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:reserve-launch",
  tag: WS_METHODS.workTasksReserveLaunch,
});
export const nativeTaskInspectNativeRun = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "native-task:inspect-native-run",
  tag: WS_METHODS.workTasksInspectNativeRun,
});
