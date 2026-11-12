import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { ORCHESTRATION_V2_WS_METHODS } from "./orchestrationV2.ts";
import { DevelopmentPlanRpcError, WS_METHODS, WsRpcGroup } from "./rpc.ts";

const importRpc = WsRpcGroup.requests.get(WS_METHODS.developmentPlansImport)!;
const decodeImport = Schema.decodeUnknownSync(importRpc.payloadSchema);
const encodeImport = Schema.encodeUnknownSync(importRpc.payloadSchema);
const encodePlanError = Schema.encodeUnknownSync(DevelopmentPlanRpcError);
const decodePlanError = Schema.decodeUnknownSync(DevelopmentPlanRpcError);

describe("WebSocket RPC contracts", () => {
  it("round-trips imported bytes and typed conflict errors", () => {
    const value = {
      projectId: "project",
      format: "v1",
      bytesBase64: "WA==",
      planId: "plan",
      revisionId: "revision",
      title: "Example",
      commandId: "import",
    };
    const decoded = decodeImport(value);
    expect(encodeImport(decoded)).toEqual(value);
    const error = new DevelopmentPlanRpcError({ reason: "conflict", currentVersion: 3 });
    const encoded = encodePlanError(error);
    expect(decodePlanError(encoded)).toMatchObject({
      _tag: "DevelopmentPlanRpcError",
      reason: "conflict",
      currentVersion: 3,
    });
  });

  it("does not expose planning pass settlement", () => {
    expect(WsRpcGroup.requests.has("developmentPlans.settlePass" as never)).toBe(false);
  });

  it("rejects server-internal commands sent to dispatchCommand", () => {
    const dispatchCommand = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (dispatchCommand === undefined) throw new Error("dispatchCommand is not registered");
    const decode = Schema.decodeUnknownExit(dispatchCommand.payloadSchema);

    expect(
      Exit.isFailure(
        decode({
          type: "checkpoint.rollback.fail",
          commandId: "forged-rollback-failure",
          threadId: "thread-1",
          requestId: "rollback-1",
          message: "Forged failure.",
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decode({
          type: "checkpoint.rollback",
          commandId: "rollback-1",
          threadId: "thread-1",
          scopeId: "scope-1",
          checkpointId: "checkpoint-1",
        }),
      ),
    ).toBe(true);
  });

  it("lets the server pick a new worktree's folder", () => {
    const createWorktree = WsRpcGroup.requests.get(WS_METHODS.vcsCreateWorktree);
    if (createWorktree === undefined) throw new Error("vcs.createWorktree is not registered");
    const decode = Schema.decodeUnknownExit(createWorktree.payloadSchema);
    const request = { cwd: "/repo", refName: "main", newRefName: "feature/x" };

    expect(Exit.isSuccess(decode({ ...request, path: null }))).toBe(true);
    expect(Exit.isFailure(decode({ ...request, path: "/repo/.git/hooks" }))).toBe(true);
  });
});
