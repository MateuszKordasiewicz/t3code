import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { ExecutionEnvironmentDescriptor } from "./environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("ExecutionEnvironmentDescriptor", () => {
  it("decodes old, recognized and future manual installation descriptors", () => {
    expect(decodeDescriptor(descriptor).capabilities.workflowV1).toBeUndefined();
    expect(decodeDescriptor(descriptor).label).toBe("Local");
    expect(
      [
        { planning: true, runs: false },
        { planning: false, runs: true },
        { planning: true, runs: true },
        { planning: false, runs: false },
      ].map(
        (workflowV1) =>
          decodeDescriptor({
            ...descriptor,
            capabilities: { ...descriptor.capabilities, workflowV1 },
          }).capabilities.workflowV1,
      ),
    ).toEqual([
      { planning: true, runs: false },
      { planning: false, runs: true },
      { planning: true, runs: true },
      { planning: false, runs: false },
    ]);
    for (const workflowV1 of [{ planning: true }, { planning: "true", runs: true }]) {
      expect(() =>
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, workflowV1 },
        }),
      ).toThrow();
    }
    const decodeOlderDescriptor = Schema.decodeUnknownSync(
      Schema.Struct({
        ...ExecutionEnvironmentDescriptor.fields,
        capabilities: Schema.Struct({ repositoryIdentity: Schema.Boolean }),
      }),
    );
    expect(
      decodeOlderDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          workflowV1: { planning: true, runs: false },
        },
      }),
    ).toEqual({
      environmentId: "environment-1",
      label: "Local",
      platform: { os: "darwin", arch: "arm64" },
      serverVersion: "0.0.32",
      capabilities: { repositoryIdentity: true },
    });

    expect(decodeDescriptor(descriptor).capabilities.serverInstallation).toBeUndefined();
    for (const installation of [{ kind: "npx" }, { kind: "npm-global", prefix: "/opt/node" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toEqual(installation);
    }
    for (const installation of [{ kind: "future-manager" }, { kind: "npm-global" }]) {
      expect(
        decodeDescriptor({
          ...descriptor,
          capabilities: { ...descriptor.capabilities, serverInstallation: installation },
        }).capabilities.serverInstallation,
      ).toBeUndefined();
    }
  });
});
