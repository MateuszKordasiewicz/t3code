import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { ProviderAdapterV2RuntimePolicy } from "./ProviderAdapter.ts";

const decode = Schema.decodeUnknownSync(ProviderAdapterV2RuntimePolicy);
const encode = Schema.encodeSync(ProviderAdapterV2RuntimePolicy);
const policy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
  approvalPolicy: { mode: "ask" },
  sandboxPolicy: { mode: "workspace" },
  maxRuntimeMode: "full-access",
} as const;
const ceiling = {
  role: "reviewer",
  policyHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  cwd: "/candidate",
} as const;

describe("ProviderAdapterV2RuntimePolicy", () => {
  it("decodes existing policies unchanged", () => {
    expect(decode(policy)).toEqual({
      runtimeMode: "full-access",
      interactionMode: "default",
      cwd: null,
      approvalPolicy: { mode: "ask" },
      sandboxPolicy: { mode: "workspace" },
      maxRuntimeMode: "full-access",
    });
  });

  it("roundtrips a reviewer ceiling", () => {
    expect(decode(encode(decode({ ...policy, workflowExecutionCeiling: ceiling })))).toEqual({
      runtimeMode: "full-access",
      interactionMode: "default",
      cwd: null,
      approvalPolicy: { mode: "ask" },
      sandboxPolicy: { mode: "workspace" },
      maxRuntimeMode: "full-access",
      workflowExecutionCeiling: {
        role: "reviewer",
        policyHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        cwd: "/candidate",
      },
    });
  });

  it.each([
    { ...ceiling, role: "implementation" },
    { role: "reviewer", cwd: "/candidate" },
    { ...ceiling, policyHash: "not-a-hash" },
    { ...ceiling, policyHash: "A123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" },
    { ...ceiling, cwd: "" },
  ])("rejects a malformed ceiling %#", (workflowExecutionCeiling) => {
    expect(() => decode({ ...policy, workflowExecutionCeiling })).toThrow();
  });
});
