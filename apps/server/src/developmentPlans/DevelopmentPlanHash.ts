import type { DevelopmentPlanRevisionV1, RevisionEnvelope } from "@t3tools/contracts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

const sourceTag = "t3.planning-source-packet.v1";
const commandTag = "t3.development-plan-command.v1";

function sourcePacket(packet: RevisionEnvelope["sourcePacket"]): string {
  WorkflowCanonicalHash.assertCanonical(packet);
  return WorkflowCanonicalHash.tagged(sourceTag, {
    ...packet,
    sources: packet.sources.map(({ bytesBase64: _bytesBase64, ...source }) => source),
  });
}

function command(value: {
  actor: unknown;
  destination: unknown;
  method: string;
  input: object;
}): string {
  WorkflowCanonicalHash.assertCanonical(value);
  const { commandId: _commandId, ...input } = value.input as Record<string, unknown>;
  return WorkflowCanonicalHash.tagged(commandTag, {
    actor: value.actor,
    destination: value.destination,
    method: value.method,
    input,
  });
}

export const DevelopmentPlanHash = {
  sourcePacket,
  revision: (revision: DevelopmentPlanRevisionV1): string =>
    WorkflowCanonicalHash.tagged("t3.development-plan-revision.v1", revision),
  command,
};
