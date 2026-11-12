import {
  decodeDevelopmentPlanRevisionV1,
  type DevelopmentPlanRevisionV1,
  type PlanningPassV1,
  type RevisionEnvelope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";

export type BlockerCode =
  | "requirement-without-criterion"
  | "criterion-without-work-item"
  | "criterion-without-verification"
  | "open-blocking-question"
  | "review-missing"
  | "review-not-review"
  | "review-unsettled"
  | "review-stale"
  | "review-not-passing"
  | "anchored-omission"
  | "blocking-finding"
  | "invalid-reference"
  | "dependency-cycle"
  | "invalid-record";
export type Blocker = { code: BlockerCode; objectId?: string };
export type CoverageResult = {
  revisionHash: string;
  sourcePacketHash: string;
  structuralBlockers: Blocker[];
  reviewBlockers: Blocker[];
};

const sortBlockers = (blockers: Blocker[]) =>
  blockers.sort((a, b) =>
    a.code < b.code
      ? -1
      : a.code > b.code
        ? 1
        : (a.objectId ?? "") < (b.objectId ?? "")
          ? -1
          : (a.objectId ?? "") > (b.objectId ?? "")
            ? 1
            : 0,
  );

function validate(input: {
  revision: DevelopmentPlanRevisionV1;
  sourcePacket: RevisionEnvelope["sourcePacket"];
  review?: PlanningPassV1;
}): CoverageResult {
  const { revision, sourcePacket, review } = input;
  const revisionHash = DevelopmentPlanHash.revision(revision);
  const sourcePacketHash = DevelopmentPlanHash.sourcePacket(sourcePacket);
  const structuralBlockers: Blocker[] = [];
  const activeWork = revision.workItems.filter((item) => item.state === "active");
  const mapped = new Set(activeWork.flatMap((item) => item.criterionIds));
  const verified = new Set(
    activeWork.flatMap((item) => item.verification.flatMap((v) => v.criterionIds)),
  );
  for (const requirement of revision.requirements) {
    if (requirement.state === "withdrawn") continue;
    const criteria = requirement.criteria.filter((criterion) => criterion.state !== "withdrawn");
    if (criteria.length === 0)
      structuralBlockers.push({ code: "requirement-without-criterion", objectId: requirement.id });
    for (const criterion of criteria) {
      if (!mapped.has(criterion.id))
        structuralBlockers.push({ code: "criterion-without-work-item", objectId: criterion.id });
      if (!verified.has(criterion.id))
        structuralBlockers.push({ code: "criterion-without-verification", objectId: criterion.id });
    }
  }
  for (const question of revision.questions) {
    if (question.state === "open" && question.executionBlocking)
      structuralBlockers.push({ code: "open-blocking-question", objectId: question.id });
  }
  const reviewBlockers: Blocker[] = [];
  if (!review) reviewBlockers.push({ code: "review-missing" });
  else if (review.role !== "review" || (review.report && review.report.kind !== "review"))
    reviewBlockers.push({ code: "review-not-review" });
  else if (review.status !== "settled" || !review.report)
    reviewBlockers.push({ code: "review-unsettled" });
  else if (review.report.kind === "review") {
    const { assessment, verdict, findings } = review.report;
    if (
      review.binding.revisionHash !== revisionHash ||
      review.binding.sourcePacketHash !== sourcePacketHash ||
      assessment.revisionHash !== revisionHash ||
      assessment.sourcePacketHash !== sourcePacketHash
    )
      reviewBlockers.push({ code: "review-stale" });
    if (verdict !== "pass") reviewBlockers.push({ code: "review-not-passing" });
    for (const entry of assessment.inventory) {
      if (entry.disposition !== "mapped")
        reviewBlockers.push({ code: "anchored-omission", objectId: entry.id });
    }
    for (const finding of findings) {
      if (finding.severity === "blocking")
        reviewBlockers.push({ code: "blocking-finding", objectId: finding.id });
    }
  }
  return {
    revisionHash,
    sourcePacketHash,
    structuralBlockers: sortBlockers(structuralBlockers),
    reviewBlockers: sortBlockers(reviewBlockers),
  };
}

const prevalidate = (envelope: unknown) =>
  decodeDevelopmentPlanRevisionV1(envelope).pipe(
    Effect.match({
      onSuccess: (value) => ({ ok: true as const, envelope: value }),
      onFailure: (error) => ({
        ok: false as const,
        blockers: [
          {
            code:
              error.reason === "dangling reference"
                ? ("invalid-reference" as const)
                : error.reason === "dependency cycle"
                  ? ("dependency-cycle" as const)
                  : ("invalid-record" as const),
          },
        ],
      }),
    }),
  );

export const DevelopmentPlanCoverage = { validate, prevalidate };
