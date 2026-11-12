import type { PlanningPassV1, PlanningSessionV1 } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

type Native = NonNullable<PlanningPassV1["native"]>;

export type PlanningObservation =
  | { readonly kind: "unknown" }
  | {
      readonly kind: "terminal";
      readonly operationId: PlanningPassV1["operationId"];
      readonly passId: PlanningPassV1["id"];
      readonly native: Required<Native>;
      readonly outcome: "succeeded" | "failed" | "cancelled";
      readonly actualInputManifest: PlanningPassV1["inputManifest"];
      readonly inputManifestHash: string;
      readonly freshConversation: boolean;
      readonly sourcePacketDelivered: boolean;
      readonly forbiddenContextDelivered: boolean;
      readonly restrictedProfileQualified: boolean;
    };

export class PlanningPassObservation extends Context.Service<
  PlanningPassObservation,
  {
    readonly observe: (input: {
      readonly sessionId: PlanningSessionV1["id"];
      readonly passId: PlanningPassV1["id"];
      readonly operationId: PlanningPassV1["operationId"];
      readonly native: Native;
    }) => Effect.Effect<PlanningObservation>;
  }
>()("t3/developmentPlans/PlanningPassObservation") {}

export const unknownObservationLayer = Layer.succeed(PlanningPassObservation, {
  observe: Effect.fn("PlanningPassObservation.observe")(() =>
    Effect.succeed({ kind: "unknown" as const }),
  ),
});
