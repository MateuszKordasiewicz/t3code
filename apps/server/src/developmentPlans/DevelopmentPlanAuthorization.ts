import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Service from "./DevelopmentPlanService.ts";

const isEnvironmentId = Schema.is(EnvironmentId);
const isProjectId = Schema.is(ProjectId);
const isThreadId = Schema.is(ThreadId);

const make = Effect.gen(function* () {
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const check = Effect.fn("DevelopmentPlanAuthorization.check")(
    function* (input: Parameters<Service.DevelopmentPlanAuthorization["Service"]["check"]>[0]) {
      const deny = () => new Service.DevelopmentPlanServiceError({ reason: "unauthorized" });
      const destination = input.destination;
      const servingId = yield* environment.getEnvironmentId;
      if (
        !isEnvironmentId(destination.environmentId) ||
        !isProjectId(destination.projectId) ||
        destination.environmentId !== servingId
      )
        return yield* deny();
      const project = yield* projects.get(ProjectId.make(destination.projectId));
      if (
        Option.isNone(project) ||
        project.value.deletedAt !== null ||
        project.value.projectId !== destination.projectId
      )
        return yield* deny();
      for (const origin of input.origins) {
        if (
          origin.environmentId !== servingId ||
          origin.projectId !== destination.projectId ||
          (origin.threadId !== undefined && !isThreadId(origin.threadId))
        )
          return yield* deny();
        if (origin.threadId !== undefined) {
          const thread = yield* projections.getThreadShell(ThreadId.make(origin.threadId));
          if (
            thread === null ||
            thread.deletedAt !== null ||
            thread.projectId !== destination.projectId ||
            thread.id !== origin.threadId
          )
            return yield* deny();
        }
      }
    },
    Effect.mapError(() => new Service.DevelopmentPlanServiceError({ reason: "unauthorized" })),
  );
  return Service.DevelopmentPlanAuthorization.of({ check });
});

export const layer = Layer.effect(Service.DevelopmentPlanAuthorization, make);
