import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Authorization from "./DevelopmentPlanAuthorization.ts";
import * as Service from "./DevelopmentPlanService.ts";

const destination = { environmentId: "A", projectId: "P" };
const input = {
  actor: { kind: "human" as const, id: "actor" },
  destination,
  operation: "read" as const,
  origins: [],
};
const project = { projectId: ProjectId.make("P"), deletedAt: null } as ProjectStore.ProjectRow;
const thread = {
  id: ThreadId.make("T"),
  projectId: ProjectId.make("P"),
  deletedAt: null,
} as OrchestrationV2ThreadShell;

const fixture = (
  options: {
    project?: "missing" | "deleted" | "error";
    thread?: "missing" | "deleted" | "foreign" | "error";
  } = {},
) => {
  const environment = {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("A")),
  } as ServerEnvironment.ServerEnvironment["Service"];
  const projects = {
    get: () =>
      options.project === "error"
        ? Effect.fail(new ProjectStore.ProjectStoreV2Error({ operation: "get", cause: "private" }))
        : Effect.succeed(
            options.project === "missing"
              ? Option.none()
              : Option.some(
                  options.project === "deleted"
                    ? {
                        ...project,
                        deletedAt:
                          "2026-01-01T00:00:00.000Z" as ProjectStore.ProjectRow["deletedAt"],
                      }
                    : project,
                ),
          ),
  } as unknown as ProjectStore.ProjectStoreV2["Service"];
  const projections = {
    getThreadShell: () =>
      options.thread === "error"
        ? Effect.fail(
            new ProjectionStore.ProjectionStoreReadError({
              threadId: ThreadId.make("T"),
              cause: "private",
            }),
          )
        : Effect.succeed(
            options.thread === "missing"
              ? null
              : options.thread === "foreign"
                ? { ...thread, projectId: ProjectId.make("Q") }
                : options.thread === "deleted"
                  ? {
                      ...thread,
                      deletedAt: {} as NonNullable<OrchestrationV2ThreadShell["deletedAt"]>,
                    }
                  : thread,
          ),
  } as unknown as ProjectionStore.ProjectionStoreV2["Service"];
  return Authorization.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(ServerEnvironment.ServerEnvironment, environment),
        Layer.succeed(ProjectStore.ProjectStoreV2, projects),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
      ),
    ),
  );
};

it.effect("authorizes only active resources in the serving destination", () =>
  Effect.gen(function* () {
    const auth = yield* Service.DevelopmentPlanAuthorization;
    yield* auth.check(input);
    yield* auth.check({ ...input, origins: [destination, { ...destination, threadId: "T" }] });
    for (const invalid of [
      { ...input, destination: { ...destination, environmentId: "B" } },
      { ...input, destination: { ...destination, projectId: " " } },
      { ...input, origins: [{ ...destination, environmentId: "B" }] },
      { ...input, origins: [{ ...destination, projectId: "Q" }] },
      { ...input, origins: [{ ...destination, threadId: " " }] },
    ]) {
      const error = yield* Effect.flip(auth.check(invalid));
      assert.strictEqual(error.reason, "unauthorized");
    }
  }).pipe(Effect.provide(fixture())),
);

it.effect("denies missing, deleted, foreign and failed lookups", () =>
  Effect.gen(function* () {
    for (const options of [
      { project: "missing" as const },
      { project: "deleted" as const },
      { project: "error" as const },
      { thread: "missing" as const },
      { thread: "deleted" as const },
      { thread: "foreign" as const },
      { thread: "error" as const },
    ]) {
      const error = yield* Effect.gen(function* () {
        const auth = yield* Service.DevelopmentPlanAuthorization;
        return yield* Effect.flip(
          auth.check({
            ...input,
            origins: options.project ? [] : [{ ...destination, threadId: "T" }],
          }),
        );
      }).pipe(Effect.provide(fixture(options)));
      assert.strictEqual(error.reason, "unauthorized");
    }
  }),
);
