import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { WorkTaskDraftDefinition } from "@t3tools/contracts";
import { runMigrations } from "../src/persistence/Migrations.ts";
import * as TaskStore from "../src/tasks/TaskStore.ts";
import * as TaskService from "../src/tasks/TaskService.ts";

class DemoInputError extends Schema.TaggedError<DemoInputError>()("DemoInputError", {
  message: Schema.String,
}) {}
const decodeDefinition = Schema.decodeUnknownEffect(WorkTaskDraftDefinition);
const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.resolve(
    path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
    "../../..",
  );
  const directory = path.join(root, ".t3", "native-task-demo");
  yield* fs.makeDirectory(directory, { recursive: true });
  const [command, ...args] = process.argv.slice(2);
  if (
    !((command === "create" && args.length === 3) || (command === "inspect" && args.length === 1))
  )
    return yield* new DemoInputError({
      message:
        'Usage: native-task-demo.ts create "title" "objective" "criterion" | inspect <task-id>',
    });
  const database = NodeSqliteClient.layer({ filename: path.join(directory, "tasks.sqlite") });
  const services = TaskService.layer.pipe(
    Layer.provideMerge(TaskStore.layer.pipe(Layer.provideMerge(database))),
  );
  return yield* Effect.gen(function* () {
    yield* runMigrations();
    const tasks = yield* TaskService.TaskService;
    if (command === "create") {
      const definition = yield* decodeDefinition({
        revision: 1,
        title: args[0],
        objective: args[1],
        criteria: [{ id: "criterion-1", condition: args[2], verification: "Human inspection" }],
        actionPolicy: { version: 1, rules: [] },
      });
      yield* Console.log(yield* tasks.create({ definition }));
    } else {
      const taskId = args[0]!;
      const task = yield* tasks.inspect(taskId);
      if (!task) return yield* new DemoInputError({ message: "Task not found" });
      yield* Console.dir(
        { task, results: yield* tasks.inspectResults({ taskId }) },
        { depth: null },
      );
    }
  }).pipe(Effect.provide(services));
});
await Effect.runPromise(program.pipe(Effect.provide(NodeServices.layer)));
