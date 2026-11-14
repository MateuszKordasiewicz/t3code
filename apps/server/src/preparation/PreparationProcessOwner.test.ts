import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Owner from "./PreparationProcessOwner.ts";

const input = { command: "/usr/bin/git", args: ["status"], cwd: "/workspace" };
const bytes = (value: string) => new TextEncoder().encode(value);
const fixture = Effect.fn("test.preparationControllerFixture")(function* (
  options: {
    readonly beforeCreate?: Effect.Effect<void, Owner.PreparationProcessOwnerError>;
    readonly beforeStartup?: Effect.Effect<void, Owner.PreparationProcessOwnerError>;
    readonly beforeCleanup?: Effect.Effect<void>;
    readonly insideWriter?: { value: boolean };
    readonly closeCallerDuringPrepare?: boolean;
  } = {},
) {
  const storeScope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
  const flags = { intakeSealed: false, stopping: false, blocked: false };
  const createEntered = yield* Deferred.make<void>();
  const startupEntered = yield* Deferred.make<void>();
  const cleanupEntered = yield* Deferred.make<void>();
  const calls = { prepared: 0, created: 0, started: 0, cleaned: 0, assessed: 0 };
  const inputs: Owner.CommandInput[] = [];
  const records = new Map<
    object,
    { state: Owner.CommandState; id: string; started: boolean; retired: boolean }
  >();
  const read = (ref: object) => {
    const value = records.get(ref);
    if (!value) throw new Error("Missing controller fixture command");
    return value;
  };
  const port: Owner.CommandPort = {
    checkOutsideWriter: (stage) =>
      Effect.gen(function* () {
        if (options.insideWriter?.value) return yield* Owner.error(stage, "invalid");
      }),
    prepareCommand: (owned, state) =>
      Effect.gen(function* () {
        calls.prepared++;
        inputs.push(owned);
        const ref = Object.freeze({});
        const value = { state, id: `resource-${calls.prepared}`, started: false, retired: false };
        records.set(ref, value);
        yield* Scope.addFinalizer(
          state.commandScope,
          Effect.gen(function* () {
            yield* Deferred.succeed(cleanupEntered, undefined);
            yield* options.beforeCleanup ?? Effect.void;
            calls.cleaned++;
            value.retired = value.started;
          }),
        );
        if (options.closeCallerDuringPrepare)
          yield* Scope.close(state.callerScope as Scope.Closeable, Exit.void);
        return ref;
      }),
    createCommand: (ref) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(createEntered, undefined);
        yield* options.beforeCreate ?? Effect.void;
        calls.created++;
        return { resourceId: read(ref).id, stderr: Stream.make(bytes("early stderr")) };
      }),
    startCommand: (ref) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(startupEntered, undefined);
        yield* options.beforeStartup ?? Effect.void;
        read(ref).started = true;
        calls.started++;
        return Stream.make(bytes("payload"));
      }),
    collectCommand: () => Effect.succeed(0),
    assessInventory: () =>
      Effect.sync(() => {
        calls.assessed++;
        const resources = [...records.values()].map((record) => ({
          resourceId: record.id,
          state: record.retired ? ("settled" as const) : ("held" as const),
        }));
        return {
          kind: "sealed" as const,
          jobId: "job",
          inventory: resources.every((record) => record.state === "settled")
            ? ("all-retired" as const)
            : ("held" as const),
          resources,
        };
      }),
  };
  const owner = yield* Owner.make(
    { jobId: "job", threadId: "thread", runId: "run" },
    storeScope,
    port,
    flags,
  );
  return { owner, flags, calls, inputs, createEntered, startupEntered, cleanupEntered, storeScope };
});
const collect = Effect.fn("test.collectOwnedCommand")(function* (facade: {
  stdout: Stream.Stream<Uint8Array, Owner.PreparationProcessOwnerError>;
  stderr: Stream.Stream<Uint8Array, Owner.PreparationProcessOwnerError>;
  exitCode: Effect.Effect<number, Owner.PreparationProcessOwnerError>;
}) {
  const [stdout, stderr, exitCode] = yield* Effect.all(
    [Stream.runCollect(facade.stdout), Stream.runCollect(facade.stderr), facade.exitCode],
    { concurrency: "unbounded" },
  );
  return {
    stdout: new TextDecoder().decode(Buffer.concat(stdout)),
    stderr: new TextDecoder().decode(Buffer.concat(stderr)),
    exitCode,
  };
});

it.effect(
  "returns early stderr while owned startup waits and refuses seal from its own live caller scope",
  () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const test = yield* fixture({ beforeStartup: Deferred.await(release) });
      const caller = yield* Scope.make();
      const facade = yield* test.owner
        .start(input)
        .pipe(Effect.provideService(Scope.Scope, caller));
      yield* Deferred.await(test.startupEntered);
      assert.deepStrictEqual(Object.keys(facade).sort(), [
        "exitCode",
        "resourceId",
        "stderr",
        "stdout",
      ]);
      assert.strictEqual(
        new TextDecoder().decode(Buffer.concat(yield* Stream.runCollect(facade.stderr))),
        "early stderr",
      );
      assert.strictEqual(test.calls.started, 0);
      const selfSeal = yield* Effect.result(
        test.owner.seal().pipe(Effect.provideService(Scope.Scope, caller)),
      );
      assert.strictEqual(selfSeal._tag, "Failure");
      if (selfSeal._tag === "Failure") assert.strictEqual(selfSeal.failure.reason, "invalid");
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual(
        new TextDecoder().decode(Buffer.concat(yield* Stream.runCollect(facade.stdout))),
        "payload",
      );
      assert.strictEqual(yield* facade.exitCode, 0);
      yield* Scope.close(caller, Exit.void);
      assert.strictEqual((yield* test.owner.seal()).inventory, "all-retired");
      assert.strictEqual(
        (yield* Effect.result(
          test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller)),
        ))._tag,
        "Failure",
      );
    }).pipe(Effect.scoped),
);

it.effect("graceful seal leaves collectors alive until their caller scope closes", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const caller = yield* Scope.make();
    const facade = yield* test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller));
    const sealing = yield* test.owner.seal().pipe(Effect.forkScoped({ startImmediately: true }));
    assert.isTrue(test.flags.intakeSealed);
    assert.isUndefined(sealing.pollUnsafe());
    assert.strictEqual(test.calls.cleaned, 0);
    assert.deepStrictEqual(yield* collect(facade), {
      stdout: "payload",
      stderr: "early stderr",
      exitCode: 0,
    });
    yield* Scope.close(caller, Exit.void);
    assert.strictEqual((yield* Fiber.join(sealing)).inventory, "all-retired");
    assert.strictEqual(test.calls.cleaned, 1);
  }).pipe(Effect.scoped),
);

it.effect("concurrent caller close and stop await the same actual cleanup completion", () =>
  Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const test = yield* fixture({ beforeCleanup: Deferred.await(release) });
    const caller = yield* Scope.make();
    const facade = yield* test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller));
    yield* collect(facade);
    const first = yield* Scope.close(caller, Exit.void).pipe(
      Effect.forkScoped({ startImmediately: true }),
    );
    yield* Deferred.await(test.cleanupEntered);
    const second = yield* test.owner.stop().pipe(Effect.forkScoped({ startImmediately: true }));
    const third = yield* test.owner.stop().pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Effect.sync(() => {
      assert.isUndefined(second.pollUnsafe());
      assert.isUndefined(third.pollUnsafe());
      assert.strictEqual(test.calls.assessed, 0);
      assert.strictEqual(test.calls.cleaned, 0);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    yield* Fiber.join(first);
    assert.deepStrictEqual(yield* Fiber.join(second), yield* Fiber.join(third));
    assert.strictEqual(test.calls.cleaned, 1);
    assert.strictEqual(test.calls.assessed, 1);
  }).pipe(Effect.scoped),
);

it.effect("stop joins a borrowed start body before its caller finalizer", () =>
  Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const test = yield* fixture({ beforeCreate: Deferred.await(release) });
    const calling = yield* test.owner.start(input).pipe(Effect.scoped, Effect.forkScoped);
    yield* Deferred.await(test.createEntered);
    const stopping = yield* test.owner.stop().pipe(Effect.forkScoped);
    const exit = yield* Fiber.await(calling);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
    assert.strictEqual((yield* Fiber.join(stopping)).inventory, "held");
    assert.strictEqual(test.calls.created, 0);
    assert.strictEqual(test.calls.cleaned, 1);
    yield* Deferred.succeed(release, undefined);
    assert.strictEqual(test.calls.created, 0);
  }).pipe(Effect.scoped),
);

it.effect("a caller closing during preparation cannot create or deadlock its own finalizer", () =>
  Effect.gen(function* () {
    const test = yield* fixture({ closeCallerDuringPrepare: true });
    const caller = yield* Scope.make();
    const result = yield* Effect.result(
      test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller)),
    );
    assert.strictEqual(result._tag, "Failure");
    assert.strictEqual(test.calls.created, 0);
    assert.strictEqual(test.calls.cleaned, 1);
    assert.strictEqual((yield* test.owner.stop()).inventory, "held");
  }).pipe(Effect.scoped),
);

it.effect("owns command data and refuses caller authority fields and writer context", () =>
  Effect.gen(function* () {
    const insideWriter = { value: false };
    const test = yield* fixture({ insideWriter });
    const caller = yield* Scope.make();
    let reads = 0;
    for (const value of [
      { ...input, stdin: undefined },
      { ...input, stdin: "" },
      { ...input, threadId: "other" },
      {
        command: input.command,
        get args() {
          reads++;
          return [];
        },
      },
    ]) {
      assert.strictEqual(
        (yield* Effect.result(
          test.owner.start(value).pipe(Effect.provideService(Scope.Scope, caller)),
        ))._tag,
        "Failure",
      );
    }
    assert.strictEqual(reads, 0);
    assert.strictEqual(test.calls.prepared, 0);
    insideWriter.value = true;
    assert.strictEqual(
      (yield* Effect.result(
        test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller)),
      ))._tag,
      "Failure",
    );
    assert.strictEqual(test.calls.prepared, 0);
    insideWriter.value = false;
    const args = ["status"];
    const env = { VALUE: "original" };
    const start = test.owner.start({ command: input.command, args, env });
    args[0] = "changed";
    env.VALUE = "changed";
    const facade = yield* start.pipe(Effect.provideService(Scope.Scope, caller));
    yield* collect(facade);
    assert.deepStrictEqual(test.inputs[0], {
      command: input.command,
      args: ["status"],
      env: { VALUE: "original" },
    });
    insideWriter.value = true;
    assert.strictEqual((yield* Effect.result(test.owner.stop()))._tag, "Failure");
    assert.strictEqual(test.calls.cleaned, 0);
    insideWriter.value = false;
    assert.strictEqual((yield* test.owner.stop()).inventory, "all-retired");
    yield* Scope.close(caller, Exit.void);
    assert.strictEqual(test.calls.cleaned, 1);
  }).pipe(Effect.scoped),
);

it.effect.each([false, true])(
  "forced stop cancels an acquired-writer wait under outer mask %s before creation",
  (masked) =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const test = yield* fixture({ beforeCreate: Deferred.await(release) });
      const caller = yield* Scope.make();
      const work = test.owner.start(input).pipe(Effect.provideService(Scope.Scope, caller));
      const calling = yield* (masked ? Effect.uninterruptible(work) : work).pipe(Effect.forkScoped);
      yield* Deferred.await(test.createEntered);
      const stopping = yield* test.owner.stop().pipe(Effect.forkScoped);
      const exit = yield* Fiber.await(calling);
      assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
      assert.strictEqual(test.calls.created, 0);
      assert.strictEqual((yield* Fiber.join(stopping)).inventory, "held");
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual(test.calls.created, 0);
      yield* Scope.close(caller, Exit.void);
    }).pipe(Effect.scoped),
);
