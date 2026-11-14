// @effect-diagnostics nodeBuiltinImport:off -- These fixtures exercise the actual Node adapter boundary without launching children.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { prepare } from "./PreparationNodeChild.ts";

vi.mock("node:child_process", { spy: true });

const image = {
  executable: "/bin/sh",
  args: ["immutable"],
  cwd: "/fixture",
  environment: { PRIVATE: "secret" },
};
const fixture = (
  options: {
    pid?: number;
    ignoreSignals?: boolean;
    setupFailure?: boolean;
    onSignal?: (signal: NodeJS.Signals) => void;
  } = {},
) => {
  const raw = new NodeEvents.EventEmitter();
  const stdin = new NodeStream.PassThrough();
  const stdout = new NodeStream.PassThrough();
  const stderr = new NodeStream.PassThrough();
  const signals: NodeJS.Signals[] = [];
  const state: { exitCode: number | null; signalCode: NodeJS.Signals | null } = {
    exitCode: null,
    signalCode: null,
  };
  const finish = (code: number | null, signal: NodeJS.Signals | null) => {
    state.exitCode = code;
    state.signalCode = signal;
    raw.emit("exit", code, signal);
    stdin.end();
    stdout.end();
    stderr.end();
  };
  const handle = Object.assign(raw, {
    pid: options.pid ?? 123456789,
    stdin,
    stderr,
    kill: (signal: NodeJS.Signals) => {
      signals.push(signal);
      options.onSignal?.(signal);
      if (!options.ignoreSignals) finish(null, signal);
      return true;
    },
  });
  Object.defineProperties(handle, {
    stdout: {
      get: () => {
        if (options.setupFailure) throw new Error("private setup diagnostic");
        return stdout;
      },
    },
    exitCode: { get: () => state.exitCode },
    signalCode: { get: () => state.signalCode },
  });
  return {
    raw,
    handle: handle as unknown as NodeChildProcess.ChildProcessWithoutNullStreams,
    stdin,
    stdout,
    stderr,
    finish,
    signals,
  };
};

it.effect("creates locally without awaiting spawn and owns exact immutable Node options", () =>
  Effect.gen(function* () {
    const child = fixture();
    using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
    const mutable = structuredClone(image);
    const preparation = prepare(mutable);
    mutable.args[0] = "changed";
    mutable.environment.PRIVATE = "changed";
    const prepared = yield* preparation;
    assert.strictEqual(spawn.mock.calls.length, 0);
    const local = yield* prepared.createLocal();
    assert.strictEqual(spawn.mock.calls.length, 1);
    assert.deepStrictEqual(spawn.mock.calls[0], [
      image.executable,
      ["immutable"],
      {
        cwd: image.cwd,
        env: Object.setPrototypeOf({ ...image.environment }, null),
        detached: true,
        shell: false,
        windowsHide: false,
        stdio: ["pipe", "pipe", "pipe"],
      },
    ]);
    assert.isFalse(yield* local.isRunning);
    assert.deepStrictEqual(child.signals, []);
    const ready = yield* local.awaitSpawn.pipe(Effect.forkScoped);
    assert.isUndefined(ready.pollUnsafe());
    child.raw.emit("spawn");
    yield* Fiber.join(ready);
    assert.isTrue(yield* local.isRunning);
    child.finish(0, null);
    assert.deepStrictEqual(yield* local.awaitTerminal, { code: 0, signal: null });
    assert.strictEqual(yield* local.exitCode, 0);
    assert.strictEqual((yield* Effect.result(prepared.createLocal()))._tag, "Failure");
    assert.strictEqual(spawn.mock.calls.length, 1);
  }).pipe(Effect.scoped),
);

it.effect(
  "closed original owner refuses creation and its retained child alone receives cleanup",
  () =>
    Effect.gen(function* () {
      const child = fixture();
      using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
      const closedOwner = yield* Scope.make();
      const unused = yield* prepare(image).pipe(Effect.provideService(Scope.Scope, closedOwner));
      yield* Scope.close(closedOwner, Exit.void);
      const result = yield* Effect.result(unused.createLocal());
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "closed");
      assert.strictEqual(spawn.mock.calls.length, 0);
      const owner = yield* Scope.make();
      const prepared = yield* prepare(image).pipe(Effect.provideService(Scope.Scope, owner));
      const local = yield* prepared.createLocal();
      assert.deepStrictEqual(child.signals, []);
      yield* Scope.close(owner, Exit.void);
      assert.deepStrictEqual(child.signals, ["SIGTERM"]);
      assert.deepStrictEqual(yield* local.awaitTerminal, { code: null, signal: "SIGTERM" });
      assert.strictEqual((yield* Effect.result(local.exitCode))._tag, "Failure");
      assert.strictEqual((yield* Effect.result(prepared.createLocal()))._tag, "Failure");
      child.raw.emit("spawn");
      assert.strictEqual(spawn.mock.calls.length, 1);
    }),
);

it.effect("retains early sanitized errors and never retries synchronous creation failures", () =>
  Effect.gen(function* () {
    const child = fixture();
    using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
    const local = yield* (yield* prepare(image)).createLocal();
    child.raw.emit("error", new Error("PRIVATE secret command argv"));
    const error = yield* Effect.result(local.awaitSpawn);
    assert.strictEqual(error._tag, "Failure");
    assert.isFalse(JSON.stringify(error).includes("PRIVATE"));
    assert.strictEqual((yield* Effect.result(local.exitCode))._tag, "Failure");
    child.finish(null, "SIGKILL");
    assert.deepStrictEqual(yield* local.awaitTerminal, { code: null, signal: "SIGKILL" });
    child.raw.emit("error", new Error("late private diagnostic"));
    assert.isAtLeast(child.raw.listenerCount("error"), 1);
    const prepared = yield* prepare(image);
    spawn.mockImplementation(() => {
      throw new Error("secret argv cwd environment");
    });
    const failed = yield* Effect.result(prepared.createLocal());
    assert.strictEqual(failed._tag, "Failure");
    assert.isFalse(JSON.stringify(failed).includes("secret"));
    assert.strictEqual((yield* Effect.result(prepared.createLocal()))._tag, "Failure");
    assert.strictEqual(spawn.mock.calls.length, 2);
  }).pipe(Effect.scoped),
);

it.effect.each(["missing-pid", "setup-failure"] as const)(
  "owns cleanup after actual handle return followed by %s",
  (failure) =>
    Effect.gen(function* () {
      const child = fixture({
        pid: failure === "missing-pid" ? 0 : 123456789,
        setupFailure: failure === "setup-failure",
      });
      using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
      const owner = yield* Scope.make();
      const prepared = yield* prepare(image).pipe(Effect.provideService(Scope.Scope, owner));
      const result = yield* Effect.result(prepared.createLocal());
      assert.strictEqual(result._tag, "Failure");
      assert.isFalse(JSON.stringify(result).includes("private setup"));
      assert.deepStrictEqual(child.signals, []);
      yield* Scope.close(owner, Exit.void);
      assert.isAtLeast(child.signals.length, 1);
      assert.strictEqual((yield* Effect.result(prepared.createLocal()))._tag, "Failure");
      assert.strictEqual(spawn.mock.calls.length, 1);
    }),
);

it.effect.each([false, true])(
  "retains created child after actual pending interruption under mask %s",
  (masked) =>
    Effect.gen(function* () {
      const child = fixture();
      let creator: Fiber.Fiber<unknown, unknown> | undefined;
      using spawn = vi.spyOn(NodeChildProcess, "spawn").mockImplementation(() => {
        creator!.interruptUnsafe();
        return child.handle;
      });
      const owner = yield* Scope.make();
      const prepared = yield* prepare(image).pipe(Effect.provideService(Scope.Scope, owner));
      const calling = yield* Effect.withFiber((fiber) => {
        creator = fiber;
        return masked ? Effect.uninterruptible(prepared.createLocal()) : prepared.createLocal();
      }).pipe(Effect.forkScoped);
      const exit = yield* Fiber.await(calling);
      assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
      assert.strictEqual(spawn.mock.calls.length, 1);
      assert.deepStrictEqual(child.signals, []);
      assert.isAtLeast(child.raw.listenerCount("error"), 1);
      yield* Scope.close(owner, Exit.void);
      assert.isAtLeast(child.signals.length, 1);
      child.raw.emit("spawn");
      assert.strictEqual(spawn.mock.calls.length, 1);
    }).pipe(Effect.scoped),
);

it.effect(
  "cleanup has separate outside-creation budgets and preserves missing terminal evidence",
  () =>
    Effect.gen(function* () {
      const signalEvents = yield* Queue.unbounded<NodeJS.Signals>();
      const child = fixture({
        ignoreSignals: true,
        onSignal: (signal) => {
          Queue.offerUnsafe(signalEvents, signal);
        },
      });
      using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
      const local = yield* (yield* prepare(image)).createLocal();
      child.raw.emit("spawn");
      const stopping = yield* local.kill().pipe(Effect.result, Effect.forkScoped);
      assert.strictEqual(yield* Queue.take(signalEvents), "SIGTERM");
      yield* TestClock.adjust("1 second");
      assert.strictEqual(yield* Queue.take(signalEvents), "SIGKILL");
      yield* TestClock.adjust("1 second");
      const result = yield* Fiber.join(stopping);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "timeout");
      assert.isTrue(yield* local.isRunning);
      child.finish(null, "SIGKILL");
      assert.deepStrictEqual(yield* local.awaitTerminal, { code: null, signal: "SIGKILL" });
      assert.strictEqual(spawn.mock.calls.length, 1);
    }).pipe(Effect.scoped),
);

it.effect("refuses descriptor and image substitution before any Node creation", () =>
  Effect.gen(function* () {
    const child = fixture();
    using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
    let getters = 0;
    const getter = Object.defineProperty({}, "executable", {
      enumerable: true,
      get: () => {
        getters++;
        return image.executable;
      },
    });
    for (const candidate of [
      getter,
      { ...image, cwd: "relative" },
      { ...image, args: ["bad\0argument"] },
      { ...image, args: Array(1) },
      { ...image, environment: { PRIVATE: undefined } },
      { ...image, stdin: "payload" },
    ]) {
      assert.strictEqual(
        (yield* Effect.result(prepare(candidate as typeof image)))._tag,
        "Failure",
      );
    }
    assert.strictEqual(getters, 0);
    assert.strictEqual(spawn.mock.calls.length, 0);
  }).pipe(Effect.scoped),
);

it.effect(
  "refuses creation as soon as original owner closing begins before its own finalizer",
  () =>
    Effect.gen(function* () {
      const child = fixture();
      using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
      const owner = yield* Scope.make();
      const prepared = yield* prepare(image).pipe(Effect.provideService(Scope.Scope, owner));
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Scope.addFinalizer(
        owner,
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      );
      const closing = yield* Scope.close(owner, Exit.void).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Effect.gen(function* () {
        const result = yield* Effect.result(prepared.createLocal());
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "closed");
        assert.strictEqual(spawn.mock.calls.length, 0);
      }).pipe(
        Effect.ensuring(Deferred.succeed(release, undefined)),
        Effect.ensuring(Fiber.join(closing)),
      );
    }).pipe(Effect.scoped),
);

it.effect("passes a null-prototype complete environment to the real Node entry point", () =>
  Effect.gen(function* () {
    const child = fixture();
    using spawn = vi.spyOn(NodeChildProcess, "spawn").mockReturnValue(child.handle);
    const prepared = yield* prepare(image);
    yield* prepared.createLocal();
    const environment = spawn.mock.calls[0]?.[2]?.env;
    assert.strictEqual(Object.getPrototypeOf(environment), null);
    assert.deepStrictEqual(Object.entries(environment!), Object.entries(image.environment));
    child.finish(0, null);
  }).pipe(Effect.scoped),
);
