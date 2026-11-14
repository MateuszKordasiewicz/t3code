// @effect-diagnostics nodeBuiltinImport:off -- Real subprocess fixtures use isolated temporary files.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodePathService from "@effect/platform-node/NodePath";
import { AgentScope, type AgentScopeCustody } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { preparePreparationProcessGate } from "./PreparationProcessGate.ts";
import * as NodeChild from "./PreparationNodeChild.ts";

const encode = (value: string) => new TextEncoder().encode(value);
const input = { command: "/usr/bin/fixture", args: ["original"], cwd: "/work" };
const makeHarness = Effect.fn("test.makePreparationGateHarness")(function* (
  options: {
    readonly ready?: "split" | "wrong-pid" | "oversize" | "eof" | "early-exec" | "withheld";
    readonly writer?: "withheld-ack" | "blocked" | "typed" | "defect";
    readonly passive?: boolean;
    readonly exec?: "split" | "bad" | "oversize" | "eof";
    readonly binding?: "missing" | "wrong-unit" | "wrong-pid" | "dead";
    readonly payload?: Uint8Array;
    readonly creationEvent?: "withheld";
  } = {},
) {
  const spawnWaitStarted = yield* Deferred.make<void>();
  const creationStarted = yield* Deferred.make<void>();
  const spawnEvent = yield* Deferred.make<void>();
  const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const writes = yield* Queue.unbounded<string>();
  const entered = yield* Deferred.make<void>();
  const allowWrite = yield* Deferred.make<void>();
  const writerClosed = yield* Deferred.make<void>();
  const readStarted = yield* Deferred.make<void>();
  const readRequests = yield* Queue.unbounded<void>();
  const state = { wraps: 0, spawns: 0, binds: 0, pulls: 0, written: 0, kills: 0, running: true };
  let nonce = "";
  let command: ChildProcess.StandardCommand | undefined;
  const binding = Object.freeze({
    unit: "gate-fixture.scope",
    invocationId: "1".repeat(32),
    controlGroup: "/gate-fixture.scope",
    pid: 999999991,
    pidStartTicks: "123",
  });
  const custody: AgentScopeCustody = {
    bind: (pid) =>
      Effect.sync(() => {
        state.binds++;
        assert.strictEqual(pid, binding.pid);
        if (options.binding === "missing") return undefined;
        if (options.binding === "dead") state.running = false;
        return options.binding === "wrong-unit"
          ? Object.freeze({ ...binding, unit: "foreign.scope" })
          : options.binding === "wrong-pid"
            ? Object.freeze({ ...binding, pid: 1 })
            : binding;
      }),
    stopAndObserve: () => Effect.undefined,
    ...(options.passive === false ? {} : { observeRetirement: () => Effect.undefined }),
  };
  const scope: typeof AgentScope.Service = {
    ...AgentScope.defaultValue(),
    wrap: (request) =>
      Effect.sync(() => {
        state.wraps++;
        nonce = request.args[3]!;
        return { command: request.command, args: request.args, unit: binding.unit, custody };
      }),
  };
  const emitExec = Effect.fn("test.emitPreparationExec")(function* () {
    const header =
      options.exec === "bad"
        ? "EXEC:foreign\n"
        : options.exec === "oversize"
          ? "x".repeat(256) + "\n"
          : "EXEC:" + nonce + "\n";
    if (options.exec === "split") {
      for (const byte of encode(header)) yield* Queue.offer(output, Uint8Array.of(byte));
      yield* Queue.offer(output, options.payload ?? encode("payload"));
    } else if (options.exec !== "eof")
      yield* Queue.offer(
        output,
        Buffer.concat([encode(header), options.payload ?? encode("payload")]),
      );
    yield* Queue.end(output);
  });
  const createFake = (selected: ChildProcess.Command) =>
    Effect.gen(function* () {
      state.spawns++;
      assert.strictEqual(selected._tag, "StandardCommand");
      if (selected._tag !== "StandardCommand") throw new Error("unexpected pipeline");
      command = selected;
      if (options.ready === "eof") yield* Queue.end(output);
      else if (options.ready !== "withheld") {
        const header =
          options.ready === "oversize"
            ? "x".repeat(256) + "\n"
            : "READY:" +
              nonce +
              ":" +
              (options.ready === "wrong-pid" ? "1" : String(binding.pid)) +
              "\n";
        if (options.ready === "split")
          for (const byte of encode(header)) yield* Queue.offer(output, Uint8Array.of(byte));
        else
          yield* Queue.offer(
            output,
            encode(header + (options.ready === "early-exec" ? "EXEC:" + nonce + "\n" : "")),
          );
      }
      Deferred.doneUnsafe(creationStarted, Exit.void);
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(binding.pid),
        exitCode: Effect.never,
        isRunning: Effect.sync(() => state.running),
        kill: () =>
          Effect.sync(() => {
            state.kills++;
            state.running = false;
          }),
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((bytes: Uint8Array) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            if (options.writer === "blocked") yield* Deferred.await(allowWrite);
            state.written++;
            yield* Queue.offer(writes, new TextDecoder().decode(bytes));
            if (options.writer === "typed")
              return yield* PlatformError.systemError({
                _tag: "Unknown",
                module: "Sink",
                method: "write",
              });
            if (options.writer === "defect") return yield* Effect.die("private sink diagnostic");
            if (options.writer !== "withheld-ack") yield* emitExec();
          }).pipe(Effect.ensuring(Deferred.succeed(writerClosed, undefined))),
        ),
        stdout: Stream.fromPull(
          Effect.succeed(
            Effect.gen(function* () {
              yield* Deferred.succeed(readStarted, undefined);
              yield* Queue.offer(readRequests, undefined);
              const bytes = yield* Queue.take(output);
              state.pulls++;
              return [bytes] as const;
            }),
          ),
        ),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    });
  const spawner = ChildProcessSpawner.make((selected) =>
    createFake(selected).pipe(
      Effect.tap(() =>
        options.creationEvent === "withheld"
          ? Deferred.succeed(spawnWaitStarted, undefined).pipe(
              Effect.andThen(Deferred.await(spawnEvent)),
            )
          : Effect.void,
      ),
    ),
  );
  const nodeCreator: typeof NodeChild.PreparationNodeChild.Service = {
    prepare: (image) => {
      const selected = ChildProcess.make(image.executable, image.args, {
        cwd: image.cwd,
        env: image.environment,
        extendEnv: false,
        stdin: { stream: "pipe", endOnDone: false },
        stdout: "pipe",
        stderr: "pipe",
        detached: true,
        shell: false,
        windowsHide: false,
      });
      Object.freeze(selected.args);
      Object.freeze(selected.options);
      Object.freeze(selected);
      const unknown = () => new NodeChild.PreparationNodeChildError({ reason: "unknown" });
      return Effect.succeed({
        createLocal: () =>
          createFake(selected).pipe(
            Effect.map((child) =>
              Object.freeze({
                pid: child.pid,
                awaitSpawn:
                  options.creationEvent === "withheld" ? Deferred.await(spawnEvent) : Effect.void,
                awaitTerminal: Effect.never,
                exitCode: child.exitCode.pipe(Effect.mapError(unknown)),
                isRunning: Effect.sync(() => state.running),
                kill: () => child.kill().pipe(Effect.mapError(unknown), Effect.as(undefined)),
                stdin: child.stdin.pipe(Sink.mapError(unknown)),
                stdout: child.stdout.pipe(Stream.mapError(unknown)),
                stderr: child.stderr.pipe(Stream.mapError(unknown)),
              }),
            ),
          ),
      });
    },
  };
  const layer = Layer.mergeAll(
    NodePathService.layerPosix,
    Layer.succeed(AgentScope, scope),
    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Layer.succeed(NodeChild.PreparationNodeChild, nodeCreator),
    Layer.succeed(HostProcess.Environment, { PATH: "/usr/bin:/bin" }),
    Layer.succeed(HostProcess.WorkingDirectory, "/host"),
  );
  return {
    state,
    binding,
    creationStarted,
    spawnWaitStarted,
    spawnEvent,
    layer,
    output,
    writes,
    entered,
    allowWrite,
    writerClosed,
    readStarted,
    readRequests,
    emitExec,
    command: () => command,
  };
});

it.effect("gate refuses any stdin property before wrapping or spawning", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness();
    for (const stdin of [undefined, "", "private input"]) {
      const result = yield* Effect.result(
        preparePreparationProcessGate({ ...input, stdin } as typeof input).pipe(
          Effect.provide(harness.layer),
        ),
      );
      assert.strictEqual(result._tag, "Failure");
    }
    assert.strictEqual(harness.state.wraps, 0);
    assert.strictEqual(harness.state.spawns, 0);
  }).pipe(Effect.provide(NodePathService.layerPosix)),
);

it.effect("gate submits locally without reading EXEC and preserves coalesced binary payload", () =>
  Effect.gen(function* () {
    const payload = Uint8Array.of(0, 255, ...encode("hi 🐕\n"));
    const harness = yield* makeHarness({ writer: "withheld-ack", payload });
    yield* Effect.gen(function* () {
      const prepared = yield* preparePreparationProcessGate(input);
      const pending = yield* prepared.spawnHeld();
      assert.strictEqual(harness.state.pulls, 0);
      assert.isFalse(Object.hasOwn(pending, "stdin"));
      const held = yield* pending.awaitHeld();
      assert.strictEqual(held.binding, harness.binding);
      assert.strictEqual(harness.state.written, 0);
      assert.strictEqual(harness.state.pulls, 1);
      const receipt = yield* held.submitLocal();
      assert.strictEqual(harness.state.pulls, 1);
      assert.isTrue((yield* Queue.take(harness.writes)).startsWith("RELEASE:"));
      const waiting = yield* held.awaitExec(receipt).pipe(Effect.forkScoped);
      yield* harness.emitExec();
      const stdout = yield* Fiber.join(waiting);
      assert.deepStrictEqual(Buffer.concat(yield* Stream.runCollect(stdout)), Buffer.from(payload));
      assert.strictEqual((yield* Effect.result(Stream.runCollect(stdout)))._tag, "Failure");
      assert.strictEqual((yield* Effect.result(held.submitLocal()))._tag, "Failure");
      assert.strictEqual((yield* Effect.result(held.awaitExec(receipt)))._tag, "Failure");
      assert.strictEqual((yield* Effect.result(prepared.spawnHeld()))._tag, "Failure");
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect.each(["wrong-pid", "oversize", "eof", "early-exec"] as const)(
  "gate refuses READY %s before binding or release",
  (ready) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ ready });
      yield* Effect.gen(function* () {
        const prepared = yield* preparePreparationProcessGate(input);
        const pending = yield* prepared.spawnHeld();
        assert.strictEqual((yield* Effect.result(pending.awaitHeld()))._tag, "Failure");
        assert.strictEqual(harness.state.binds, 0);
        assert.strictEqual(harness.state.written, 0);
        assert.strictEqual((yield* Effect.result(pending.awaitHeld()))._tag, "Failure");
        yield* pending.stopOwnedChild();
        assert.strictEqual(harness.state.kills, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "gate accepts a split READY header and refuses missing passive custody before spawn",
  () =>
    Effect.gen(function* () {
      const split = yield* makeHarness({ ready: "split" });
      yield* Effect.gen(function* () {
        const prepared = yield* preparePreparationProcessGate(input);
        const pending = yield* prepared.spawnHeld();
        const held = yield* pending.awaitHeld();
        const receipt = yield* held.submitLocal();
        assert.strictEqual(
          new TextDecoder().decode(
            Buffer.concat(yield* Stream.runCollect(yield* held.awaitExec(receipt))),
          ),
          "payload",
        );
        assert.strictEqual(held.binding.pid, split.binding.pid);
      }).pipe(Effect.provide(split.layer));
      const missing = yield* makeHarness({ passive: false });
      assert.strictEqual(
        (yield* Effect.result(
          preparePreparationProcessGate(input).pipe(Effect.provide(missing.layer)),
        ))._tag,
        "Failure",
      );
      assert.strictEqual(missing.state.spawns, 0);
    }).pipe(Effect.scoped),
);

it.effect.each(["typed", "defect"] as const)(
  "gate consumes %s local failure without leaking diagnostics or resending",
  (writer) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ writer });
      yield* Effect.gen(function* () {
        const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
        const held = yield* pending.awaitHeld();
        const exit = yield* Effect.exit(held.submitLocal());
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.strictEqual(Cause.hasDies(exit.cause), writer === "defect");
          assert.isFalse(String(Cause.squash(exit.cause)).includes("private sink diagnostic"));
        }
        assert.strictEqual((yield* Effect.result(held.submitLocal()))._tag, "Failure");
        assert.strictEqual(harness.state.written, 1);
        assert.strictEqual(harness.state.pulls, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect.each([false, true])(
  "gate cancels a blocked local write before returning (masked=%s)",
  (masked) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ writer: "blocked" });
      yield* Effect.gen(function* () {
        const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
        const held = yield* pending.awaitHeld();
        const work = held.submitLocal();
        const owner = yield* (masked ? Effect.uninterruptible(work) : work).pipe(Effect.forkScoped);
        yield* Deferred.await(harness.entered);
        yield* Fiber.interrupt(owner);
        const exit = yield* Fiber.await(owner);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
        yield* Deferred.await(harness.writerClosed);
        yield* Deferred.succeed(harness.allowWrite, undefined);
        assert.strictEqual(harness.state.written, 0);
        assert.strictEqual((yield* Effect.result(held.submitLocal()))._tag, "Failure");
        assert.strictEqual(harness.state.written, 0);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect("gate local budget expires without waiting EXEC or leaving a delayed write", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness({ writer: "blocked" });
    yield* Effect.gen(function* () {
      const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
      const held = yield* pending.awaitHeld();
      const owner = yield* held.submitLocal().pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(harness.entered);
      yield* TestClock.adjust("2 seconds");
      const result = yield* Fiber.join(owner);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "unknown");
      yield* Deferred.await(harness.writerClosed);
      yield* Deferred.succeed(harness.allowWrite, undefined);
      assert.strictEqual(harness.state.written, 0);
      assert.strictEqual((yield* Effect.result(held.submitLocal()))._tag, "Failure");
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect("gate READY wait stays outside local creation and has its own budget", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness({ ready: "withheld" });
    yield* Effect.gen(function* () {
      const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
      assert.strictEqual(harness.state.binds, 0);
      const waiting = yield* pending.awaitHeld().pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(harness.readStarted);
      yield* TestClock.adjust("15 seconds");
      const result = yield* Fiber.join(waiting);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "timeout");
      assert.strictEqual(harness.state.binds, 0);
      assert.strictEqual(harness.state.written, 0);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect("gate owns image inputs before evaluation and never exposes the private image", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness();
    yield* Effect.gen(function* () {
      const mutable = {
        command: input.command,
        args: ["payload-original"],
        cwd: "nested",
        env: { PRIVATE_GATE_VALUE: "original-secret" },
      };
      const work = preparePreparationProcessGate(mutable);
      mutable.args[0] = "changed";
      mutable.env.PRIVATE_GATE_VALUE = "changed";
      const prepared = yield* work;
      assert.match(prepared.creationInputHash, /^[0-9a-f]{64}$/);
      assert.deepStrictEqual(Object.keys(prepared).sort(), [
        "creationInputHash",
        "proposedUnit",
        "spawnHeld",
      ]);
      assert.isFalse(JSON.stringify(prepared).includes("original-secret"));
      assert.strictEqual(harness.state.spawns, 0);
      yield* prepared.spawnHeld();
      const command = harness.command();
      assert.isDefined(command);
      assert.strictEqual(command!.args.at(-1), "payload-original");
      assert.strictEqual(command!.options.cwd, "/host/nested");
      assert.strictEqual(command!.options.env?.PRIVATE_GATE_VALUE, "original-secret");
      assert.isTrue(Object.isFrozen(command));
      assert.isTrue(Object.isFrozen(command!.options.env));
      assert.deepStrictEqual(command!.options.stdin, { stream: "pipe", endOnDone: false });
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect(
  "gate refuses getters, exotic input, sparse args and excess fields before scope work",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness();
      let reads = 0;
      const getter = Object.defineProperty({ ...input }, "args", {
        enumerable: true,
        get() {
          reads++;
          return [];
        },
      });
      const environment = Object.defineProperty({}, "PRIVATE", {
        enumerable: true,
        get() {
          reads++;
          return "secret";
        },
      });
      for (const value of [
        getter,
        new Map(),
        { ...input, args: Array(1) },
        { ...input, extra: true },
        { ...input, args: ["\ud800"] },
        { ...input, env: environment },
      ]) {
        assert.strictEqual(
          (yield* Effect.result(
            preparePreparationProcessGate(value as typeof input).pipe(
              Effect.provide(harness.layer),
            ),
          ))._tag,
          "Failure",
        );
      }
      assert.strictEqual(reads, 0);
      assert.strictEqual(harness.state.wraps, 0);
      assert.strictEqual(harness.state.spawns, 0);
    }),
);

it.live("gate releases a real Node payload with stdin EOF and exact output", () =>
  Effect.gen(function* () {
    const directory = yield* Effect.acquireRelease(
      Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-preparation-gate-"))),
      (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
    );
    const marker = NodePath.join(directory, "payload-started");
    const payload = Uint8Array.of(0, 255, ...encode("real 🐕\n"));
    const script =
      "const fs=require('node:fs');fs.writeFileSync(process.argv[1],'started');let bytes=0;process.stdin.on('data',chunk=>bytes+=chunk.length);process.stdin.on('end',()=>{process.stdout.write(Buffer.from(" +
      JSON.stringify([...payload]) +
      "));process.stderr.write(String(bytes));});";
    const custody: AgentScopeCustody = {
      bind: (pid) =>
        Effect.succeed(
          Object.freeze({
            unit: "real-gate-fixture.scope",
            invocationId: "1".repeat(32),
            controlGroup: "/real-gate-fixture.scope",
            pid,
            pidStartTicks: "1",
          }),
        ),
      stopAndObserve: () => Effect.undefined,
      observeRetirement: () => Effect.undefined,
    };
    const scope: typeof AgentScope.Service = {
      ...AgentScope.defaultValue(),
      wrap: (request) =>
        Effect.succeed({
          command: request.command,
          args: request.args,
          unit: "real-gate-fixture.scope",
          custody,
        }),
    };
    yield* Effect.gen(function* () {
      const prepared = yield* preparePreparationProcessGate({
        command: process.execPath,
        args: ["-e", script, marker],
        cwd: directory,
        env: { PATH: "/usr/bin:/bin" },
        extendEnv: false,
      });
      const pending = yield* prepared.spawnHeld();
      const stderr = yield* Stream.runCollect(pending.stderr).pipe(Effect.forkScoped);
      const held = yield* pending.awaitHeld();
      assert.isFalse(NodeFS.existsSync(marker));
      assert.isTrue(yield* pending.isRunning);
      const receipt = yield* held.submitLocal();
      const stdout = yield* held.awaitExec(receipt);
      assert.deepStrictEqual(Buffer.concat(yield* Stream.runCollect(stdout)), Buffer.from(payload));
      assert.strictEqual(yield* pending.awaitExit, 0);
      assert.deepStrictEqual(yield* pending.awaitTerminal, { code: 0, signal: null });
      assert.strictEqual(new TextDecoder().decode(Buffer.concat(yield* Fiber.join(stderr))), "0");
      assert.strictEqual(NodeFS.readFileSync(marker, "utf8"), "started");
      assert.isUndefined(yield* pending.observeRetirement());
      const stoppedMarker = NodePath.join(directory, "stopped-payload-started");
      const stopped = yield* (yield* preparePreparationProcessGate({
        command: process.execPath,
        args: [
          "-e",
          "require('node:fs').writeFileSync(process.argv[1],'unexpected')",
          stoppedMarker,
        ],
        cwd: directory,
        env: { PATH: "/usr/bin:/bin" },
        extendEnv: false,
      })).spawnHeld();
      yield* stopped.awaitHeld();
      yield* stopped.stopOwnedChild();
      assert.deepStrictEqual(yield* stopped.awaitTerminal, { code: null, signal: "SIGTERM" });
      assert.strictEqual((yield* Effect.result(stopped.awaitExit))._tag, "Failure");
      assert.isFalse(NodeFS.existsSync(stoppedMarker));
      assert.isUndefined(yield* stopped.observeRetirement());
    }).pipe(Effect.provideService(AgentScope, scope));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["bad", "oversize", "eof"] as const)(
  "gate refuses EXEC %s without a second release",
  (exec) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ exec });
      yield* Effect.gen(function* () {
        const held = yield* (yield* (yield* preparePreparationProcessGate(
          input,
        )).spawnHeld()).awaitHeld();
        const receipt = yield* held.submitLocal();
        const result = yield* Effect.result(held.awaitExec(receipt));
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "protocol");
        assert.strictEqual((yield* Effect.result(held.submitLocal()))._tag, "Failure");
        assert.strictEqual(harness.state.written, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "gate accepts split EXEC and rejects copied receipts without consuming the real receipt",
  () =>
    Effect.gen(function* () {
      const payload = Uint8Array.of(0, 255, ...encode("split 🐕"));
      const harness = yield* makeHarness({ exec: "split", payload });
      yield* Effect.gen(function* () {
        const held = yield* (yield* (yield* preparePreparationProcessGate(
          input,
        )).spawnHeld()).awaitHeld();
        const receipt = yield* held.submitLocal();
        assert.strictEqual(
          (yield* Effect.result(held.awaitExec(Object.freeze({ ...receipt }))))._tag,
          "Failure",
        );
        assert.strictEqual(harness.state.pulls, 1);
        assert.deepStrictEqual(
          Buffer.concat(yield* Stream.runCollect(yield* held.awaitExec(receipt))),
          Buffer.from(payload),
        );
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect("gate EXEC acknowledgement has a separate budget after local submission", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness({ writer: "withheld-ack" });
    yield* Effect.gen(function* () {
      const held = yield* (yield* (yield* preparePreparationProcessGate(
        input,
      )).spawnHeld()).awaitHeld();
      yield* Queue.take(harness.readRequests);
      const receipt = yield* held.submitLocal();
      const waiting = yield* held.awaitExec(receipt).pipe(Effect.result, Effect.forkScoped);
      yield* Queue.take(harness.readRequests);
      yield* TestClock.adjust("5 seconds");
      const result = yield* Fiber.join(waiting);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "timeout");
      assert.strictEqual(harness.state.written, 1);
      assert.strictEqual((yield* Effect.result(held.awaitExec(receipt)))._tag, "Failure");
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect.each(["missing", "wrong-unit", "wrong-pid", "dead"] as const)(
  "gate refuses original binding %s without releasing payload",
  (binding) =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ binding });
      yield* Effect.gen(function* () {
        const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
        const result = yield* Effect.result(pending.awaitHeld());
        assert.strictEqual(result._tag, "Failure");
        assert.strictEqual(harness.state.binds, 1);
        assert.strictEqual(harness.state.written, 0);
        assert.strictEqual((yield* Effect.result(pending.awaitHeld()))._tag, "Failure");
        assert.strictEqual(harness.state.binds, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.scoped),
);

it.effect("gate stderr has one consumer and does not expose raw child channels", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness();
    yield* Effect.gen(function* () {
      const pending = yield* (yield* preparePreparationProcessGate(input)).spawnHeld();
      assert.isFalse(Object.hasOwn(pending, "all"));
      assert.isFalse(Object.hasOwn(pending, "stdout"));
      assert.isFalse(Object.hasOwn(pending, "stdin"));
      assert.deepStrictEqual(yield* Stream.runCollect(pending.stderr), []);
      assert.strictEqual((yield* Effect.result(Stream.runCollect(pending.stderr)))._tag, "Failure");
      yield* pending.stopOwnedChild();
      assert.strictEqual(harness.state.kills, 1);
      assert.isFalse(yield* pending.isRunning);
      assert.strictEqual(harness.state.written, 0);
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);

it.effect("local gate creation completes while the Node spawn event is withheld", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness({ creationEvent: "withheld" });
    yield* Effect.gen(function* () {
      const prepared = yield* preparePreparationProcessGate(input);
      const completed = yield* Deferred.make<void>();
      const creating = yield* prepared.spawnHeld().pipe(
        Effect.tap(() => Deferred.succeed(completed, undefined)),
        Effect.forkScoped,
      );
      const milestone = yield* Effect.raceFirst(
        Deferred.await(completed).pipe(Effect.as("completed")),
        Deferred.await(harness.spawnWaitStarted).pipe(Effect.as("waiting")),
      );
      assert.strictEqual(milestone, "completed", "local creation must not await the spawn event");
      const pending = yield* Fiber.join(creating);
      assert.strictEqual(harness.state.spawns, 1);
      assert.strictEqual(harness.state.pulls, 0);
      assert.strictEqual(harness.state.written, 0);
      yield* pending.stopOwnedChild();
    }).pipe(Effect.provide(harness.layer));
  }).pipe(Effect.scoped),
);
