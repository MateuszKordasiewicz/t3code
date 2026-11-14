// @effect-diagnostics-next-line nodeBuiltinImport:off -- Launch nonces use Node's cryptographic UUID source.
import * as NodeCrypto from "node:crypto";

import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Pull from "effect/Pull";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as NodeChild from "./PreparationNodeChild.ts";

import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

export class PreparationProcessGateError extends Schema.TaggedError<PreparationProcessGateError>()(
  "PreparationProcessGateError",
  {
    stage: Schema.Literals([
      "input",
      "prepare",
      "spawn",
      "ready",
      "submit",
      "exec",
      "output",
      "cleanup",
    ]),
    reason: Schema.Literals(["invalid", "unavailable", "used", "protocol", "timeout", "unknown"]),
  },
) {}

type Stage = PreparationProcessGateError["stage"];
const failure = (stage: Stage, reason: PreparationProcessGateError["reason"]) =>
  new PreparationProcessGateError({ stage, reason });
const isGateError = Schema.is(PreparationProcessGateError);
const sanitize = <A, E, R>(stage: Stage, work: Effect.Effect<A, E, R>) =>
  work.pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.fromReasons(
          cause.reasons.map((reason) =>
            Cause.isInterruptReason(reason)
              ? reason
              : Cause.isDieReason(reason)
                ? Cause.makeDieReason(new Error("Preparation gate unavailable"))
                : Cause.makeFailReason(
                    isGateError(reason.error) ? reason.error : failure(stage, "unknown"),
                  ),
          ),
        ),
      ),
    ),
  );

export interface PreparationProcessGateInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly extendEnv?: boolean | undefined;
  readonly threadId?: string | undefined;
}

function properties(value: unknown, environment = false) {
  if (
    typeof value !== "object" ||
    value === null ||
    ((!environment || value !== process.env) &&
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null)
  )
    throw failure("input", "invalid");
  const result: Record<string, unknown> = {};
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (
      typeof key !== "string" ||
      key === "__proto__" ||
      !descriptor?.enumerable ||
      !("value" in descriptor)
    )
      throw failure("input", "invalid");
    result[key] = descriptor.value;
  }
  return result;
}
const validString = (value: unknown, empty = false): value is string =>
  typeof value === "string" &&
  (empty || value.length > 0) &&
  value.isWellFormed() &&
  !value.includes("\0");
function ownEnvironment(value: unknown) {
  const result: Record<string, string> = {};
  for (const [key, item] of Object.entries(properties(value, true))) {
    if (!validString(key) || key.includes("=") || (item !== undefined && !validString(item, true)))
      throw failure("input", "invalid");
    if (item !== undefined) result[key] = item;
  }
  return Object.freeze(result);
}
function ownInput(value: unknown) {
  const raw = properties(value);
  if (
    Object.keys(raw).some(
      (key) => !["command", "args", "cwd", "env", "extendEnv", "threadId"].includes(key),
    ) ||
    !validString(raw.command) ||
    !Array.isArray(raw.args)
  )
    throw failure("input", "invalid");
  WorkflowCanonicalHash.assertCanonical(raw.args);
  if (
    !raw.args.every((arg: unknown) => validString(arg, true)) ||
    (raw.cwd !== undefined && !validString(raw.cwd)) ||
    (raw.threadId !== undefined && !validString(raw.threadId)) ||
    (raw.extendEnv !== undefined && typeof raw.extendEnv !== "boolean")
  )
    throw failure("input", "invalid");
  return Object.freeze({
    command: raw.command,
    args: Object.freeze([...raw.args] as string[]),
    ...(raw.cwd === undefined ? {} : { cwd: raw.cwd }),
    ...(raw.threadId === undefined ? {} : { threadId: raw.threadId }),
    ...(raw.extendEnv === undefined ? {} : { extendEnv: raw.extendEnv }),
    ...(raw.env === undefined ? {} : { env: ownEnvironment(raw.env) }),
  });
}

const gateScript = [
  'printf \'READY:%s:%s\\n\' "$1" "$$"',
  "IFS= read -r release || exit 121",
  '[ "$release" = "RELEASE:$1" ] || exit 122',
  "printf 'EXEC:%s\\n' \"$1\"",
  "shift",
  'exec "$@" </dev/null',
].join("\n");
const receiptBrand: unique symbol = Symbol("PreparationGateLocalReceipt");
export interface PreparationGateLocalReceipt {
  readonly [receiptBrand]: true;
}

const prepareOwned = Effect.fn("PreparationProcessGate.prepare")(
  function* (input: ReturnType<typeof ownInput>) {
    const path = yield* Path.Path;
    const hostCwd = yield* HostProcess.WorkingDirectory;
    const hostEnvironment = yield* HostProcess.Environment;
    const scope = yield* AgentScope;
    const creator = yield* NodeChild.PreparationNodeChild;
    const owned = yield* Effect.try({
      try: () => {
        // oxlint-disable-next-line t3code/no-global-process-runtime -- The image describes the actual Node spawner, not an overridable platform reference.
        const actualPlatform = process.platform;
        if (
          actualPlatform !== "linux" ||
          process.release.name !== "node" ||
          process.permission !== undefined ||
          !path.isAbsolute(hostCwd) ||
          !path.isAbsolute(input.command)
        )
          throw failure("prepare", "unavailable");
        const host = ownEnvironment(hostEnvironment);
        const env = {
          ...(input.env === undefined
            ? host
            : input.extendEnv === true
              ? { ...host, ...input.env }
              : input.env),
        };
        if (env.NODE_V8_COVERAGE === undefined)
          // @effect-diagnostics-next-line processEnvInEffect:off -- Node injects its ambient coverage setting even for an explicit environment.
          env.NODE_V8_COVERAGE = process.env.NODE_V8_COVERAGE ?? "";
        return { cwd: path.resolve(hostCwd, input.cwd ?? "."), env: Object.freeze(env) };
      },
      catch: () => failure("prepare", "unavailable"),
    });
    const nonce = yield* Effect.sync(NodeCrypto.randomUUID);
    const launch = yield* scope.wrap({
      command: "/bin/sh",
      args: ["-c", gateScript, "t3-preparation-gate", nonce, input.command, ...input.args],
      name: "preparation",
      env: owned.env,
      ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
    });
    const original = launch.custody;
    const unit = launch.unit;
    const executable = launch.command;
    if (
      !unit ||
      !original ||
      typeof original.bind !== "function" ||
      typeof original.stopAndObserve !== "function" ||
      typeof original.observeRetirement !== "function" ||
      !path.isAbsolute(executable)
    )
      return yield* failure("prepare", "unavailable");
    const custody = Object.freeze({
      bind: original.bind,
      stopAndObserve: original.stopAndObserve,
      observeRetirement: original.observeRetirement,
    });
    const finalArgs = Object.freeze([...launch.args]);
    yield* Effect.try({
      try: () => {
        WorkflowCanonicalHash.assertCanonical(finalArgs);
        if (!validString(executable) || !finalArgs.every((arg) => validString(arg, true)))
          throw failure("prepare", "invalid");
      },
      catch: () => failure("prepare", "invalid"),
    });
    const image = Object.freeze({
      executable,
      args: finalArgs,
      cwd: owned.cwd,
      environment: owned.env,
    });
    const preparedChild = yield* creator.prepare(image);
    const creationInputHash = WorkflowCanonicalHash.tagged("t3.preparation-creation-input.v2", {
      v: 2,
      creator: "synchronous-node-v1",
      platform: "linux",
      runtime: { kind: "node", version: process.versions.node },
      executable,
      proposedUnit: unit,
      args: finalArgs,
      cwd: owned.cwd,
      environment: Object.entries(owned.env).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      spawn: { shell: false, detached: true, windowsHide: false, stdio: ["pipe", "pipe", "pipe"] },
      input: { sinkEndOnDone: false, encoding: "utf-8", payloadStdin: "/dev/null", nonce },
      cleanup: {
        originalHandleOnly: true,
        termWaitMs: 1000,
        killWaitMs: 1000,
        outsideWriter: true,
      },
    });
    let spawned = false;
    return Object.freeze({
      creationInputHash,
      proposedUnit: unit,
      spawnHeld: Effect.fn("PreparationProcessGate.spawnHeld")(function* () {
        if (spawned) return yield* failure("spawn", "used");
        spawned = true;
        const child = yield* sanitize("spawn", preparedChild.createLocal());
        const initializeOutput = Stream.toPull(child.stdout);
        let pull: Effect.Success<typeof initializeOutput> | undefined;
        let pending: ReadonlyArray<Uint8Array> = [];
        let head = 0;
        const readLine = Effect.fn("PreparationProcessGate.readHeader")(function* (
          stage: "ready" | "exec",
        ) {
          const activePull = pull;
          if (!activePull) return yield* failure(stage, "protocol");
          const bytes: number[] = [];
          for (;;) {
            if (head >= pending.length) {
              pending = yield* activePull.pipe(
                Pull.catchDone(() => Effect.fail(failure(stage, "protocol"))),
                Effect.mapError(() => failure(stage, "protocol")),
              );
              head = 0;
            }
            const chunk = pending[head]!;
            for (let index = 0; index < chunk.length; index++) {
              const byte = chunk[index]!;
              if (byte > 127) return yield* failure(stage, "protocol");
              if (byte === 10) {
                const rest = chunk.subarray(index + 1);
                pending = rest.length
                  ? [rest, ...pending.slice(head + 1)]
                  : pending.slice(head + 1);
                head = 0;
                return String.fromCharCode(...bytes);
              }
              if (bytes.length >= 255) return yield* failure(stage, "protocol");
              bytes.push(byte);
            }
            head++;
          }
        });
        let stderrConsumed = false;
        const stderr = Stream.unwrap(
          Effect.suspend(() => {
            if (stderrConsumed) return Effect.fail(failure("output", "used"));
            stderrConsumed = true;
            return Effect.succeed(
              child.stderr.pipe(Stream.mapError(() => failure("output", "unknown"))),
            );
          }),
        );
        let waited = false;
        const awaitHeld = Effect.fn("PreparationProcessGate.awaitHeld")(function* () {
          if (waited) return yield* failure("ready", "used");
          waited = true;
          const binding = yield* sanitize(
            "ready",
            Effect.gen(function* () {
              yield* child.awaitSpawn;
              pull = yield* initializeOutput;
              const line = yield* readLine("ready");
              if (
                line !== "READY:" + nonce + ":" + String(child.pid) ||
                pending.slice(head).some((chunk) => chunk.length > 0) ||
                !(yield* child.isRunning)
              )
                return yield* failure("ready", "protocol");
              const pinned = yield* custody.bind(child.pid);
              if (
                !pinned ||
                pinned.pid !== child.pid ||
                pinned.unit !== unit ||
                !(yield* child.isRunning)
              )
                return yield* failure("ready", "unavailable");
              return pinned;
            }).pipe(
              Effect.timeoutOption("15 seconds"),
              Effect.interruptible,
              Effect.flatMap((result) =>
                Effect.fromOption(result).pipe(Effect.mapError(() => failure("ready", "timeout"))),
              ),
            ),
          );
          let submitted = false;
          let receipt: PreparationGateLocalReceipt | undefined;
          let execConsumed = false;
          let outputConsumed = false;
          return Object.freeze({
            binding,
            submitLocal: Effect.fn("PreparationProcessGate.submitLocal")(function* () {
              if (submitted) return yield* failure("submit", "used");
              submitted = true;
              const release = new TextEncoder().encode("RELEASE:" + nonce + "\n");
              if (release.length > 256) return yield* failure("submit", "invalid");
              yield* sanitize(
                "submit",
                Stream.run(Stream.make(release), child.stdin).pipe(
                  Effect.timeoutOption("2 seconds"),
                  Effect.interruptible,
                  Effect.flatMap((result) =>
                    Option.isSome(result) ? Effect.void : Effect.fail(failure("submit", "unknown")),
                  ),
                ),
              );
              receipt = Object.freeze({ [receiptBrand]: true as const });
              return receipt;
            }),
            awaitExec: Effect.fn("PreparationProcessGate.awaitExec")(function* (
              selected: PreparationGateLocalReceipt,
            ) {
              if (!receipt || selected !== receipt) return yield* failure("exec", "invalid");
              if (execConsumed) return yield* failure("exec", "used");
              execConsumed = true;
              const line = yield* sanitize(
                "exec",
                readLine("exec").pipe(
                  Effect.timeoutOption("5 seconds"),
                  Effect.interruptible,
                  Effect.flatMap((result) =>
                    Effect.fromOption(result).pipe(
                      Effect.mapError(() => failure("exec", "timeout")),
                    ),
                  ),
                ),
              );
              if (line !== "EXEC:" + nonce) return yield* failure("exec", "protocol");
              return Stream.fromPull(
                Effect.suspend(() => {
                  if (outputConsumed) return Effect.fail(failure("output", "used"));
                  outputConsumed = true;
                  const outputPull = Effect.suspend(() => {
                    if (head < pending.length) {
                      const chunks = pending.slice(head) as [Uint8Array, ...Uint8Array[]];
                      pending = [];
                      head = 0;
                      return Effect.succeed(chunks);
                    }
                    const activePull = pull;
                    if (!activePull) return Effect.fail(failure("output", "protocol"));
                    return activePull.pipe(
                      Effect.mapError((error) =>
                        Cause.isDone(error) ? error : failure("output", "unknown"),
                      ),
                    );
                  });
                  return Effect.succeed(outputPull);
                }),
              );
            }),
          });
        });
        return Object.freeze({
          pid: child.pid,
          isRunning: sanitize("output", child.isRunning),
          awaitExit: sanitize("output", child.exitCode),
          awaitTerminal: sanitize("output", child.awaitTerminal),
          stderr,
          awaitHeld,
          stopOwnedChild: () => sanitize("cleanup", child.kill()),
          stopAndObserve: () => custody.stopAndObserve(),
          observeRetirement: () => custody.observeRetirement(),
        });
      }),
    });
  },
  (work) => sanitize("prepare", work),
);

export function preparePreparationProcessGate(input: PreparationProcessGateInput) {
  try {
    return prepareOwned(ownInput(input));
  } catch {
    return Effect.fail(failure("input", "invalid"));
  }
}
