// @effect-diagnostics nodeBuiltinImport:off -- This adapter owns the actual synchronous Node creation boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeSink from "@effect/platform-node/NodeSink";
import * as NodeStream from "@effect/platform-node/NodeStream";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export class PreparationNodeChildError extends Schema.TaggedError<PreparationNodeChildError>()(
  "PreparationNodeChildError",
  { reason: Schema.Literals(["invalid", "closed", "used", "unknown", "timeout"]) },
) {}

export interface PreparationNodeImage {
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment: Readonly<Record<string, string>>;
}
const isChildError = Schema.is(PreparationNodeChildError);
const failure = (reason: PreparationNodeChildError["reason"]) =>
  new PreparationNodeChildError({ reason });
function data(value: unknown) {
  if (
    typeof value !== "object" ||
    value === null ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  )
    throw failure("invalid");
  const entries = Reflect.ownKeys(value).map((key) => {
    const property = Object.getOwnPropertyDescriptor(value, key)!;
    if (typeof key !== "string" || !property.enumerable || !("value" in property))
      throw failure("invalid");
    return [key, property.value] as const;
  });
  return Object.fromEntries(entries) as Record<string, unknown>;
}
function text(value: unknown): string {
  if (typeof value !== "string" || !value.isWellFormed() || value.includes("\0"))
    throw failure("invalid");
  return value;
}
function ownImage(input: PreparationNodeImage) {
  const raw = data(input);
  if (Object.keys(raw).sort().join() !== "args,cwd,environment,executable")
    throw failure("invalid");
  const executable = text(raw.executable);
  const cwd = text(raw.cwd);
  if (!executable.startsWith("/") || !cwd.startsWith("/") || !Array.isArray(raw.args))
    throw failure("invalid");
  const args: string[] = [];
  if (Reflect.ownKeys(raw.args).length !== raw.args.length + 1) throw failure("invalid");
  for (let index = 0; index < raw.args.length; index++) {
    const property = Object.getOwnPropertyDescriptor(raw.args, index);
    if (!property || !("value" in property) || !property.enumerable) throw failure("invalid");
    args.push(text(property.value));
  }
  const environment = Object.fromEntries(
    Object.entries(data(raw.environment)).map(([key, value]) => {
      if (!text(key) || key.includes("=")) throw failure("invalid");
      return [key, text(value)];
    }),
  );
  Object.setPrototypeOf(environment, null);
  return Object.freeze({
    executable,
    args: Object.freeze(args),
    cwd,
    environment: Object.freeze(environment),
  });
}

const prepareOwned = Effect.fn("PreparationNodeChild.prepare")(function* (
  image: ReturnType<typeof ownImage>,
) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- The creator is the actual Linux Node adapter.
  const platform = process.platform;
  if (platform !== "linux" || process.release.name !== "node" || process.permission !== undefined)
    return yield* failure("invalid");
  const ownerScope = yield* Effect.scope;
  const spawned = yield* Deferred.make<void, PreparationNodeChildError>();
  const errors = yield* Deferred.make<never, PreparationNodeChildError>();
  const terminal = yield* Deferred.make<
    Readonly<{ code: number | null; signal: NodeJS.Signals | null }>,
    PreparationNodeChildError
  >();
  let child: NodeChildProcess.ChildProcessWithoutNullStreams | undefined;
  let used = false;
  let closed = false;
  let observedTerminal = false;
  let observedSpawn = false;
  const bestEffortKill = (signal: NodeJS.Signals) => {
    try {
      return child?.kill(signal) ?? false;
    } catch {
      return false;
    }
  };
  const stop = Effect.fn("PreparationNodeChild.stop")(
    function* () {
      if (!child || observedTerminal) return;
      if (!Number.isSafeInteger(child.pid) || child.pid! <= 0) return yield* failure("unknown");
      bestEffortKill("SIGTERM");
      const first = yield* Deferred.await(terminal).pipe(
        Effect.timeoutOption("1 second"),
        Effect.interruptible,
      );
      if (Option.isSome(first)) return;
      bestEffortKill("SIGKILL");
      const second = yield* Deferred.await(terminal).pipe(
        Effect.timeoutOption("1 second"),
        Effect.interruptible,
      );
      if (Option.isNone(second)) return yield* failure("timeout");
    },
    Effect.ensuring(
      Effect.sync(() => {
        if (child && !observedTerminal) bestEffortKill("SIGKILL");
      }),
    ),
  );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true;
    }).pipe(Effect.andThen(stop()), Effect.ignore),
  );
  return Object.freeze({
    createLocal: Effect.fn("PreparationNodeChild.createLocal")(() =>
      Effect.try({
        try: () => {
          if (used) throw failure("used");
          used = true;
          if (closed || ownerScope.state._tag === "Closed") throw failure("closed");
          // No awaited acquisition or cleanup belongs between this call and retaining its returned handle.
          child = NodeChildProcess.spawn(image.executable, [...image.args], {
            cwd: image.cwd,
            env: image.environment,
            shell: false,
            detached: true,
            windowsHide: false,
            stdio: ["pipe", "pipe", "pipe"],
          });
          const retained = child;
          retained.on("error", () => {
            const error = failure("unknown");
            Deferred.doneUnsafe(errors, Exit.fail(error));
            Deferred.doneUnsafe(spawned, Exit.fail(error));
          });
          retained.on("exit", (code, signal) => {
            if (observedTerminal) return;
            observedTerminal = true;
            if (
              (code === null) === (signal === null) ||
              (code !== null && (!Number.isSafeInteger(code) || code < 0))
            ) {
              Deferred.doneUnsafe(terminal, Exit.fail(failure("unknown")));
              return;
            }
            Deferred.doneUnsafe(terminal, Exit.succeed(Object.freeze({ code, signal })));
          });
          retained.on("spawn", () => {
            observedSpawn = true;
            Deferred.doneUnsafe(spawned, Exit.void);
          });
          retained.stdin.on("error", () => undefined);
          retained.stdout.on("error", () => undefined);
          retained.stderr.on("error", () => undefined);
          if (!Number.isSafeInteger(retained.pid) || retained.pid! <= 0) throw failure("unknown");
          const awaitTerminal = Deferred.await(terminal);
          return Object.freeze({
            pid: retained.pid!,
            awaitSpawn: Deferred.await(spawned),
            awaitTerminal,
            exitCode: Effect.raceFirst(awaitTerminal, Deferred.await(errors)).pipe(
              Effect.flatMap((event) =>
                event.code === null ? Effect.fail(failure("unknown")) : Effect.succeed(event.code),
              ),
            ),
            isRunning: Effect.sync(
              () =>
                observedSpawn &&
                !observedTerminal &&
                retained.exitCode === null &&
                retained.signalCode === null,
            ),
            kill: stop,
            stdin: NodeSink.fromWritable<PreparationNodeChildError, Uint8Array>({
              evaluate: () => retained.stdin,
              onError: () => failure("unknown"),
              endOnDone: false,
              encoding: "utf-8",
            }),
            stdout: NodeStream.fromReadable({
              evaluate: () => retained.stdout,
              onError: () => failure("unknown"),
            }),
            stderr: NodeStream.fromReadable({
              evaluate: () => retained.stderr,
              onError: () => failure("unknown"),
            }),
          });
        },
        catch: (error) => (isChildError(error) ? error : failure("unknown")),
      }),
    ),
  });
});

export function prepare(image: PreparationNodeImage) {
  try {
    return prepareOwned(ownImage(image));
  } catch {
    return Effect.fail(failure("invalid"));
  }
}

export const PreparationNodeChild = Context.Reference("t3/PreparationNodeChild", {
  defaultValue: () => ({ prepare }),
});
