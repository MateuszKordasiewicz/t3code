import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";

export class PreparationProcessOwnerError extends Schema.TaggedError<PreparationProcessOwnerError>()(
  "PreparationProcessOwnerError",
  {
    stage: Schema.Literals([
      "input",
      "admission",
      "creation",
      "ready",
      "binding",
      "release",
      "output",
      "cleanup",
      "inventory",
    ]),
    reason: Schema.Literals(["invalid", "closed", "stale", "denied", "persistence", "unknown"]),
  },
) {}
const isOwnerError = Schema.is(PreparationProcessOwnerError);
export const error = (
  stage: PreparationProcessOwnerError["stage"],
  reason: PreparationProcessOwnerError["reason"],
) => new PreparationProcessOwnerError({ stage, reason });
export const sanitize = <A, E, R>(
  stage: PreparationProcessOwnerError["stage"],
  work: Effect.Effect<A, E, R>,
) =>
  work.pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.fromReasons(
          cause.reasons.map((reason) =>
            Cause.isInterruptReason(reason)
              ? reason
              : Cause.isDieReason(reason)
                ? Cause.makeDieReason(new Error("Preparation owner unavailable"))
                : Cause.makeFailReason(
                    isOwnerError(reason.error) ? reason.error : error(stage, "unknown"),
                  ),
          ),
        ),
      ),
    ),
  );

export interface CommandInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly extendEnv?: boolean | undefined;
}
export interface ControllerFlags {
  intakeSealed: boolean;
  stopping: boolean;
  blocked: boolean;
}
export interface Inventory {
  readonly kind: "sealed";
  readonly jobId: string;
  readonly inventory: "all-retired" | "held";
  readonly resources: ReadonlyArray<{
    readonly resourceId: string;
    readonly state: "settled" | "held";
  }>;
}
export interface CommandState {
  readonly callerScope: Scope.Scope;
  readonly commandScope: Scope.Closeable;
  readonly ownerScope: Scope.Scope;
  readonly storeScope: Scope.Scope;
  readonly startFiber: Fiber.Fiber<unknown, unknown>;
  readonly startDone: Deferred.Deferred<void>;
  readonly ready: Deferred.Deferred<
    Stream.Stream<Uint8Array, PreparationProcessOwnerError>,
    PreparationProcessOwnerError
  >;
  sealed: boolean;
}
export interface CommandPort {
  readonly checkOutsideWriter: (
    stage: PreparationProcessOwnerError["stage"],
  ) => Effect.Effect<void, PreparationProcessOwnerError>;
  readonly prepareCommand: (
    input: CommandInput,
    state: CommandState,
  ) => Effect.Effect<object, PreparationProcessOwnerError>;
  readonly createCommand: (ref: object) => Effect.Effect<
    {
      readonly resourceId: string;
      readonly stderr: Stream.Stream<Uint8Array, PreparationProcessOwnerError>;
    },
    PreparationProcessOwnerError
  >;
  readonly startCommand: (
    ref: object,
  ) => Effect.Effect<
    Stream.Stream<Uint8Array, PreparationProcessOwnerError>,
    PreparationProcessOwnerError
  >;
  readonly collectCommand: (ref: object) => Effect.Effect<number, PreparationProcessOwnerError>;
  readonly assessInventory: () => Effect.Effect<Inventory, PreparationProcessOwnerError>;
}
export interface Identity {
  readonly jobId: string;
  readonly threadId: string;
  readonly runId: string;
}
function properties(value: unknown) {
  if (
    typeof value !== "object" ||
    value === null ||
    (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
  )
    throw error("input", "invalid");
  return Object.fromEntries(
    Reflect.ownKeys(value).map((key) => {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (
        typeof key !== "string" ||
        !descriptor?.enumerable ||
        !("value" in descriptor) ||
        key === "__proto__"
      )
        throw error("input", "invalid");
      return [key, descriptor.value] as const;
    }),
  ) as Record<string, unknown>;
}
const text = (value: unknown, empty = false): value is string =>
  typeof value === "string" &&
  (empty || value.length > 0) &&
  value.isWellFormed() &&
  !value.includes("\0");
function ownInput(value: unknown): CommandInput {
  const raw = properties(value);
  if (
    Object.keys(raw).some((key) => !["command", "args", "cwd", "env", "extendEnv"].includes(key)) ||
    !text(raw.command) ||
    !Array.isArray(raw.args)
  )
    throw error("input", "invalid");
  WorkflowCanonicalHash.assertCanonical(raw.args);
  if (
    !raw.args.every((arg: unknown) => text(arg, true)) ||
    (raw.cwd !== undefined && !text(raw.cwd)) ||
    (raw.extendEnv !== undefined && typeof raw.extendEnv !== "boolean")
  )
    throw error("input", "invalid");
  let env: Readonly<Record<string, string>> | undefined;
  if (raw.env !== undefined) {
    const entries = Object.entries(properties(raw.env)).flatMap(([key, item]) => {
      if (!text(key) || key.includes("=") || (item !== undefined && !text(item, true)))
        throw error("input", "invalid");
      return item === undefined ? [] : [[key, item] as const];
    });
    env = Object.freeze(Object.fromEntries(entries));
  }
  return Object.freeze({
    command: raw.command,
    args: Object.freeze([...raw.args] as string[]),
    ...(raw.cwd === undefined ? {} : { cwd: raw.cwd }),
    ...(env === undefined ? {} : { env }),
    ...(raw.extendEnv === undefined ? {} : { extendEnv: raw.extendEnv }),
  });
}

export const make = Effect.fn("PreparationProcessOwner.make")(function* (
  identity: Identity,
  storeScope: Scope.Scope,
  port: CommandPort,
  flags: ControllerFlags,
) {
  if (storeScope.state._tag === "Closed") return yield* error("admission", "closed");
  const ownerScope = yield* Scope.fork(storeScope, "sequential");
  const cleanupScope = yield* Scope.make();
  type Invocation = {
    readonly fiber: Fiber.Fiber<unknown, unknown>;
    readonly callerScope: Scope.Scope;
    readonly done: Deferred.Deferred<void>;
  };
  type Slot = {
    readonly state: CommandState;
    readonly done: Deferred.Deferred<void, PreparationProcessOwnerError>;
    closing: boolean;
    startup?: Fiber.Fiber<void>;
  };
  const invocations = new Set<Invocation>();
  const commands = new Set<Slot>();
  const closed = (slot: Slot) =>
    flags.stopping ||
    flags.blocked ||
    slot.state.sealed ||
    [storeScope, ownerScope, slot.state.callerScope, slot.state.commandScope].some(
      (scope) => scope.state._tag === "Closed",
    );
  const closeCommand = Effect.fn("PreparationProcessOwner.closeCommand")((slot: Slot) =>
    port.checkOutsideWriter("cleanup").pipe(
      Effect.andThen(
        Effect.uninterruptible(
          Effect.suspend(() => {
            if (slot.closing) return Deferred.await(slot.done);
            slot.closing = true;
            slot.state.sealed = true;
            Deferred.doneUnsafe(slot.state.ready, Exit.fail(error("output", "closed")));
            const cleanup = Effect.gen(function* () {
              if (!(yield* Deferred.isDone(slot.state.startDone)))
                slot.state.startFiber.interruptUnsafe();
              yield* Deferred.await(slot.state.startDone);
              if (slot.startup) {
                yield* Fiber.interrupt(slot.startup);
                yield* Fiber.await(slot.startup);
              }
              yield* Scope.close(slot.state.commandScope, Exit.void);
            }).pipe(
              (work) => sanitize("cleanup", work),
              Effect.provideService(Scope.Scope, cleanupScope),
            );
            return cleanup.pipe(
              Effect.exit,
              Effect.tap((exit) =>
                Effect.sync(() => {
                  if (Exit.isFailure(exit)) flags.blocked = true;
                  Deferred.doneUnsafe(slot.done, exit);
                }),
              ),
              Effect.forkIn(cleanupScope, { startImmediately: true }),
              Effect.andThen(Deferred.await(slot.done)),
            );
          }),
        ),
      ),
    ),
  );
  let stopDone: Deferred.Deferred<Inventory, PreparationProcessOwnerError> | undefined;
  const stop = Effect.fn("PreparationProcessOwner.stop")(() =>
    port.checkOutsideWriter("cleanup").pipe(
      Effect.andThen(
        Effect.uninterruptible(
          Effect.withFiber((fiber) => {
            flags.intakeSealed = true;
            flags.stopping = true;
            if (
              [...invocations].some(
                (invocation) =>
                  invocation.fiber.id === fiber.id && !Deferred.isDoneUnsafe(invocation.done),
              )
            )
              return Effect.fail(error("cleanup", "invalid"));
            if (stopDone) return Deferred.await(stopDone);
            const completion = Deferred.makeUnsafe<Inventory, PreparationProcessOwnerError>();
            stopDone = completion;
            const cleanup = Effect.gen(function* () {
              for (const invocation of invocations)
                if (!Deferred.isDoneUnsafe(invocation.done)) invocation.fiber.interruptUnsafe();
              yield* Effect.forEach(invocations, (invocation) => Deferred.await(invocation.done), {
                concurrency: "unbounded",
                discard: true,
              });
              const exits = yield* Effect.forEach(
                commands,
                (slot) => Effect.exit(closeCommand(slot)),
                {
                  concurrency: "unbounded",
                },
              );
              const failures = exits.flatMap((exit) =>
                Exit.isFailure(exit) ? exit.cause.reasons : [],
              );
              const inventory = yield* sanitize("inventory", port.assessInventory());
              if (failures.length > 0) return yield* Effect.failCause(Cause.fromReasons(failures));
              return inventory;
            }).pipe(Effect.provideService(Scope.Scope, cleanupScope));
            return cleanup.pipe(
              Effect.exit,
              Effect.tap((exit) =>
                Effect.sync(() => {
                  Deferred.doneUnsafe(completion, exit);
                }),
              ),
              Effect.forkIn(cleanupScope, { startImmediately: true }),
              Effect.andThen(Deferred.await(completion)),
            );
          }),
        ),
      ),
    ),
  );
  yield* Scope.addFinalizer(
    storeScope,
    stop().pipe(Effect.ignore, Effect.ensuring(Scope.close(cleanupScope, Exit.void))),
  );
  const seal = Effect.fn("PreparationProcessOwner.seal")(function* () {
    flags.intakeSealed = true;
    yield* port.checkOutsideWriter("inventory");
    const current = yield* Effect.serviceOption(Scope.Scope);
    if (
      Option.isSome(current) &&
      ([...invocations].some(
        (invocation) =>
          invocation.callerScope === current.value && !Deferred.isDoneUnsafe(invocation.done),
      ) ||
        [...commands].some(
          (slot) => slot.state.callerScope === current.value && !Deferred.isDoneUnsafe(slot.done),
        ))
    )
      return yield* error("inventory", "invalid");
    yield* Effect.forEach(invocations, (invocation) => Deferred.await(invocation.done), {
      concurrency: "unbounded",
      discard: true,
    });
    yield* Effect.forEach(commands, (slot) => Deferred.await(slot.done), {
      concurrency: "unbounded",
      discard: true,
    });
    return yield* sanitize("inventory", port.assessInventory());
  }, Effect.interruptible);
  const startOwned = Effect.fn("PreparationProcessOwner.start")(function* (input: CommandInput) {
    const callerScope = yield* Effect.scope;
    yield* port.checkOutsideWriter("admission");
    return yield* Effect.uninterruptibleMask(() =>
      Effect.withFiber((fiber) => {
        if (
          flags.intakeSealed ||
          flags.stopping ||
          flags.blocked ||
          storeScope.state._tag === "Closed" ||
          ownerScope.state._tag === "Closed"
        )
          return Effect.fail(error("admission", "closed"));
        if (
          callerScope === storeScope ||
          callerScope === ownerScope ||
          callerScope.state._tag === "Closed"
        )
          return Effect.fail(error("admission", "invalid"));
        const invocation: Invocation = { fiber, callerScope, done: Deferred.makeUnsafe<void>() };
        invocations.add(invocation);
        let slot: Slot | undefined;
        const body = Effect.gen(function* () {
          const commandScope = yield* Scope.fork(ownerScope, "sequential");
          const ready = Deferred.makeUnsafe<
            Stream.Stream<Uint8Array, PreparationProcessOwnerError>,
            PreparationProcessOwnerError
          >();
          const command: Slot = {
            state: {
              callerScope,
              commandScope,
              ownerScope,
              storeScope,
              startFiber: fiber,
              startDone: invocation.done,
              ready,
              sealed: false,
            },
            done: Deferred.makeUnsafe<void, PreparationProcessOwnerError>(),
            closing: false,
          };
          slot = command;
          commands.add(command);
          yield* Scope.addFinalizer(
            callerScope,
            Effect.withFiber((closer) => {
              if (closer.id === fiber.id && !Deferred.isDoneUnsafe(invocation.done)) {
                command.state.sealed = true;
                return Effect.void;
              }
              return closeCommand(command).pipe(Effect.ignore);
            }),
          );
          if (closed(command)) return yield* error("admission", "closed");
          const ref = yield* port.prepareCommand(input, command.state);
          if (closed(command)) return yield* error("creation", "closed");
          const created = yield* port.createCommand(ref);
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              if (closed(command)) return yield* error("creation", "closed");
              const startup = Effect.gen(function* () {
                const exit = yield* Effect.exit(
                  sanitize("ready", port.startCommand(ref)).pipe(Effect.interruptible),
                );
                if (Exit.isFailure(exit)) {
                  flags.blocked = true;
                  flags.intakeSealed = true;
                }
                Deferred.doneUnsafe(ready, exit);
              });
              command.startup = yield* Effect.forkIn(startup, commandScope);
            }),
          );
          return Object.freeze({
            resourceId: created.resourceId,
            stdout: Stream.unwrap(Deferred.await(ready)),
            stderr: created.stderr,
            exitCode: Deferred.await(ready).pipe(
              Effect.andThen(sanitize("output", port.collectCommand(ref))),
            ),
          });
        }).pipe((work) => sanitize("creation", work), Effect.interruptible);
        return Effect.gen(function* () {
          const result = yield* Effect.exit(body);
          Deferred.doneUnsafe(invocation.done, Exit.void);
          if (Exit.isFailure(result)) {
            flags.blocked = true;
            flags.intakeSealed = true;
            if (slot) yield* closeCommand(slot);
          }
          return yield* result;
        });
      }),
    );
  }, Effect.interruptible);
  return Object.freeze({
    jobId: identity.jobId,
    threadId: identity.threadId,
    runId: identity.runId,
    start(input: CommandInput) {
      try {
        return startOwned(ownInput(input));
      } catch {
        return Effect.fail(error("input", "invalid"));
      }
    },
    seal,
    stop,
  });
});
export type Owner = Effect.Success<ReturnType<typeof make>>;
