import * as Context from "effect/Context";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";

import * as CodexError from "./errors.ts";
import { JsonRpcId, JsonRpcResponseEnvelope } from "./_internal/shared.ts";
const isJsonRpcId = Schema.is(JsonRpcId);
const isJsonRpcResponseEnvelope = Schema.is(JsonRpcResponseEnvelope);
const isCodexAppServerError = Schema.is(CodexError.CodexAppServerError);
const isFrameSubmissionError = Schema.is(CodexError.CodexAppServerFrameSubmissionError);
const MAX_BUFFERED_RAW_MESSAGES = 32;
// Bound JSONL lines before joining and parsing; the LF delimiter is excluded.
const MAX_INCOMING_MESSAGE_BYTES = 128 * 1024 * 1024;
const EXIT_STATUS_GRACE = "250 millis";

export interface CodexAppServerProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly stage: "raw" | "decoded" | "decode_failed";
  readonly payload: unknown;
}

export interface CodexAppServerIncomingNotification {
  readonly method: string;
  readonly params?: unknown;
}

export interface CodexAppServerIncomingRequest {
  readonly id: string | number;
  readonly method: string;
  readonly params?: unknown;
}

export interface CodexAppServerPatchedProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly terminationError?: Effect.Effect<CodexError.CodexAppServerError>;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  // Test override; production uses MAX_INCOMING_MESSAGE_BYTES.
  readonly maxIncomingMessageBytes?: number;
  readonly logger?: (event: CodexAppServerProtocolLogEvent) => Effect.Effect<void, never>;
  readonly onNotification?: (
    notification: CodexAppServerIncomingNotification,
  ) => Effect.Effect<void, never>;
  readonly onRequest?: (
    request: CodexAppServerIncomingRequest,
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
  readonly onTermination?: (error: CodexError.CodexAppServerError) => Effect.Effect<void, never>;
}

export interface CodexAppServerPatchedProtocol {
  readonly incomingNotifications: Stream.Stream<CodexAppServerIncomingNotification>;
  readonly incomingRequests: Stream.Stream<CodexAppServerIncomingRequest>;
  readonly request: (
    method: string,
    payload?: unknown,
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
  readonly notify: (
    method: string,
    payload?: unknown,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respond: (
    requestId: string | number,
    result: unknown,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respondError: (
    requestId: string | number,
    error: CodexError.CodexAppServerRequestError,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
}

interface CodexAppServerPendingRequest {
  readonly local?: ControlledLocalFrame;
  readonly deferred: Deferred.Deferred<unknown, CodexError.CodexAppServerError>;
  readonly method: string;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isIncomingRequest(value: unknown): value is CodexAppServerIncomingRequest {
  if (!isObject(value) || typeof value.method !== "string") {
    return false;
  }
  return isJsonRpcId(value.id);
}

function isIncomingNotification(value: unknown): value is CodexAppServerIncomingNotification {
  return isObject(value) && typeof value.method === "string" && !("id" in value);
}

function isIncomingResponse(value: unknown): value is typeof JsonRpcResponseEnvelope.Type {
  return isJsonRpcResponseEnvelope(value);
}

const encodeJsonString = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJsonString = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const encodeWireMessage = (
  message: Record<string, unknown>,
): Effect.Effect<string, CodexError.CodexAppServerProtocolParseError> =>
  encodeJsonString(message).pipe(
    Effect.map((encoded) => `${encoded}\n`),
    Effect.mapError((cause) => {
      const method = typeof message.method === "string" ? message.method : undefined;
      const requestId =
        typeof message.id === "string" || typeof message.id === "number"
          ? String(message.id)
          : undefined;
      return CodexError.CodexAppServerProtocolParseError.fromSchemaError(
        "encode-wire-message",
        cause,
        {
          ...(method === undefined ? {} : { method }),
          ...(requestId === undefined ? {} : { requestId }),
        },
      );
    }),
  );

const decodeWireMessage = (
  line: string,
): Effect.Effect<unknown, CodexError.CodexAppServerProtocolParseError> =>
  decodeJsonString(line).pipe(
    Effect.mapError((cause) =>
      CodexError.CodexAppServerProtocolParseError.fromSchemaError("decode-wire-message", cause),
    ),
  );

const normalizeIncomingError = (
  error: unknown,
  operation: CodexError.CodexAppServerTransportOperation,
): CodexError.CodexAppServerError =>
  isCodexAppServerError(error)
    ? error
    : new CodexError.CodexAppServerTransportError({
        operation,
        cause: error,
      });

const toProtocolMessage = (
  requestId: string | number,
  fields: {
    readonly result?: unknown;
    readonly error?: CodexError.CodexAppServerProtocolErrorShape;
  },
): { readonly [key: string]: unknown } => ({
  id: requestId,
  ...(fields.result !== undefined ? { result: fields.result } : {}),
  ...(fields.error !== undefined ? { error: fields.error } : {}),
});

const makeProtocolEngine = Effect.fn("makeCodexAppServerPatchedProtocol")(function* <E>(
  options: CodexAppServerPatchedProtocolOptions,
  controlled?: CodexAppServerControlledProtocolOptions<E>,
): Effect.fn.Return<
  Omit<CodexAppServerControlledProtocol, "quiesceOutput"> &
    Partial<Pick<CodexAppServerControlledProtocol, "quiesceOutput">>,
  never,
  Scope.Scope
> {
  const protocolScope = yield* Scope.Scope;
  const requestHandlerScope = controlled
    ? yield* Scope.make("parallel")
    : yield* Scope.fork(protocolScope, "parallel");
  const controlledInputScope = controlled ? yield* Scope.make("sequential") : undefined;
  const outgoing = yield* Queue.unbounded<string | ControlledEnvelope, Cause.Done<void>>();
  const incomingNotifications =
    yield* Queue.sliding<CodexAppServerIncomingNotification>(MAX_BUFFERED_RAW_MESSAGES);
  const incomingRequests =
    yield* Queue.sliding<CodexAppServerIncomingRequest>(MAX_BUFFERED_RAW_MESSAGES);
  const pending = yield* Ref.make(new Map<string, CodexAppServerPendingRequest>());
  const nextRequestId = yield* Ref.make(1);
  const maxIncomingMessageBytes = options.maxIncomingMessageBytes ?? MAX_INCOMING_MESSAGE_BYTES;
  const remainder: Array<string> = [];
  let remainderBytes = 0;
  const terminationHandled = yield* Ref.make(false);
  const terminationFailure = yield* Ref.make(Option.none<CodexError.CodexAppServerError>());
  const terminationSignal = yield* Deferred.make<void>();
  const activeRequestHandlers = yield* Ref.make(0);
  const requestHandlers = yield* FiberMap.make<string | number>().pipe(
    Scope.provide(requestHandlerScope),
  );

  const logProtocol = (event: CodexAppServerProtocolLogEvent) => {
    if (event.direction === "incoming" && !options.logIncoming) {
      return Effect.void;
    }
    if (event.direction === "outgoing" && !options.logOutgoing) {
      return Effect.void;
    }
    return (
      options.logger?.(event) ??
      Effect.logDebug("Codex App Server protocol event").pipe(Effect.annotateLogs({ event }))
    );
  };

  const failAllPending = (error: CodexError.CodexAppServerError) =>
    Ref.get(pending).pipe(
      Effect.flatMap((current) =>
        Effect.forEach([...current.values()], ({ deferred }) => Deferred.fail(deferred, error), {
          discard: true,
        }),
      ),
      Effect.andThen(Ref.set(pending, new Map())),
    );

  let controlledRuntime: Effect.Success<ReturnType<typeof makeControlledRuntime<E>>> | undefined;
  const handleLegacyTermination = (classify: () => Effect.Effect<CodexError.CodexAppServerError>) =>
    Ref.modify(terminationHandled, (handled) => {
      if (handled) {
        return [Effect.void, true] as const;
      }
      return [
        Effect.gen(function* () {
          const error = yield* classify();
          yield* Ref.set(terminationFailure, Option.some(error));
          yield* failAllPending(error);
          yield* Queue.end(outgoing);
          yield* Deferred.succeed(terminationSignal, undefined);
          yield* Scope.close(requestHandlerScope, Exit.void).pipe(
            Effect.forkIn(protocolScope, { startImmediately: true }),
            Effect.asVoid,
          );
          if (options.onTermination) {
            yield* options.onTermination(error);
          }
        }),
        true,
      ] as const;
    }).pipe(Effect.flatten);

  const handleTermination = (classify: () => Effect.Effect<CodexError.CodexAppServerError>) =>
    controlledRuntime ? controlledRuntime.terminate(classify) : handleLegacyTermination(classify);

  const offerOutgoing =
    /* Every protocol write reaches this helper; request spans cover the operation. */
    Effect.fnUntraced(function* (message: Record<string, unknown>) {
      const failure = yield* Ref.get(terminationFailure);
      if (Option.isSome(failure)) return yield* failure.value;

      yield* logProtocol({
        direction: "outgoing",
        stage: "decoded",
        payload: message,
      });
      const encoded = yield* encodeWireMessage(message);
      yield* logProtocol({
        direction: "outgoing",
        stage: "raw",
        payload: encoded,
      });
      const accepted = yield* Queue.offer(outgoing, encoded);
      if (!accepted) {
        const closed = yield* Ref.get(terminationFailure);
        return yield* Option.getOrElse(
          closed,
          () => new CodexError.CodexAppServerInputStreamEndedError({}),
        );
      }
    });

  const removePending = (requestId: string) =>
    Ref.update(pending, (current) => {
      if (!current.has(requestId)) {
        return current;
      }
      const next = new Map(current);
      next.delete(requestId);
      return next;
    });

  const resolvePending = (
    requestId: string,
    result: (
      request: CodexAppServerPendingRequest,
    ) => Exit.Exit<unknown, CodexError.CodexAppServerError>,
  ) =>
    Ref.modify(pending, (current) => {
      const request = current.get(requestId);
      if (!request) return [Effect.void, current] as const;
      let response = result(request);
      if (request.local) {
        const local = request.local;
        if (local.cancelled || local.error) return [Effect.void, current] as const;
        local.response ??= response;
        if (local.phase !== "done") return [Effect.void, current] as const;
        response = local.response;
      }
      const next = new Map(current);
      next.delete(requestId);
      return [Deferred.done(request.deferred, response).pipe(Effect.asVoid), next] as const;
    }).pipe(Effect.flatten);

  const respond = (requestId: string | number, result: unknown, selection?: string) =>
    controlledRuntime
      ? controlledRuntime.sendSnapshot(toProtocolMessage(requestId, { result }), selection)
      : offerOutgoing(toProtocolMessage(requestId, { result }));

  const respondError = (
    requestId: string | number,
    error: CodexError.CodexAppServerRequestError,
    selection?: string,
  ) =>
    controlledRuntime
      ? controlledRuntime.sendSnapshot(
          toProtocolMessage(requestId, { error: error.toProtocolError() }),
          selection,
        )
      : offerOutgoing(toProtocolMessage(requestId, { error: error.toProtocolError() }));

  const handleResponse = (response: typeof JsonRpcResponseEnvelope.Type) => {
    const requestId = String(response.id);
    const protocolError = response.error;
    if (protocolError !== undefined) {
      return resolvePending(requestId, ({ method }) =>
        Exit.fail(
          CodexError.CodexAppServerRequestError.fromProtocolError(protocolError, method, requestId),
        ),
      );
    }
    return resolvePending(requestId, () => Exit.succeed(response.result));
  };

  const handleRequest = (request: CodexAppServerIncomingRequest) =>
    Queue.offer(incomingRequests, request).pipe(
      Effect.flatMap(() => {
        const handler = options.onRequest;
        if (!handler) return Effect.void;

        return Ref.modify(activeRequestHandlers, (count) =>
          count >= MAX_BUFFERED_RAW_MESSAGES ? [false, count] : [true, count + 1],
        ).pipe(
          Effect.flatMap((accepted) => {
            if (!accepted) {
              return respondError(
                request.id,
                CodexError.CodexAppServerRequestError.overloaded(
                  "Too many Codex requests are already active.",
                ),
              );
            }

            return handler(request).pipe(
              Effect.matchEffect({
                onFailure: (error) =>
                  respondError(
                    request.id,
                    CodexError.CodexAppServerRequestError.fromAppServerError(error, request.method),
                  ),
                onSuccess: (result) => respond(request.id, result),
              }),
              Effect.ensuring(Ref.update(activeRequestHandlers, (count) => Math.max(0, count - 1))),
              Effect.catch((error) =>
                controlled && isFrameSubmissionError(error)
                  ? Effect.void
                  : handleTermination(() => Effect.succeed(error)).pipe(
                      Effect.forkIn(protocolScope),
                      Effect.asVoid,
                    ),
              ),
              FiberMap.run(requestHandlers, request.id),
              Effect.asVoid,
            );
          }),
        );
      }),
      Effect.asVoid,
    );

  // Codex settles requests itself when their turn ends; a handler still waiting would hold its slot.
  const releaseResolvedRequest = (notification: CodexAppServerIncomingNotification) =>
    notification.method === "serverRequest/resolved" &&
    isObject(notification.params) &&
    isJsonRpcId(notification.params.requestId)
      ? FiberMap.remove(requestHandlers, notification.params.requestId)
      : Effect.void;

  const handleNotification = (notification: CodexAppServerIncomingNotification) =>
    releaseResolvedRequest(notification).pipe(
      Effect.andThen(Queue.offer(incomingNotifications, notification)),
      Effect.andThen(options.onNotification ? options.onNotification(notification) : Effect.void),
      Effect.asVoid,
    );

  const routeMessage = Effect.fnUntraced(function* (message: unknown) {
    if (Option.isSome(yield* Ref.get(terminationFailure))) return;
    if (isIncomingRequest(message)) return yield* handleRequest(message);
    if (isIncomingNotification(message)) return yield* handleNotification(message);
    if (isIncomingResponse(message)) return yield* handleResponse(message);
    return yield* CodexError.CodexAppServerProtocolParseError.fromUnroutableMessage(message);
  });

  const handleLine = (line: string): Effect.Effect<void, CodexError.CodexAppServerError> => {
    if (line.trim().length === 0) {
      return Effect.void;
    }
    return logProtocol({
      direction: "incoming",
      stage: "raw",
      payload: line,
    }).pipe(
      Effect.flatMap(() => decodeWireMessage(line)),
      Effect.tap((decoded) =>
        logProtocol({
          direction: "incoming",
          stage: "decoded",
          payload: decoded,
        }),
      ),
      Effect.tapErrorTag("CodexAppServerProtocolParseError", (error) =>
        logProtocol({
          direction: "incoming",
          stage: "decode_failed",
          payload: {
            operation: error.operation,
            ...(error.method === undefined ? {} : { method: error.method }),
            ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
            ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
            ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
            ...(error.maximumPathDepth === undefined
              ? {}
              : { maximumPathDepth: error.maximumPathDepth }),
          },
        }),
      ),
      Effect.flatMap(routeMessage),
    );
  };

  if (controlled && controlledInputScope)
    controlledRuntime = yield* makeControlledRuntime({
      options: controlled,
      parent: protocolScope,
      inputScope: controlledInputScope,
      handlerScope: requestHandlerScope,
      outgoing,
      pending,
      nextRequestId,
      terminationFailure,
      terminationSignal,
      failAllPending,
    });

  yield* options.stdio.stdin.pipe(
    Stream.interruptWhen(Deferred.await(terminationSignal)),
    Stream.decodeText(),
    Stream.runForEach(
      Effect.fnUntraced(function* (chunk) {
        let start = 0;
        while (start < chunk.length) {
          const newline = chunk.indexOf("\n", start);
          const fragment = chunk.slice(start, newline === -1 ? chunk.length : newline);
          // decodeText holds back split UTF-8 sequences, so fragment byte sums are exact.
          remainderBytes += Buffer.byteLength(fragment, "utf8");
          if (remainderBytes > maxIncomingMessageBytes) {
            remainder.length = 0;
            return yield* new CodexError.CodexAppServerTransportError({
              operation: "read-input-stream",
              cause: new Error(`Incoming message exceeded ${maxIncomingMessageBytes} bytes.`),
            });
          }
          remainder.push(fragment);
          if (newline === -1) break;
          const line = remainder.join("").replace(/\r$/, "");
          remainder.length = 0;
          remainderBytes = 0;
          yield* handleLine(line);
          start = newline + 1;
        }
      }),
    ),
    Effect.matchEffect({
      onFailure: (error) =>
        handleTermination(() => Effect.succeed(normalizeIncomingError(error, "read-input-stream"))),
      onSuccess: () =>
        Effect.sync(() => {
          const line = remainder.join("");
          remainder.length = 0;
          return line;
        }).pipe(
          Effect.flatMap(handleLine),
          Effect.matchEffect({
            onFailure: (error) => handleTermination(() => Effect.succeed(error)),
            onSuccess: () =>
              handleTermination(() => {
                const ended = new CodexError.CodexAppServerInputStreamEndedError({});
                // Node ends stdout before it reports the exit of a crashed process, so
                // wait briefly for the exit status. stdout can also close while the
                // process keeps running, so the wait must not hang pending requests.
                return options.terminationError === undefined
                  ? Effect.succeed(ended)
                  : options.terminationError.pipe(
                      Effect.timeoutOrElse({
                        duration: EXIT_STATUS_GRACE,
                        orElse: () => Effect.succeed(ended),
                      }),
                    );
              }),
          }),
        ),
    }),
    controlledInputScope ? Effect.forkIn(controlledInputScope) : Effect.forkScoped,
  );

  if (!controlled)
    yield* Stream.fromQueue(outgoing).pipe(
      Stream.map((entry) => {
        if (typeof entry !== "string") throw new Error("Unexpected controlled frame in raw writer");
        return entry;
      }),
      Stream.run(options.stdio.stdout()),
      Effect.catch((cause) =>
        handleTermination(() =>
          Effect.succeed(
            new CodexError.CodexAppServerTransportError({
              operation: "write-output-stream",
              cause,
            }),
          ),
        ),
      ),
      Effect.forkScoped,
    );

  const request = Effect.fn("CodexAppServerProtocol.request")(function* (
    method: string,
    payload?: unknown,
  ) {
    const requestId = yield* Ref.modify(
      nextRequestId,
      (current) => [current, current + 1] as const,
    );
    const attributes = {
      "rpc.system": "jsonrpc",
      "rpc.method": method,
      "rpc.request_id": requestId,
    };
    yield* Effect.annotateCurrentSpan(attributes);
    const deferred = yield* Deferred.make<unknown, CodexError.CodexAppServerError>();
    return yield* Effect.acquireUseRelease(
      Ref.update(pending, (current) =>
        new Map(current).set(String(requestId), { deferred, method }),
      ),
      () =>
        Effect.gen(function* () {
          yield* offerOutgoing({
            id: requestId,
            method,
            ...(payload !== undefined ? { params: payload } : {}),
          }).pipe(Effect.withSpan("CodexAppServerProtocol.request.enqueue", { attributes }));
          // Includes queue/stdin delivery, Codex work and response routing, not just T3 CPU.
          return yield* Deferred.await(deferred).pipe(
            Effect.withSpan("CodexAppServerProtocol.request.awaitResponse", { attributes }),
          );
        }),
      () => removePending(String(requestId)),
    );
  });

  const notify = (method: string, payload?: unknown, selection?: string) =>
    controlledRuntime
      ? controlledRuntime.sendSnapshot(
          { method, ...(payload !== undefined ? { params: payload } : {}) },
          selection,
        )
      : offerOutgoing({
          method,
          ...(payload !== undefined ? { params: payload } : {}),
        });

  return {
    incomingNotifications: Stream.fromQueue(incomingNotifications),
    incomingRequests: Stream.fromQueue(incomingRequests),
    request: controlledRuntime ? controlledRuntime.requestSnapshot : request,
    ...(controlledRuntime ? { quiesceOutput: controlledRuntime.quiesceOutput } : {}),
    notify,
    respond,
    respondError,
  };
});

const FRAME_BYTES_LIMIT = 1024 * 1024;
const FRAME_SINK_TIMEOUT_MS = 2_000;
const frameReceiptBrand = Symbol("controlled-frame-receipt");
interface FrameReceipt {
  readonly [frameReceiptBrand]: true;
}
export type CodexAppServerFrameSubmit = () => Effect.Effect<
  FrameReceipt,
  CodexError.CodexAppServerFrameSubmissionError
>;
export interface CodexAppServerControlledFrame {
  readonly encoded: string;
  readonly byteLength: number;
  readonly method: string | null;
  readonly requestId: string | number | null;
  readonly selection: string | null;
}
export interface CodexAppServerFrameOutcome {
  readonly frame: CodexAppServerControlledFrame;
  readonly outcome: "not_submitted" | "unknown" | "submitted";
  readonly reason: "completed" | CodexError.CodexAppServerFrameSubmissionError["reason"];
}
interface ControlledFrameOptions<E> {
  readonly stdio: Stdio.Stdio;
  readonly encoded: string;
  readonly selection?: string;
  readonly wrapper: (
    frame: CodexAppServerControlledFrame,
    submit: CodexAppServerFrameSubmit,
  ) => Effect.Effect<FrameReceipt, E>;
  readonly onOutcome: (outcome: CodexAppServerFrameOutcome) => Effect.Effect<void>;
  // Tests may shorten the local Sink interval, but cannot increase the production maximum.
  readonly testSinkTimeoutMs?: number;
}
const runControlledFrame = Effect.fn("CodexAppServerProtocol.submitControlledFrame")(function* <
  E,
>(owned: {
  readonly frame: CodexAppServerControlledFrame;
  readonly sink: ReturnType<Stdio.Stdio["stdout"]>;
  readonly wrapper: ControlledFrameOptions<E>["wrapper"];
  readonly onOutcome: ControlledFrameOptions<E>["onOutcome"];
  readonly timeoutMs: number;
}) {
  return yield* Effect.withFiber((writer) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let open = true;
        let used = false;
        let attempted = false;
        let completed = false;
        let misused = false;
        let writeFailure: CodexError.CodexAppServerFrameSubmissionError | undefined;
        const receipt: FrameReceipt = Object.freeze({ [frameReceiptBrand]: true as const });
        const submit: CodexAppServerFrameSubmit = () =>
          Effect.withFiber((current) =>
            Effect.suspend(() => {
              if (!open || used || misused || current.id !== writer.id) {
                misused = true;
                return Effect.fail(
                  new CodexError.CodexAppServerFrameSubmissionError({
                    outcome: attempted ? "unknown" : "not_submitted",
                    reason: "continuation",
                  }),
                );
              }
              used = true;
              attempted = true;
              return Stream.make(owned.frame.encoded).pipe(
                Stream.run(owned.sink),
                Effect.interruptible,
                Effect.mapError(
                  (cause) =>
                    new CodexError.CodexAppServerFrameSubmissionError({
                      outcome: "unknown",
                      reason: "sink",
                      cause,
                    }),
                ),
                Effect.timeoutOrElse({
                  duration: owned.timeoutMs,
                  orElse: () =>
                    Effect.fail(
                      new CodexError.CodexAppServerFrameSubmissionError({
                        outcome: "unknown",
                        reason: "timeout",
                      }),
                    ),
                }),
                Effect.tapError((error) =>
                  Effect.sync(() => {
                    writeFailure = error;
                  }),
                ),
                Effect.andThen(
                  Effect.sync(() => {
                    completed = true;
                    return receipt;
                  }),
                ),
              );
            }),
          );
        const exit = yield* Effect.exit(
          restore(Effect.suspend(() => owned.wrapper(owned.frame, submit))).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                open = false;
              }),
            ),
          ),
        );
        const succeeded = Exit.isSuccess(exit) && exit.value === receipt && completed && !misused;
        const outcome: CodexAppServerFrameOutcome = Object.freeze({
          frame: owned.frame,
          outcome: succeeded ? "submitted" : attempted ? "unknown" : "not_submitted",
          reason: succeeded
            ? "completed"
            : Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)
              ? "interrupted"
              : misused
                ? "continuation"
                : (writeFailure?.reason ?? "wrapper"),
        });
        const publication = yield* Effect.exit(Effect.suspend(() => owned.onOutcome(outcome)));
        if (Exit.isFailure(publication)) {
          const error = new CodexError.CodexAppServerFrameSubmissionError({
            outcome: outcome.outcome,
            reason: "outcome_hook",
            cause: publication.cause,
          });
          return yield* Effect.failCause(
            Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)
              ? Cause.combine(
                  Cause.map(exit.cause, () => error),
                  Cause.fail(error),
                )
              : Cause.fail(error),
          );
        }
        if (succeeded) return outcome;
        const error = new CodexError.CodexAppServerFrameSubmissionError({
          outcome: outcome.outcome,
          reason: outcome.reason === "completed" ? "wrapper" : outcome.reason,
          ...(Exit.isFailure(exit) ? { cause: exit.cause } : {}),
        });
        if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause))
          return yield* Effect.failCause(Cause.map(exit.cause, () => error));
        return yield* error;
      }),
    ),
  );
});

const prepareControlledFrame = (encoded: string, selection?: string) => {
  try {
    if (
      typeof encoded !== "string" ||
      !encoded.isWellFormed() ||
      !encoded.endsWith("\n") ||
      encoded.indexOf("\n") !== encoded.length - 1 ||
      (selection !== undefined && (typeof selection !== "string" || !selection.trim()))
    )
      throw new Error("invalid controlled frame");
    const byteLength = Buffer.byteLength(encoded, "utf8");
    if (byteLength > FRAME_BYTES_LIMIT)
      return Result.fail(
        new CodexError.CodexAppServerFrameSubmissionError({
          outcome: "not_submitted",
          reason: "frame_limit",
        }),
      );
    const decoded: unknown = JSON.parse(encoded);
    // Canonical bytes exclude duplicate selectors and UTF-8 or numeric identity changes.
    if (JSON.stringify(decoded) + "\n" !== encoded)
      throw new Error("noncanonical controlled frame");
    if (
      !isObject(decoded) ||
      Array.isArray(decoded) ||
      ("method" in decoded && (typeof decoded.method !== "string" || !decoded.method.trim())) ||
      ("id" in decoded &&
        (!isJsonRpcId(decoded.id) ||
          (typeof decoded.id === "number" && !Number.isSafeInteger(decoded.id)))) ||
      (!("method" in decoded) && !("id" in decoded))
    )
      throw new Error("invalid controlled frame");
    const frame: CodexAppServerControlledFrame = Object.freeze({
      encoded,
      byteLength,
      method: typeof decoded.method === "string" ? decoded.method : null,
      requestId: isJsonRpcId(decoded.id) ? decoded.id : null,
      selection: selection ?? null,
    });
    return Result.succeed(frame);
  } catch (cause) {
    return Result.fail(
      new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "invalid_frame",
        cause,
      }),
    );
  }
};

// The supplied Sink must already be non-ending; a Stdio option cannot prove its construction.
export const submitControlledCodexAppServerFrame = <E>(options: ControlledFrameOptions<E>) => {
  try {
    const { encoded, selection, wrapper, onOutcome, stdio } = options;
    const timeoutMs = options.testSinkTimeoutMs ?? FRAME_SINK_TIMEOUT_MS;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > FRAME_SINK_TIMEOUT_MS)
      return Effect.fail(
        new CodexError.CodexAppServerFrameSubmissionError({
          outcome: "not_submitted",
          reason: "invalid_timeout",
        }),
      );
    if (typeof wrapper !== "function" || typeof onOutcome !== "function")
      throw new Error("invalid callbacks");
    const prepared = prepareControlledFrame(encoded, selection);
    if (Result.isFailure(prepared)) return Effect.fail(prepared.failure);
    const frame = prepared.success;
    return runControlledFrame({
      frame,
      sink: stdio.stdout({ endOnDone: false }),
      wrapper,
      onOutcome,
      timeoutMs,
    });
  } catch (cause) {
    return Effect.fail(
      new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "invalid_frame",
        cause,
      }),
    );
  }
};

export interface CodexAppServerControlledProtocol extends CodexAppServerPatchedProtocol {
  readonly quiesceOutput: Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly request: (
    method: string,
    payload?: unknown,
    selection?: string,
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
  readonly notify: (
    method: string,
    payload?: unknown,
    selection?: string,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respond: (
    requestId: string | number,
    result: unknown,
    selection?: string,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respondError: (
    requestId: string | number,
    error: CodexError.CodexAppServerRequestError,
    selection?: string,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
}
export interface CodexAppServerControlledProtocolOptions<
  E,
> extends CodexAppServerPatchedProtocolOptions {
  readonly wrapper: ControlledFrameOptions<E>["wrapper"];
  readonly onOutcome: ControlledFrameOptions<E>["onOutcome"];
  readonly testSinkTimeoutMs?: number;
}
interface ControlledLocalFrame {
  readonly frame: CodexAppServerControlledFrame;
  readonly done: Deferred.Deferred<void>;
  phase: "queued" | "active" | "settling" | "done";
  cancelled: boolean;
  dequeued: boolean;
  released: boolean;
  outcome?: CodexAppServerFrameOutcome;
  error?: CodexError.CodexAppServerFrameSubmissionError;
  response?: Exit.Exit<unknown, CodexError.CodexAppServerError>;
  fiber?: Fiber.Fiber<
    Result.Result<CodexAppServerFrameOutcome, CodexError.CodexAppServerFrameSubmissionError>
  >;
}
interface ControlledEnvelope {
  readonly local: ControlledLocalFrame;
  readonly method: string;
  readonly deferred?: Deferred.Deferred<unknown, CodexError.CodexAppServerError>;
}
const CurrentControlledCallbacks = Context.Reference<ReadonlySet<symbol>>(
  "effect-codex-app-server/CurrentControlledCallbacks",
  { defaultValue: () => new Set() },
);
const encodeOwnedJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const snapshotControlledMessage = (message: Record<string, unknown>) =>
  Result.try({
    try: () => {
      const value: unknown = JSON.parse(encodeOwnedJsonString(message));
      if (!isObject(value) || Array.isArray(value)) throw new Error("Invalid message snapshot");
      return value;
    },
    catch: (cause) =>
      new CodexError.CodexAppServerProtocolParseError({ operation: "encode-wire-message", cause }),
  });

const controlledOutgoingLogEvents = (
  frame: CodexAppServerControlledFrame,
): ReadonlyArray<CodexAppServerProtocolLogEvent> => [
  { direction: "outgoing", stage: "decoded", payload: JSON.parse(frame.encoded) },
  { direction: "outgoing", stage: "raw", payload: frame.encoded },
];

const combineControlledCleanup = (exits: ReadonlyArray<Exit.Exit<unknown>>) => {
  let cause = Cause.empty;
  for (const exit of exits) if (Exit.isFailure(exit)) cause = Cause.combine(cause, exit.cause);
  return cause.reasons.length ? Exit.failCause(cause) : Exit.void;
};

const makeControlledRuntime = Effect.fn("CodexAppServerProtocol.makeControlledRuntime")(function* <
  E,
>(input: {
  readonly options: CodexAppServerControlledProtocolOptions<E>;
  readonly parent: Scope.Scope;
  readonly inputScope: Scope.Closeable;
  readonly handlerScope: Scope.Closeable;
  readonly outgoing: Queue.Queue<string | ControlledEnvelope, Cause.Done<void>>;
  readonly pending: Ref.Ref<Map<string, CodexAppServerPendingRequest>>;
  readonly nextRequestId: Ref.Ref<number>;
  readonly terminationFailure: Ref.Ref<Option.Option<CodexError.CodexAppServerError>>;
  readonly terminationSignal: Deferred.Deferred<void>;
  readonly failAllPending: (error: CodexError.CodexAppServerError) => Effect.Effect<void>;
}) {
  const { options, outgoing, pending } = input;
  const marker = Symbol("controlled-protocol");
  const writerScope = yield* Scope.make("sequential");
  const coordinatorScope = yield* Scope.make("sequential");
  const shutdownSignal =
    yield* Deferred.make<() => Effect.Effect<CodexError.CodexAppServerError>>();
  const shutdownDone = yield* Deferred.make<Exit.Exit<void>>();
  const lifecycleStart = yield* Deferred.make<void>();
  const quiesceDone = yield* Deferred.make<Exit.Exit<void, CodexError.CodexAppServerError>>();
  let active: ControlledEnvelope | undefined;
  let closing = false;
  let outputBlock: CodexError.CodexAppServerFrameSubmissionError | undefined;
  let publicationFailure: CodexError.CodexAppServerFrameSubmissionError | undefined;
  let frameCount = 0;
  let frameBytes = 0;

  const blockedError = () =>
    new CodexError.CodexAppServerFrameSubmissionError({
      outcome: "not_submitted",
      reason: "output_blocked",
      ...(outputBlock ? { cause: outputBlock } : {}),
    });
  const blockOutput = Effect.fn("CodexAppServerProtocol.blockOutput")(function* (
    error: CodexError.CodexAppServerFrameSubmissionError,
  ) {
    outputBlock ??= error;
    yield* Queue.end(outgoing);
  });
  const releaseCapacity = (entry: ControlledEnvelope) =>
    Effect.sync(() => {
      const local = entry.local;
      if (local.released || !local.dequeued || local.phase !== "done") return;
      local.released = true;
      frameCount--;
      frameBytes -= local.frame.byteLength;
    });
  const callHook = Effect.fn("CodexAppServerProtocol.publishLocalOutcome")(function* (
    outcome: CodexAppServerFrameOutcome,
  ) {
    const previous = yield* CurrentControlledCallbacks;
    return yield* Effect.suspend(() => options.onOutcome(outcome)).pipe(
      Effect.provideService(CurrentControlledCallbacks, new Set([...previous, marker])),
    );
  });
  const publishResponse = Effect.fn("CodexAppServerProtocol.publishControlledResponse")(function* (
    entry: ControlledEnvelope,
  ) {
    if (!entry.deferred || entry.local.cancelled || entry.local.error || !entry.local.response)
      return;
    const response = entry.local.response;
    const requestId = String(entry.local.frame.requestId);
    yield* Ref.modify(pending, (current) => {
      if (current.get(requestId) !== entry) return [Effect.void, current] as const;
      const next = new Map(current);
      next.delete(requestId);
      return [Deferred.done(entry.deferred!, response).pipe(Effect.asVoid), next] as const;
    }).pipe(Effect.flatten);
  });
  const finish = Effect.fn("CodexAppServerProtocol.finishControlledFrame")(function* (
    entry: ControlledEnvelope,
  ) {
    const local = entry.local;
    if (local.cancelled && !local.error && local.outcome)
      local.error = new CodexError.CodexAppServerFrameSubmissionError({
        outcome: local.outcome.outcome,
        reason: "interrupted",
      });
    local.phase = "done";
    yield* releaseCapacity(entry);
    yield* Deferred.succeed(entry.local.done, undefined);
    yield* publishResponse(entry);
  });
  const settleKnown = Effect.fn("CodexAppServerProtocol.settleUnsubmittedFrame")(function* (
    entry: ControlledEnvelope,
    error: CodexError.CodexAppServerFrameSubmissionError,
  ) {
    const local = entry.local;
    if (local.phase === "done") {
      yield* releaseCapacity(entry);
      return;
    }
    if (local.phase === "settling") {
      yield* Deferred.await(local.done);
      yield* releaseCapacity(entry);
      return;
    }
    local.phase = "settling";
    const outcome: CodexAppServerFrameOutcome = Object.freeze({
      frame: local.frame,
      outcome: "not_submitted",
      reason: error.reason,
    });
    local.outcome = outcome;
    local.error = error;
    const publication = yield* Effect.exit(callHook(outcome));
    if (Exit.isFailure(publication)) {
      local.error = new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "outcome_hook",
        cause: publication.cause,
      });
      publicationFailure ??= local.error;
      yield* blockOutput(local.error);
    }
    yield* finish(entry);
  }, Effect.uninterruptible);
  const processFrame = Effect.fn("CodexAppServerProtocol.processControlledFrame")(
    function* (entry: ControlledEnvelope) {
      const local = entry.local;
      local.dequeued = true;
      if (local.phase === "done" || local.phase === "settling") {
        yield* Deferred.await(local.done);
        yield* releaseCapacity(entry);
        return;
      }
      if (local.cancelled || closing || outputBlock) {
        yield* settleKnown(
          entry,
          local.cancelled
            ? new CodexError.CodexAppServerFrameSubmissionError({
                outcome: "not_submitted",
                reason: "interrupted",
              })
            : blockedError(),
        );
        return;
      }
      active = entry;
      local.phase = "active";
      const start = yield* Deferred.make<void>();
      const previous = yield* CurrentControlledCallbacks;
      const work = Deferred.await(start).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            submitControlledCodexAppServerFrame({
              stdio: options.stdio,
              encoded: local.frame.encoded,
              ...(local.frame.selection !== null ? { selection: local.frame.selection } : {}),
              ...(options.testSinkTimeoutMs !== undefined
                ? { testSinkTimeoutMs: options.testSinkTimeoutMs }
                : {}),
              wrapper: (frame, submit) =>
                options.wrapper(frame, () =>
                  Effect.suspend(() => {
                    if (local.cancelled || closing) return Effect.fail(blockedError());
                    return submit();
                  }),
                ),
              onOutcome: Effect.fn("CodexAppServerProtocol.recordLocalOutcome")(function* (
                outcome: CodexAppServerFrameOutcome,
              ) {
                local.outcome = outcome;
                yield* callHook(outcome);
              }),
            }),
          ),
        ),
        Effect.result,
        Effect.provideService(CurrentControlledCallbacks, new Set([...previous, marker])),
        Effect.interruptible,
      );
      const child = yield* work.pipe(Effect.forkIn(writerScope));
      local.fiber = child;
      if (local.cancelled || closing) yield* Fiber.interrupt(child);
      else yield* Deferred.succeed(start, undefined);
      const exit = yield* Fiber.await(child);
      delete local.fiber;
      if (!local.outcome) {
        const error =
          Exit.isSuccess(exit) && Result.isFailure(exit.value)
            ? exit.value.failure
            : new CodexError.CodexAppServerFrameSubmissionError({
                outcome:
                  Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
                    ? "not_submitted"
                    : "unknown",
                reason:
                  Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)
                    ? "interrupted"
                    : "wrapper",
                ...(Exit.isFailure(exit) ? { cause: exit.cause } : {}),
              });
        if (error.outcome === "not_submitted") {
          yield* settleKnown(entry, error);
          return;
        }
        local.outcome = Object.freeze({
          frame: local.frame,
          outcome: error.outcome,
          reason: error.reason,
        });
        const publication = yield* Effect.exit(callHook(local.outcome));
        local.error = Exit.isFailure(publication)
          ? new CodexError.CodexAppServerFrameSubmissionError({
              outcome: error.outcome,
              reason: "outcome_hook",
              cause: publication.cause,
            })
          : error;
      } else if (Exit.isSuccess(exit) && Result.isFailure(exit.value))
        local.error = exit.value.failure;
      else if (Exit.isFailure(exit))
        local.error = new CodexError.CodexAppServerFrameSubmissionError({
          outcome: local.outcome.outcome,
          reason: "interrupted",
          cause: exit.cause,
        });
      if (
        local.error &&
        (local.error.outcome === "unknown" || local.error.reason === "outcome_hook")
      ) {
        if (local.error.reason === "outcome_hook") publicationFailure ??= local.error;
        yield* blockOutput(local.error);
      }
      yield* finish(entry);
    },
    Effect.uninterruptible,
    Effect.ensuring(
      Effect.sync(() => {
        active = undefined;
      }),
    ),
  );

  const stopOutput = Effect.fn("CodexAppServerProtocol.stopControlledOutput")(function* () {
    outputBlock ??= blockedError();
    if (active?.local.phase === "active") active.local.cancelled = true;
    yield* Queue.end(outgoing);
    yield* Deferred.succeed(lifecycleStart, undefined);
  }, Effect.uninterruptible);
  const quiesceOutput = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if ((yield* CurrentControlledCallbacks).has(marker))
        return yield* new CodexError.CodexAppServerFrameSubmissionError({
          outcome: "not_submitted",
          reason: "reentrant",
        });
      yield* stopOutput();
      return yield* restore(Deferred.await(quiesceDone).pipe(Effect.flatten));
    }),
  );

  const terminate = Effect.fn("CodexAppServerProtocol.signalControlledTermination")(function* (
    classify: () => Effect.Effect<CodexError.CodexAppServerError>,
  ) {
    if (closing) return;
    closing = true;
    yield* stopOutput();
    yield* Deferred.succeed(input.terminationSignal, undefined);
    yield* Deferred.succeed(shutdownSignal, classify);
  }, Effect.uninterruptible);

  // This actor owns output settlement and shutdown so neither pump can join itself.
  yield* Effect.gen(function* () {
    yield* Deferred.await(lifecycleStart);
    const outputCleanups: Array<Exit.Exit<unknown>> = [];
    if (active?.local.fiber)
      outputCleanups.push(yield* Effect.exit(Fiber.interrupt(active.local.fiber)));
    const writerExit = yield* Fiber.await(writer);
    outputCleanups.push(writerExit);
    for (;;) {
      const queued = yield* Queue.poll(outgoing);
      if (Option.isNone(queued)) break;
      if (typeof queued.value === "string") continue;
      const entry = queued.value;
      entry.local.dequeued = true;
      outputCleanups.push(yield* Effect.exit(settleKnown(entry, blockedError())));
    }
    if (publicationFailure) outputCleanups.push(Exit.die(publicationFailure));
    const outputExit = combineControlledCleanup(outputCleanups);
    yield* Deferred.succeed(
      quiesceDone,
      publicationFailure
        ? Exit.fail(publicationFailure)
        : Exit.isFailure(outputExit)
          ? Exit.fail(
              new CodexError.CodexAppServerTransportError({
                operation: "write-output-stream",
                cause: outputExit.cause,
              }),
            )
          : Exit.void,
    );
    const classify = yield* Deferred.await(shutdownSignal);
    const classified = yield* Effect.exit(Effect.suspend(classify));
    const error = Exit.isSuccess(classified)
      ? classified.value
      : new CodexError.CodexAppServerTransportError({
          operation: "read-process-exit-status",
          cause: classified.cause,
        });
    const cleanups: Array<Exit.Exit<unknown>> = [outputExit];
    cleanups.push(yield* Effect.exit(Ref.set(input.terminationFailure, Option.some(error))));
    cleanups.push(yield* Effect.exit(Scope.close(writerScope, Exit.void)));
    cleanups.push(yield* Effect.exit(input.failAllPending(error)));
    cleanups.push(yield* Effect.exit(Scope.close(input.inputScope, Exit.void)));
    cleanups.push(yield* Effect.exit(Scope.close(input.handlerScope, Exit.void)));
    const onTermination = options.onTermination;
    if (onTermination)
      cleanups.push(yield* Effect.exit(Effect.suspend(() => onTermination(error))));
    yield* Deferred.succeed(shutdownDone, combineControlledCleanup(cleanups));
  }).pipe(Effect.uninterruptible, Effect.forkIn(coordinatorScope, { startImmediately: true }));
  const writer = yield* Stream.fromQueue(outgoing).pipe(
    Stream.runForEach((entry) =>
      typeof entry === "string"
        ? Effect.die("Unexpected raw frame in controlled writer")
        : processFrame(entry),
    ),
    Effect.onExit((exit) =>
      Exit.isFailure(exit) && !closing
        ? terminate(() =>
            Effect.succeed(
              new CodexError.CodexAppServerTransportError({
                operation: "write-output-stream",
                cause: exit.cause,
              }),
            ),
          )
        : Effect.void,
    ),
    Effect.interruptible,
    Effect.forkIn(writerScope),
  );

  yield* Scope.addFinalizer(
    input.parent,
    Effect.gen(function* () {
      yield* terminate(() =>
        Effect.succeed(new CodexError.CodexAppServerInputStreamEndedError({})),
      );
      const exit = yield* Deferred.await(shutdownDone);
      const coordinatorClose = yield* Effect.exit(Scope.close(coordinatorScope, Exit.void));
      return yield* combineControlledCleanup([exit, coordinatorClose]);
    }).pipe(Effect.uninterruptible),
  );

  const logFrame = Effect.fn("CodexAppServerProtocol.logControlledOutgoing")(function* (
    frame: CodexAppServerControlledFrame,
  ) {
    if (!options.logOutgoing) return;
    const previous = yield* CurrentControlledCallbacks;
    yield* Effect.forEach(
      controlledOutgoingLogEvents(frame),
      (event) =>
        Effect.suspend(
          () =>
            options.logger?.(event) ??
            Effect.logDebug("Codex App Server protocol event").pipe(Effect.annotateLogs({ event })),
        ),
      { discard: true },
    ).pipe(Effect.provideService(CurrentControlledCallbacks, new Set([...previous, marker])));
  });
  const admit = Effect.fn("CodexAppServerProtocol.admitControlledFrame")(function* (
    entry: ControlledEnvelope,
    request?: CodexAppServerPendingRequest,
  ) {
    if ((yield* CurrentControlledCallbacks).has(marker))
      return yield* new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "reentrant",
      });
    yield* logFrame(entry.local.frame);
    const result = yield* Ref.modify(
      pending,
      (
        current,
      ): readonly [
        Result.Result<void, CodexError.CodexAppServerFrameSubmissionError>,
        Map<string, CodexAppServerPendingRequest>,
      ] => {
        if (closing || outputBlock) return [Result.fail(blockedError()), current] as const;
        if (frameCount >= 64 || frameBytes + entry.local.frame.byteLength > 16 * 1024 * 1024)
          return [
            Result.fail(
              new CodexError.CodexAppServerFrameSubmissionError({
                outcome: "not_submitted",
                reason: "queue_limit",
              }),
            ),
            current,
          ] as const;
        if (!Queue.offerUnsafe(outgoing, entry))
          return [Result.fail(blockedError()), current] as const;
        frameCount++;
        frameBytes += entry.local.frame.byteLength;
        return [
          Result.succeed(undefined),
          request ? new Map(current).set(String(entry.local.frame.requestId), request) : current,
        ] as const;
      },
    );
    yield* Effect.fromResult(result);
    return entry;
  });
  const cancel = Effect.fn("CodexAppServerProtocol.cancelControlledFrame")(function* (
    entry: ControlledEnvelope,
  ) {
    const local = entry.local;
    local.cancelled = true;
    // Queued ownership stays with the writer; waiting here could block the owner finalizer.
    if (local.phase !== "active") return;
    if (local.fiber) yield* Fiber.interrupt(local.fiber);
    yield* Deferred.await(local.done);
    if (local.error && local.error.outcome !== "not_submitted") return yield* local.error;
  });
  const awaitLocal = Effect.fn("CodexAppServerProtocol.awaitLocalSubmission")(function* (
    entry: ControlledEnvelope,
  ) {
    yield* Deferred.await(entry.local.done);
    if (entry.local.error) return yield* entry.local.error;
  });
  const makeLocal = Effect.fn("CodexAppServerProtocol.makeLocalFrame")(function* (
    frame: CodexAppServerControlledFrame,
  ) {
    const local: ControlledLocalFrame = {
      frame,
      done: yield* Deferred.make<void>(),
      phase: "queued",
      cancelled: false,
      dequeued: false,
      released: false,
    };
    return local;
  });
  const sendOwned = Effect.fn("CodexAppServerProtocol.sendControlledMessage")(function* (
    message: Record<string, unknown>,
    selection?: string,
  ) {
    const frame = yield* Effect.fromResult(
      prepareControlledFrame(encodeOwnedJsonString(message) + "\n", selection),
    );
    const entry: ControlledEnvelope = {
      local: yield* makeLocal(frame),
      method: frame.method ?? "",
    };
    yield* Effect.acquireUseRelease(
      admit(entry),
      () => awaitLocal(entry),
      (_entry, exit) =>
        Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause) ? cancel(entry) : Effect.void,
    );
  });
  const requestOwned = Effect.fn("CodexAppServerProtocol.request")(function* (
    message: Record<string, unknown>,
    selection?: string,
  ) {
    const requestId = yield* Ref.modify(
      input.nextRequestId,
      (current) => [current, current + 1] as const,
    );
    const frame = yield* Effect.fromResult(
      prepareControlledFrame(
        encodeOwnedJsonString({ id: requestId, ...message }) + "\n",
        selection,
      ),
    );
    yield* Effect.annotateCurrentSpan({
      "rpc.system": "jsonrpc",
      "rpc.method": frame.method,
      "rpc.request_id": requestId,
    });
    const entry: ControlledEnvelope & CodexAppServerPendingRequest = {
      local: yield* makeLocal(frame),
      method: frame.method ?? "",
      deferred: yield* Deferred.make<unknown, CodexError.CodexAppServerError>(),
    };
    return yield* Effect.acquireUseRelease(
      admit(entry, entry),
      () => awaitLocal(entry).pipe(Effect.andThen(Deferred.await(entry.deferred))),
      (_entry, exit) =>
        (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)
          ? cancel(entry)
          : Effect.void
        ).pipe(
          Effect.ensuring(
            Ref.update(pending, (current) => {
              if (current.get(String(requestId)) !== entry) return current;
              const next = new Map(current);
              next.delete(String(requestId));
              return next;
            }),
          ),
        ),
    );
  });
  const sendSnapshot = (message: Record<string, unknown>, selection?: string) => {
    const owned = snapshotControlledMessage(message);
    return Result.isFailure(owned)
      ? Effect.fail(owned.failure)
      : sendOwned(owned.success, selection);
  };
  const requestSnapshot = (method: string, payload?: unknown, selection?: string) => {
    const owned = snapshotControlledMessage({
      method,
      ...(payload !== undefined ? { params: payload } : {}),
    });
    return Result.isFailure(owned)
      ? Effect.fail(owned.failure)
      : requestOwned(owned.success, selection);
  };
  return { terminate, sendSnapshot, requestSnapshot, quiesceOutput };
}, Effect.uninterruptible);

export const makeCodexAppServerPatchedProtocol = (
  options: CodexAppServerPatchedProtocolOptions,
): Effect.Effect<CodexAppServerPatchedProtocol, never, Scope.Scope> => makeProtocolEngine(options);
export const makeControlledCodexAppServerProtocol = <E>(
  options: CodexAppServerControlledProtocolOptions<E>,
) => {
  const owned = { ...options };
  const timeout = owned.testSinkTimeoutMs ?? FRAME_SINK_TIMEOUT_MS;
  if (!Number.isFinite(timeout) || timeout <= 0 || timeout > FRAME_SINK_TIMEOUT_MS)
    return Effect.fail(
      new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "invalid_timeout",
      }),
    );
  if (typeof owned.wrapper !== "function" || typeof owned.onOutcome !== "function")
    return Effect.fail(
      new CodexError.CodexAppServerFrameSubmissionError({
        outcome: "not_submitted",
        reason: "invalid_frame",
      }),
    );
  return makeProtocolEngine(owned, owned).pipe(
    Effect.flatMap((protocol) =>
      protocol.quiesceOutput
        ? Effect.succeed({ ...protocol, quiesceOutput: protocol.quiesceOutput })
        : Effect.die("Missing controlled output lifecycle"),
    ),
  );
};
