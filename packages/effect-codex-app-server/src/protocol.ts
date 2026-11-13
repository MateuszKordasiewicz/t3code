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

export const makeCodexAppServerPatchedProtocol = Effect.fn("makeCodexAppServerPatchedProtocol")(
  function* (
    options: CodexAppServerPatchedProtocolOptions,
  ): Effect.fn.Return<CodexAppServerPatchedProtocol, never, Scope.Scope> {
    const protocolScope = yield* Scope.Scope;
    const requestHandlerScope = yield* Scope.fork(protocolScope, "parallel");
    const outgoing = yield* Queue.unbounded<string, Cause.Done<void>>();
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

    const handleTermination = (classify: () => Effect.Effect<CodexError.CodexAppServerError>) =>
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
      handler: (pendingRequest: CodexAppServerPendingRequest) => Effect.Effect<void>,
    ) =>
      Ref.modify(pending, (current) => {
        const pendingRequest = current.get(requestId);
        if (!pendingRequest) {
          return [Effect.void, current] as const;
        }
        const next = new Map(current);
        next.delete(requestId);
        return [handler(pendingRequest), next] as const;
      }).pipe(Effect.flatten);

    const respond = (requestId: string | number, result: unknown) =>
      offerOutgoing(toProtocolMessage(requestId, { result }));

    const respondError = (
      requestId: string | number,
      error: CodexError.CodexAppServerRequestError,
    ) => offerOutgoing(toProtocolMessage(requestId, { error: error.toProtocolError() }));

    const handleResponse = (response: typeof JsonRpcResponseEnvelope.Type) => {
      const requestId = String(response.id);
      const protocolError = response.error;
      if (protocolError !== undefined) {
        return resolvePending(requestId, ({ deferred, method }) =>
          Deferred.fail(
            deferred,
            CodexError.CodexAppServerRequestError.fromProtocolError(
              protocolError,
              method,
              requestId,
            ),
          ),
        );
      }
      return resolvePending(requestId, ({ deferred }) =>
        Deferred.succeed(deferred, response.result),
      );
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
                      CodexError.CodexAppServerRequestError.fromAppServerError(
                        error,
                        request.method,
                      ),
                    ),
                  onSuccess: (result) => respond(request.id, result),
                }),
                Effect.ensuring(
                  Ref.update(activeRequestHandlers, (count) => Math.max(0, count - 1)),
                ),
                Effect.catch((error) =>
                  handleTermination(() => Effect.succeed(error)).pipe(
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
          handleTermination(() =>
            Effect.succeed(normalizeIncomingError(error, "read-input-stream")),
          ),
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
      Effect.forkScoped,
    );

    yield* Stream.fromQueue(outgoing).pipe(
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

    const notify = (method: string, payload?: unknown) =>
      offerOutgoing({
        method,
        ...(payload !== undefined ? { params: payload } : {}),
      });

    return {
      incomingNotifications: Stream.fromQueue(incomingNotifications),
      incomingRequests: Stream.fromQueue(incomingRequests),
      request,
      notify,
      respond,
      respondError,
    } satisfies CodexAppServerPatchedProtocol;
  },
);

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
    if (
      typeof encoded !== "string" ||
      !encoded.isWellFormed() ||
      !encoded.endsWith("\n") ||
      encoded.indexOf("\n") !== encoded.length - 1 ||
      typeof wrapper !== "function" ||
      typeof onOutcome !== "function" ||
      (selection !== undefined && (typeof selection !== "string" || !selection.trim()))
    )
      throw new Error("invalid controlled frame");
    const byteLength = Buffer.byteLength(encoded, "utf8");
    if (byteLength > FRAME_BYTES_LIMIT)
      return Effect.fail(
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
