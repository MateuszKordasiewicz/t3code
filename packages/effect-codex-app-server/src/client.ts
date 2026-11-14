import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as ChildProcess from "effect/process/ChildProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stdio from "effect/Stdio";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as CodexRpc from "./_generated/meta.gen.ts";
import * as CodexSchema from "./_generated/schema.gen.ts";
import * as CodexError from "./errors.ts";
import * as CodexProtocol from "./protocol.ts";
import {
  decodeNotificationPayload,
  decodeOptionalPayload,
  encodeOptionalPayload,
  runHandler,
} from "./_internal/shared.ts";
import { makeChildStdio, makeStderrTail, makeTerminationError } from "./_internal/stdio.ts";

export interface CodexAppServerClientOptions {
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  readonly logger?: (
    event: CodexProtocol.CodexAppServerProtocolLogEvent,
  ) => Effect.Effect<void, never>;
}

interface CodexAppServerClientRaw {
  readonly notifications: CodexProtocol.CodexAppServerPatchedProtocol["incomingNotifications"];
  readonly requests: CodexProtocol.CodexAppServerPatchedProtocol["incomingRequests"];
  readonly request: CodexProtocol.CodexAppServerPatchedProtocol["request"];
  readonly notify: CodexProtocol.CodexAppServerPatchedProtocol["notify"];
  readonly respond: CodexProtocol.CodexAppServerPatchedProtocol["respond"];
  readonly respondError: CodexProtocol.CodexAppServerPatchedProtocol["respondError"];
}

export class CodexAppServerClient extends Context.Service<
  CodexAppServerClient,
  {
    readonly raw: CodexAppServerClientRaw;
    readonly awaitTermination: Effect.Effect<never, CodexError.CodexAppServerError>;
    readonly request: <M extends CodexRpc.ClientRequestMethod>(
      method: M,
      payload: CodexRpc.ClientRequestParamsByMethod[M],
    ) => Effect.Effect<CodexRpc.ClientRequestResponsesByMethod[M], CodexError.CodexAppServerError>;
    readonly notify: <M extends CodexRpc.ClientNotificationMethod>(
      method: M,
      payload: CodexRpc.ClientNotificationParamsByMethod[M],
    ) => Effect.Effect<void, CodexError.CodexAppServerError>;
    readonly handleServerRequest: <M extends CodexRpc.ServerRequestMethod>(
      method: M,
      handler: (
        payload: CodexRpc.ServerRequestParamsByMethod[M],
      ) => Effect.Effect<
        CodexRpc.ServerRequestResponsesByMethod[M],
        CodexError.CodexAppServerError
      >,
    ) => Effect.Effect<void>;
    readonly handleServerNotification: <M extends CodexRpc.ServerNotificationMethod>(
      method: M,
      handler: (
        payload: CodexRpc.ServerNotificationParamsByMethod[M],
      ) => Effect.Effect<void, CodexError.CodexAppServerError>,
    ) => Effect.Effect<void>;
    readonly handleUnknownServerRequest: (
      handler: (
        method: string,
        params: unknown,
      ) => Effect.Effect<unknown, CodexError.CodexAppServerError>,
    ) => Effect.Effect<void>;
    readonly handleUnknownServerNotification: (
      handler: (
        method: string,
        params: unknown,
      ) => Effect.Effect<void, CodexError.CodexAppServerError>,
    ) => Effect.Effect<void>;
  }
>()("effect-codex-app-server/client/CodexAppServerClient") {}

type ServerRequestHandler = (
  payload: unknown,
) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
type ServerNotificationHandler = (
  payload: unknown,
) => Effect.Effect<void, CodexError.CodexAppServerError>;

const V2TurnStartParamsWithCollaborationMode = CodexSchema.V2TurnStartParams.pipe(
  Schema.fieldsAssign({
    collaborationMode: Schema.optionalKey(CodexSchema.ClientRequest__CollaborationMode),
    additionalContext: Schema.optionalKey(
      Schema.Record(Schema.String, CodexSchema.V2TurnStartParams__AdditionalContextEntry),
    ),
  }),
);

const makeClientState = Effect.fn("effect-codex-app-server/CodexAppServerClient.makeState")(
  function* () {
    const requestHandlers = new Map<string, ServerRequestHandler>();
    const termination = yield* Deferred.make<never, CodexError.CodexAppServerError>();
    const notificationHandlers = new Map<string, Array<ServerNotificationHandler>>();
    let unknownRequestHandler:
      | ((
          method: string,
          params: unknown,
        ) => Effect.Effect<unknown, CodexError.CodexAppServerError>)
      | undefined;
    let unknownNotificationHandler:
      | ((method: string, params: unknown) => Effect.Effect<void, CodexError.CodexAppServerError>)
      | undefined;

    const getServerRequestParamSchema = <M extends CodexRpc.ServerRequestMethod>(
      method: M,
    ):
      | Schema.Codec<
          CodexRpc.ServerRequestParamsByMethod[M],
          CodexRpc.ServerRequestParamsByMethod[M]
        >
      | undefined => CodexRpc.SERVER_REQUEST_PARAMS[method] as never;

    const getServerRequestResponseSchema = <M extends CodexRpc.ServerRequestMethod>(
      method: M,
    ):
      | Schema.Codec<
          CodexRpc.ServerRequestResponsesByMethod[M],
          CodexRpc.ServerRequestResponsesByMethod[M]
        >
      | undefined => CodexRpc.SERVER_REQUEST_RESPONSES[method] as never;

    const getClientRequestParamSchema = <M extends CodexRpc.ClientRequestMethod>(
      method: M,
    ):
      | Schema.Codec<
          CodexRpc.ClientRequestParamsByMethod[M],
          CodexRpc.ClientRequestParamsByMethod[M]
        >
      | undefined =>
      method === "turn/start"
        ? (V2TurnStartParamsWithCollaborationMode as never)
        : (CodexRpc.CLIENT_REQUEST_PARAMS[method] as never);

    const getClientRequestResponseSchema = <M extends CodexRpc.ClientRequestMethod>(
      method: M,
    ):
      | Schema.Codec<
          CodexRpc.ClientRequestResponsesByMethod[M],
          CodexRpc.ClientRequestResponsesByMethod[M]
        >
      | undefined => CodexRpc.CLIENT_REQUEST_RESPONSES[method] as never;

    const getClientNotificationParamSchema = <M extends CodexRpc.ClientNotificationMethod>(
      method: M,
    ):
      | Schema.Codec<
          CodexRpc.ClientNotificationParamsByMethod[M],
          CodexRpc.ClientNotificationParamsByMethod[M]
        >
      | undefined => CodexRpc.CLIENT_NOTIFICATION_PARAMS[method] as never;

    // A notification the schema rejects has no reply to fail, so it is dropped.
    // A Codex build that drifted from the generated schema repeats the bad shape
    // on every frame, so each distinct failure is warned about once per client.
    const warnedDecodeFailures = new Set<string>();
    const warnUndecodableNotification = (error: CodexError.CodexAppServerProtocolParseError) => {
      const key = `${error.method}\n${error.issueKinds?.join(",")}`;
      if (warnedDecodeFailures.has(key)) {
        return Effect.void;
      }
      warnedDecodeFailures.add(key);
      return Effect.logWarning(
        "Dropped a Codex App Server notification that failed to decode",
      ).pipe(
        Effect.annotateLogs({
          operation: error.operation,
          ...(error.method === undefined ? {} : { method: error.method }),
          ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
          ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
          ...(error.maximumPathDepth === undefined
            ? {}
            : { maximumPathDepth: error.maximumPathDepth }),
        }),
      );
    };

    const dispatchNotification = (
      notification: CodexProtocol.CodexAppServerIncomingNotification,
    ): Effect.Effect<void, never> => {
      const schema =
        notification.method in CodexRpc.SERVER_NOTIFICATION_PARAMS
          ? CodexRpc.SERVER_NOTIFICATION_PARAMS[
              notification.method as CodexRpc.ServerNotificationMethod
            ]
          : undefined;
      const handlers = notificationHandlers.get(notification.method) ?? [];

      if (schema) {
        return decodeNotificationPayload(notification.method, schema, notification.params).pipe(
          Effect.tapError(warnUndecodableNotification),
          Effect.flatMap((decoded) =>
            Effect.forEach(handlers, (handler) => handler(decoded), { discard: true }),
          ),
          Effect.ignore,
        );
      }

      return unknownNotificationHandler
        ? unknownNotificationHandler(notification.method, notification.params).pipe(Effect.ignore)
        : Effect.void;
    };

    const dispatchRequest = (
      request: CodexProtocol.CodexAppServerIncomingRequest,
    ): Effect.Effect<unknown, CodexError.CodexAppServerError> => {
      if (request.method in CodexRpc.SERVER_REQUEST_PARAMS) {
        const method = request.method as CodexRpc.ServerRequestMethod;
        const payloadSchema = getServerRequestParamSchema(method);
        const responseSchema = getServerRequestResponseSchema(method);
        const handler = requestHandlers.get(method);

        return decodeOptionalPayload(method, payloadSchema, request.params).pipe(
          Effect.flatMap((decoded) => runHandler(handler, decoded, method)),
          Effect.flatMap((result) => encodeOptionalPayload(method, responseSchema, result)),
        );
      }

      return unknownRequestHandler
        ? unknownRequestHandler(request.method, request.params)
        : Effect.fail(CodexError.CodexAppServerRequestError.methodNotFound(request.method));
    };

    const handlers: Pick<
      CodexAppServerClient["Service"],
      | "handleServerRequest"
      | "handleServerNotification"
      | "handleUnknownServerRequest"
      | "handleUnknownServerNotification"
    > = {
      handleServerRequest: Effect.fn("CodexAppServerClient.handleServerRequest")(
        function (method, handler) {
          return Effect.sync(() => {
            requestHandlers.set(method, handler as ServerRequestHandler);
          });
        },
      ),
      handleServerNotification: Effect.fn("CodexAppServerClient.handleServerNotification")(
        function (method, handler) {
          return Effect.sync(() => {
            const current = notificationHandlers.get(method) ?? [];
            current.push(handler as ServerNotificationHandler);
            notificationHandlers.set(method, current);
          });
        },
      ),
      handleUnknownServerRequest: Effect.fn("CodexAppServerClient.handleUnknownServerRequest")(
        function (handler) {
          return Effect.sync(() => {
            unknownRequestHandler = handler;
          });
        },
      ),
      handleUnknownServerNotification: Effect.fn(
        "CodexAppServerClient.handleUnknownServerNotification",
      )(function (handler) {
        return Effect.sync(() => {
          unknownNotificationHandler = handler;
        });
      }),
    };
    return {
      termination,
      dispatchNotification,
      dispatchRequest,
      getClientRequestParamSchema,
      getClientRequestResponseSchema,
      getClientNotificationParamSchema,
      handlers,
    };
  },
);

export const make = Effect.fn("effect-codex-app-server/CodexAppServerClient.make")(function* (
  stdio: Stdio.Stdio,
  options: CodexAppServerClientOptions = {},
  terminationError?: Effect.Effect<CodexError.CodexAppServerError>,
): Effect.fn.Return<CodexAppServerClient["Service"], never, Scope.Scope> {
  const state = yield* makeClientState();
  const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
    stdio,
    ...(terminationError ? { terminationError } : {}),
    ...(options.logIncoming !== undefined ? { logIncoming: options.logIncoming } : {}),
    ...(options.logOutgoing !== undefined ? { logOutgoing: options.logOutgoing } : {}),
    ...(options.logger ? { logger: options.logger } : {}),
    onNotification: state.dispatchNotification,
    onRequest: state.dispatchRequest,
    onTermination: (error) => Deferred.fail(state.termination, error).pipe(Effect.asVoid),
  });

  const request = <M extends CodexRpc.ClientRequestMethod>(
    method: M,
    payload: CodexRpc.ClientRequestParamsByMethod[M],
  ): Effect.Effect<CodexRpc.ClientRequestResponsesByMethod[M], CodexError.CodexAppServerError> =>
    encodeOptionalPayload(method, state.getClientRequestParamSchema(method), payload).pipe(
      Effect.flatMap((encoded) => transport.request(method, encoded)),
      Effect.flatMap(
        (
          raw,
        ): Effect.Effect<
          CodexRpc.ClientRequestResponsesByMethod[M],
          CodexError.CodexAppServerError
        > => decodeOptionalPayload(method, state.getClientRequestResponseSchema(method), raw),
      ),
    );

  const notify = <M extends CodexRpc.ClientNotificationMethod>(
    method: M,
    payload: CodexRpc.ClientNotificationParamsByMethod[M],
  ) =>
    encodeOptionalPayload(method, state.getClientNotificationParamSchema(method), payload).pipe(
      Effect.flatMap((encoded) => transport.notify(method, encoded)),
    );

  return CodexAppServerClient.of({
    ...state.handlers,
    awaitTermination: Deferred.await(state.termination),
    raw: {
      notifications: transport.incomingNotifications,
      requests: transport.incomingRequests,
      request: transport.request,
      notify: transport.notify,
      respond: transport.respond,
      respondError: transport.respondError,
    },
    request,
    notify,
  });
});

export const layerChildProcess = (
  handle: ChildProcessSpawner.ChildProcessHandle,
  options: CodexAppServerClientOptions = {},
): Layer.Layer<CodexAppServerClient> =>
  Layer.effect(CodexAppServerClient, makeChildProcessClient(handle, options));

const makeChildProcessClient = Effect.fn(
  "effect-codex-app-server/CodexAppServerClient.makeChildProcessClient",
)(function* (handle: ChildProcessSpawner.ChildProcessHandle, options: CodexAppServerClientOptions) {
  const stderrTail = yield* makeStderrTail(handle.stderr);
  return yield* make(
    makeChildStdio(handle, stderrTail.awaitDrained),
    options,
    makeTerminationError(handle, stderrTail.read),
  );
});

export class CodexAppServerControlledClient extends Context.Service<
  CodexAppServerControlledClient,
  Omit<CodexAppServerClient["Service"], "raw" | "request" | "notify"> & {
    readonly quiesceOutput: CodexProtocol.CodexAppServerControlledProtocol["quiesceOutput"];
    readonly assertCanQuiesceOutput: CodexProtocol.CodexAppServerControlledProtocol["assertCanQuiesceOutput"];
    readonly ownedProcess: {
      readonly pid: ChildProcessSpawner.ChildProcessHandle["pid"];
      readonly isRunning: ChildProcessSpawner.ChildProcessHandle["isRunning"];
      readonly awaitExit: ChildProcessSpawner.ChildProcessHandle["exitCode"];
    };
    readonly raw: Omit<CodexAppServerClientRaw, "request" | "notify" | "respond" | "respondError"> &
      Pick<
        CodexProtocol.CodexAppServerControlledProtocol,
        "request" | "notify" | "respond" | "respondError"
      >;
    readonly request: <M extends CodexRpc.ClientRequestMethod>(
      method: M,
      payload: CodexRpc.ClientRequestParamsByMethod[M],
      selection?: string,
    ) => Effect.Effect<CodexRpc.ClientRequestResponsesByMethod[M], CodexError.CodexAppServerError>;
    readonly notify: <M extends CodexRpc.ClientNotificationMethod>(
      method: M,
      payload: CodexRpc.ClientNotificationParamsByMethod[M],
      selection?: string,
    ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  }
>()("effect-codex-app-server/client/CodexAppServerControlledClient") {}

export interface CodexAppServerControlledClientOptions<E> extends CodexAppServerClientOptions {
  readonly wrapper: CodexProtocol.CodexAppServerControlledProtocolOptions<E>["wrapper"];
  readonly onOutcome: CodexProtocol.CodexAppServerControlledProtocolOptions<E>["onOutcome"];
  readonly testSinkTimeoutMs?: number;
}

const encodeControlledPayload = <A, I>(
  method: string,
  schema: Schema.Codec<A, I> | undefined,
  payload: A,
) => {
  if (!schema)
    return payload === undefined
      ? Result.succeed(undefined)
      : Result.fail(
          CodexError.CodexAppServerRequestError.unexpectedPayload(
            method,
            "encode-payload",
            payload,
          ),
        );
  return Result.try({
    try: () => Schema.encodeSync(schema)(payload),
    catch: (cause) =>
      Schema.isSchemaError(cause)
        ? CodexError.CodexAppServerRequestError.invalidPayload(method, "encode-payload", cause)
        : CodexError.CodexAppServerRequestError.internalError(
            "Failed to encode Codex request payload",
            undefined,
            { method, operation: "encode-payload", cause },
          ),
  });
};

const snapshotControlledChild = (command: ChildProcess.Command) =>
  Result.try({
    try: () => {
      if (
        command._tag !== "StandardCommand" ||
        (command.options.additionalFds && Object.keys(command.options.additionalFds).length)
      )
        throw new Error("Controlled child requires standard stdin/stdout/stderr only");
      const {
        stdin: _stdin,
        stdout: _stdout,
        stderr: _stderr,
        additionalFds: _fds,
        ...options
      } = command.options;
      return ChildProcess.make(command.command, [...command.args], {
        ...options,
        ...(options.env ? { env: { ...options.env } } : {}),
        ...(options.forceKillAfter !== undefined
          ? {
              forceKillAfter: Duration.match(Duration.fromInputUnsafe(options.forceKillAfter), {
                onMillis: (millis) => millis,
                onNanos: (nanos) => nanos,
                onInfinity: () => Infinity,
                onNegativeInfinity: () => -Infinity,
              }),
            }
          : {}),
        stdin: { stream: "pipe", endOnDone: false },
        stdout: "pipe",
        stderr: "pipe",
      });
    },
    catch: (cause) => new CodexError.CodexAppServerSpawnError({ cause }),
  });

const CurrentControlledHandlers = Context.Reference<ReadonlySet<symbol>>(
  "effect-codex-app-server/CurrentControlledHandlers",
  { defaultValue: () => new Set() },
);

type ControlledProcessIo = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "pid" | "stdin" | "stdout" | "stderr" | "exitCode"
>;

const makeControlledProcessProtocol = Effect.fn(
  "effect-codex-app-server/CodexAppServerControlledClient.makeProcessProtocol",
)(function* <E>(handle: ControlledProcessIo, options: CodexAppServerControlledClientOptions<E>) {
  const owner = yield* Scope.fork(yield* Scope.Scope, "sequential");
  return yield* Effect.gen(function* () {
    const stderr = yield* makeStderrTail(handle.stderr);
    const state = yield* makeClientState();
    const transport = yield* CodexProtocol.makeControlledCodexAppServerProtocol({
      stdio: makeChildStdio(handle, stderr.awaitDrained),
      terminationError: makeTerminationError(handle, stderr.read),
      ...(options.logIncoming !== undefined ? { logIncoming: options.logIncoming } : {}),
      ...(options.logOutgoing !== undefined ? { logOutgoing: options.logOutgoing } : {}),
      ...(options.logger ? { logger: options.logger } : {}),
      ...(options.testSinkTimeoutMs !== undefined
        ? { testSinkTimeoutMs: options.testSinkTimeoutMs }
        : {}),
      wrapper: options.wrapper,
      onOutcome: options.onOutcome,
      onNotification: state.dispatchNotification,
      onRequest: state.dispatchRequest,
      onTermination: (error) => Deferred.fail(state.termination, error).pipe(Effect.asVoid),
    });
    const request = <M extends CodexRpc.ClientRequestMethod>(
      method: M,
      payload: CodexRpc.ClientRequestParamsByMethod[M],
      selection?: string,
    ): Effect.Effect<
      CodexRpc.ClientRequestResponsesByMethod[M],
      CodexError.CodexAppServerError
    > => {
      const encoded = encodeControlledPayload(
        method,
        state.getClientRequestParamSchema(method),
        payload,
      );
      if (Result.isFailure(encoded)) return Effect.fail(encoded.failure);
      const pending = transport.request(method, encoded.success, selection);
      return pending.pipe(
        Effect.flatMap((raw) =>
          decodeOptionalPayload(method, state.getClientRequestResponseSchema(method), raw),
        ),
      );
    };
    const notify = <M extends CodexRpc.ClientNotificationMethod>(
      method: M,
      payload: CodexRpc.ClientNotificationParamsByMethod[M],
      selection?: string,
    ) => {
      const encoded = encodeControlledPayload(
        method,
        state.getClientNotificationParamSchema(method),
        payload,
      );
      return Result.isFailure(encoded)
        ? Effect.fail(encoded.failure)
        : transport.notify(method, encoded.success, selection);
    };
    const marker = Symbol("controlled-client-handler");
    // Per-event callbacks share routing spans and inherit the SDK cleanup context.
    const inHandler = Effect.fnUntraced(function* <A, E, R>(work: Effect.Effect<A, E, R>) {
      const previous = yield* CurrentControlledHandlers;
      return yield* work.pipe(
        Effect.provideService(CurrentControlledHandlers, new Set([...previous, marker])),
      );
    });
    const handlers: typeof state.handlers = {
      handleServerRequest: (method, handler) =>
        state.handlers.handleServerRequest(method, (payload) => inHandler(handler(payload))),
      handleServerNotification: (method, handler) =>
        state.handlers.handleServerNotification(method, (payload) => inHandler(handler(payload))),
      handleUnknownServerRequest: (handler) =>
        state.handlers.handleUnknownServerRequest((method, params) =>
          inHandler(handler(method, params)),
        ),
      handleUnknownServerNotification: (handler) =>
        state.handlers.handleUnknownServerNotification((method, params) =>
          inHandler(handler(method, params)),
        ),
    };
    const assertCanQuiesceOutput = Effect.gen(function* () {
      if ((yield* CurrentControlledHandlers).has(marker))
        return yield* new CodexError.CodexAppServerFrameSubmissionError({
          outcome: "not_submitted",
          reason: "reentrant",
        });
      yield* transport.assertCanQuiesceOutput;
    });
    return Object.freeze({
      ...handlers,
      assertCanQuiesceOutput,
      quiesceOutput: assertCanQuiesceOutput.pipe(Effect.andThen(transport.quiesceOutput)),
      awaitTermination: Deferred.await(state.termination),
      raw: {
        notifications: transport.incomingNotifications,
        requests: transport.incomingRequests,
        request: transport.request,
        notify: transport.notify,
        respond: transport.respond,
        respondError: transport.respondError,
      },
      request,
      notify,
    });
  }).pipe(
    Scope.provide(owner),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(owner, exit) : Effect.void)),
  );
});

export function makeControlledProcessProtocolClient<E>(
  handle: ControlledProcessIo,
  options: CodexAppServerControlledClientOptions<E>,
) {
  const captured = {
    pid: handle.pid,
    stdin: handle.stdin,
    stdout: handle.stdout,
    stderr: handle.stderr,
    exitCode: handle.exitCode,
  };
  return makeControlledProcessProtocol(captured, { ...options });
}

const makeOwnedControlledClient = Effect.fn(
  "effect-codex-app-server/CodexAppServerControlledClient.makeChildProcess",
)(function* <E>(
  command: ChildProcess.StandardCommand,
  options: CodexAppServerControlledClientOptions<E>,
) {
  const owner = yield* Scope.fork(yield* Scope.Scope, "sequential");
  return yield* Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner
      .spawn(command)
      .pipe(
        Effect.mapError(
          (cause) => new CodexError.CodexAppServerSpawnError({ command: command.command, cause }),
        ),
      );
    const client = yield* makeControlledProcessProtocolClient(handle, options);
    return CodexAppServerControlledClient.of({
      ...client,
      ownedProcess: Object.freeze({
        pid: handle.pid,
        isRunning: handle.isRunning,
        awaitExit: handle.exitCode,
      }),
    });
  }).pipe(
    Scope.provide(owner),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(owner, exit) : Effect.void)),
  );
});

export const makeControlledChildProcessClient = <E>(
  command: ChildProcess.Command,
  options: CodexAppServerControlledClientOptions<E>,
) => {
  const owned = snapshotControlledChild(command);
  const captured = { ...options };
  return Result.isFailure(owned)
    ? Effect.fail(owned.failure)
    : makeOwnedControlledClient(owned.success, captured);
};
