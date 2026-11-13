// @effect-diagnostics nodeBuiltinImport:off
// Real Node Writable fixtures verify backpressure at the actual write boundary.
import * as Cause from "effect/Cause";
import * as NodeStream from "node:stream";
import * as NodeSink from "@effect/platform-node/NodeSink";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as TestClock from "effect/testing/TestClock";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";

import * as CodexError from "./errors.ts";
import * as CodexProtocol from "./protocol.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
const isControlledFrameFailure = Schema.is(CodexError.CodexAppServerFrameSubmissionError);
const isCodexRequestFailure = Schema.is(CodexError.CodexAppServerRequestError);
const isCodexTransportFailure = Schema.is(CodexError.CodexAppServerTransportError);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const encoder = new TextEncoder();

const encodeJsonl = (value: unknown) => encoder.encode(`${encodeUnknownJsonString(value)}\n`);

const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

it.layer(NodeServices.layer)("effect-codex-app-server protocol", (it) => {
  it.effect(
    "encodes requests without a jsonrpc field and routes inbound requests and notifications",
    () =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({ stdio });

        const notificationDeferred =
          yield* Deferred.make<ReadonlyArray<CodexProtocol.CodexAppServerIncomingNotification>>();
        const requestDeferred =
          yield* Deferred.make<ReadonlyArray<CodexProtocol.CodexAppServerIncomingRequest>>();

        yield* transport.incomingNotifications.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.flatMap((notifications) => Deferred.succeed(notificationDeferred, notifications)),
          Effect.forkScoped,
        );

        yield* transport.incomingRequests.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.flatMap((requests) => Deferred.succeed(requestDeferred, requests)),
          Effect.forkScoped,
        );

        yield* transport.notify("initialized");
        assert.equal(yield* Queue.take(output), '{"method":"initialized"}\n');

        const initializeParams = {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        };

        const pendingInitialize = yield* transport
          .request("initialize", initializeParams)
          .pipe(Effect.forkScoped);
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 1,
          method: "initialize",
          params: initializeParams,
        });

        yield* Queue.offer(
          input,
          encodeJsonl({
            method: "item/agentMessage/delta",
            params: {
              delta: "Hello from the mock peer.",
              itemId: "item-1",
              threadId: "thread-1",
              turnId: "turn-1",
            },
          }),
        );
        yield* Queue.offer(
          input,
          encodeJsonl({
            id: 77,
            method: "item/tool/requestUserInput",
            params: {
              itemId: "item-approval-1",
              threadId: "thread-1",
              turnId: "turn-1",
              questions: [
                {
                  id: "approved",
                  header: "Approve",
                  question: "Continue?",
                },
              ],
            },
          }),
        );
        yield* Queue.offer(
          input,
          encodeJsonl({
            id: 1,
            result: {
              userAgent: "mock-codex-app-server",
              codexHome: "/tmp/codex-home",
              platformFamily: "unix",
              platformOs: "macos",
            },
          }),
        );

        assert.deepEqual(yield* Fiber.join(pendingInitialize), {
          userAgent: "mock-codex-app-server",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        });
        assert.deepEqual(yield* Deferred.await(notificationDeferred), [
          {
            method: "item/agentMessage/delta",
            params: {
              delta: "Hello from the mock peer.",
              itemId: "item-1",
              threadId: "thread-1",
              turnId: "turn-1",
            },
          },
        ]);
        assert.deepEqual(yield* Deferred.await(requestDeferred), [
          {
            id: 77,
            method: "item/tool/requestUserInput",
            params: {
              itemId: "item-approval-1",
              threadId: "thread-1",
              turnId: "turn-1",
              questions: [
                {
                  id: "approved",
                  header: "Approve",
                  question: "Continue?",
                },
              ],
            },
          },
        ]);

        yield* transport.respond(77, {
          answers: {
            approved: {
              answers: ["yes"],
            },
          },
        });
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 77,
          result: {
            answers: {
              approved: {
                answers: ["yes"],
              },
            },
          },
        });

        yield* transport.respondError(
          78,
          CodexError.CodexAppServerRequestError.methodNotFound("x/test"),
        );
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 78,
          error: {
            code: -32601,
            message: "Method not found: x/test",
          },
        });
      }),
  );

  it.effect(
    "correlates concurrent request spans through responses, failures and interruption",
    () => {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      return Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({ stdio });
        const methods = ["model/list", "account/read", "skills/list"];
        const requests = [];
        for (const method of methods) {
          requests.push(
            yield* transport.request(method, { secret: "private-payload" }).pipe(Effect.forkScoped),
          );
          yield* Queue.take(output);
        }
        yield* Queue.offer(
          input,
          encodeJsonl({ id: 2, error: { code: -32602, message: "Invalid params" } }),
        );
        yield* Fiber.await(requests[1]!);
        yield* Queue.offer(input, encodeJsonl({ id: 1, result: { secret: "private-result" } }));
        yield* Fiber.join(requests[0]!);
        yield* Fiber.interrupt(requests[2]!);
        // A late response for an interrupted request must not affect the next request.
        yield* Queue.offer(input, encodeJsonl({ id: 3, result: {} }));
        const next = yield* transport.request("config/read").pipe(Effect.forkScoped);
        yield* Queue.take(output);
        yield* Queue.offer(input, encodeJsonl({ id: 4, result: {} }));
        yield* Fiber.join(next);
        for (const [index, method] of [...methods, "config/read"].entries()) {
          const request = spans.find(
            (span) =>
              span.name === "CodexAppServerProtocol.request" &&
              span.attributes.get("rpc.request_id") === index + 1,
          );
          assert.isDefined(request);
          assert.equal(request!.attributes.get("rpc.method"), method);
          assert.equal(request!.attributes.get("rpc.system"), "jsonrpc");
          assert.equal(request!.status._tag, "Ended");
          if (request!.status._tag === "Ended") {
            assert.equal(
              request!.status.exit._tag,
              index === 1 || index === 2 ? "Failure" : "Success",
            );
          }
          for (const phase of ["enqueue", "awaitResponse"]) {
            const child: Tracer.NativeSpan | undefined = spans.find(
              (span) =>
                span.name === `CodexAppServerProtocol.request.${phase}` &&
                span.parent?._tag === "Some" &&
                span.parent.value.spanId === request!.spanId,
            );
            assert.isDefined(child);
            assert.equal(child!.traceId, request!.traceId);
            assert.equal(child!.attributes.get("rpc.method"), method);
            assert.equal(child!.attributes.get("rpc.request_id"), index + 1);
            assert.equal(child!.status._tag, "Ended");
          }
        }
        const failedSend = yield* transport
          .request("thread/start", { value: 1n })
          .pipe(Effect.exit);
        assert.equal(failedSend._tag, "Failure");
        const sendSpans = spans.filter((span) => span.attributes.get("rpc.request_id") === 5);
        assert.deepEqual(
          sendSpans.map((span) => span.name),
          ["CodexAppServerProtocol.request", "CodexAppServerProtocol.request.enqueue"],
        );
        for (const span of sendSpans) {
          assert.equal(span.attributes.get("rpc.method"), "thread/start");
          assert.equal(span.status._tag, "Ended");
          if (span.status._tag === "Ended") assert.equal(span.status.exit._tag, "Failure");
        }
        for (const span of spans) {
          const attrs = JSON.stringify([...span.attributes]);
          assert.notInclude(attrs, "private-payload");
          assert.notInclude(attrs, "private-result");
        }
      }).pipe(Effect.withTracer(tracer));
    },
  );

  it.effect.each([1, 7, 1024])(
    "preserves JSONL framing and UTF-8 across %i-byte input chunks",
    (chunkSize) =>
      Effect.gen(function* () {
        const { stdio, input } = yield* makeInMemoryStdio();
        const notifications: Array<CodexProtocol.CodexAppServerIncomingNotification> = [];
        const rawLines: Array<unknown> = [];
        const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
        yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
          stdio,
          logIncoming: true,
          logger: (event) =>
            Effect.sync(() => {
              if (event.stage === "raw") {
                rawLines.push(event.payload);
              }
            }),
          onNotification: (notification) =>
            Effect.sync(() => {
              notifications.push(notification);
            }),
          onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
        });

        const firstLine = '{"method":"x/first",\r"params":{"text":"hé🙂"}}';
        const secondLine = '{"method":"x/second","params":{"value":2}}';
        const finalLine = '{"method":"x/final","params":{"text":"最後"}}\r';
        const bytes = encoder.encode(`\n \t\r\n${firstLine}\r\n\n${secondLine}\n${finalLine}`);
        for (let offset = 0; offset < bytes.length; offset += chunkSize) {
          yield* Queue.offer(input, bytes.subarray(offset, offset + chunkSize));
        }
        yield* Queue.end(input);

        assert.instanceOf(
          yield* Deferred.await(termination),
          CodexError.CodexAppServerInputStreamEndedError,
        );
        assert.deepEqual(notifications, [
          { method: "x/first", params: { text: "hé🙂" } },
          { method: "x/second", params: { value: 2 } },
          { method: "x/final", params: { text: "最後" } },
        ]);
        assert.deepEqual(rawLines, [firstLine, secondLine, finalLine]);
      }),
  );

  it.effect.each(["newline-terminated", "unfinished"])(
    "rejects an oversized %s message using UTF-8 bytes before logging or parsing",
    (framing) =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const notifications: Array<CodexProtocol.CodexAppServerIncomingNotification> = [];
        const logged: Array<unknown> = [];
        const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
        let reachedEnd = false;
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
          stdio: Stdio.make({
            ...stdio,
            stdin: stdio.stdin.pipe(
              Stream.concat(
                Stream.suspend(() => {
                  reachedEnd = true;
                  return Stream.empty;
                }),
              ),
            ),
          }),
          maxIncomingMessageBytes: 16,
          logIncoming: true,
          logger: (event) =>
            Effect.sync(() => {
              logged.push(event.payload);
            }),
          onNotification: (notification) =>
            Effect.sync(() => {
              notifications.push(notification);
            }),
          onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
        });
        const pending = yield* Effect.forEach(["thread/read", "thread/list"], (method) =>
          transport.request(method, {}).pipe(Effect.forkScoped),
        );
        yield* Effect.forEach(pending, () => Queue.take(output));

        // Two 16-byte lines must survive; the 17-byte line is only 15 UTF-16 code units.
        const firstLine = '{"method":"xé"}';
        const secondLine = '{"method":"x/a"}';
        const bytes = encoder.encode(
          `${firstLine}\n${secondLine}\n{"method":"éé"}${framing === "unfinished" ? "" : "\n"}`,
        );
        // Split a UTF-8 sequence on both sides of the byte limit.
        const firstSplit = bytes.indexOf(0xc3) + 1;
        yield* Queue.offer(input, bytes.subarray(0, firstSplit));
        if (framing === "unfinished") {
          const lastSplit = bytes.lastIndexOf(0xc3) + 1;
          yield* Queue.offer(input, bytes.subarray(firstSplit, lastSplit));
          yield* Queue.offer(input, bytes.subarray(lastSplit));
        } else {
          yield* Queue.offer(input, bytes.subarray(firstSplit));
        }
        yield* Queue.end(input);

        const error = yield* Deferred.await(termination);
        assert.instanceOf(error, CodexError.CodexAppServerTransportError);
        assert.equal(error.operation, "read-input-stream");
        assert.isFalse(reachedEnd, "Must reject the unfinished line before reading EOF");
        for (const response of pending) {
          const failure = yield* Fiber.join(response).pipe(
            Effect.match({
              onFailure: (error) => error,
              onSuccess: () => assert.fail("Expected the oversized message to fail the request"),
            }),
          );
          assert.strictEqual(failure, error);
        }
        assert.deepEqual(notifications, [{ method: "xé" }, { method: "x/a" }]);
        assert.deepEqual(logged, [firstLine, { method: "xé" }, secondLine, { method: "x/a" }]);
      }),
  );

  it.effect("reports a malformed fragmented final line before input stream termination", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });
      const response = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);

      yield* Queue.offer(input, encoder.encode('{"id":1,'));
      yield* Queue.offer(input, encoder.encode('"result":'));
      yield* Queue.end(input);

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
      assert.equal(error.operation, "decode-wire-message");
      const responseError = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (failure) => failure,
          onSuccess: () => assert.fail("Expected the malformed response to fail the request"),
        }),
      );
      assert.strictEqual(responseError, error);
    }),
  );

  it.effect("keeps processing protocol messages while an approval is pending", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const approvalStarted = yield* Deferred.make<void>();
      const approvalDecision = yield* Deferred.make<{ readonly decision: string }>();
      const notificationReceived = yield* Deferred.make<void>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(approvalStarted, undefined).pipe(
            Effect.andThen(Deferred.await(approvalDecision)),
          ),
        onNotification: () => Deferred.succeed(notificationReceived, undefined).pipe(Effect.asVoid),
      });

      const pendingRequest = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(
        input,
        encoder.encode(
          `${[
            encodeUnknownJsonString({ id: 7, method: "item/tool/requestUserInput", params: {} }),
            encodeUnknownJsonString({ method: "item/agentMessage/delta", params: { delta: "ok" } }),
            encodeUnknownJsonString({ id: 1, result: { threadId: "thread-1" } }),
          ].join("\n")}\n`,
        ),
      );

      yield* Deferred.await(approvalStarted);
      yield* Deferred.await(notificationReceived);
      assert.deepEqual(yield* Fiber.join(pendingRequest), { threadId: "thread-1" });

      yield* Deferred.succeed(approvalDecision, { decision: "accept" });
      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 7,
        result: { decision: "accept" },
      });
    }),
  );

  it.effect("rejects incoming requests after the active handler limit is reached", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const handlersStarted = yield* Deferred.make<void>();
      const releaseHandlers = yield* Deferred.make<void>();
      let activeHandlers = 0;
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Effect.sync(() => ++activeHandlers).pipe(
            Effect.flatMap((count) =>
              count === 32
                ? Deferred.succeed(handlersStarted, undefined).pipe(Effect.asVoid)
                : Effect.void,
            ),
            Effect.andThen(Deferred.await(releaseHandlers)),
            Effect.as({ decision: "accept" }),
          ),
      });

      const requests = Array.from({ length: 33 }, (_, index) =>
        encodeUnknownJsonString({
          id: index + 1,
          method: "item/tool/requestUserInput",
          params: {},
        }),
      );
      yield* Queue.offer(input, encoder.encode(`${requests.join("\n")}\n`));
      yield* Deferred.await(handlersStarted);

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 33,
        error: {
          code: -32001,
          message: "Too many Codex requests are already active.",
        },
      });
      assert.equal(activeHandlers, 32);

      yield* Deferred.succeed(releaseHandlers, undefined);
      yield* Effect.forEach(Array.from({ length: 32 }), () => Queue.take(output), {
        discard: true,
      });
    }),
  );

  it.effect("releases a waiting request handler when Codex resolves its request", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: (request) =>
          request.id === 33 ? Effect.succeed({ decision: "accept" }) : Effect.never,
      });

      const messages = Array.from({ length: 32 }, (_, index) =>
        encodeUnknownJsonString({
          id: index + 1,
          method: "item/commandExecution/requestApproval",
          params: {},
        }),
      );
      for (let requestId = 1; requestId <= 32; requestId++) {
        messages.push(
          encodeUnknownJsonString({
            method: "serverRequest/resolved",
            params: { threadId: "thread-1", requestId },
          }),
        );
      }
      messages.push(
        encodeUnknownJsonString({
          id: 33,
          method: "item/commandExecution/requestApproval",
          params: {},
        }),
      );
      yield* Queue.offer(input, encoder.encode(`${messages.join("\n")}\n`));

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 33,
        result: { decision: "accept" },
      });
    }),
  );

  it.effect("keeps numeric and string request ids apart when Codex resolves one", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const interrupted: Array<string | number> = [];
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: (request) =>
          request.id === 2
            ? Effect.sync(() => ({ interrupted }))
            : Effect.never.pipe(
                Effect.onInterrupt(() => Effect.sync(() => interrupted.push(request.id))),
              ),
      });

      const messages = [
        { id: 1, method: "item/commandExecution/requestApproval", params: {} },
        { id: "1", method: "item/commandExecution/requestApproval", params: {} },
        { method: "serverRequest/resolved", params: { threadId: "thread-1", requestId: "1" } },
        { id: 2, method: "item/commandExecution/requestApproval", params: {} },
      ].map((message) => encodeUnknownJsonString(message));
      yield* Queue.offer(input, encoder.encode(`${messages.join("\n")}\n`));

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 2,
        result: { interrupted: ["1"] },
      });
    }),
  );

  it.effect("interrupts pending request handlers when the protocol terminates", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const approvalStarted = yield* Deferred.make<void>();
      const approvalInterrupted = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<void>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(approvalStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Deferred.succeed(approvalInterrupted, undefined).pipe(Effect.asVoid),
            ),
          ),
        onTermination: () => Deferred.succeed(terminated, undefined).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: 7, method: "item/tool/requestUserInput", params: {} }),
      );
      yield* Deferred.await(approvalStarted);
      yield* Queue.end(input);

      yield* Deferred.await(approvalInterrupted);
      yield* Deferred.await(terminated);
    }),
  );

  it.effect("rejects outgoing messages after an approval response cannot be encoded", () =>
    Effect.gen(function* () {
      const { stdio: baseStdio, input } = yield* makeInMemoryStdio();
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const readerStopped = yield* Deferred.make<void>();
      let notificationCount = 0;
      let requestCount = 0;
      const stdio = Stdio.make({
        args: baseStdio.args,
        stdin: baseStdio.stdin.pipe(
          Stream.ensuring(Deferred.succeed(readerStopped, undefined).pipe(Effect.asVoid)),
        ),
        stdout: baseStdio.stdout,
        stderr: baseStdio.stderr,
      });
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Effect.sync(() => ++requestCount).pipe(
            Effect.map((count) => (count === 1 ? { invalid: 1n } : { ok: true })),
          ),
        onNotification: () => Effect.sync(() => notificationCount++).pipe(Effect.asVoid),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: 7, method: "item/tool/requestUserInput", params: {} }),
      );

      const failure = yield* Deferred.await(terminated);
      assert.instanceOf(failure, CodexError.CodexAppServerProtocolParseError);
      const requestFailure = yield* transport.request("thread/read", {}).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected a terminated protocol request to fail"),
        }),
      );
      const notificationFailure = yield* transport.notify("initialized").pipe(Effect.flip);
      assert.strictEqual(requestFailure, failure);
      assert.strictEqual(notificationFailure, failure);
      yield* Deferred.await(readerStopped);

      yield* Queue.offer(
        input,
        encoder.encode(
          `${[
            encodeUnknownJsonString({ method: "x/late-notification" }),
            encodeUnknownJsonString({ id: 8, method: "x/late-request" }),
          ].join("\n")}\n`,
        ),
      );

      assert.equal(notificationCount, 0);
      assert.equal(requestCount, 1);
      assert.equal(yield* Queue.size(input), 1);
    }),
  );

  it.effect("fails pending requests before interrupted handler cleanup completes", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const handlerStarted = yield* Deferred.make<void>();
      const finalizerStarted = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(handlerStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Deferred.succeed(finalizerStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFinalizer)),
              ),
            ),
          ),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(input, encodeJsonl({ id: 7, method: "x/approval" }));
      yield* Deferred.await(handlerStarted);
      yield* Queue.end(input);

      const failure = yield* Deferred.await(terminated);
      yield* Deferred.await(finalizerStarted);
      const pendingFailure = yield* Fiber.join(pending).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected the pending request to fail"),
        }),
      );
      assert.strictEqual(pendingFailure, failure);

      yield* Deferred.succeed(releaseFinalizer, undefined);
    }),
  );

  it.effect("correlates response errors with the originating request", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({ stdio });

      const response = yield* transport.request("thread/start", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(
        input,
        encodeJsonl({
          id: 1,
          error: {
            code: -32602,
            message: "Invalid params",
            data: { field: "cwd" },
          },
        }),
      );

      const error = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected Codex App Server request to fail"),
        }),
      );
      assert.instanceOf(error, CodexError.CodexAppServerRequestError);
      assert.deepInclude(error, {
        code: -32602,
        errorMessage: "Invalid params",
        method: "thread/start",
        requestId: "1",
        operation: "receive-response",
      });
    }),
  );

  it.effect("logs decode failures without copying the cause or wire payload", () =>
    Effect.gen(function* () {
      const secret = "codex-wire-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const events: Array<CodexProtocol.CodexAppServerProtocolLogEvent> = [];
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        logIncoming: true,
        logger: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`{"secret":"${secret}"\n`));
      yield* Deferred.await(termination);

      const event = events.find(({ stage }) => stage === "decode_failed");
      assert.exists(event);
      assert.equal(event.direction, "incoming");
      const payload = event.payload as Record<string, unknown>;
      assert.equal(payload.operation, "decode-wire-message");
      assert.isNumber(payload.issueCount);
      assert.isArray(payload.issueKinds);
      assert.isNumber(payload.maximumPathDepth);
      assert.equal("cause" in payload, false);
      assert.equal("detail" in payload, false);
      assert.notInclude(encodeUnknownJsonString(event), secret);
    }),
  );

  it.effect("describes unroutable messages with safe structural diagnostics", () =>
    Effect.gen(function* () {
      const secret = "codex-unroutable-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: true, method: "thread/start", params: { token: secret } }),
      );

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
      assert.deepInclude(error, {
        operation: "route-wire-message",
        method: "thread/start",
        payloadKind: "object",
        presentFields: ["id", "method", "params"],
      });
      assert.isUndefined(error.requestId);
      assert.notProperty(error, "detail");
      assert.notProperty(error, "cause");
      assert.notInclude(error.message, secret);
    }),
  );

  it.effect("fails pending requests when input ends while the process is still running", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        terminationError: Effect.never,
      });
      const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);

      yield* Queue.end(input);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const failure = yield* Fiber.join(pending).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected the pending request to fail"),
        }),
      );
      assert.instanceOf(failure, CodexError.CodexAppServerInputStreamEndedError);
    }),
  );

  it.effect("fails pending requests when a write fails while input stays open", () =>
    Effect.gen(function* () {
      const { stdio: baseStdio } = yield* makeInMemoryStdio();
      const writeFailure = PlatformError.systemError({
        _tag: "Unknown",
        module: "Stdio",
        method: "stdout",
        cause: new Error("EPIPE"),
      });
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio: Stdio.make({
          args: baseStdio.args,
          stdin: baseStdio.stdin,
          stdout: () => Sink.forEach(() => Effect.fail(writeFailure)),
          stderr: baseStdio.stderr,
        }),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);

      const failure = yield* Deferred.await(terminated);
      assert.instanceOf(failure, CodexError.CodexAppServerTransportError);
      assert.equal(failure.operation, "write-output-stream");
      assert.strictEqual(failure.cause, writeFailure);
      for (const request of [Fiber.join(pending), transport.request("thread/read", {})]) {
        const requestFailure = yield* request.pipe(
          Effect.match({
            onFailure: (error) => error,
            onSuccess: () => assert.fail("Expected the broken transport to reject the request"),
          }),
        );
        assert.strictEqual(requestFailure, failure);
      }
    }),
  );

  it.effect("reports a process exit that follows input end", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const exitError = new CodexError.CodexAppServerProcessExitedError({ code: 1, pid: 123 });
      const exited = yield* Deferred.make<CodexError.CodexAppServerError>();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        terminationError: Deferred.await(exited),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.end(input);
      yield* TestClock.adjust("100 millis");
      yield* Deferred.succeed(exited, exitError);

      assert.strictEqual(yield* Deferred.await(termination), exitError);
    }),
  );
});

const makeControlledFrameWritable = Effect.fn("test.makeControlledFrameWritable")(function* (
  blocked = true,
) {
  const writes = yield* Queue.unbounded<string>();
  const callbacks = yield* Queue.unbounded<(error?: Error | null) => void>();
  const writable = yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new NodeStream.Writable({
          highWaterMark: blocked ? 1 : 2 * 1024 * 1024,
          write(chunk: Buffer, _encoding, callback) {
            Queue.offerUnsafe(writes, chunk.toString("utf8"));
            if (blocked) Queue.offerUnsafe(callbacks, callback);
            else callback();
          },
        }),
    ),
    (stream) =>
      Effect.sync(() => {
        stream.destroy();
      }),
  );
  const stdio = Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.never,
    stderr: () => Sink.drain,
    stdout: () =>
      NodeSink.fromWritable({
        evaluate: () => writable,
        endOnDone: false,
        onError: (cause) =>
          PlatformError.systemError({
            module: "Stream",
            method: "write",
            _tag: "Unknown",
            cause,
          }),
      }),
  });
  return { writes, callbacks, writable, stdio };
});
const controlledFrame = '{"id":7,"method":"turn/start","params":{"text":"owned"}}\n';

it.effect(
  "controlled frame submission spans actual write and drain with owned input and open stdin",
  () =>
    Effect.gen(function* () {
      const io = yield* makeControlledFrameWritable();
      const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
      let wrapperOpen = false;
      const options = {
        stdio: io.stdio,
        encoded: controlledFrame,
        selection: "native-selection",
        wrapper: (
          frame: CodexProtocol.CodexAppServerControlledFrame,
          submit: CodexProtocol.CodexAppServerFrameSubmit,
        ) =>
          Effect.gen(function* () {
            wrapperOpen = true;
            assert.strictEqual(frame.encoded, controlledFrame);
            assert.strictEqual(frame.method, "turn/start");
            assert.strictEqual(frame.requestId, 7);
            assert.strictEqual(frame.selection, "native-selection");
            assert.isTrue(Object.isFrozen(frame));
            return yield* submit();
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                wrapperOpen = false;
              }),
            ),
          ),
        onOutcome: (outcome: CodexProtocol.CodexAppServerFrameOutcome) =>
          Effect.gen(function* () {
            assert.isFalse(wrapperOpen);
            yield* Queue.offer(outcomes, outcome);
          }),
      };
      const pending = CodexProtocol.submitControlledCodexAppServerFrame(options);
      options.encoded = '{"method":"mutated"}\n';
      options.selection = "mutated";
      const fiber = yield* pending.pipe(Effect.forkScoped);
      assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
      assert.isTrue(wrapperOpen);
      assert.strictEqual(yield* Queue.size(outcomes), 0);
      const releaseDrain = yield* Queue.take(io.callbacks);
      yield* Effect.sync(releaseDrain);
      const result = yield* Fiber.join(fiber);
      assert.strictEqual(result.outcome, "submitted");
      assert.isTrue(Object.isFrozen(result));
      assert.deepStrictEqual(yield* Queue.take(outcomes), result);
      assert.isFalse(io.writable.writableEnded);
      assert.isFalse(io.writable.destroyed);
      const next = yield* CodexProtocol.submitControlledCodexAppServerFrame({
        ...options,
        encoded: controlledFrame,
        selection: "native-selection",
      }).pipe(Effect.forkScoped);
      assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
      const releaseNextDrain = yield* Queue.take(io.callbacks);
      yield* Effect.sync(releaseNextDrain);
      assert.strictEqual((yield* Fiber.join(next)).outcome, "submitted");
      assert.isFalse(io.writable.writableEnded);
    }),
);

it.effect(
  "controlled frame submission retains unknown after successful write and wrapper commit failure",
  () =>
    Effect.gen(function* () {
      const io = yield* makeControlledFrameWritable(false);
      const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          stdio: io.stdio,
          encoded: controlledFrame,
          wrapper: (_frame, submit) =>
            Effect.gen(function* () {
              yield* submit();
              return yield* Effect.fail("commit-failed");
            }),
          onOutcome: (outcome) => Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
        }),
      );
      assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.outcome, "unknown");
      assert.strictEqual((yield* Queue.take(outcomes)).outcome, "unknown");
    }),
);

const controlledFrameFixture = Effect.fn("test.controlledFrameFixture")(function* (
  blocked = false,
) {
  const io = yield* makeControlledFrameWritable(blocked);
  const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
  return {
    ...io,
    outcomes,
    options: {
      stdio: io.stdio,
      encoded: controlledFrame,
      wrapper: (
        _frame: CodexProtocol.CodexAppServerControlledFrame,
        submit: CodexProtocol.CodexAppServerFrameSubmit,
      ) => submit(),
      onOutcome: (outcome: CodexProtocol.CodexAppServerFrameOutcome) =>
        Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
    },
  };
});

it.effect.each([
  "",
  "{}\n",
  "[]\n",
  "null\n",
  "{bad}\n",
  '{"method":7}\n',
  '{"method":"turn/start","method":"model/list"}\n',
  '{"id":7,"id":8,"method":"turn/start"}\n',
  '{"id":"' + String.fromCharCode(0xd800) + '","method":"turn/start"}\n',
  '{"id":9007199254740992,"method":"turn/start"}\n',
  '{"id":1.5,"method":"turn/start"}\n',
  '{"method":"turn/start","id":null}\n',
  '{"method":"turn/start"}',
  '{"method":"turn/start"}\n{}\n',
  '{\n"method":"turn/start"}\n',
])("controlled frame submission denies invalid JSONL %j before wrapper or write", (encoded) =>
  Effect.gen(function* () {
    const io = yield* controlledFrameFixture();
    const result = yield* Effect.result(
      CodexProtocol.submitControlledCodexAppServerFrame({
        ...io.options,
        encoded,
        wrapper: () => Effect.die("invalid frame must never enter wrapper"),
      }),
    );
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.strictEqual(result.failure.outcome, "not_submitted");
      assert.strictEqual(result.failure.reason, "invalid_frame");
    }
    assert.strictEqual(yield* Queue.size(io.writes), 0);
    assert.strictEqual(yield* Queue.size(io.outcomes), 0);
  }),
);

it.effect.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2001])(
  "controlled frame submission rejects invalid Sink interval %s",
  (testSinkTimeoutMs) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          ...io.options,
          testSinkTimeoutMs,
          wrapper: () => Effect.die("invalid limit must never enter wrapper"),
        }),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "invalid_timeout");
      assert.strictEqual(yield* Queue.size(io.writes), 0);
    }),
);

it.effect.each([0, 1])(
  "controlled frame submission enforces exact UTF8 cap with extra byte %s",
  (extra) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      const empty = '{"id":7,"method":"turn/start","params":""}\n';
      const remaining = 1048576 - Buffer.byteLength(empty);
      const text = "é".repeat(Math.floor(remaining / 2)) + "x".repeat((remaining % 2) + extra);
      const encoded = `{"id":7,"method":"turn/start","params":"${text}"}\n`;
      assert.strictEqual(Buffer.byteLength(encoded), 1048576 + extra);
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({ ...io.options, encoded }),
      );
      if (extra === 0) {
        assert.strictEqual(result._tag, "Success");
        assert.strictEqual(Buffer.byteLength(yield* Queue.take(io.writes)), 1048576);
        assert.strictEqual((yield* Queue.take(io.outcomes)).outcome, "submitted");
      } else {
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.strictEqual(result.failure.outcome, "not_submitted");
          assert.strictEqual(result.failure.reason, "frame_limit");
        }
        assert.strictEqual(yield* Queue.size(io.writes), 0);
        assert.strictEqual(yield* Queue.size(io.outcomes), 0);
      }
    }),
);

it.effect.each([false, true])(
  "controlled frame submission forbids repeated continuation after attempted=%s",
  (attempt) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          ...io.options,
          wrapper: (_frame, submit) =>
            Effect.gen(function* () {
              if (!attempt)
                return yield* Effect.scoped(
                  submit().pipe(Effect.forkScoped, Effect.flatMap(Fiber.join)),
                );
              const receipt = yield* submit();
              yield* Effect.result(submit());
              return receipt;
            }),
        }),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.strictEqual(result.failure.reason, "continuation");
        assert.strictEqual(result.failure.outcome, attempt ? "unknown" : "not_submitted");
      }
      assert.strictEqual(yield* Queue.size(io.writes), attempt ? 1 : 0);
      assert.strictEqual(
        (yield* Queue.take(io.outcomes)).outcome,
        attempt ? "unknown" : "not_submitted",
      );
    }),
);

it.effect(
  "controlled frame submission rejects escaped continuation and foreign receipt without writing",
  () =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      let escaped: CodexProtocol.CodexAppServerFrameSubmit | undefined;
      let oldReceipt:
        | Effect.Success<ReturnType<CodexProtocol.CodexAppServerFrameSubmit>>
        | undefined;
      yield* CodexProtocol.submitControlledCodexAppServerFrame({
        ...io.options,
        wrapper: (_frame, submit) =>
          Effect.gen(function* () {
            escaped = submit;
            oldReceipt = yield* submit();
            return oldReceipt;
          }),
      });
      assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
      if (!escaped || !oldReceipt) throw new Error("expected captured continuation and receipt");
      assert.strictEqual((yield* Effect.result(escaped()))._tag, "Failure");
      const receipt = oldReceipt;
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          ...io.options,
          wrapper: () => Effect.succeed(receipt),
        }),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.outcome, "not_submitted");
      assert.strictEqual(yield* Queue.size(io.writes), 0);
    }),
);

it.effect.each([false, true])(
  "controlled frame submission times out delayed drain with masked wrapper=%s",
  (masked) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture(true);
      const fiber = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          ...io.options,
          testSinkTimeoutMs: 20,
          wrapper: (_frame, submit) => (masked ? Effect.uninterruptible(submit()) : submit()),
        }),
      ).pipe(Effect.forkScoped);
      assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
      yield* TestClock.adjust("20 millis");
      const result = yield* Fiber.join(fiber);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.strictEqual(result.failure.reason, "timeout");
        assert.strictEqual(result.failure.outcome, "unknown");
      }
      assert.strictEqual((yield* Queue.take(io.outcomes)).outcome, "unknown");
      assert.isFalse(io.writable.destroyed);
      assert.isFalse(io.writable.writableEnded);
    }),
);

it.effect("controlled frame submission retains unknown on actual Writable error", () =>
  Effect.gen(function* () {
    const io = yield* controlledFrameFixture(true);
    const fiber = yield* Effect.result(
      CodexProtocol.submitControlledCodexAppServerFrame(io.options),
    ).pipe(Effect.forkScoped);
    assert.strictEqual(yield* Queue.take(io.writes), controlledFrame);
    const failWrite = yield* Queue.take(io.callbacks);
    yield* Effect.sync(() => failWrite(new Error("fixture write failed")));
    const result = yield* Fiber.join(fiber);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.strictEqual(result.failure.outcome, "unknown");
      assert.strictEqual(result.failure.reason, "sink");
    }
    assert.strictEqual((yield* Queue.take(io.outcomes)).outcome, "unknown");
  }),
);

it.effect.each(["before", "drain", "after"] as const)(
  "controlled frame submission classifies interruption %s write",
  (stage) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture(stage === "drain");
      const entered = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      const fiber = yield* CodexProtocol.submitControlledCodexAppServerFrame({
        ...io.options,
        wrapper: (_frame, submit) =>
          Effect.gen(function* () {
            if (stage === "before") {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(hold);
            }
            const receipt = yield* submit();
            yield* Deferred.succeed(entered, undefined);
            if (stage === "after") yield* Deferred.await(hold);
            return receipt;
          }),
      }).pipe(Effect.forkScoped);
      if (stage === "drain") yield* Queue.take(io.writes);
      else yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
      const outcome = yield* Queue.take(io.outcomes);
      assert.strictEqual(outcome.outcome, stage === "before" ? "not_submitted" : "unknown");
      assert.strictEqual(outcome.reason, "interrupted");
      assert.strictEqual(yield* Queue.size(io.writes), stage === "after" ? 1 : 0);
    }),
);

it.effect("controlled frame submission completes outcome hook before owned Scope closes", () =>
  Effect.gen(function* () {
    const io = yield* controlledFrameFixture(true);
    const scope = yield* Effect.acquireRelease(Scope.make("sequential"), (scope) =>
      Scope.close(scope, Exit.void),
    );
    const hookEntered = yield* Deferred.make<void>();
    const releaseHook = yield* Deferred.make<void>();
    const closeFinished = yield* Deferred.make<void>();
    let wrapperReleased = false;
    const fiber = yield* CodexProtocol.submitControlledCodexAppServerFrame({
      ...io.options,
      wrapper: (_frame, submit) =>
        submit().pipe(
          Effect.ensuring(
            Effect.sync(() => {
              wrapperReleased = true;
            }),
          ),
        ),
      onOutcome: (outcome) =>
        Effect.gen(function* () {
          assert.isTrue(wrapperReleased);
          yield* Deferred.succeed(hookEntered, undefined);
          yield* Deferred.await(releaseHook);
          yield* io.options.onOutcome(outcome);
        }),
    }).pipe(Effect.forkIn(scope));
    yield* Queue.take(io.writes);
    const closing = yield* Scope.close(scope, Exit.void).pipe(
      Effect.andThen(Deferred.succeed(closeFinished, undefined)),
      Effect.forkScoped,
    );
    yield* Deferred.await(hookEntered);
    try {
      assert.isFalse(yield* Deferred.isDone(closeFinished));
    } finally {
      yield* Deferred.succeed(releaseHook, undefined);
    }
    yield* Fiber.join(closing);
    assert.strictEqual((yield* Queue.take(io.outcomes)).outcome, "unknown");
    const exit = yield* Fiber.await(fiber);
    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
  }),
);

it.effect.each([false, true])(
  "controlled frame submission preserves outcome on hook defect with sent=%s",
  (sent) =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      let calls = 0;
      const result = yield* Effect.result(
        CodexProtocol.submitControlledCodexAppServerFrame({
          ...io.options,
          wrapper: (_frame, submit) =>
            Effect.gen(function* () {
              if (!sent) return yield* Effect.fail("refused");
              return yield* submit();
            }),
          onOutcome: (outcome) =>
            Effect.gen(function* () {
              calls++;
              yield* io.options.onOutcome(outcome);
              return yield* Effect.die("fixture outcome failed");
            }),
        }),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.strictEqual(result.failure.reason, "outcome_hook");
        assert.strictEqual(result.failure.outcome, sent ? "submitted" : "not_submitted");
      }
      assert.strictEqual(
        (yield* Queue.take(io.outcomes)).outcome,
        sent ? "submitted" : "not_submitted",
      );
      assert.strictEqual(calls, 1);
      assert.strictEqual(yield* Queue.size(io.writes), sent ? 1 : 0);
    }),
);

it.effect(
  "controlled frame submission preserves completed outcome during protected hook cancellation",
  () =>
    Effect.gen(function* () {
      const io = yield* controlledFrameFixture();
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const interruptFinished = yield* Deferred.make<void>();
      const fiber = yield* CodexProtocol.submitControlledCodexAppServerFrame({
        ...io.options,
        onOutcome: (outcome) =>
          Effect.gen(function* () {
            yield* io.options.onOutcome(outcome);
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }),
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const interrupting = yield* Fiber.interrupt(fiber).pipe(
        Effect.andThen(Deferred.succeed(interruptFinished, undefined)),
        Effect.forkScoped({ startImmediately: true }),
      );
      try {
        assert.isFalse(yield* Deferred.isDone(interruptFinished));
      } finally {
        yield* Deferred.succeed(release, undefined);
      }
      yield* Fiber.join(interrupting);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
      assert.strictEqual((yield* Queue.take(io.outcomes)).outcome, "submitted");
      assert.strictEqual(yield* Queue.size(io.outcomes), 0);
    }),
);

it.effect("controlled protocol owns request payload and local selection before evaluation", () =>
  Effect.gen(function* () {
    const io = yield* makeInMemoryStdio();
    const frames = yield* Queue.unbounded<CodexProtocol.CodexAppServerControlledFrame>();
    const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
    const transport = yield* CodexProtocol.makeControlledCodexAppServerProtocol({
      stdio: io.stdio,
      wrapper: (frame, submit) => Queue.offer(frames, frame).pipe(Effect.andThen(submit())),
      onOutcome: (outcome) => Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
    });
    const payload = { text: "owned" };
    const request = transport.request("turn/start", payload, "owned-selection");
    payload.text = "mutated";
    const fiber = yield* request.pipe(Effect.forkScoped);
    const written = yield* Queue.take(io.output);
    assert.strictEqual(written, '{"id":1,"method":"turn/start","params":{"text":"owned"}}\n');
    const frame = yield* Queue.take(frames);
    assert.strictEqual(frame.selection, "owned-selection");
    assert.strictEqual(frame.method, "turn/start");
    assert.strictEqual(frame.requestId, 1);
    assert.strictEqual((yield* Queue.take(outcomes)).outcome, "submitted");
    yield* Queue.offer(io.input, encodeJsonl({ id: 1, result: { accepted: true } }));
    assert.deepStrictEqual(yield* Fiber.join(fiber), { accepted: true });
  }),
);

it.effect("controlled protocol holds an early response until wrapper and hook settle", () =>
  Effect.gen(function* () {
    const io = yield* makeInMemoryStdio();
    const commit = yield* Deferred.make<void>();
    const hookEntered = yield* Deferred.make<void>();
    const publish = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    const responseObserved = yield* Deferred.make<void>();
    const transport = yield* CodexProtocol.makeControlledCodexAppServerProtocol({
      stdio: io.stdio,
      onNotification: (notification) =>
        notification.method === "fixture/response-routed"
          ? Deferred.succeed(responseObserved, undefined).pipe(Effect.asVoid)
          : Effect.void,
      wrapper: (_frame, submit) =>
        Effect.gen(function* () {
          const receipt = yield* submit();
          yield* Deferred.await(commit);
          return receipt;
        }),
      onOutcome: () =>
        Deferred.succeed(hookEntered, undefined).pipe(Effect.andThen(Deferred.await(publish))),
    });
    const fiber = yield* transport.request("turn/start", {}, "selected").pipe(
      Effect.tap(() => Deferred.succeed(completed, undefined)),
      Effect.forkScoped,
    );
    yield* Queue.take(io.output);
    yield* Queue.offer(
      io.input,
      encoder.encode(
        encodeUnknownJsonString({ id: 1, result: { accepted: true } }) +
          "\n" +
          encodeUnknownJsonString({ method: "fixture/response-routed" }) +
          "\n",
      ),
    );
    yield* Deferred.await(responseObserved);
    assert.isFalse(yield* Deferred.isDone(completed));
    yield* Deferred.succeed(commit, undefined);
    yield* Deferred.await(hookEntered);
    assert.isFalse(yield* Deferred.isDone(completed));
    yield* Deferred.succeed(publish, undefined);
    assert.deepStrictEqual(yield* Fiber.join(fiber), { accepted: true });
  }),
);

type ControlledPeerOverrides = Partial<
  CodexProtocol.CodexAppServerControlledProtocolOptions<CodexError.CodexAppServerFrameSubmissionError>
>;
const makeControlledPeer = Effect.fn("test.makeControlledPeer")(function* (
  overrides: ControlledPeerOverrides = {},
) {
  const io = yield* makeInMemoryStdio();
  const frames = yield* Queue.unbounded<CodexProtocol.CodexAppServerControlledFrame>();
  const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
  const { wrapper, onOutcome, ...other } = overrides;
  const transport = yield* CodexProtocol.makeControlledCodexAppServerProtocol({
    stdio: io.stdio,
    ...other,
    wrapper: (frame, submit) =>
      Queue.offer(frames, frame).pipe(
        Effect.andThen(Effect.suspend(() => (wrapper ? wrapper(frame, submit) : submit()))),
      ),
    onOutcome: (outcome) =>
      Queue.offer(outcomes, outcome).pipe(
        Effect.andThen(Effect.suspend(() => (onOutcome ? onOutcome(outcome) : Effect.void))),
        Effect.asVoid,
      ),
  });
  return { ...io, transport, frames, outcomes };
});

it.effect("controlled protocol serializes all output methods on one FIFO", () =>
  Effect.gen(function* () {
    const peer = yield* makeControlledPeer();
    const request = yield* peer.transport
      .request("first", {}, "request-selection")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    const notification = yield* peer.transport
      .notify("second", {}, "notify-selection")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    const response = yield* peer.transport
      .respond("server-ok", { ok: true }, "response-selection")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    const failure = yield* peer.transport
      .respondError(
        "server-error",
        CodexError.CodexAppServerRequestError.internalError(),
        "error-selection",
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    const written = yield* Queue.takeN(peer.output, 4);
    assert.deepStrictEqual(
      written.map((text) => JSON.parse(text).method ?? JSON.parse(text).id),
      ["first", "second", "server-ok", "server-error"],
    );
    const frames = yield* Queue.takeN(peer.frames, 4);
    assert.deepStrictEqual(
      frames.map((frame) => frame.selection),
      ["request-selection", "notify-selection", "response-selection", "error-selection"],
    );
    assert.isTrue(written.every((text) => !text.includes("selection")));
    yield* Fiber.join(notification);
    yield* Fiber.join(response);
    yield* Fiber.join(failure);
    yield* Queue.offer(peer.input, encodeJsonl({ id: 1, result: "finished" }));
    assert.strictEqual(yield* Fiber.join(request), "finished");
  }),
);

it.effect.each([false, true])(
  "controlled protocol publishes early protocol error only after hook with error=%s",
  (errorReply) =>
    Effect.gen(function* () {
      const hook = yield* Deferred.make<void>();
      const received = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      const peer = yield* makeControlledPeer({
        onOutcome: () => Deferred.await(hook),
        onNotification: () => Deferred.succeed(received, undefined).pipe(Effect.asVoid),
      });
      const fiber = yield* Effect.result(peer.transport.request("early", {}, "selected")).pipe(
        Effect.tap(() => Deferred.succeed(completed, undefined)),
        Effect.forkScoped,
      );
      yield* Queue.take(peer.output);
      const reply = errorReply
        ? { id: 1, error: { code: -32001, message: "peer error" } }
        : { id: 1, result: "reply" };
      yield* Queue.offer(
        peer.input,
        encoder.encode(
          encodeUnknownJsonString(reply) +
            "\n" +
            encodeUnknownJsonString({ method: "fixture/routed" }) +
            "\n",
        ),
      );
      yield* Deferred.await(received);
      try {
        assert.isFalse(yield* Deferred.isDone(completed));
      } finally {
        yield* Deferred.succeed(hook, undefined);
      }
      const result = yield* Fiber.join(fiber);
      assert.strictEqual(result._tag, errorReply ? "Failure" : "Success");
      if (errorReply && result._tag === "Failure")
        assert.isTrue(isCodexRequestFailure(result.failure));
      if (!errorReply && result._tag === "Success") assert.strictEqual(result.success, "reply");
    }),
);

it.effect(
  "controlled protocol unknown blocks later output and preserves earlier replies and late input",
  () =>
    Effect.gen(function* () {
      const failSecond = yield* Deferred.make<void>();
      const late = yield* Deferred.make<void>();
      let terminated = 0;
      const peer = yield* makeControlledPeer({
        wrapper: (frame, submit) =>
          Effect.gen(function* () {
            const receipt = yield* submit();
            if (frame.method === "second") {
              yield* Deferred.await(failSecond);
              return yield* new CodexError.CodexAppServerFrameSubmissionError({
                outcome: "unknown",
                reason: "wrapper",
              });
            }
            return receipt;
          }),
        onNotification: () => Deferred.succeed(late, undefined).pipe(Effect.asVoid),
        onTermination: () =>
          Effect.sync(() => {
            terminated++;
          }),
      });
      const first = yield* peer.transport
        .request("first")
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Queue.take(peer.output);
      yield* Queue.take(peer.outcomes);
      const second = yield* Effect.result(peer.transport.request("second")).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Queue.take(peer.output);
      const third = yield* Effect.result(peer.transport.notify("third")).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Deferred.succeed(failSecond, undefined);
      const failed = yield* Fiber.join(second);
      assert.strictEqual(failed._tag, "Failure");
      if (failed._tag === "Failure") assert.isTrue(isControlledFrameFailure(failed.failure));
      if (failed._tag === "Failure" && isControlledFrameFailure(failed.failure))
        assert.strictEqual(failed.failure.outcome, "unknown");
      const refused = yield* Fiber.join(third);
      assert.strictEqual(refused._tag, "Failure");
      if (refused._tag === "Failure") assert.isTrue(isControlledFrameFailure(refused.failure));
      if (refused._tag === "Failure" && isControlledFrameFailure(refused.failure))
        assert.strictEqual(refused.failure.outcome, "not_submitted");
      for (const write of [
        peer.transport.request("never"),
        peer.transport.notify("never"),
        peer.transport.respond("server", {}),
        peer.transport.respondError(
          "server",
          CodexError.CodexAppServerRequestError.internalError(),
        ),
      ]) {
        const result = yield* Effect.result(write);
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") assert.isTrue(isControlledFrameFailure(result.failure));
        if (result._tag === "Failure" && isControlledFrameFailure(result.failure))
          assert.strictEqual(result.failure.reason, "output_blocked");
      }
      assert.strictEqual(yield* Queue.size(peer.output), 0);
      yield* Queue.offer(
        peer.input,
        encoder.encode(
          encodeUnknownJsonString({ id: 1, result: "late-reply" }) +
            "\n" +
            encodeUnknownJsonString({ method: "late-notification" }) +
            "\n",
        ),
      );
      assert.strictEqual(yield* Fiber.join(first), "late-reply");
      yield* Deferred.await(late);
      assert.strictEqual(terminated, 0);
    }),
);

it.effect("controlled protocol queued cancellation never writes and retains owned cleanup", () =>
  Effect.gen(function* () {
    const hold = yield* Deferred.make<void>();
    const peer = yield* makeControlledPeer({
      wrapper: (frame, submit) =>
        frame.method === "held" ? Deferred.await(hold).pipe(Effect.andThen(submit())) : submit(),
    });
    const first = yield* peer.transport
      .notify("held")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Queue.take(peer.frames);
    const queued = yield* peer.transport
      .request("cancelled")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Fiber.interrupt(queued);
    assert.strictEqual(yield* Queue.size(peer.outcomes), 0);
    assert.strictEqual(yield* Queue.size(peer.output), 0);
    yield* Deferred.succeed(hold, undefined);
    yield* Fiber.join(first);
    const outcomes = yield* Queue.takeN(peer.outcomes, 2);
    assert.deepStrictEqual(
      outcomes.map((outcome) => outcome.outcome),
      ["submitted", "not_submitted"],
    );
    assert.strictEqual(yield* Queue.take(peer.output), '{"method":"held"}\n');
    assert.strictEqual(yield* Queue.size(peer.output), 0);
    yield* peer.transport.notify("capacity-reused");
    assert.strictEqual(yield* Queue.take(peer.output), '{"method":"capacity-reused"}\n');
  }),
);

it.effect.each([false, true])(
  "controlled protocol active cancellation preserves uncertainty after attempted=%s",
  (attempted) =>
    Effect.gen(function* () {
      const hold = yield* Deferred.make<void>();
      const entered = yield* Deferred.make<void>();
      const peer = yield* makeControlledPeer({
        wrapper: (_frame, submit) =>
          Effect.gen(function* () {
            if (!attempted) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(hold);
            }
            const receipt = yield* submit();
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(hold);
            return receipt;
          }),
      });
      const fiber = yield* peer.transport.request("cancel-active").pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
      assert.strictEqual(
        (yield* Queue.take(peer.outcomes)).outcome,
        attempted ? "unknown" : "not_submitted",
      );
      assert.strictEqual(yield* Queue.size(peer.output), attempted ? 1 : 0);
    }),
);

it.effect("controlled protocol sequential owner close settles active and queued callers", () =>
  Effect.gen(function* () {
    const scope = yield* Effect.acquireRelease(Scope.make("sequential"), (scope) =>
      Scope.close(scope, Exit.void),
    );
    const hold = yield* Deferred.make<void>();
    let wrapperReleased = false;
    const peer = yield* makeControlledPeer({
      wrapper: (_frame, submit) =>
        Deferred.await(hold).pipe(
          Effect.andThen(submit()),
          Effect.ensuring(
            Effect.sync(() => {
              wrapperReleased = true;
            }),
          ),
        ),
      onOutcome: () =>
        Effect.sync(() => {
          assert.isTrue(wrapperReleased);
        }),
    }).pipe(Scope.provide(scope));
    const active = yield* peer.transport
      .request("active")
      .pipe(Effect.forkIn(scope, { startImmediately: true }));
    yield* Queue.take(peer.frames);
    const queued = yield* peer.transport
      .request("queued")
      .pipe(Effect.forkIn(scope, { startImmediately: true }));
    yield* Scope.close(scope, Exit.void);
    const outcomes = yield* Queue.takeN(peer.outcomes, 2);
    assert.isTrue(outcomes.every((outcome) => outcome.outcome === "not_submitted"));
    assert.strictEqual(yield* Queue.size(peer.output), 0);
    assert.isTrue(Exit.isFailure(yield* Fiber.await(active)));
    assert.isTrue(Exit.isFailure(yield* Fiber.await(queued)));
  }),
);

it.effect.each(["wrapper", "hook"] as const)(
  "controlled protocol retains canceled reservations until dequeue with %s held",
  (stage) =>
    Effect.gen(function* () {
      const hold = yield* Deferred.make<void>();
      const hookEntered = yield* Deferred.make<void>();
      const peer = yield* makeControlledPeer({
        wrapper: (frame, submit) =>
          frame.method === "held" && stage === "wrapper"
            ? Deferred.await(hold).pipe(Effect.andThen(submit()))
            : submit(),
        onOutcome: (outcome) =>
          outcome.frame.method === "held" && stage === "hook"
            ? Deferred.succeed(hookEntered, undefined).pipe(Effect.andThen(Deferred.await(hold)))
            : Effect.void,
      });
      const active = yield* peer.transport
        .notify("held")
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Queue.take(peer.frames);
      if (stage === "hook") yield* Deferred.await(hookEntered);
      const queued = [];
      for (let index = 0; index < 63; index++)
        queued.push(
          yield* peer.transport
            .notify("queued")
            .pipe(Effect.forkScoped({ startImmediately: true })),
        );
      const full = yield* Effect.result(peer.transport.notify("too-many"));
      assert.strictEqual(full._tag, "Failure");
      if (full._tag === "Failure") assert.isTrue(isControlledFrameFailure(full.failure));
      if (full._tag === "Failure" && isControlledFrameFailure(full.failure))
        assert.strictEqual(full.failure.reason, "queue_limit");
      yield* Fiber.interruptAll(queued);
      const stillFull = yield* Effect.result(peer.transport.notify("still-full"));
      assert.strictEqual(stillFull._tag, "Failure");
      assert.strictEqual(yield* Queue.size(peer.outcomes), stage === "hook" ? 1 : 0);
      yield* Deferred.succeed(hold, undefined);
      yield* Fiber.join(active);
      const settled = yield* Queue.takeN(peer.outcomes, 64);
      assert.strictEqual(
        settled.filter((outcome) => outcome.outcome === "not_submitted").length,
        63,
      );
      yield* peer.transport.notify("reused");
      assert.deepStrictEqual(yield* Queue.takeN(peer.output, 2), [
        '{"method":"held"}\n',
        '{"method":"reused"}\n',
      ]);
    }),
);

it.effect("controlled protocol caps encoded bytes independently of frame count", () =>
  Effect.gen(function* () {
    const hold = yield* Deferred.make<void>();
    const peer = yield* makeControlledPeer({
      wrapper: (_frame, submit) => Deferred.await(hold).pipe(Effect.andThen(submit())),
    });
    const header = '{"method":"large","params":""}\n';
    const payload = "x".repeat(1048576 - Buffer.byteLength(header));
    const calls = [];
    for (let index = 0; index < 16; index++)
      calls.push(
        yield* peer.transport
          .notify("large", payload)
          .pipe(Effect.forkScoped({ startImmediately: true })),
      );
    const rejected = yield* Effect.result(peer.transport.notify("large", payload));
    assert.strictEqual(rejected._tag, "Failure");
    if (rejected._tag === "Failure") assert.isTrue(isControlledFrameFailure(rejected.failure));
    if (rejected._tag === "Failure" && isControlledFrameFailure(rejected.failure))
      assert.strictEqual(rejected.failure.reason, "queue_limit");
    yield* Deferred.succeed(hold, undefined);
    yield* Fiber.joinAll(calls);
    assert.strictEqual((yield* Queue.takeN(peer.output, 16)).length, 16);
  }),
);

it.effect.each(["wrapper", "hook"] as const)(
  "controlled protocol refuses same-owner reentrancy from %s",
  (location) =>
    Effect.gen(function* () {
      let transport: CodexProtocol.CodexAppServerControlledProtocol | undefined;
      const rejected = yield* Queue.unbounded<string>();
      const recurse = Effect.fn("test.reenterControlledProtocol")(function* () {
        if (!transport) return yield* Effect.die("transport not initialized");
        const result = yield* Effect.result(transport.notify("recursive"));
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") assert.isTrue(isControlledFrameFailure(result.failure));
        if (result._tag === "Failure" && isControlledFrameFailure(result.failure))
          yield* Queue.offer(rejected, result.failure.reason);
      });
      const peer = yield* makeControlledPeer({
        wrapper: (_frame, submit) =>
          location === "wrapper" ? recurse().pipe(Effect.andThen(submit())) : submit(),
        onOutcome: () => (location === "hook" ? recurse() : Effect.void),
      });
      transport = peer.transport;
      yield* transport.notify("only-frame");
      assert.strictEqual(yield* Queue.take(rejected), "reentrant");
      assert.deepStrictEqual(yield* Queue.take(peer.output), '{"method":"only-frame"}\n');
      assert.strictEqual(yield* Queue.size(peer.output), 0);
    }),
);

it.effect(
  "controlled protocol classifier defect still joins active execution and hook before close",
  () =>
    Effect.gen(function* () {
      const scope = yield* Effect.acquireRelease(Scope.make("sequential"), (scope) =>
        Scope.close(scope, Exit.void),
      );
      const wrapperEntered = yield* Deferred.make<void>();
      const releaseWrapper = yield* Deferred.make<void>();
      const hookEntered = yield* Deferred.make<void>();
      const releaseHook = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      const terminations = yield* Queue.unbounded<CodexError.CodexAppServerError>();
      const sentinel = new Error("fixture classifier failed");
      const peer = yield* makeControlledPeer({
        terminationError: Effect.die(sentinel),
        wrapper: (_frame, submit) =>
          Effect.gen(function* () {
            const receipt = yield* submit();
            yield* Deferred.succeed(wrapperEntered, undefined);
            yield* Deferred.await(releaseWrapper);
            return receipt;
          }),
        onOutcome: () =>
          Deferred.succeed(hookEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseHook)),
          ),
        onTermination: (error) => Queue.offer(terminations, error).pipe(Effect.asVoid),
      }).pipe(Scope.provide(scope));
      const request = yield* peer.transport.request("held").pipe(Effect.forkScoped);
      yield* Deferred.await(wrapperEntered);
      yield* Queue.end(peer.input);
      const closing = yield* Effect.exit(Scope.close(scope, Exit.void)).pipe(
        Effect.tap(() => Deferred.succeed(closed, undefined)),
        Effect.forkScoped,
      );
      const first = yield* Effect.raceFirst(
        Deferred.await(hookEntered).pipe(Effect.as("hook")),
        Deferred.await(closed).pipe(Effect.as("closed")),
      );
      try {
        assert.strictEqual(first, "hook");
        assert.isFalse(yield* Deferred.isDone(closed));
      } finally {
        yield* Deferred.succeed(releaseWrapper, undefined);
        yield* Deferred.succeed(releaseHook, undefined);
        yield* Fiber.interrupt(request);
      }
      assert.isTrue(Exit.isSuccess(yield* Fiber.join(closing)));
      const error = yield* Queue.take(terminations);
      assert.isTrue(isCodexTransportFailure(error));
      if (isCodexTransportFailure(error)) {
        assert.strictEqual(error.operation, "read-process-exit-status");
        assert.isTrue(Cause.isCause(error.cause));
        if (Cause.isCause(error.cause)) {
          const defect = Cause.findDefect(error.cause);
          assert.strictEqual(defect._tag, "Success");
          if (defect._tag === "Success") assert.strictEqual(defect.success, sentinel);
        }
      }
    }),
);

it.effect.each(["commit", "hook"] as const)(
  "controlled protocol local %s failure wins over an early successful reply",
  (stage) =>
    Effect.gen(function* () {
      const scope = yield* Effect.acquireRelease(Scope.make("sequential"), (scope) =>
        Scope.close(scope, Exit.void),
      );
      const release = yield* Deferred.make<void>();
      const received = yield* Deferred.make<void>();
      const peer = yield* makeControlledPeer({
        wrapper: (_frame, submit) =>
          Effect.gen(function* () {
            const receipt = yield* submit();
            yield* Deferred.await(release);
            if (stage === "commit")
              return yield* new CodexError.CodexAppServerFrameSubmissionError({
                outcome: "unknown",
                reason: "wrapper",
              });
            return receipt;
          }),
        onOutcome: () => (stage === "hook" ? Effect.die("fixture hook failure") : Effect.void),
        onNotification: () => Deferred.succeed(received, undefined).pipe(Effect.asVoid),
      }).pipe(Scope.provide(scope));
      const request = yield* Effect.result(peer.transport.request("early")).pipe(Effect.forkScoped);
      yield* Queue.take(peer.output);
      yield* Queue.offer(
        peer.input,
        encoder.encode(
          encodeUnknownJsonString({ id: 1, result: "must-not-publish" }) +
            "\n" +
            encodeUnknownJsonString({ method: "routed" }) +
            "\n",
        ),
      );
      yield* Deferred.await(received);
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(request);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.isTrue(isControlledFrameFailure(result.failure));
        if (isControlledFrameFailure(result.failure)) {
          assert.strictEqual(result.failure.outcome, stage === "commit" ? "unknown" : "submitted");
          assert.strictEqual(
            result.failure.reason,
            stage === "commit" ? "wrapper" : "outcome_hook",
          );
        }
      }
      const closed = yield* Effect.exit(Scope.close(scope, Exit.void));
      assert.strictEqual(closed._tag, stage === "hook" ? "Failure" : "Success");
    }),
);

it.effect("controlled protocol preserves first reply across duplicate input and hook barrier", () =>
  Effect.gen(function* () {
    const release = yield* Deferred.make<void>();
    const received = yield* Deferred.make<void>();
    const peer = yield* makeControlledPeer({
      onOutcome: () => Deferred.await(release),
      onNotification: () => Deferred.succeed(received, undefined).pipe(Effect.asVoid),
    });
    const request = yield* peer.transport.request("duplicate").pipe(Effect.forkScoped);
    yield* Queue.take(peer.output);
    yield* Queue.offer(
      peer.input,
      encoder.encode(
        encodeUnknownJsonString({ id: 1, result: "first" }) +
          "\n" +
          encodeUnknownJsonString({ id: 1, error: { code: -32603, message: "duplicate error" } }) +
          "\n" +
          encodeUnknownJsonString({ method: "routed" }) +
          "\n",
      ),
    );
    yield* Deferred.await(received);
    yield* Deferred.succeed(release, undefined);
    assert.strictEqual(yield* Fiber.join(request), "first");
    yield* Queue.offer(peer.input, encodeJsonl({ id: 1, result: "third" }));
  }),
);

it.effect("controlled protocol expected prewrite denial permits the next frame", () =>
  Effect.gen(function* () {
    const peer = yield* makeControlledPeer({
      wrapper: (frame, submit) =>
        frame.method === "deny"
          ? Effect.fail(
              new CodexError.CodexAppServerFrameSubmissionError({
                outcome: "not_submitted",
                reason: "wrapper",
              }),
            )
          : submit(),
    });
    const denied = yield* Effect.result(peer.transport.notify("deny"));
    assert.strictEqual(denied._tag, "Failure");
    assert.strictEqual((yield* Queue.take(peer.outcomes)).outcome, "not_submitted");
    yield* peer.transport.notify("allowed");
    assert.strictEqual((yield* Queue.take(peer.outcomes)).outcome, "submitted");
    assert.deepStrictEqual(yield* Queue.take(peer.output), '{"method":"allowed"}\n');
    assert.strictEqual(yield* Queue.size(peer.output), 0);
  }),
);

it.effect("controlled protocol response-wait cancellation does not rewrite submitted outcome", () =>
  Effect.gen(function* () {
    const received = yield* Deferred.make<void>();
    const peer = yield* makeControlledPeer({
      onNotification: () => Deferred.succeed(received, undefined).pipe(Effect.asVoid),
    });
    const request = yield* peer.transport.request("sent").pipe(Effect.forkScoped);
    yield* Queue.take(peer.output);
    const outcome = yield* Queue.take(peer.outcomes);
    assert.strictEqual(outcome.outcome, "submitted");
    yield* Fiber.interrupt(request);
    yield* Queue.offer(
      peer.input,
      encoder.encode(
        encodeUnknownJsonString({ id: 1, result: "late" }) +
          "\n" +
          encodeUnknownJsonString({ method: "after-late" }) +
          "\n",
      ),
    );
    yield* Deferred.await(received);
    assert.strictEqual(yield* Queue.size(peer.outcomes), 0);
    yield* peer.transport.notify("still-usable");
    assert.strictEqual(yield* Queue.take(peer.output), '{"method":"still-usable"}\n');
  }),
);

it.effect("controlled protocol actual non-ending Writable enforces serial drain", () =>
  Effect.gen(function* () {
    const io = yield* makeControlledFrameWritable(true);
    const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
    const transport = yield* CodexProtocol.makeControlledCodexAppServerProtocol({
      stdio: io.stdio,
      wrapper: (_frame, submit) => submit(),
      onOutcome: (outcome) => Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
    });
    const first = yield* transport
      .notify("first")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    assert.strictEqual(yield* Queue.take(io.writes), '{"method":"first"}\n');
    const second = yield* transport
      .notify("second")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    assert.strictEqual(yield* Queue.size(io.writes), 0);
    assert.strictEqual(yield* Queue.size(outcomes), 0);
    const releaseFirst = yield* Queue.take(io.callbacks);
    yield* Effect.sync(releaseFirst);
    yield* Fiber.join(first);
    assert.strictEqual(yield* Queue.take(io.writes), '{"method":"second"}\n');
    const releaseSecond = yield* Queue.take(io.callbacks);
    yield* Effect.sync(releaseSecond);
    yield* Fiber.join(second);
    assert.strictEqual((yield* Queue.takeN(outcomes, 2)).length, 2);
    assert.isFalse(io.writable.writableEnded);
    assert.isFalse(io.writable.destroyed);
  }),
);

it.effect.each(["eof", "malformed"] as const)(
  "controlled protocol %s termination joins queued work without self-join",
  (kind) =>
    Effect.gen(function* () {
      const hold = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<CodexError.CodexAppServerError>();
      const peer = yield* makeControlledPeer({
        wrapper: (_frame, submit) => Deferred.await(hold).pipe(Effect.andThen(submit())),
        onTermination: (error) => Deferred.succeed(stopped, error).pipe(Effect.asVoid),
      });
      const active = yield* Effect.result(peer.transport.request("active")).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Queue.take(peer.frames);
      const queued = yield* Effect.result(peer.transport.notify("queued")).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      if (kind === "eof") yield* Queue.end(peer.input);
      else yield* Queue.offer(peer.input, encoder.encode("{bad}\n"));
      const error = yield* Deferred.await(stopped);
      assert.strictEqual(
        error._tag,
        kind === "eof" ? "CodexAppServerInputStreamEndedError" : "CodexAppServerProtocolParseError",
      );
      assert.strictEqual((yield* Fiber.join(active))._tag, "Failure");
      assert.strictEqual((yield* Fiber.join(queued))._tag, "Failure");
      assert.isTrue(
        (yield* Queue.takeN(peer.outcomes, 2)).every(
          (outcome) => outcome.outcome === "not_submitted",
        ),
      );
      assert.strictEqual(yield* Queue.size(peer.output), 0);
    }),
);

it.effect.each(["selection", "oversize"] as const)(
  "controlled protocol rejects %s before admission without a hook",
  (kind) =>
    Effect.gen(function* () {
      const peer = yield* makeControlledPeer();
      const result = yield* Effect.result(
        peer.transport.notify(
          "invalid",
          kind === "oversize" ? "x".repeat(1048576) : {},
          kind === "selection" ? " " : undefined,
        ),
      );
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.isTrue(isControlledFrameFailure(result.failure));
        if (isControlledFrameFailure(result.failure))
          assert.strictEqual(
            result.failure.reason,
            kind === "oversize" ? "frame_limit" : "invalid_frame",
          );
      }
      assert.strictEqual(yield* Queue.size(peer.frames), 0);
      assert.strictEqual(yield* Queue.size(peer.outcomes), 0);
      assert.strictEqual(yield* Queue.size(peer.output), 0);
    }),
);
