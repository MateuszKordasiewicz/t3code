import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Duration from "effect/Duration";
import * as Result from "effect/Result";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, expect, it } from "@effect/vitest";

import * as CodexClient from "./client.ts";
import type * as CodexProtocol from "./protocol.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
import * as CodexError from "./errors.ts";

const mockPeerPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/codex-app-server-mock-peer.ts"),
);
const mockPeerArgs = (path: string) => [path];

it.layer(NodeServices.layer)("effect-codex-app-server client", (it) => {
  const makeHandle = (env?: Record<string, string>) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const path = yield* Path.Path;
      const peerCwd = path.join(import.meta.dirname, "..");
      const command = ChildProcess.make(process.execPath, mockPeerArgs(yield* mockPeerPath), {
        cwd: peerCwd,
        ...(env ? { env: { ...process.env, ...env } } : {}),
      });
      return yield* spawner.spawn(command);
    });

  it.effect("initializes, handles typed server requests, and reads account and skills data", () =>
    Effect.gen(function* () {
      const userInputRequests = yield* Ref.make<Array<unknown>>([]);
      const messageDeltas = yield* Ref.make<Array<unknown>>([]);
      const handle = yield* makeHandle();
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const result = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;

        yield* client.handleServerRequest("item/tool/requestUserInput", (payload) =>
          Ref.update(userInputRequests, (current) => [...current, payload]).pipe(
            Effect.as({
              answers: {
                approved: {
                  answers: ["yes"],
                },
              },
            }),
          ),
        );

        yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
          Ref.update(messageDeltas, (current) => [...current, payload]),
        );

        const initialized = yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
        assert.equal(initialized.userAgent, "mock-codex-app-server");

        yield* client.notify("initialized", undefined);

        const account = yield* client.request("account/read", {});
        assert.equal(account.requiresOpenaiAuth, false);
        assert.deepEqual(account.account, {
          type: "chatgpt",
          email: "mock@example.com",
          planType: "plus",
        });

        const path = yield* Path.Path;
        const peerCwd = path.join(import.meta.dirname, "..");
        const skills = yield* client.request("skills/list", { cwds: [peerCwd] });
        assert.equal(skills.data.length, 1);
        assert.equal(skills.data[0]?.cwd, peerCwd);

        return {
          account,
          skills,
        };
      }).pipe(Effect.provide(context), Effect.ensuring(Scope.close(scope, Exit.void)));

      assert.equal(result.skills.data[0]?.skills.length, 0);
      assert.deepEqual(yield* Ref.get(userInputRequests), [
        {
          isBlocking: true,
          itemId: "item-approval-1",
          threadId: "thread-1",
          turnId: "turn-1",
          questions: [
            {
              id: "approved",
              header: "Approve",
              question: "Continue with the mock skills request?",
              options: [
                {
                  label: "yes",
                  description: "Approve the request",
                },
              ],
            },
          ],
        },
      ]);
      assert.deepEqual(yield* Ref.get(messageDeltas), [
        {
          delta: "Mock server is ready.",
          itemId: "item-1",
          threadId: "thread-1",
          turnId: "turn-1",
        },
      ]);
    }),
  );
  it.effect("drains child stderr so large diagnostics cannot block protocol responses", () =>
    Effect.gen(function* () {
      const handle = yield* makeHandle({
        CODEX_APP_SERVER_TEST_STDERR_BYTES: String(512 * 1024),
      });
      const scope = yield* Scope.make();
      const layerClient = CodexClient.layerChildProcess(handle);
      const context = yield* Layer.buildWithScope(layerClient, scope);

      const initialized = yield* Effect.gen(function* () {
        const client = yield* CodexClient.CodexAppServerClient;
        return yield* client.request("initialize", {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        });
      }).pipe(
        Effect.timeout("5 seconds"),
        Effect.provide(context),
        Effect.ensuring(Scope.close(scope, Exit.void)),
      );

      assert.equal(initialized.userAgent, "mock-codex-app-server");
    }),
  );

  it.effect("warns once about a notification that fails to decode and keeps dispatching", () =>
    Effect.gen(function* () {
      const secret = "codex-notification-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const warnings: Array<unknown> = [];
      const client = yield* CodexClient.make(stdio).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make(({ logLevel, fiber }) => {
              if (logLevel === "Warn")
                warnings.push(fiber.getRef(References.CurrentLogAnnotations));
            }),
          ]),
        ),
      );
      const delivered = yield* Deferred.make<unknown>();
      yield* client.handleServerNotification("item/agentMessage/delta", (payload) =>
        Deferred.succeed(delivered, payload).pipe(Effect.asVoid),
      );

      const send = (params: unknown) =>
        Queue.offer(
          input,
          new TextEncoder().encode(
            `${JSON.stringify({ method: "item/agentMessage/delta", params })}\n`,
          ),
        );
      const malformed = { itemId: "item-1", threadId: "thread-1", turnId: secret };
      yield* send(malformed);
      yield* send(malformed);
      yield* send({ delta: "ok", itemId: "item-1", threadId: "thread-1", turnId: "turn-1" });

      assert.deepEqual(yield* Deferred.await(delivered), {
        delta: "ok",
        itemId: "item-1",
        threadId: "thread-1",
        turnId: "turn-1",
      });
      assert.lengthOf(warnings, 1);
      expect(warnings[0]).toMatchObject({
        operation: "decode-notification-payload",
        method: "item/agentMessage/delta",
        issueKinds: expect.arrayContaining(["MissingKey"]),
      });
      assert.notInclude(JSON.stringify(warnings), secret);
    }),
  );
});

it.live("reports the stderr error when the process exits before initialize", () =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner.spawn(
      ChildProcess.make(process.execPath, [
        "-e",
        'throw new Error("Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest")',
      ]),
    );
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(CodexClient.layerChildProcess(handle), scope);

    const error = yield* Effect.gen(function* () {
      const client = yield* CodexClient.CodexAppServerClient;
      return yield* client.request("initialize", {
        clientInfo: {
          name: "effect-codex-app-server-test",
          title: "Effect Codex App Server Test",
          version: "0.0.0",
        },
        capabilities: {
          experimentalApi: true,
          optOutNotificationMethods: null,
        },
      });
    }).pipe(Effect.flip, Effect.provide(context), Effect.ensuring(Scope.close(scope, Exit.void)));

    assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
    assert.equal(
      error.message,
      "Codex App Server process exited with code 1: Error: Missing optional dependency @openai/codex-darwin-arm64. Reinstall Codex: npm install -g @openai/codex@latest",
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.live("owns non-ending child stdin through repeated selected typed and raw submissions", () =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const spawned = yield* Queue.unbounded<ChildProcessSpawner.ChildProcessHandle>();
    const frames = yield* Queue.unbounded<CodexProtocol.CodexAppServerControlledFrame>();
    const notification = yield* Deferred.make<void>();
    const requests = yield* Queue.unbounded<unknown>();
    const expectedPeer = yield* mockPeerPath;
    const recording = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        assert.strictEqual(command._tag, "StandardCommand");
        if (command._tag !== "StandardCommand")
          return yield* Effect.die("unexpected piped fixture command");
        assert.deepStrictEqual(command.options.stdin, { stream: "pipe", endOnDone: false });
        assert.strictEqual(command.options.stdout, "pipe");
        assert.strictEqual(command.options.stderr, "pipe");
        assert.deepStrictEqual(command.args, [expectedPeer]);
        assert.strictEqual(command.options.env?.CODEX_APP_SERVER_TEST_STDERR_BYTES, "524288");
        assert.strictEqual(
          Duration.toMillis(Duration.fromInputUnsafe(command.options.forceKillAfter!)),
          2000,
        );
        const handle = yield* spawner.spawn(command);
        yield* Queue.offer(spawned, handle);
        return handle;
      }),
    );
    const scope = yield* Scope.make("sequential");
    const path = yield* Path.Path;
    const args = [yield* mockPeerPath];
    const env = { CODEX_APP_SERVER_TEST_STDERR_BYTES: "524288" };
    const forceKillAfter: [number, number] = [2, 0];
    const command = ChildProcess.make(process.execPath, args, {
      cwd: path.join(import.meta.dirname, ".."),
      env,
      forceKillAfter,
    });
    const options: CodexClient.CodexAppServerControlledClientOptions<CodexError.CodexAppServerFrameSubmissionError> =
      {
        wrapper: (frame, submit) => Queue.offer(frames, frame).pipe(Effect.andThen(submit())),
        onOutcome: () => Effect.void,
      };
    const pendingClient = CodexClient.makeControlledChildProcessClient(command, options);
    args[0] = "changed-after-construction";
    env.CODEX_APP_SERVER_TEST_STDERR_BYTES = "0";
    forceKillAfter[0] = 99;
    const service = yield* pendingClient.pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, recording),
      Scope.provide(scope),
    );
    const handle = yield* Queue.take(spawned);
    yield* Effect.gen(function* () {
      const client = yield* CodexClient.CodexAppServerControlledClient;
      assert.isDefined(client.ownedProcess);
      assert.deepStrictEqual(Object.keys(client.ownedProcess).toSorted(), [
        "awaitExit",
        "isRunning",
        "pid",
      ]);
      assert.isTrue(Object.isFrozen(client.ownedProcess));
      assert.strictEqual(client.ownedProcess.pid, handle.pid);
      assert.isTrue(yield* client.ownedProcess.isRunning);
      yield* client.handleServerNotification("item/agentMessage/delta", () =>
        Deferred.succeed(notification, undefined).pipe(Effect.asVoid),
      );
      yield* client.handleServerRequest("item/tool/requestUserInput", (payload) =>
        Queue.offer(requests, payload).pipe(
          Effect.as({ answers: { approved: { answers: ["yes"] } } }),
        ),
      );
      const payload = {
        clientInfo: { name: "controlled-test", title: "controlled", version: "1" },
        capabilities: { experimentalApi: true, optOutNotificationMethods: null },
      };
      const initialize = client.request("initialize", payload, "initialize-selection");
      const rawPayload = { nested: { value: "original" } };
      const rawAccount = client.raw.request("account/read", rawPayload, "raw-selection");
      payload.clientInfo.name = "mutated-after-call";
      rawPayload.nested.value = "mutated-after-call";
      const [result, raw] = yield* Effect.all([initialize, rawAccount], {
        concurrency: "unbounded",
      });
      assert.strictEqual(result.userAgent, "mock-codex-app-server");
      assert.isObject(raw);
      yield* client.notify("initialized", undefined, "notification-selection");
      yield* Deferred.await(notification);
      const account = yield* client.request("account/read", {}, "typed-selection");
      assert.isFalse(account.requiresOpenaiAuth);
      const skills = yield* client.request(
        "skills/list",
        { cwds: ["/fixture"] },
        "skills-selection",
      );
      assert.lengthOf(skills.data, 1);
      const approval = yield* Queue.take(requests);
      expect(approval).toMatchObject({ threadId: "thread-1", turnId: "turn-1" });
      const observed = yield* Queue.takeN(frames, 6);
      assert.deepStrictEqual(
        observed.map((frame) => frame.selection),
        [
          "initialize-selection",
          "raw-selection",
          "notification-selection",
          "typed-selection",
          "skills-selection",
          null,
        ],
      );
      assert.isTrue(observed.every((frame) => !frame.encoded.includes("selection")));
      expect(JSON.parse(observed[0]!.encoded)).toMatchObject({
        params: { clientInfo: { name: "controlled-test" } },
      });
      expect(JSON.parse(observed[1]!.encoded)).toMatchObject({
        params: { nested: { value: "original" } },
      });
      const invalid = yield* client
        .request("initialize", {
          // @ts-expect-error A remote caller can send a clientInfo without its required name.
          clientInfo: { version: "1" },
        })
        .pipe(Effect.flip);
      assert.instanceOf(invalid, CodexError.CodexAppServerRequestError);
      // @ts-expect-error The no-payload notification must also reject untyped callers.
      const unexpected = yield* client.notify("initialized", {}).pipe(Effect.flip);
      assert.instanceOf(unexpected, CodexError.CodexAppServerRequestError);
      assert.strictEqual(yield* Queue.size(frames), 0);
      yield* client.quiesceOutput;
      yield* client.quiesceOutput;
      assert.isTrue(yield* handle.isRunning);
      assert.isTrue(yield* client.ownedProcess.isRunning);
      const refused = yield* client.request("account/read", {}).pipe(Effect.flip);
      assert.instanceOf(refused, CodexError.CodexAppServerFrameSubmissionError);
      assert.strictEqual(yield* Queue.size(frames), 0);
    }).pipe(
      Effect.provideService(CodexClient.CodexAppServerControlledClient, service),
      Effect.ensuring(Scope.close(scope, Exit.void)),
    );
    const exited = yield* Effect.result(handle.exitCode);
    if (Result.isFailure(exited)) assert.include(String(exited.failure.cause), "SIGTERM");
    else assert.strictEqual(exited.success, 0);
    assert.isFalse(yield* handle.isRunning);
    assert.isFalse(yield* service.ownedProcess.isRunning);
    const observedExit = yield* Effect.result(service.ownedProcess.awaitExit);
    assert.strictEqual(observedExit._tag, exited._tag);
    if (Result.isSuccess(observedExit) && Result.isSuccess(exited))
      assert.strictEqual(observedExit.success, exited.success);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("rejects unsupported controlled commands before spawning", () =>
  Effect.gen(function* () {
    const commands = [
      ChildProcess.make("left").pipe(ChildProcess.pipeTo(ChildProcess.make("right"))),
      ChildProcess.make("unused", [], { additionalFds: { fd3: { type: "output" } } }),
    ];
    let spawned = 0;
    const spawner = ChildProcessSpawner.make(() => {
      spawned++;
      return Effect.die("unsupported command reached spawner");
    });
    for (const command of commands) {
      const error = yield* CodexClient.makeControlledChildProcessClient(command, {
        wrapper: (_frame, submit) => submit(),
        onOutcome: () => Effect.void,
      }).pipe(Effect.flip, Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      assert.instanceOf(error, CodexError.CodexAppServerSpawnError);
    }
    assert.strictEqual(spawned, 0);
  }).pipe(Effect.scoped),
);

it.live("preserves controlled child spawn and stderr exit diagnostics", () =>
  Effect.gen(function* () {
    const options: CodexClient.CodexAppServerControlledClientOptions<CodexError.CodexAppServerFrameSubmissionError> =
      {
        wrapper: (_frame, submit) => submit(),
        onOutcome: () => Effect.void,
      };
    const missing = yield* CodexClient.makeControlledChildProcessClient(
      ChildProcess.make("/nonexistent-t3-controlled-client-executable"),
      options,
    ).pipe(Effect.flip);
    assert.instanceOf(missing, CodexError.CodexAppServerSpawnError);
    const scope = yield* Scope.make("sequential");
    const client = yield* CodexClient.makeControlledChildProcessClient(
      ChildProcess.make(
        process.execPath,
        ["-e", 'throw new Error("controlled-child-diagnostic")'],
        { forceKillAfter: "2 seconds" },
      ),
      options,
    ).pipe(Scope.provide(scope));
    const error = yield* client
      .request("account/read", {})
      .pipe(Effect.flip, Effect.ensuring(Scope.close(scope, Exit.void)));
    assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
    assert.include(error.message, "controlled-child-diagnostic");
    assert.strictEqual(error._tag === "CodexAppServerProcessExitedError" && error.code, 1);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.live.each(["denied", "unknown"] as const)(
  "controlled typed calls propagate %s local submission without publishing a reply",
  (stage) =>
    Effect.gen(function* () {
      const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
      const scope = yield* Scope.make("sequential");
      const client = yield* CodexClient.makeControlledChildProcessClient(
        ChildProcess.make(process.execPath, [yield* mockPeerPath], { forceKillAfter: "2 seconds" }),
        {
          wrapper: (_frame, submit) =>
            stage === "denied"
              ? Effect.fail("denied")
              : submit().pipe(Effect.andThen(Effect.fail("post-submit-failure"))),
          onOutcome: (outcome) => Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
        },
      ).pipe(Scope.provide(scope));
      const error = yield* client
        .request("account/read", {}, "selected-call")
        .pipe(Effect.flip, Effect.ensuring(Scope.close(scope, Exit.void)));
      assert.instanceOf(error, CodexError.CodexAppServerFrameSubmissionError);
      if (error._tag === "CodexAppServerFrameSubmissionError")
        assert.strictEqual(error.outcome, stage === "denied" ? "not_submitted" : "unknown");
      const outcome = yield* Queue.take(outcomes);
      assert.strictEqual(outcome.frame.selection, "selected-call");
      assert.strictEqual(outcome.outcome, stage === "denied" ? "not_submitted" : "unknown");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.live("failed controlled construction closes its already spawned child", () =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const spawned = yield* Queue.unbounded<ChildProcessSpawner.ChildProcessHandle>();
    const recording = ChildProcessSpawner.make((command) =>
      spawner.spawn(command).pipe(Effect.tap((handle) => Queue.offer(spawned, handle))),
    );
    const error = yield* CodexClient.makeControlledChildProcessClient(
      ChildProcess.make(process.execPath, [yield* mockPeerPath], { forceKillAfter: "2 seconds" }),
      {
        wrapper: (_frame, submit) => submit(),
        onOutcome: () => Effect.void,
        testSinkTimeoutMs: 0,
      },
    ).pipe(Effect.flip, Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, recording));
    assert.instanceOf(error, CodexError.CodexAppServerFrameSubmissionError);
    const handle = yield* Queue.take(spawned);
    yield* Effect.exit(handle.exitCode);
    assert.isFalse(yield* handle.isRunning);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["millis", "nanos"] as const)(
  "owns the %s duration object before controlled construction runs",
  (representation) =>
    Effect.gen(function* () {
      const input =
        representation === "millis" ? Duration.millis(2000) : Duration.nanos(2000000001n);
      const expected = Duration.toNanosUnsafe(input);
      const commands = yield* Queue.unbounded<ChildProcess.Command>();
      const spawner = ChildProcessSpawner.make((command) =>
        Queue.offer(commands, command).pipe(
          Effect.andThen(Effect.die("fixture stops at owned spawn boundary")),
        ),
      );
      const pending = CodexClient.makeControlledChildProcessClient(
        ChildProcess.make("not-spawned", [], { forceKillAfter: input }),
        {
          wrapper: (_frame, submit) => submit(),
          onOutcome: () => Effect.void,
        },
      );
      if (input.value._tag === "Millis") input.value.millis = 9000;
      if (input.value._tag === "Nanos") input.value.nanos = 9000000001n;
      yield* Effect.exit(pending).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const command = yield* Queue.take(commands);
      assert.strictEqual(command._tag, "StandardCommand");
      if (command._tag === "StandardCommand")
        assert.strictEqual(
          Duration.toNanosUnsafe(Duration.fromInputUnsafe(command.options.forceKillAfter!)),
          expected,
        );
    }).pipe(Effect.scoped),
);
