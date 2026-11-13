// @effect-diagnostics nodeBuiltinImport:off -- fake systemd binaries live in a temp dir.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as FileSystem from "effect/FileSystem";
import * as PlatformError from "effect/PlatformError";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../config.ts";
import { makeCodexAppServerSpawnCommand } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProcessRunner from "../processRunner.ts";
import { classifyScope, make, ownedAgentSlice, type ScopeState } from "./agentScope.ts";

const running: ScopeState = {
  loadState: "loaded",
  activeState: "active",
  result: "success",
  oomKills: 0,
  populated: true,
};

describe("classifyScope", () => {
  it("reports an OOM kill only once systemd records it", () => {
    expect(classifyScope({ ...running, activeState: "failed", result: "oom-kill" })).toBe(
      "oom-killed",
    );
    expect(classifyScope({ ...running, oomKills: 1 })).toBe("stopping");
    expect(classifyScope({ ...running, populated: false })).toBe("stopping");
    expect(classifyScope({ ...running, activeState: "deactivating" })).toBe("stopping");
  });
});

interface FakeUnit {
  activeState: string;
  result: string;
  controlGroup?: string;
  invocationId?: string;
  subState?: string;
  id?: string;
}

const withFakeSystemd = <A, E>(
  body: (input: {
    readonly scope: Effect.Success<typeof make>;
    readonly units: Map<string, FakeUnit>;
    readonly calls: ReadonlyArray<ReadonlyArray<string>>;
    readonly stateDir: string;
    readonly cgroupRoot: string;
    readonly procRoot: string;
    readonly procReads: ReadonlyArray<string>;
  }) => Effect.Effect<A, E, Scope.Scope>,
  // A user manager older than systemd 253 rejects OOMPolicy on scopes.
  {
    oomPolicy = true,
    stopFails = false,
    transformBindingShow,
    beforeProcRead,
    beforeBindingShow,
    onBlockingStop,
    transformBlockingStop,
    beforeCgroupIO,
  }: {
    readonly oomPolicy?: boolean;
    readonly stopFails?: boolean;
    readonly transformBindingShow?: (
      output: ProcessRunner.ProcessRunOutput,
    ) => ProcessRunner.ProcessRunOutput;
    readonly beforeProcRead?: (file: string) => Effect.Effect<void, PlatformError.PlatformError>;
    readonly beforeBindingShow?: () => Effect.Effect<void, ProcessRunner.ProcessRunError>;
    readonly onBlockingStop?: (unit: string) => Effect.Effect<void, ProcessRunner.ProcessRunError>;
    readonly transformBlockingStop?: (
      output: ProcessRunner.ProcessRunOutput,
    ) => ProcessRunner.ProcessRunOutput;
    readonly beforeCgroupIO?: (file: string) => Effect.Effect<void, PlatformError.PlatformError>;
  } = {},
) =>
  Effect.gen(function* () {
    const bin = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agent-scope-"));
    for (const name of ["systemd-run", "systemctl", "agent"]) {
      NodeFS.writeFileSync(NodePath.join(bin, name), "", { mode: 0o755 });
    }
    const cgroupRoot = NodePath.join(bin, "cgroup");
    const procRoot = NodePath.join(bin, "proc");
    const procReads: Array<string> = [];
    const units = new Map<string, FakeUnit>();
    const calls: Array<ReadonlyArray<string>> = [];
    const output = (stdout: string, code = 0): ProcessRunner.ProcessRunOutput => ({
      stdout,
      stderr: "",
      code: ChildProcessSpawner.ExitCode(code),
      timedOut: false,
      stdoutTruncated: false,
      stderrTruncated: false,
      stdoutInvalidUtf8: false,
      stderrInvalidUtf8: false,
    });
    const runner = ProcessRunner.ProcessRunner.of({
      run: ({ args }) =>
        Effect.gen(function* () {
          calls.push(args);
          if (args[1] === "stop" && !args.includes("--no-block") && onBlockingStop)
            yield* onBlockingStop(args[2]!);
          if (beforeBindingShow && args.some((arg) => arg.includes("InvocationID")))
            yield* beforeBindingShow();
          return yield* Effect.sync(() => {
            if (!oomPolicy && args.some((arg) => arg.startsWith("--property=OOMPolicy="))) {
              return output("", 1);
            }
            const [, verb, unit = ""] = args;
            if (stopFails && verb === "stop") {
              return {
                ...output(""),
                stderr: `Failed to stop ${args.at(-1)}: Unit not loaded.`,
                code: ChildProcessSpawner.ExitCode(5),
              };
            }
            if (verb === "stop" && !args.includes("--no-block")) {
              const result = output("");
              return transformBlockingStop ? transformBlockingStop(result) : result;
            }
            if (verb === "reset-failed") units.delete(unit);
            if (verb !== "show") return output("");
            const state = units.get(unit);
            if (args.some((arg) => arg.includes("InvocationID"))) {
              const result = output(
                state === undefined
                  ? `Id=${unit}\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nInvocationID=\nControlGroup=\n`
                  : `Id=${state.id ?? unit}\nLoadState=loaded\nActiveState=${state.activeState}\nSubState=${state.subState ?? "running"}\nInvocationID=${state.invocationId ?? "11111111111111111111111111111111"}\nControlGroup=${state.controlGroup ?? ""}\n`,
              );
              return transformBindingShow ? transformBindingShow(result) : result;
            }
            return output(
              state === undefined
                ? "LoadState=not-found\nActiveState=inactive\nResult=success\n"
                : `LoadState=loaded\nActiveState=${state.activeState}\nResult=${state.result}\n` +
                    `ControlGroup=${state.controlGroup ?? ""}\n`,
            );
          });
        }),
    });
    const fileSystem = yield* FileSystem.FileSystem;
    const scope = yield* make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.provideService(HostProcess.Environment, { PATH: bin, XDG_RUNTIME_DIR: "/run/user/1" }),
      Effect.provideService(FileSystem.FileSystem, {
        ...fileSystem,
        stat: (file) =>
          (file.startsWith("/sys/fs/cgroup/") && beforeCgroupIO
            ? beforeCgroupIO(file)
            : Effect.void
          ).pipe(
            Effect.andThen(
              fileSystem.stat(
                file.replace("/sys/fs/cgroup", cgroupRoot).replace(/^\/proc(?=\/)/, procRoot),
              ),
            ),
          ),
        readFileString: (path, encoding) =>
          path.startsWith("/proc/")
            ? Effect.sync(() => {
                procReads.push(path);
              }).pipe(
                Effect.andThen(beforeProcRead ? beforeProcRead(path) : Effect.void),
                Effect.andThen(
                  fileSystem.readFileString(path.replace("/proc", procRoot), encoding),
                ),
              )
            : (path.startsWith("/sys/fs/cgroup/") && beforeCgroupIO
                ? beforeCgroupIO(path)
                : Effect.void
              ).pipe(
                Effect.andThen(
                  fileSystem.readFileString(path.replace("/sys/fs/cgroup", cgroupRoot), encoding),
                ),
              ),
        writeFileString: (path, data, options) =>
          fileSystem.writeFileString(path.replace("/sys/fs/cgroup", cgroupRoot), data, options),
      }),
    );
    const { stateDir } = yield* ServerConfig.ServerConfig;
    return yield* body({ scope, units, calls, stateDir, cgroupRoot, procRoot, procReads }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(bin, { recursive: true, force: true }))),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-agent-scope-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  );

const unitOf = (launch: Pick<ChildProcess.StandardCommand, "args">) =>
  launch.args
    .map(String)
    .find((arg) => arg.startsWith("--unit="))
    ?.slice("--unit=".length) ?? "";

const sliceOf = (launch: { readonly args: ReadonlyArray<string> }) =>
  launch.args.find((arg) => arg.startsWith("--slice="))?.slice("--slice=".length) ?? "";

/** An active scope as systemd and the kernel report it, with the agent itself gone. */
const exitedAgent = (
  units: Map<string, FakeUnit>,
  cgroupRoot: string,
  unit: string,
  { populated, oomKills }: { readonly populated: boolean; readonly oomKills: number | undefined },
) => {
  const controlGroup = `/${unit}`;
  NodeFS.mkdirSync(NodePath.join(cgroupRoot, controlGroup), { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(cgroupRoot, controlGroup, "cgroup.events"),
    `populated ${populated ? 1 : 0}\nfrozen 0\n`,
  );
  // An undefined count leaves memory.events unreadable.
  if (oomKills !== undefined) {
    NodeFS.writeFileSync(
      NodePath.join(cgroupRoot, controlGroup, "memory.events"),
      `low 0\nhigh 0\nmax 0\noom ${oomKills}\noom_kill ${oomKills}\noom_group_kill 0\n`,
    );
  }
  units.set(unit, { activeState: "active", result: "success", controlGroup });
};

const stopsOf = (calls: ReadonlyArray<ReadonlyArray<string>>) =>
  calls.filter(([, verb]) => verb === "stop");
const launchCodex = (scope: Effect.Success<typeof make>, threadId: string) =>
  makeCodexAppServerSpawnCommand({ command: "agent", args: ["app-server"], threadId }).pipe(
    Effect.provideService(AgentScope, scope),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.map((command) => {
      assert(ChildProcess.isStandardCommand(command));
      return command;
    }),
  );

describe("AgentScope service", () => {
  it.effect("labels a thread from its newest scope until its next session clears it", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const first = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(first), { activeState: "failed", result: "oom-kill" });
        const second = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(second), { activeState: "active", result: "success" });
        expect(yield* scope.oomKilled("a")).toBe(false);

        units.set(unitOf(second), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("a")).toBe(true);
        units.delete(unitOf(second));
        expect(yield* scope.oomKilled("a")).toBe(true);

        yield* scope.clear("a");
        expect(yield* scope.oomKilled("a")).toBe(false);
      }),
    ),
  );

  it.effect(
    "keeps shared Codex scopes running without attributing their OOM to the launch thread",
    () =>
      withFakeSystemd(({ scope, units, calls }) =>
        Effect.gen(function* () {
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          expect(agent.args).toContain("--property=OOMPolicy=stop");
          units.set(unitOf(agent), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(true);

          const codex = yield* launchCodex(scope, "a");
          expect(codex.args).toContain("--property=OOMPolicy=continue");
          expect(codex.args).not.toContain("--property=OOMPolicy=stop");
          expect(codex.args).toContain("--property=TimeoutStopSec=10s");
          expect(codex.args).toContain("--collect");
          units.set(unitOf(codex), { activeState: "active", result: "success" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          units.set(unitOf(codex), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          expect(
            calls.filter(([, verb, unit]) => verb === "show" && unit === unitOf(codex)),
          ).toEqual([]);
        }),
      ),
  );

  it.effect.each([{ oomPolicy: true }, { oomPolicy: false }])(
    "Force stop kills a thread's own scope but never a shared Codex scope (OOM policy $oomPolicy)",
    ({ oomPolicy }) =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "t",
              threadId: "a",
            });
            exitedAgent(units, cgroupRoot, unitOf(agent), { populated: true, oomKills: 0 });
            const codex = yield* launchCodex(scope, "b");
            exitedAgent(units, cgroupRoot, unitOf(codex), { populated: true, oomKills: 0 });

            expect(yield* scope.kill("a")).toBe(true);
            expect(
              NodeFS.readFileSync(NodePath.join(cgroupRoot, unitOf(agent), "cgroup.kill"), "utf8"),
            ).toBe("1");
            expect(yield* scope.kill("b")).toBe(false);
            expect(NodeFS.existsSync(NodePath.join(cgroupRoot, unitOf(codex), "cgroup.kill"))).toBe(
              false,
            );
            expect(calls.filter(([, verb]) => verb === "kill")).toEqual([]);
          }),
        { oomPolicy },
      ),
  );

  it.effect("keeps a running scope while hundreds of other threads launch", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const live = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "live" });
        units.set(unitOf(live), { activeState: "active", result: "success" });
        for (let index = 0; index < 600; index++) {
          yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: `other-${index}` });
        }
        units.set(unitOf(live), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("live")).toBe(true);
      }),
    ),
  );

  it.effect("delegates every agent scope so rootless containers can manage their cgroups", () =>
    withFakeSystemd(({ scope }) =>
      Effect.gen(function* () {
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const codex = yield* launchCodex(scope, "a");
        const terminal = yield* scope.wrap({
          command: "agent",
          args: [],
          name: "terminal",
          survivesRestart: true,
        });
        for (const launch of [agent, codex, terminal]) {
          expect(launch.args).toContain("--property=Delegate=yes");
        }
      }),
    ),
  );

  it.effect("caps agent memory and yields CPU, disk, and tasks to the server", () =>
    withFakeSystemd(({ scope, calls }) =>
      Effect.gen(function* () {
        yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const setProperties = calls.filter(([, verb]) => verb === "set-property");
        expect(setProperties).toEqual([
          [
            "--user",
            "set-property",
            "--runtime",
            "app-t3code-agents.slice",
            expect.stringMatching(/^MemoryHigh=\d+$/),
            expect.stringMatching(/^MemoryMax=\d+$/),
          ],
          [
            "--user",
            "set-property",
            "--runtime",
            "app-t3code.slice",
            "CPUWeight=10",
            "IOWeight=10",
            "TasksMax=25%",
          ],
        ]);
      }),
    ),
  );

  it.effect("stops agents a killed server left before the first launch", () =>
    withFakeSystemd(({ scope, units, calls, stateDir }) =>
      Effect.gen(function* () {
        units.set(ownedAgentSlice(stateDir), { activeState: "active", result: "success" });
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const stops = calls.filter(([, verb]) => verb === "stop");
        expect(stops).toEqual([["--user", "stop", ownedAgentSlice(stateDir)]]);
        expect(sliceOf(agent)).toBe(ownedAgentSlice(stateDir));
        expect(agent.args).toContain("--property=TimeoutStopSec=10s");
        const terminal = yield* scope.wrap({
          command: "agent",
          args: [],
          name: "terminal",
          survivesRestart: true,
        });
        expect(sliceOf(terminal)).toBe("app-t3code-agents.slice");
        expect(calls.filter(([, verb]) => verb === "stop")).toHaveLength(1);
      }),
    ),
  );

  it.effect("stops leftover processes only in an active agent scope", () =>
    withFakeSystemd(({ scope, units, calls, cgroupRoot }) =>
      Effect.gen(function* () {
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const unit = unitOf(agent);
        expect(agent.unit).toBe(unit);

        yield* scope.stop("session-2.scope");
        expect(calls.some((args) => args.includes("session-2.scope"))).toBe(false);
        yield* scope.stop(unit);
        exitedAgent(units, cgroupRoot, unit, { populated: false, oomKills: 0 });
        yield* scope.stop(unit);
        exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 0 });
        units.get(unit)!.activeState = "deactivating";
        yield* scope.stop(unit);
        expect(stopsOf(calls)).toEqual([]);

        units.get(unit)!.activeState = "active";
        yield* scope.stop(unit);
        expect(stopsOf(calls)).toEqual([["--user", "stop", "--no-block", unit]]);
        expect(yield* scope.oomKilled("a")).toBe(false);
      }),
    ),
  );

  it.effect.each([true, false])(
    "cleans up an OOM exit without losing its diagnosis (OOMPolicy=%s)",
    (oomPolicy) =>
      withFakeSystemd(
        ({ scope, units, calls, cgroupRoot }) =>
          Effect.gen(function* () {
            const agent = yield* scope.wrap({
              command: "agent",
              args: [],
              name: "t",
              threadId: "a",
            });
            const unit = unitOf(agent);
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: undefined });
            yield* scope.stop(unit);
            expect(stopsOf(calls)).toEqual(
              oomPolicy ? [] : [["--user", "stop", "--no-block", unit]],
            );

            // The kernel records the kill before systemd records the OOM result.
            exitedAgent(units, cgroupRoot, unit, { populated: true, oomKills: 1 });
            yield* scope.stop(unit);
            expect(stopsOf(calls)).toHaveLength(oomPolicy ? 0 : 2);

            if (oomPolicy) {
              units.set(unit, { activeState: "failed", result: "oom-kill" });
              expect(yield* scope.oomKilled("a")).toBe(true);
              units.delete(unit);
              expect(yield* scope.oomKilled("a")).toBe(true);
            }
          }),
        { oomPolicy },
      ),
  );

  it.effect("reports failed daemon cleanup without failing session teardown", () => {
    const warnings: Array<unknown> = [];
    const logger = Logger.make(({ message }) => {
      warnings.push(message);
    });
    return withFakeSystemd(
      ({ scope, units, cgroupRoot }) =>
        Effect.gen(function* () {
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          exitedAgent(units, cgroupRoot, unitOf(agent), { populated: true, oomKills: 0 });
          yield* scope.stop(unitOf(agent));
          expect(warnings.flat()).toContain("Could not stop an exited agent's scope");
        }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false }))),
      { stopFails: true },
    );
  });

  it.effect("leaves a quiet slice alone", () =>
    withFakeSystemd(({ scope, calls }) =>
      Effect.gen(function* () {
        yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        expect(calls.filter(([, verb]) => verb === "stop")).toEqual([]);
      }),
    ),
  );
});

const fixtureProcStat = (pid: number, ticks = "9007199254740993", state = "S") => {
  const fields = Array<string>(20).fill("0");
  fields[0] = state;
  fields[1] = "1";
  fields[5] = "-1";
  fields[19] = ticks;
  return `${pid} (process ) with spaces) ${fields.join(" ")}\n`;
};

const seedScopeBinding = (input: {
  readonly units: Map<string, FakeUnit>;
  readonly procRoot: string;
  readonly unit: string;
  readonly pid: number;
}) => {
  const controlGroup = `/user.slice/${input.unit}`;
  input.units.set(input.unit, { activeState: "active", result: "success", controlGroup });
  const directory = NodePath.join(input.procRoot, String(input.pid));
  NodeFS.mkdirSync(directory, { recursive: true });
  NodeFS.writeFileSync(NodePath.join(directory, "stat"), fixtureProcStat(input.pid));
  NodeFS.writeFileSync(
    NodePath.join(directory, "cgroup"),
    `9:memory:/legacy\n0::${controlGroup}/delegated\n`,
  );
  return { controlGroup, directory };
};

it.effect("launch custody binds only stable scope and process identity without mutation", () =>
  withFakeSystemd(({ scope, units, procRoot, procReads, calls }) =>
    Effect.gen(function* () {
      const unscoped = yield* AgentScope.defaultValue().wrap({
        command: "agent",
        args: [],
        name: "binding",
      });
      const unresolved = yield* scope.wrap({
        command: "missing-binding-executable",
        args: [],
        name: "binding",
      });
      assert.isUndefined(unscoped.custody);
      assert.isUndefined(unresolved.custody);
      const launch = yield* scope.wrap({
        command: "agent",
        args: [],
        name: "binding",
        shared: true,
      });
      assert.isDefined(launch.custody);
      if (!launch.custody || !launch.unit) return yield* Effect.die("missing launch custody");
      assert.isUndefined(yield* launch.custody.bind(101));
      assert.strictEqual(procReads.length, 0);
      const { controlGroup } = seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
      const before = calls.length;
      const binding = yield* launch.custody.bind(101);
      assert.deepStrictEqual(binding, {
        unit: launch.unit,
        invocationId: "11111111111111111111111111111111",
        controlGroup,
        pid: 101,
        pidStartTicks: "9007199254740993",
      });
      assert.isTrue(Object.isFrozen(binding));
      assert.isTrue(Object.isFrozen(launch.custody));
      assert.strictEqual(yield* launch.custody.bind(101), binding);
      assert.isTrue(calls.slice(before).every(([, verb]) => verb === "show"));
      assert.strictEqual(calls.length - before, 4);
      assert.deepStrictEqual(procReads, [
        "/proc/101/stat",
        "/proc/101/cgroup",
        "/proc/101/cgroup",
        "/proc/101/stat",
        "/proc/101/stat",
        "/proc/101/cgroup",
        "/proc/101/cgroup",
        "/proc/101/stat",
      ]);
    }),
  ),
);

it.effect.each([
  "timedOut",
  "stdoutTruncated",
  "stderrTruncated",
  "stdoutInvalidUtf8",
  "stderrInvalidUtf8",
] as const)("launch custody refuses incomplete process output with %s", (flag) =>
  withFakeSystemd(
    ({ scope, units, procRoot }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
        assert.isUndefined(yield* launch.custody.bind(101));
      }),
    { transformBindingShow: (shown) => ({ ...shown, [flag]: true }) },
  ),
);

it.effect.each([
  "nonzero_exit",
  "missing_exit",
  "missing_property",
  "duplicate_property",
  "extra_property",
  "wrong_id",
  "inactive",
  "not_loaded",
  "not_running",
  "zero_invocation",
  "uppercase_invocation",
  "short_invocation",
  "root_group",
  "dot_group",
  "double_slash_group",
  "control_group",
  "wrong_unit_group",
] as const)("launch custody refuses malformed unit observation %s", (kind) =>
  withFakeSystemd(
    ({ scope, units, procRoot }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
        assert.isUndefined(yield* launch.custody.bind(101));
      }),
    {
      transformBindingShow: (shown) => {
        switch (kind) {
          case "nonzero_exit":
            return { ...shown, code: ChildProcessSpawner.ExitCode(5) };
          case "missing_exit":
            return { ...shown, code: null };
          case "missing_property":
            return { ...shown, stdout: shown.stdout.replace(/^InvocationID=.*\n/m, "") };
          case "duplicate_property":
            return {
              ...shown,
              stdout: shown.stdout + "InvocationID=11111111111111111111111111111111\n",
            };
          case "extra_property":
            return { ...shown, stdout: shown.stdout + "Unexpected=true\n" };
          case "wrong_id":
            return { ...shown, stdout: shown.stdout.replace(/^Id=.*$/m, "Id=unrelated.scope") };
          case "inactive":
            return {
              ...shown,
              stdout: shown.stdout.replace("ActiveState=active", "ActiveState=inactive"),
            };
          case "not_loaded":
            return {
              ...shown,
              stdout: shown.stdout.replace("LoadState=loaded", "LoadState=not-found"),
            };
          case "not_running":
            return { ...shown, stdout: shown.stdout.replace("SubState=running", "SubState=dead") };
          case "zero_invocation":
            return {
              ...shown,
              stdout: shown.stdout.replace(/^InvocationID=.*$/m, "InvocationID=" + "0".repeat(32)),
            };
          case "uppercase_invocation":
            return {
              ...shown,
              stdout: shown.stdout.replace(/^InvocationID=.*$/m, "InvocationID=" + "A".repeat(32)),
            };
          case "short_invocation":
            return {
              ...shown,
              stdout: shown.stdout.replace(/^InvocationID=.*$/m, "InvocationID=abc"),
            };
          case "root_group":
            return {
              ...shown,
              stdout: shown.stdout.replace(/^ControlGroup=.*$/m, "ControlGroup=/"),
            };
          case "dot_group":
            return {
              ...shown,
              stdout: shown.stdout.replace(
                "ControlGroup=/user.slice/",
                "ControlGroup=/user.slice/../",
              ),
            };
          case "double_slash_group":
            return {
              ...shown,
              stdout: shown.stdout.replace(
                "ControlGroup=/user.slice/",
                "ControlGroup=/user.slice//",
              ),
            };
          case "control_group":
            return {
              ...shown,
              stdout: shown.stdout.replace(
                "ControlGroup=/user.slice/",
                "ControlGroup=/user\t.slice/",
              ),
            };
          case "wrong_unit_group":
            return {
              ...shown,
              stdout: shown.stdout.replace(/^(ControlGroup=.*)$/m, "$1-lookalike"),
            };
        }
      },
    },
  ),
);

it.effect.each([
  "missing_stat",
  "wrong_pid",
  "zombie",
  "dead",
  "invalid_state",
  "short_stat",
  "invalid_start",
  "missing_membership",
  "v1_only",
  "duplicate_unified",
  "bad_unified_controller",
  "prefix_lookalike",
  "dot_membership",
] as const)("launch custody refuses malformed process observation %s", (kind) =>
  withFakeSystemd(({ scope, units, procRoot }) =>
    Effect.gen(function* () {
      const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
      if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
      const { directory, controlGroup } = seedScopeBinding({
        units,
        procRoot,
        unit: launch.unit,
        pid: 101,
      });
      const stat = NodePath.join(directory, "stat");
      const cgroup = NodePath.join(directory, "cgroup");
      switch (kind) {
        case "missing_stat":
          NodeFS.unlinkSync(stat);
          break;
        case "wrong_pid":
          NodeFS.writeFileSync(stat, fixtureProcStat(102));
          break;
        case "zombie":
          NodeFS.writeFileSync(stat, fixtureProcStat(101, "22", "Z"));
          break;
        case "dead":
          NodeFS.writeFileSync(stat, fixtureProcStat(101, "22", "X"));
          break;
        case "invalid_state":
          NodeFS.writeFileSync(stat, fixtureProcStat(101, "22", "?"));
          break;
        case "short_stat":
          NodeFS.writeFileSync(stat, "101 (short) S 1 2\n");
          break;
        case "invalid_start":
          NodeFS.writeFileSync(stat, fixtureProcStat(101, "9e9"));
          break;
        case "missing_membership":
          NodeFS.unlinkSync(cgroup);
          break;
        case "v1_only":
          NodeFS.writeFileSync(cgroup, "9:memory:/legacy\n");
          break;
        case "duplicate_unified":
          NodeFS.writeFileSync(cgroup, `0::${controlGroup}\n0::${controlGroup}\n`);
          break;
        case "bad_unified_controller":
          NodeFS.writeFileSync(cgroup, `0:memory:${controlGroup}\n`);
          break;
        case "prefix_lookalike":
          NodeFS.writeFileSync(cgroup, `0::${controlGroup}-other/child\n`);
          break;
        case "dot_membership":
          NodeFS.writeFileSync(cgroup, `0::${controlGroup}/../other\n`);
          break;
      }
      assert.isUndefined(yield* launch.custody.bind(101));
    }),
  ),
);

it.effect.each([0, -1, 1.1, Number.MAX_SAFE_INTEGER + 1, Number.NaN])(
  "launch custody refuses invalid pid %s before reads",
  (pid) =>
    withFakeSystemd(({ scope, calls, procReads }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
        if (!launch.custody) return yield* Effect.die("missing launch custody");
        const before = calls.length;
        assert.isUndefined(yield* launch.custody.bind(pid));
        assert.strictEqual(calls.length, before);
        assert.strictEqual(procReads.length, 0);
      }),
    ),
);

it.effect.each(["pid", "start", "invocation", "group"] as const)(
  "launch custody never replaces a pinned %s identity",
  (kind) =>
    withFakeSystemd(({ scope, units, procRoot }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        const { directory, controlGroup } = seedScopeBinding({
          units,
          procRoot,
          unit: launch.unit,
          pid: 101,
        });
        const original = yield* launch.custody.bind(101);
        assert.isDefined(original);
        const state = units.get(launch.unit)!;
        switch (kind) {
          case "pid":
            seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 102 });
            break;
          case "start":
            NodeFS.writeFileSync(
              NodePath.join(directory, "stat"),
              fixtureProcStat(101, "9007199254740994"),
            );
            break;
          case "invocation":
            units.set(launch.unit, { ...state, invocationId: "2".repeat(32) });
            break;
          case "group": {
            const next = `/another.slice/${launch.unit}`;
            units.set(launch.unit, { ...state, controlGroup: next });
            NodeFS.writeFileSync(NodePath.join(directory, "cgroup"), `0::${next}\n`);
            break;
          }
        }
        assert.isUndefined(yield* launch.custody.bind(kind === "pid" ? 102 : 101));
        units.set(launch.unit, state);
        NodeFS.writeFileSync(NodePath.join(directory, "stat"), fixtureProcStat(101));
        NodeFS.writeFileSync(NodePath.join(directory, "cgroup"), `0::${controlGroup}\n`);
        assert.strictEqual(yield* launch.custody.bind(101), original);
      }),
    ),
);

it.effect.each(["invocation", "start", "membership"] as const)(
  "launch custody rejects %s changing between samples",
  (kind) => {
    let change: (() => void) | undefined;
    let statReads = 0;
    let groupReads = 0;
    return withFakeSystemd(
      ({ scope, units, procRoot }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          const unit = launch.unit;
          const { directory, controlGroup } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
          change = () => {
            if (kind === "invocation")
              units.set(unit, { ...units.get(unit)!, invocationId: "2".repeat(32) });
            else if (kind === "start")
              NodeFS.writeFileSync(NodePath.join(directory, "stat"), fixtureProcStat(101, "23"));
            else
              NodeFS.writeFileSync(
                NodePath.join(directory, "cgroup"),
                `0::${controlGroup}/different\n`,
              );
          };
          assert.isUndefined(yield* launch.custody.bind(101));
        }),
      {
        beforeProcRead: (file) =>
          Effect.sync(() => {
            if (file.endsWith("/stat") && ++statReads === 2 && kind !== "membership") change!();
            if (file.endsWith("/cgroup") && ++groupReads === 2 && kind === "membership") change!();
          }),
      },
    );
  },
);

it.effect.each([101, 102])(
  "launch custody pins the first concurrent valid completion with winner %s",
  (winner) =>
    Effect.gen(function* () {
      const ready = yield* Queue.unbounded<number>();
      const first = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      const gates = new Map([
        [101, first],
        [102, second],
      ]);
      const counts = new Map<number, number>();
      yield* withFakeSystemd(
        ({ scope, units, procRoot }) =>
          Effect.gen(function* () {
            const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
            if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
            seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
            seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 102 });
            const one = yield* launch.custody
              .bind(101)
              .pipe(Effect.forkScoped({ startImmediately: true }));
            const two = yield* launch.custody
              .bind(102)
              .pipe(Effect.forkScoped({ startImmediately: true }));
            assert.deepStrictEqual((yield* Queue.takeN(ready, 2)).toSorted(), [101, 102]);
            yield* Deferred.succeed(gates.get(winner)!, undefined);
            const selected = yield* Fiber.join(winner === 101 ? one : two);
            assert.strictEqual(selected?.pid, winner);
            yield* Deferred.succeed(gates.get(winner === 101 ? 102 : 101)!, undefined);
            assert.isUndefined(yield* Fiber.join(winner === 101 ? two : one));
            assert.strictEqual(yield* launch.custody.bind(winner), selected);
          }),
        {
          beforeProcRead: (file) =>
            Effect.gen(function* () {
              if (!file.endsWith("/stat")) return;
              const pid = Number(file.split("/")[2]);
              const count = (counts.get(pid) ?? 0) + 1;
              counts.set(pid, count);
              if (count === 2) {
                yield* Queue.offer(ready, pid);
                yield* Deferred.await(gates.get(pid)!);
              }
            }),
        },
      );
    }),
);

it.effect("launch custody preserves read interruption and defects", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const sentinel = new Error("fixture read defect");
    let defect = false;
    yield* withFakeSystemd(
      ({ scope, units, procRoot }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
          const pending = yield* launch.custody.bind(101).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(pending);
          const interrupted = yield* Fiber.await(pending);
          assert.strictEqual(interrupted._tag, "Failure");
          if (interrupted._tag === "Failure") assert.isTrue(Cause.hasInterrupts(interrupted.cause));
          defect = true;
          const failed = yield* Effect.exit(launch.custody.bind(101));
          assert.strictEqual(failed._tag, "Failure");
          if (failed._tag === "Failure") {
            const found = Cause.findDefect(failed.cause);
            assert.strictEqual(found._tag, "Success");
            if (found._tag === "Success") assert.strictEqual(found.success, sentinel);
          }
        }),
      {
        beforeProcRead: () =>
          Effect.suspend(() =>
            defect
              ? Effect.die(sentinel)
              : Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ),
      },
    );
  }),
);

it.effect.each(["proc", "show"] as const)(
  "launch custody preserves mixed read error and interruption at %s",
  (seam) => {
    let interrupted = true;
    const interrupt = Cause.interrupt(123456);
    const readError = PlatformError.systemError({
      _tag: "NotFound",
      module: "FileSystem",
      method: "readFileString",
    });
    const showError = new ProcessRunner.ProcessSpawnError({
      command: "systemctl",
      argumentCount: 4,
      cause: "fixture typed error",
    });
    return withFakeSystemd(
      ({ scope, units, procRoot }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({ command: "agent", args: [], name: "binding" });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
          const exit = yield* Effect.exit(launch.custody.bind(101));
          assert.strictEqual(exit._tag, "Failure");
          if (exit._tag === "Failure") assert.isTrue(Cause.hasInterrupts(exit.cause));
          interrupted = false;
          assert.isUndefined(yield* launch.custody.bind(101));
        }),
      {
        beforeProcRead: () =>
          seam === "proc"
            ? Effect.failCause(
                interrupted
                  ? Cause.combine(Cause.fail(readError), interrupt)
                  : Cause.fail(readError),
              )
            : Effect.void,
        beforeBindingShow: () =>
          seam === "show"
            ? Effect.failCause(
                interrupted
                  ? Cause.combine(Cause.fail(showError), interrupt)
                  : Cause.fail(showError),
              )
            : Effect.void,
      },
    );
  },
);

it.effect.each(["empty", "removed", "leader_exited"] as const)(
  "launch custody observes completed scope retirement as %s",
  (kind) => {
    let stopped: (() => void) | undefined;
    return withFakeSystemd(
      ({ scope, units, procRoot, cgroupRoot, calls }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({
            command: "agent",
            args: [],
            name: "settlement",
            shared: true,
          });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          const custody = launch.custody;
          const noPinCalls = calls.length;
          assert.isUndefined(yield* custody.stopAndObserve());
          assert.strictEqual(calls.length, noPinCalls);
          const unit = launch.unit;
          const { controlGroup, directory } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
          const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
          NodeFS.mkdirSync(groupDirectory, { recursive: true });
          NodeFS.writeFileSync(
            NodePath.join(groupDirectory, "cgroup.events"),
            "populated 1\nfrozen 0\n",
          );
          const binding = yield* custody.bind(101);
          assert.isDefined(binding);
          if (kind === "leader_exited") NodeFS.rmSync(directory, { recursive: true });
          stopped = () => {
            NodeFS.rmSync(directory, { recursive: true, force: true });
            if (kind !== "empty") {
              NodeFS.rmSync(groupDirectory, { recursive: true });
              units.delete(unit);
            } else {
              units.set(unit, { ...units.get(unit)!, activeState: "inactive", subState: "dead" });
              NodeFS.writeFileSync(
                NodePath.join(groupDirectory, "cgroup.events"),
                "populated 0\nfrozen 0\n",
              );
            }
          };
          const before = calls.length;
          const settled = yield* custody.stopAndObserve();
          assert.deepStrictEqual(settled, {
            binding,
            observation: kind === "empty" ? "empty" : "removed",
          });
          assert.isTrue(Object.isFrozen(settled));
          const after = calls.length;
          assert.strictEqual(yield* custody.stopAndObserve(), settled);
          assert.strictEqual(calls.length, after);
          assert.deepStrictEqual(
            calls.slice(before).filter(([, verb]) => verb === "stop"),
            [["--user", "stop", unit]],
          );
        }),
      { onBlockingStop: () => Effect.sync(() => stopped!()) },
    );
  },
);

it.effect.each(["collected", "replacement"] as const)(
  "launch custody refuses %s before retirement without stopping",
  (kind) =>
    withFakeSystemd(({ scope, units, procRoot, calls }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        const { custody, unit } = launch;
        seedScopeBinding({ units, procRoot, unit, pid: 101 });
        assert.isDefined(yield* custody.bind(101));
        const state = units.get(unit)!;
        if (kind === "collected") units.delete(unit);
        else units.set(unit, { ...state, invocationId: "2".repeat(32) });
        const before = calls.length;
        assert.isUndefined(yield* custody.stopAndObserve());
        units.set(unit, state);
        assert.isUndefined(yield* custody.stopAndObserve());
        assert.isTrue(calls.slice(before).every(([, verb]) => verb === "show"));
        assert.strictEqual(calls.length - before, 1);
      }),
    ),
);

it.effect.each([
  "timedOut",
  "stdoutTruncated",
  "stderrTruncated",
  "stdoutInvalidUtf8",
  "stderrInvalidUtf8",
  "nonzero",
] as const)("launch custody retains unknown after stop result %s", (flag) =>
  withFakeSystemd(
    ({ scope, units, procRoot, calls }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
        assert.isDefined(yield* launch.custody.bind(101));
        const before = calls.length;
        assert.isUndefined(yield* launch.custody.stopAndObserve());
        assert.isUndefined(yield* launch.custody.stopAndObserve());
        assert.deepStrictEqual(
          calls.slice(before).map(([, verb]) => verb),
          ["show", "stop"],
        );
      }),
    {
      transformBlockingStop: (result) =>
        flag === "nonzero"
          ? { ...result, code: ChildProcessSpawner.ExitCode(5) }
          : { ...result, [flag]: true },
    },
  ),
);

it.effect.each([
  "missing_events",
  "populated",
  "duplicate_populated",
  "malformed_events",
  "group_not_directory",
  "live_original_pid",
  "malformed_pid",
  "zombie_pid",
  "wrong_unit_invocation",
  "wrong_unit_group",
  "active_unit",
] as const)("launch custody retains unknown after conflicting post-stop evidence %s", (kind) => {
  let stopped: (() => void) | undefined;
  return withFakeSystemd(
    ({ scope, units, procRoot, cgroupRoot, calls }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        const { custody, unit } = launch;
        const { controlGroup, directory } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
        const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
        NodeFS.mkdirSync(groupDirectory, { recursive: true });
        assert.isDefined(yield* custody.bind(101));
        stopped = () => {
          units.set(unit, { ...units.get(unit)!, activeState: "inactive", subState: "dead" });
          NodeFS.rmSync(directory, { recursive: true });
          const events = NodePath.join(groupDirectory, "cgroup.events");
          NodeFS.writeFileSync(events, "populated 0\nfrozen 0\n");
          switch (kind) {
            case "missing_events":
              NodeFS.unlinkSync(events);
              break;
            case "populated":
              NodeFS.writeFileSync(events, "populated 1\nfrozen 0\n");
              break;
            case "duplicate_populated":
              NodeFS.writeFileSync(events, "populated 0\npopulated 0\n");
              break;
            case "malformed_events":
              NodeFS.writeFileSync(events, "populated nope\n");
              break;
            case "group_not_directory":
              NodeFS.rmSync(groupDirectory, { recursive: true });
              NodeFS.writeFileSync(groupDirectory, "not a cgroup");
              break;
            case "live_original_pid":
              NodeFS.mkdirSync(directory);
              NodeFS.writeFileSync(NodePath.join(directory, "stat"), fixtureProcStat(101));
              break;
            case "malformed_pid":
              NodeFS.mkdirSync(directory);
              NodeFS.writeFileSync(NodePath.join(directory, "stat"), "not process stat");
              break;
            case "zombie_pid":
              NodeFS.mkdirSync(directory);
              NodeFS.writeFileSync(
                NodePath.join(directory, "stat"),
                fixtureProcStat(101, "9007199254740993", "Z"),
              );
              break;
            case "wrong_unit_invocation":
              units.set(unit, { ...units.get(unit)!, invocationId: "2".repeat(32) });
              break;
            case "wrong_unit_group":
              units.set(unit, { ...units.get(unit)!, controlGroup: "/foreign.scope" });
              break;
            case "active_unit":
              units.set(unit, { ...units.get(unit)!, activeState: "active", subState: "running" });
              break;
          }
        };
        const before = calls.length;
        assert.isUndefined(yield* custody.stopAndObserve());
        assert.isUndefined(yield* custody.stopAndObserve());
        assert.strictEqual(calls.slice(before).filter(([, verb]) => verb === "stop").length, 1);
      }),
    { onBlockingStop: () => Effect.sync(() => stopped!()) },
  );
});

it.effect(
  "launch custody accepts a different live birth at a reused pid without targeting it",
  () => {
    let stopped: (() => void) | undefined;
    return withFakeSystemd(
      ({ scope, units, procRoot, cgroupRoot, calls }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          const { custody, unit } = launch;
          const { controlGroup, directory } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
          const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
          NodeFS.mkdirSync(groupDirectory, { recursive: true });
          const binding = yield* custody.bind(101);
          stopped = () => {
            units.set(unit, {
              ...units.get(unit)!,
              activeState: "inactive",
              subState: "dead",
              controlGroup: "",
            });
            NodeFS.writeFileSync(
              NodePath.join(groupDirectory, "cgroup.events"),
              "populated 0\nfrozen 0\n",
            );
            NodeFS.writeFileSync(
              NodePath.join(directory, "stat"),
              fixtureProcStat(101, "9007199254740994"),
            );
          };
          const before = calls.length;
          assert.deepStrictEqual(yield* custody.stopAndObserve(), {
            binding,
            observation: "empty",
          });
          assert.isFalse(calls.slice(before).some(([, verb]) => verb === "kill"));
        }),
      { onBlockingStop: () => Effect.sync(() => stopped!()) },
    );
  },
);

it.effect(
  "launch custody shares an in-progress retirement without giving waiter cancellation ownership",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let stopped: (() => void) | undefined;
      yield* withFakeSystemd(
        ({ scope, units, procRoot, cgroupRoot, calls }) =>
          Effect.gen(function* () {
            const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
            if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
            const { custody, unit } = launch;
            const { controlGroup, directory } = seedScopeBinding({
              units,
              procRoot,
              unit,
              pid: 101,
            });
            const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
            NodeFS.mkdirSync(groupDirectory, { recursive: true });
            const binding = yield* custody.bind(101);
            stopped = () => {
              units.delete(unit);
              NodeFS.rmSync(groupDirectory, { recursive: true });
              NodeFS.rmSync(directory, { recursive: true });
            };
            const before = calls.length;
            const owner = yield* custody.stopAndObserve().pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            const waiter = yield* custody
              .stopAndObserve()
              .pipe(Effect.forkScoped({ startImmediately: true }));
            yield* Fiber.interrupt(waiter);
            const waiting = yield* Fiber.await(waiter);
            assert.strictEqual(waiting._tag, "Failure");
            yield* Deferred.succeed(release, undefined);
            const receipt = yield* Fiber.join(owner);
            assert.deepStrictEqual(receipt, { binding, observation: "removed" });
            assert.strictEqual(yield* custody.stopAndObserve(), receipt);
            assert.strictEqual(calls.slice(before).filter(([, verb]) => verb === "stop").length, 1);
          }),
        {
          onBlockingStop: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(Effect.sync(() => stopped!())),
            ),
        },
      );
    }),
);

it.effect.each([false, true])(
  "launch custody owner interruption caches unknown even from masked cleanup=%s",
  (masked) =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      yield* withFakeSystemd(
        ({ scope, units, procRoot, calls }) =>
          Effect.gen(function* () {
            const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
            if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
            const { custody, unit } = launch;
            seedScopeBinding({ units, procRoot, unit, pid: 101 });
            assert.isDefined(yield* custody.bind(101));
            const before = calls.length;
            const retiring = custody.stopAndObserve();
            const owner = yield* (masked ? Effect.uninterruptible(retiring) : retiring).pipe(
              Effect.forkScoped,
            );
            yield* Deferred.await(entered);
            const waiter = yield* custody
              .stopAndObserve()
              .pipe(Effect.forkScoped({ startImmediately: true }));
            yield* Fiber.interrupt(owner);
            const interrupted = yield* Fiber.await(owner);
            assert.strictEqual(interrupted._tag, "Failure");
            if (interrupted._tag === "Failure")
              assert.isTrue(Cause.hasInterrupts(interrupted.cause));
            assert.isUndefined(yield* Fiber.join(waiter));
            assert.isUndefined(yield* custody.stopAndObserve());
            assert.strictEqual(calls.slice(before).filter(([, verb]) => verb === "stop").length, 1);
          }),
        {
          onBlockingStop: () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        },
      );
    }),
);

it.effect("launch custody owner defect remains a defect while repeats retain unknown", () => {
  const sentinel = new Error("fixture stop defect");
  return withFakeSystemd(
    ({ scope, units, procRoot, calls }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        seedScopeBinding({ units, procRoot, unit: launch.unit, pid: 101 });
        assert.isDefined(yield* launch.custody.bind(101));
        const before = calls.length;
        const failed = yield* Effect.exit(launch.custody.stopAndObserve());
        assert.strictEqual(failed._tag, "Failure");
        if (failed._tag === "Failure") {
          const defect = Cause.findDefect(failed.cause);
          assert.strictEqual(defect._tag, "Success");
          if (defect._tag === "Success") assert.strictEqual(defect.success, sentinel);
        }
        assert.isUndefined(yield* launch.custody.stopAndObserve());
        assert.strictEqual(calls.slice(before).filter(([, verb]) => verb === "stop").length, 1);
      }),
    { onBlockingStop: () => Effect.die(sentinel) },
  );
});

it.effect.each([
  "empty_to_removed",
  "removed_to_empty",
  "pid_reappears",
  "collected_to_loaded",
] as const)("launch custody verifies post-stop transition %s", (kind) => {
  let stopped: (() => void) | undefined;
  let change: (() => void) | undefined;
  let afterStop = false;
  let pidReads = 0;
  return withFakeSystemd(
    ({ scope, units, procRoot, cgroupRoot }) =>
      Effect.gen(function* () {
        const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
        if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
        const { custody, unit } = launch;
        const { controlGroup, directory } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
        const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
        NodeFS.mkdirSync(groupDirectory, { recursive: true });
        const binding = yield* custody.bind(101);
        const retired = { ...units.get(unit)!, activeState: "inactive", subState: "dead" };
        stopped = () => {
          afterStop = true;
          NodeFS.rmSync(directory, { recursive: true });
          if (kind === "removed_to_empty" || kind === "collected_to_loaded") {
            NodeFS.rmSync(groupDirectory, { recursive: true });
            units.delete(unit);
          } else {
            units.set(unit, retired);
            NodeFS.writeFileSync(
              NodePath.join(groupDirectory, "cgroup.events"),
              "populated 0\nfrozen 0\n",
            );
          }
        };
        change = () => {
          switch (kind) {
            case "empty_to_removed":
              NodeFS.rmSync(groupDirectory, { recursive: true });
              units.delete(unit);
              break;
            case "removed_to_empty":
              NodeFS.mkdirSync(groupDirectory, { recursive: true });
              NodeFS.writeFileSync(
                NodePath.join(groupDirectory, "cgroup.events"),
                "populated 0\nfrozen 0\n",
              );
              break;
            case "pid_reappears":
              NodeFS.mkdirSync(directory);
              NodeFS.writeFileSync(NodePath.join(directory, "stat"), fixtureProcStat(101));
              break;
            case "collected_to_loaded":
              units.set(unit, retired);
              break;
          }
        };
        const result = yield* custody.stopAndObserve();
        if (kind === "empty_to_removed")
          assert.deepStrictEqual(result, { binding, observation: "removed" });
        else assert.isUndefined(result);
      }),
    {
      onBlockingStop: () => Effect.sync(() => stopped!()),
      beforeProcRead: (file) =>
        Effect.sync(() => {
          if (afterStop && file.endsWith("/stat") && ++pidReads === 2) change!();
        }),
    },
  );
});

it.effect.each(["directory", "events"] as const)(
  "launch custody treats unreadable original cgroup %s as unknown",
  (seam) => {
    let stopped: (() => void) | undefined;
    let afterStop = false;
    return withFakeSystemd(
      ({ scope, units, procRoot, cgroupRoot }) =>
        Effect.gen(function* () {
          const launch = yield* scope.wrap({ command: "agent", args: [], name: "settlement" });
          if (!launch.unit || !launch.custody) return yield* Effect.die("missing launch custody");
          const { custody, unit } = launch;
          const { controlGroup, directory } = seedScopeBinding({ units, procRoot, unit, pid: 101 });
          const groupDirectory = NodePath.join(cgroupRoot, controlGroup);
          NodeFS.mkdirSync(groupDirectory, { recursive: true });
          assert.isDefined(yield* custody.bind(101));
          stopped = () => {
            afterStop = true;
            units.set(unit, { ...units.get(unit)!, activeState: "inactive", subState: "dead" });
            NodeFS.rmSync(directory, { recursive: true });
            NodeFS.writeFileSync(
              NodePath.join(groupDirectory, "cgroup.events"),
              "populated 0\nfrozen 0\n",
            );
          };
          assert.isUndefined(yield* custody.stopAndObserve());
        }),
      {
        onBlockingStop: () => Effect.sync(() => stopped!()),
        beforeCgroupIO: (file) =>
          Effect.suspend(() =>
            afterStop && file.endsWith("/cgroup.events") === (seam === "events")
              ? Effect.fail(
                  PlatformError.systemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "read",
                    pathOrDescriptor: file,
                  }),
                )
              : Effect.void,
          ),
      },
    );
  },
);
