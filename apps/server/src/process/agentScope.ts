// @effect-diagnostics nodeBuiltinImport:off -- unit names and RAM size come from the host.
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";

import {
  AgentScope,
  agentScopeCommand,
  type AgentScopeShape,
  type AgentScopeBinding,
  type AgentScopeCustody,
  type AgentScopeSettlement,
  type AgentSystemdScope,
} from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveCommandPath } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import type * as PlatformError from "effect/PlatformError";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";

const AGENT_SLICE = "app-t3code-agents.slice";
// Owns every agent slice and competes with the server's unit or scope under app.slice.
const AGENTS_PARENT_SLICE = "app-t3code.slice";
// Low weights yield CPU and disk to the server under contention, where the user manager
// has those controllers: current systemd delegates cpu, rarely io. TasksMax keeps agents
// below logind's 33% per-user task limit, leaving forks for the server.
const AGENTS_PARENT_PROPERTIES = ["CPUWeight=10", "IOWeight=10", "TasksMax=25%"];

export function ownedAgentSlice(stateDir: string): string {
  const hash = NodeCrypto.createHash("sha256").update(stateDir).digest("hex").slice(0, 12);
  return `app-t3code-agents-${hash}.slice`;
}

const GiB = 1024 ** 3;

/**
 * Agents together leave about 6 GB free before they are throttled and 4 GB
 * before the kernel kills one, so the server, sshd, and tailscaled keep
 * working. Small machines still give agents at least half of RAM before
 * throttling and three quarters before a kill.
 */
function agentSliceMemoryLimits(totalBytes: number) {
  return {
    high: Math.max(totalBytes - 6 * GiB, Math.floor(totalBytes / 2)),
    max: Math.max(totalBytes - 4 * GiB, Math.floor(totalBytes * 0.75)),
  };
}

export interface ScopeState {
  readonly loadState: string;
  readonly activeState: string;
  readonly result: string;
  readonly oomKills: number | undefined;
  readonly populated: boolean | undefined;
}

/** Pipes can close before systemd records oom-kill; stopping, empty or OOM-counted scopes remain pending. */
export function classifyScope(state: ScopeState): "oom-killed" | "gone" | "running" | "stopping" {
  if (state.result === "oom-kill") return "oom-killed";
  if (
    state.loadState !== "loaded" ||
    state.activeState === "inactive" ||
    state.activeState === "failed"
  ) {
    return "gone";
  }
  if (
    state.activeState === "deactivating" ||
    (state.oomKills ?? 0) > 0 ||
    state.populated === false
  ) {
    return "stopping";
  }
  return "running";
}

function parseKeyValues(text: string): Map<string, string> {
  const values = new Map<string, string>();
  for (const line of text.split("\n")) {
    const index = line.indexOf("=");
    if (index > 0) values.set(line.slice(0, index), line.slice(index + 1).trim());
  }
  return values;
}

function parseCgroupCounter(text: string, key: string): number | undefined {
  const match = new RegExp(`^${key} (\\d+)$`, "m").exec(text);
  return match === null ? undefined : Number(match[1]);
}

const recoverBindingRead = Effect.fn("AgentScope.recoverBindingRead")(function* <E>(
  cause: Cause.Cause<E>,
) {
  const exceptional = Cause.fromReasons<never>(
    cause.reasons.filter((reason) => reason._tag !== "Fail"),
  );
  if (exceptional.reasons.length > 0) return yield* Effect.failCause(exceptional);
  return undefined;
});

const BINDING_SCOPE_PROPERTIES = [
  "Id",
  "LoadState",
  "ActiveState",
  "SubState",
  "InvocationID",
  "ControlGroup",
];

const isCgroupPath = (value: string) =>
  value.startsWith("/") &&
  value.length > 1 &&
  !value.endsWith("/") &&
  !/[\x00-\x1f\x7f]/.test(value) &&
  value
    .slice(1)
    .split("/")
    .every((part) => part !== "" && part !== "." && part !== "..");

function parseScopeProperties(text: string, unit: string) {
  const values = new Map<string, string>();
  for (const line of text.split("\n")) {
    if (line === "") continue;
    const index = line.indexOf("=");
    const key = line.slice(0, index);
    if (index < 1 || !BINDING_SCOPE_PROPERTIES.includes(key) || values.has(key)) return undefined;
    values.set(key, line.slice(index + 1));
  }
  if (values.size !== BINDING_SCOPE_PROPERTIES.length || values.get("Id") !== unit)
    return undefined;
  return values;
}

function parseBindingScope(values: ReadonlyMap<string, string>) {
  const invocationId = values.get("InvocationID");
  const controlGroup = values.get("ControlGroup");
  if (
    values.get("LoadState") !== "loaded" ||
    values.get("ActiveState") !== "active" ||
    values.get("SubState") !== "running" ||
    invocationId === undefined ||
    !/^[0-9a-f]{32}$/.test(invocationId) ||
    /^0+$/.test(invocationId) ||
    controlGroup === undefined ||
    !isCgroupPath(controlGroup) ||
    !controlGroup.endsWith(`/${values.get("Id")}`)
  )
    return undefined;
  return { invocationId, controlGroup };
}

function parseRetiredScope(values: ReadonlyMap<string, string>, binding: AgentScopeBinding) {
  if (values.get("ActiveState") !== "inactive" || values.get("SubState") !== "dead")
    return undefined;
  const group = values.get("ControlGroup");
  if (
    values.get("LoadState") === "loaded" &&
    values.get("InvocationID") === binding.invocationId &&
    (group === binding.controlGroup || group === "")
  )
    return "loaded" as const;
  if (values.get("LoadState") === "not-found" && values.get("InvocationID") === "" && group === "")
    return "collected" as const;
  return undefined;
}

function isEmptyCgroup(text: string) {
  const counters = new Map<string, string>();
  for (const line of text.split("\n")) {
    if (line === "") continue;
    const match = /^([a-z_]+) (0|[1-9]\d*)$/.exec(line);
    if (!match || counters.has(match[1]!)) return false;
    counters.set(match[1]!, match[2]!);
  }
  return counters.get("populated") === "0";
}

function parseProcessStart(text: string, pid: number) {
  const end = text.lastIndexOf(")");
  if (!text.startsWith(`${pid} (`) || end < `${pid} (`.length || text[end + 1] !== " ")
    return undefined;
  const fields = text
    .slice(end + 2)
    .trimEnd()
    .split(" ");
  if (
    fields.length < 20 ||
    !/^[RSDTtKWPI]$/.test(fields[0]!) ||
    !fields.slice(1, 20).every((field) => /^-?\d+$/.test(field)) ||
    !/^(0|[1-9]\d*)$/.test(fields[19]!)
  )
    return undefined;
  return fields[19];
}

function parseUnifiedCgroup(text: string) {
  let unified: string | undefined;
  for (const line of text.split("\n")) {
    if (line === "") continue;
    const match = /^(0|[1-9]\d*):([^:]*):(\/.*)$/.exec(line);
    if (!match) return undefined;
    if (match[1] === "0") {
      if (match[2] !== "" || unified !== undefined || !isCgroupPath(match[3]!)) return undefined;
      unified = match[3];
    } else if (match[2] === "") return undefined;
  }
  return unified;
}

// systemd's default 90 s stop timeout would outlast our leftover-agent wait.
// Delegation lets rootless containers (Podman, kind) manage their own cgroups.
const SCOPE_OPTIONS = ["--property=TimeoutStopSec=10s", "--property=Delegate=yes"];
const STOPPING_POLL_INTERVAL = "100 millis";
const STOPPING_POLL_ATTEMPTS = 20;
const CLEAR_FAILED_INTERVAL = "1 second";
const CLEAR_FAILED_ATTEMPTS = 120;
const MAX_TRACKED_THREADS = 500;

export const make = Effect.gen(function* () {
  const platform = yield* HostProcess.Platform;
  if (platform !== "linux") return AgentScope.defaultValue();

  const runner = yield* ProcessRunner.ProcessRunner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const hostEnvironment = yield* HostProcess.Environment;
  const { stateDir } = yield* ServerConfig.ServerConfig;
  const ownedSlice = ownedAgentSlice(stateDir);
  const layerScope = yield* Effect.scope;

  const resolve = (command: string, env: NodeJS.ProcessEnv) =>
    resolveCommandPath(command, { env }).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.provideService(HostProcess.Platform, platform),
    );

  const run = (command: string, args: ReadonlyArray<string>) =>
    runner.run({ command, args, timeout: "5 seconds" }).pipe(Effect.option);

  const readCgroupFile = (controlGroup: string, file: string) =>
    fileSystem
      .readFileString(path.join("/sys/fs/cgroup", controlGroup, file))
      .pipe(Effect.orElseSucceed(() => ""));

  // Old agents keep their scopes when the server is killed, and the restarted
  // server resumes their sessions. Stop them first, so two copies of one
  // session never work in the same worktree.
  const stopLeftoverAgents = Effect.fn("agentScope.stopLeftoverAgents")(function* (
    systemctl: string,
  ) {
    const shown = yield* run(systemctl, [
      "--user",
      "show",
      ownedSlice,
      "--property=ActiveState,ControlGroup",
    ]);
    if (shown._tag === "None") return;
    const values = parseKeyValues(shown.value.stdout);
    if (values.get("ActiveState") !== "active") return;
    const controlGroup = values.get("ControlGroup") ?? "";
    const events = controlGroup ? yield* readCgroupFile(controlGroup, "cgroup.events") : "";
    if (parseCgroupCounter(events, "populated") === 0) return;
    yield* Effect.logWarning("Stopping agents left by a previous server", { slice: ownedSlice });
    const stopped = yield* runner
      .run({ command: systemctl, args: ["--user", "stop", ownedSlice], timeout: "30 seconds" })
      .pipe(Effect.option);
    if (stopped._tag === "None" || stopped.value.code !== 0) {
      yield* Effect.logWarning("Could not stop agents left by a previous server", {
        slice: ownedSlice,
        stderr: stopped._tag === "Some" ? stopped.value.stderr.trim() : undefined,
      });
    }
  });

  const systemd = yield* Effect.cached(
    Effect.gen(function* () {
      const runtimeDir = hostEnvironment.XDG_RUNTIME_DIR;
      if (runtimeDir === undefined || runtimeDir.length === 0) return undefined;
      const systemdRun = yield* resolve("systemd-run", hostEnvironment);
      const systemctl = yield* resolve("systemctl", hostEnvironment);
      if (systemdRun === null || systemctl === null) return undefined;
      yield* stopLeftoverAgents(systemctl);
      const probe = (options: ReadonlyArray<string>) =>
        run(systemdRun, [
          "--user",
          "--scope",
          "--quiet",
          "--collect",
          `--slice=${AGENT_SLICE}`,
          ...options,
          "--",
          "true",
        ]).pipe(Effect.map((result) => result._tag === "Some" && result.value.code === 0));
      // Scopes take OOMPolicy from systemd 253; older managers still get scopes.
      const supportsOomPolicy = yield* probe([...SCOPE_OPTIONS, "--property=OOMPolicy=stop"]);
      if (!supportsOomPolicy && !(yield* probe(SCOPE_OPTIONS))) {
        yield* Effect.logInfo("Agent scopes are off: no systemd user manager");
        return undefined;
      }
      const setProperties = Effect.fn("agentScope.setProperties")(function* (
        slice: string,
        properties: ReadonlyArray<string>,
      ) {
        const result = yield* run(systemctl, [
          "--user",
          "set-property",
          "--runtime",
          slice,
          ...properties,
        ]);
        if (result._tag === "None" || result.value.code !== 0) {
          yield* Effect.logWarning("Could not set resource limits on an agents slice", {
            slice,
            properties,
            stderr: result._tag === "Some" ? result.value.stderr.trim() : undefined,
          });
        }
      });
      const limits = agentSliceMemoryLimits(NodeOS.totalmem());
      yield* setProperties(AGENT_SLICE, [`MemoryHigh=${limits.high}`, `MemoryMax=${limits.max}`]);
      yield* setProperties(AGENTS_PARENT_SLICE, AGENTS_PARENT_PROPERTIES);
      yield* Effect.logInfo("Agent scopes are on", {
        slice: AGENT_SLICE,
        ...limits,
        oomPolicy: supportsOomPolicy,
      });
      return {
        systemdRun,
        systemctl,
        runtimeDir,
        supportsOomPolicy,
      };
    }),
  );

  const runCustodyCommand = Effect.fn("AgentScope.runCustodyCommand")(function* (
    systemctl: string,
    args: ReadonlyArray<string>,
    timeout: "5 seconds" | "30 seconds",
  ) {
    const result = yield* runner
      .run({
        command: systemctl,
        args,
        timeout,
        maxOutputBytes: 16_384,
        outputMode: "error",
        timeoutBehavior: "error",
      })
      .pipe(Effect.catchCause(recoverBindingRead), Effect.interruptible);
    if (
      result === undefined ||
      result.code !== 0 ||
      result.timedOut !== false ||
      result.stdoutTruncated !== false ||
      result.stderrTruncated !== false ||
      result.stdoutInvalidUtf8 !== false ||
      result.stderrInvalidUtf8 !== false
    )
      return undefined;
    return result;
  });
  const readStrictScope = Effect.fn("AgentScope.readStrictScope")(function* (
    systemctl: string,
    unit: string,
  ) {
    const result = yield* runCustodyCommand(
      systemctl,
      ["--user", "show", unit, `--property=${BINDING_SCOPE_PROPERTIES.join(",")}`],
      "5 seconds",
    );
    return result === undefined ? undefined : parseScopeProperties(result.stdout, unit);
  });
  const readBindingScope = Effect.fn("AgentScope.readBindingScope")(function* (
    systemctl: string,
    unit: string,
  ) {
    const values = yield* readStrictScope(systemctl, unit);
    return values === undefined ? undefined : parseBindingScope(values);
  });
  const readBindingProc = Effect.fn("AgentScope.readBindingProc")(function* (file: string) {
    return yield* fileSystem
      .readFileString(file)
      .pipe(Effect.catchCause(recoverBindingRead), Effect.interruptible);
  });
  const missing = Object.freeze({ _tag: "Missing" as const });
  const recoverMissing = Effect.fn("AgentScope.recoverMissing")(function* (
    cause: Cause.Cause<PlatformError.PlatformError>,
  ) {
    yield* recoverBindingRead(cause);
    const reason = cause.reasons[0];
    return cause.reasons.length === 1 &&
      reason &&
      Cause.isFailReason(reason) &&
      reason.error.reason._tag === "NotFound"
      ? missing
      : undefined;
  });
  const observeOriginalCgroup = Effect.fn("AgentScope.observeOriginalCgroup")(function* (
    binding: AgentScopeBinding,
  ) {
    const directory = path.join("/sys/fs/cgroup", binding.controlGroup);
    const info = yield* fileSystem
      .stat(directory)
      .pipe(Effect.catchCause(recoverMissing), Effect.interruptible);
    if (info === missing) return "removed" as const;
    if (!info || !("type" in info) || info.type !== "Directory") return undefined;
    const events = yield* fileSystem
      .readFileString(path.join(directory, "cgroup.events"))
      .pipe(Effect.catchCause(recoverMissing), Effect.interruptible);
    return typeof events === "string" && isEmptyCgroup(events) ? ("empty" as const) : undefined;
  });
  const originalPidGone = Effect.fn("AgentScope.originalPidGone")(function* (
    binding: AgentScopeBinding,
  ) {
    const stat = yield* fileSystem
      .readFileString(`/proc/${binding.pid}/stat`)
      .pipe(Effect.catchCause(recoverMissing), Effect.interruptible);
    if (stat === missing) return true;
    if (typeof stat !== "string") return false;
    const start = parseProcessStart(stat, binding.pid);
    return start !== undefined && start !== binding.pidStartTicks;
  });
  const observeRetiredSettlement = Effect.fn("AgentScope.observeRetiredSettlement")(function* (
    systemctl: string,
    binding: AgentScopeBinding,
  ) {
    const firstValues = yield* readStrictScope(systemctl, binding.unit);
    const firstUnit = firstValues ? parseRetiredScope(firstValues, binding) : undefined;
    if (!firstUnit) return undefined;
    const firstGroup = yield* observeOriginalCgroup(binding);
    if (!firstGroup || !(yield* originalPidGone(binding)) || !(yield* originalPidGone(binding)))
      return undefined;
    const lastGroup = yield* observeOriginalCgroup(binding);
    if (!lastGroup || (firstGroup === "removed" && lastGroup !== "removed")) return undefined;
    const lastValues = yield* readStrictScope(systemctl, binding.unit);
    const lastUnit = lastValues ? parseRetiredScope(lastValues, binding) : undefined;
    if (!lastUnit || (firstUnit === "collected" && lastUnit !== "collected")) return undefined;
    return Object.freeze({ binding, observation: lastGroup });
  });
  const observeSettlement = Effect.fn("AgentScope.observeSettlement")(function* (
    systemctl: string,
    binding: AgentScopeBinding,
  ) {
    const current = yield* readBindingScope(systemctl, binding.unit);
    if (
      !current ||
      current.invocationId !== binding.invocationId ||
      current.controlGroup !== binding.controlGroup
    )
      return undefined;
    if (
      (yield* runCustodyCommand(systemctl, ["--user", "stop", binding.unit], "30 seconds")) ===
      undefined
    )
      return undefined;
    return yield* observeRetiredSettlement(systemctl, binding);
  });
  const makeCustody = Effect.fn("AgentScope.makeCustody")(function* (
    systemctl: string,
    unit: string,
  ) {
    let pinned: AgentScopeBinding | undefined;
    let retiring = false;
    const completion = yield* Deferred.make<AgentScopeSettlement | undefined>();
    return Object.freeze({
      bind: Effect.fn("AgentScope.bind")(function* (pid: number) {
        if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
        const firstUnit = yield* readBindingScope(systemctl, unit);
        if (!firstUnit) return undefined;
        const firstStat = yield* readBindingProc(`/proc/${pid}/stat`);
        const firstStart = firstStat === undefined ? undefined : parseProcessStart(firstStat, pid);
        if (firstStart === undefined) return undefined;
        const firstMembership = yield* readBindingProc(`/proc/${pid}/cgroup`);
        const firstGroup =
          firstMembership === undefined ? undefined : parseUnifiedCgroup(firstMembership);
        if (
          firstGroup === undefined ||
          (firstGroup !== firstUnit.controlGroup &&
            !firstGroup.startsWith(firstUnit.controlGroup + "/"))
        )
          return undefined;
        const lastMembership = yield* readBindingProc(`/proc/${pid}/cgroup`);
        const lastGroup =
          lastMembership === undefined ? undefined : parseUnifiedCgroup(lastMembership);
        if (lastGroup !== firstGroup) return undefined;
        const lastStat = yield* readBindingProc(`/proc/${pid}/stat`);
        const lastStart = lastStat === undefined ? undefined : parseProcessStart(lastStat, pid);
        if (lastStart !== firstStart) return undefined;
        const lastUnit = yield* readBindingScope(systemctl, unit);
        if (
          !lastUnit ||
          lastUnit.invocationId !== firstUnit.invocationId ||
          lastUnit.controlGroup !== firstUnit.controlGroup
        )
          return undefined;
        const candidate = Object.freeze({ unit, ...firstUnit, pid, pidStartTicks: firstStart });
        return yield* Effect.sync(() => {
          if (!pinned) pinned = candidate;
          return pinned.pid === candidate.pid &&
            pinned.pidStartTicks === candidate.pidStartTicks &&
            pinned.invocationId === candidate.invocationId &&
            pinned.controlGroup === candidate.controlGroup
            ? pinned
            : undefined;
        });
      }),
      observeRetirement: Effect.fn("AgentScope.observeRetirement")(function* () {
        const binding = pinned;
        if (!binding) return undefined;
        return yield* observeRetiredSettlement(systemctl, binding).pipe(Effect.interruptible);
      }),
      stopAndObserve: Effect.fn("AgentScope.stopAndObserve")(function* () {
        const binding = pinned;
        if (!binding) return undefined;
        if (retiring) return yield* Deferred.await(completion).pipe(Effect.interruptible);
        retiring = true;
        return yield* observeSettlement(systemctl, binding).pipe(
          Effect.interruptible,
          Effect.onExit((exit) =>
            Deferred.succeed(completion, Exit.isSuccess(exit) ? exit.value : undefined).pipe(
              Effect.asVoid,
            ),
          ),
        );
      }, Effect.uninterruptible),
    }) satisfies AgentScopeCustody;
  });

  // `shared` scopes serve other threads too: tracked only for OOM diagnosis on old systemd.
  const scopeByThread = new Map<string, { unit: string; oomKilled: boolean; shared: boolean }>();

  const readScope = Effect.fn("agentScope.readScope")(function* (systemctl: string, unit: string) {
    const shown = yield* run(systemctl, [
      "--user",
      "show",
      unit,
      "--property=LoadState,ActiveState,Result,ControlGroup",
    ]);
    if (shown._tag === "None" || shown.value.code !== 0) return undefined;
    const values = parseKeyValues(shown.value.stdout);
    const controlGroup = values.get("ControlGroup") ?? "";
    const memoryEvents = controlGroup ? yield* readCgroupFile(controlGroup, "memory.events") : "";
    const cgroupEvents = controlGroup ? yield* readCgroupFile(controlGroup, "cgroup.events") : "";
    const populated = parseCgroupCounter(cgroupEvents, "populated");
    return {
      loadState: values.get("LoadState") ?? "",
      activeState: values.get("ActiveState") ?? "",
      result: values.get("Result") ?? "",
      oomKills: parseCgroupCounter(memoryEvents, "oom_kill"),
      populated: populated === undefined ? undefined : populated === 1,
    } satisfies ScopeState;
  });

  const clearFailed = Effect.fn("agentScope.clearFailed")(function* (
    systemctl: string,
    unit: string,
  ) {
    for (let attempt = 0; attempt < CLEAR_FAILED_ATTEMPTS; attempt++) {
      const state = yield* readScope(systemctl, unit);
      if (state === undefined) return;
      if (state.activeState === "failed") {
        yield* run(systemctl, ["--user", "reset-failed", unit]);
        return;
      }
      if (state.activeState !== "deactivating") return;
      yield* Effect.sleep(CLEAR_FAILED_INTERVAL);
    }
  });

  const forget = (systemctl: string, unit: string) =>
    clearFailed(systemctl, unit).pipe(Effect.forkIn(layerScope), Effect.asVoid);

  const evictOldest = Effect.fn("agentScope.evictOldest")(function* (systemctl: string) {
    const oldest = scopeByThread.entries().next();
    if (oldest.done === true) return;
    const [threadId, tracked] = oldest.value;
    const state = yield* readScope(systemctl, tracked.unit);
    if (scopeByThread.get(threadId) !== tracked) return;
    scopeByThread.delete(threadId);
    const status = state === undefined ? "running" : classifyScope(state);
    if (status === "running" || status === "stopping") {
      scopeByThread.set(threadId, tracked);
      return;
    }
    yield* forget(systemctl, tracked.unit);
  });

  const track = Effect.fn("agentScope.track")(function* (
    systemctl: string,
    threadId: string,
    unit: string | undefined,
    shared = false,
  ) {
    const previous = scopeByThread.get(threadId);
    scopeByThread.delete(threadId);
    if (unit !== undefined) scopeByThread.set(threadId, { unit, oomKilled: false, shared });
    if (previous !== undefined) yield* forget(systemctl, previous.unit);
    if (scopeByThread.size > MAX_TRACKED_THREADS) yield* evictOldest(systemctl);
  });

  const oomKilled: AgentScopeShape["oomKilled"] = Effect.fn("agentScope.oomKilled")(
    function* (threadId) {
      const tracked = scopeByThread.get(threadId);
      if (tracked === undefined) return false;
      if (tracked.oomKilled) return true;
      const scope = yield* systemd;
      if (scope === undefined) return false;
      for (let attempt = 0; attempt < STOPPING_POLL_ATTEMPTS; attempt++) {
        const state = yield* readScope(scope.systemctl, tracked.unit);
        if (state === undefined) return false;
        const status = classifyScope(state);
        if (status === "running") return false;
        if (status === "stopping") {
          yield* Effect.sleep(STOPPING_POLL_INTERVAL);
          continue;
        }
        yield* forget(scope.systemctl, tracked.unit);
        if (status === "gone") return false;
        yield* Effect.logWarning("Agent scope was killed: out of memory", {
          threadId,
          unit: tracked.unit,
        });
        tracked.oomKilled = true;
        return true;
      }
      return false;
    },
  );

  const stop = Effect.fn("AgentScope.stop")(function* (unit: string) {
    if (!unit.startsWith("t3code-") || !unit.endsWith(".scope")) return;
    const scope = yield* systemd;
    if (scope === undefined) return;
    const state = yield* readScope(scope.systemctl, unit);
    if (
      state === undefined ||
      state.loadState !== "loaded" ||
      state.activeState !== "active" ||
      state.result === "oom-kill" ||
      state.populated === false
    )
      return;
    // The kernel counts OOM kills before signaling the victim. Let OOMPolicy
    // record the result before a manual stop can overwrite it; an unreadable
    // counter cannot rule out that race. Older managers need us to stop it.
    if (scope.supportsOomPolicy && (state.oomKills === undefined || state.oomKills > 0)) return;
    const stopped = yield* run(scope.systemctl, ["--user", "stop", "--no-block", unit]);
    if (stopped._tag === "None" || stopped.value.code !== 0) {
      yield* Effect.logWarning("Could not stop an exited agent's scope", {
        unit,
        exitCode: stopped._tag === "Some" ? stopped.value.code : undefined,
      });
    }
  });

  const kill: AgentScopeShape["kill"] = Effect.fn("AgentScope.kill")(function* (threadId) {
    const tracked = scopeByThread.get(threadId);
    if (tracked === undefined || tracked.shared) return false;
    const scope = yield* systemd;
    if (scope === undefined) return false;
    const shown = yield* run(scope.systemctl, [
      "--user",
      "show",
      tracked.unit,
      "--property=ActiveState,ControlGroup",
    ]);
    if (shown._tag === "None" || shown.value.code !== 0) return false;
    const values = parseKeyValues(shown.value.stdout);
    if (values.get("ActiveState") !== "active") return false;
    const controlGroup = values.get("ControlGroup") ?? "";
    // cgroup.kill (Linux 5.14+) kills the whole tree at once; systemd's kill is the fallback.
    const killedByCgroup =
      controlGroup.length > 0 &&
      (yield* fileSystem
        .writeFileString(path.join("/sys/fs/cgroup", controlGroup, "cgroup.kill"), "1")
        .pipe(
          Effect.as(true),
          Effect.orElseSucceed(() => false),
        ));
    if (killedByCgroup) return true;
    const killed = yield* run(scope.systemctl, [
      "--user",
      "kill",
      "--signal=SIGKILL",
      tracked.unit,
    ]);
    return killed._tag === "Some" && killed.value.code === 0;
  });

  const clear: AgentScopeShape["clear"] = Effect.fn("agentScope.clear")(function* (threadId) {
    if (!scopeByThread.has(threadId)) return;
    const scope = yield* systemd;
    if (scope !== undefined) yield* track(scope.systemctl, threadId, undefined);
  });

  return {
    wrap: Effect.fn("AgentScope.wrap")(function* ({
      command,
      args,
      name,
      threadId,
      env,
      survivesRestart,
      shared,
    }) {
      const resolved = yield* resolve(command, env ?? hostEnvironment);
      if (resolved === null) return { command, args };
      const scope = yield* systemd;
      if (scope === undefined) return agentScopeCommand({ command: resolved, args });
      const unit = `t3code-${name}-${NodeCrypto.randomUUID()}.scope`;
      // A continuing shared scope cannot attribute a tool's OOM to one thread.
      // Clear the launch thread's previous scope instead.
      const continuing = shared === true && scope.supportsOomPolicy;
      const owned = threadId !== undefined && !continuing;
      if (threadId !== undefined)
        yield* track(scope.systemctl, threadId, owned ? unit : undefined, shared === true);
      const systemdScope: AgentSystemdScope = {
        systemdRun: scope.systemdRun,
        runtimeDir: scope.runtimeDir,
        slice: survivesRestart === true ? AGENT_SLICE : ownedSlice,
        unit,
        // Without an owning thread nothing reads the result, so let systemd
        // unload the unit even when it fails.
        options: [
          ...SCOPE_OPTIONS,
          ...(scope.supportsOomPolicy
            ? [`--property=OOMPolicy=${continuing ? "continue" : "stop"}`]
            : []),
          ...(owned ? [] : ["--collect"]),
        ],
      };
      return {
        ...agentScopeCommand({ command: resolved, args, scope: systemdScope }),
        unit,
        custody: yield* makeCustody(scope.systemctl, unit),
      };
    }),
    oomKilled,
    stop,
    kill,
    clear,
  } satisfies AgentScopeShape;
});

export const layer = Layer.effect(AgentScope, make).pipe(Layer.provide(ProcessRunner.layer));
