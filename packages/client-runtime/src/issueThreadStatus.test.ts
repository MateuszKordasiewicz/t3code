import { EnvironmentId, IssueTrackerKind } from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";

import {
  createIssueThreadAtoms,
  indexOpenIssueThreads,
  issueThreadIndexKey,
  mostUrgentIssueThread,
} from "./issueThreadStatus.ts";

const local = EnvironmentId.make("env-local");
const remote = EnvironmentId.make("env-remote");
const issue = {
  tracker: IssueTrackerKind.make("github"),
  host: "github.com",
  scope: "acme/app",
  key: "12",
};

function link(overrides: Partial<typeof issue> = {}) {
  return { ...issue, ...overrides } as never;
}

function shell(
  id: string,
  options: {
    environmentId?: EnvironmentId;
    issues?: ReadonlyArray<never>;
    archived?: boolean;
    deleted?: boolean;
    settledOverride?: "settled" | "active" | null;
  } = {},
) {
  return {
    id,
    environmentId: options.environmentId ?? local,
    issues: options.issues ?? [link()],
    archivedAt: options.archived ? "2026-11-10T00:00:00.000Z" : null,
    deletedAt: options.deleted ? "2026-11-10T00:00:00.000Z" : null,
    settledOverride: options.settledOverride ?? null,
  };
}

const ids = (threads: ReadonlyArray<{ id: string }> | undefined) =>
  (threads ?? []).map((thread) => thread.id);

describe("indexOpenIssueThreads", () => {
  it("matches an issue at tracker level, per environment", () => {
    const index = indexOpenIssueThreads([
      shell("a"),
      shell("b", { issues: [link({ host: "GitHub.com", scope: "Acme/App" })] }),
      shell("c", { environmentId: remote }),
      shell("d", { issues: [link({ key: "13" })] }),
      shell("e", { issues: [] }),
    ]);
    expect(ids(index.get(issueThreadIndexKey(local, issue)))).toEqual(["a", "b"]);
    expect(ids(index.get(issueThreadIndexKey(remote, issue)))).toEqual(["c"]);
    expect(ids(index.get(issueThreadIndexKey(local, { ...issue, key: "13" })))).toEqual(["d"]);
  });

  it("leaves out settled, archived and deleted threads, and counts a double link once", () => {
    const index = indexOpenIssueThreads([
      shell("settled", { settledOverride: "settled" }),
      shell("archived", { archived: true }),
      shell("deleted", { deleted: true }),
      shell("reopened", {
        settledOverride: "active",
        issues: [link(), link({ host: "GITHUB.COM" })],
      }),
    ]);
    expect(ids(index.get(issueThreadIndexKey(local, issue)))).toEqual(["reopened"]);
  });
});

describe("mostUrgentIssueThread", () => {
  const order = ["approval", "working", "idle"];
  const rank = (status: string) => order.indexOf(status);

  it("shows the most urgent thread and counts the rest", () => {
    const statuses: Record<string, string> = { a: "idle", b: "approval", c: "working" };
    const choice = mostUrgentIssueThread(
      [{ id: "a" }, { id: "b" }, { id: "c" }],
      (thread) => statuses[thread.id]!,
      rank,
    );
    expect(choice).toEqual({ thread: { id: "b" }, status: "approval", others: 2 });
  });

  it("keeps the earlier thread on a tie, and answers null with no thread", () => {
    const choice = mostUrgentIssueThread([{ id: "a" }, { id: "b" }], () => "working", rank);
    expect(choice).toEqual({ thread: { id: "a" }, status: "working", others: 1 });
    expect(mostUrgentIssueThread([], () => "working", rank)).toBeNull();
  });
});

describe("createIssueThreadAtoms", () => {
  it("keeps an issue's value while only other issues' threads change", () => {
    const shellsAtom = Atom.make<ReadonlyArray<ReturnType<typeof shell>>>([
      shell("a"),
      shell("b", { issues: [link({ key: "13" })] }),
    ]);
    const { openIssueThreadsAtom } = createIssueThreadAtoms(shellsAtom);
    const registry = AtomRegistry.make();
    const atom = openIssueThreadsAtom(issueThreadIndexKey(local, issue));
    const dispose = registry.mount(atom);
    try {
      const first = registry.get(atom);
      expect(ids(first)).toEqual(["a"]);
      const [a] = registry.get(shellsAtom);
      registry.set(shellsAtom, [a!, shell("b", { issues: [link({ key: "14" })] })]);
      expect(registry.get(atom)).toBe(first);
      registry.set(shellsAtom, [a!, shell("b")]);
      expect(ids(registry.get(atom))).toEqual(["a", "b"]);
    } finally {
      dispose();
      registry.dispose();
    }
  });
});
