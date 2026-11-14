// @effect-diagnostics nodeBuiltinImport:off -- Pure synchronous hashing keeps page matching bounded without a runtime dependency.
import * as NodeCrypto from "node:crypto";
import type { ThreadId } from "@t3tools/contracts";
import type { HtmlRenderReference } from "@t3tools/shared/htmlRender";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import type { McpProviderSessionConfig } from "./mcpSession.ts";

// Other providers may never consume these references.
const MAX_KEPT_HTML_RENDERS = 64;

// Antigravity truncates long HTML arguments to a marked tail of at most 8,000 characters.
const BOUNDED_TEXT_MARKER = "[Earlier output truncated]\n\n";
const KEPT_HTML_END_LENGTH = 8_000;

interface KeptHtmlRender {
  readonly path: string | undefined;
  readonly title: string;
  readonly height: number;
  readonly htmlLength: number;
  readonly htmlHash: string;
  readonly htmlEnd: string;
  readonly reference: HtmlRenderReference;
}

const hashHtml = (html: string) => NodeCrypto.createHash("sha256").update(html).digest("hex");

export class McpProviderSessions extends Context.Service<
  McpProviderSessions,
  {
    readonly set: (config: McpProviderSessionConfig) => Effect.Effect<void>;
    readonly read: (threadId: ThreadId) => Effect.Effect<McpProviderSessionConfig | undefined>;
    readonly clear: (threadId: ThreadId) => Effect.Effect<void>;
    readonly recordHtmlRender: (
      threadId: ThreadId,
      input: {
        readonly html: string;
        readonly path?: string | undefined;
        readonly title: string;
        readonly height: number;
      },
      reference: HtmlRenderReference,
    ) => Effect.Effect<void>;
    // A reported attachment consumes that exact page; otherwise the oldest page for a path, or a
    // unique inline match.
    readonly takeHtmlRender: (
      threadId: ThreadId,
      input: PageInput,
      reported?: HtmlRenderReference,
    ) => Effect.Effect<HtmlRenderReference | undefined>;
  }
>()("@t3tools/provider-core/server/McpProviderSessions") {}

interface PageInput {
  readonly html?: unknown;
  readonly path?: unknown;
  readonly title?: unknown;
  readonly height?: unknown;
}

const matchingPageIndex = (kept: ReadonlyArray<KeptHtmlRender>, input: PageInput): number => {
  const height =
    typeof input.height === "string" && input.height.trim() !== ""
      ? Number(input.height)
      : input.height;
  const sameFrame = (page: KeptHtmlRender) => page.title === input.title && page.height === height;
  const path = input.path;
  // The handler refuses a call with both sources, so it published nothing to recover. Its decoder
  // reads null as an omitted source.
  if (path !== undefined && path !== null && input.html !== undefined && input.html !== null) {
    return -1;
  }
  if (typeof path === "string" && path.length > 0) {
    // An edited file is published again under the same path; completions arrive in publish order.
    return kept.findIndex((page) => page.path === path && sameFrame(page));
  }
  const html = input.html;
  if (typeof html !== "string" || html.length === 0) {
    return -1;
  }
  const htmlHash = hashHtml(html);
  const tail = html.startsWith(BOUNDED_TEXT_MARKER)
    ? html.slice(BOUNDED_TEXT_MARKER.length)
    : undefined;
  const matches = kept.flatMap((page, index) =>
    page.path === undefined &&
    sameFrame(page) &&
    (page.htmlHash === htmlHash ||
      (tail !== undefined &&
        tail.length > 0 &&
        tail.length < page.htmlLength &&
        page.htmlEnd.endsWith(tail)))
      ? [index]
      : [],
  );
  return matches.length === 1 ? matches[0]! : -1;
};

interface SessionState {
  readonly sessions: ReadonlyMap<ThreadId, McpProviderSessionConfig>;
  readonly htmlRenders: ReadonlyMap<ThreadId, ReadonlyArray<KeptHtmlRender>>;
}

const serviceStates = new WeakMap<object, Ref.Ref<SessionState>>();

export const isService = (service: object) => serviceStates.has(service);

const replaceSession = (
  current: SessionState,
  threadId: ThreadId,
  replacement: McpProviderSessionConfig | undefined,
): SessionState => {
  const sessions = new Map(current.sessions);
  if (replacement === undefined) sessions.delete(threadId);
  else sessions.set(threadId, replacement);
  if (
    replacement === undefined ||
    current.sessions.get(threadId)?.providerSessionId !== replacement.providerSessionId
  ) {
    const htmlRenders = new Map(current.htmlRenders);
    htmlRenders.delete(threadId);
    return { sessions, htmlRenders };
  }
  return { sessions, htmlRenders: current.htmlRenders };
};

export const replaceIfCurrent = Effect.fn("McpProviderSessions.replaceIfCurrent")(function* (
  service: object,
  threadId: ThreadId,
  expected: McpProviderSessionConfig | undefined,
  replacement: McpProviderSessionConfig | undefined,
) {
  const state = serviceStates.get(service);
  if (!state) return false;
  return yield* Ref.modify(state, (current) => {
    if (
      !threadId.trim() ||
      (expected !== undefined && expected.threadId !== threadId) ||
      (replacement !== undefined && replacement.threadId !== threadId) ||
      current.sessions.get(threadId) !== expected
    )
      return [false, current];
    return [true, replaceSession(current, threadId, replacement)];
  });
});

const make = Effect.gen(function* () {
  const state = yield* Ref.make<SessionState>({ sessions: new Map(), htmlRenders: new Map() });
  const service = McpProviderSessions.of({
    set: Effect.fn("McpProviderSessions.set")(function (config) {
      return Ref.update(state, (current) => replaceSession(current, config.threadId, config));
    }),
    read: Effect.fn("McpProviderSessions.read")(function (threadId) {
      return Ref.get(state).pipe(Effect.map((current) => current.sessions.get(threadId)));
    }),
    clear: Effect.fn("McpProviderSessions.clear")(function (threadId) {
      return Ref.update(state, (current) => replaceSession(current, threadId, undefined));
    }),
    recordHtmlRender: Effect.fn("McpProviderSessions.recordHtmlRender")(
      function (threadId, input, reference) {
        return Ref.update(state, (current) => ({
          sessions: current.sessions,
          htmlRenders: new Map(current.htmlRenders).set(
            threadId,
            [
              ...(current.htmlRenders.get(threadId) ?? []),
              {
                path: input.path,
                title: input.title,
                height: input.height,
                htmlLength: input.html.length,
                htmlHash: hashHtml(input.html),
                // Copy the tail so its backing storage cannot retain the entire HTML string.
                htmlEnd: Buffer.from(input.html.slice(-KEPT_HTML_END_LENGTH), "utf16le").toString(
                  "utf16le",
                ),
                reference,
              },
            ].slice(-MAX_KEPT_HTML_RENDERS),
          ),
        }));
      },
    ),
    takeHtmlRender: Effect.fn("McpProviderSessions.takeHtmlRender")(
      function (threadId, input, reported) {
        return Ref.modify(state, (current) => {
          const kept = current.htmlRenders.get(threadId) ?? [];
          const index =
            reported === undefined
              ? matchingPageIndex(kept, input)
              : kept.findIndex((page) => page.reference.attachmentId === reported.attachmentId);
          if (index === -1) {
            return [undefined, current];
          }
          const rest = kept.filter((_, keptIndex) => keptIndex !== index);
          const next = new Map(current.htmlRenders);
          if (rest.length === 0) {
            next.delete(threadId);
          } else {
            next.set(threadId, rest);
          }
          return [kept[index]!.reference, { sessions: current.sessions, htmlRenders: next }];
        });
      },
    ),
  });
  serviceStates.set(service, state);
  return service;
});

export const layer = Layer.effect(McpProviderSessions, make);
