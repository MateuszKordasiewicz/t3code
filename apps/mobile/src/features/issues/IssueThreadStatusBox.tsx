import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import {
  createIssueThreadAtoms,
  issueThreadIndexKey,
} from "@t3tools/client-runtime/issue-thread-status";
import { msg, useGT, useMessages } from "gt-react-native";
import { Pressable } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { cn } from "../../lib/cn";
import { environmentThreadShells } from "../../state/threads";
import { DONE_STATUS_LABEL, STATUS_LABEL_BY_STATUS } from "../threads/thread-list-v2-items";
import { issueThreadStatusBox, type IssueThreadBoxStatus } from "./issueThreadStatusBox.logic";

const { openIssueThreadsAtom } = createIssueThreadAtoms(
  environmentThreadShells.quietThreadShellsAtom,
);

const WORKING_LABEL = msg("Working", { $context: "thread status" });
const WAITING_LABEL = msg("Waiting", { $context: "thread status" });

/** The thread list's look for a status; the list draws working and waiting as an icon alone. */
function statusAppearance(status: IssueThreadBoxStatus) {
  const entry =
    status === "done"
      ? DONE_STATUS_LABEL
      : status === "idle"
        ? STATUS_LABEL_BY_STATUS.waiting!
        : STATUS_LABEL_BY_STATUS[status]!;
  return { ...entry, label: entry.label ?? (status === "working" ? WORKING_LABEL : WAITING_LABEL) };
}

export interface IssueThreadTarget {
  readonly environmentId: string;
  readonly threadId: string;
}

/**
 * The most urgent open thread linked to an issue: its label, what speech reads for the box, and
 * the thread it opens. Null when there is none.
 */
export function useIssueThreadStatusBox(item: EnvironmentIssueEntry) {
  const gt = useGT();
  const m = useMessages();
  const threads = useAtomValue(openIssueThreadsAtom(issueThreadIndexKey(item.environmentId, item)));
  const box = issueThreadStatusBox(threads);
  if (box === null) return null;
  const appearance = statusAppearance(box.status);
  const label = m(appearance.label);
  const more =
    box.others > 0
      ? gt("{count, plural, one {# more linked thread} other {# more linked threads}}", {
          count: box.others,
        })
      : null;
  return {
    others: box.others,
    appearance,
    label,
    accessibilityLabel: [gt("Open thread {title}", { title: box.thread.title }), label, more]
      .filter(Boolean)
      .join(", "),
    target: {
      environmentId: String(box.thread.environmentId),
      threadId: String(box.thread.id),
    } satisfies IssueThreadTarget,
  };
}

export function IssueThreadStatusBox(props: {
  readonly box: NonNullable<ReturnType<typeof useIssueThreadStatusBox>>;
  readonly mutedClassName: string;
  readonly onOpenThread: (target: IssueThreadTarget) => void;
}) {
  const { appearance, label, others, accessibilityLabel, target } = props.box;
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={accessibilityLabel}
      hitSlop={8}
      onPress={() => props.onOpenThread(target)}
      className="shrink-0 flex-row items-center gap-1 rounded border border-border-subtle px-1.5 py-0.5"
    >
      <SymbolView
        name={appearance.icon}
        size={11}
        tintColorClassName={appearance.iconTintClassName}
        type="monochrome"
        weight="semibold"
      />
      <Text className={cn("text-2xs font-t3-bold", appearance.className)}>{label}</Text>
      {others > 0 ? (
        <Text className={cn("text-2xs tabular-nums", props.mutedClassName)}>+{others}</Text>
      ) : null}
    </Pressable>
  );
}
