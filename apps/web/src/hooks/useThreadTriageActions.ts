import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { ScopedThreadRef, ThreadTriageLabel } from "@t3tools/contracts";
import { useGT } from "gt-react";

import { requestThreadTriageNote } from "../components/ThreadTriageNoteDialog";
import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { m } from "../i18n/messages";
import { threadEnvironment } from "../state/threads";
import { useOrchestrationCommand } from "../state/use-orchestration-command";

export function useThreadTriageActions() {
  const gt = useGT();
  const setTriageLabel = useOrchestrationCommand(threadEnvironment.setTriageLabel, {
    reportFailure: false,
  });

  const setLabel = async (threadRef: ScopedThreadRef, label: ThreadTriageLabel | null) => {
    const result = await setTriageLabel({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId, label },
    });
    if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: gt("Failed to update the thread label"),
          description:
            thrownErrorMessage(squashAtomCommandFailure(result), m) ?? gt("An error occurred."),
        }),
      );
    }
  };

  /** Asks for the note, then marks the thread as the user's move. */
  const markMyMove = async (threadRef: ScopedThreadRef, current: ThreadTriageLabel | null) => {
    const note = await requestThreadTriageNote(current?.note ?? null);
    if (note === undefined) return;
    await setLabel(threadRef, { kind: "my-move", note });
  };

  const clearLabel = (threadRef: ScopedThreadRef) => setLabel(threadRef, null);

  return { markMyMove, clearLabel };
}
