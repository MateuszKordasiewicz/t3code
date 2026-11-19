import { useEffect, useId, useState } from "react";
import { T, useGT } from "gt-react";
import { create } from "zustand";
import { THREAD_TRIAGE_NOTE_MAX_LENGTH } from "@t3tools/contracts";

import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
  DialogFooter,
} from "./ui/dialog";

/** `undefined` means cancelled; `null` means no note. */
type NoteChoice = string | null | undefined;
type Request = { readonly initial: string; readonly resolve: (choice: NoteChoice) => void };
const useRequest = create<{ request: Request | null }>(() => ({ request: null }));

export function requestThreadTriageNote(initial: string | null): Promise<NoteChoice> {
  useRequest.getState().request?.resolve(undefined);
  return new Promise((resolve) =>
    useRequest.setState({ request: { initial: initial ?? "", resolve } }),
  );
}

function finish(choice: NoteChoice) {
  const request = useRequest.getState().request;
  useRequest.setState({ request: null });
  request?.resolve(choice);
}

export function ThreadTriageNoteDialogHost() {
  const request = useRequest((state) => state.request);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- resolves any pending note request promise as cancelled when the dialog host unmounts
  useEffect(() => () => finish(undefined), []);
  return request ? <ThreadTriageNoteDialog initial={request.initial} /> : null;
}

function ThreadTriageNoteDialog(props: { readonly initial: string }) {
  const gt = useGT();
  const id = useId();
  const [note, setNote] = useState(props.initial);
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) finish(undefined);
      }}
    >
      <DialogPopup className="sm:max-w-md">
        <form
          className="flex min-h-0 flex-col"
          onSubmit={(event) => {
            event.preventDefault();
            const trimmed = note.replace(/\s+/g, " ").trim();
            finish(trimmed === "" ? null : trimmed);
          }}
        >
          <DialogHeader>
            <DialogTitle>
              <T>My move</T>
            </DialogTitle>
            <DialogDescription>
              <T>Mark this thread as waiting on you. Sending it a new message clears the label.</T>
            </DialogDescription>
          </DialogHeader>
          <DialogPanel>
            <Label className="flex min-w-0 flex-col items-stretch" htmlFor={`${id}-note`}>
              <T>What you need to do (optional)</T>
              <Input
                nativeInput
                autoFocus
                id={`${id}-note`}
                maxLength={THREAD_TRIAGE_NOTE_MAX_LENGTH}
                placeholder={gt("e.g. try PR #49")}
                value={note}
                onChange={(event) => setNote(event.target.value)}
              />
            </Label>
          </DialogPanel>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => finish(undefined)}>
              <T>Cancel</T>
            </Button>
            <Button type="submit">
              <T context="verb, save the label">Save</T>
            </Button>
          </DialogFooter>
        </form>
      </DialogPopup>
    </Dialog>
  );
}
