import "~/index.css";
import type { EnvironmentIssueEntry } from "@t3tools/client-runtime/issue-list";
import { EnvironmentId, IssueTrackerKind, ProjectId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

vi.mock(import("./IssueThreadStatusBox"), () => ({
  useIssueThreadStatus: (() => null) as never,
  IssueThreadStatusBoxSpacer: (() => null) as never,
  IssueThreadStatusBoxView: (() => null) as never,
}));

import { render } from "../../../test/render";
import { IssueGraphView } from "./IssueGraphView";

const blockers = ["t3-alpha.1", "t3-bravo.2", "t3-charlie.3", "t3-delta.4", "t3-echo.5"];

const entry: EnvironmentIssueEntry = {
  environmentId: EnvironmentId.make("env-1"),
  projectId: ProjectId.make("project-1"),
  projectTitle: "T3",
  tracker: IssueTrackerKind.make("beads"),
  host: "local",
  scope: "t3",
  key: "t3-x",
  displayKey: "t3-x",
  title: "Blocked card",
  url: "beads://t3-x",
  author: null,
  state: { category: "open", name: "open" },
  createdAt: "2026-11-01T00:00:00.000Z",
  updatedAt: "2026-11-01T00:00:00.000Z",
  closedAt: null,
  assignees: [],
  labels: [],
  commentCount: 0,
  dependsOnKeys: blockers,
};

const popupText = () => document.querySelector('[data-slot="tooltip-popup"]')?.textContent;

it("shows every blocked-by name on hover and on keyboard focus, and announces them once", async () => {
  await using _screen = await render(
    <IssueGraphView entries={[entry]} groupBy="tree" selectedId={null} onSelect={() => {}} />,
  );
  const full = `Blocked by ${blockers.join(", ")}`;
  const card = page.getByRole("button", { name: /Blocked card/ });
  // The card's name carries the list once; the tooltip is hidden from screen readers.
  await expect
    .element(card)
    .toHaveAccessibleName(
      new RegExp(`^(?!.*Blocked by.*Blocked by).*${full.replaceAll(".", "\\.")}`),
    );

  await userEvent.hover(card);
  await vi.waitFor(() => expect(popupText()).toBe(full));
  expect(document.querySelector('[data-slot="tooltip-popup"]')?.getAttribute("aria-hidden")).toBe(
    "true",
  );
  await userEvent.unhover(card);
  await vi.waitFor(() => expect(popupText()).toBeUndefined());

  for (let i = 0; i < 5 && document.activeElement !== card.element(); i++) await userEvent.tab();
  expect(document.activeElement).toBe(card.element());
  await vi.waitFor(() => expect(popupText()).toBe(full));
});

it("keeps keyboard focus on a card when a refresh draws its missing blocker", async () => {
  const blocked = { ...entry, dependsOnKeys: ["t3-b"] };
  const blocker = {
    ...entry,
    key: "t3-b",
    displayKey: "t3-b",
    title: "Blocker card",
    url: "beads://t3-b",
    dependsOnKeys: [],
  };
  const onSelect = vi.fn();
  await using screen = await render(
    <IssueGraphView entries={[blocked]} groupBy="tree" selectedId={null} onSelect={onSelect} />,
  );
  const card = page.getByRole("button", { name: /Blocked card/ });
  for (let i = 0; i < 5 && document.activeElement !== card.element(); i++) await userEvent.tab();
  await expect.element(card).toHaveFocus();

  await screen.rerender(
    <IssueGraphView
      entries={[blocker, blocked]}
      groupBy="tree"
      selectedId={null}
      onSelect={onSelect}
    />,
  );
  // The blocker is now drawn as an arrow, so the label and its tooltip are gone.
  await expect.element(page.getByText("Blocker card")).toBeVisible();
  await expect.element(card).toHaveFocus();
  await userEvent.keyboard("{Enter}");
  expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ key: "t3-x" }));
});
