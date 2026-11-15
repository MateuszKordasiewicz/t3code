import { createFileRoute } from "@tanstack/react-router";
import { WorkInbox } from "../components/WorkInbox";

export const Route = createFileRoute("/_chat/work")({ component: WorkInbox });
