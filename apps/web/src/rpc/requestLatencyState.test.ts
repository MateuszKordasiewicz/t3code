import { WS_METHODS } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  acknowledgeRpcRequest,
  type SlowRpcAckRequest,
  subscribeSlowRpcAckRequests,
  trackRpcRequestSent,
} from "./requestLatencyState";

function observeSlowRequests() {
  vi.useFakeTimers();
  let current: ReadonlyArray<SlowRpcAckRequest> = [];
  const unsubscribe = subscribeSlowRpcAckRequests((requests) => {
    current = requests;
  });
  return {
    get current() {
      return current;
    },
    [Symbol.dispose]() {
      for (const request of current) {
        acknowledgeRpcRequest(request.requestId);
      }
      unsubscribe();
      vi.useRealTimers();
    },
  };
}

describe("requestLatencyState", () => {
  it("ignores issues.* and pullRequests.* while still warning on other unacknowledged requests", () => {
    using slow = observeSlowRequests();

    trackRpcRequestSent("issue-1", WS_METHODS.issuesDetail, `${WS_METHODS.issuesDetail} · bazzite`);
    trackRpcRequestSent("issue-2", WS_METHODS.issuesList, `${WS_METHODS.issuesList} · bazzite`);
    trackRpcRequestSent(
      "pr-1",
      WS_METHODS.pullRequestsDetail,
      `${WS_METHODS.pullRequestsDetail} · bazzite`,
    );
    trackRpcRequestSent("other-1", WS_METHODS.serverGetConfig, `${WS_METHODS.serverGetConfig}`);

    vi.advanceTimersByTime(15_000);
    expect(slow.current.map((request) => request.requestId)).toEqual(["other-1"]);

    acknowledgeRpcRequest("other-1");
    expect(slow.current).toEqual([]);
  });
});
