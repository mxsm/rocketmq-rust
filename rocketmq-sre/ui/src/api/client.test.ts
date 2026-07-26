import { describe, expect, it } from "vitest";

import { stateLabel } from "./client";

describe("stateLabel", () => {
  it("renders every onboarding state without exposing implementation names", () => {
    expect(stateLabel("pending")).toBe("待接入");
    expect(stateLabel("handshaking")).toBe("握手中");
    expect(stateLabel("ready_read_only")).toBe("只读就绪");
    expect(stateLabel("read_only_degraded")).toBe("只读降级");
    expect(stateLabel("rejected")).toBe("已拒绝");
    expect(stateLabel("offboarded")).toBe("已下线");
  });
});
