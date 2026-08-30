import { describe, expect, it } from "vitest";

import { sanitizePlanParameters } from "./planPresentation";

describe("sanitizePlanParameters", () => {
  it("redacts credential-like fields recursively", () => {
    const result = sanitizePlanParameters({
      broker: "broker-a",
      tls_private_key: "private-material",
      nested: {
        accessToken: "token-material",
        queueCount: 8,
      },
    });

    expect(result).toEqual({
      broker: "broker-a",
      tls_private_key: "[REDACTED]",
      nested: {
        accessToken: "[REDACTED]",
        queueCount: 8,
      },
    });
    expect(JSON.stringify(result)).not.toContain("private-material");
    expect(JSON.stringify(result)).not.toContain("token-material");
  });
});
