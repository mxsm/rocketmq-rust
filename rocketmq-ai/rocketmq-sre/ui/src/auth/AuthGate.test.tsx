import { render, screen } from "@testing-library/react";

import { AuthGate } from "./AuthGate";
import { AuthProvider, resolveAuthMode } from "./AuthContext";

describe("AuthGate", () => {
  it("fails closed to OIDC for production builds", () => {
    expect(resolveAuthMode(undefined, false)).toBe("oidc");
    expect(resolveAuthMode("development", false)).toBe("development");
    expect(resolveAuthMode(undefined, true)).toBe("development");
  });

  it("establishes the scoped development session", async () => {
    render(
      <AuthProvider>
        <AuthGate>
          <div>受保护的 SRE 工作区</div>
        </AuthGate>
      </AuthProvider>,
    );

    expect(
      await screen.findByText("受保护的 SRE 工作区"),
    ).toBeInTheDocument();
  });
});
