import { render, screen } from "@testing-library/react";

import { AuthGate } from "./AuthGate";
import { AuthProvider, resolveAuthMode } from "./AuthContext";

describe("AuthGate", () => {
  afterEach(() => {
    delete window.__ROCKETMQ_SRE_CONFIG__;
    vi.unstubAllEnvs();
  });

  it("requires deployment configuration in production without granting a session", async () => {
    vi.stubEnv("VITE_SRE_AUTH_MODE", "oidc");
    vi.stubEnv("VITE_SRE_OIDC_AUTHORITY", "");
    vi.stubEnv("VITE_SRE_OIDC_CLIENT_ID", "");
    render(
      <AuthProvider>
        <AuthGate>
          <div>受保护的 SRE 工作区</div>
        </AuthGate>
      </AuthProvider>,
    );
    expect(await screen.findByText("无法建立安全会话")).toBeInTheDocument();
    expect(screen.queryByText("受保护的 SRE 工作区")).not.toBeInTheDocument();
  });

  it("loads the runtime provider while keeping the workspace behind OIDC login", async () => {
    vi.stubEnv("VITE_SRE_AUTH_MODE", "oidc");
    vi.stubEnv("VITE_SRE_OIDC_AUTHORITY", "");
    vi.stubEnv("VITE_SRE_OIDC_CLIENT_ID", "");
    window.__ROCKETMQ_SRE_CONFIG__ = {
      oidc: { authority: "https://id.example.com", clientId: "sre-ui" },
    };
    render(
      <AuthProvider>
        <AuthGate>
          <div>受保护的 SRE 工作区</div>
        </AuthGate>
      </AuthProvider>,
    );
    expect(await screen.findByText("需要 OIDC 登录")).toBeInTheDocument();
    expect(screen.queryByText("受保护的 SRE 工作区")).not.toBeInTheDocument();
  });

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
