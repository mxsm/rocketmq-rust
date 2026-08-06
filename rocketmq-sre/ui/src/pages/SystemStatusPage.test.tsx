import { render, screen } from "@testing-library/react";

import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider } from "@/data/SreDataContext";

import { SystemStatusPage } from "./SystemStatusPage";

describe("SystemStatusPage", () => {
  beforeEach(() => {
    window.sessionStorage.clear();
    window.history.replaceState({}, "", "/system?demo=1");
  });

  it("renders runtime provider state instead of the stale descriptor-only claim", async () => {
    render(
      <AuthProvider>
        <SreDataProvider>
          <SystemStatusPage />
        </SreDataProvider>
      </AuthProvider>,
    );

    expect(await screen.findByText("Provider 网络已启用")).toBeInTheDocument();
    expect(screen.queryByText("descriptor / fixture only")).not.toBeInTheDocument();
    expect(screen.queryByText(/Phase 00/)).not.toBeInTheDocument();
    expect(await screen.findByText("deepseek-prod")).toBeInTheDocument();
    expect(screen.getAllByText("凭据已配置").length).toBeGreaterThan(0);
    expect(screen.getByText("kimi-prod")).toBeInTheDocument();
    expect(screen.getByText("已隔离")).toBeInTheDocument();
  });
});
