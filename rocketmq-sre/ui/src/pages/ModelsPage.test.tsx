import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider } from "@/data/SreDataContext";

import { ModelsPage } from "./ModelsPage";

describe("ModelsPage", () => {
  beforeEach(() => {
    window.sessionStorage.clear();
    window.history.replaceState({}, "", "/models?demo=1");
  });

  it("renders lifecycle health separately from cluster mutation", async () => {
    const user = userEvent.setup();
    renderPage();

    expect(
      await screen.findByText("模型生命周期与 Provider Health"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("AI 路由治理，不是 RocketMQ 集群变更"),
    ).toBeInTheDocument();
    expect(screen.getByText("cluster_mutation=false")).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: /deepseek-prod/ }),
    ).toBeInTheDocument();
    await user.click(
      screen.getByRole("button", { name: /kimi-prod/ }),
    );
    expect(screen.getByText("provider_timeout")).toBeInTheDocument();
  });

  it("requires explicit confirmation before promoting a certified profile", async () => {
    const user = userEvent.setup();
    renderPage();

    await user.click(
      await screen.findByRole("button", { name: /zhipu-glm-prod/ }),
    );
    const promote = screen.getByRole("button", {
      name: "提升为生产",
    });
    expect(promote).toBeDisabled();

    await user.click(
      screen.getByRole("checkbox", {
        name: "我已核对 smoke、revision、回滚目标和路由影响",
      }),
    );
    expect(promote).toBeEnabled();
    await user.click(promote);

    expect(
      await screen.findByText(/已进入生产路由，revision 5/),
    ).toBeInTheDocument();
  });
});

function renderPage() {
  return render(
    <AuthProvider>
      <SreDataProvider>
        <ModelsPage />
      </SreDataProvider>
    </AuthProvider>,
  );
}
