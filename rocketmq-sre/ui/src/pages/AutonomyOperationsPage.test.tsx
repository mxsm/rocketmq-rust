import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider } from "@/data/SreDataContext";

import { AutonomyOperationsPage } from "./AutonomyOperationsPage";

describe("AutonomyOperationsPage", () => {
  beforeAll(() => {
    Object.defineProperties(HTMLElement.prototype, {
      hasPointerCapture: {
        configurable: true,
        value: () => false,
      },
      releasePointerCapture: {
        configurable: true,
        value: () => undefined,
      },
      scrollIntoView: {
        configurable: true,
        value: () => undefined,
      },
      setPointerCapture: {
        configurable: true,
        value: () => undefined,
      },
    });
  });

  beforeEach(() => {
    window.sessionStorage.clear();
    window.history.replaceState({}, "", "/autonomy?demo=1");
  });

  it("renders bounded quality and cost operations without a publish surface", async () => {
    const user = userEvent.setup();
    renderPage();

    expect(
      await screen.findByText("自治运营与成本"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("运营观察面与生产发布面严格分离"),
    ).toBeInTheDocument();
    expect(await screen.findByText("83.9%")).toBeInTheDocument();

    await user.click(
      screen.getByRole("tab", { name: "模型与成本" }),
    );
    expect(
      screen.getByText("Provider / Model 成本"),
    ).toBeInTheDocument();
    expect(screen.getByText("kimi-moonshot")).toBeInTheDocument();
    expect(
      screen.getByText("自动路由变更：否"),
    ).toBeInTheDocument();

    await user.click(
      screen.getByRole("tab", { name: /人工候选/ }),
    );
    expect(screen.getByText("候选不等于发布")).toBeInTheDocument();
    expect(
      screen.getAllByText("publication_allowed=false"),
    ).toHaveLength(2);
    expect(
      screen.queryByRole("button", { name: /发布/ }),
    ).not.toBeInTheDocument();
  });

  it("switches between weekly and monthly reports", async () => {
    const user = userEvent.setup();
    renderPage();

    await screen.findByText("42 / 31");
    await user.click(
      screen.getByRole("combobox", { name: "报告周期" }),
    );
    await user.click(
      screen.getByRole("option", { name: "本月运营" }),
    );

    expect(await screen.findByText("186 / 139")).toBeInTheDocument();
    expect(screen.getByText(/79\.86/)).toBeInTheDocument();
    expect(screen.getByText("38.3 小时")).toBeInTheDocument();
  });

  it("filters recent outcomes while preserving denied and failure semantics", async () => {
    const user = userEvent.setup();
    renderPage();

    await screen.findByText("42 / 31");
    await user.click(
      screen.getByRole("tab", { name: "Outcome 明细" }),
    );
    expect(screen.getAllByText("预期拒绝")).not.toHaveLength(0);
    expect(screen.getByText("自治执行失败")).toBeInTheDocument();

    await user.click(
      screen.getByRole("combobox", { name: "Outcome 分类" }),
    );
    await user.click(
      screen.getByRole("option", { name: "成功" }),
    );

    await waitFor(() => {
      expect(
        screen.queryByText("自治执行失败"),
      ).not.toBeInTheDocument();
    });
    expect(
      screen.getAllByText("verification_passed"),
    ).not.toHaveLength(0);
  });

  it("queries operating quality and cost across scenario model and action dimensions", async () => {
    const user = userEvent.setup();
    renderPage();

    expect(
      await screen.findByText("多维运维分析"),
    ).toBeInTheDocument();
    expect(screen.getByText("MTTD")).toBeInTheDocument();
    expect(screen.getByText("建议采纳率")).toBeInTheDocument();
    expect(screen.getAllByText("执行成功率")).not.toHaveLength(0);
    expect(screen.getByText("自治节省工时")).toBeInTheDocument();
    expect(screen.getByText("90.0%")).toBeInTheDocument();
    expect(screen.getByText("4.5 小时")).toBeInTheDocument();

    await user.type(
      screen.getByRole("textbox", { name: "运维场景" }),
      "consumer_lag",
    );
    await user.type(
      screen.getByRole("textbox", { name: "模型 Provider" }),
      "deepseek",
    );
    await user.type(
      screen.getByRole("textbox", { name: "模型族" }),
      "deepseek",
    );
    await user.type(
      screen.getByRole("textbox", { name: "动作 ID" }),
      "observability.logger_level_ttl.v1",
    );
    await user.click(
      screen.getByRole("button", { name: "应用维度" }),
    );

    expect(
      await screen.findByText("场景 consumer_lag"),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        "动作 observability.logger_level_ttl.v1",
      ),
    ).toBeInTheDocument();
  });
});

function renderPage() {
  return render(
    <AuthProvider>
      <SreDataProvider>
        <AutonomyOperationsPage />
      </SreDataProvider>
    </AuthProvider>,
  );
}
