import { render, screen } from "@testing-library/react";

import { ApiError } from "@/api/client";

import { DataState, PartialNotice } from "./Phase1Primitives";

describe("Phase1Primitives", () => {
  it("keeps permission failures distinct from backend outages", () => {
    const { rerender } = render(
      <DataState
        empty={false}
        error={new ApiError(403, "cluster_not_allowed", "outside scope")}
        loading={false}
      />,
    );

    expect(
      screen.getByText("当前身份没有该集群权限"),
    ).toBeInTheDocument();

    rerender(
      <DataState
        empty={false}
        error={new ApiError(503, "source_unavailable", "offline")}
        loading={false}
      />,
    );

    expect(screen.getByText("后端暂不可用")).toBeInTheDocument();
    expect(screen.getByText(/不会被显示为 0/)).toBeInTheDocument();
  });

  it("renders loading, empty, and partial states explicitly", () => {
    const { rerender } = render(
      <DataState empty={false} loading />,
    );
    expect(screen.getByText("正在读取只读数据")).toBeInTheDocument();

    rerender(
      <DataState
        empty
        emptyTitle="没有证据"
        loading={false}
      />,
    );
    expect(screen.getByText("没有证据")).toBeInTheDocument();

    rerender(
      <PartialNotice
        envelope={{
          items: [],
          observed_at: "2026-07-27T00:00:00Z",
          partial: true,
          warnings: ["MCP timeout"],
        }}
      />,
    );
    expect(screen.getByText("部分结果")).toBeInTheDocument();
    expect(screen.getByText("MCP timeout")).toBeInTheDocument();
  });
});
