import { render, screen } from "@testing-library/react";
import { vi } from "vitest";

import { ForecastWorkspace } from "@/pages/ForecastPage";
import {
  forecastSummary,
  formatRunway,
} from "@/pages/forecastFormat";
import { demoClusters } from "@/data/demo";
import { demoForecastReport } from "@/data/phase2ForecastDemo";

describe("forecast workspace", () => {
  const cluster = demoClusters[0];
  const report = demoForecastReport(cluster.tenant_id, cluster.id);

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-27T09:00:00Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("summarizes explainable forecast and backtest results", () => {
    expect(forecastSummary(report)).toEqual({
      thresholdRisks: 2,
      clearableBacklogs: 1,
      anomalies: 1,
      mae: 0.019,
      bias: -0.004,
      coverage: 0.913,
    });
    expect(formatRunway("2026-07-27T17:00:00Z")).toBe("8 小时");
    expect(formatRunway(undefined)).toBe("未预计到达");
  });

  it("keeps prediction and simulation advisory-only", () => {
    render(
      <ForecastWorkspace
        busy={false}
        cluster={cluster}
        report={report}
        simulationKind="traffic_increase"
        trafficPercent="50"
        onRunSimulation={vi.fn()}
        onSimulationKindChange={vi.fn()}
        onTrafficPercentChange={vi.fn()}
      />,
    );

    expect(screen.getByText("容量与到期趋势")).toBeInTheDocument();
    expect(screen.getByText("实际回测")).toBeInTheDocument();
    expect(
      screen.getByText(/execution_eligible=false/),
    ).toBeInTheDocument();
    expect(screen.queryByText("自动扩容")).not.toBeInTheDocument();
    expect(screen.queryByText("执行变更")).not.toBeInTheDocument();
  });
});
