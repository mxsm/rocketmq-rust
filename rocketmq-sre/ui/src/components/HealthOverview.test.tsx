import { render, screen } from "@testing-library/react";

import {
  ClusterHealthOverview,
  FleetHealthOverview,
} from "@/components/HealthOverview";
import { demoClusters } from "@/data/demo";
import {
  demoClusterHealth,
  demoFleetHealth,
} from "@/data/phase2HealthDemo";

describe("health overview", () => {
  it("renders all deterministic dimensions and the multi-window trigger", () => {
    render(
      <ClusterHealthOverview
        report={demoClusterHealth[demoClusters[1].id]}
      />,
    );

    expect(screen.getByText("确定性 SLO 与集群健康")).toBeInTheDocument();
    expect(screen.getByText("58")).toBeInTheDocument();
    expect(
      screen.getByText("Consumer lag backlog"),
    ).toBeInTheDocument();
    expect(screen.getByText("5m / 1h")).toBeInTheDocument();
    expect(screen.getByText("30m / 6h")).toBeInTheDocument();
    expect(screen.getByText("6h / 3d")).toBeInTheDocument();
    expect(screen.getByText("rocketmq-sre.health-score.v1")).toBeInTheDocument();
    expect(screen.getByText("禁止")).toBeInTheDocument();
    expect(screen.getByText("只读")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("surfaces the worst cluster instead of presenting an average", () => {
    render(<FleetHealthOverview report={demoFleetHealth()} />);

    expect(screen.getByText("Fleet 健康总览")).toBeInTheDocument();
    expect(screen.getByText("rmq-staging")).toBeInTheDocument();
    expect(
      screen.getByText("worst_cluster_no_average_masking"),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/不会使用平均值掩盖严重集群/),
    ).toBeInTheDocument();
  });
});
