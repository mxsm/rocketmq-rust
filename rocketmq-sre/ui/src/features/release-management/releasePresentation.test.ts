import { describe, expect, it } from "vitest";

import type {
  IntegrationAdapterKind,
  IntegrationDeliveryStatus,
  ReleaseReadinessSnapshot,
  ReleaseStatus,
} from "@/api/types";

import {
  adapterKindLabel,
  deliveryStatusLabel,
  deliveryStatusTone,
  readinessGates,
  releaseProgress,
  releaseStatusLabel,
  releaseStatusTone,
} from "./releasePresentation";

describe("release presentation", () => {
  it("presents every durable release state without a fallback label", () => {
    const statuses: ReleaseStatus[] = [
      "planned",
      "readiness_checking",
      "ready",
      "canary_running",
      "paused",
      "verifying",
      "rolling_back",
      "rolled_back",
      "completed",
      "manual_takeover",
      "failed",
    ];

    expect(statuses.map(releaseStatusLabel)).toEqual([
      "待准备",
      "门禁检查中",
      "可发布",
      "Canary 运行中",
      "已暂停",
      "发布后验证",
      "回滚中",
      "已回滚",
      "已完成",
      "人工接管",
      "失败",
    ]);
    expect(statuses.map(releaseStatusTone)).not.toContain(undefined);
    expect(statuses.map(releaseProgress)).toEqual([
      0, 1, 2, 3, 3, 4, 4, 5, 5, 5, 5,
    ]);
  });

  it("keeps adapter and delivery labels exhaustive", () => {
    const adapters: IntegrationAdapterKind[] = [
      "mock_itsm",
      "signed_webhook_itsm",
      "chat_ops_webhook",
      "pager",
      "email",
    ];
    const deliveries: IntegrationDeliveryStatus[] = [
      "pending",
      "delivering",
      "delivered",
      "retry_scheduled",
      "failed",
    ];

    expect(adapters.map(adapterKindLabel)).toEqual([
      "Mock ITSM",
      "Signed Webhook ITSM",
      "ChatOps Webhook",
      "Pager",
      "Email",
    ]);
    expect(deliveries.map(deliveryStatusLabel)).toEqual([
      "待投递",
      "投递中",
      "已送达",
      "等待重试",
      "投递失败",
    ]);
    expect(deliveries.map(deliveryStatusTone)).not.toContain(undefined);
  });

  it("projects each deterministic release gate explicitly", () => {
    const readiness = {
      pdb_ready: true,
      capacity_ready: true,
      quorum_ready: false,
      store_recovery_ready: true,
      synthetic_probe_ready: false,
    } as ReleaseReadinessSnapshot;

    expect(readinessGates(readiness)).toEqual([
      { id: "pdb", label: "PDB 可用", passed: true },
      { id: "capacity", label: "容量余量", passed: true },
      { id: "quorum", label: "Quorum 健康", passed: false },
      { id: "store", label: "Store 恢复验证", passed: true },
      { id: "synthetic", label: "Synthetic Probe", passed: false },
    ]);
    expect(readinessGates()).toHaveLength(5);
    expect(readinessGates().every((gate) => !gate.passed)).toBe(true);
  });
});
