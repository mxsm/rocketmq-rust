import type {
  IntegrationAdapterKind,
  IntegrationDeliveryStatus,
  IntegrationEventKind,
  ReleaseObservationPhase,
  ReleaseReadinessSnapshot,
  ReleaseStatus,
} from "@/api/types";
import type { BadgeProps } from "@/components/ui/badge";

const releaseStatusLabels: Record<ReleaseStatus, string> = {
  planned: "待准备",
  readiness_checking: "门禁检查中",
  ready: "可发布",
  canary_running: "Canary 运行中",
  paused: "已暂停",
  verifying: "发布后验证",
  rolling_back: "回滚中",
  rolled_back: "已回滚",
  completed: "已完成",
  manual_takeover: "人工接管",
  failed: "失败",
};

const releaseStatusTones: Record<
  ReleaseStatus,
  BadgeProps["variant"]
> = {
  planned: "secondary",
  readiness_checking: "info",
  ready: "success",
  canary_running: "info",
  paused: "warning",
  verifying: "info",
  rolling_back: "warning",
  rolled_back: "secondary",
  completed: "success",
  manual_takeover: "destructive",
  failed: "destructive",
};

const adapterLabels: Record<IntegrationAdapterKind, string> = {
  mock_itsm: "Mock ITSM",
  signed_webhook_itsm: "Signed Webhook ITSM",
  chat_ops_webhook: "ChatOps Webhook",
  pager: "Pager",
  email: "Email",
};

const eventLabels: Record<IntegrationEventKind, string> = {
  plan_submitted: "计划已提交",
  approval_changed: "审批已变化",
  release_started: "发布已开始",
  release_paused: "发布已暂停",
  release_rolling_back: "发布回滚中",
  release_completed: "发布已完成",
  manual_takeover_required: "需要人工接管",
};

const deliveryLabels: Record<IntegrationDeliveryStatus, string> = {
  pending: "待投递",
  delivering: "投递中",
  delivered: "已送达",
  retry_scheduled: "等待重试",
  failed: "投递失败",
};

const deliveryTones: Record<
  IntegrationDeliveryStatus,
  BadgeProps["variant"]
> = {
  pending: "secondary",
  delivering: "info",
  delivered: "success",
  retry_scheduled: "warning",
  failed: "destructive",
};

const observationPhaseLabels: Record<
  ReleaseObservationPhase,
  string
> = {
  before: "发布前",
  during: "发布中",
  after: "发布后",
};

const progressByStatus: Record<ReleaseStatus, number> = {
  planned: 0,
  readiness_checking: 1,
  ready: 2,
  canary_running: 3,
  paused: 3,
  verifying: 4,
  rolling_back: 4,
  rolled_back: 5,
  completed: 5,
  manual_takeover: 5,
  failed: 5,
};

export interface ReleaseGate {
  id: string;
  label: string;
  passed: boolean;
}

export function releaseStatusLabel(status: ReleaseStatus) {
  return releaseStatusLabels[status];
}

export function releaseStatusTone(status: ReleaseStatus) {
  return releaseStatusTones[status];
}

export function releaseProgress(status: ReleaseStatus) {
  return progressByStatus[status];
}

export function adapterKindLabel(kind: IntegrationAdapterKind) {
  return adapterLabels[kind];
}

export function integrationEventLabel(kind: IntegrationEventKind) {
  return eventLabels[kind];
}

export function deliveryStatusLabel(status: IntegrationDeliveryStatus) {
  return deliveryLabels[status];
}

export function deliveryStatusTone(status: IntegrationDeliveryStatus) {
  return deliveryTones[status];
}

export function observationPhaseLabel(phase: ReleaseObservationPhase) {
  return observationPhaseLabels[phase];
}

export function readinessGates(
  readiness?: ReleaseReadinessSnapshot | null,
): ReleaseGate[] {
  return [
    {
      id: "pdb",
      label: "PDB 可用",
      passed: readiness?.pdb_ready ?? false,
    },
    {
      id: "capacity",
      label: "容量余量",
      passed: readiness?.capacity_ready ?? false,
    },
    {
      id: "quorum",
      label: "Quorum 健康",
      passed: readiness?.quorum_ready ?? false,
    },
    {
      id: "store",
      label: "Store 恢复验证",
      passed: readiness?.store_recovery_ready ?? false,
    },
    {
      id: "synthetic",
      label: "Synthetic Probe",
      passed: readiness?.synthetic_probe_ready ?? false,
    },
  ];
}

export function formatReleaseTime(value?: string | null) {
  return value
    ? new Date(value).toLocaleString("zh-CN", { hour12: false })
    : "尚未记录";
}
