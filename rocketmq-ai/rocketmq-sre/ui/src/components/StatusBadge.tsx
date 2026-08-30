import { Circle } from "lucide-react";

import { stateLabel } from "@/api/client";
import type {
  CoverageCellStatus,
  DataSourceAvailability,
  OnboardingState,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";

const stateVariants = {
  pending: "info",
  handshaking: "warning",
  ready_read_only: "success",
  read_only_degraded: "warning",
  rejected: "destructive",
  offboarded: "secondary",
} as const;

export function StatusBadge({ state }: { state: OnboardingState }) {
  return (
    <Badge variant={stateVariants[state]}>
      <Circle aria-hidden="true" fill="currentColor" size={7} />
      {stateLabel(state)}
    </Badge>
  );
}

const availabilityLabels: Record<DataSourceAvailability, string> = {
  queryable: "可查询",
  existing: "已实现本地",
  in_process_only: "进程内",
  missing_instrumentation: "缺少埋点",
  not_production_verified: "未生产验证",
};

const coverageLabels: Record<CoverageCellStatus, string> = {
  queryable: "可查询",
  implemented_local: "已实现本地",
  in_process_only: "进程内",
  missing_instrumentation: "缺少埋点",
  not_production_verified: "未生产验证",
};

const coverageVariants = {
  queryable: "success",
  implemented_local: "warning",
  in_process_only: "info",
  missing_instrumentation: "destructive",
  not_production_verified: "secondary",
} as const;

export function AvailabilityBadge({
  availability,
}: {
  availability: DataSourceAvailability;
}) {
  const normalized =
    availability === "existing" ? "implemented_local" : availability;
  return (
    <Badge
      className="coverage-badge"
      variant={coverageVariants[normalized]}
    >
      <Circle aria-hidden="true" fill="currentColor" size={7} />
      {availabilityLabels[availability]}
    </Badge>
  );
}

export function CoverageBadge({ status }: { status: CoverageCellStatus }) {
  return (
    <Badge className="coverage-badge" variant={coverageVariants[status]}>
      <Circle aria-hidden="true" fill="currentColor" size={7} />
      {coverageLabels[status]}
    </Badge>
  );
}

export function coverageLabel(status: CoverageCellStatus) {
  return coverageLabels[status];
}
