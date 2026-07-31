import type {
  ClusterForecastReport,
  UpgradeReadinessReport,
} from "@/api/types";

type Forecast = ClusterForecastReport["forecasts"][number];

export function forecastSummary(report: ClusterForecastReport) {
  const horizon = Date.now() + 30 * 86_400_000;
  const thresholdRisks = report.forecasts.filter(
    (forecast) =>
      forecast.exhaustion_at &&
      Date.parse(forecast.exhaustion_at) <= horizon,
  ).length;
  const accuracy = report.accuracy[0];
  return {
    thresholdRisks,
    clearableBacklogs: report.backlog_etas.filter(
      (backlog) => backlog.estimated_clear_at,
    ).length,
    anomalies: report.anomalies.filter((anomaly) => anomaly.anomaly).length,
    mae: accuracy?.mean_absolute_error,
    bias: accuracy?.bias,
    coverage: accuracy?.interval_coverage_ratio,
  };
}

export function formatRunway(value?: string | null) {
  if (!value) {
    return "未预计到达";
  }
  const hours = (Date.parse(value) - Date.now()) / 3_600_000;
  if (!Number.isFinite(hours)) {
    return "数据不足";
  }
  if (hours <= 0) {
    return "已达到阈值";
  }
  if (hours < 48) {
    return `${Math.round(hours)} 小时`;
  }
  return `${Math.round(hours / 24)} 天`;
}

export function formatDate(value: string) {
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

export function formatNumber(value?: number | null) {
  if (value === undefined || value === null || !Number.isFinite(value)) {
    return "—";
  }
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: 3,
    notation: Math.abs(value) >= 100_000 ? "compact" : "standard",
  }).format(value);
}

export function formatSigned(value?: number | null) {
  if (value === undefined || value === null || !Number.isFinite(value)) {
    return "—";
  }
  return `${value > 0 ? "+" : ""}${formatNumber(value)}`;
}

export function formatPercent(value?: number | null) {
  if (value === undefined || value === null || !Number.isFinite(value)) {
    return "—";
  }
  return `${Math.round(value * 100)}%`;
}

export function windowLabel(window: Forecast["window"]) {
  return window === "seven_days" ? "7 天" : "30 天";
}

export function statusLabel(status: Forecast["status"]) {
  return status === "insufficient_data"
    ? "数据不足"
    : status === "stale"
      ? "已过期"
      : status;
}

export function readinessVariant(
  status?: UpgradeReadinessReport["status"],
): "success" | "destructive" | "outline" | "warning" {
  if (status === "ready") {
    return "success";
  }
  if (status === "blocked") {
    return "destructive";
  }
  if (status === "ready_with_warnings") {
    return "warning";
  }
  return "outline";
}
