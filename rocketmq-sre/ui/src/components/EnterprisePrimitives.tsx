import {
  CheckCircle2,
  CircleDotDashed,
  Download,
  FilterX,
  Search,
  ShieldAlert,
  TriangleAlert,
  XCircle,
} from "lucide-react";
import type { ReactNode } from "react";

import type { EnterpriseFilters } from "@/hooks/useEnterpriseData";

import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";

const ALL = "__all__";

export function EnterpriseScopeBar({
  filters,
  regions,
  owners,
  showHealth = true,
  onFilter,
  onReset,
  onExport,
}: {
  filters: EnterpriseFilters;
  regions: Array<{ id: string; label: string }>;
  owners: string[];
  showHealth?: boolean;
  onFilter: (key: keyof EnterpriseFilters, value: string | number) => void;
  onReset: () => void;
  onExport: () => void;
}) {
  return (
    <section className="enterprise-scope-bar" aria-label="Fleet 查询条件">
      <label className="enterprise-search">
        <Search aria-hidden="true" size={14} />
        <span className="sr-only">搜索</span>
        <input
          aria-label="搜索集群、组件或 owner"
          onChange={(event) => onFilter("search", event.target.value)}
          placeholder="搜索 cluster / component / owner"
          spellCheck={false}
          value={filters.search}
        />
      </label>
      <FilterSelect
        label="区域"
        onChange={(value) => onFilter("region", fromSelectValue(value))}
        options={regions.map((region) => ({
          value: region.id,
          label: region.label,
        }))}
        value={toSelectValue(filters.region)}
      />
      <FilterSelect
        label="环境"
        onChange={(value) =>
          onFilter("environment", fromSelectValue(value))
        }
        options={[
          { value: "production", label: "Production" },
          { value: "staging", label: "Staging" },
          { value: "test", label: "Test" },
        ]}
        value={toSelectValue(filters.environment)}
      />
      <FilterSelect
        label="Owner"
        onChange={(value) => onFilter("owner", fromSelectValue(value))}
        options={owners.map((owner) => ({ value: owner, label: owner }))}
        value={toSelectValue(filters.owner)}
      />
      {showHealth && (
        <FilterSelect
          label="健康"
          onChange={(value) => onFilter("health", fromSelectValue(value))}
          options={[
            { value: "healthy", label: "Healthy" },
            { value: "degraded", label: "Degraded" },
            { value: "critical", label: "Critical" },
            { value: "disconnected", label: "Disconnected" },
          ]}
          value={toSelectValue(filters.health)}
        />
      )}
      <span className="enterprise-toolbar-spacer" />
      <Button onClick={onReset} size="sm" type="button" variant="ghost">
        <FilterX size={14} />
        重置
      </Button>
      <Button onClick={onExport} size="sm" type="button" variant="outline">
        <Download size={14} />
        导出 CSV
      </Button>
    </section>
  );
}

export function EnterpriseMetric({
  label,
  value,
  detail,
  icon,
  tone = "neutral",
}: {
  label: string;
  value: ReactNode;
  detail: string;
  icon: ReactNode;
  tone?: "neutral" | "success" | "warning" | "critical";
}) {
  return (
    <article className={`enterprise-metric tone-${tone}`}>
      <span className="enterprise-metric-icon">{icon}</span>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
    </article>
  );
}

export function EnterpriseStatus({
  value,
  label,
}: {
  value: string;
  label?: string;
}) {
  const normalized = value.toLowerCase();
  const tone = statusTone(normalized);
  const Icon =
    tone === "success"
      ? CheckCircle2
      : tone === "critical"
        ? XCircle
        : tone === "warning"
          ? TriangleAlert
          : CircleDotDashed;
  return (
    <Badge className={`enterprise-status tone-${tone}`} variant="outline">
      <Icon aria-hidden="true" size={12} />
      {label ?? normalized.replaceAll("_", " ")}
    </Badge>
  );
}

export function EnterprisePageFooter({
  page,
  pageSize,
  total,
  onPage,
}: {
  page: number;
  pageSize: number;
  total: number;
  onPage: (page: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(page, totalPages);
  return (
    <footer className="enterprise-pagination">
      <span>
        第 {safePage} / {totalPages} 页 · {total} 条
      </span>
      <div>
        <Button
          disabled={safePage <= 1}
          onClick={() => onPage(safePage - 1)}
          size="sm"
          type="button"
          variant="outline"
        >
          上一页
        </Button>
        <Button
          disabled={safePage >= totalPages}
          onClick={() => onPage(safePage + 1)}
          size="sm"
          type="button"
          variant="outline"
        >
          下一页
        </Button>
      </div>
    </footer>
  );
}

export function EnterpriseBoundary({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <aside className="enterprise-boundary">
      <ShieldAlert aria-hidden="true" size={17} />
      <div>
        <strong>类型化控制边界</strong>
        <span>{children}</span>
      </div>
    </aside>
  );
}

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: Array<{ value: string; label: string }>;
  onChange: (value: string) => void;
}) {
  return (
    <Select onValueChange={onChange} value={value}>
      <SelectTrigger aria-label={label} className="enterprise-filter-select">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ALL}>{label} · 全部</SelectItem>
        {options.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function statusTone(value: string) {
  if (
    ["healthy", "active", "completed", "resolved", "done", "compliant"].includes(
      value,
    )
  ) {
    return "success";
  }
  if (
    [
      "critical",
      "failed",
      "blocked",
      "quarantined",
      "disconnected",
    ].includes(value)
  ) {
    return "critical";
  }
  if (
    [
      "degraded",
      "read_only_degraded",
      "warning",
      "error",
      "running",
      "open",
      "in_progress",
      "awaiting_manual_confirmation",
    ].includes(value)
  ) {
    return "warning";
  }
  return "neutral";
}

function toSelectValue(value: string) {
  return value.length === 0 ? ALL : value;
}

function fromSelectValue(value: string) {
  return value === ALL ? "" : value;
}
