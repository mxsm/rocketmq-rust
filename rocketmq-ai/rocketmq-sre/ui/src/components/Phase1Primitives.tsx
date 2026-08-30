import {
  AlertTriangle,
  Ban,
  DatabaseZap,
  LoaderCircle,
  Radio,
  RefreshCw,
  ServerCrash,
} from "lucide-react";
import type { ReactNode } from "react";

import { ApiError } from "@/api/client";
import type {
  ClusterSummary,
  CollectionEnvelope,
  TimelineEvent,
} from "@/api/types";
import type { ProgressTransport } from "@/hooks/useWorkflowProgress";

import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";

export function ClusterScopeSelect({
  clusters,
  value,
  onChange,
}: {
  clusters: ClusterSummary[];
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger aria-label="集群范围" className="cluster-select">
        <SelectValue placeholder="选择集群范围" />
      </SelectTrigger>
      <SelectContent>
        {clusters.map((cluster) => (
          <SelectItem key={cluster.id} value={cluster.id}>
            {cluster.external_cluster_key} · {cluster.environment}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export function DataState({
  loading,
  error,
  empty,
  onRetry,
  emptyTitle = "暂无数据",
  emptyDescription = "当前只读范围内没有可展示的记录。",
}: {
  loading: boolean;
  error?: unknown;
  empty: boolean;
  onRetry?: () => void;
  emptyTitle?: string;
  emptyDescription?: string;
}) {
  if (loading) {
    return (
      <div className="state-panel" role="status">
        <LoaderCircle className="spin" size={24} />
        <div>
          <strong>正在读取只读数据</strong>
          <span>加载结果会保留 partial 与 missing 语义。</span>
        </div>
      </div>
    );
  }
  if (error) {
    const permission =
      error instanceof ApiError &&
      (error.status === 401 ||
        error.status === 403 ||
        error.code === "cluster_not_allowed");
    return (
      <div
        className={`state-panel ${permission ? "permission" : "unavailable"}`}
        role="alert"
      >
        {permission ? <Ban size={24} /> : <ServerCrash size={24} />}
        <div>
          <strong>
            {permission ? "当前身份没有该集群权限" : "后端暂不可用"}
          </strong>
          <span>
            {permission
              ? "请检查 OIDC cluster claim；界面不会跨范围降级查询。"
              : "未返回的数据保持 unavailable，不会被显示为 0。"}
          </span>
        </div>
        {onRetry && (
          <Button onClick={onRetry} size="sm" variant="outline">
            <RefreshCw size={14} />
            重试
          </Button>
        )}
      </div>
    );
  }
  if (empty) {
    return (
      <div className="state-panel empty">
        <DatabaseZap size={24} />
        <div>
          <strong>{emptyTitle}</strong>
          <span>{emptyDescription}</span>
        </div>
      </div>
    );
  }
  return null;
}

export function PartialNotice<T>({
  envelope,
}: {
  envelope?: CollectionEnvelope<T>;
}) {
  if (!envelope?.partial) {
    return null;
  }
  return (
    <div className="partial-notice" role="status">
      <AlertTriangle size={15} />
      <div>
        <strong>部分结果</strong>
        <span>
          {envelope.warnings.join("；") ||
            "一个或多个数据源返回 partial。"}
        </span>
      </div>
    </div>
  );
}

export function DataSurface({
  title,
  description,
  meta,
  children,
  className = "",
}: {
  title: string;
  description?: string;
  meta?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`data-surface ${className}`.trim()}>
      <div className="surface-heading">
        <div>
          <h2>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        {meta}
      </div>
      {children}
    </section>
  );
}

export function DefinitionGrid({
  items,
}: {
  items: Array<{ label: string; value: ReactNode; mono?: boolean }>;
}) {
  return (
    <dl className="phase1-definition-grid">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}</dt>
          <dd className={item.mono ? "mono" : undefined}>{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function Timeline({ events }: { events: TimelineEvent[] }) {
  if (events.length === 0) {
    return (
      <div className="state-message">尚无时间线事件，等待下一次只读采集。</div>
    );
  }
  return (
    <ol className="workflow-timeline">
      {events.map((event) => (
        <li key={event.id}>
          <span aria-hidden="true" />
          <div>
            <strong>{event.summary}</strong>
            <small>
              {event.event_type} ·{" "}
              {new Date(event.occurred_at).toLocaleString("zh-CN", {
                hour12: false,
              })}
            </small>
          </div>
          <code>{event.correlation_id.slice(0, 8)}</code>
        </li>
      ))}
    </ol>
  );
}

export function LiveTransport({
  transport,
}: {
  transport: ProgressTransport;
}) {
  const labels: Record<ProgressTransport, string> = {
    connecting: "连接中",
    sse: "SSE 实时",
    polling: "轮询回退",
  };
  return (
    <Badge variant={transport === "polling" ? "warning" : "success"}>
      {transport === "connecting" ? (
        <LoaderCircle className="spin" size={11} />
      ) : transport === "sse" ? (
        <Radio size={11} />
      ) : (
        <RefreshCw size={11} />
      )}
      {labels[transport]}
    </Badge>
  );
}

export function formatTime(value?: string | null) {
  return value
    ? new Date(value).toLocaleString("zh-CN", { hour12: false })
    : "未采集";
}
