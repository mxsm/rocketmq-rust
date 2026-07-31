import { ChevronRight, SearchCheck } from "lucide-react";
import { Link } from "react-router-dom";

import type { IncidentView } from "@/api/types";
import { formatTime } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";

import {
  incidentIsStale,
  incidentStatusLabels,
  latestDiagnosisIsPartial,
} from "./incidentPresentation";

export interface IncidentInboxFiltersProps {
  severity: string;
  status: string;
  owner: string;
  query: string;
  owners: string[];
  onSeverityChange: (value: string) => void;
  onStatusChange: (value: string) => void;
  onOwnerChange: (value: string) => void;
  onQueryChange: (value: string) => void;
}

export function IncidentInboxFilters({
  severity,
  status,
  owner,
  query,
  owners,
  onSeverityChange,
  onStatusChange,
  onOwnerChange,
  onQueryChange,
}: IncidentInboxFiltersProps) {
  return (
    <div
      aria-label="Incident Inbox 筛选"
      className="operator-filter-bar incident-filter-bar"
    >
      <label className="operator-filter">
        <span>严重度</span>
        <select
          aria-label="严重度筛选"
          className="native-select"
          onChange={(event) => onSeverityChange(event.target.value)}
          value={severity}
        >
          <option value="all">全部严重度</option>
          <option value="critical">critical</option>
          <option value="error">error</option>
          <option value="warning">warning</option>
          <option value="info">info</option>
        </select>
      </label>
      <label className="operator-filter">
        <span>状态</span>
        <select
          aria-label="Incident 状态筛选"
          className="native-select"
          onChange={(event) => onStatusChange(event.target.value)}
          value={status}
        >
          <option value="all">全部状态</option>
          {Object.entries(incidentStatusLabels).map(([value, label]) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </select>
      </label>
      <label className="operator-filter">
        <span>Owner</span>
        <select
          aria-label="Owner 筛选"
          className="native-select"
          onChange={(event) => onOwnerChange(event.target.value)}
          value={owner}
        >
          <option value="all">全部 Owner</option>
          <option value="unassigned">未分派</option>
          {owners.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
      </label>
      <label className="operator-search">
        <SearchCheck aria-hidden="true" size={15} />
        <input
          aria-label="搜索 Incident"
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder="cluster / resource / symptom"
          type="search"
          value={query}
        />
      </label>
    </div>
  );
}

export function IncidentInboxCards({
  incidents,
  now,
}: {
  incidents: IncidentView[];
  now: Date;
}) {
  return (
    <div className="record-grid">
      {incidents.map((view) => {
        const partial = latestDiagnosisIsPartial(view);
        const stale = incidentIsStale(view, now);
        return (
          <Link
            className="record-card incident-inbox-card"
            key={view.incident.id}
            to={`/incidents/${view.incident.id}`}
          >
            <header>
              <div className="incident-badge-row">
                <Badge
                  variant={
                    view.incident.severity === "critical"
                      ? "destructive"
                      : view.incident.severity === "error"
                        ? "warning"
                        : "outline"
                  }
                >
                  {view.incident.severity ?? "warning"}
                </Badge>
                <Badge variant="outline">
                  {incidentStatusLabels[view.incident.status]}
                </Badge>
                {partial && <Badge variant="warning">partial</Badge>}
                {stale && <Badge variant="secondary">stale</Badge>}
              </div>
              <ChevronRight size={15} />
            </header>
            <h3>{view.incident.title}</h3>
            <p>{view.incident.summary ?? "等待诊断摘要。"}</p>
            <dl className="incident-inbox-meta">
              <div>
                <dt>Cluster</dt>
                <dd>{view.incident.cluster_id}</dd>
              </div>
              <div>
                <dt>Resource</dt>
                <dd>{view.incident.resource ?? "未绑定"}</dd>
              </div>
              <div>
                <dt>Symptom</dt>
                <dd>{view.incident.symptom_family ?? "未分类"}</dd>
              </div>
              <div>
                <dt>Owner</dt>
                <dd>{view.incident.owner ?? "未分派"}</dd>
              </div>
            </dl>
            <footer>
              <span>
                合并告警 {view.incident.occurrence_count} ·{" "}
                {view.diagnosis_revisions.length} revisions
              </span>
              <span>{formatTime(view.incident.updated_at)}</span>
            </footer>
          </Link>
        );
      })}
    </div>
  );
}
