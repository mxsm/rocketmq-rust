import { ArrowRight, Search } from "lucide-react";
import { useMemo, type ReactNode } from "react";
import { Link } from "react-router-dom";

import type { IncidentView } from "@/api/types";
import { useIncidentInboxMessages } from "@/i18n/incidentInboxMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

import {
  incidentIsStale,
  latestDiagnosisIsPartial,
  type IncidentFilters,
} from "./incidentPresentation";
import "./incidentInbox.css";

interface FiltersProps {
  filters: IncidentFilters;
  change: (key: keyof IncidentFilters, value: string) => void;
  owners: string[];
  day: string;
  days: string[];
  setDay: (value: string) => void;
  clusterSelect: ReactNode;
  meta: ReactNode;
}

export function IncidentInboxFilters({
  filters,
  change,
  owners,
  day,
  days,
  setDay,
  clusterSelect,
  meta,
}: FiltersProps) {
  const m = useIncidentInboxMessages();
  const { timeZone } = useOperatorPreferences();
  return (
    <section className="incident-inbox-filters" aria-label={m.incidents}>
      <div className="incident-inbox-fields">
        <div className="incident-inbox-field">
          <span>{m.scope}</span>
          {clusterSelect}
        </div>
        <label className="incident-inbox-field">
          <span>{m.status}</span>
          <select
            className="native-select"
            value={filters.status}
            onChange={(event) => change("status", event.target.value)}
          >
            <option value="all">{m.allStatus}</option>
            {Object.entries(m.statusLabels).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label className="incident-inbox-field">
          <span>{m.severity}</span>
          <select
            className="native-select"
            value={filters.severity}
            onChange={(event) => change("severity", event.target.value)}
          >
            <option value="all">{m.allSeverity}</option>
            {Object.entries(m.severityLabels).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label className="incident-inbox-field">
          <span>{m.owner}</span>
          <select
            className="native-select"
            value={filters.owner}
            onChange={(event) => change("owner", event.target.value)}
          >
            <option value="all">{m.allOwners}</option>
            <option value="unassigned">{m.unassigned}</option>
            {owners.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>
        <label className="incident-inbox-field incident-inbox-date-field">
          <span>
            {m.dates} · {timeZone}
          </span>
          <select
            className="native-select"
            value={day}
            onChange={(event) => setDay(event.target.value)}
          >
            <option value="">{m.anyDate}</option>
            {day && !days.includes(day) && <option>{day}</option>}
            {days.map((item) => (
              <option key={item}>{item}</option>
            ))}
          </select>
        </label>
      </div>
      <div className="incident-inbox-controls">
        <label className="incident-inbox-search">
          <Search size={15} aria-hidden="true" />
          <input
            type="search"
            aria-label={m.search}
            placeholder={m.searchHint}
            value={filters.query}
            onChange={(event) => change("query", event.target.value)}
          />
        </label>
        <div className="incident-inbox-controls-meta">{meta}</div>
      </div>
    </section>
  );
}

export function IncidentInboxTimeline({
  incidents,
  now,
}: {
  incidents: IncidentView[];
  now: Date;
}) {
  const m = useIncidentInboxMessages();
  const { locale, timeZone, formatDateTime } = useOperatorPreferences();
  const clock = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        minute: "2-digit",
        hour: "2-digit",
        hourCycle: "h23",
        timeZone,
      }),
    [locale, timeZone],
  );
  const date = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        year: "numeric",
        month: "short",
        day: "numeric",
        timeZone,
      }),
    [locale, timeZone],
  );
  return (
    <ol className="incident-inbox-timeline" aria-label={m.incidents}>
      {incidents.map((view) => {
        const { incident } = view;
        const severity = incident.severity;
        const timestamp = Date.parse(incident.updated_at);
        const valid = Number.isFinite(timestamp);
        return (
          <li
            key={incident.id}
            className="incident-inbox-entry"
            data-severity={severity ?? "unknown"}
          >
            <div className="incident-inbox-clock">
              {valid ? (
                <time
                  dateTime={incident.updated_at}
                  title={formatDateTime(incident.updated_at)}
                >
                  <b>{clock.format(timestamp)}</b>
                  <small>{date.format(timestamp)}</small>
                </time>
              ) : (
                <span>{m.unknown}</span>
              )}
            </div>
            <div className="incident-inbox-body">
              <h2>
                <span className="incident-inbox-severity">
                  {severity ? m.severityLabels[severity] : m.unknown}
                </span>
                {incident.title}
              </h2>
              <div className="incident-inbox-flags">
                <span>{m.statusLabels[incident.status]}</span>
                {latestDiagnosisIsPartial(view) && (
                  <span className="incident-inbox-caution">{m.partial}</span>
                )}
                {incidentIsStale(view, now) && (
                  <span className="incident-inbox-caution">{m.stale}</span>
                )}
              </div>
              <div className="incident-inbox-counts">
                <span>
                  {m.alerts} <b>{incident.occurrence_count}</b>
                </span>
                <span>
                  {m.revisions} <b>{view.diagnosis_revisions.length}</b>
                </span>
              </div>
              <p>{incident.summary ?? m.noSummary}</p>
              {(incident.resource || incident.symptom_family) && (
                <div className="incident-inbox-resources">
                  {incident.resource && (
                    <span>
                      {m.resource} <b>{incident.resource}</b>
                    </span>
                  )}
                  {incident.symptom_family && (
                    <span>
                      {m.symptom} <b>{incident.symptom_family}</b>
                    </span>
                  )}
                </div>
              )}
            </div>
            <div className="incident-inbox-owner">
              <span>{m.owner}</span>
              <strong>{incident.owner || m.unassigned}</strong>
              <small>
                {m.updated} · {valid ? clock.format(timestamp) : m.unknown}{" "}
                {timeZone}
              </small>
              <Link
                className="incident-inbox-detail"
                to={`/incidents/${encodeURIComponent(incident.id)}`}
                aria-label={`${m.details} · ${incident.title}`}
              >
                {m.details}
                <ArrowRight size={16} aria-hidden="true" />
              </Link>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
