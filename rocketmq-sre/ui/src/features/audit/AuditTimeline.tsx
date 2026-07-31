import { Fingerprint, UserRound } from "lucide-react";

import type { AuditPage } from "@/api/types";
import { Badge } from "@/components/ui/badge";

import { formatTimestamp, shortDigest } from "../plans/planPresentation";

export function AuditTimeline({ audit }: { audit: AuditPage }) {
  return (
    <section className="data-surface audit-surface">
      <header className="surface-heading">
        <div>
          <h2>不可变审计链</h2>
          <p>按 correlation ID 聚合计划、策略、审批、执行、验证和隔离事件。</p>
        </div>
        <Badge variant={audit.partial ? "warning" : "success"}>
          {audit.partial ? "部分结果" : `${audit.items.length} events`}
        </Badge>
      </header>
      <ol className="audit-timeline">
        {audit.items.map((event) => (
          <li key={event.id}>
            <span className="audit-dot" />
            <div>
              <header>
                <strong>{event.event_kind}</strong>
                <time>{formatTimestamp(event.occurred_at)}</time>
              </header>
              <p>{event.reason_code}</p>
              <footer>
                <span>
                  <UserRound size={12} />
                  {event.actor_subject} · {event.actor_role}
                </span>
                <code>
                  {event.resource_kind}/{shortDigest(event.resource_id)}
                </code>
              </footer>
            </div>
          </li>
        ))}
      </ol>
      <div className="audit-correlation">
        <Fingerprint size={14} />
        <span>Correlation</span>
        <code>{audit.correlation_id}</code>
      </div>
    </section>
  );
}
