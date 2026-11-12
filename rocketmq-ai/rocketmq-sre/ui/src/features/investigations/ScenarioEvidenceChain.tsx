import { useState } from "react";
import { Link } from "react-router-dom";

import type {
  CapabilityQualityProjection,
  InvestigationDiagnosisRevision,
  TimelineEvent,
} from "@/api/types";
import { DataSurface } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/auth/AuthContext";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useRevisionEvidenceMetadata } from "@/features/evidence/useRevisionEvidenceMetadata";
import type { EvidenceFreshnessPolicy } from "@/features/evidence/evidencePresentation";
import {
  useCapabilityMessages,
  type ScenarioId,
} from "@/i18n/capabilityMessages";
import { useEvidenceMessages } from "@/i18n/evidenceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import {
  requirementFreshnessPolicy,
  scenarioEvidence,
} from "./scenarioEvidence";

export function ScenarioEvidenceChain({
  projection,
  revision,
  timeline,
  onOpenEvidence,
  conversationId,
  loading,
  error,
}: {
  projection?: CapabilityQualityProjection;
  revision?: InvestigationDiagnosisRevision;
  timeline: TimelineEvent[];
  onOpenEvidence: (id: string, policy?: EvidenceFreshnessPolicy) => void;
  conversationId?: string;
  loading?: boolean;
  error?: unknown;
}) {
  const m = useCapabilityMessages();
  const scope = useWorkspaceScope();
  const tenantId = useAuth().session?.tenantId ?? "";
  const e = useEvidenceMessages();
  const { formatDateTime } = useOperatorPreferences();
  const [selected, setSelected] = useState("");
  const scenarios = projection?.scenarios ?? [];
  const scenario =
    scenarios.find((item) => item.id === selected) ??
    scenarios.find(
      (item) => revision && item.pack_ids.includes(revision.pack_id),
    ) ??
    scenarios[0];
  const requestedIds =
    projection?.packs
      .filter((pack) => scenario?.pack_ids.includes(pack.pack_id))
      .flatMap((pack) =>
        pack.requirements.flatMap((item) => item.evidence_ids),
      ) ?? [];
  const metadata = useRevisionEvidenceMetadata(
    revision,
    requestedIds,
    projection?.observed_at,
  );
  const chain =
    projection && scenario && revision
      ? scenarioEvidence(projection, scenario, revision, {
          metadata: metadata.data?.metadata ?? {},
          failures: metadata.data?.failures,
          scope,
          tenantId,
        })
      : undefined;
  const matched =
    chain?.required.filter((slot) => slot.quality === "available").length ?? 0;

  return (
    <DataSurface
      className="scenario-evidence-chain"
      title={m.chain}
      description={m.chainHint}
    >
      <div className="scenario-chain-toolbar">
        <label>
          <span>{m.scenario}</span>
          <select
            aria-label={m.chain}
            className="native-select"
            disabled={!scenarios.length}
            onChange={(event) => setSelected(event.target.value)}
            value={scenario?.id ?? ""}
          >
            {!scenarios.length && (
              <option value="">{loading ? m.loading : m.unknown}</option>
            )}
            {scenarios.map((item) => (
              <option key={item.id} value={item.id}>
                {m[item.id as ScenarioId] ?? item.id}
              </option>
            ))}
          </select>
        </label>
        {chain && !metadata.loading && (
          <Badge
            variant={
              matched === chain.required.length && chain.required.length > 0
                ? "outline"
                : "warning"
            }
          >
            {m.matched}: {matched} / {chain.required.length}
          </Badge>
        )}
      </div>
      {loading && !projection ? (
        <p className="workspace-empty" role="status">
          {m.loading}
        </p>
      ) : error ? (
        <p className="workspace-empty" role="alert">
          {m.unavailable}
        </p>
      ) : !revision ? (
        <p className="workspace-empty">{m.noRevision}</p>
      ) : !projection ? (
        <p className="workspace-empty">{m.noProjectionHint}</p>
      ) : (
        chain && (
          <>
            <p className="scenario-chain-note">
              {m.asOf}: {formatDateTime(projection.observed_at)}
            </p>
            <ol className="scenario-chain-slots">
              {chain.slots.map(
                ({ packId, requirement, ids, quality, reason, relation }) => (
                  <li key={`${packId}:${requirement.id}`}>
                    <header>
                      <div>
                        <strong>{requirement.id}</strong>
                        <span>
                          {packId} ·{" "}
                          {requirement.required ? m.required : m.optional}
                        </span>
                      </div>
                      <Badge
                        variant={
                          quality === "available" ? "outline" : "warning"
                        }
                      >
                        {metadata.loading && ids.length
                          ? m.loading
                          : (m[quality] ?? m.unknown)}
                      </Badge>
                    </header>
                    <p>
                      {requirement.source} · {requirement.resource_prefix} · ≤{" "}
                      {requirement.max_age_seconds}s
                    </p>
                    {ids.length ? (
                      <div className="scenario-chain-references">
                        {ids.map((id, index) => {
                          const linked = relation(id);
                          return (
                            <div key={id}>
                              <Button
                                onClick={() =>
                                  onOpenEvidence(
                                    id,
                                    requirementFreshnessPolicy(
                                      projection,
                                      packId,
                                      requirement,
                                    ),
                                  )
                                }
                                size="sm"
                                title={id}
                                variant="outline"
                              >
                                {m.viewEvidence} {index + 1}
                              </Button>
                              {linked.support && (
                                <Badge variant="outline">{e.supports}</Badge>
                              )}
                              {linked.counter && (
                                <Badge variant="warning">{e.counters}</Badge>
                              )}
                              {!linked.support && !linked.counter && (
                                <span>{m.noRelation}</span>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <p className="scenario-gap">{m.revisionGap}</p>
                    )}
                    {ids.length > 0 &&
                      requirement.missing_fields.length > 0 && (
                        <p className="scenario-gap">
                          {m.missingFields}:{" "}
                          {requirement.missing_fields.join(" · ")}
                        </p>
                      )}
                    <details>
                      <summary>{m.reason}</summary>
                      <p>{reason ? m[reason] : requirement.reason_code}</p>
                      {reason && ids.length > 0 && (
                        <p>
                          {m.factDetails}: {requirement.reason_code}
                        </p>
                      )}
                      <p>
                        {m.fields}:{" "}
                        {requirement.fields
                          .map((field) => `${field.path}: ${field.kind}`)
                          .join(" · ") || m.unknown}
                      </p>
                    </details>
                  </li>
                ),
              )}
            </ol>
            {metadata.data &&
              Object.keys(metadata.data.failures).length > 0 && (
                <div className="scenario-chain-note" role="alert">
                  <p>{m.metadata_unavailable}</p>
                  <Button onClick={metadata.reload} size="sm" variant="outline">
                    {e.retry}
                  </Button>
                </div>
              )}
            {chain.contextIds.length > 0 && (
              <div className="scenario-context">
                <h3>{m.relatedContext}</h3>
                {chain.contextIds.map((id, index) => (
                  <Button
                    key={id}
                    onClick={() => onOpenEvidence(id)}
                    size="sm"
                    title={id}
                    variant="ghost"
                  >
                    {m.viewEvidence} {index + 1}
                  </Button>
                ))}
              </div>
            )}
            {timeline.length > 0 && (
              <details className="scenario-context">
                <summary>
                  {m.temporalEvents} · {timeline.length}
                </summary>
                <p>{m.temporalHint}</p>
                <ul>
                  {timeline.slice(0, 8).map((event) => (
                    <li key={event.id}>
                      {event.summary}
                      <small>{formatDateTime(event.occurred_at)}</small>
                    </li>
                  ))}
                </ul>
              </details>
            )}
          </>
        )
      )}
      <p className="scenario-chain-note">{e.noCausality}</p>
      <div className="investigation-related-links">
        <Button asChild size="sm" variant="outline">
          <Link
            to={conversationId ? `/conversations/${conversationId}` : "/ask"}
          >
            {m.gapTask}
          </Link>
        </Button>
        <Button asChild size="sm" variant="ghost">
          <Link to="/coverage">{m.openMatrix}</Link>
        </Button>
      </div>
    </DataSurface>
  );
}
