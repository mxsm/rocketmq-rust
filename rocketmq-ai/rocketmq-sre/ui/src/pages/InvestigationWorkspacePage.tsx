import { ArrowLeft, RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";

import type { InvestigationView } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  DataState,
  DataSurface,
  DefinitionGrid,
  LiveTransport,
  Timeline,
} from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { withinWindow } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { EvidenceReferencePanel } from "@/features/investigations/EvidenceReferencePanel";
import { EvidenceDrawer } from "@/features/evidence/EvidenceDrawer";
import type { EvidenceFreshnessPolicy } from "@/features/evidence/evidencePresentation";
import { useCapabilityQuality } from "@/features/capabilities/useCapabilityQuality";
import { ScenarioEvidenceChain } from "@/features/investigations/ScenarioEvidenceChain";
import { policyForEvidence } from "@/features/investigations/scenarioEvidence";
import { InvestigationPromotion } from "@/features/investigations/InvestigationPromotion";
import {
  diagnosisHypotheses,
  investigationStatusLabels,
  missingEvidence,
} from "@/features/investigations/investigationPresentation";
import { ObjectScopeBoundary } from "@/features/scope/ObjectScopeBoundary";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useWorkflowProgress } from "@/hooks/useWorkflowProgress";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

export function InvestigationWorkspacePage() {
  const { investigationId = "" } = useParams();
  const { api } = useSreData();
  const m = useWorkspaceMessages();
  const load = useCallback(
    (signal: AbortSignal) => api.getInvestigation(investigationId, signal),
    [api, investigationId],
  );
  const resource = useAsyncResource(load);
  return (
    <div className="page investigation-workspace-page">
      <Button asChild className="back-link" size="sm" variant="ghost">
        <Link to="/investigations">
          <ArrowLeft size={14} />
          {m.back}
        </Link>
      </Button>
      <DataState
        loading={resource.loading && !resource.data}
        error={resource.error}
        empty={!resource.loading && !resource.data}
        onRetry={resource.reload}
      />
      {resource.data && (
        <ObjectScopeBoundary object={resource.data.investigation}>
          <InvestigationWorkspace
            view={resource.data}
            reload={resource.reload}
            refreshing={resource.loading}
          />
        </ObjectScopeBoundary>
      )}
    </div>
  );
}

function InvestigationWorkspace({
  view,
  reload,
  refreshing,
}: {
  view: InvestigationView;
  reload: () => void;
  refreshing: boolean;
}) {
  const scope = useWorkspaceScope();
  const m = useWorkspaceMessages();
  const { locale, formatDateTime } = useOperatorPreferences();
  const english = locale === "en-US";
  const object = view.investigation;
  const revisions = view.diagnosis_revisions
    .filter(
      (revision) =>
        revision.investigation_id === object.id &&
        withinWindow(revision.created_at, scope.window),
    )
    .sort((a, b) => b.revision - a.revision);
  const revisionTimes = view.diagnosis_revisions
    .filter((item) => item.investigation_id === object.id)
    .map((item) => Date.parse(item.created_at))
    .filter(Number.isFinite);
  const outsideRevisions = revisionTimes.length - revisions.length;
  const includeOutsideRevisions = () =>
    scope.setWindow({
      start: new Date(
        Math.min(Date.parse(scope.window.start), ...revisionTimes),
      ).toISOString(),
      end: new Date(
        Math.max(Date.parse(scope.window.end), Date.now(), ...revisionTimes),
      ).toISOString(),
    });
  const [revisionId, setRevisionId] = useState("");
  const revision =
    revisions.find((item) => item.id === revisionId) ?? revisions[0];
  const [evidenceId, setEvidenceId] = useState("");
  const [drawer, setDrawer] = useState<{
    revisionId: string;
    evidenceId: string;
    policy?: EvidenceFreshnessPolicy;
  }>();
  const evidenceIds = [...new Set(revision?.evidence_ids ?? [])].slice(0, 128);
  const selectedEvidence = evidenceIds.includes(evidenceId)
    ? evidenceId
    : evidenceIds[0];
  const hypotheses = revision ? diagnosisHypotheses(revision) : [];
  const missing = revision ? missingEvidence(revision) : [];
  const quality = useCapabilityQuality(
    revision
      ? { investigationId: object.id, revisionId: revision.id }
      : undefined,
  );
  const reloadQuality = quality.reload;
  const reloadAll = useCallback(() => {
    reload();
    reloadQuality();
  }, [reload, reloadQuality]);
  const progress = useWorkflowProgress(object.cluster_id, reloadAll, {
    aggregateType: "investigation",
    aggregateId: object.id,
  });
  const openEvidence = (id: string, policy?: EvidenceFreshnessPolicy) => {
    if (!revision || !evidenceIds.includes(id)) return;
    setEvidenceId(id);
    setDrawer({
      revisionId: revision.id,
      evidenceId: id,
      policy: policy ?? policyForEvidence(quality.projection, id),
    });
  };
  const timeline = view.timeline.filter(
    (event) =>
      event.tenant_id === object.tenant_id &&
      event.cluster_id === object.cluster_id &&
      (!event.investigation_id || event.investigation_id === object.id) &&
      withinWindow(event.occurred_at, scope.window),
  );
  const conversationId = object.conversation_id ?? revision?.conversation_id;
  const diagnosisLabels = english
    ? {
        healthy: "Healthy",
        fault: "Fault detected",
        inconclusive: "Inconclusive",
        unsupported: "Unsupported",
      }
    : {
        healthy: "未发现故障",
        fault: "发现故障",
        inconclusive: "证据不足，尚无结论",
        unsupported: "当前诊断不支持",
      };

  return (
    <>
      <PageHeader
        eyebrow="INVESTIGATION"
        title={object.title}
        description={m.investigationDescription}
        actions={
          <>
            <LiveTransport transport={progress.transport} />
            <Button
              disabled={refreshing}
              onClick={reloadAll}
              size="sm"
              variant="outline"
            >
              <RefreshCw
                className={refreshing ? "spin" : undefined}
                size={14}
              />
              {m.refresh}
            </Button>
          </>
        }
      />
      <div className="investigation-mode" role="status">
        <ShieldCheck aria-hidden="true" size={18} />
        <div>
          <strong>{m.readOnly}</strong>
          <p>{m.manualHint}</p>
        </div>
        <Badge variant="outline">
          {investigationStatusLabels[object.status][english ? 1 : 0]}
        </Badge>
      </div>
      <p className="investigation-window-note">{m.timeFiltered}</p>
      {outsideRevisions > 0 && (
        <div
          className="investigation-outside-window inline-alert warning"
          role="status"
        >
          <span>
            {outsideRevisions} {m.outsideRevisions}
          </span>
          <Button onClick={includeOutsideRevisions} size="sm" variant="outline">
            {m.includeOutsideRevisions}
          </Button>
        </div>
      )}
      <div className="investigation-workspace-grid">
        <div className="investigation-main-stack">
          <DataSurface
            title={m.conclusion}
            meta={
              revision && (
                <Badge variant={revision.partial ? "warning" : "outline"}>
                  {revision.partial ? m.partial : m.complete}
                </Badge>
              )
            }
          >
            <div className="investigation-assessment">
              <strong>
                {revision ? diagnosisLabels[revision.status] : m.noConclusion}
              </strong>
              <span>
                {revision
                  ? `${revision.pack_id}@${revision.pack_version} · Revision ${revision.revision} · ${formatDateTime(revision.created_at)}`
                  : m.noRevisions}
              </span>
            </div>
            <DefinitionGrid
              items={[
                { label: m.resourceScope, value: object.resource ?? "cluster" },
              ]}
            />
            {revision && (
              <details className="investigation-audit-details">
                <summary>{m.auditDetails}</summary>
                <DefinitionGrid
                  items={[
                    {
                      label: "Correlation",
                      value: revision.correlation_id,
                      mono: true,
                    },
                    { label: "Turn", value: revision.turn_id, mono: true },
                  ]}
                />
              </details>
            )}
          </DataSurface>
          <DataSurface
            title={m.revisions}
            meta={<span>{revisions.length}</span>}
          >
            {!revisions.length ? (
              <p className="workspace-empty">{m.noRevisions}</p>
            ) : (
              <div
                aria-label={m.revisions}
                className="investigation-revision-picker"
              >
                {revisions.slice(0, 50).map((item) => (
                  <button
                    aria-pressed={item.id === revision?.id}
                    className={item.id === revision?.id ? "selected" : ""}
                    key={item.id}
                    onClick={() => setRevisionId(item.id)}
                    type="button"
                  >
                    <strong>Revision {item.revision}</strong>
                    <span>
                      {item.status} · {formatDateTime(item.created_at)}
                    </span>
                  </button>
                ))}
              </div>
            )}
          </DataSurface>
          <ScenarioEvidenceChain
            projection={quality.projection}
            loading={quality.loading}
            error={quality.error}
            revision={revision}
            timeline={timeline}
            onOpenEvidence={openEvidence}
            conversationId={conversationId}
          />
          <DataSurface title={m.hypotheses}>
            {!hypotheses.length ? (
              <p className="workspace-empty">{m.noHypotheses}</p>
            ) : (
              <div className="investigation-hypotheses">
                {hypotheses.map((hypothesis, index) => (
                  <article key={`${index}:${hypothesis.title}`}>
                    <header>
                      <h3>{hypothesis.title}</h3>
                      {hypothesis.confidence !== undefined && (
                        <span>
                          {m.confidence} {Math.round(hypothesis.confidence)}/100
                        </span>
                      )}
                    </header>
                    <p>{hypothesis.status ?? m.unsupportedHypothesis}</p>
                    {hypothesis.support.length > 0 && (
                      <p>
                        <strong>
                          {english ? "Supporting references" : "支持证据引用"}
                        </strong>{" "}
                        {hypothesis.support.map((id) => (
                          <button
                            className="evidence-reference"
                            key={id}
                            onClick={() => openEvidence(id)}
                            type="button"
                          >
                            {id}
                          </button>
                        ))}
                      </p>
                    )}
                    {hypothesis.counter.length > 0 && (
                      <p>
                        <strong>
                          {english ? "Counter references" : "反证引用"}
                        </strong>{" "}
                        {hypothesis.counter.map((id) => (
                          <button
                            className="evidence-reference"
                            key={id}
                            onClick={() => openEvidence(id)}
                            type="button"
                          >
                            {id}
                          </button>
                        ))}
                      </p>
                    )}
                  </article>
                ))}
              </div>
            )}
          </DataSurface>
          <DataSurface
            title={m.evidenceReferences}
            meta={<span>{evidenceIds.length}</span>}
          >
            {evidenceIds.length ? (
              <div className="investigation-evidence-references">
                {evidenceIds.map((id) => (
                  <button
                    aria-pressed={selectedEvidence === id}
                    className={`evidence-reference${selectedEvidence === id ? " selected" : ""}`}
                    key={id}
                    onClick={() => openEvidence(id)}
                    type="button"
                  >
                    {id}
                  </button>
                ))}
              </div>
            ) : (
              <p className="workspace-empty">{m.noEvidence}</p>
            )}
          </DataSurface>
          <DataSurface title={m.missingEvidence}>
            {missing.length ? (
              <ul className="investigation-missing-list">
                {missing.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            ) : (
              <p className="workspace-empty">{m.noMissingRecorded}</p>
            )}
          </DataSurface>
          <DataSurface title={m.timeline}>
            {timeline.length ? (
              <Timeline events={timeline} />
            ) : (
              <p className="workspace-empty">{m.noMatchingTimeline}</p>
            )}
          </DataSurface>
        </div>
        <div className="investigation-side-stack">
          <EvidenceReferencePanel
            evidenceId={selectedEvidence}
            onOpen={openEvidence}
          />
          <DataSurface title={m.assistant}>
            <p className="workspace-empty">{m.assistantHint}</p>
            {missing.length > 0 && (
              <ul className="investigation-missing-list">
                {missing.slice(0, 3).map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            )}
            <div className="investigation-related-links">
              <Button asChild size="sm" variant="outline">
                <Link
                  to={
                    conversationId ? `/conversations/${conversationId}` : "/ask"
                  }
                >
                  {conversationId ? m.openConversation : m.newInvestigation}
                </Link>
              </Button>
            </div>
          </DataSurface>
          <DataSurface title={m.links}>
            <DefinitionGrid
              items={[
                { label: m.updated, value: formatDateTime(object.updated_at) },
                { label: m.resourceScope, value: object.resource ?? "cluster" },
              ]}
            />
            <div className="investigation-related-links">
              {conversationId && (
                <Button asChild size="sm" variant="outline">
                  <Link to={`/conversations/${conversationId}`}>
                    {m.openConversation}
                  </Link>
                </Button>
              )}
              {object.incident_id && (
                <Button asChild size="sm" variant="outline">
                  <Link to={`/incidents/${object.incident_id}`}>
                    {m.openIncident}
                  </Link>
                </Button>
              )}
              <Button asChild size="sm" variant="ghost">
                <Link to="/operations">{m.handoff}</Link>
              </Button>
            </div>
            {!object.incident_id &&
              !["closed", "promoted"].includes(object.status) && (
                <InvestigationPromotion investigation={object} />
              )}
          </DataSurface>
          <DataSurface title={m.plans}>
            <p className="workspace-empty">{m.plansHint}</p>
            <div className="investigation-related-links">
              <Button asChild size="sm" variant="outline">
                <Link
                  to={
                    object.incident_id
                      ? `/incidents/${object.incident_id}`
                      : "/changes"
                  }
                >
                  {object.incident_id ? m.openIncident : m.changeCenter}
                </Link>
              </Button>
            </div>
          </DataSurface>
        </div>
      </div>
      {drawer &&
        revision &&
        drawer.revisionId === revision.id &&
        evidenceIds.includes(drawer.evidenceId) && (
          <EvidenceDrawer
            evidenceId={drawer.evidenceId}
            onClose={() => setDrawer(undefined)}
            freshnessPolicy={drawer.policy}
            access={{
              kind: "revision_evidence",
              revisionId: revision.id,
              evidenceIds,
            }}
          />
        )}
    </>
  );
}
