import { ApiError, type SreApi } from "@/api/client";
import type {
  ActionItem,
  ActionItemPatchRequest,
  CollectionEnvelope,
  ConversationView,
  IncidentTopologyView,
  IncidentOperationRequest,
  IncidentOperationResult,
  IncidentView,
  InspectionView,
  InvestigationView,
  PostmortemPatchRequest,
  PostmortemPublishRequest,
  PostmortemView,
  Recommendation,
} from "@/api/types";
import type { ApiRequestContext } from "@/auth/AuthContext";
import {
  demoCapabilities,
  demoCatalog,
  demoClusters,
  demoCoverage,
} from "@/data/demo";

import {
  DEMO_TENANT_ID,
  envelope,
  phase1Assets,
  phase1Conversations,
  phase1Evidence,
  phase1Incidents,
  phase1Inspections,
  phase1Investigations,
  phase1Knowledge,
  phase1MessageJourney,
  phase1Models,
  phase1Recommendations,
  phase1Topology,
  phase1WorkflowEvents,
} from "./phase1Demo";
import {
  demoClusterHealth,
  demoFleetHealth,
} from "./phase2HealthDemo";
import {
  demoDrReadiness,
  demoForecastReport,
  demoSimulation,
  demoUpgradeReadiness,
} from "./phase2ForecastDemo";
import {
  demoIncidentOperations,
  demoOperationsReport,
  demoShiftHandoff,
} from "./phase2OperationsDemo";

const WAIT_MS = 90;
const mockPostmortems: PostmortemView[] = [];
const mockActionItems: ActionItem[] = [];

function wait(signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = window.setTimeout(resolve, WAIT_MS);
    signal?.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function unavailable(resource: string): never {
  throw new ApiError(404, "source_unavailable", `${resource} is unavailable`);
}

export function createMockSreApi(auth?: ApiRequestContext): SreApi {
  const clusters = clone(demoClusters);
  const conversations = clone(phase1Conversations.items);
  const investigations = clone(phase1Investigations.items);
  const incidents = clone(phase1Incidents.items);
  const inspections = clone(phase1Inspections.items);
  const recommendations = clone(phase1Recommendations.items);
  const evidence = clone(phase1Evidence.items);
  const postmortems = mockPostmortems;
  const actionItems = mockActionItems;
  const incidentOperations = clone(demoIncidentOperations);

  const scope = (clusterId: string) => {
    if (auth && !auth.clusterIds.includes(clusterId)) {
      throw new ApiError(
        403,
        "cluster_not_allowed",
        "cluster is outside the authenticated scope",
      );
    }
  };

  const scopedEnvelope = <T extends { cluster_id?: string }>(
    source: CollectionEnvelope<T>,
    clusterId: string,
  ): CollectionEnvelope<T> => {
    scope(clusterId);
    return {
      ...clone(source),
      items: source.items
        .filter((item) => !item.cluster_id || item.cluster_id === clusterId)
        .map(clone),
    };
  };

  const listViews = <T>(
    source: T[],
    clusterId: string,
    getCluster: (item: T) => string,
  ) => {
    scope(clusterId);
    return envelope(source.filter((item) => getCluster(item) === clusterId));
  };

  return {
    listClusters: async (signal) => {
      await wait(signal);
      return clone(
        auth
          ? clusters.filter((cluster) => auth.clusterIds.includes(cluster.id))
          : clusters,
      );
    },
    getCluster: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        clusters.find((cluster) => cluster.id === clusterId) ??
          unavailable("cluster"),
      );
    },
    getClusterCapabilities: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        demoCapabilities[clusterId] ?? unavailable("cluster capability"),
      );
    },
    getCapabilities: async (signal) => {
      await wait(signal);
      return clone(demoCatalog);
    },
    getPhase2Contract: async (signal) => {
      await wait(signal);
      return {
        schema_version: "rocketmq-sre.api.v1",
        effective_access: "read_only",
        cluster_mutation_supported: false,
        operations: [
          "read_alerts",
          "read_topology",
          "read_forecasts",
          "read_slo_health",
          "run_simulation",
          "read_readiness",
          "manage_postmortem_metadata",
          "manage_action_item_metadata",
        ],
      };
    },
    getCoverage: async (signal) => {
      await wait(signal);
      return clone(demoCoverage);
    },
    getHealth: async (signal) => {
      await wait(signal);
      return { status: "healthy" };
    },
    getReadiness: async (signal) => {
      await wait(signal);
      return { status: "ready" };
    },
    getClusterSlo: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        demoClusterHealth[clusterId] ?? unavailable("cluster SLO"),
      );
    },
    getClusterHealth: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        demoClusterHealth[clusterId] ?? unavailable("cluster health"),
      );
    },
    getFleetHealth: async (region, signal) => {
      await wait(signal);
      return clone(demoFleetHealth(auth?.clusterIds, region));
    },
    getClusterForecasts: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      const cluster =
        clusters.find((item) => item.id === clusterId) ??
        unavailable("cluster");
      return clone(demoForecastReport(cluster.tenant_id, clusterId));
    },
    runSimulation: async (input, signal) => {
      await wait(signal);
      scope(input.cluster_id);
      return clone(
        demoSimulation(auth?.tenantId ?? DEMO_TENANT_ID, input),
      );
    },
    getUpgradeReadiness: async (
      clusterId,
      targetVersion,
      signal,
    ) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        demoUpgradeReadiness(
          auth?.tenantId ?? DEMO_TENANT_ID,
          clusterId,
          targetVersion,
        ),
      );
    },
    getDrReadiness: async (clusterId, targetRegion, signal) => {
      await wait(signal);
      scope(clusterId);
      return clone(
        demoDrReadiness(
          auth?.tenantId ?? DEMO_TENANT_ID,
          clusterId,
          targetRegion,
        ),
      );
    },
    onboardCluster: async (input, signal) => {
      await wait(signal);
      const existing = clusters.find(
        (cluster) =>
          cluster.external_cluster_key === input.external_cluster_key,
      );
      if (existing) {
        return { cluster: clone(existing), created: false };
      }
      const cluster = {
        id: crypto.randomUUID(),
        tenant_id: input.tenant_id,
        external_cluster_key: input.external_cluster_key,
        environment: input.environment,
        region: input.region,
        rocketmq_version: input.rocketmq_version,
        deployment_mode: input.deployment_mode,
        owner: input.owner,
        state: "pending" as const,
        effective_access_profile: "read_only" as const,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      clusters.push(cluster);
      return { cluster: clone(cluster), created: true };
    },
    listAssets: async (clusterId, signal) => {
      await wait(signal);
      return scopedEnvelope(phase1Assets, clusterId);
    },
    getTopology: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return {
        ...clone(phase1Topology),
        assets: phase1Topology.assets.filter(
          (asset) => asset.cluster_id === clusterId,
        ),
        edges: phase1Topology.edges.filter(
          (edge) => edge.cluster_id === clusterId,
        ),
      };
    },
    listConversations: async (clusterId, signal) => {
      await wait(signal);
      return listViews(
        conversations,
        clusterId,
        (item) => item.conversation.cluster_id,
      );
    },
    getConversation: async (id, signal) => {
      await wait(signal);
      const result =
        conversations.find((item) => item.conversation.id === id) ??
        unavailable("conversation");
      scope(result.conversation.cluster_id);
      return clone(result);
    },
    createConversation: async (input, signal) => {
      await wait(signal);
      scope(input.cluster_id);
      const now = new Date().toISOString();
      const investigation: InvestigationView["investigation"] | undefined =
        input.persist_investigation
          ? {
              id: crypto.randomUUID(),
              tenant_id: auth?.tenantId ?? DEMO_TENANT_ID,
              cluster_id: input.cluster_id,
              title: input.question.slice(0, 120),
              resource: input.resource,
              symptom_family: "operator_question",
              fingerprint: `operator_question:${input.resource ?? "cluster"}`,
              status: "collecting",
              created_by: {
                subject: auth?.subject ?? "rocketmq-sre-development",
              },
              created_at: now,
              updated_at: now,
            }
          : undefined;
      const result: ConversationView = {
        conversation: {
          id: crypto.randomUUID(),
          tenant_id: auth?.tenantId ?? DEMO_TENANT_ID,
          cluster_id: input.cluster_id,
          question: input.question,
          resource: input.resource,
          status: "active",
          investigation_id: investigation?.id,
          created_by: {
            subject: auth?.subject ?? "rocketmq-sre-development",
          },
          created_at: now,
          updated_at: now,
        },
        investigation,
      };
      conversations.unshift(result);
      if (investigation) {
        investigations.unshift({
          investigation,
          timeline: [],
        });
      }
      return clone(result);
    },
    listInvestigations: async (clusterId, signal) => {
      await wait(signal);
      return listViews(
        investigations,
        clusterId,
        (item) => item.investigation.cluster_id,
      );
    },
    getInvestigation: async (id, signal) => {
      await wait(signal);
      const result =
        investigations.find((item) => item.investigation.id === id) ??
        unavailable("investigation");
      scope(result.investigation.cluster_id);
      return clone(result);
    },
    promoteInvestigation: async (id, input, signal) => {
      await wait(signal);
      const result =
        investigations.find((item) => item.investigation.id === id) ??
        unavailable("investigation");
      scope(result.investigation.cluster_id);
      if (result.investigation.incident_id) {
        throw new ApiError(
          409,
          "workflow_conflict",
          "investigation is already promoted",
        );
      }
      const now = new Date().toISOString();
      const incidentId = crypto.randomUUID();
      result.investigation.incident_id = incidentId;
      result.investigation.status = "promoted";
      result.investigation.updated_at = now;
      const incident: IncidentView = {
        incident: {
          id: incidentId,
          tenant_id: result.investigation.tenant_id,
          cluster_id: result.investigation.cluster_id,
          title: input.title?.trim() || result.investigation.title,
          status: "new",
          summary: input.reason,
          severity: "warning",
          owner: "unassigned",
          occurrence_count: 0,
          created_at: now,
          updated_at: now,
        },
        investigation: clone(result.investigation),
        timeline: [],
        diagnosis_revisions: [],
      };
      incidents.unshift(incident);
      return clone(incident);
    },
    listIncidents: async (clusterId, signal) => {
      await wait(signal);
      return listViews(
        incidents,
        clusterId,
        (item) => item.incident.cluster_id,
      );
    },
    getIncident: async (id, signal) => {
      await wait(signal);
      const result =
        incidents.find((item) => item.incident.id === id) ??
        unavailable("incident");
      scope(result.incident.cluster_id);
      return clone(result);
    },
    getIncidentTopology: async (id, signal) => {
      await wait(signal);
      const result =
        incidents.find((item) => item.incident.id === id) ??
        unavailable("incident");
      scope(result.incident.cluster_id);
      const resource =
        result.incident.resource ?? `cluster:${result.incident.cluster_id}`;
      return clone<IncidentTopologyView>({
        schema_version: "rocketmq-sre.incident-topology.v1",
        incident_id: id,
        nodes: [
          {
            key: resource,
            kind: resource.split(":")[0] || "resource",
            display_name: resource,
            alert_count: Math.max(result.incident.occurrence_count, 1),
          },
          {
            key: `cluster:${result.incident.cluster_id}`,
            kind: "cluster",
            display_name: "RocketMQ cluster",
            alert_count: 0,
          },
        ],
        edges: [
          {
            from: resource,
            to: `cluster:${result.incident.cluster_id}`,
            relation: "member_of",
          },
        ],
        partial: result.diagnosis_revisions.some(
          (revision) => revision.partial,
        ),
        warnings: [],
      });
    },
    getIncidentOperations: async (id, signal) => {
      await wait(signal);
      const state =
        incidentOperations[id] ?? unavailable("incident operations");
      scope(state.cluster_id);
      return clone(state);
    },
    applyIncidentOperation: async (
      id,
      input: IncidentOperationRequest,
      signal,
    ) => {
      await wait(signal);
      const state =
        incidentOperations[id] ?? unavailable("incident operations");
      const incident =
        incidents.find((item) => item.incident.id === id) ??
        unavailable("incident");
      scope(state.cluster_id);
      const now = new Date().toISOString();
      let relatedIncidentId: string | undefined;
      switch (input.action) {
        case "acknowledge":
          state.acknowledged_by ??= auth?.subject ?? "demo-operator";
          state.sla.acknowledged_at ??= now;
          state.sla.acknowledgement_breached = false;
          break;
        case "assign":
          state.owner = input.owner;
          incident.incident.owner = input.owner;
          break;
        case "suppress":
          state.suppressed_until = input.until;
          state.suppression_reason = input.reason;
          break;
        case "merge":
          state.merged_into_incident_id = input.target_incident_id;
          relatedIncidentId = input.target_incident_id;
          break;
        case "split":
          relatedIncidentId = crypto.randomUUID();
          state.split_incident_ids.push(relatedIncidentId);
          break;
        case "reopen":
          relatedIncidentId = crypto.randomUUID();
          break;
      }
      state.updated_at = now;
      const timeline = {
        id: crypto.randomUUID(),
        tenant_id: state.tenant_id,
        cluster_id: state.cluster_id,
        incident_id: id,
        event_type: `incident_${input.action}`,
        summary: `Incident ${input.action}`,
        details: {
          related_incident_id: relatedIncidentId,
          cluster_mutation_performed: false,
        },
        correlation_id: crypto.randomUUID(),
        actor: {
          subject: auth?.subject ?? "demo-operator",
        },
        occurred_at: now,
      };
      incident.timeline.push(timeline);
      return clone<IncidentOperationResult>({
        schema_version: "rocketmq-sre.incident-operation-result.v1",
        state,
        related_incident_id: relatedIncidentId,
        timeline_event: timeline,
        cluster_mutation_performed: false,
      });
    },
    getShiftHandoff: async (clusterId, signal) => {
      await wait(signal);
      if (clusterId) {
        scope(clusterId);
      }
      const summary = clone(demoShiftHandoff);
      if (clusterId && clusterId !== summary.unresolved_incidents[0]?.cluster_id) {
        for (const key of [
          "new_incidents",
          "unresolved_incidents",
          "risk_trends",
          "recent_changes",
          "expiring_certificates",
          "capacity_risks",
          "overdue_action_items",
          "source_gaps",
        ] as const) {
          summary[key] = [];
        }
      }
      return summary;
    },
    getOperationsReport: async (window, clusterId, signal) => {
      await wait(signal);
      if (clusterId) {
        scope(clusterId);
      }
      const report = clone(demoOperationsReport);
      report.window = window;
      const end = new Date(report.window_end);
      report.window_start = new Date(
        end.getTime() -
          (window === "weekly" ? 7 : 1) * 24 * 60 * 60 * 1_000,
      ).toISOString();
      if (clusterId && clusterId !== report.worst_clusters[0]?.cluster_id) {
        report.worst_clusters = [];
        report.slo_burns = [];
        report.diagnostic_pack_findings = [];
        report.repeat_incidents = [];
        report.forecast_errors = [];
        report.source_gaps = [];
        report.forecast_mean_absolute_error = null;
      }
      return report;
    },
    downloadOperationsReport: async (
      window,
      format,
      clusterId,
      signal,
    ) => {
      await wait(signal);
      if (clusterId) {
        scope(clusterId);
      }
      const report = {
        ...clone(demoOperationsReport),
        window,
      };
      const content =
        format === "html"
          ? `<h1>RocketMQ AI SRE ${window} report</h1><p>RocketMQ mutations: 0</p>`
          : `# RocketMQ AI SRE ${window} report\n\n- RocketMQ mutations: 0\n`;
      return new Blob([content, JSON.stringify(report)], {
        type:
          format === "html"
            ? "text/html; charset=utf-8"
            : "text/markdown; charset=utf-8",
      });
    },
    createPostmortem: async (incidentId, _input, signal) => {
      await wait(signal);
      const existing = postmortems.find(
        (item) => item.postmortem.incident_id === incidentId,
      );
      if (existing) {
        return clone(existing);
      }
      const incident =
        incidents.find((item) => item.incident.id === incidentId) ??
        unavailable("incident");
      scope(incident.incident.cluster_id);
      const now = new Date().toISOString();
      const postmortemId = crypto.randomUUID();
      const evidenceIds =
        incident.diagnosis_revisions.at(-1)?.evidence_ids ?? [];
      const rootCause = {
        code: "consumer_lag_growth",
        statement:
          "消费速率低于到达速率，导致队列堆积持续增长。",
        evidence_ids: evidenceIds.slice(0, 2),
      };
      const action: ActionItem = {
        id: crypto.randomUUID(),
        tenant_id: incident.incident.tenant_id,
        cluster_id: incident.incident.cluster_id,
        postmortem_id: postmortemId,
        incident_id: incidentId,
        title: "验证消费者扩容后的净消费速率",
        owner: null,
        due_at: null,
        status: "open",
        verification: null,
        evidence_ids: evidenceIds.slice(0, 1),
        execution_journal: null,
        created_at: now,
        updated_at: now,
        completed_at: null,
      };
      const result: PostmortemView = {
        postmortem: {
          id: postmortemId,
          tenant_id: incident.incident.tenant_id,
          cluster_id: incident.incident.cluster_id,
          incident_id: incidentId,
          status: "draft",
          current_revision: 1,
          confirmed_by: null,
          confirmed_at: null,
          published_knowledge_item_id: null,
          created_by:
            auth?.subject ?? "rocketmq-sre-development",
          created_at: now,
          updated_at: now,
        },
        revisions: [
          {
            id: crypto.randomUUID(),
            postmortem_id: postmortemId,
            revision: 1,
            summary: `${incident.incident.title}；AI 已生成证据化复盘草稿。`,
            impact: `影响资源：${incident.incident.resource ?? "cluster"}；严重度：${incident.incident.severity ?? "未分类"}。`,
            detection: `由 ${incident.incident.symptom_family ?? "operator_query"} 检测。`,
            timeline: clone(incident.timeline),
            root_causes: evidenceIds.length > 0 ? [rootCause] : [],
            contributing_factors: [],
            conclusions:
              evidenceIds.length > 0
                ? [
                    {
                      code: "incident_scope_confirmed",
                      statement: "影响范围已由只读 Evidence 确认。",
                      evidence_ids: evidenceIds.slice(0, 2),
                    },
                  ]
                : [],
            recovery: "恢复步骤尚待操作员确认。",
            effective_actions: [],
            ineffective_actions: [],
            evidence_ids: evidenceIds,
            model_invocation_id: crypto.randomUUID(),
            edited_by:
              auth?.subject ?? "rocketmq-sre-development",
            human_confirmed: false,
            created_at: now,
          },
        ],
        action_items: [action],
        recurrences: [],
        todos: [],
        knowledge_item: null,
        execution_journal_empty: true,
      };
      postmortems.push(result);
      actionItems.push(action);
      return clone(result);
    },
    getPostmortem: async (id, signal) => {
      await wait(signal);
      const result =
        postmortems.find((item) => item.postmortem.id === id) ??
        unavailable("postmortem");
      scope(result.postmortem.cluster_id);
      return clone(result);
    },
    patchPostmortem: async (
      id,
      input: PostmortemPatchRequest,
      signal,
    ) => {
      await wait(signal);
      const result =
        postmortems.find((item) => item.postmortem.id === id) ??
        unavailable("postmortem");
      scope(result.postmortem.cluster_id);
      const current = result.revisions.at(-1);
      if (!current) {
        return unavailable("postmortem revision");
      }
      const now = new Date().toISOString();
      result.revisions.push({
        ...clone(current),
        ...clone(input),
        id: crypto.randomUUID(),
        revision: current.revision + 1,
        postmortem_id: id,
        edited_by: auth?.subject ?? "rocketmq-sre-development",
        human_confirmed: input.human_confirmed,
        created_at: now,
      });
      result.postmortem.current_revision += 1;
      result.postmortem.status = input.human_confirmed
        ? "confirmed"
        : "in_review";
      result.postmortem.updated_at = now;
      result.postmortem.confirmed_by = input.human_confirmed
        ? auth?.subject ?? "rocketmq-sre-development"
        : result.postmortem.confirmed_by;
      result.postmortem.confirmed_at = input.human_confirmed
        ? now
        : result.postmortem.confirmed_at;
      return clone(result);
    },
    publishPostmortem: async (
      id,
      input: PostmortemPublishRequest,
      signal,
    ) => {
      await wait(signal);
      const result =
        postmortems.find((item) => item.postmortem.id === id) ??
        unavailable("postmortem");
      scope(result.postmortem.cluster_id);
      if (
        !input.human_confirmed ||
        result.postmortem.status !== "confirmed"
      ) {
        throw new ApiError(
          400,
          "human_validation_required",
          "当前 Revision 需要人工确认后才能发布。",
        );
      }
      const now = new Date().toISOString();
      const knowledgeId = crypto.randomUUID();
      result.postmortem.status = "published";
      result.postmortem.published_knowledge_item_id = knowledgeId;
      result.postmortem.updated_at = now;
      result.knowledge_item = {
        id: knowledgeId,
        tenant_id: result.postmortem.tenant_id,
        cluster_id: result.postmortem.cluster_id,
        title: `Postmortem: ${result.revisions.at(-1)?.summary ?? id}`,
        component: input.component,
        rocketmq_version_range: input.rocketmq_version_range,
        source_uri: `rocketmq-sre://postmortems/${id}`,
        source_version: `revision-${result.postmortem.current_revision}`,
        valid_from: now,
        valid_until: null,
        owner: input.owner,
        review_status: "validated",
        review_due_at: input.review_due_at,
        sensitivity: "internal",
        content_hash: `sha256:${"0".repeat(64)}`,
        conflict: false,
        created_at: now,
        updated_at: now,
      };
      return clone(result);
    },
    listActionItems: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return {
        items: clone(
          actionItems.filter((item) => item.cluster_id === clusterId),
        ),
        partial: false,
        observed_at: new Date().toISOString(),
      };
    },
    patchActionItem: async (
      id,
      input: ActionItemPatchRequest,
      signal,
    ) => {
      await wait(signal);
      const item =
        actionItems.find((candidate) => candidate.id === id) ??
        unavailable("action item");
      scope(item.cluster_id);
      item.status = input.status;
      item.owner = input.owner ?? item.owner;
      item.due_at = input.due_at ?? item.due_at;
      item.verification = input.verification ?? null;
      item.evidence_ids = input.evidence_ids ?? [];
      item.updated_at = new Date().toISOString();
      item.completed_at =
        input.status === "completed" ? item.updated_at : null;
      for (const postmortem of postmortems) {
        const index = postmortem.action_items.findIndex(
          (candidate) => candidate.id === id,
        );
        if (index >= 0) {
          postmortem.action_items[index] = clone(item);
        }
      }
      return clone(item);
    },
    diagnoseIncident: async (id, signal) => {
      await wait(signal);
      const result =
        incidents.find((item) => item.incident.id === id) ??
        unavailable("incident");
      scope(result.incident.cluster_id);
      return {
        schema_version: "rocketmq-sre.diagnosis-dispatch.v1",
        incident_id: id,
        status: "queued",
        execution_eligible: false,
        correlation_id: crypto.randomUUID(),
      };
    },
    listInspections: async (clusterId, signal) => {
      await wait(signal);
      return listViews(
        inspections,
        clusterId,
        (item) => item.run.cluster_id,
      );
    },
    getInspection: async (id, signal) => {
      await wait(signal);
      const result =
        inspections.find((item) => item.run.id === id) ??
        unavailable("inspection");
      scope(result.run.cluster_id);
      return clone(result);
    },
    createInspection: async (input, signal) => {
      await wait(signal);
      scope(input.cluster_id);
      const now = new Date().toISOString();
      const result: InspectionView = {
        run: {
          id: crypto.randomUUID(),
          tenant_id: auth?.tenantId ?? DEMO_TENANT_ID,
          cluster_id: input.cluster_id,
          template: input.template,
          status: input.schedule ? "scheduled" : "running",
          schedule: input.schedule,
          finding_count: 0,
          partial: false,
          started_at: input.schedule ? undefined : now,
          created_at: now,
        },
        recommendations: [],
      };
      inspections.unshift(result);
      return clone(result);
    },
    runInspection: async (id, signal) => {
      await wait(signal);
      const result =
        inspections.find((item) => item.run.id === id) ??
        unavailable("inspection");
      scope(result.run.cluster_id);
      const now = new Date().toISOString();
      result.run.status = "completed";
      result.run.started_at ??= now;
      result.run.completed_at = now;
      result.run.finding_count = Math.max(result.run.finding_count, 1);
      if (result.recommendations.length === 0) {
        const recommendation: Recommendation = {
          id: crypto.randomUUID(),
          inspection_run_id: result.run.id,
          tenant_id: result.run.tenant_id,
          cluster_id: result.run.cluster_id,
          severity: "warning",
          title: `${result.run.template} 检查发现需人工确认的证据`,
          rationale: "建议复核 partial 证据并保留当前只读边界。",
          evidence_ids: [],
          status: "open",
          created_at: now,
          updated_at: now,
        };
        result.recommendations.push(recommendation);
        recommendations.unshift(clone(recommendation));
      }
      return clone(result);
    },
    getInspectionReport: async (id, format, signal) => {
      await wait(signal);
      const result =
        inspections.find((item) => item.run.id === id) ??
        unavailable("inspection");
      scope(result.run.cluster_id);
      const markdown = [
        `# Inspection ${result.run.id}`,
        "",
        `- Template: ${result.run.template}`,
        `- Status: ${result.run.status}`,
        `- Partial: ${String(result.run.partial)}`,
        `- Findings: ${result.run.finding_count}`,
      ].join("\n");
      return format === "html"
        ? {
            schema_version: "rocketmq-sre.inspection-report.v1",
            media_type: "text/html; charset=utf-8",
            file_name: `inspection-${id}.html`,
            content: `<h1>Inspection ${id}</h1><p>Status: ${result.run.status}</p>`,
          }
        : {
            schema_version: "rocketmq-sre.inspection-report.v1",
            media_type: "text/markdown; charset=utf-8",
            file_name: `inspection-${id}.md`,
            content: markdown,
          };
    },
    listRecommendations: async (clusterId, signal) => {
      await wait(signal);
      return scopedEnvelope(
        { ...phase1Recommendations, items: recommendations },
        clusterId,
      );
    },
    dispositionRecommendation: async (id, input, signal) => {
      await wait(signal);
      const recommendation =
        recommendations.find((item) => item.id === id) ??
        inspections
          .flatMap((item) => item.recommendations)
          .find((item) => item.id === id) ??
        unavailable("recommendation");
      scope(recommendation.cluster_id);
      if (
        ["dismissed", "resolved", "promoted"].includes(
          recommendation.status,
        )
      ) {
        throw new ApiError(
          409,
          "workflow_conflict",
          "terminal recommendation cannot be changed",
        );
      }
      if (input.status === "assigned" && !input.assignee?.trim()) {
        throw new ApiError(
          400,
          "invalid_recommendation",
          "assigned recommendations require an assignee",
        );
      }
      const updated = {
        ...recommendation,
        status: input.status,
        assignee: input.assignee?.trim() || undefined,
        investigation_id:
          input.status === "promoted"
            ? (recommendation.investigation_id ?? crypto.randomUUID())
            : recommendation.investigation_id,
        incident_id:
          input.status === "promoted" && input.promote_to === "incident"
            ? (recommendation.incident_id ?? crypto.randomUUID())
            : recommendation.incident_id,
        updated_at: new Date().toISOString(),
      };
      for (const source of [
        recommendations,
        ...inspections.map((item) => item.recommendations),
      ]) {
        const index = source.findIndex((item) => item.id === id);
        if (index >= 0) {
          source[index] = clone(updated);
        }
      }
      return clone(updated);
    },
    listEvidence: async (clusterId, signal) => {
      await wait(signal);
      return scopedEnvelope({ ...phase1Evidence, items: evidence }, clusterId);
    },
    getEvidence: async (id, signal) => {
      await wait(signal);
      const result =
        evidence.find((item) => item.evidence_id === id) ??
        unavailable("evidence");
      scope(result.cluster_id);
      return clone(result);
    },
    getEvidenceContent: async (id, signal) => {
      await wait(signal);
      const result =
        evidence.find((item) => item.evidence_id === id) ??
        unavailable("evidence");
      scope(result.cluster_id);
      return result.content.storage === "inline"
        ? clone(result.content.value)
        : {
            visible_connections: 3,
            truncated: true,
          };
    },
    getMessageJourney: async (clusterId, traceOrMessageId, signal) => {
      await wait(signal);
      scope(clusterId);
      if (!traceOrMessageId.trim()) {
        throw new ApiError(
          400,
          "invalid_request",
          "message or trace identifier is required",
        );
      }
      return {
        ...clone(phase1MessageJourney),
        cluster_id: clusterId,
      };
    },
    listKnowledge: async (clusterId, signal) => {
      await wait(signal);
      scope(clusterId);
      return {
        ...clone(phase1Knowledge),
        items: phase1Knowledge.items.filter(
          (item) => !item.cluster_id || item.cluster_id === clusterId,
        ),
      };
    },
    getModelCapabilities: async (signal) => {
      await wait(signal);
      return clone(phase1Models);
    },
    subscribeWorkflowEvents: async (onEvent, signal) => {
      await new Promise<void>((resolve) => {
        let index = 0;
        const emit = () => {
          if (signal.aborted) {
            resolve();
            return;
          }
          const allowed = phase1WorkflowEvents.filter(
            (event) =>
              !auth || auth.clusterIds.includes(event.cluster_id),
          );
          if (allowed.length > 0) {
            onEvent(clone(allowed[index % allowed.length]));
            index += 1;
          }
        };
        const timer = window.setInterval(emit, 4_000);
        const stop = () => {
          window.clearInterval(timer);
          resolve();
        };
        signal.addEventListener("abort", stop, { once: true });
        window.setTimeout(emit, 250);
      });
    },
  };
}
