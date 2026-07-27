import { ApiError, type SreApi } from "@/api/client";
import type {
  CollectionEnvelope,
  ConversationView,
  IncidentView,
  InspectionView,
  InvestigationView,
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

const WAIT_MS = 90;

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
