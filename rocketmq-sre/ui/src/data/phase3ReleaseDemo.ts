import { ApiError } from "@/api/client";
import type { ReleaseManagementApi } from "@/api/releaseManagementClient";
import type {
  IntegrationAdapterKind,
  IntegrationDelivery,
  IntegrationDescriptor,
  IntegrationTargetView,
  ReleaseDetail,
  ReleaseObservation,
  ReleaseReport,
  ReleaseWorkflow,
} from "@/api/types";
import type { ApiRequestContext } from "@/auth/AuthContext";

const DEMO_CLUSTER_ID = "10000000-0000-4000-8000-000000000001";
const DEMO_TENANT_ID = "00000000-0000-4000-8000-000000000001";
const DEMO_INCIDENT_ID = "20000000-0000-4000-8000-000000000001";
const WAIT_MS = 90;
const now = Date.now();

const descriptors = [
  descriptor("rocketmq-sre.integration.mock-itsm", "mock_itsm", true),
  descriptor(
    "rocketmq-sre.integration.signed-webhook-itsm",
    "signed_webhook_itsm",
    true,
  ),
  descriptor(
    "rocketmq-sre.integration.chat-ops",
    "chat_ops_webhook",
    false,
  ),
  descriptor("rocketmq-sre.integration.pager", "pager", false),
  descriptor("rocketmq-sre.integration.email", "email", false),
] satisfies IntegrationDescriptor[];

const integrationTargets: IntegrationTargetView[] = [
  {
    id: "41000000-0000-4000-8000-000000000001",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    descriptor_id: "rocketmq-sre.integration.signed-webhook-itsm",
    descriptor_version: "1.0.0",
    name: "生产变更工单",
    adapter_kind: "signed_webhook_itsm",
    endpoint: "https://itsm.example.invalid/sre/events",
    secret_reference: "vault://rocketmq-sre/itsm/signing-key",
    enabled: true,
    inbound_approval: true,
    outbound_events: [
      "plan_submitted",
      "approval_changed",
      "release_started",
      "release_paused",
      "release_rolling_back",
      "release_completed",
      "manual_takeover_required",
    ],
    created_at: new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date(now - 8 * 60 * 1000).toISOString(),
    notification_target_id: null,
  },
  {
    id: "41000000-0000-4000-8000-000000000002",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    descriptor_id: "rocketmq-sre.integration.chat-ops",
    descriptor_version: "1.0.0",
    name: "Messaging SRE ChatOps",
    adapter_kind: "chat_ops_webhook",
    endpoint: "https://chatops.example.invalid/rocketmq-sre",
    secret_reference: null,
    enabled: true,
    inbound_approval: false,
    outbound_events: [
      "release_started",
      "release_paused",
      "release_completed",
      "manual_takeover_required",
    ],
    created_at: new Date(now - 14 * 24 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date(now - 5 * 60 * 1000).toISOString(),
    notification_target_id:
      "42000000-0000-4000-8000-000000000001",
  },
];

const integrationDeliveries: IntegrationDelivery[] = [
  {
    schema_version: "rocketmq-sre.integration-delivery.v1",
    id: "43000000-0000-4000-8000-000000000001",
    target_id: integrationTargets[0].id,
    descriptor_id: integrationTargets[0].descriptor_id,
    descriptor_version: "1.0.0",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    incident_id: DEMO_INCIDENT_ID,
    plan_id: "44000000-0000-4000-8000-000000000001",
    release_id: "45000000-0000-4000-8000-000000000001",
    event_kind: "release_started",
    idempotency_key: "release-started:45000000:41000000",
    sanitized_summary: "Proxy 5.3.0 canary 已开始，持续观察 SLO 与探针。",
    deep_link: "/changes/releases/45000000-0000-4000-8000-000000000001",
    status: "delivered",
    attempt_count: 1,
    delivered_at: new Date(now - 18 * 60 * 1000).toISOString(),
    created_at: new Date(now - 19 * 60 * 1000).toISOString(),
  },
  {
    schema_version: "rocketmq-sre.integration-delivery.v1",
    id: "43000000-0000-4000-8000-000000000002",
    target_id: integrationTargets[1].id,
    descriptor_id: integrationTargets[1].descriptor_id,
    descriptor_version: "1.0.0",
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    incident_id: DEMO_INCIDENT_ID,
    plan_id: "44000000-0000-4000-8000-000000000001",
    release_id: "45000000-0000-4000-8000-000000000001",
    event_kind: "release_paused",
    idempotency_key: "release-paused:45000000:41000000",
    sanitized_summary: "Canary 观察短暂回归，发布已进入受控暂停。",
    deep_link: "/changes/releases/45000000-0000-4000-8000-000000000001",
    status: "retry_scheduled",
    attempt_count: 2,
    next_attempt_at: new Date(now + 2 * 60 * 1000).toISOString(),
    last_error_code: "adapter_unavailable",
    created_at: new Date(now - 3 * 60 * 1000).toISOString(),
  },
];

const canaryRelease = workflow(
  "45000000-0000-4000-8000-000000000001",
  "REL-2026.07.28-PROXY",
  "CHG-20260728-018",
  "5.3.0",
  "canary_running",
);
canaryRelease.active_execution_id =
  "46000000-0000-4000-8000-000000000001";
canaryRelease.readiness = readiness();

const completedRelease = workflow(
  "45000000-0000-4000-8000-000000000002",
  "REL-2026.07.27-BROKER",
  "CHG-20260727-009",
  "5.2.1",
  "completed",
);
completedRelease.readiness = readiness();

const beforeObservation = observation(
  "before",
  "发布前 SLO、消费延迟和 synthetic Probe 均健康。",
  -45,
);
const duringObservation = observation(
  "during",
  "Canary 已承载 10% 流量，错误率和 P99 保持预算内。",
  -12,
);
const afterObservation = observation(
  "after",
  "全量完成后连续三个窗口无回归，消费延迟已回到基线。",
  -90,
);

const releaseDetails: ReleaseDetail[] = [
  {
    schema_version: "rocketmq-sre.release-detail.v1",
    workflow: canaryRelease,
    observations: [beforeObservation, duringObservation],
    report: null,
  },
  {
    schema_version: "rocketmq-sre.release-detail.v1",
    workflow: completedRelease,
    observations: [beforeObservation, duringObservation, afterObservation],
    report: report(
      completedRelease,
      [beforeObservation],
      [duringObservation],
      [afterObservation],
    ),
  },
];

export function createMockReleaseManagementApi(
  auth: ApiRequestContext,
): ReleaseManagementApi {
  const checkScope = (clusterId: string) => {
    if (!auth.clusterIds.includes(clusterId)) {
      throw new ApiError(
        403,
        "cluster_not_allowed",
        "cluster is outside the authenticated scope",
      );
    }
  };
  const findTarget = (targetId: string) => {
    const target = integrationTargets.find((item) => item.id === targetId);
    if (!target || !target.cluster_id) {
      throw new ApiError(
        404,
        "source_unavailable",
        "integration target is unavailable",
      );
    }
    checkScope(target.cluster_id);
    return target;
  };
  const findRelease = (releaseId: string) => {
    const detail = releaseDetails.find(
      (item) => item.workflow.id === releaseId,
    );
    if (!detail) {
      throw new ApiError(
        404,
        "source_unavailable",
        "release workflow is unavailable",
      );
    }
    checkScope(detail.workflow.cluster_id);
    return detail;
  };

  return {
    listIntegrationDescriptors: async (signal) => {
      await wait(signal);
      return clone(descriptors);
    },
    listIntegrationTargets: async (
      clusterId,
      adapterKind,
      enabled,
      limit = 200,
      signal,
    ) => {
      checkScope(clusterId);
      await wait(signal);
      const items = integrationTargets
        .filter(
          (item) =>
            item.cluster_id === clusterId &&
            (!adapterKind || item.adapter_kind === adapterKind) &&
            (enabled === undefined || item.enabled === enabled),
        )
        .slice(0, limit);
      return page(
        "rocketmq-sre.integration-target-page.v1",
        clone(items),
      );
    },
    registerIntegrationTarget: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      const target: IntegrationTargetView = {
        id: crypto.randomUUID(),
        tenant_id: auth.tenantId,
        cluster_id: input.cluster_id,
        descriptor_id: input.descriptor_id,
        descriptor_version: input.descriptor_version,
        name: input.name,
        adapter_kind: input.adapter_kind,
        endpoint: input.endpoint,
        secret_reference: input.secret_reference,
        enabled: input.enabled,
        inbound_approval: input.inbound_approval,
        outbound_events: input.outbound_events,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        notification_target_id: input.notification_target_id ?? null,
      };
      integrationTargets.unshift(target);
      return clone(target);
    },
    getIntegrationTarget: async (targetId, signal) => {
      await wait(signal);
      return clone(findTarget(targetId));
    },
    setIntegrationTargetState: async (targetId, input, signal) => {
      await wait(signal);
      const target = findTarget(targetId);
      target.enabled = input.enabled;
      target.updated_at = new Date().toISOString();
      return clone(target);
    },
    listIntegrationDeliveries: async (
      clusterId,
      targetId,
      limit = 200,
      signal,
    ) => {
      checkScope(clusterId);
      await wait(signal);
      const items = integrationDeliveries
        .filter(
          (item) =>
            item.cluster_id === clusterId &&
            (!targetId || item.target_id === targetId),
        )
        .slice(0, limit);
      return page(
        "rocketmq-sre.integration-delivery-page.v1",
        clone(items),
      );
    },
    applyExternalApproval: async (input, signal) => {
      await wait(signal);
      findTarget(input.target_id);
      return {
        schema_version: "rocketmq-sre.external-approval-view.v1",
        duplicate: false,
        plan_status:
          input.decision === "approved" ? "approved" : "rejected",
        approval: {
          id: crypto.randomUUID(),
          tenant_id: auth.tenantId,
          cluster_id: DEMO_CLUSTER_ID,
          plan_id: input.plan_id,
          plan_hash: input.plan_hash,
          decision: input.decision,
          requester_subject: "release-operator",
          approver_subject: input.subject,
          approver_role: "approver",
          reason: `External approval ${input.external_event_id}`,
          decided_at: input.occurred_at,
          expires_at: input.expires_at,
        },
      };
    },
    listReleases: async (
      clusterId,
      status,
      limit = 200,
      signal,
    ) => {
      checkScope(clusterId);
      await wait(signal);
      const items = releaseDetails
        .map((item) => item.workflow)
        .filter(
          (item) =>
            item.cluster_id === clusterId &&
            (!status || item.status === status),
        )
        .slice(0, limit);
      return page("rocketmq-sre.release-page.v1", clone(items));
    },
    createRelease: async (input, signal) => {
      checkScope(input.cluster_id);
      await wait(signal);
      const created = workflow(
        crypto.randomUUID(),
        input.release_ref,
        input.change_id,
        input.target_version,
        "planned",
      );
      created.incident_id = input.incident_id;
      created.runbook_id = input.runbook_id;
      created.runbook_version = input.runbook_version;
      created.plan_id = input.plan_id;
      created.plan_hash = input.plan_hash;
      created.rollback_plan_id = input.rollback_plan_id;
      created.rollback_plan_hash = input.rollback_plan_hash;
      created.created_by = auth.subject;
      const detail: ReleaseDetail = {
        schema_version: "rocketmq-sre.release-detail.v1",
        workflow: created,
        observations: [],
        report: null,
      };
      releaseDetails.unshift(detail);
      return clone(detail);
    },
    getRelease: async (releaseId, signal) => {
      await wait(signal);
      return clone(findRelease(releaseId));
    },
    prepareRelease: async (releaseId, input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      detail.workflow.status = "ready";
      detail.workflow.readiness = {
        ...readiness(),
        pdb_ready: input.pdb_ready,
        synthetic_probe_ready: input.synthetic_probe_ready,
        evidence_ids: input.evidence_ids,
      };
      touch(detail.workflow);
      return {
        schema_version: "rocketmq-sre.release-preparation-view.v1",
        workflow: clone(detail.workflow),
        upgrade_readiness: {} as never,
        simulation: {} as never,
      };
    },
    startRelease: async (releaseId, _input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      detail.workflow.status = "canary_running";
      detail.workflow.active_execution_id = crypto.randomUUID();
      touch(detail.workflow);
      return {
        schema_version: "rocketmq-sre.release-execution-view.v1",
        workflow: clone(detail.workflow),
        execution_id: detail.workflow.active_execution_id,
      };
    },
    recordReleaseObservation: async (releaseId, input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      const captured: ReleaseObservation = {
        ...input,
        regression_detected:
          !input.slo_healthy || !input.synthetic_probe_healthy,
        observed_at: new Date().toISOString(),
      };
      detail.observations.push(captured);
      if (captured.regression_detected) {
        detail.workflow.status = "paused";
        detail.workflow.regression_detected = true;
        detail.workflow.pause_reason = captured.sanitized_summary;
      }
      touch(detail.workflow);
      return clone(detail);
    },
    pauseRelease: async (releaseId, input, signal) => {
      await wait(signal);
      return transition(findRelease(releaseId), "paused", input.reason);
    },
    resumeRelease: async (releaseId, _input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      detail.workflow.regression_detected = false;
      return transition(detail, "canary_running");
    },
    beginReleaseVerification: async (releaseId, signal) => {
      await wait(signal);
      return transition(findRelease(releaseId), "verifying");
    },
    completeRelease: async (releaseId, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      transition(detail, "completed");
      detail.report = buildReport(detail);
      return clone(detail);
    },
    startReleaseRollback: async (releaseId, _input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      detail.workflow.status = "rolling_back";
      detail.workflow.active_execution_id = crypto.randomUUID();
      touch(detail.workflow);
      return {
        schema_version: "rocketmq-sre.release-execution-view.v1",
        workflow: clone(detail.workflow),
        execution_id: detail.workflow.active_execution_id,
      };
    },
    completeReleaseRollback: async (releaseId, input, signal) => {
      await wait(signal);
      const detail = findRelease(releaseId);
      detail.observations.push({
        ...input.observation,
        regression_detected:
          !input.observation.slo_healthy ||
          !input.observation.synthetic_probe_healthy,
        observed_at: new Date().toISOString(),
      });
      transition(
        detail,
        input.succeeded ? "rolled_back" : "manual_takeover",
        input.reason,
      );
      detail.report = buildReport(detail);
      return clone(detail);
    },
    enterManualTakeover: async (releaseId, input, signal) => {
      await wait(signal);
      return transition(
        findRelease(releaseId),
        "manual_takeover",
        input.reason,
      );
    },
  };
}

function descriptor(
  id: string,
  kind: IntegrationAdapterKind,
  inbound: boolean,
): IntegrationDescriptor {
  return {
    id,
    version: "1.0.0",
    owner: "rocketmq-sre",
    supported_versions: [
      {
        family: "rocketmq-sre.integration",
        major: 1,
        minor: 0,
        required_features: [],
      },
    ],
    required_capabilities: [],
    config_schema: {},
    status: "active",
    deprecation: null,
    integration_kind: kind,
    inbound,
    outbound: true,
  };
}

function workflow(
  id: string,
  releaseRef: string,
  changeId: string,
  targetVersion: string,
  status: ReleaseWorkflow["status"],
): ReleaseWorkflow {
  return {
    schema_version: "rocketmq-sre.release-workflow.v1",
    id,
    tenant_id: DEMO_TENANT_ID,
    cluster_id: DEMO_CLUSTER_ID,
    incident_id: DEMO_INCIDENT_ID,
    correlation_id: crypto.randomUUID(),
    change_id: changeId,
    release_ref: releaseRef,
    target_version: targetVersion,
    runbook_id: "47000000-0000-4000-8000-000000000001",
    runbook_version: "1.0.0",
    plan_id: "44000000-0000-4000-8000-000000000001",
    plan_hash: digest("a"),
    rollback_plan_id: "44000000-0000-4000-8000-000000000002",
    rollback_plan_hash: digest("b"),
    readiness: null,
    status,
    active_execution_id: null,
    regression_detected: false,
    pause_reason: null,
    created_by: "release-operator",
    created_at: new Date(now - 2 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date(now - 3 * 60 * 1000).toISOString(),
  };
}

function readiness() {
  return {
    upgrade_readiness_id:
      "48000000-0000-4000-8000-000000000001",
    simulation_id: "49000000-0000-4000-8000-000000000001",
    pdb_ready: true,
    capacity_ready: true,
    quorum_ready: true,
    store_recovery_ready: true,
    synthetic_probe_ready: true,
    evidence_ids: [
      "4a000000-0000-4000-8000-000000000001",
      "4a000000-0000-4000-8000-000000000002",
    ],
    observed_at: new Date(now - 50 * 60 * 1000).toISOString(),
    valid_until: new Date(now + 70 * 60 * 1000).toISOString(),
  };
}

function observation(
  phase: ReleaseObservation["phase"],
  summary: string,
  offsetMinutes: number,
): ReleaseObservation {
  return {
    phase,
    slo_healthy: true,
    synthetic_probe_healthy: true,
    regression_detected: false,
    evidence_ids: ["4a000000-0000-4000-8000-000000000001"],
    sanitized_summary: summary,
    observed_at: new Date(
      now + offsetMinutes * 60 * 1000,
    ).toISOString(),
  };
}

function report(
  workflowValue: ReleaseWorkflow,
  before: ReleaseObservation[],
  during: ReleaseObservation[],
  after: ReleaseObservation[],
): ReleaseReport {
  return {
    schema_version: "rocketmq-sre.release-report.v1",
    id: crypto.randomUUID(),
    release_id: workflowValue.id,
    tenant_id: workflowValue.tenant_id,
    cluster_id: workflowValue.cluster_id,
    incident_id: workflowValue.incident_id,
    change_id: workflowValue.change_id,
    release_ref: workflowValue.release_ref,
    final_status: workflowValue.status,
    before,
    during,
    after,
    generated_at: new Date().toISOString(),
  };
}

function buildReport(detail: ReleaseDetail) {
  return report(
    detail.workflow,
    detail.observations.filter((item) => item.phase === "before"),
    detail.observations.filter((item) => item.phase === "during"),
    detail.observations.filter((item) => item.phase === "after"),
  );
}

function transition(
  detail: ReleaseDetail,
  status: ReleaseWorkflow["status"],
  reason?: string,
) {
  detail.workflow.status = status;
  detail.workflow.pause_reason =
    status === "paused" || status === "manual_takeover"
      ? reason ?? null
      : null;
  touch(detail.workflow);
  return clone(detail);
}

function touch(workflowValue: ReleaseWorkflow) {
  workflowValue.updated_at = new Date().toISOString();
}

function page<TSchema extends string, T>(
  schemaVersion: TSchema,
  items: T[],
) {
  return {
    schema_version: schemaVersion,
    items,
    partial: false,
  };
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

async function wait(signal?: AbortSignal) {
  if (signal?.aborted) {
    throw new DOMException("The operation was aborted", "AbortError");
  }
  await new Promise((resolve) => window.setTimeout(resolve, WAIT_MS));
  if (signal?.aborted) {
    throw new DOMException("The operation was aborted", "AbortError");
  }
}

function digest(character: string) {
  return `sha256:${character.repeat(64)}`;
}
