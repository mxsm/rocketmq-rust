import type { ExecutionSubmissionView, SubmitExecutionRequest } from "@/api/types";
import { actionPlanFixture, READINESS_NOW, READINESS_SUBJECT } from "@/features/incidents/test-fixtures/actionReadiness";

/** Synthetic receipt only: these marker signatures establish no real authority. */
export function executionFixture() {
  const { planView } = actionPlanFixture("broker/127.0.0.1:10911");
  const plan = planView.plan;
  plan.status = "approved";
  const selector: SubmitExecutionRequest = {
    plan_id: plan.id, plan_hash: plan.plan_hash, precondition_hash: planView.precondition_hash,
    idempotency_key: "synthetic-d20-original-intent",
  };
  const receipt: ExecutionSubmissionView = {
    submitted_at: READINESS_NOW, state: "unknown",
    execution: {
      schema_version: "rocketmq-sre.execution-request.v1", id: "35000000-0000-4000-8000-000000000001",
      tenant_id: plan.tenant_id, cluster_id: plan.cluster_id,
      correlation_id: "33000000-0000-4000-8000-000000000001", plan,
      approvals: [{
        approval_id: "38000000-0000-4000-8000-000000000001", approver_subject: "synthetic-other-approver",
        audience: "rocketmq-sre-executor", tenant_id: plan.tenant_id, cluster_id: plan.cluster_id,
        plan_id: plan.id, plan_hash: plan.plan_hash, precondition_hash: selector.precondition_hash,
        issued_at: READINESS_NOW, expires_at: plan.expires_at, issuer: "synthetic-control-plane",
        nonce: "synthetic-private-grant-nonce", signature: "synthetic-private-grant-signature",
      }],
      requested_by: READINESS_SUBJECT, idempotency_key: selector.idempotency_key,
      issuer: "synthetic-control-plane", audience: "rocketmq-sre-executor", issued_at: READINESS_NOW,
      expires_at: plan.expires_at, nonce: "synthetic-private-request-nonce", signature: "synthetic-private-request-signature",
    },
  };
  const owner = { tenantId: plan.tenant_id, clusterIds: [plan.cluster_id], executionId: receipt.execution.id, subject: READINESS_SUBJECT };
  return { receipt, selector, owner };
}
