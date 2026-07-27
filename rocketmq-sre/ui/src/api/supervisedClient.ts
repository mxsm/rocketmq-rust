import type { ApiRequestContext } from "@/auth/AuthContext";

import { ApiError } from "./client";
import type {
  ActionPlanView,
  ApprovalDecisionRequest,
  ApprovalDecisionResponse,
  AuditPage,
  ClearQuarantineRequest,
  CreatePlanRequest,
  CreatePlanResponse,
  ExecutionSubmissionView,
  QuarantinePage,
  ResourceQuarantine,
  SubmitExecutionRequest,
} from "./types";

export interface SupervisedSreApi {
  createPlan: (
    input: CreatePlanRequest,
    signal?: AbortSignal,
  ) => Promise<CreatePlanResponse>;
  getPlan: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<ActionPlanView>;
  approvePlan: (
    id: string,
    input: ApprovalDecisionRequest,
    signal?: AbortSignal,
  ) => Promise<ApprovalDecisionResponse>;
  rejectPlan: (
    id: string,
    input: ApprovalDecisionRequest,
    signal?: AbortSignal,
  ) => Promise<ApprovalDecisionResponse>;
  submitExecution: (
    input: SubmitExecutionRequest,
    signal?: AbortSignal,
  ) => Promise<ExecutionSubmissionView>;
  getExecution: (
    id: string,
    signal?: AbortSignal,
  ) => Promise<ExecutionSubmissionView>;
  getAudit: (
    correlationId: string,
    signal?: AbortSignal,
  ) => Promise<AuditPage>;
  listQuarantines: (
    clusterId: string,
    includeCleared?: boolean,
    signal?: AbortSignal,
  ) => Promise<QuarantinePage>;
  clearQuarantine: (
    id: string,
    input: ClearQuarantineRequest,
    signal?: AbortSignal,
  ) => Promise<ResourceQuarantine>;
}

export function createSupervisedSreApi(
  auth: ApiRequestContext,
): SupervisedSreApi {
  const get = <T>(path: string, signal?: AbortSignal) =>
    supervisedRequest<T>(path, auth, "GET", undefined, signal);
  const post = <T>(
    path: string,
    body: unknown,
    signal?: AbortSignal,
  ) => supervisedRequest<T>(path, auth, "POST", body, signal);

  return {
    createPlan: (input, signal) =>
      post<CreatePlanResponse>("/v1/plans", input, signal),
    getPlan: (id, signal) =>
      get<ActionPlanView>(`/v1/plans/${encodeURIComponent(id)}`, signal),
    approvePlan: (id, input, signal) =>
      post<ApprovalDecisionResponse>(
        `/v1/plans/${encodeURIComponent(id)}/approve`,
        input,
        signal,
      ),
    rejectPlan: (id, input, signal) =>
      post<ApprovalDecisionResponse>(
        `/v1/plans/${encodeURIComponent(id)}/reject`,
        input,
        signal,
      ),
    submitExecution: (input, signal) =>
      post<ExecutionSubmissionView>("/v1/executions", input, signal),
    getExecution: (id, signal) =>
      get<ExecutionSubmissionView>(
        `/v1/executions/${encodeURIComponent(id)}`,
        signal,
      ),
    getAudit: (correlationId, signal) =>
      get<AuditPage>(
        `/v1/audit/${encodeURIComponent(correlationId)}`,
        signal,
      ),
    listQuarantines: (clusterId, includeCleared = false, signal) => {
      const query = new URLSearchParams({
        cluster_id: clusterId,
        include_cleared: String(includeCleared),
      });
      return get<QuarantinePage>(
        `/v1/resource-quarantines?${query.toString()}`,
        signal,
      );
    },
    clearQuarantine: (id, input, signal) =>
      post<ResourceQuarantine>(
        `/v1/resource-quarantines/${encodeURIComponent(id)}/clear`,
        input,
        signal,
      ),
  };
}

async function supervisedRequest<T>(
  path: string,
  auth: ApiRequestContext,
  method: "GET" | "POST",
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const headers = new Headers({
    Accept: "application/json",
    Authorization: `Bearer ${auth.token}`,
    "X-RocketMQ-Tenant": auth.tenantId,
    "X-RocketMQ-Clusters": auth.clusterIds.join(","),
    "X-RocketMQ-Subject": auth.subject,
  });
  if (body !== undefined) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, {
    method,
    headers,
    credentials: "same-origin",
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });
  if (!response.ok) {
    const fallback = {
      code: "source_unavailable",
      message: `API request failed with ${response.status}`,
    };
    const error = await response
      .json()
      .then(
        (value) =>
          value as {
            code?: string;
            message?: string;
          },
      )
      .catch(() => fallback);
    throw new ApiError(
      response.status,
      error.code ?? fallback.code,
      error.message ?? fallback.message,
    );
  }
  return (await response.json()) as T;
}
