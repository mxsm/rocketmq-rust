import type { ApiRequestContext } from "@/auth/AuthContext";

import { apiQuery, apiRequest } from "./client";
import type {
  CompleteRollbackRequest,
  CreateReleaseRequest,
  ExternalApprovalInput,
  ExternalApprovalView,
  IntegrationAdapterKind,
  IntegrationDeliveryPage,
  IntegrationDescriptor,
  IntegrationTargetPage,
  IntegrationTargetView,
  PrepareReleaseRequest,
  RecordReleaseObservationRequest,
  RegisterIntegrationTargetRequest,
  ReleaseDetail,
  ReleaseExecutionRequest,
  ReleaseExecutionView,
  ReleasePage,
  ReleasePreparationView,
  ReleaseStatus,
  ReleaseTransitionRequest,
  SetIntegrationTargetStateRequest,
} from "./types";

export interface ReleaseManagementApi {
  listIntegrationDescriptors: (
    signal?: AbortSignal,
  ) => Promise<IntegrationDescriptor[]>;
  listIntegrationTargets: (
    clusterId: string,
    adapterKind?: IntegrationAdapterKind,
    enabled?: boolean,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<IntegrationTargetPage>;
  registerIntegrationTarget: (
    input: RegisterIntegrationTargetRequest,
    signal?: AbortSignal,
  ) => Promise<IntegrationTargetView>;
  getIntegrationTarget: (
    targetId: string,
    signal?: AbortSignal,
  ) => Promise<IntegrationTargetView>;
  setIntegrationTargetState: (
    targetId: string,
    input: SetIntegrationTargetStateRequest,
    signal?: AbortSignal,
  ) => Promise<IntegrationTargetView>;
  listIntegrationDeliveries: (
    clusterId: string,
    targetId?: string,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<IntegrationDeliveryPage>;
  applyExternalApproval: (
    input: ExternalApprovalInput,
    signal?: AbortSignal,
  ) => Promise<ExternalApprovalView>;
  listReleases: (
    clusterId: string,
    status?: ReleaseStatus,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<ReleasePage>;
  createRelease: (
    input: CreateReleaseRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  getRelease: (
    releaseId: string,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  prepareRelease: (
    releaseId: string,
    input: PrepareReleaseRequest,
    signal?: AbortSignal,
  ) => Promise<ReleasePreparationView>;
  startRelease: (
    releaseId: string,
    input: ReleaseExecutionRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseExecutionView>;
  recordReleaseObservation: (
    releaseId: string,
    input: RecordReleaseObservationRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  pauseRelease: (
    releaseId: string,
    input: ReleaseTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  resumeRelease: (
    releaseId: string,
    input: ReleaseTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  beginReleaseVerification: (
    releaseId: string,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  completeRelease: (
    releaseId: string,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  startReleaseRollback: (
    releaseId: string,
    input: ReleaseExecutionRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseExecutionView>;
  completeReleaseRollback: (
    releaseId: string,
    input: CompleteRollbackRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
  enterManualTakeover: (
    releaseId: string,
    input: ReleaseTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ReleaseDetail>;
}

export function createReleaseManagementApi(
  auth: ApiRequestContext,
): ReleaseManagementApi {
  const get = <T>(path: string, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, signal });
  const post = <T>(
    path: string,
    body: unknown,
    signal?: AbortSignal,
  ) => apiRequest<T>(path, { auth, body, method: "POST", signal });
  const postWithoutBody = <T>(path: string, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, method: "POST", signal });
  const releasePath = (releaseId: string, suffix = "") =>
    `/v1/releases/${encodeURIComponent(releaseId)}${suffix}`;

  return {
    listIntegrationDescriptors: (signal) =>
      get<IntegrationDescriptor[]>(
        "/v1/integrations/descriptors",
        signal,
      ),
    listIntegrationTargets: (
      clusterId,
      adapterKind,
      enabled,
      limit,
      signal,
    ) =>
      get<IntegrationTargetPage>(
        apiQuery("/v1/integrations/targets", {
          cluster_id: clusterId,
          adapter_kind: adapterKind,
          enabled: enabled === undefined ? undefined : String(enabled),
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    registerIntegrationTarget: (input, signal) =>
      post<IntegrationTargetView>(
        "/v1/integrations/targets",
        input,
        signal,
      ),
    getIntegrationTarget: (targetId, signal) =>
      get<IntegrationTargetView>(
        `/v1/integrations/targets/${encodeURIComponent(targetId)}`,
        signal,
      ),
    setIntegrationTargetState: (targetId, input, signal) =>
      post<IntegrationTargetView>(
        `/v1/integrations/targets/${encodeURIComponent(targetId)}/state`,
        input,
        signal,
      ),
    listIntegrationDeliveries: (
      clusterId,
      targetId,
      limit,
      signal,
    ) =>
      get<IntegrationDeliveryPage>(
        apiQuery("/v1/integrations/deliveries", {
          cluster_id: clusterId,
          target_id: targetId,
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    applyExternalApproval: (input, signal) =>
      post<ExternalApprovalView>(
        "/v1/integrations/approvals/external",
        input,
        signal,
      ),
    listReleases: (clusterId, status, limit, signal) =>
      get<ReleasePage>(
        apiQuery("/v1/releases", {
          cluster_id: clusterId,
          status,
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    createRelease: (input, signal) =>
      post<ReleaseDetail>("/v1/releases", input, signal),
    getRelease: (releaseId, signal) =>
      get<ReleaseDetail>(releasePath(releaseId), signal),
    prepareRelease: (releaseId, input, signal) =>
      post<ReleasePreparationView>(
        releasePath(releaseId, "/prepare"),
        input,
        signal,
      ),
    startRelease: (releaseId, input, signal) =>
      post<ReleaseExecutionView>(
        releasePath(releaseId, "/start"),
        input,
        signal,
      ),
    recordReleaseObservation: (releaseId, input, signal) =>
      post<ReleaseDetail>(
        releasePath(releaseId, "/observations"),
        input,
        signal,
      ),
    pauseRelease: (releaseId, input, signal) =>
      post<ReleaseDetail>(
        releasePath(releaseId, "/pause"),
        input,
        signal,
      ),
    resumeRelease: (releaseId, input, signal) =>
      post<ReleaseDetail>(
        releasePath(releaseId, "/resume"),
        input,
        signal,
      ),
    beginReleaseVerification: (releaseId, signal) =>
      postWithoutBody<ReleaseDetail>(
        releasePath(releaseId, "/verification/start"),
        signal,
      ),
    completeRelease: (releaseId, signal) =>
      postWithoutBody<ReleaseDetail>(
        releasePath(releaseId, "/complete"),
        signal,
      ),
    startReleaseRollback: (releaseId, input, signal) =>
      post<ReleaseExecutionView>(
        releasePath(releaseId, "/rollback/start"),
        input,
        signal,
      ),
    completeReleaseRollback: (releaseId, input, signal) =>
      post<ReleaseDetail>(
        releasePath(releaseId, "/rollback/complete"),
        input,
        signal,
      ),
    enterManualTakeover: (releaseId, input, signal) =>
      post<ReleaseDetail>(
        releasePath(releaseId, "/manual-takeover"),
        input,
        signal,
      ),
  };
}
