import type { ApiRequestContext } from "@/auth/AuthContext";

import { apiQuery, apiRequest } from "./client";
import type {
  ChangeSchedule,
  ChangeSchedulePage,
  ChangeSchedulePreview,
  ChangeScheduleStatus,
  ChangeWindow,
  ChangeWindowPage,
  CreateChangeScheduleRequest,
  CreateChangeWindowRequest,
  CreateRunbookRequest,
  ManualGateDecisionRequest,
  RunbookDefinition,
  RunbookPage,
  ScheduleTransitionRequest,
} from "./types";

export interface ChangeManagementApi {
  listRunbooks: (
    clusterId: string,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<RunbookPage>;
  getRunbook: (
    clusterId: string,
    runbookId: string,
    version: string,
    signal?: AbortSignal,
  ) => Promise<RunbookDefinition>;
  createRunbook: (
    input: CreateRunbookRequest,
    signal?: AbortSignal,
  ) => Promise<RunbookDefinition>;
  listChangeWindows: (
    clusterId: string,
    from: string,
    to: string,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<ChangeWindowPage>;
  createChangeWindow: (
    input: CreateChangeWindowRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeWindow>;
  previewSchedule: (
    input: CreateChangeScheduleRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedulePreview>;
  listSchedules: (
    clusterId: string,
    status?: ChangeScheduleStatus,
    limit?: number,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedulePage>;
  createSchedule: (
    input: CreateChangeScheduleRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  getSchedule: (
    scheduleId: string,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  pauseSchedule: (
    scheduleId: string,
    input: ScheduleTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  resumeSchedule: (
    scheduleId: string,
    input: ScheduleTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  cancelSchedule: (
    scheduleId: string,
    input: ScheduleTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  reconcileSchedule: (
    scheduleId: string,
    input: ScheduleTransitionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  approveManualGate: (
    scheduleId: string,
    stepId: string,
    input: ManualGateDecisionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
  rejectManualGate: (
    scheduleId: string,
    stepId: string,
    input: ManualGateDecisionRequest,
    signal?: AbortSignal,
  ) => Promise<ChangeSchedule>;
}

export function createChangeManagementApi(
  auth: ApiRequestContext,
): ChangeManagementApi {
  const get = <T>(path: string, signal?: AbortSignal) =>
    apiRequest<T>(path, { auth, signal });
  const post = <T>(
    path: string,
    body: unknown,
    signal?: AbortSignal,
  ) => apiRequest<T>(path, { auth, body, method: "POST", signal });

  return {
    listRunbooks: (clusterId, limit, signal) =>
      get<RunbookPage>(
        apiQuery("/v1/runbooks", {
          cluster_id: clusterId,
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    getRunbook: (clusterId, runbookId, version, signal) =>
      get<RunbookDefinition>(
        apiQuery(
          `/v1/runbooks/${encodeURIComponent(runbookId)}/versions/${encodeURIComponent(version)}`,
          { cluster_id: clusterId },
        ),
        signal,
      ),
    createRunbook: (input, signal) =>
      post<RunbookDefinition>("/v1/runbooks", input, signal),
    listChangeWindows: (clusterId, from, to, limit, signal) =>
      get<ChangeWindowPage>(
        apiQuery("/v1/change-windows", {
          cluster_id: clusterId,
          from,
          to,
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    createChangeWindow: (input, signal) =>
      post<ChangeWindow>("/v1/change-windows", input, signal),
    previewSchedule: (input, signal) =>
      post<ChangeSchedulePreview>(
        "/v1/change-schedules/preview",
        input,
        signal,
      ),
    listSchedules: (clusterId, status, limit, signal) =>
      get<ChangeSchedulePage>(
        apiQuery("/v1/change-schedules", {
          cluster_id: clusterId,
          status,
          limit: limit === undefined ? undefined : String(limit),
        }),
        signal,
      ),
    createSchedule: (input, signal) =>
      post<ChangeSchedule>("/v1/change-schedules", input, signal),
    getSchedule: (scheduleId, signal) =>
      get<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}`,
        signal,
      ),
    pauseSchedule: (scheduleId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/pause`,
        input,
        signal,
      ),
    resumeSchedule: (scheduleId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/resume`,
        input,
        signal,
      ),
    cancelSchedule: (scheduleId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/cancel`,
        input,
        signal,
      ),
    reconcileSchedule: (scheduleId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/reconcile`,
        input,
        signal,
      ),
    approveManualGate: (scheduleId, stepId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/manual-gates/${encodeURIComponent(stepId)}/approve`,
        input,
        signal,
      ),
    rejectManualGate: (scheduleId, stepId, input, signal) =>
      post<ChangeSchedule>(
        `/v1/change-schedules/${encodeURIComponent(scheduleId)}/manual-gates/${encodeURIComponent(stepId)}/reject`,
        input,
        signal,
      ),
  };
}
