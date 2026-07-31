// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

/**
 * Minimal, read-only TypeScript client for RocketMQ Rust AI SRE.
 *
 * No generic HTTP method is public. The network surface is fixed to status,
 * cluster, incident, inspection, plan, and OpenAPI GET operations.
 */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface ServiceStatus {
  status: string;
}

export type OnboardingState =
  | "pending"
  | "handshaking"
  | "ready_read_only"
  | "read_only_degraded"
  | "rejected"
  | "offboarded";

export interface Cluster {
  id: string;
  tenant_id: string;
  external_cluster_key: string;
  environment: string;
  region: string;
  rocketmq_version: string;
  deployment_mode: string;
  owner: string;
  state: OnboardingState;
  effective_access_profile: string;
  created_at: string;
  updated_at: string;
  offboarded_at?: string;
}

export type IncidentStatus =
  | "new"
  | "collecting"
  | "diagnosing"
  | "needs_evidence"
  | "monitoring"
  | "resolved"
  | "escalated";

export interface Incident {
  id: string;
  tenant_id: string;
  cluster_id: string;
  title: string;
  resource?: string;
  symptom_family?: string;
  fingerprint?: string;
  severity?: string;
  owner?: string;
  occurrence_count: number;
  last_alert_at?: string;
  reopened_from_incident_id?: string;
  status: IncidentStatus;
  created_at: string;
  updated_at: string;
  hypotheses: JsonValue[];
}

export interface IncidentView {
  incident: Incident;
  investigation: JsonValue | null;
  timeline: JsonValue[];
  diagnosis_revisions: JsonValue[];
}

export type InspectionStatus =
  | "scheduled"
  | "running"
  | "needs_evidence"
  | "completed"
  | "failed"
  | "cancelled";

export interface InspectionRun {
  id: string;
  tenant_id: string;
  cluster_id: string;
  template: string;
  status: InspectionStatus;
  schedule: string | null;
  finding_count: number;
  partial: boolean;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
}

export interface Recommendation {
  id: string;
  inspection_run_id: string;
  tenant_id: string;
  cluster_id: string;
  severity: string;
  title: string;
  rationale: string;
  evidence_ids: string[];
  status: string;
  assignee: string | null;
  investigation_id: string | null;
  incident_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface InspectionView {
  run: InspectionRun;
  recommendations: Recommendation[];
  pack_diffs: JsonValue[];
}

export type PlanStatus =
  | "draft"
  | "needs_critic"
  | "ready_for_approval"
  | "in_review"
  | "approved"
  | "rejected"
  | "expired"
  | "superseded";

export interface ActionPlan {
  schema_version: string;
  id: string;
  tenant_id: string;
  cluster_id: string;
  incident_id: string;
  diagnosis_revision: string;
  primary_model_invocation_id: string;
  diagnosis_execution_eligible: boolean;
  version: number;
  created_by: string;
  created_at: string;
  expires_at: string;
  evidence_hash: string;
  steps: JsonValue[];
  status: PlanStatus;
  submitted_at: string | null;
  plan_hash: string;
}

export interface ActionPlanView {
  plan: ActionPlan;
  risk: "read" | "plan" | "r1" | "r2" | "r3";
  critic_state: JsonValue;
  latest_critic_review: JsonValue | null;
  latest_policy_decision: JsonValue | null;
  latest_approval: JsonValue | null;
}

export interface LocalPlanDraftStep {
  action_id: string;
  descriptor_version: string;
  resource: string;
  parameters: Record<string, JsonValue>;
  evidence_ids: string[];
}

export interface LocalPlanDraftInput {
  cluster_id: string;
  incident_id: string;
  diagnosis_revision_id: string;
  expires_at?: string;
  steps: LocalPlanDraftStep[];
}

/**
 * A local-only plan draft. It is not an approval, execution request, or
 * authorization to mutate RocketMQ.
 */
export interface LocalPlanDraft {
  schema_version: "rocketmq-sre.local-plan-draft.v1";
  mode: "local_only";
  id: string;
  cluster_id: string;
  incident_id: string;
  diagnosis_revision_id: string;
  created_at: string;
  expires_at: string | null;
  steps: LocalPlanDraftStep[];
}

export interface LocalPlanDraftContext {
  id?: string;
  now?: Date;
}

export type TokenProvider =
  | string
  | (() => string | Promise<string>);

export type FetchLike = (
  input: string | URL | Request,
  init?: RequestInit,
) => Promise<Response>;

export interface SreClientOptions {
  baseUrl: string | URL;
  token?: TokenProvider;
  allowedClusters?: Iterable<string>;
  timeoutMs?: number;
  maxResponseBytes?: number;
  fetch?: FetchLike;
}

export class SreApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryable: boolean;
  readonly correlationId: string | null;

  constructor(options: {
    status: number;
    code: string;
    message: string;
    retryable: boolean;
    correlationId: string | null;
  }) {
    super(options.message);
    this.name = "SreApiError";
    this.status = options.status;
    this.code = options.code;
    this.retryable = options.retryable;
    this.correlationId = options.correlationId;
  }
}

export class SreClientConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SreClientConfigurationError";
  }
}

export class SreResponseTooLargeError extends Error {
  readonly limit: number;

  constructor(limit: number) {
    super(`response exceeded the configured ${limit} byte limit`);
    this.name = "SreResponseTooLargeError";
    this.limit = limit;
  }
}

const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function validatedBaseUrl(input: string | URL): URL {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new SreClientConfigurationError(
      "baseUrl must be an absolute HTTP(S) URL",
    );
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new SreClientConfigurationError(
      "baseUrl scheme must be http or https",
    );
  }
  if (url.username || url.password) {
    throw new SreClientConfigurationError(
      "baseUrl must not contain credentials",
    );
  }
  if (url.search || url.hash) {
    throw new SreClientConfigurationError(
      "baseUrl must not contain query or fragment data",
    );
  }
  if (!url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}/`;
  }
  return url;
}

function validatePositiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new SreClientConfigurationError(
      `${name} must be a positive safe integer`,
    );
  }
  return value;
}

function requireUuid(name: string, value: string): string {
  if (!UUID_PATTERN.test(value)) {
    throw new SreClientConfigurationError(`${name} must be a UUID`);
  }
  return value;
}

function requireText(
  name: string,
  value: string,
  maxLength: number,
): string {
  const normalized = value.trim();
  if (!normalized || normalized.length > maxLength) {
    throw new SreClientConfigurationError(
      `${name} must contain 1-${maxLength} characters`,
    );
  }
  return normalized;
}

function sanitizeServerText(value: unknown, fallback: string): string {
  if (typeof value !== "string") {
    return fallback;
  }
  const sanitized = value
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .trim()
    .slice(0, 512);
  return sanitized || fallback;
}

function copyDraftStep(
  step: LocalPlanDraftStep,
  index: number,
): LocalPlanDraftStep {
  if (step.evidence_ids.length === 0 || step.evidence_ids.length > 32) {
    throw new SreClientConfigurationError(
      `steps[${index}].evidence_ids must contain 1-32 IDs`,
    );
  }
  return {
    action_id: requireText(
      `steps[${index}].action_id`,
      step.action_id,
      255,
    ),
    descriptor_version: requireText(
      `steps[${index}].descriptor_version`,
      step.descriptor_version,
      64,
    ),
    resource: requireText(
      `steps[${index}].resource`,
      step.resource,
      512,
    ),
    parameters: structuredClone(step.parameters),
    evidence_ids: step.evidence_ids.map((id, evidenceIndex) =>
      requireUuid(
        `steps[${index}].evidence_ids[${evidenceIndex}]`,
        id,
      ),
    ),
  };
}

/**
 * Creates a typed draft locally. This function performs no network I/O and
 * cannot approve, submit, or execute the result.
 */
export function createLocalPlanDraft(
  input: LocalPlanDraftInput,
  context: LocalPlanDraftContext = {},
): LocalPlanDraft {
  if (input.steps.length === 0 || input.steps.length > 64) {
    throw new SreClientConfigurationError(
      "steps must contain 1-64 typed actions",
    );
  }
  const now = context.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new SreClientConfigurationError("now must be a valid date");
  }
  const expiresAt =
    input.expires_at === undefined ? null : new Date(input.expires_at);
  if (expiresAt !== null && Number.isNaN(expiresAt.getTime())) {
    throw new SreClientConfigurationError(
      "expires_at must be an RFC 3339 timestamp",
    );
  }

  return {
    schema_version: "rocketmq-sre.local-plan-draft.v1",
    mode: "local_only",
    id: requireUuid(
      "id",
      context.id ?? globalThis.crypto.randomUUID(),
    ),
    cluster_id: requireUuid("cluster_id", input.cluster_id),
    incident_id: requireUuid("incident_id", input.incident_id),
    diagnosis_revision_id: requireUuid(
      "diagnosis_revision_id",
      input.diagnosis_revision_id,
    ),
    created_at: now.toISOString(),
    expires_at: expiresAt?.toISOString() ?? null,
    steps: input.steps.map(copyDraftStep),
  };
}

async function boundedBody(
  response: Response,
  limit: number,
): Promise<Uint8Array> {
  const contentLength = response.headers.get("content-length");
  if (
    contentLength !== null &&
    Number.parseInt(contentLength, 10) > limit
  ) {
    throw new SreResponseTooLargeError(limit);
  }
  if (response.body === null) {
    return new Uint8Array();
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  while (true) {
    const result = await reader.read();
    if (result.done) {
      break;
    }
    length += result.value.byteLength;
    if (length > limit) {
      await reader.cancel();
      throw new SreResponseTooLargeError(limit);
    }
    chunks.push(result.value);
  }

  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function decodeJson<T>(body: Uint8Array): T {
  try {
    return JSON.parse(new TextDecoder().decode(body)) as T;
  } catch {
    throw new SreApiError({
      status: 502,
      code: "invalid_response",
      message:
        "Control Plane response did not match the versioned JSON contract",
      retryable: false,
      correlationId: null,
    });
  }
}

function parseApiError(status: number, body: Uint8Array): SreApiError {
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(body));
  } catch {
    value = null;
  }
  const record =
    typeof value === "object" && value !== null
      ? (value as Record<string, unknown>)
      : {};
  const fallback = `request failed with HTTP status ${status}`;
  return new SreApiError({
    status,
    code:
      typeof record.code === "string"
        ? record.code.slice(0, 128)
        : "http_error",
    message: sanitizeServerText(record.message, fallback),
    retryable: record.retryable === true,
    correlationId:
      typeof record.correlation_id === "string"
        ? record.correlation_id.slice(0, 64)
        : null,
  });
}

/**
 * Read-only RocketMQ Rust AI SRE client.
 *
 * This class intentionally has no public generic request method and no
 * approval, execution, raw Admin, or shell method.
 */
export class SreClient {
  readonly #baseUrl: URL;
  readonly #token: TokenProvider | undefined;
  readonly #allowedClusters: ReadonlySet<string> | undefined;
  readonly #timeoutMs: number;
  readonly #maxResponseBytes: number;
  readonly #fetch: FetchLike;

  constructor(options: SreClientOptions) {
    this.#baseUrl = validatedBaseUrl(options.baseUrl);
    this.#token = options.token;
    this.#allowedClusters =
      options.allowedClusters === undefined
        ? undefined
        : new Set(options.allowedClusters);
    this.#timeoutMs = validatePositiveInteger(
      "timeoutMs",
      options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    );
    this.#maxResponseBytes = validatePositiveInteger(
      "maxResponseBytes",
      options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES,
    );
    if (options.fetch !== undefined) {
      this.#fetch = options.fetch;
    } else if (typeof globalThis.fetch === "function") {
      this.#fetch = globalThis.fetch.bind(globalThis);
    } else {
      throw new SreClientConfigurationError(
        "a Fetch API implementation is required",
      );
    }
  }

  async status(): Promise<ServiceStatus> {
    return this.#get<ServiceStatus>("healthz");
  }

  async readiness(): Promise<JsonValue> {
    return this.#get<JsonValue>("readyz");
  }

  async openapi(): Promise<Record<string, JsonValue>> {
    return this.#get<Record<string, JsonValue>>("v1/openapi.json");
  }

  async clusters(): Promise<Cluster[]> {
    const clusters = await this.#get<Cluster[]>("v1/clusters");
    if (this.#allowedClusters === undefined) {
      return clusters;
    }
    return clusters.filter((cluster) =>
      this.#allowedClusters?.has(cluster.id),
    );
  }

  async cluster(clusterId: string): Promise<Cluster> {
    this.#ensureClusterAllowed(requireUuid("clusterId", clusterId));
    const cluster = await this.#get<Cluster>(
      `v1/clusters/${encodeURIComponent(clusterId)}`,
    );
    this.#ensureClusterAllowed(cluster.id);
    return cluster;
  }

  async incident(incidentId: string): Promise<IncidentView> {
    requireUuid("incidentId", incidentId);
    const view = await this.#get<IncidentView>(
      `v1/incidents/${encodeURIComponent(incidentId)}`,
    );
    this.#ensureClusterAllowed(view.incident.cluster_id);
    return view;
  }

  async inspection(inspectionId: string): Promise<InspectionView> {
    requireUuid("inspectionId", inspectionId);
    const view = await this.#get<InspectionView>(
      `v1/inspections/${encodeURIComponent(inspectionId)}`,
    );
    this.#ensureClusterAllowed(view.run.cluster_id);
    return view;
  }

  async plan(planId: string): Promise<ActionPlanView> {
    requireUuid("planId", planId);
    const view = await this.#get<ActionPlanView>(
      `v1/plans/${encodeURIComponent(planId)}`,
    );
    this.#ensureClusterAllowed(view.plan.cluster_id);
    return view;
  }

  #ensureClusterAllowed(clusterId: string): void {
    if (
      this.#allowedClusters !== undefined &&
      !this.#allowedClusters.has(clusterId)
    ) {
      throw new SreClientConfigurationError(
        `cluster ${clusterId} is outside the configured client allowlist`,
      );
    }
  }

  async #resolveToken(): Promise<string | undefined> {
    if (this.#token === undefined) {
      return undefined;
    }
    const token =
      typeof this.#token === "function"
        ? await this.#token()
        : this.#token;
    const normalized = token.trim();
    if (!normalized || /[\r\n]/.test(normalized)) {
      throw new SreClientConfigurationError(
        "token provider returned an invalid bearer token",
      );
    }
    return normalized;
  }

  async #get<T>(path: string): Promise<T> {
    const headers = new Headers({
      accept: "application/json",
    });
    const token = await this.#resolveToken();
    if (token !== undefined) {
      headers.set("authorization", `Bearer ${token}`);
    }
    const response = await this.#fetch(new URL(path, this.#baseUrl), {
      method: "GET",
      headers,
      redirect: "error",
      signal: AbortSignal.timeout(this.#timeoutMs),
    });
    const body = await boundedBody(response, this.#maxResponseBytes);
    if (!response.ok) {
      throw parseApiError(response.status, body);
    }
    return decodeJson<T>(body);
  }
}
