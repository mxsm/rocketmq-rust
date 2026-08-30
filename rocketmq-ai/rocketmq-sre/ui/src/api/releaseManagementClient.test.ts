import { afterEach, describe, expect, it, vi } from "vitest";

import { createReleaseManagementApi } from "./releaseManagementClient";
import type {
  CompleteRollbackRequest,
  CreateReleaseRequest,
  ExternalApprovalInput,
  PrepareReleaseRequest,
  RecordReleaseObservationRequest,
  RegisterIntegrationTargetRequest,
  ReleaseExecutionRequest,
  ReleaseTransitionRequest,
} from "./types";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("createReleaseManagementApi", () => {
  it("uses the complete scoped integration and release escort surface", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () =>
        new Response(JSON.stringify({}), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        }),
      );
    const api = createReleaseManagementApi({
      token: "phase3-token",
      tenantId: "tenant-1",
      clusterIds: ["cluster/one"],
      subject: "operator-a",
      roles: ["operator", "approver"],
    });
    const target = {
      cluster_id: "cluster/one",
      descriptor_id: "mock-itsm",
    } as RegisterIntegrationTargetRequest;
    const externalApproval = {
      target_id: "target/one",
      external_event_id: "evt-100",
    } as ExternalApprovalInput;
    const release = {
      cluster_id: "cluster/one",
      change_id: "CHG-100",
    } as CreateReleaseRequest;
    const preparation = {
      pdb_ready: true,
      synthetic_probe_ready: true,
      evidence_ids: ["evidence-1"],
    } as PrepareReleaseRequest;
    const execution = {
      precondition_hash: "sha256:precondition",
      idempotency_key: "release-100-start",
    } as ReleaseExecutionRequest;
    const observation = {
      phase: "during",
      slo_healthy: true,
      synthetic_probe_healthy: true,
      sanitized_summary: "canary remains healthy",
    } as RecordReleaseObservationRequest;
    const transition = {
      reason: "operator reviewed the live release evidence",
    } as ReleaseTransitionRequest;
    const rollback = {
      succeeded: true,
      reason: "rollback verified",
      observation: {
        ...observation,
        phase: "after",
      },
    } as CompleteRollbackRequest;

    await api.listIntegrationDescriptors();
    await api.listIntegrationTargets(
      "cluster/one",
      "mock_itsm",
      true,
      25,
    );
    await api.registerIntegrationTarget(target);
    await api.getIntegrationTarget("target/one");
    await api.setIntegrationTargetState("target/one", {
      enabled: false,
    });
    await api.listIntegrationDeliveries(
      "cluster/one",
      "target/one",
      50,
    );
    await api.applyExternalApproval(externalApproval);
    await api.listReleases("cluster/one", "canary_running", 30);
    await api.createRelease(release);
    await api.getRelease("release/one");
    await api.prepareRelease("release/one", preparation);
    await api.startRelease("release/one", execution);
    await api.recordReleaseObservation("release/one", observation);
    await api.pauseRelease("release/one", transition);
    await api.resumeRelease("release/one", transition);
    await api.beginReleaseVerification("release/one");
    await api.completeRelease("release/one");
    await api.startReleaseRollback("release/one", execution);
    await api.completeReleaseRollback("release/one", rollback);
    await api.enterManualTakeover("release/one", transition);

    expect(fetchMock.mock.calls.map(([input]) => String(input))).toEqual([
      "/v1/integrations/descriptors",
      "/v1/integrations/targets?cluster_id=cluster%2Fone&adapter_kind=mock_itsm&enabled=true&limit=25",
      "/v1/integrations/targets",
      "/v1/integrations/targets/target%2Fone",
      "/v1/integrations/targets/target%2Fone/state",
      "/v1/integrations/deliveries?cluster_id=cluster%2Fone&target_id=target%2Fone&limit=50",
      "/v1/integrations/approvals/external",
      "/v1/releases?cluster_id=cluster%2Fone&status=canary_running&limit=30",
      "/v1/releases",
      "/v1/releases/release%2Fone",
      "/v1/releases/release%2Fone/prepare",
      "/v1/releases/release%2Fone/start",
      "/v1/releases/release%2Fone/observations",
      "/v1/releases/release%2Fone/pause",
      "/v1/releases/release%2Fone/resume",
      "/v1/releases/release%2Fone/verification/start",
      "/v1/releases/release%2Fone/complete",
      "/v1/releases/release%2Fone/rollback/start",
      "/v1/releases/release%2Fone/rollback/complete",
      "/v1/releases/release%2Fone/manual-takeover",
    ]);

    const headers = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(headers.get("Authorization")).toBe("Bearer phase3-token");
    expect(headers.get("X-RocketMQ-Tenant")).toBe("tenant-1");
    expect(headers.get("X-RocketMQ-Clusters")).toBe("cluster/one");
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe("GET");
    expect(fetchMock.mock.calls[2]?.[1]?.method).toBe("POST");
    expect(JSON.parse(String(fetchMock.mock.calls[6]?.[1]?.body))).toEqual(
      externalApproval,
    );
    expect(JSON.parse(String(fetchMock.mock.calls[12]?.[1]?.body))).toEqual(
      observation,
    );
    expect(fetchMock.mock.calls[15]?.[1]?.body).toBeUndefined();
    expect(fetchMock.mock.calls[16]?.[1]?.body).toBeUndefined();
  });
});
