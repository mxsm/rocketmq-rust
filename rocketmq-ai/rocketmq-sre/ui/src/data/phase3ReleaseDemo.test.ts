import { describe, expect, it } from "vitest";

import { ApiError } from "@/api/client";
import type { ApiRequestContext } from "@/auth/AuthContext";

import { createMockReleaseManagementApi } from "./phase3ReleaseDemo";

const CLUSTER_ID = "10000000-0000-4000-8000-000000000001";
const RELEASE_ID = "45000000-0000-4000-8000-000000000001";
const auth: ApiRequestContext = {
  token: "demo",
  tenantId: "00000000-0000-4000-8000-000000000001",
  clusterIds: [CLUSTER_ID],
  subject: "release-operator",
  roles: ["operator", "approver"],
};

describe("createMockReleaseManagementApi", () => {
  it("supports the release and integration workspace without bypassing scope", async () => {
    const api = createMockReleaseManagementApi(auth);

    const descriptors = await api.listIntegrationDescriptors();
    const targets = await api.listIntegrationTargets(CLUSTER_ID);
    const deliveries = await api.listIntegrationDeliveries(CLUSTER_ID);
    const releases = await api.listReleases(CLUSTER_ID);
    const paused = await api.recordReleaseObservation(RELEASE_ID, {
      phase: "during",
      slo_healthy: false,
      synthetic_probe_healthy: true,
      evidence_ids: ["4a000000-0000-4000-8000-000000000003"],
      sanitized_summary: "P99 exceeded the approved release budget.",
    });

    expect(descriptors).toHaveLength(5);
    expect(targets.items).toHaveLength(2);
    expect(deliveries.items.map((item) => item.status)).toEqual([
      "delivered",
      "retry_scheduled",
    ]);
    expect(releases.items).toHaveLength(2);
    expect(paused.workflow).toMatchObject({
      status: "paused",
      regression_detected: true,
      pause_reason: "P99 exceeded the approved release budget.",
    });

    const disabled = await api.setIntegrationTargetState(
      targets.items[0].id,
      { enabled: false },
    );
    expect(disabled.enabled).toBe(false);

    const failure = await api
      .listReleases("10000000-0000-4000-8000-000000000099")
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ApiError);
    expect(failure).toMatchObject({
      status: 403,
      code: "cluster_not_allowed",
    });
  });
});
