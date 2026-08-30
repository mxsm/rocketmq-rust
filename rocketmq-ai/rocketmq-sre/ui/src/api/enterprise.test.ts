import { describe, expect, it, vi } from "vitest";

import {
  enterpriseDemoSnapshot,
  loadEnterpriseSnapshot,
  loadGovernanceVersions,
} from "./enterprise";

describe("enterprise operation read model", () => {
  it("renders a bounded 100-cluster Fleet without issuing network requests", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    const snapshot = await loadEnterpriseSnapshot(
      undefined,
      {},
      true,
    );

    expect(snapshot.fleet.registrations).toHaveLength(100);
    expect(snapshot.assets.items).toHaveLength(400);
    expect(snapshot.inspections.items[0]?.max_concurrency).toBe(8);
    expect(snapshot.inspections.items[0]?.cluster_ids).toHaveLength(100);
    expect(fetch).not.toHaveBeenCalled();
    fetch.mockRestore();
  });

  it("keeps DR exercises outside production cutover", () => {
    const snapshot = enterpriseDemoSnapshot();
    const modes = snapshot.drPlans.items.flatMap(
      (plan) => plan.allowed_modes,
    );

    expect(modes).toEqual(
      expect.arrayContaining([
        "readiness",
        "tabletop",
        "supervised_test",
      ]),
    );
    expect(modes).not.toContain("production_cutover");
    expect(
      snapshot.drExercises.items.every(
        (exercise) => exercise.boundary === "test_resources_only",
      ),
    ).toBe(true);
  });

  it("exposes governed versions with explicit signature state", async () => {
    const snapshot = enterpriseDemoSnapshot();
    const artifact = snapshot.governanceArtifacts.items[0];
    expect(artifact).toBeDefined();

    const versions = await loadGovernanceVersions(
      artifact!.id,
      undefined,
      true,
    );

    expect(versions.items).toHaveLength(1);
    expect(versions.items[0]?.artifact_id).toBe(artifact!.id);
    expect(versions.items[0]?.signature?.algorithm).toBe("ed25519");
  });

  it("reports missing cost coverage instead of fabricating zero", () => {
    const report = enterpriseDemoSnapshot().finops;

    expect(report.entries_missing_cost).toBeGreaterThan(0);
    expect(report.cost_coverage_basis_points).toBeLessThan(10_000);
    expect(report.warnings).toContain(
      "SLO outcome attribution is unavailable for 31 ledger entries.",
    );
    expect(report.chargeback_enabled).toBe(false);
  });
});
