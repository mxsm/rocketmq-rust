import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import type { CapabilityFact, CapabilityQualityProjection } from "@/api/types";
import {
  createEvidenceQualityDemo,
  demoCapabilityQuality,
} from "@/data/evidenceQualityDemo";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { CapabilityQualityMatrix } from "./CapabilityQualityMatrix";

function fixture() {
  const demo = createEvidenceQualityDemo(new Date("2026-11-11T10:00:00Z"));
  const projection = demoCapabilityQuality(
    DEMO_CLUSTER_ID,
    DEMO_TENANT_ID,
    demo.evidence,
    "2026-11-11T10:00:00Z",
  );
  projection.investigation_id = "synthetic-investigation";
  projection.revision_id = "synthetic-revision";
  projection.packs[0].pack_version = "synthetic-version";
  const record: NonNullable<CapabilityFact["verification"]> = {
    record_id: "synthetic-record",
    verified_by: "synthetic-verifier",
    recorded_at: "2026-11-11T09:55:00Z",
    assessment: {
      schema_version: "rocketmq-sre.capability-verification.v1",
      tenant_id: DEMO_TENANT_ID,
      cluster_id: DEMO_CLUSTER_ID,
      pack_id: projection.packs[0].pack_id,
      pack_version: "synthetic-version",
      level: "test",
      method: "deterministic_fixture",
      outcome: "passed",
      resource: "consumer-lag/synthetic-target",
      investigation_id: "synthetic-investigation",
      revision_id: "synthetic-revision",
      time_range: {
        start: "2026-11-11T09:00:00Z",
        end: "2026-11-11T09:50:00Z",
      },
      observed_at: "2026-11-11T09:50:00Z",
      valid_until: "2026-11-11T10:30:00Z",
      source_ref: "synthetic-controlled-report",
      report_digest: "sha256:synthetic-digest",
      evidence_ids: ["synthetic-saved-evidence"],
    },
  };
  projection.packs[0].layers.test_verified = {
    status: "present",
    reason_code: "synthetic_test_record_passed",
    source_ref: record.assessment.source_ref,
    observed_at: record.assessment.observed_at,
    verification: record,
  };
  return { projection, record };
}

function renderMatrix(projection: CapabilityQualityProjection) {
  return render(
    <OperatorPreferencesProvider>
      <MemoryRouter>
        <CapabilityQualityMatrix projection={projection} />
      </MemoryRouter>
    </OperatorPreferencesProvider>,
  );
}

async function selectVerification(status = "Established") {
  await userEvent.setup().click(screen.getByRole("button", {
    name: `consumer-lag.v2 · Test verification · ${status}`,
  }));
}

describe("controlled verification provenance", () => {
  beforeEach(() => {
    localStorage.setItem(
      "rocketmq-sre-operator-preferences.v1",
      JSON.stringify({ locale: "en-US", timeZone: "UTC", theme: "dark" }),
    );
  });
  afterEach(() => localStorage.clear());

  it("shows the exact record scope and synthetic method without granting production or execution authority", async () => {
    const { projection } = fixture();
    renderMatrix(projection);
    await selectVerification();
    expect(screen.getByText("Deterministic synthetic fixture")).toBeVisible();
    expect(screen.getByText("This resource only: consumer-lag/synthetic-target")).toBeVisible();
    expect(screen.getByText("synthetic-verifier")).toBeVisible();
    const details = screen.getByRole("region", { name: "Controlled verification record" });
    expect(within(details).getByText("synthetic-controlled-report")).toBeVisible();
    expect(screen.getByRole("button", {
      name: "consumer-lag.v2 · Production verification · Unknown",
    })).toBeVisible();
    expect(screen.queryByRole("button", {
      name: /grant|upload|approve|execute/i,
    })).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByText("Verification audit details"));
    expect(screen.getByText("synthetic-investigation / synthetic-revision")).toBeVisible();
    expect(screen.getByText("sha256:synthetic-digest")).toBeVisible();
    expect(screen.getByText("synthetic-saved-evidence")).toBeVisible();
  });

  it.each(["tenant", "cluster", "pack", "version", "revision", "level", "resource", "canonical_prefix"])(
    "hides nested record metadata when its %s differs from the projection",
    async (field) => {
      const { projection, record } = fixture();
      if (field === "tenant") record.assessment.tenant_id = "foreign-tenant";
      if (field === "cluster") record.assessment.cluster_id = "foreign-cluster";
      if (field === "pack") record.assessment.pack_id = "foreign-pack";
      if (field === "version") record.assessment.pack_version = "foreign-version";
      if (field === "revision") record.assessment.revision_id = "foreign-revision";
      if (field === "level") record.assessment.level = "production";
      if (field === "resource") projection.resource = "store-pressure/other";
      if (field === "canonical_prefix") record.assessment.resource = "store-pressure/foreign";
      record.verified_by = "foreign-verifier-marker";
      projection.packs[0].layers.test_verified.source_ref = "foreign-fact-source-marker";
      projection.packs[0].layers.test_verified.observed_at = "1980-01-01T12:34:56Z";
      renderMatrix(projection);
      await selectVerification("Unknown");
      expect(screen.getByRole("alert")).toHaveTextContent("Verification record differs");
      expect(screen.queryByText("foreign-verifier-marker")).not.toBeInTheDocument();
      expect(screen.queryByText("foreign-fact-source-marker")).not.toBeInTheDocument();
      expect(screen.queryByText(/1980/)).not.toBeInTheDocument();
      expect(screen.queryByRole("button", {
        name: "consumer-lag.v2 · Test verification · Established",
      })).not.toBeInTheDocument();
      expect(screen.queryByText("This resource only: consumer-lag/synthetic-target")).not.toBeInTheDocument();
    },
  );

  it("does not treat a resource-only record as cluster overview verification", async () => {
    const { projection, record } = fixture();
    projection.investigation_id = null;
    projection.revision_id = null;
    record.assessment.investigation_id = null;
    record.assessment.revision_id = null;
    renderMatrix(projection);
    await selectVerification("Unknown");
    expect(screen.getByRole("alert")).toHaveTextContent("Verification record differs");
    expect(screen.queryByText("synthetic-verifier")).not.toBeInTheDocument();
  });

  it("allows a bound revision to show a Store record within that pack's own canonical resource", async () => {
    const { projection, record } = fixture();
    const store = projection.packs.find((pack) => pack.pack_id === "store-pressure.v1")!;
    store.pack_version = "synthetic-version";
    record.assessment.pack_id = store.pack_id;
    record.assessment.resource = "store-pressure/synthetic-store";
    store.layers.test_verified = { ...projection.packs[0].layers.test_verified, verification: record };
    renderMatrix(projection);
    await userEvent.setup().click(screen.getByRole("button", {
      name: "store-pressure.v1 · Test verification · Established",
    }));
    expect(screen.getByText("This resource only: store-pressure/synthetic-store")).toBeVisible();
    expect(screen.getByText("synthetic-verifier")).toBeVisible();
  });

  it.each([true, false])("requires current qualified proof IDs for a production fact: present=%s", async (qualified) => {
    const { projection, record } = fixture();
    const pack = projection.packs[0];
    record.assessment.level = "production";
    record.assessment.method = "live_read_only";
    record.assessment.evidence_ids = qualified
      ? pack.requirements.flatMap((item) => item.evidence_ids)
      : ["synthetic-unrelated-evidence"];
    pack.layers.production_verified = { ...pack.layers.test_verified, verification: record };
    renderMatrix(projection);
    await userEvent.setup().click(screen.getByRole("button", {
      name: `consumer-lag.v2 · Production verification · ${qualified ? "Established" : "Unknown"}`,
    }));
    if (qualified) {
      expect(screen.getByText("Live target read-only collection")).toBeVisible();
      expect(screen.getByText("synthetic-verifier")).toBeVisible();
    } else {
      expect(screen.getByRole("alert")).toHaveTextContent("Verification record differs");
      expect(screen.queryByRole("button", {
        name: "consumer-lag.v2 · Production verification · Established",
      })).not.toBeInTheDocument();
    }
  });

  it.each(["record", "version"])("keeps a claimed verification unknown when %s proof is missing", async (missing) => {
    const { projection } = fixture();
    const pack = projection.packs[0];
    if (missing === "record") pack.layers.test_verified.verification = undefined;
    else pack.pack_version = "";
    pack.layers.test_verified.source_ref = "unproven-source-marker";
    renderMatrix(projection);
    await selectVerification("Unknown");
    expect(screen.queryByRole("button", {
      name: "consumer-lag.v2 · Test verification · Established",
    })).not.toBeInTheDocument();
    expect(screen.queryByText("unproven-source-marker")).not.toBeInTheDocument();
    expect(screen.queryByText("synthetic-verifier")).not.toBeInTheDocument();
  });

  it.each(["expired", "revoked"] as const)(
    "keeps %s record provenance distinct from an established fact",
    async (state) => {
      const { projection, record } = fixture();
      const fact = projection.packs[0].layers.test_verified;
      fact.status = "unknown";
      fact.reason_code = `verification_${state}`;
      if (state === "revoked") record.assessment.outcome = "revoked";
      else record.assessment.valid_until = "2026-11-11T09:59:00Z";
      renderMatrix(projection);
      await userEvent.setup().click(screen.getByRole("button", {
        name: "consumer-lag.v2 · Test verification · Unknown",
      }));
      expect(screen.getByText(`verification_${state}`)).toBeVisible();
      expect(screen.getByText(state === "revoked" ? "Revoked" : "Passed")).toBeVisible();
      expect(screen.getByText("synthetic-verifier")).toBeVisible();
      expect(screen.queryByRole("button", {
        name: "consumer-lag.v2 · Test verification · Established",
      })).not.toBeInTheDocument();
    },
  );
});
