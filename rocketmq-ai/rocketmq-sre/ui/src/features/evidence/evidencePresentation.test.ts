import { phase1Evidence } from "@/data/phase1Demo";
import {
  evidenceAvailability,
  evidenceFreshness,
  metadataInScope,
  safeEvidenceJson,
  toEvidenceMetadata,
} from "./evidencePresentation";

const evidence = phase1Evidence.items[0];
const scope = {
  clusterId: evidence.cluster_id,
  resourceKind: "consumer" as const,
  resourceKey: "worker",
  window: { start: "2026-07-27T08:00:00Z", end: "2026-07-27T09:00:00Z" },
};

describe("evidence quality and display boundaries", () => {
  it("accepts related resources only through an explicit saved revision reference", () => {
    const bound = {
      kind: "revision_evidence" as const,
      revisionId: "saved-revision",
      evidenceIds: [evidence.evidence_id],
    };
    expect(
      metadataInScope(
        evidence,
        evidence.evidence_id,
        scope,
        evidence.tenant_id,
        bound,
      ),
    ).toBe(true);
    expect(
      metadataInScope(
        evidence,
        evidence.evidence_id,
        scope,
        evidence.tenant_id,
        { ...bound, evidenceIds: [] },
      ),
    ).toBe(false);
    expect(
      metadataInScope(
        { ...evidence, cluster_id: "foreign" },
        evidence.evidence_id,
        scope,
        evidence.tenant_id,
        bound,
      ),
    ).toBe(false);
    expect(
      metadataInScope(
        evidence,
        evidence.evidence_id,
        scope,
        evidence.tenant_id,
        { kind: "selected_resource" },
      ),
    ).toBe(false);
  });

  it("does not treat freshness_seconds as an expiry policy", () => {
    expect(evidenceFreshness(evidence)).toBe("unknown");
    const policy = {
      maxAgeSeconds: 60,
      asOf: "2026-07-27T08:42:00Z",
      sourceRef: "registry/consumer-lag.v2",
    };
    expect(evidenceFreshness(evidence, policy)).toBe("current");
    expect(
      evidenceFreshness(evidence, { ...policy, asOf: "2026-07-27T08:45:00Z" }),
    ).toBe("stale");
    expect(evidenceFreshness(evidence, { ...policy, asOf: "invalid" })).toBe(
      "unknown",
    );
    expect(evidenceAvailability({ ...evidence, coverage: "missing" })).toBe(
      "missing",
    );
    expect(evidenceAvailability({ ...evidence, partial: true })).toBe(
      "partial",
    );
  });

  it("adds source-reported age to elapsed storage time when showing live run evidence freshness", () => {
    const sample = { ...evidence, observed_at: "2026-11-14T10:00:00Z", freshness_seconds: 50 };
    expect(evidenceFreshness(sample, { maxAgeSeconds: 60, asOf: "2026-11-14T10:00:50Z", sourceRef: "saved-run/broker-health.v1" })).toBe("stale");
    expect(evidenceFreshness(sample, { maxAgeSeconds: 60, asOf: "2026-11-14T10:00:05Z", sourceRef: "saved-run/broker-health.v1" })).toBe("current");
  });

  it("removes inline content from metadata and bounds camel-case secrets, bodies and configuration", () => {
    expect(toEvidenceMetadata(evidence)).not.toHaveProperty("content");
    const content = safeEvidenceJson({
      lag: 0,
      messageBody: "hidden-body",
      accessToken: "hidden-token",
      tlsConfig: { cert: "hidden-tls" },
      configuration: { any: "hidden-config" },
      nested: { password: "hidden-password" },
      auth: "Bearer hidden-bearer",
      many: Array.from({ length: 1000 }, () => "x".repeat(3000)),
    });
    expect(content).toContain('"lag": 0');
    expect(content).not.toMatch(
      /hidden-(body|token|tls|config|password|bearer)/,
    );
    expect(content.length).toBeLessThan(8300);
  });

  it("redacts TLS stores and certificate material regardless of the containing key", () => {
    const content = safeEvidenceJson({
      certificate_pem: "hidden-certificate",
      serverCertificate: "hidden-server-cert",
      keystore: "hidden-keystore",
      truststore: "hidden-truststore",
      arbitrary:
        "-----BEGIN CERTIFICATE-----\nhidden-base64\n-----END CERTIFICATE-----",
      configValues: { diagnostic: "hidden-full-config" },
      safe: { reachable: true },
    });
    expect(content).not.toMatch(
      /hidden-(certificate|server-cert|keystore|truststore|base64|full-config)/,
    );
    expect(content).toContain('"reachable": true');
  });
});
