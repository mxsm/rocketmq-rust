import {
  createEvidenceQualityDemo,
  demoCapabilityQuality,
} from "@/data/evidenceQualityDemo";
import { DEMO_CLUSTER_ID, DEMO_TENANT_ID } from "@/data/phase1Demo";
import { toEvidenceMetadata } from "@/features/evidence/evidencePresentation";
import { scenarioEvidence } from "./scenarioEvidence";

const now = new Date("2026-11-11T10:00:00Z");
const scope = {
  clusterId: DEMO_CLUSTER_ID,
  window: { start: "2026-11-11T09:00:00Z", end: "2026-11-11T10:01:00Z" },
};

function fixture(index: number) {
  const demo = createEvidenceQualityDemo(now);
  const projection = demoCapabilityQuality(
    DEMO_CLUSTER_ID,
    DEMO_TENANT_ID,
    demo.evidence,
    now.toISOString(),
  );
  const revision = demo.investigations[index].diagnosis_revisions[0];
  const scenario = projection.scenarios[index];
  const metadata = Object.fromEntries(
    demo.evidence.map((item) => [item.evidence_id, toEvidenceMetadata(item)]),
  );
  return {
    projection,
    revision,
    scenario,
    metadata,
    evaluation: { metadata, scope, tenantId: DEMO_TENANT_ID },
  };
}

describe("saved revision scenario chains", () => {
  it("keeps complete backlog/Store samples, latency partial/stale, and routing missing/context distinct", () => {
    const backlog = fixture(0);
    expect(
      scenarioEvidence(
        backlog.projection,
        backlog.scenario,
        backlog.revision,
        backlog.evaluation,
      ).required.map((slot) => slot.quality),
    ).toEqual(["available", "available"]);
    const latency = fixture(1);
    expect(
      scenarioEvidence(
        latency.projection,
        latency.scenario,
        latency.revision,
        latency.evaluation,
      ).required.map((slot) => slot.quality),
    ).toEqual(["partial", "stale"]);
    const routing = fixture(2);
    const chain = scenarioEvidence(
      routing.projection,
      routing.scenario,
      routing.revision,
      routing.evaluation,
    );
    expect(chain.required.map((slot) => slot.quality)).toEqual([
      "available",
      "missing",
    ]);
    expect(chain.contextIds).toEqual([routing.revision.evidence_ids[1]]);
    expect(
      chain.required[0].relation(routing.revision.evidence_ids[0]),
    ).toEqual({ support: false, counter: false });
  });

  it.each([0, 1, 2])(
    "never counts unknown, foreign or out-of-window metadata as complete in scenario %s",
    (index) => {
      const item = fixture(index);
      const empty = scenarioEvidence(
        item.projection,
        item.scenario,
        item.revision,
        { ...item.evaluation, metadata: {} },
      );
      expect(empty.required.every((slot) => slot.quality !== "available")).toBe(
        true,
      );
      const id = item.revision.evidence_ids[0];
      const foreign = {
        ...item.metadata,
        [id]: { ...item.metadata[id], cluster_id: "foreign-cluster" },
      };
      const foreignSlot = scenarioEvidence(
        item.projection,
        item.scenario,
        item.revision,
        { ...item.evaluation, metadata: foreign },
      ).required[0];
      expect(foreignSlot.quality).toBe("unknown");
      expect(foreignSlot.reason).toBe("snapshot_scope_mismatch");
      const historicalWindow = {
        ...scope,
        window: { start: "2026-11-11T08:00:00Z", end: "2026-11-11T09:00:00Z" },
      };
      const outsideSlot = scenarioEvidence(
        item.projection,
        item.scenario,
        item.revision,
        { ...item.evaluation, scope: historicalWindow },
      ).required[0];
      expect(outsideSlot.quality).toBe("outside");
      expect(outsideSlot.reason).toBe("outside_selected_window");
      const wrongSource = {
        ...item.metadata,
        [id]: { ...item.metadata[id], source: "unrelated-source" },
      };
      expect(
        scenarioEvidence(item.projection, item.scenario, item.revision, {
          ...item.evaluation,
          metadata: wrongSource,
        }).required[0].quality,
      ).toBe("unknown");
    },
  );

  it("does not borrow a newer cluster sample or invent supporting relationships", () => {
    const item = fixture(0);
    const id = item.revision.evidence_ids[0];
    const old = { ...item.revision, evidence_ids: ["old-reference"] };
    expect(
      scenarioEvidence(
        item.projection,
        item.scenario,
        old,
        item.evaluation,
      ).required.every((slot) => slot.quality === "missing"),
    ).toBe(true);
    item.revision.hypotheses = [
      {
        root_cause: "Stored assessment",
        supporting_evidence: [
          { evidence_id: id },
          { evidence_id: "unreferenced" },
        ],
        counter_evidence: [{ evidence_id: item.revision.evidence_ids[1] }],
      },
    ];
    const chain = scenarioEvidence(
      item.projection,
      item.scenario,
      item.revision,
      item.evaluation,
    );
    expect(chain.required[0].relation(id)).toEqual({
      support: true,
      counter: false,
    });
    expect(chain.required[1].relation(item.revision.evidence_ids[1])).toEqual({
      support: false,
      counter: true,
    });
    expect(chain.required[0].relation("unreferenced")).toEqual({
      support: false,
      counter: false,
    });
  });
});
