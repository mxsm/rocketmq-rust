import { Link } from "react-router-dom";

import { DataSurface } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { useCapabilityMessages } from "@/i18n/capabilityMessages";
import { useCapabilityQuality } from "./useCapabilityQuality";

export function CapabilityQualitySummary() {
  const m = useCapabilityMessages();
  const resource = useCapabilityQuality();
  const projection = resource.projection;
  const observed = projection?.packs.filter(
    (pack) => pack.layers.target_observed.status === "present",
  ).length;
  const verification = (layer: "test_verified" | "production_verified") => {
    const packs = projection?.packs ?? [];
    const unknown = packs.filter(
      (pack) => pack.layers[layer].status === "unknown",
    ).length;
    const present = packs.filter(
      (pack) => pack.layers[layer].status === "present",
    ).length;
    return unknown === packs.length
      ? m.unknown
      : `${m.present} ${present} / ${packs.length} · ${m.unknown} ${unknown}`;
  };
  return (
    <DataSurface
      className="capability-quality-summary"
      title={m.summary}
      description={m.description}
      meta={
        <Button asChild size="sm" variant="ghost">
          <Link to="/coverage">{m.openMatrix} →</Link>
        </Button>
      }
    >
      {projection ? (
        <div className="duty-queue-summary">
          <span>
            {m.target_observed}{" "}
            <strong>
              {observed} / {projection.packs.length}
            </strong>
          </span>
          <span>
            {m.test_verified}: {verification("test_verified")}
          </span>
          <span>
            {m.production_verified}: {verification("production_verified")}
          </span>
        </div>
      ) : (
        <p
          className="workspace-empty"
          role={resource.loading ? "status" : undefined}
        >
          {resource.loading
            ? m.loading
            : resource.error
              ? m.unavailable
              : m.noProjectionHint}
        </p>
      )}
    </DataSurface>
  );
}
