import { useState } from "react";
import { Link } from "react-router-dom";

import type {
  CapabilityLayers,
  CapabilityQualityProjection,
} from "@/api/types";
import { DataSurface, DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  useCapabilityMessages,
  type ScenarioId,
} from "@/i18n/capabilityMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";

const LAYERS: Array<keyof CapabilityLayers> = [
  "registered",
  "handler",
  "configured",
  "target_observed",
  "test_verified",
  "production_verified",
];

export function CapabilityQualityMatrix({
  projection,
}: {
  projection: CapabilityQualityProjection;
}) {
  const m = useCapabilityMessages();
  const { formatDateTime } = useOperatorPreferences();
  const [scenarioId, setScenarioId] = useState("all");
  const [selection, setSelection] = useState<{
    packId: string;
    layer: keyof CapabilityLayers;
  }>();
  const scenario = projection.scenarios.find((item) => item.id === scenarioId);
  const packs = projection.packs.filter(
    (pack) => !scenario || scenario.pack_ids.includes(pack.pack_id),
  );
  const selectedPack =
    packs.find((pack) => pack.pack_id === selection?.packId) ?? packs[0];
  const selectedLayer = selection?.layer ?? "target_observed";
  const fact = selectedPack?.layers[selectedLayer];

  return (
    <div className="capability-quality-stack">
      <div className="capability-quality-context">
        <span>
          {m.assessed}: {formatDateTime(projection.observed_at)}
        </span>
        {projection.partial && <Badge variant="warning">{m.partial}</Badge>}
        <span>{m.registryHint}</span>
      </div>
      <label className="capability-scenario-select">
        <span>{m.scenario}</span>
        <select
          aria-label={m.scenario}
          className="native-select"
          onChange={(event) => setScenarioId(event.target.value)}
          value={scenarioId}
        >
          <option value="all">{m.allScenarios}</option>
          {projection.scenarios.map((item) => (
            <option key={item.id} value={item.id}>
              {m[item.id as ScenarioId] ?? item.id}
            </option>
          ))}
        </select>
      </label>
      <DataSurface title={m.title}>
        <div className="table-scroll">
          <table className="phase1-table capability-layer-table">
            <thead>
              <tr>
                <th>{m.pack}</th>
                {LAYERS.map((layer) => (
                  <th key={layer}>{m[layer]}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {packs.map((pack) => (
                <tr key={pack.pack_id}>
                  <th scope="row">{pack.pack_id}</th>
                  {LAYERS.map((layer) => (
                    <td key={layer}>
                      <button
                        aria-label={`${pack.pack_id} · ${m[layer]} · ${m[pack.layers[layer].status]}`}
                        aria-pressed={
                          selectedPack?.pack_id === pack.pack_id &&
                          selectedLayer === layer
                        }
                        className="capability-fact-button"
                        onClick={() =>
                          setSelection({ packId: pack.pack_id, layer })
                        }
                        type="button"
                      >
                        <Badge
                          variant={
                            pack.layers[layer].status === "present"
                              ? "outline"
                              : pack.layers[layer].status === "absent"
                                ? "warning"
                                : "secondary"
                          }
                        >
                          {m[pack.layers[layer].status] ?? m.unknown}
                        </Badge>
                      </button>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DataSurface>
      {fact && selectedPack && (
        <DataSurface
          title={`${m.factDetails} · ${selectedPack.pack_id} · ${m[selectedLayer]}`}
        >
          <DefinitionGrid
            items={[
              { label: m.reason, value: fact.reason_code },
              { label: m.source, value: fact.source_ref },
              {
                label: m.observed,
                value: fact.observed_at
                  ? formatDateTime(fact.observed_at)
                  : m.unknown,
              },
            ]}
          />
        </DataSurface>
      )}
      {selectedPack && (
        <DataSurface
          title={`${m.requirements} · ${selectedPack.pack_id}`}
          description={m.registryHint}
        >
          <div className="table-scroll">
            <table className="phase1-table capability-requirements-table">
              <thead>
                <tr>
                  <th>{m.signal}</th>
                  <th>{m.source}</th>
                  <th>{m.maxAge}</th>
                  <th>{m.fields}</th>
                  <th>{m.reason}</th>
                </tr>
              </thead>
              <tbody>
                {selectedPack.requirements.map((requirement) => (
                  <tr key={requirement.id}>
                    <td>
                      <strong>{requirement.id}</strong>
                      <small>
                        {requirement.required ? m.required : m.optional}
                      </small>
                      <Badge
                        variant={
                          requirement.quality === "available"
                            ? "outline"
                            : "warning"
                        }
                      >
                        {m[requirement.quality] ?? m.unknown}
                      </Badge>
                    </td>
                    <td>
                      {requirement.source}
                      <small>{requirement.resource_prefix}</small>
                      <small>
                        {requirement.observed_at
                          ? formatDateTime(requirement.observed_at)
                          : m.unknown}
                      </small>
                    </td>
                    <td>{requirement.max_age_seconds}s</td>
                    <td>
                      {requirement.fields.length
                        ? requirement.fields
                            .map((field) => `${field.path}: ${field.kind}`)
                            .join(" · ")
                        : m.unknown}
                      {requirement.missing_fields.length > 0 && (
                        <p>
                          {m.missingFields}:{" "}
                          {requirement.missing_fields.join(" · ")}
                        </p>
                      )}
                    </td>
                    <td>{requirement.reason_code}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="investigation-related-links">
            <Button asChild size="sm" variant="outline">
              <Link to="/ask">{m.gapTask}</Link>
            </Button>
          </div>
        </DataSurface>
      )}
    </div>
  );
}
