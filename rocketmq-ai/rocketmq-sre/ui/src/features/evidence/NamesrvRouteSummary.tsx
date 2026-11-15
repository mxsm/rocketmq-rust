import { useId } from "react";

import { DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { useNamesrvRouteMessages } from "@/i18n/namesrvRouteMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import type { NamesrvRouteDisplay, RouteSpan } from "./namesrvRoutePresentation";

export function NamesrvRouteSummary({ display }: { display: NamesrvRouteDisplay }) {
  const m = useNamesrvRouteMessages();
  const { formatDateTime, locale } = useOperatorPreferences();
  const titleId = useId();
  const number = (value: number) => new Intl.NumberFormat(locale).format(value);
  const span = (range?: RouteSpan) => range
    ? `${formatDateTime(range.start)} → ${formatDateTime(range.end)}`
    : m.unknown;
  if (display.kind === "invalid") {
    return <p role="alert">{display.reason === "scope" ? m.invalidScope : m.invalid}</p>;
  }
  const dimensions = (values: string[]) => values.length
    ? values.map(m.dimension).join(" · ")
    : m.unknown;
  return (
    <section aria-labelledby={titleId} className="namesrv-route-summary">
      <h4 id={titleId}>{m.title}</h4>
      <p>{m.saved}</p>
      <div className="namesrv-route-state">
        <Badge className="namesrv-summary-badge" variant="warning">{m.partial}</Badge>
        <span>{m.savedComparison}</span>
        <strong>{m[display.state]}</strong>
        <p>{m.reason(display.reason)}</p>
        <strong>{m.historical}</strong>
      </div>
      <DefinitionGrid
        items={[
          { label: m.topic, value: display.topic },
          { label: m.collection, value: span(display.collection) },
          { label: m.collected, value: formatDateTime(display.collection.end) },
          { label: m.age, value: `${number(display.age)}s` },
        ]}
      />
      <ul aria-label={m.dimensions} className="namesrv-dimensions">
        <li><strong>{m.compared}: </strong>{dimensions(display.compared)}</li>
        <li>
          <strong>{m.different}: </strong>
          {display.different.length ? dimensions(display.different) : m.noDifference}
        </li>
        {!!display.unknown.length && <li><strong>{m.unavailable}: </strong>{dimensions(display.unknown)}</li>}
      </ul>
      <p>{m.authority}</p>
      <p>{m.independence}</p>
      <ul aria-label={m.peers} className="namesrv-peer-list">
        {display.peers.map((peer) => (
          <li key={peer.id}>
            <h5>{m.peer} · {peer.id}</h5>
            <div className="namesrv-rounds">
              {peer.rounds.map((round) => (
                <section
                  aria-label={m.round.replace("{round}", String(round.round))}
                  className="namesrv-round-card"
                  key={round.round}
                >
                  <strong>{m.round.replace("{round}", String(round.round))}</strong>
                  <dl>
                    <dt>{m.config}</dt>
                    <dd>{m.status(round.configuration.status)}</dd>
                    <dt>{m.route}</dt>
                    <dd>{m.status(round.route.status)}</dd>
                    <dt>{m.readSpan}</dt>
                    <dd>{span(round.route.span)}</dd>
                  </dl>
                  {round.configuration.reason !== "view_configuration_observed" && (
                    <p>{m.reason(round.configuration.reason)}</p>
                  )}
                  {round.route.reason !== "route_observed" && <p>{m.reason(round.route.reason)}</p>}
                </section>
              ))}
            </div>
          </li>
        ))}
      </ul>
      <details className="investigation-audit-details">
        <summary>{m.audit}</summary>
        <p>{m.observationOnly}</p>
        <DefinitionGrid
          items={[
            { label: m.requested, value: span(display.requested) },
            {
              label: `${m.physicalReads} · ${m.configReads}`,
              value: `${number(display.statistics.plannedConfig)} / ${number(display.statistics.startedConfig)}`,
            },
            {
              label: `${m.physicalReads} · ${m.routeReads}`,
              value: `${number(display.statistics.plannedRoute)} / ${number(display.statistics.startedRoute)}`,
            },
            {
              label: m.actualBytes,
              value: `${number(display.statistics.receivedBytes)} / ${number(display.statistics.unknownBytes)}`,
            },
            {
              label: m.acceptedBytes,
              value: `${number(display.statistics.acceptedBytes)} / ${display.statistics.rows === null ? m.unknown : number(display.statistics.rows)}`,
            },
          ]}
        />
        {display.statistics.bounded && <p>{m.budgetReached}</p>}
        <strong>{m.missingFacts}</strong>
        <ul>{display.missing.map((fact) => <li key={fact}>{m.missing(fact)}</li>)}</ul>
      </details>
    </section>
  );
}
