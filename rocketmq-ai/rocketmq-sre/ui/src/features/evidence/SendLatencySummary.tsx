import { DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { useSendLatencyMessages } from "@/i18n/sendLatencyMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import type { LatencyDisplay } from "./sendLatencyPresentation";

export function SendLatencySummary({ display }: { display: LatencyDisplay }) {
  const m = useSendLatencyMessages();
  const { formatDateTime, locale } = useOperatorPreferences();
  const number = (value: number) =>
    new Intl.NumberFormat(locale, { maximumFractionDigits: 3 }).format(value);
  const span = (range?: { start: string; end: string }) =>
    range
      ? `${formatDateTime(range.start)} → ${formatDateTime(range.end)}`
      : m.unknown;
  if (display.kind === "invalid") {
    return <p role="alert">{display.reason === "scope" ? m.invalidScope : m.invalid}</p>;
  }
  return (
    <section aria-labelledby="send-latency-title" className="send-latency-summary">
      <h4 id="send-latency-title">{m.title}</h4>
      <p>{m.saved}</p>
      <DefinitionGrid
        items={[
          { label: m.topic, value: display.topic },
          { label: m.data, value: span(display.data) },
          { label: m.collected, value: formatDateTime(display.collectedAt) },
          {
            label: m.age,
            value: display.age === undefined ? m.unknown : `${number(display.age)}s`,
          },
        ]}
      />
      <ul className="latency-segments-list" aria-label={m.title}>
        {display.segments.map((segment) => (
          <li key={segment.id}>
            <div className="latency-segment-heading">
              <strong>{m[segment.id]}</strong>
              <Badge
                variant={segment.availability === "available" ? "outline" : "warning"}
              >
                {segment.availability === "invalid"
                  ? m.invalidState
                  : m[segment.availability]}
              </Badge>
            </div>
            <p>{m[`${segment.id}Boundary`]}</p>
            <p>{m.reason(segment.reason)}</p>
            {segment.observation && (
              <div className="latency-observation">
                <strong>
                  {m.estimate}: {number(segment.observation.estimate)} ms
                </strong>
                <span>
                  {m.bucket}: {number(segment.observation.lower)}–
                  {segment.observation.upper === null
                    ? "+∞"
                    : number(segment.observation.upper)} ms
                </span>
                {segment.observation.tailLimited && (
                  <p className="latency-tail-note">{m.tail}</p>
                )}
                <small>
                  {m.counts}: {number(segment.observation.observedIncrements)} /{" "}
                  {number(segment.observation.estimatedIncrements)}
                </small>
                <small>
                  {m.nodes}: {number(segment.observation.nodes)} /{" "}
                  {number(segment.observation.scrapes)} /{" "}
                  {number(segment.observation.resets)}
                </small>
              </div>
            )}
          </li>
        ))}
      </ul>
      <div className="latency-policy">
        <strong>{m.policy}: {m[display.policy.status]}</strong>
        {display.policy.threshold !== undefined && (
          <p>{m.threshold}: {number(display.policy.threshold)} ms</p>
        )}
        <p>{m.reason(display.policy.reason)}</p>
        <p>{m.policyHint}</p>
      </div>
      <details className="investigation-audit-details">
        <summary>{m.method}</summary>
        <p>{m.estimator}</p>
        <DefinitionGrid
          items={[
            { label: m.requested, value: span(display.requested) },
            { label: m.query, value: span(display.query) },
            { label: "Method", value: "classic_linear_p99_strict.v1", mono: true },
            { label: "Grid", value: "otel_default_explicit_ms.v1", mono: true },
          ]}
        />
      </details>
    </section>
  );
}
