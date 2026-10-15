import { useEffect, useState, type CSSProperties } from 'react';
import { Badge } from '../../components/ui/Badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../components/ui/Card';
import { StatusDot } from '../../components/ui/StatusDot';
import type { TopologyCluster, TopologySourceStatus } from '../../types/topology';
import { formatTps, SLOW_SOURCE_MS, TOPOLOGY_SOURCE_LABELS, type TopologyChangeTone } from './topology-model';
import type { TopologyChangeEntry } from './useTopologySnapshot';

export function TrafficPanel({ clusters }: { clusters: TopologyCluster[] }) {
  const largest = Math.max(1, ...clusters.flatMap((cluster) => [cluster.produceTps, cluster.consumeTps]));
  const produce = clusters.reduce((total, cluster) => total + cluster.produceTps, 0);
  const consume = clusters.reduce((total, cluster) => total + cluster.consumeTps, 0);
  return (
    <Card className="topology-traffic">
      <CardHeader>
        <CardTitle>Traffic by cluster</CardTitle>
        <CardDescription>Sum of reported broker produce and consume TPS.</CardDescription>
      </CardHeader>
      <CardContent className="topology-panel-body">
        {clusters.length === 0 ? <p className="topology-panel-empty">No cluster reported traffic.</p> : null}
        {clusters.map((cluster) => (
          <div key={cluster.clusterName} className="topology-traffic-cluster">
            <h3>{cluster.clusterName}</h3>
            <TrafficBar label="Produce" kind="produce" value={cluster.produceTps} largest={largest} />
            <TrafficBar label="Consume" kind="consume" value={cluster.consumeTps} largest={largest} />
          </div>
        ))}
        {clusters.length > 0 ? (
          <div className="topology-traffic-total">
            <span>All clusters</span>
            <span className="topology-mono">{`in ${formatTps(produce)} · out ${formatTps(consume)}`}</span>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

function TrafficBar({ label, kind, value, largest }: { label: string; kind: 'produce' | 'consume'; value: number; largest: number }) {
  const share = Math.min(1, Math.max(0, value / largest));
  return (
    <div className="topology-traffic-row">
      <span className="topology-traffic-label">{label}</span>
      <span
        className="topology-traffic-track"
        role="meter"
        aria-label={`${label} TPS`}
        aria-valuemin={0}
        aria-valuemax={largest}
        aria-valuenow={value}
        aria-valuetext={`${formatTps(value)} TPS`}
      >
        <span className="topology-traffic-fill" data-kind={kind} style={{ '--share': share } as CSSProperties} />
      </span>
      <span className="topology-mono">{formatTps(value)}</span>
    </div>
  );
}

const changeDot: Record<TopologyChangeTone, 'success' | 'warning' | 'danger' | 'info'> = {
  success: 'success',
  warning: 'warning',
  danger: 'danger',
  info: 'info'
};

interface ChangesPanelProps {
  changes: TopologyChangeEntry[];
  /** Changes found by the latest refresh; null before there are two snapshots. */
  latestChangeCount: number | null;
}

export function ChangesPanel({ changes, latestChangeCount }: ChangesPanelProps) {
  let announcement = '';
  if (latestChangeCount !== null && latestChangeCount > 0) {
    announcement = `${latestChangeCount} ${latestChangeCount === 1 ? 'change' : 'changes'} since the last refresh.`;
  }
  return (
    <Card className="topology-changes">
      <CardHeader>
        <CardTitle>Changes since last refresh</CardTitle>
        <CardDescription>Differences between the two latest snapshots.</CardDescription>
      </CardHeader>
      <CardContent className="topology-panel-body">
        {/* One summary per refresh, rather than one announcement per entry. */}
        <p className="sr-only" aria-live="polite">{announcement}</p>
        {changes.length === 0 ? (
          <p className="topology-panel-empty">
            {latestChangeCount === null ? 'Changes appear here after the next refresh.' : 'Nothing has changed so far.'}
          </p>
        ) : (
          <ul className="topology-change-list">
            {changes.map((change) => (
              <li key={change.id} className="topology-change">
                <StatusDot tone={changeDot[change.tone]} />
                <span>
                  <strong>{change.subject}</strong> {change.detail}
                </span>
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function sourceBadge(source: TopologySourceStatus) {
  if (source.status === 'failed') return <Badge tone="destructive">Failed</Badge>;
  if (source.durationMs > SLOW_SOURCE_MS) return <Badge tone="warning">Slow</Badge>;
  return <Badge tone="success">OK</Badge>;
}

interface SourcesPanelProps {
  sources: TopologySourceStatus[];
  querying: boolean;
  intervalMs: number | null;
  nextRefreshAt: number | null;
  refreshCount: number;
}

/** Seconds left until `target`, updated every second while one is scheduled. */
function useSecondsUntil(target: number | null) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (target === null) return undefined;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, [target]);
  return target === null ? null : Math.max(0, Math.ceil((target - now) / 1_000));
}

export function SourcesPanel({ sources, querying, intervalMs, nextRefreshAt, refreshCount }: SourcesPanelProps) {
  const seconds = useSecondsUntil(nextRefreshAt);
  let countdown = 'Paused';
  if (intervalMs === null) countdown = 'Manual';
  else if (querying) countdown = 'Querying…';
  else if (seconds !== null) countdown = `in ${seconds} s`;

  return (
    <Card className="topology-sources">
      <CardHeader>
        <CardTitle>Snapshot sources</CardTitle>
        <CardDescription>Timing and result of the latest query for each layer of the map.</CardDescription>
      </CardHeader>
      <CardContent className="topology-panel-body">
        <ul className="topology-source-list">
          {sources.map((source) => (
            <li key={source.source} className="topology-source" title={source.status === 'failed' ? source.message ?? undefined : undefined}>
              <span>{TOPOLOGY_SOURCE_LABELS[source.source]}</span>
              <span className="topology-mono">{`${source.durationMs} ms`}</span>
              {sourceBadge(source)}
            </li>
          ))}
        </ul>
        <div className="topology-next-refresh">
          <span>Next refresh</span>
          <span className="topology-countdown-track" aria-hidden="true">
            {intervalMs !== null && nextRefreshAt !== null && !querying ? (
              <span
                key={`${refreshCount}-${nextRefreshAt}`}
                className="topology-countdown-fill"
                style={{ '--countdown': `${Math.max(0, nextRefreshAt - Date.now())}ms` } as CSSProperties}
              />
            ) : null}
          </span>
          <span className="topology-mono">{countdown}</span>
        </div>
      </CardContent>
    </Card>
  );
}
