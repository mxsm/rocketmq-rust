import { GitBranch } from 'lucide-react';
import EmptyState from '../../components/EmptyState';
import ErrorState from '../../components/ErrorState';
import LoadingState from '../../components/LoadingState';
import StatusBadge from '../../components/StatusBadge';
import type { MessageTraceNode } from '../../types/message';
import { formatMessageTimestamp, sortTraceNodes, traceNodeTone } from './message-model';

interface TraceTimelineProps {
  nodes: MessageTraceNode[];
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
}

export default function TraceTimeline({ nodes, loading = false, error, onRetry }: TraceTimelineProps) {
  if (loading) return <LoadingState label="Loading trace nodes" />;
  if (error) return <ErrorState message={error} onRetry={onRetry} retryLabel="Reload trace" />;
  if (nodes.length === 0) return <EmptyState title="No trace nodes" detail="The trace API returned no nodes for this message." />;

  return (
    <ol className="trace-timeline" aria-label="Returned trace nodes">
      {sortTraceNodes(nodes).map((node, index) => (
        <li key={`${node.timestamp}-${node.nodeType}-${node.name}-${index}`}>
          <span className={`trace-timeline-marker trace-timeline-marker-${traceNodeTone(node)}`}>
            <GitBranch size={14} aria-hidden="true" />
          </span>
          <div className="trace-timeline-card">
            <div>
              <strong>{node.name || 'Unnamed node'}</strong>
              <StatusBadge status={node.status || 'UNKNOWN'} tone={traceNodeTone(node)} />
            </div>
            <dl>
              <div><dt>Node type</dt><dd>{node.nodeType || '-'}</dd></div>
              <div><dt>Timestamp</dt><dd>{formatMessageTimestamp(node.timestamp)}</dd></div>
            </dl>
          </div>
        </li>
      ))}
    </ol>
  );
}
