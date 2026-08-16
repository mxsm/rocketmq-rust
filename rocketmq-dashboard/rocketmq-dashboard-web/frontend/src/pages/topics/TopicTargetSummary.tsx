import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '../../components/ui/Tooltip';

interface TopicTargetSummaryProps {
  clusters: string[];
  brokers: string[];
}

export default function TopicTargetSummary({ clusters, brokers }: TopicTargetSummaryProps) {
  const visibleClusters = clusters.filter((cluster) => cluster.trim().length > 0);
  const visibleBrokers = brokers.filter((broker) => broker.trim().length > 0);
  const clusterLabel = visibleClusters.join(', ') || 'No clusters';
  const brokerLabel = visibleBrokers.join(', ') || 'No brokers';

  return (
    <TooltipProvider delayDuration={0}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className="entity-name-cell"
            tabIndex={0}
            aria-label={`Clusters: ${clusterLabel}; Brokers: ${brokerLabel}`}
          >
            <span>{compactTargetLabel(visibleClusters, 'No clusters')}</span>
            <small>{compactTargetLabel(visibleBrokers, 'No brokers')}</small>
          </span>
        </TooltipTrigger>
        <TooltipContent>
          <div><strong>Clusters:</strong> {clusterLabel}</div>
          <div><strong>Brokers:</strong> {brokerLabel}</div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

function compactTargetLabel(targets: string[], emptyLabel: string) {
  if (targets.length === 0) return emptyLabel;
  return targets.length === 1 ? targets[0] : `${targets[0]} +${targets.length - 1}`;
}
