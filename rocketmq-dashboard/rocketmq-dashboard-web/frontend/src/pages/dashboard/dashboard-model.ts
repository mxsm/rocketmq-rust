import type { DashboardOverview, TopicCurrentMetric } from '../../types/dashboard';

export interface DashboardAdvisory {
  id: 'system-status' | 'nameserver' | 'brokers' | 'backlog';
  title: string;
  detail: string;
  target: '/brokers' | '/config' | '/consumers';
  actionLabel: string;
  tone: 'warning' | 'destructive' | 'info';
}

export function buildDashboardAdvisories(overview: DashboardOverview): DashboardAdvisory[] {
  const advisories: DashboardAdvisory[] = [];

  if (overview.systemStatus.toUpperCase() !== 'UP') {
    advisories.push({
      id: 'system-status',
      title: `System status is ${overview.systemStatus}`,
      detail: 'Inspect the broker inventory for the current cluster state.',
      target: '/brokers',
      actionLabel: 'Inspect cluster',
      tone: 'warning'
    });
  }

  if (!overview.currentNamesrv) {
    advisories.push({
      id: 'nameserver',
      title: 'NameServer is not configured',
      detail: 'Configure a NameServer address before running admin queries.',
      target: '/config',
      actionLabel: 'Open OPS',
      tone: 'destructive'
    });
  }

  if (overview.brokerCount === 0) {
    advisories.push({
      id: 'brokers',
      title: 'No brokers are visible',
      detail: 'The current NameServer response contains no broker entries.',
      target: '/brokers',
      actionLabel: 'Open cluster',
      tone: 'destructive'
    });
  }

  if (overview.messageBacklog > 0) {
    advisories.push({
      id: 'backlog',
      title: 'Consumer backlog requires review',
      detail: `${new Intl.NumberFormat('en-US').format(Math.round(overview.messageBacklog))} messages are waiting across consumer groups.`,
      target: '/consumers',
      actionLabel: 'Inspect consumers',
      tone: 'warning'
    });
  }

  return advisories;
}

function trimCompact(value: number) {
  return value.toFixed(2).replace(/\.0+$|(?<=\.[0-9])0$/, '');
}

export function formatDashboardMetric(value: number) {
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000) return `${trimCompact(value / 1_000_000)}M`;
  if (absolute >= 1_000) return `${trimCompact(value / 1_000)}K`;
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
}

export function sortTopTopics(topics: TopicCurrentMetric[], limit = 10) {
  return topics
    .map((topic, index) => ({ topic, index }))
    .sort((left, right) => right.topic.totalMsg - left.topic.totalMsg || left.index - right.index)
    .slice(0, limit)
    .map(({ topic }) => topic);
}
