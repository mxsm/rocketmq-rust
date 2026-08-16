import type { TopicInfo } from '../../types/topic';

export type TopicMessageType = 'NORMAL' | 'DELAY' | 'FIFO' | 'TRANSACTION' | 'UNSPECIFIED';
export type TopicCategory = 'APPLICATION' | 'RETRY' | 'DLQ' | 'SYSTEM';
export type TopicOperationalCategory = Lowercase<TopicCategory>;

export interface TopicFilters {
  query: string;
  brokerName: string;
  clusterName: string;
  messageTypes: TopicMessageType[];
  categories: TopicCategory[];
}

interface LegacyTopicFilters {
  query: string;
  brokerName: string;
  category: TopicOperationalCategory | 'all';
}

export interface TopicMetrics {
  total: number;
  application: number;
  retry: number;
  dlq: number;
  system: number;
}

export interface TopicActionAvailability {
  edit: boolean;
  send: boolean;
  reset: boolean;
  skip: boolean;
  deleteBroker: boolean;
  deleteTopic: boolean;
}

export function getTopicCategory(topic: TopicInfo): TopicOperationalCategory {
  const category = topic.category?.trim().toUpperCase();
  if (topic.systemTopic === true || category === 'SYSTEM') return 'system';
  if (category === 'RETRY') return 'retry';
  if (category === 'DLQ') return 'dlq';
  if (category) return 'application';

  const name = topic.topic.toUpperCase();
  if (name.startsWith('%RETRY%')) return 'retry';
  if (name.startsWith('%DLQ%')) return 'dlq';
  if (isLegacySystemTopicName(name)) return 'system';
  return 'application';
}

export function filterTopics(topics: TopicInfo[], filters: TopicFilters): TopicInfo[];
export function filterTopics(topics: TopicInfo[], filters: LegacyTopicFilters): TopicInfo[];
export function filterTopics(topics: TopicInfo[], filters: TopicFilters | LegacyTopicFilters) {
  const query = filters.query.trim().toLowerCase();
  return topics.filter((topic) => {
    const matchesQuery = !query || topic.topic.toLowerCase().includes(query);
    const matchesBroker = filters.brokerName === 'all'
      || topic.brokers?.includes(filters.brokerName)
      || topic.brokerName === filters.brokerName;

    if (!matchesQuery || !matchesBroker) return false;

    if (isCatalogFilter(filters)) {
      const matchesCluster = filters.clusterName === 'all' || topic.clusters?.includes(filters.clusterName);
      const classificationUnrestricted = filters.messageTypes.length === 0 && filters.categories.length === 0;
      const matchesMessageType = filters.messageTypes.includes(topic.messageType?.toUpperCase() as TopicMessageType);
      const matchesCategory = filters.categories.includes(toTopicCategory(getTopicCategory(topic)));
      return matchesCluster && (classificationUnrestricted || matchesMessageType || matchesCategory);
    }

    return filters.category === 'all' || getTopicCategory(topic) === filters.category;
  });
}

export function getTopicActionAvailability(topic: TopicInfo): TopicActionAvailability {
  const available = getTopicCategory(topic) !== 'system';
  return {
    edit: available,
    send: available,
    reset: available,
    skip: available,
    deleteBroker: available,
    deleteTopic: available
  };
}

export function getTopicPermissionLabel(perm: number) {
  const readable = (perm & 4) === 4;
  const writable = (perm & 2) === 2;
  if (readable && writable) return 'RW';
  if (readable) return 'R';
  if (writable) return 'W';
  return 'None';
}

export function getTopicMetrics(topics: TopicInfo[]): TopicMetrics {
  return topics.reduce<TopicMetrics>((metrics, topic) => {
    metrics.total += 1;
    metrics[getTopicCategory(topic)] += 1;
    return metrics;
  }, { total: 0, application: 0, retry: 0, dlq: 0, system: 0 });
}

function isCatalogFilter(filters: TopicFilters | LegacyTopicFilters): filters is TopicFilters {
  return 'messageTypes' in filters && 'categories' in filters && 'clusterName' in filters;
}

function toTopicCategory(category: TopicOperationalCategory): TopicCategory {
  return category.toUpperCase() as TopicCategory;
}

function isLegacySystemTopicName(name: string) {
  return (
    name.startsWith('%SYS%')
    || name.startsWith('RMQ_SYS_')
    || name.startsWith('SCHEDULE_TOPIC_')
    || name.startsWith('DEFAULTCLUSTER')
    || name.startsWith('BROKER-')
    || name.endsWith('_REPLY_TOPIC')
    || name === 'TRANS_CHECK_MAX_TIME_TOPIC'
    || name === 'CHECKPOINT_TOPIC'
    || name === 'SELF_TEST_TOPIC'
    || name === 'DEFAULTHEARTBEATSYNCERTOPIC'
    || name === 'TBW102'
    || name === 'OFFSET_MOVED_EVENT'
    || name === 'BENCHMARKTEST'
  );
}
