import type { TopicInfo } from '../../types/topic';

export type TopicOperationalCategory = 'application' | 'retry' | 'dlq' | 'system';

export interface TopicFilters {
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

export function getTopicCategory(topic: TopicInfo): TopicOperationalCategory {
  const name = topic.topic.toUpperCase();
  if (name.startsWith('%RETRY%')) return 'retry';
  if (name.startsWith('%DLQ%')) return 'dlq';
  if (isSystemTopic(topic)) return 'system';
  return 'application';
}

export function filterTopics(topics: TopicInfo[], filters: TopicFilters) {
  const query = filters.query.trim().toLowerCase();
  return topics.filter((topic) => {
    const matchesQuery = !query || topic.topic.toLowerCase().includes(query);
    const matchesBroker = filters.brokerName === 'all' || topic.brokerName === filters.brokerName;
    const matchesCategory = filters.category === 'all' || getTopicCategory(topic) === filters.category;
    return matchesQuery && matchesBroker && matchesCategory;
  });
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

function isSystemTopic(topic: TopicInfo) {
  const name = topic.topic.toUpperCase();
  return (
    topic.category.toUpperCase() === 'SYSTEM'
    || name.startsWith('%SYS%')
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
