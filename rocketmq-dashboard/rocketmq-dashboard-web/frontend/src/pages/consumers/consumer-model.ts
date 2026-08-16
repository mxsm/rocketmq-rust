import type {
  ConsumerConfigTarget,
  ConsumerGroupListItem,
  ConsumerOperationResult,
  ConsumerQueryScope
} from '../../types/consumer';

export type ConsumerCategory = 'NORMAL' | 'FIFO' | 'SYSTEM';
export type ConsumerLagFilter = 'all' | 'lagging' | 'clear';
export type ConsumerSortKey =
  | 'rawGroupName'
  | 'connectionCount'
  | 'consumeTps'
  | 'diffTotal'
  | 'version'
  | 'updateTimestamp';
export type ConsumerSortDirection = 'asc' | 'desc';

export interface ConsumerFilters {
  query: string;
  categories: ConsumerCategory[];
  consumeTypes: string[];
  messageModels: string[];
  lag: ConsumerLagFilter;
  brokers: string[];
  versions: string[];
}

export interface ConsumerSort {
  key: ConsumerSortKey;
  direction: ConsumerSortDirection;
}

export interface ConsumerActionAvailability {
  inspect: boolean;
  clients: boolean;
  progress: boolean;
  config: boolean;
  edit: boolean;
  reset: boolean;
  delete: boolean;
}

export const DEFAULT_CONSUMER_FILTERS: ConsumerFilters = {
  query: '',
  categories: [],
  consumeTypes: [],
  messageModels: [],
  lag: 'all',
  brokers: [],
  versions: []
};

export const DEFAULT_CONSUMER_SORT: ConsumerSort = {
  key: 'rawGroupName',
  direction: 'asc'
};

export function normalizeConsumerValue(value: string): string {
  return value
    .replace(/^CONSUME_/, '')
    .replace(/^MESSAGE_MODEL_/, '') || 'UNKNOWN';
}

export function consumerCategoryOf(item: Pick<ConsumerGroupListItem, 'rawGroupName' | 'category'>): ConsumerCategory {
  const normalized = item.category.toUpperCase();
  if (normalized === 'FIFO') return 'FIFO';
  if (isSystemConsumerGroup(item)) return 'SYSTEM';
  if (normalized === 'SYSTEM') return 'SYSTEM';
  return 'NORMAL';
}

export function isSystemConsumerGroup(item: Pick<ConsumerGroupListItem, 'rawGroupName' | 'category'>): boolean {
  const group = item.rawGroupName;
  const category = item.category.toUpperCase();
  return group.startsWith('%SYS%')
    || group.startsWith('CID_RMQ_SYS_')
    || category === 'SYSTEM'
    || [
      'TOOLS_CONSUMER',
      'FILTERSRV_CONSUMER',
      'SELF_TEST_C_GROUP',
      'CID_ONS-HTTP-PROXY',
      'CID_ONSAPI_PULL',
      'CID_ONSAPI_PERMISSION',
      'CID_ONSAPI_OWNER',
      'CID_RMQ_SYS_TRANS',
      'CID_DefaultHeartBeatSyncerTopic'
    ].includes(group);
}

export function getConsumerActionAvailability(
  item: Pick<ConsumerGroupListItem, 'rawGroupName' | 'category'>
): ConsumerActionAvailability {
  const inspectable = true;
  const mutable = !isSystemConsumerGroup(item);
  return {
    inspect: inspectable,
    clients: true,
    progress: true,
    config: true,
    edit: mutable,
    reset: mutable,
    delete: mutable
  };
}

export function getConsumerMetrics(consumers: ConsumerGroupListItem[]) {
  return consumers.reduce(
    (metrics, consumer) => ({
      groups: metrics.groups + 1,
      connectedClients: metrics.connectedClients + consumer.connectionCount,
      totalLag: metrics.totalLag + consumer.diffTotal,
      laggingGroups: metrics.laggingGroups + (consumer.diffTotal > 0 ? 1 : 0)
    }),
    { groups: 0, connectedClients: 0, totalLag: 0, laggingGroups: 0 }
  );
}

export function selectConsumerRows(
  consumers: ConsumerGroupListItem[],
  filters: ConsumerFilters,
  sort: ConsumerSort = DEFAULT_CONSUMER_SORT
): ConsumerGroupListItem[] {
  const query = filters.query.trim().toLowerCase();
  const categories = new Set(filters.categories);
  const consumeTypes = new Set(filters.consumeTypes.map((value) => value.toUpperCase()));
  const messageModels = new Set(filters.messageModels.map((value) => value.toUpperCase()));
  const brokers = new Set(filters.brokers);
  const versions = new Set(filters.versions);

  const filtered = consumers.filter((consumer) => {
    const category = consumerCategoryOf(consumer);
    const matchesQuery = !query
      || consumer.rawGroupName.toLowerCase().includes(query)
      || consumer.displayGroupName.toLowerCase().includes(query);
    const matchesCategory = categories.size === 0 || categories.has(category);
    const matchesConsumeType = consumeTypes.size === 0
      || consumeTypes.has(normalizeConsumerValue(consumer.consumeType).toUpperCase());
    const matchesMessageModel = messageModels.size === 0
      || messageModels.has(normalizeConsumerValue(consumer.messageModel).toUpperCase());
    const matchesLag = filters.lag === 'all'
      || (filters.lag === 'lagging' && consumer.diffTotal > 0)
      || (filters.lag === 'clear' && consumer.diffTotal === 0);
    const matchesBroker = brokers.size === 0
      || consumer.brokerNames.some((broker) => brokers.has(broker));
    const matchesVersion = versions.size === 0 || versions.has(consumer.versionDesc);
    return matchesQuery
      && matchesCategory
      && matchesConsumeType
      && matchesMessageModel
      && matchesLag
      && matchesBroker
      && matchesVersion;
  });

  const direction = sort.direction === 'asc' ? 1 : -1;
  return filtered.sort((left, right) => {
    const leftValue = sortValue(left, sort.key);
    const rightValue = sortValue(right, sort.key);
    if (leftValue < rightValue) return -1 * direction;
    if (leftValue > rightValue) return 1 * direction;
    return left.rawGroupName.localeCompare(right.rawGroupName);
  });
}

function sortValue(item: ConsumerGroupListItem, key: ConsumerSortKey): string | number {
  switch (key) {
    case 'rawGroupName':
      return item.rawGroupName;
    case 'connectionCount':
      return item.connectionCount;
    case 'consumeTps':
      return item.consumeTps;
    case 'diffTotal':
      return item.diffTotal;
    case 'version':
      return item.version ?? -1;
    case 'updateTimestamp':
      return item.updateTimestamp;
  }
}

export function clampConsumerPage(page: number, pageSize: number, total: number): number {
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  return Math.min(Math.max(1, page), pageCount);
}

export function summarizeConsumerTargets(item: ConsumerGroupListItem): string {
  if (item.brokerNames.length === 0) return 'Unknown';
  if (item.brokerNames.length === 1) return item.brokerNames[0];
  return `${item.brokerNames[0]} +${item.brokerNames.length - 1}`;
}

export function deriveInconsistentConsumerFields(targets: ConsumerConfigTarget[]): string[] {
  const values = targets
    .map((target) => target.config)
    .filter((config): config is NonNullable<typeof config> => config !== null);
  if (values.length < 2) return [];
  const first = values[0];
  const fields: string[] = [];
  const compare = <K extends keyof typeof first>(key: K, label: string) => {
    if (values.some((value) => value[key] !== first[key])) fields.push(label);
  };
  compare('consumeEnable', 'consumeEnable');
  compare('consumeFromMinEnable', 'consumeFromMinEnable');
  compare('consumeBroadcastEnable', 'consumeBroadcastEnable');
  compare('consumeMessageOrderly', 'consumeMessageOrderly');
  compare('retryQueueNums', 'retryQueueNums');
  compare('retryMaxTimes', 'retryMaxTimes');
  compare('brokerId', 'brokerId');
  compare('whichBrokerWhenConsumeSlowly', 'whichBrokerWhenConsumeSlowly');
  compare('notifyConsumerIdsChangedEnable', 'notifyConsumerIdsChangedEnable');
  compare('groupSysFlag', 'groupSysFlag');
  compare('consumeTimeoutMinute', 'consumeTimeoutMinute');
  compare('groupRetryPolicyJson', 'groupRetryPolicyJson');
  return fields;
}

export function consumerOperationSucceeded(result: ConsumerOperationResult): boolean {
  return result.success && result.targets.every((target) => target.success);
}

export function consumerQueryIdentity(scope: ConsumerQueryScope): string {
  return `${scope.mode}:${scope.mode === 'proxy' ? scope.proxyAddress ?? '' : ''}`;
}
