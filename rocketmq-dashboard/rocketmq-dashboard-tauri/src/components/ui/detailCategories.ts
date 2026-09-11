export const DETAIL_CATEGORIES = ['All', 'Broker', 'Runtime', 'Storage', 'Messaging', 'Security', 'Other'];

export const categorizeDetailKey = (key: string) => {
  const normalized = key.toLowerCase();

  if (normalized.includes('acl') || normalized.includes('auth') || normalized.includes('permission')) {
    return 'Security';
  }

  if (
    normalized.includes('disk') ||
    normalized.includes('commitlog') ||
    normalized.includes('consumequeue') ||
    normalized.includes('flush') ||
    normalized.includes('store')
  ) {
    return 'Storage';
  }

  if (
    normalized.includes('topic') ||
    normalized.includes('message') ||
    normalized.includes('queue') ||
    normalized.includes('subscription') ||
    normalized.includes('dispatch')
  ) {
    return 'Messaging';
  }

  if (
    normalized.includes('tps') ||
    normalized.includes('ratio') ||
    normalized.includes('offset') ||
    normalized.includes('time') ||
    normalized.includes('timestamp') ||
    normalized.includes('active')
  ) {
    return 'Runtime';
  }

  if (
    normalized.includes('broker') ||
    normalized.includes('name') ||
    normalized.includes('addr') ||
    normalized.includes('port') ||
    normalized.includes('cluster') ||
    normalized.includes('version')
  ) {
    return 'Broker';
  }

  return 'Other';
};
