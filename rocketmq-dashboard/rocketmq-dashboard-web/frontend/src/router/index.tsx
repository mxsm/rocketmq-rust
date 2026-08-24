export const canonicalRoutePatterns = [
  '/login',
  '/proxy',
  '/dashboard',
  '/topics',
  '/topics/:topic',
  '/consumers',
  '/consumers/:group',
  '/producers',
  '/brokers',
  '/brokers/:brokerName',
  '/messages',
  '/messages/dlq',
  '/message-trace',
  '/acl',
  '/audit',
  '/monitors',
  '/sessions',
  '/config'
] as const;

export const compatibilityRedirects = {
  '/': '/dashboard',
  '/ops': '/config',
  '/cluster': '/brokers',
  '/dlq': '/messages/dlq'
} as const;

export const routes = [
  '/ops',
  '/proxy',
  '/dashboard',
  '/cluster',
  '/topics',
  '/consumers',
  '/producers',
  '/brokers',
  '/messages',
  '/messages/dlq',
  '/dlq',
  '/message-trace',
  '/acl',
  '/audit',
  '/monitors',
  '/sessions',
  '/login',
  '/config'
];
