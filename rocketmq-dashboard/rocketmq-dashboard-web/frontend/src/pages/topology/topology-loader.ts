import { brokerApi } from '../../api/broker_api';
import { ApiClientError, safeServerErrorMessage, userErrorMessage } from '../../api/client';
import { configApi } from '../../api/config_api';
import { consumerApi } from '../../api/consumer_api';
import { producerApi } from '../../api/producer_api';
import { topicApi } from '../../api/topic_api';
import { topologyApi } from '../../api/topology_api';
import type { TopologyLayerId, TopologySnapshot, TopologySourceStatus } from '../../types/topology';
import { buildTopologySnapshot, sortClusters, TOPOLOGY_SOURCES, type TopologyLayers } from './topology-model';

type LayerQueries = { [Source in TopologyLayerId]: () => Promise<NonNullable<TopologyLayers[Source]>> };

export interface TopologyLoaderDeps {
  aggregated: () => Promise<TopologySnapshot>;
  layers: LayerQueries;
  /** Monotonic clock for request timing. */
  elapsed: () => number;
  /** Wall clock for the snapshot time. */
  now: () => number;
}

const defaultDeps: TopologyLoaderDeps = {
  aggregated: topologyApi.snapshot,
  layers: {
    config: configApi.getConfig,
    nameservers: configApi.getNameserverAvailability,
    brokers: async () => (await brokerApi.list()).items,
    topics: async () => (await topicApi.list()).items,
    producers: producerApi.list,
    consumers: async () => (await consumerApi.list()).items
  },
  elapsed: () => performance.now(),
  now: () => Date.now()
};

const failureMessage = (error: unknown) => userErrorMessage(error, 'The query failed.');

function isMissingRoute(error: unknown) {
  return error instanceof ApiClientError && (error.code === 'API_ROUTE_NOT_FOUND' || error.status === 404);
}

/** A snapshot in which every layer failed, carrying the reason for the source list. */
export function failedTopologySnapshot(error: unknown, durationMs: number, generatedAt: number): TopologySnapshot {
  const message = failureMessage(error);
  return {
    generatedAt,
    sources: TOPOLOGY_SOURCES.map((source) => ({ source, status: 'failed', durationMs, message })),
    nameServers: [],
    clusters: [],
    controllers: [],
    producers: [],
    consumers: [],
    totals: { producerGroups: 0, consumerGroups: 0, messageBacklog: 0 }
  };
}

/**
 * Brings an aggregated response in line with what the list queries produce: tolerates fields a
 * different backend version leaves out, turns error codes into text and uses the same ordering.
 */
function normalize(snapshot: TopologySnapshot, now: number): TopologySnapshot {
  return {
    generatedAt: snapshot.generatedAt ?? now,
    sources: (snapshot.sources ?? []).map((source) => ({
      ...source,
      message: source.message ?? (source.status === 'failed' ? safeServerErrorMessage(source.code ?? '', 0) : null)
    })),
    nameServers: snapshot.nameServers ?? [],
    clusters: sortClusters(snapshot.clusters ?? []),
    controllers: snapshot.controllers ?? [],
    producers: snapshot.producers ?? [],
    consumers: (snapshot.consumers ?? []).map((consumer) => ({ ...consumer, displayName: consumer.displayName || consumer.group })),
    totals: snapshot.totals ?? { producerGroups: 0, consumerGroups: 0, messageBacklog: 0 }
  };
}

async function timed<Source extends TopologyLayerId>(
  source: Source,
  query: LayerQueries[Source],
  elapsed: () => number
): Promise<{ status: TopologySourceStatus; data: TopologyLayers[Source] }> {
  const started = elapsed();
  try {
    const data = await query();
    return { status: { source, status: 'ok', durationMs: Math.round(elapsed() - started) }, data };
  } catch (error) {
    return {
      status: { source, status: 'failed', durationMs: Math.round(elapsed() - started), message: failureMessage(error) },
      data: null
    };
  }
}

async function loadLayers(deps: TopologyLoaderDeps): Promise<TopologySnapshot> {
  const { layers, elapsed } = deps;
  const [config, nameservers, brokers, topics, producers, consumers] = await Promise.all([
    timed('config', layers.config, elapsed),
    timed('nameservers', layers.nameservers, elapsed),
    timed('brokers', layers.brokers, elapsed),
    timed('topics', layers.topics, elapsed),
    timed('producers', layers.producers, elapsed),
    timed('consumers', layers.consumers, elapsed)
  ]);
  return buildTopologySnapshot(
    {
      config: config.data,
      nameservers: nameservers.data,
      brokers: brokers.data,
      topics: topics.data,
      producers: producers.data,
      consumers: consumers.data
    },
    [config.status, nameservers.status, brokers.status, topics.status, producers.status, consumers.status],
    deps.now()
  );
}

/**
 * Loads one snapshot: from the aggregated endpoint when the backend has it, otherwise from the
 * existing list queries in parallel. Never rejects; failures are reported per source.
 */
export function createTopologyLoader(deps: TopologyLoaderDeps = defaultDeps): () => Promise<TopologySnapshot> {
  let aggregatedMissing = false;

  return async () => {
    if (!aggregatedMissing) {
      const started = deps.elapsed();
      try {
        return normalize(await deps.aggregated(), deps.now());
      } catch (error) {
        if (!isMissingRoute(error)) {
          return failedTopologySnapshot(error, Math.round(deps.elapsed() - started), deps.now());
        }
        // An older backend: remember it so later refreshes go straight to the list queries.
        aggregatedMissing = true;
      }
    }
    return loadLayers(deps);
  };
}
