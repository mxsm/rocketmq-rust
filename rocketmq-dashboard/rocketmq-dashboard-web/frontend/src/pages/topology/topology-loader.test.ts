import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiClientError } from '../../api/client';
import type { TopologySnapshot } from '../../types/topology';
import {
  fixtureAvailability,
  fixtureBrokers,
  fixtureConfig,
  fixtureConsumers,
  fixtureProducers,
  fixtureSnapshot,
  fixtureTopics
} from './topology-fixtures';
import { createTopologyLoader, type TopologyLoaderDeps } from './topology-loader';

const missingRoute = () => new ApiClientError('API_ROUTE_NOT_FOUND', 'The requested resource was not found.', { status: 404 });

/** A query that answers after `durationMs` of fake time. */
function after<T>(durationMs: number, answer: () => T) {
  return () => new Promise<T>((resolve, reject) => {
    setTimeout(() => {
      try {
        resolve(answer());
      } catch (error) {
        reject(error);
      }
    }, durationMs);
  });
}

function deps(overrides: Partial<TopologyLoaderDeps['layers']> = {}, aggregated?: TopologyLoaderDeps['aggregated']): TopologyLoaderDeps {
  return {
    aggregated: aggregated ?? vi.fn(() => Promise.reject(missingRoute())),
    layers: {
      config: after(38, () => fixtureConfig),
      nameservers: after(12, () => fixtureAvailability),
      brokers: after(61, () => fixtureBrokers),
      topics: after(95, () => fixtureTopics),
      producers: after(124, () => fixtureProducers),
      consumers: after(410, () => fixtureConsumers),
      ...overrides
    },
    elapsed: () => performance.now(),
    now: () => 50_000
  };
}

async function settle(load: () => Promise<TopologySnapshot>) {
  const pending = load();
  await vi.advanceTimersByTimeAsync(1_000);
  return pending;
}

describe('createTopologyLoader', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('uses the aggregated endpoint when the backend has it', async () => {
    const aggregated = fixtureSnapshot({}, [], 42);
    // A backend that leaves out the display name still yields a label for every group.
    const fromBackend = {
      ...aggregated,
      consumers: aggregated.consumers.map((consumer) => ({ ...consumer, displayName: '' }))
    };
    const load = createTopologyLoader(deps({}, vi.fn(() => Promise.resolve(fromBackend))));

    const snapshot = await settle(load);

    expect(snapshot.generatedAt).toBe(42);
    expect(snapshot.clusters).toEqual(aggregated.clusters);
    expect(snapshot.consumers.every((consumer) => consumer.displayName === consumer.group)).toBe(true);
  });

  it('turns the error code of a failed aggregated layer into text and orders clusters like the list path', async () => {
    const aggregated = fixtureSnapshot();
    const fromBackend: TopologySnapshot = {
      ...aggregated,
      clusters: [...aggregated.clusters].reverse(),
      sources: aggregated.sources.map((source) =>
        source.source === 'consumers' ? { ...source, status: 'failed', code: 'tools.operation.failed' } : source
      )
    };
    const load = createTopologyLoader(deps({}, vi.fn(() => Promise.resolve(fromBackend))));

    const snapshot = await settle(load);

    expect(snapshot.clusters.map((cluster) => cluster.clusterName)).toEqual(aggregated.clusters.map((cluster) => cluster.clusterName));
    expect(snapshot.sources.find((source) => source.source === 'consumers')?.message).toBe(
      'The RocketMQ administration operation failed.'
    );
    expect(snapshot.sources.find((source) => source.source === 'brokers')?.message).toBeNull();
  });

  it('falls back to the list queries on an older backend and stops asking for the aggregated endpoint', async () => {
    const dependencies = deps();
    const load = createTopologyLoader(dependencies);

    const first = await settle(load);
    await settle(load);

    expect(dependencies.aggregated).toHaveBeenCalledTimes(1);
    expect(first.generatedAt).toBe(50_000);
    expect(first.clusters).toHaveLength(2);
    expect(first.producers).toHaveLength(6);
  });

  it('runs the list queries in parallel and records how long each one took', async () => {
    const snapshot = await settle(createTopologyLoader(deps()));

    expect(snapshot.sources.map((source) => [source.source, source.status, source.durationMs])).toEqual([
      ['config', 'ok', 38],
      ['nameservers', 'ok', 12],
      ['brokers', 'ok', 61],
      ['topics', 'ok', 95],
      ['producers', 'ok', 124],
      ['consumers', 'ok', 410]
    ]);
  });

  it('reports a failing query without losing the other layers', async () => {
    const load = createTopologyLoader(deps({
      brokers: after(61, () => {
        throw new ApiClientError('tools.operation.failed', 'The RocketMQ administration operation failed.');
      })
    }));

    const snapshot = await settle(load);

    expect(snapshot.sources.find((source) => source.source === 'brokers')).toEqual({
      source: 'brokers',
      status: 'failed',
      durationMs: 61,
      message: 'The RocketMQ administration operation failed.'
    });
    expect(snapshot.clusters).toEqual([]);
    expect(snapshot.consumers).toHaveLength(6);
  });

  it('reports every layer as failed when the aggregated endpoint fails, and asks it again next time', async () => {
    const aggregated = vi.fn<() => Promise<TopologySnapshot>>()
      .mockRejectedValueOnce(new ApiClientError('NETWORK_ERROR', 'Unable to reach the dashboard backend.'))
      .mockResolvedValueOnce(fixtureSnapshot());
    const load = createTopologyLoader(deps({}, aggregated));

    const failed = await settle(load);
    const recovered = await settle(load);

    expect(failed.sources).toHaveLength(6);
    expect(failed.sources.every((source) => source.status === 'failed')).toBe(true);
    expect(failed.sources[0].message).toBe('Unable to reach the dashboard backend.');
    expect(recovered.clusters).toHaveLength(2);
    expect(aggregated).toHaveBeenCalledTimes(2);
  });
});
