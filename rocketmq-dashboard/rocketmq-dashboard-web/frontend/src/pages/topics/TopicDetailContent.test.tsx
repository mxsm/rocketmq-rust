import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { StrictMode } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../../api/topic_api';
import { deferred } from '../../test/deferred';
import { renderAtRoute } from '../../test/render';
import type { TopicConfigView, TopicInfo, TopicStatsInfo } from '../../types/topic';
import TopicDetailContent from './TopicDetailContent';

vi.mock('../../api/topic_api', () => ({
  topicApi: {
    get: vi.fn(),
    route: vi.fn(),
    stats: vi.fn(),
    config: vi.fn(),
    consumers: vi.fn()
  }
}));

const topicFixture: TopicInfo = {
  topic: 'orders',
  brokerName: 'broker-a',
  brokers: ['broker-a'],
  clusters: ['DefaultCluster'],
  readQueueCount: 8,
  writeQueueCount: 8,
  perm: 6,
  category: 'NORMAL',
  messageType: 'NORMAL',
  order: false,
  systemTopic: false
};

const statsFixture: TopicStatsInfo = {
  topic: 'orders',
  queueCount: 2,
  totalMessageCount: 8_280,
  totalMinOffset: 120,
  totalMaxOffset: 8_400,
  offsets: [
    { brokerName: 'broker-a', queueId: 0, minOffset: 120, maxOffset: 4_200, lastUpdateTimestamp: 1_723_766_400_000 },
    { brokerName: 'broker-b', queueId: 0, minOffset: 200, maxOffset: 4_400, lastUpdateTimestamp: 1_723_766_460_000 }
  ]
};

const configFixture: TopicConfigView = {
  topicName: 'orders',
  brokerName: 'broker-a',
  clusterName: 'DefaultCluster',
  brokerNameList: ['broker-a', 'broker-b'],
  clusterNameList: ['DefaultCluster'],
  readQueueNums: 8,
  writeQueueNums: 8,
  perm: 6,
  order: false,
  messageType: 'NORMAL',
  attributes: { 'message.type': 'NORMAL', 'cleanup.policy': 'DELETE' },
  inconsistentFields: ['writeQueueNums']
};

describe('TopicDetailContent', () => {
  beforeAll(() => {
    Element.prototype.scrollIntoView = vi.fn();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.get).mockResolvedValue(topicFixture);
    vi.mocked(topicApi.stats).mockResolvedValue(statsFixture);
    vi.mocked(topicApi.route).mockResolvedValue({
      topic: 'orders',
      brokers: [{ brokerName: 'broker-a', brokerAddrs: ['127.0.0.1:10911'] }],
      queues: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6 }]
    });
    vi.mocked(topicApi.consumers).mockResolvedValue({
      items: [{ consumerGroup: 'order-service', totalDiff: 120, inflightDiff: 4, consumeTps: 8.5 }]
    });
    vi.mocked(topicApi.config).mockResolvedValue(configFixture);
  });

  it('loads route and status independently and caches both for the selected topic', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicDetailContent topicName="orders" topic={topicFixture} />, '/topics');

    expect(topicApi.get).not.toHaveBeenCalled();
    expect(await screen.findByRole('group', { name: 'Queue entries: 2' })).toBeInTheDocument();
    expect(topicApi.route).not.toHaveBeenCalled();

    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
    expect(await screen.findByRole('row', { name: /broker-a.*8.*8.*RW/ })).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /broker-a.*0.*120.*4200.*4080/ })).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /broker-b.*0.*200.*4400.*4200/ })).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Overview' }));
    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));

    expect(topicApi.stats).toHaveBeenCalledTimes(1);
    expect(topicApi.route).toHaveBeenCalledTimes(1);
    expect(topicApi.consumers).not.toHaveBeenCalled();
    expect(topicApi.config).not.toHaveBeenCalled();
  });

  it('loads topic identity for a direct route when list context is unavailable', async () => {
    renderAtRoute(<TopicDetailContent topicName="orders" />, '/topics/orders');

    expect(await screen.findByText('broker-a')).toBeInTheDocument();
    expect(topicApi.get).toHaveBeenCalledWith('orders');
  });

  it('loads consumers independently and exposes group-scoped reset and skip actions', async () => {
    const user = userEvent.setup();
    const onReset = vi.fn();
    const onSkip = vi.fn();
    renderAtRoute(
      <TopicDetailContent topicName="orders" topic={topicFixture} onReset={onReset} onSkip={onSkip} />,
      '/topics'
    );

    await user.click(screen.getByRole('tab', { name: 'Consumers' }));
    expect(topicApi.consumers).toHaveBeenCalledWith('orders');
    const row = await screen.findByRole('row', { name: /order-service.*120.*4.*8.5/ });
    await user.click(within(row).getByRole('button', { name: 'Reset order-service' }));
    await user.click(within(row).getByRole('button', { name: 'Skip order-service' }));

    expect(onReset).toHaveBeenCalledWith('order-service');
    expect(onSkip).toHaveBeenCalledWith('order-service');
    expect(topicApi.config).not.toHaveBeenCalled();
  });

  it('keeps consumer loading, error, retry, and empty states within the consumers tab', async () => {
    const user = userEvent.setup();
    const consumersRequest = deferred<{ items: [] }>();
    vi.mocked(topicApi.consumers)
      .mockImplementationOnce(() => consumersRequest.promise)
      .mockResolvedValueOnce({ items: [] });
    renderAtRoute(<TopicDetailContent topicName="orders" topic={topicFixture} />, '/topics');

    await user.click(screen.getByRole('tab', { name: 'Consumers' }));
    expect(screen.getByRole('status', { name: 'Loading topic consumers' })).toBeInTheDocument();
    await act(async () => consumersRequest.reject(new Error('consumers unavailable')));
    expect(await screen.findByRole('alert')).toHaveTextContent('consumers unavailable');

    await user.click(screen.getByRole('button', { name: 'Retry consumers' }));
    expect(await screen.findByText('No consumers')).toBeInTheDocument();
    expect(topicApi.consumers).toHaveBeenCalledTimes(2);
    expect(topicApi.route).not.toHaveBeenCalled();
    expect(topicApi.config).not.toHaveBeenCalled();
  });

  it('keeps route data visible when configuration fails and retries only configuration', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.config)
      .mockRejectedValueOnce(new Error('config unavailable'))
      .mockResolvedValueOnce(configFixture);
    renderAtRoute(<TopicDetailContent topicName="orders" topic={topicFixture} />, '/topics');

    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
    expect(await screen.findByRole('region', { name: 'Topic routes' })).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('config unavailable');
    await user.click(screen.getByRole('button', { name: 'Retry configuration' }));
    expect(await screen.findByRole('combobox', { name: 'Configuration broker' })).toHaveTextContent('broker-a');
    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));

    expect(topicApi.route).toHaveBeenCalledTimes(1);
    expect(topicApi.config).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('region', { name: 'Topic routes' })).toBeInTheDocument();
  });

  it('renders effective configuration, attributes, inconsistencies, and edit callback', async () => {
    const user = userEvent.setup();
    const onEdit = vi.fn();
    renderAtRoute(
      <TopicDetailContent topicName="orders" topic={topicFixture} onEdit={onEdit} />,
      '/topics'
    );

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByRole('combobox', { name: 'Configuration broker' })).toHaveTextContent('broker-a');
    expect(screen.getByText('message.type')).toBeInTheDocument();
    expect(screen.getByText('cleanup.policy')).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('writeQueueNums');
    await user.click(screen.getByRole('button', { name: 'Edit topic' }));

    expect(onEdit).toHaveBeenCalledWith(configFixture);
  });

  it.each(['resolve', 'reject'] as const)(
    'keeps the latest broker configuration request after an older request settles with %s',
    async (oldRequestOutcome) => {
      const user = userEvent.setup();
      const olderBrokerBRequest = deferred<TopicConfigView>();
      const brokerARequest = deferred<TopicConfigView>();
      const latestBrokerBRequest = deferred<TopicConfigView>();
      vi.mocked(topicApi.config)
        .mockResolvedValueOnce(configFixture)
        .mockImplementationOnce(() => olderBrokerBRequest.promise)
        .mockImplementationOnce(() => brokerARequest.promise)
        .mockImplementationOnce(() => latestBrokerBRequest.promise);
      renderAtRoute(<TopicDetailContent topicName="orders" topic={topicFixture} />, '/topics');

      await user.click(screen.getByRole('tab', { name: 'Configuration' }));
      const brokerSelect = await screen.findByRole('combobox', { name: 'Configuration broker' });
      brokerSelect.focus();
      await user.keyboard('{Enter}');
      await user.click(screen.getByRole('option', { name: 'broker-b' }));
      expect(screen.getByRole('status', { name: 'Loading topic configuration' })).toBeInTheDocument();

      brokerSelect.focus();
      await user.keyboard('{Enter}');
      await user.click(screen.getByRole('option', { name: 'broker-a' }));

      brokerSelect.focus();
      await user.keyboard('{Enter}');
      await user.click(screen.getByRole('option', { name: 'broker-b' }));

      expect(topicApi.config).toHaveBeenCalledTimes(4);
      expect(topicApi.config).toHaveBeenNthCalledWith(4, 'orders', 'broker-b');

      await act(async () => {
        if (oldRequestOutcome === 'resolve') {
          olderBrokerBRequest.resolve({ ...configFixture, brokerName: 'broker-b', readQueueNums: 64 });
        } else {
          olderBrokerBRequest.reject(new Error('stale broker-b failed'));
        }
      });
      expect(screen.getByRole('status', { name: 'Loading topic configuration' })).toBeInTheDocument();
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
      expect(screen.queryByText('64')).not.toBeInTheDocument();

      await act(async () => brokerARequest.resolve({ ...configFixture, readQueueNums: 12 }));
      expect(screen.getByRole('status', { name: 'Loading topic configuration' })).toBeInTheDocument();
      expect(screen.queryByText('12')).not.toBeInTheDocument();

      await act(async () => latestBrokerBRequest.resolve({
        ...configFixture,
        brokerName: 'broker-b',
        readQueueNums: 32
      }));
      expect(await screen.findByText('32')).toBeInTheDocument();
      expect(screen.getByRole('combobox', { name: 'Configuration broker' })).toHaveTextContent('broker-b');
      expect(screen.queryByText('stale broker-b failed')).not.toBeInTheDocument();
      expect(topicApi.config).toHaveBeenNthCalledWith(2, 'orders', 'broker-b');
      expect(topicApi.config).toHaveBeenNthCalledWith(3, 'orders', 'broker-a');
    }
  );

  it('invalidates in-flight consumer results when the topic identity changes', async () => {
    const user = userEvent.setup();
    const ordersRequest = deferred<{
      items: Array<{ consumerGroup: string; totalDiff: number; inflightDiff: number; consumeTps: number }>;
    }>();
    vi.mocked(topicApi.consumers).mockImplementation((topicName) => topicName === 'orders'
      ? ordersRequest.promise
      : Promise.resolve({
        items: [{ consumerGroup: 'shipping-service', totalDiff: 9, inflightDiff: 1, consumeTps: 3.5 }]
      }));
    const { rerender } = render(
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <TopicDetailContent topicName="orders" topic={topicFixture} />
      </MemoryRouter>
    );

    await user.click(screen.getByRole('tab', { name: 'Consumers' }));
    rerender(
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <TopicDetailContent
          topicName="shipments"
          topic={{ ...topicFixture, topic: 'shipments', brokerName: 'broker-b', brokers: ['broker-b'] }}
        />
      </MemoryRouter>
    );
    await user.click(screen.getByRole('tab', { name: 'Consumers' }));
    expect(await screen.findByRole('row', { name: /shipping-service.*9.*1.*3.5/ })).toBeInTheDocument();

    await act(async () => ordersRequest.resolve({
      items: [{ consumerGroup: 'stale-order-service', totalDiff: 999, inflightDiff: 99, consumeTps: 0 }]
    }));
    expect(screen.queryByText('stale-order-service')).not.toBeInTheDocument();
  });

  it('does not duplicate lazy requests during Strict Mode effect replay', async () => {
    const user = userEvent.setup();
    renderAtRoute(
      <StrictMode>
        <TopicDetailContent topicName="orders" topic={topicFixture} initialTab="consumers" />
      </StrictMode>,
      '/topics'
    );

    expect(await screen.findByRole('row', { name: /order-service.*120.*4.*8.5/ })).toBeInTheDocument();
    expect(topicApi.consumers).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByRole('combobox', { name: 'Configuration broker' })).toBeInTheDocument();
    expect(topicApi.config).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
    expect(await screen.findByRole('region', { name: 'Topic routes' })).toBeInTheDocument();
    expect(topicApi.route).toHaveBeenCalledTimes(1);
    expect(topicApi.stats).toHaveBeenCalledTimes(1);
  });

  it('keeps system topics readable while hiding every mutation callback', async () => {
    const user = userEvent.setup();
    renderAtRoute(
      <TopicDetailContent
        topicName="SYSTEM_TOPIC"
        topic={{ ...topicFixture, topic: 'SYSTEM_TOPIC', systemTopic: true }}
        onEdit={vi.fn()}
        onReset={vi.fn()}
        onSkip={vi.fn()}
      />,
      '/topics'
    );

    await user.click(screen.getByRole('tab', { name: 'Consumers' }));
    expect(await screen.findByRole('region', { name: 'Topic consumers' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Reset order-service/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Skip order-service/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByRole('combobox', { name: 'Configuration broker' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit topic' })).not.toBeInTheDocument();
  });
});
