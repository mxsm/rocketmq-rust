import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterAll, beforeAll, beforeEach, vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
import { renderAtRoute } from '../test/render';
import type { TopicConfigView, TopicInfo, TopicListView, TopicOperationResult } from '../types/topic';
import TopicListPage from './TopicListPage';

vi.mock('../api/topic_api', () => ({
  topicApi: {
    list: vi.fn(), get: vi.fn(), create: vi.fn(), update: vi.fn(), delete: vi.fn(),
    deleteFromBroker: vi.fn(), route: vi.fn(), stats: vi.fn(), config: vi.fn(),
    consumers: vi.fn(), sendTestMessage: vi.fn(), resetOffset: vi.fn(), skipBacklog: vi.fn()
  }
}));

vi.mock('../api/consumer_api', () => ({ consumerApi: { list: vi.fn() } }));

const topics: TopicInfo[] = [
  { topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a', 'broker-b'], clusters: ['DefaultCluster'], readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL', order: false, systemTopic: false },
  { topic: 'payments', brokerName: 'broker-b', brokers: ['broker-b'], clusters: ['DefaultCluster'], readQueueCount: 4, writeQueueCount: 4, perm: 4, category: 'NORMAL', messageType: 'FIFO', order: true, systemTopic: false },
  { topic: '%RETRY%order-service', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 2, category: 'RETRY', messageType: 'RETRY', order: false, systemTopic: false },
  { topic: '%DLQ%payment-service', brokerName: 'broker-b', brokers: ['broker-b'], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 0, category: 'DLQ', messageType: 'DLQ', order: false, systemTopic: false },
  { topic: 'RMQ_SYS_TRACE_TOPIC', brokerName: null, brokers: [], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 7, category: 'SYSTEM', messageType: 'SYSTEM', order: false, systemTopic: true }
];

const listView: TopicListView = {
  items: topics,
  total: topics.length,
  targets: [
    { clusterName: 'DefaultCluster', brokerNames: ['broker-a', 'broker-b'] },
    { clusterName: 'ArchiveCluster', brokerNames: ['broker-c'] }
  ]
};

const config = (topicName: string, brokerName = 'broker-a'): TopicConfigView => ({
  topicName,
  brokerName,
  clusterName: 'DefaultCluster',
  brokerNameList: [brokerName],
  clusterNameList: ['DefaultCluster'],
  readQueueNums: topicName === 'payments' ? 4 : 8,
  writeQueueNums: topicName === 'payments' ? 4 : 8,
  perm: topicName === 'payments' ? 4 : 6,
  order: topicName === 'payments',
  messageType: topicName === 'payments' ? 'FIFO' : 'NORMAL',
  attributes: {},
  inconsistentFields: []
});

const operationResult = (operation: string, topic: string, message: string): TopicOperationResult => ({
  operation,
  topic,
  success: true,
  targetCount: 1,
  message,
  targets: [{ target: 'broker-a', success: true, message }]
});

async function openActions(user: ReturnType<typeof userEvent.setup>, topic: string) {
  await user.click(screen.getByRole('button', { name: `Actions for ${topic}` }));
}

async function chooseRowAction(user: ReturnType<typeof userEvent.setup>, topic: string, action: string) {
  await openActions(user, topic);
  await user.click(screen.getByRole('menuitem', { name: action }));
}

async function submitCreate(user: ReturnType<typeof userEvent.setup>, topic: string) {
  await user.click(screen.getByRole('button', { name: 'Create topic' }));
  const dialog = screen.getByRole('dialog', { name: 'Create topic' });
  await user.type(within(dialog).getByRole('textbox', { name: 'Topic name' }), topic);
  await user.click(within(dialog).getByRole('checkbox', { name: 'DefaultCluster' }));
  await user.click(within(dialog).getByRole('button', { name: 'Save topic' }));
  await user.click(within(screen.getByRole('alertdialog', { name: 'Create topic?' })).getByRole('button', { name: 'Create topic' }));
  return dialog;
}

describe('TopicListPage', () => {
  beforeAll(() => {
    Object.defineProperty(Element.prototype, 'scrollIntoView', { configurable: true, value: vi.fn() });
  });

  afterAll(() => {
    Reflect.deleteProperty(Element.prototype, 'scrollIntoView');
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.list).mockResolvedValue(listView);
    vi.mocked(topicApi.get).mockImplementation(async (topicName) => topics.find((item) => item.topic === topicName)!);
    vi.mocked(topicApi.create).mockImplementation(async (request) => operationResult('CREATE', request.topic, 'created'));
    vi.mocked(topicApi.update).mockImplementation(async (topicName) => operationResult('UPDATE', topicName, 'updated'));
    vi.mocked(topicApi.delete).mockImplementation(async (topicName) => operationResult('DELETE_TOPIC', topicName, 'deleted'));
    vi.mocked(topicApi.deleteFromBroker).mockImplementation(async (topicName) => operationResult('DELETE_BROKER', topicName, 'deleted'));
    vi.mocked(topicApi.config).mockImplementation(async (topicName) => config(topicName, topicName === 'payments' ? 'broker-b' : 'broker-a'));
    vi.mocked(topicApi.consumers).mockResolvedValue({
      items: [
        { consumerGroup: 'order-service', totalDiff: 120, inflightDiff: 4, consumeTps: 8.5 },
        { consumerGroup: 'audit-service', totalDiff: 5, inflightDiff: 0, consumeTps: 1.25 }
      ]
    });
    vi.mocked(topicApi.stats).mockImplementation(async (topicName) => ({
      topic: topicName, queueCount: 2, totalMessageCount: 8_280,
      totalMinOffset: 120, totalMaxOffset: 8_400, offsets: []
    }));
    vi.mocked(topicApi.route).mockImplementation(async (topicName) => ({ topic: topicName, brokers: [], queues: [] }));
    vi.mocked(topicApi.sendTestMessage).mockResolvedValue({
      topic: 'orders', success: true, sendStatus: 'SEND_OK', messageId: 'msg-1', brokerName: 'broker-a',
      queueId: 0, queueOffset: 1, transactionId: null, regionId: null, localTransactionState: null
    });
    vi.mocked(topicApi.resetOffset).mockResolvedValue({
      operation: 'RESET_OFFSET', topic: 'orders', consumerGroup: 'order-service', success: true,
      affectedQueueCount: 8, appliedTimestamp: 1_786_762_800_000, message: 'reset'
    });
    vi.mocked(topicApi.skipBacklog).mockResolvedValue({
      operation: 'SKIP_BACKLOG', topic: 'orders', consumerGroup: 'order-service', success: true,
      affectedQueueCount: 8, appliedTimestamp: 1_786_762_800_000, message: 'skipped'
    });
    vi.mocked(consumerApi.list).mockRejectedValue(new Error('global consumer discovery must not run'));
  });

  it('uses enriched catalog filters, target options, and operational columns', async () => {
    const user = userEvent.setup();
    const longTargetOrders = {
      ...topics[0],
      clusters: ['DefaultCluster', 'ArchiveCluster', 'LongTermCluster'],
      brokers: ['broker-a', 'broker-b', 'broker-c', 'broker-d']
    };
    vi.mocked(topicApi.list).mockResolvedValueOnce({
      ...listView,
      items: [longTargetOrders, ...topics.slice(1)]
    });
    renderAtRoute(<TopicListPage />, '/topics');

    expect(await screen.findByRole('heading', { name: 'Topics' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Total topics: 5' })).toBeInTheDocument();
    for (const header of ['Category', 'Message type', 'Targets', 'Ordered', 'Permission']) {
      expect(screen.getByRole('columnheader', { name: header })).toBeInTheDocument();
    }
    const ordersRow = screen.getByRole('row', { name: /^orders Full page/ });
    expect(within(ordersRow).getByRole('status', { name: 'NORMAL' })).toBeInTheDocument();
    const targets = within(ordersRow).getByLabelText(
      'Clusters: DefaultCluster, ArchiveCluster, LongTermCluster; Brokers: broker-a, broker-b, broker-c, broker-d'
    );
    expect(targets).toHaveTextContent('DefaultCluster +2');
    expect(targets).toHaveTextContent('broker-a +3');
    fireEvent.focus(targets);
    const tooltip = await screen.findByRole('tooltip');
    expect(tooltip).toHaveTextContent('DefaultCluster, ArchiveCluster, LongTermCluster');
    expect(tooltip).toHaveTextContent('broker-a, broker-b, broker-c, broker-d');
    fireEvent.blur(targets);
    expect(screen.getByRole('row', { name: /^payments Full page/ })).toHaveTextContent('Yes');

    const clusterFilter = screen.getByRole('combobox', { name: 'Cluster filter' });
    clusterFilter.focus();
    await user.keyboard('{Enter}');
    expect(screen.getByRole('option', { name: 'ArchiveCluster' })).toBeInTheDocument();
    await user.keyboard('{Escape}');
    const brokerFilter = screen.getByRole('combobox', { name: 'Broker filter' });
    brokerFilter.focus();
    await user.keyboard('{Enter}');
    expect(screen.getByRole('option', { name: 'broker-c' })).toBeInTheDocument();
    await user.keyboard('{Escape}');

    await user.click(screen.getByRole('button', { name: 'Message types: All types' }));
    await user.click(screen.getByRole('menuitemcheckbox', { name: 'FIFO' }));
    expect(screen.getByRole('row', { name: /^payments Full page/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /^orders Full page/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Categories: All categories' }));
    await user.click(screen.getByRole('menuitemcheckbox', { name: 'Retry' }));
    expect(screen.getByRole('row', { name: /%RETRY%order-service/ })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(screen.getByRole('row', { name: /^orders Full page/ })).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: 'Full page' })[0]).toHaveAttribute('href', '/topics/orders');
    expect(consumerApi.list).not.toHaveBeenCalled();
  });

  it('synchronizes an open detail with the authoritative catalog after refresh', async () => {
    const user = userEvent.setup();
    const refreshedOrders = {
      ...topics[0],
      brokerName: 'broker-b',
      readQueueCount: 16,
      writeQueueCount: 12,
      perm: 4,
      category: 'UPDATED',
      messageType: 'FIFO',
      order: true
    };
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockResolvedValueOnce({ ...listView, items: [refreshedOrders, ...topics.slice(1)] });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByText('orders'));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    expect(detail).toHaveAccessibleDescription('NORMAL · broker-a');
    fireEvent.click(screen.getByRole('button', { name: 'Refresh', hidden: true }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    expect(detail).toHaveAccessibleDescription('UPDATED · broker-b');
    expect(await within(detail).findByText('UPDATED')).toBeInTheDocument();
    expect(within(detail).getByText('FIFO')).toBeInTheDocument();
    expect(within(detail).getByText('R')).toBeInTheDocument();
    expect(within(detail).getByText('Yes')).toBeInTheDocument();
  });

  it('clamps page state when the catalog shrinks so later growth stays on the valid page', async () => {
    const user = userEvent.setup();
    const largeItems = Array.from({ length: 12 }, (_, index) => ({
      ...topics[0],
      topic: `catalog-topic-${String(index).padStart(2, '0')}`
    }));
    const largeView = { ...listView, items: largeItems, total: largeItems.length };
    const smallView = { ...listView, items: largeItems.slice(0, 5), total: 5 };
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(largeView)
      .mockResolvedValueOnce(smallView)
      .mockResolvedValueOnce(largeView);
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Next page' }));
    expect(screen.getByLabelText('Page 2 of 2')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(await screen.findByLabelText('Page 1 of 1')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Refresh' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(3));
    expect(await screen.findByLabelText('Page 1 of 2')).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /^catalog-topic-00 Full page/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /^catalog-topic-10 Full page/ })).not.toBeInTheDocument();
  });

  it('exposes all seven actions for eligible, retry, and dlq topics while system is view-only', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await openActions(user, 'orders');
    for (const action of [
      'View details', 'Edit configuration', 'Send test message', 'Reset consumer offset',
      'Skip accumulated messages', 'Delete from broker', 'Delete topic'
    ]) expect(screen.getByRole('menuitem', { name: action })).toBeInTheDocument();
    await user.keyboard('{Escape}');

    for (const name of ['%RETRY%order-service', '%DLQ%payment-service']) {
      await openActions(user, name);
      expect(screen.getByRole('menuitem', { name: 'Send test message' })).toBeInTheDocument();
      expect(screen.getByRole('menuitem', { name: 'Delete topic' })).toBeInTheDocument();
      await user.keyboard('{Escape}');
    }

    await openActions(user, 'RMQ_SYS_TRACE_TOPIC');
    expect(screen.getAllByRole('menuitem')).toHaveLength(1);
    expect(screen.getByRole('menuitem', { name: 'View details' })).toBeInTheDocument();
  });

  it('restores focus to the action trigger after View details closes', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    const trigger = screen.getByRole('button', { name: 'Actions for orders' });
    await user.click(trigger);
    await user.click(screen.getByRole('menuitem', { name: 'View details' }));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    await user.click(within(detail).getByRole('button', { name: 'Close details' }));

    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it('loads independent broker config before Edit can save and retries only config discovery', async () => {
    const user = userEvent.setup();
    const pending = deferred<TopicConfigView>();
    vi.mocked(topicApi.config).mockReturnValueOnce(pending.promise);
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', 'Edit configuration');
    expect(topicApi.config).toHaveBeenCalledWith('orders');
    const dialog = screen.getByRole('dialog', { name: 'Edit topic' });
    expect(within(dialog).getByRole('status', { name: 'Loading topic configuration' })).toBeInTheDocument();
    expect(within(dialog).queryByRole('button', { name: 'Save topic' })).not.toBeInTheDocument();

    await act(async () => pending.reject(new Error('orders config unavailable')));
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('orders config unavailable');
    vi.mocked(topicApi.config).mockResolvedValueOnce(config('orders'));
    await user.click(within(dialog).getByRole('button', { name: 'Retry configuration' }));
    expect(await within(dialog).findByRole('textbox', { name: 'Topic name' })).toHaveValue('orders');
    expect(within(dialog).getByRole('button', { name: 'Save topic' })).toBeEnabled();
    expect(topicApi.config).toHaveBeenCalledTimes(2);
  });

  it('drops stale Edit config errors after closing and selecting another topic', async () => {
    const user = userEvent.setup();
    const ordersConfig = deferred<TopicConfigView>();
    vi.mocked(topicApi.config).mockReturnValueOnce(ordersConfig.promise).mockResolvedValueOnce(config('payments', 'broker-b'));
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', 'Edit configuration');
    await user.click(within(screen.getByRole('dialog', { name: 'Edit topic' })).getByRole('button', { name: 'Cancel' }));
    await chooseRowAction(user, 'payments', 'Edit configuration');
    expect(await screen.findByRole('textbox', { name: 'Topic name' })).toHaveValue('payments');
    await act(async () => ordersConfig.reject(new Error('stale orders config failed')));
    expect(screen.queryByText('stale orders config failed')).not.toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Topic name' })).toHaveValue('payments');
  });

  it.each([
    ['Reset consumer offset', 'Reset consumer offset', 'Continue to reset'],
    ['Skip accumulated messages', 'Skip accumulated messages', 'Continue to skip']
  ])('discovers a per-topic group without choosing an arbitrary default for %s', async (menuAction, dialogName, continueLabel) => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', menuAction);
    const chooserName = `Choose consumer group for ${menuAction.toLowerCase()}`;
    const chooser = await screen.findByRole('dialog', { name: chooserName });
    expect(topicApi.consumers).toHaveBeenCalledWith('orders');
    const continueButton = within(chooser).getByRole('button', { name: continueLabel });
    expect(continueButton).toBeDisabled();
    expect(screen.queryByRole('dialog', { name: dialogName })).not.toBeInTheDocument();
    const consumerSelect = within(chooser).getByRole('combobox', { name: 'Consumer group' });
    expect(consumerSelect.tagName).toBe('BUTTON');
    fireEvent.change(chooser.querySelector('select')!, { target: { value: 'audit-service' } });
    await user.click(continueButton);

    const operationDialog = await screen.findByRole('dialog', { name: dialogName });
    expect(within(operationDialog).getByRole('textbox', { name: 'Topic' })).toHaveValue('orders');
    expect(within(operationDialog).getByRole('textbox', { name: 'Consumer group' })).toHaveValue('audit-service');
    expect(consumerApi.list).not.toHaveBeenCalled();
  });

  it('keeps consumer discovery failure and empty feedback scoped to the chooser with correct retries', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.consumers)
      .mockRejectedValueOnce(new Error('orders consumers unavailable'))
      .mockResolvedValueOnce({ items: [] })
      .mockResolvedValueOnce({ items: [{ consumerGroup: 'order-service', totalDiff: 0, inflightDiff: 0, consumeTps: 1 }] });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', 'Reset consumer offset');
    const chooser = await screen.findByRole('dialog', { name: 'Choose consumer group for reset consumer offset' });
    expect(await within(chooser).findByRole('alert')).toHaveTextContent('orders consumers unavailable');
    expect(screen.getByRole('row', { name: /^orders Full page/, hidden: true })).toBeInTheDocument();
    await user.click(within(chooser).getByRole('button', { name: 'Retry consumers' }));
    expect(await within(chooser).findByText('No consumers subscribe to this topic.')).toBeInTheDocument();
    await user.click(within(chooser).getByRole('button', { name: 'Reload consumers' }));
    await waitFor(() => expect(Array.from(chooser.querySelector('select')?.options ?? []).map((option) => option.value))
      .toContain('order-service'));
    expect(topicApi.consumers).toHaveBeenCalledTimes(3);
  });

  it('drops stale consumer discovery errors after another topic is selected', async () => {
    const user = userEvent.setup();
    const ordersConsumers = deferred<{ items: [] }>();
    vi.mocked(topicApi.consumers)
      .mockReturnValueOnce(ordersConsumers.promise)
      .mockResolvedValueOnce({ items: [{ consumerGroup: 'payment-service', totalDiff: 3, inflightDiff: 1, consumeTps: 2 }] });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', 'Skip accumulated messages');
    await user.click(within(screen.getByRole('dialog', { name: 'Choose consumer group for skip accumulated messages' })).getByRole('button', { name: 'Cancel' }));
    await chooseRowAction(user, 'payments', 'Skip accumulated messages');
    const chooser = await screen.findByRole('dialog', { name: 'Choose consumer group for skip accumulated messages' });
    await waitFor(() => expect(Array.from(chooser.querySelector('select')?.options ?? []).map((option) => option.value))
      .toContain('payment-service'));
    await act(async () => ordersConsumers.reject(new Error('stale orders consumers failed')));
    expect(screen.queryByText('stale orders consumers failed')).not.toBeInTheDocument();
    expect(Array.from(chooser.querySelector('select')?.options ?? []).map((option) => option.value))
      .toContain('payment-service');
  });

  it('preserves exact config and consumer identities from the detail callbacks', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByText('orders'));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    await user.click(within(detail).getByRole('tab', { name: 'Configuration' }));
    await user.click(await within(detail).findByRole('button', { name: 'Edit topic' }));
    expect(await screen.findByRole('textbox', { name: 'Topic name' })).toHaveValue('orders');
    expect(topicApi.config).toHaveBeenCalledTimes(1);
    await user.click(within(screen.getByRole('dialog', { name: 'Edit topic' })).getByRole('button', { name: 'Cancel' }));

    await user.click(within(detail).getByRole('tab', { name: 'Consumers' }));
    await user.click(await within(detail).findByRole('button', { name: 'Reset order-service' }));
    const resetDialog = await screen.findByRole('dialog', { name: 'Reset consumer offset' });
    expect(within(resetDialog).getByRole('textbox', { name: 'Consumer group' })).toHaveValue('order-service');
    expect(topicApi.consumers).toHaveBeenCalledTimes(1);
  });

  it('refreshes only config and the catalog after Edit while preserving the detail tab', async () => {
    const user = userEvent.setup();
    const refreshedOrders = { ...topics[0], category: 'EDITED', messageType: 'FIFO' };
    vi.mocked(topicApi.get).mockResolvedValue(refreshedOrders);
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockResolvedValueOnce({ ...listView, items: [refreshedOrders, ...topics.slice(1)] });
    vi.mocked(topicApi.config)
      .mockResolvedValueOnce(config('orders'))
      .mockResolvedValueOnce({ ...config('orders'), messageType: 'FIFO' });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByText('orders'));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    await user.click(within(detail).getByRole('tab', { name: 'Configuration' }));
    await user.click(await within(detail).findByRole('button', { name: 'Edit topic' }));
    const editDialog = await screen.findByRole('dialog', { name: 'Edit topic' });
    await user.click(within(editDialog).getByRole('button', { name: 'Save topic' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Save topic changes?' })).getByRole('button', { name: 'Save changes' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(topicApi.config).toHaveBeenCalledTimes(2));
    expect(within(detail).getByRole('tab', { name: 'Configuration', hidden: true })).toHaveAttribute('aria-selected', 'true');
    expect(topicApi.stats).toHaveBeenCalledTimes(1);
    expect(topicApi.route).not.toHaveBeenCalled();
    expect(topicApi.consumers).not.toHaveBeenCalled();
    await user.click(within(detail).getByRole('tab', { name: 'Overview', hidden: true }));
    expect(detail).toHaveAccessibleDescription('EDITED · broker-a');
    expect(within(detail).getByText('EDITED')).toBeInTheDocument();
  });

  it('refreshes broker-sensitive detail resources and the catalog after broker delete', async () => {
    const user = userEvent.setup();
    const refreshedOrders = { ...topics[0], category: 'BROKER_UPDATED', brokerName: 'broker-b', brokers: ['broker-b'] };
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockResolvedValueOnce({ ...listView, items: [refreshedOrders, ...topics.slice(1)] });
    vi.mocked(topicApi.config).mockResolvedValueOnce(config('orders', 'broker-b'));
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByText('orders'));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    await user.click(within(detail).getByRole('button', { name: 'Delete from broker' }));
    const deleteDialog = await screen.findByRole('alertdialog', { name: 'Delete topic from broker' });
    fireEvent.change(deleteDialog.querySelector('select')!, { target: { value: 'broker-b' } });
    await user.type(within(deleteDialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(deleteDialog).getByRole('button', { name: 'Delete from broker' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(topicApi.stats).toHaveBeenCalledTimes(2));
    expect(topicApi.route).toHaveBeenCalledTimes(1);
    expect(topicApi.config).toHaveBeenCalledTimes(1);
    expect(topicApi.consumers).not.toHaveBeenCalled();
    expect(within(detail).getByRole('tab', { name: 'Overview', hidden: true })).toHaveAttribute('aria-selected', 'true');
    expect(detail).toHaveAccessibleDescription('BROKER_UPDATED · broker-b');
    expect(within(detail).getByText('BROKER_UPDATED')).toBeInTheDocument();
  });

  it('closes the matching detail sheet and removes the row after a successful delete refresh', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockResolvedValueOnce({ ...listView, items: topics.filter((topic) => topic.topic !== 'orders'), total: topics.length - 1 });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });
    await user.click(screen.getByText('orders'));
    expect(await screen.findByRole('dialog', { name: 'orders' })).toBeInTheDocument();

    await user.click(within(screen.getByRole('dialog', { name: 'orders' })).getByRole('button', { name: 'Delete topic' }));
    const deleteDialog = await screen.findByRole('alertdialog', { name: 'Delete topic' });
    await user.type(within(deleteDialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(deleteDialog).getByRole('button', { name: 'Delete topic' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'orders' })).not.toBeInTheDocument());
    expect(screen.queryByRole('row', { name: /^orders Full page/ })).not.toBeInTheDocument();
    expect(screen.getByText('deleted')).toBeInTheDocument();
  });

  it('keeps a partial delete in its dialog while refreshing catalog and matching detail resources once', async () => {
    const user = userEvent.setup();
    const refreshedOrders = { ...topics[0], category: 'PARTIAL_UPDATED', brokers: ['broker-b'] };
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockResolvedValueOnce({ ...listView, items: [refreshedOrders, ...topics.slice(1)] });
    vi.mocked(topicApi.delete).mockResolvedValueOnce({
      operation: 'DELETE_TOPIC',
      topic: 'orders',
      success: false,
      targetCount: 2,
      message: '1 of 2 targets failed',
      targets: [
        { target: 'broker-a', success: true, message: 'deleted from broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByText('orders'));
    const detail = await screen.findByRole('dialog', { name: 'orders' });
    expect(await within(detail).findByRole('group', { name: 'Queue entries: 2' })).toBeInTheDocument();
    await user.click(within(detail).getByRole('button', { name: 'Delete topic' }));
    const deleteDialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    await user.type(within(deleteDialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(deleteDialog).getByRole('button', { name: 'Delete topic' }));

    expect(await within(deleteDialog).findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(within(deleteDialog).getByText('deleted from broker-a')).toBeInTheDocument();
    expect(within(deleteDialog).getByText('broker-b unavailable')).toBeInTheDocument();
    expect(screen.getByRole('alertdialog', { name: 'Delete topic' })).toBeInTheDocument();
    expect(screen.getAllByText('1 of 2 targets failed')).toHaveLength(1);
    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(topicApi.stats).toHaveBeenCalledTimes(2));
    expect(topicApi.route).toHaveBeenCalledTimes(1);
    expect(topicApi.config).toHaveBeenCalledTimes(1);
    expect(topicApi.consumers).not.toHaveBeenCalled();
    expect(within(detail).getByRole('tab', { name: 'Overview', hidden: true })).toHaveAttribute('aria-selected', 'true');
    expect(detail).toHaveAccessibleDescription('PARTIAL_UPDATED · broker-a');
  });

  it('keeps a page-integrated delete locked across close and selection and drops the stale result', async () => {
    const user = userEvent.setup();
    const ordersDelete = deferred<TopicOperationResult>();
    vi.mocked(topicApi.delete)
      .mockReturnValueOnce(ordersDelete.promise)
      .mockResolvedValueOnce(operationResult('DELETE_TOPIC', 'payments', 'payments deleted'));
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await chooseRowAction(user, 'orders', 'Delete topic');
    let dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'orders');
    await user.click(within(dialog).getByRole('button', { name: 'Delete topic' }));
    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    await chooseRowAction(user, 'payments', 'Delete topic');
    dialog = screen.getByRole('alertdialog', { name: 'Delete topic' });
    expect(within(dialog).getByRole('button', { name: 'Deleting' })).toBeDisabled();

    await act(async () => ordersDelete.resolve(operationResult('DELETE_TOPIC', 'orders', 'stale orders deleted')));
    expect(screen.queryByText('stale orders deleted')).not.toBeInTheDocument();
    expect(topicApi.list).toHaveBeenCalledTimes(1);
    await user.type(within(dialog).getByRole('textbox', { name: 'Confirm topic name' }), 'payments');
    await user.click(within(dialog).getByRole('button', { name: 'Delete topic' }));
    expect(topicApi.delete).toHaveBeenLastCalledWith('payments');
  });

  it('keeps partial Create outcomes in the operation dialog without replacing the catalog', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.create).mockResolvedValue({
      operation: 'CREATE', topic: 'partial-topic', success: false, targetCount: 2,
      message: '1 of 2 targets failed',
      targets: [
        { target: 'broker-a', success: true, message: 'created on broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    const dialog = await submitCreate(user, 'partial-topic');
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(screen.getByRole('row', { name: /^orders Full page/, hidden: true })).toBeInTheDocument();
    expect(screen.queryByText('Topic partial-topic created.')).not.toBeInTheDocument();
    expect(topicApi.list).toHaveBeenCalledTimes(2);
  });

  it('blocks duplicate Create names before the endpoint', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });
    const dialog = await submitCreate(user, 'orders');
    expect(await within(dialog).findByRole('alert')).toHaveTextContent('already exists');
    expect(topicApi.create).not.toHaveBeenCalled();
  });
});
