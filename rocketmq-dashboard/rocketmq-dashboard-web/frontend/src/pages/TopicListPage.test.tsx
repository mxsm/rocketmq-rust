import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import type { TopicInfo, TopicListView, TopicOperationResult } from '../types/topic';
import TopicListPage from './TopicListPage';

vi.mock('../api/topic_api', () => ({
  topicApi: {
    list: vi.fn(),
    get: vi.fn(),
    create: vi.fn(),
    update: vi.fn(),
    delete: vi.fn(),
    route: vi.fn(),
    stats: vi.fn()
  }
}));

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    list: vi.fn(),
    progress: vi.fn(),
    resetOffset: vi.fn()
  }
}));

const topics: TopicInfo[] = [
  { topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'], readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL', order: false, systemTopic: false },
  { topic: 'payments', brokerName: 'broker-b', brokers: ['broker-b'], clusters: ['DefaultCluster'], readQueueCount: 4, writeQueueCount: 4, perm: 4, category: 'NORMAL', messageType: 'FIFO', order: true, systemTopic: false },
  { topic: '%RETRY%order-service', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 2, category: 'RETRY', messageType: 'RETRY', order: false, systemTopic: false },
  { topic: '%DLQ%payment-service', brokerName: 'broker-b', brokers: ['broker-b'], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 0, category: 'DLQ', messageType: 'DLQ', order: false, systemTopic: false },
  { topic: 'RMQ_SYS_TRACE_TOPIC', brokerName: null, brokers: [], clusters: ['DefaultCluster'], readQueueCount: 1, writeQueueCount: 1, perm: 7, category: 'SYSTEM', messageType: 'SYSTEM', order: false, systemTopic: true }
];

const listView: TopicListView = {
  items: topics,
  total: topics.length,
  targets: [{ clusterName: 'DefaultCluster', brokerNames: ['broker-a', 'broker-b'] }]
};

const operationResult = (operation: string, topic: string, message: string): TopicOperationResult => ({
  operation,
  topic,
  success: true,
  targetCount: 1,
  message,
  targets: [{ target: 'broker-a', success: true, message }]
});

describe('TopicListPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.list).mockResolvedValue(listView);
    vi.mocked(topicApi.create).mockResolvedValue(operationResult('CREATE', 'inventory-events', 'created'));
    vi.mocked(topicApi.update).mockResolvedValue(operationResult('UPDATE', 'orders', 'updated'));
    vi.mocked(topicApi.delete).mockResolvedValue(operationResult('DELETE_TOPIC', 'orders', 'deleted'));
    vi.mocked(topicApi.stats).mockResolvedValue({
      topic: 'orders', queueCount: 2, totalMessageCount: 8_280,
      totalMinOffset: 120, totalMaxOffset: 8_400, offsets: []
    });
    vi.mocked(topicApi.route).mockResolvedValue({ topic: 'orders', brokers: [], queues: [] });
    vi.mocked(consumerApi.list).mockResolvedValue({ items: [], total: 0 });
  });

  it('renders inventory metrics and combines topic filters', async () => {
    const user = userEvent.setup();
    let resolveList: (value: TopicListView) => void = () => undefined;
    vi.mocked(topicApi.list).mockReturnValueOnce(new Promise((resolve) => { resolveList = resolve; }));
    renderAtRoute(<TopicListPage />, '/topics');

    expect(screen.getByRole('status', { name: 'Loading topics' })).toBeInTheDocument();
    resolveList(listView);

    expect(await screen.findByRole('heading', { name: 'Topics' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Total topics: 5' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Application: 2' })).toBeInTheDocument();

    await user.selectOptions(screen.getByRole('combobox', { name: 'Category filter' }), 'retry');
    expect(screen.getByRole('row', { name: /%RETRY%order-service/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /^orders Full page/ })).not.toBeInTheDocument();

    await user.selectOptions(screen.getByRole('combobox', { name: 'Broker filter' }), 'broker-a');
    await user.type(screen.getByRole('searchbox', { name: 'Filter topics' }), 'order');
    await waitFor(() => expect(screen.getByRole('row', { name: /%RETRY%order-service/ })).toBeInTheDocument());

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(screen.getByRole('row', { name: /^orders Full page.*NORMAL/ })).toBeInTheDocument();
  });

  it('opens reusable topic details from a table row and restores focus on close', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    const row = screen.getByRole('row', { name: /^orders Full page/ });
    await user.click(screen.getByText('orders'));

    expect(await screen.findByRole('dialog', { name: 'orders' })).toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Queue entries: 2' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(row).toHaveFocus());
  });

  it('creates topics and does not expose unsafe partial edit actions', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Create topic' }));
    const createDialog = screen.getByRole('dialog', { name: 'Create topic' });
    await user.type(within(createDialog).getByRole('textbox', { name: 'Topic name' }), 'inventory-events');
    await user.type(within(createDialog).getByRole('textbox', { name: 'Cluster names' }), 'DefaultCluster');
    await user.click(within(createDialog).getByRole('button', { name: 'Save topic' }));
    const createConfirmation = screen.getByRole('alertdialog', { name: 'Create topic?' });
    await user.click(within(createConfirmation).getByRole('button', { name: 'Create topic' }));

    await waitFor(() => expect(topicApi.create).toHaveBeenCalledWith(expect.objectContaining({ topic: 'inventory-events' })));
    expect(topicApi.update).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    expect(screen.queryByRole('menuitem', { name: 'Edit topic' })).not.toBeInTheDocument();
  });

  it('blocks a duplicate topic name before calling the create endpoint', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Create topic' }));
    const createDialog = screen.getByRole('dialog', { name: 'Create topic' });
    await user.type(within(createDialog).getByRole('textbox', { name: 'Topic name' }), 'orders');
    await user.type(within(createDialog).getByRole('textbox', { name: 'Cluster names' }), 'DefaultCluster');
    await user.click(within(createDialog).getByRole('button', { name: 'Save topic' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Create topic?' })).getByRole('button', { name: 'Create topic' }));

    expect(await within(createDialog).findByText('Topic `orders` already exists. Choose a new name.')).toBeInTheDocument();
    expect(topicApi.create).not.toHaveBeenCalled();
  });

  it('requires explicit confirmation before deleting a topic', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    await user.click(screen.getByRole('menuitem', { name: 'Delete topic' }));
    let confirmation = screen.getByRole('alertdialog', { name: 'Delete topic?' });
    await user.click(within(confirmation).getByRole('button', { name: 'Cancel' }));
    expect(topicApi.delete).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    await user.click(screen.getByRole('menuitem', { name: 'Delete topic' }));
    confirmation = screen.getByRole('alertdialog', { name: 'Delete topic?' });
    await user.click(within(confirmation).getByRole('button', { name: 'Delete topic' }));

    await waitFor(() => expect(topicApi.delete).toHaveBeenCalledWith('orders'));
    expect(await screen.findByText('Topic orders deleted.')).toBeInTheDocument();
  });

  it('surfaces consumer target load errors and disables offset reset until retry succeeds', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.list)
      .mockRejectedValueOnce(new Error('consumer targets unavailable'))
      .mockResolvedValueOnce({
        items: [{ group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 0 }],
        total: 1
      });
    renderAtRoute(<TopicListPage />, '/topics');

    expect(await screen.findByRole('heading', { name: 'Topics' })).toBeInTheDocument();
    expect(await screen.findByText('consumer targets unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    expect(screen.getByRole('menuitem', { name: 'Reset offsets' })).toHaveAttribute('aria-disabled', 'true');
    await user.keyboard('{Escape}');

    await user.click(screen.getByRole('button', { name: 'Retry consumer groups' }));
    await waitFor(() => expect(consumerApi.list).toHaveBeenCalledTimes(2));
    expect(screen.queryByText('consumer targets unavailable')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    expect(screen.getByRole('menuitem', { name: 'Reset offsets' })).not.toHaveAttribute('aria-disabled');
  });

  it('keeps offset reset disabled when the consumer target inventory is empty', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');

    expect(await screen.findByText('No consumer groups are available for offset reset.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload consumer groups' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    expect(screen.getByRole('menuitem', { name: 'Reset offsets' })).toHaveAttribute('aria-disabled', 'true');
  });

  it('marks the page refresh busy while consumer targets are still loading', async () => {
    let resolveConsumers: (value: { items: []; total: number }) => void = () => undefined;
    vi.mocked(consumerApi.list).mockReturnValueOnce(new Promise((resolve) => { resolveConsumers = resolve; }));
    renderAtRoute(<TopicListPage />, '/topics');

    await screen.findByRole('heading', { name: 'Topics' });
    expect(screen.getByRole('button', { name: 'Refreshing' })).toBeDisabled();
    resolveConsumers({ items: [], total: 0 });
    expect(await screen.findByRole('button', { name: 'Refresh' })).toBeEnabled();
  });
});
