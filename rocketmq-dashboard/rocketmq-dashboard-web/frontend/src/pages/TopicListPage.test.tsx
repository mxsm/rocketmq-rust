import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { topicApi } from '../api/topic_api';
import { deferred } from '../test/deferred';
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

async function submitCreate(user: ReturnType<typeof userEvent.setup>, topic: string) {
  await user.click(screen.getByRole('button', { name: 'Create topic' }));
  const createDialog = screen.getByRole('dialog', { name: 'Create topic' });
  await user.type(within(createDialog).getByRole('textbox', { name: 'Topic name' }), topic);
  await user.click(within(createDialog).getByRole('checkbox', { name: 'DefaultCluster' }));
  await user.click(within(createDialog).getByRole('button', { name: 'Save topic' }));
  await user.click(within(screen.getByRole('alertdialog', { name: 'Create topic?' })).getByRole('button', { name: 'Create topic' }));
  return createDialog;
}

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
    await user.click(within(createDialog).getByRole('checkbox', { name: 'DefaultCluster' }));
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
    await user.click(within(createDialog).getByRole('checkbox', { name: 'DefaultCluster' }));
    await user.click(within(createDialog).getByRole('button', { name: 'Save topic' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Create topic?' })).getByRole('button', { name: 'Create topic' }));

    expect(await within(createDialog).findByText('Topic `orders` already exists. Choose a new name.')).toBeInTheDocument();
    expect(topicApi.create).not.toHaveBeenCalled();
  });

  it('keeps a partial Create result in the dialog without a global success notice', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.create).mockResolvedValue({
      operation: 'CREATE',
      topic: 'partial-topic',
      success: false,
      targetCount: 2,
      message: '1 of 2 targets failed',
      targets: [
        { target: 'broker-a', success: true, message: 'created on broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Create topic' }));
    const createDialog = screen.getByRole('dialog', { name: 'Create topic' });
    await user.type(within(createDialog).getByRole('textbox', { name: 'Topic name' }), 'partial-topic');
    await user.click(within(createDialog).getByRole('checkbox', { name: 'DefaultCluster' }));
    await user.click(within(createDialog).getByRole('button', { name: 'Save topic' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Create topic?' })).getByRole('button', { name: 'Create topic' }));

    expect(await within(createDialog).findByRole('alert')).toHaveTextContent('1 of 2 targets failed');
    expect(within(createDialog).getByText('created on broker-a')).toBeInTheDocument();
    expect(within(createDialog).getByText('broker-b unavailable')).toBeInTheDocument();
    expect(screen.getByRole('dialog', { name: 'Create topic' })).toBeInTheDocument();
    expect(screen.queryByText('Topic partial-topic created.')).not.toBeInTheDocument();
  });

  it('presents a partial Create result without waiting for the catalog refresh', async () => {
    const user = userEvent.setup();
    const refresh = deferred<TopicListView>();
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockReturnValueOnce(refresh.promise);
    vi.mocked(topicApi.create).mockResolvedValue({
      operation: 'CREATE',
      topic: 'immediate-partial',
      success: false,
      targetCount: 2,
      message: '1 of 2 targets failed immediately',
      targets: [
        { target: 'broker-a', success: true, message: 'created immediately' },
        { target: 'broker-b', success: false, message: 'broker-b still unavailable' }
      ]
    });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    const createDialog = await submitCreate(user, 'immediate-partial');

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    expect(await within(createDialog).findByRole('alert')).toHaveTextContent('1 of 2 targets failed immediately');
    expect(within(createDialog).getByText('created immediately')).toBeInTheDocument();
    expect(within(createDialog).getByRole('button', { name: 'Save topic' })).toBeEnabled();
  });

  it('keeps the valid catalog and partial result when its background refresh fails', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockRejectedValueOnce(new Error('topic catalog refresh unavailable'));
    vi.mocked(topicApi.create).mockResolvedValue({
      operation: 'CREATE',
      topic: 'refresh-failure-partial',
      success: false,
      targetCount: 2,
      message: 'partial result survives refresh failure',
      targets: [
        { target: 'broker-a', success: true, message: 'created on broker-a' },
        { target: 'broker-b', success: false, message: 'broker-b unavailable' }
      ]
    });
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    const createDialog = await submitCreate(user, 'refresh-failure-partial');

    expect(await within(createDialog).findByRole('alert')).toHaveTextContent('partial result survives refresh failure');
    expect(await screen.findByText('topic catalog refresh unavailable')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Topics', hidden: true })).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /^orders Full page/, hidden: true })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry topic catalog', hidden: true })).toBeInTheDocument();
    await user.click(within(createDialog).getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByRole('button', { name: 'Retry topic catalog' }));
    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(3));
    expect(screen.queryByText('topic catalog refresh unavailable')).not.toBeInTheDocument();
    expect(screen.getByRole('row', { name: /^orders Full page/ })).toBeInTheDocument();
  });

  it('ignores an older catalog response after a newer refresh finishes', async () => {
    const user = userEvent.setup();
    const olderRefresh = deferred<TopicListView>();
    const newerRefresh = deferred<TopicListView>();
    const staleView: TopicListView = {
      items: [{ ...topics[0], topic: 'stale-catalog-topic' }],
      total: 1,
      targets: listView.targets
    };
    const currentView: TopicListView = {
      items: [{ ...topics[0], topic: 'current-catalog-topic' }],
      total: 1,
      targets: listView.targets
    };
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockReturnValueOnce(olderRefresh.promise)
      .mockReturnValueOnce(newerRefresh.promise);
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
    await user.click(screen.getByRole('menuitem', { name: 'Delete topic' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Delete topic?' })).getByRole('button', { name: 'Delete topic' }));
    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(3));

    await act(async () => newerRefresh.resolve(currentView));
    expect(await screen.findByRole('row', { name: /^current-catalog-topic Full page/ })).toBeInTheDocument();
    await act(async () => olderRefresh.resolve(staleView));
    expect(screen.getByRole('row', { name: /^current-catalog-topic Full page/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /^stale-catalog-topic Full page/ })).not.toBeInTheDocument();
  });

  it('closes a successful Create once and preserves its notice when the catalog refresh fails', async () => {
    const user = userEvent.setup();
    const refresh = deferred<TopicListView>();
    vi.mocked(topicApi.list)
      .mockResolvedValueOnce(listView)
      .mockReturnValueOnce(refresh.promise);
    vi.mocked(topicApi.create).mockResolvedValue(operationResult('CREATE', 'successful-topic', 'created'));
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });

    await submitCreate(user, 'successful-topic');

    expect(await screen.findByText('Topic successful-topic created.')).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Create topic' })).not.toBeInTheDocument());
    expect(screen.getAllByText('Topic successful-topic created.')).toHaveLength(1);
    await act(async () => refresh.reject(new Error('post-create catalog refresh failed')));
    expect(await screen.findByText('post-create catalog refresh failed')).toBeInTheDocument();
    expect(screen.getAllByText('Topic successful-topic created.')).toHaveLength(1);
    expect(screen.queryByRole('dialog', { name: 'Create topic' })).not.toBeInTheDocument();
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
