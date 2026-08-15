import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import type { MessageView } from '../types/message';
import type { MessageTraceView } from '../types/message';
import type { TopicInfo, TopicListView } from '../types/topic';
import MessageTracePage from './MessageTracePage';

vi.mock('../api/message_api', () => ({ messageApi: { byKey: vi.fn(), byId: vi.fn(), trace: vi.fn() } }));
vi.mock('../api/topic_api', () => ({ topicApi: { list: vi.fn() } }));

const message: MessageView = {
  topic: 'orders', messageId: 'MSG-001', keys: 'order:1', tags: 'TagA', bornTimestamp: 10, storeTimestamp: 20,
  bornHost: 'born', storeHost: 'stored', queueId: 1, queueOffset: 2, storeSize: 128, reconsumeTimes: 0,
  bodyCRC: 1, sysFlag: 0, flag: 0, preparedTransactionOffset: 0, body: 'secret',
  properties: { STORE_MESSAGE_ID: 'STORE-001' }
};

const topic: TopicInfo = {
  topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'],
  readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL',
  order: false, systemTopic: false
};
const topicList: TopicListView = {
  items: [topic],
  total: 1,
  targets: [{ clusterName: 'DefaultCluster', brokerNames: ['broker-a'] }]
};

describe('MessageTracePage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.list).mockResolvedValue(topicList);
    vi.mocked(messageApi.byId).mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.byKey).mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.trace).mockResolvedValue({
      messageId: 'MSG-001', traceTopic: 'CUSTOM_TRACE',
      nodes: [{ nodeType: 'PRODUCER', name: 'order-producer', status: 'SENT', timestamp: 10 }]
    });
  });

  it('queries candidates by ID or key and forwards the selected trace topic', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MessageTracePage />, '/message-trace');
    await screen.findByRole('heading', { name: 'Message trace' });
    await user.clear(screen.getByRole('textbox', { name: 'Trace topic' }));
    await user.type(screen.getByRole('textbox', { name: 'Trace topic' }), 'CUSTOM_TRACE');
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'MSG-001');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    expect(messageApi.byId).toHaveBeenCalledWith('orders', 'MSG-001');
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    await waitFor(() => expect(messageApi.trace).toHaveBeenCalledWith('STORE-001', 'orders', 'CUSTOM_TRACE'));
    expect(await screen.findByText('order-producer')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'By message key' }));
    await user.type(screen.getByRole('textbox', { name: 'Message key' }), 'order:1');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    expect(messageApi.byKey).toHaveBeenCalledWith('orders', 'order:1');
  });

  it('distinguishes no candidates, zero nodes, and trace failures', async () => {
    const user = userEvent.setup();
    vi.mocked(messageApi.byId)
      .mockResolvedValueOnce({ items: [], total: 0 })
      .mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.trace)
      .mockResolvedValueOnce({ messageId: 'MSG-001', traceTopic: 'RMQ_SYS_TRACE_TOPIC', nodes: [] })
      .mockRejectedValueOnce(new Error('trace backend unavailable'));
    renderAtRoute(<MessageTracePage />, '/message-trace');
    await screen.findByRole('heading', { name: 'Message trace' });
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'MSG-001');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    expect(await screen.findByText('No candidate messages')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    expect(await screen.findByText('No trace nodes')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Reload trace' }));
    expect(await screen.findByText('trace backend unavailable')).toBeInTheDocument();
  });

  it('traces the selected physical message when client identifiers collide', async () => {
    const user = userEvent.setup();
    const olderMessage = {
      ...message,
      keys: 'older-key',
      storeTimestamp: 10,
      storeHost: 'older-broker',
      queueOffset: 1,
      properties: { STORE_MESSAGE_ID: 'STORE-OLDER' }
    };
    vi.mocked(messageApi.byKey).mockResolvedValue({ items: [message, olderMessage], total: 2 });

    renderAtRoute(<MessageTracePage />, '/message-trace');
    await screen.findByRole('heading', { name: 'Message trace' });
    await user.click(screen.getByRole('tab', { name: 'By message key' }));
    await user.type(screen.getByRole('textbox', { name: 'Message key' }), 'order:1');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    const olderCandidate = (await screen.findByText('older-key')).closest('tr');
    expect(olderCandidate).not.toBeNull();
    await user.click(olderCandidate!);

    await waitFor(() => expect(messageApi.trace).toHaveBeenCalledWith(
      'STORE-OLDER', 'orders', 'RMQ_SYS_TRACE_TOPIC'
    ));
  });

  it('ignores a trace response after a new candidate query invalidates the selection', async () => {
    const user = userEvent.setup();
    let resolveTrace!: (value: MessageTraceView) => void;
    vi.mocked(messageApi.trace).mockReturnValueOnce(new Promise((resolve) => { resolveTrace = resolve; }));
    renderAtRoute(<MessageTracePage />, '/message-trace');
    await screen.findByRole('heading', { name: 'Message trace' });
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'MSG-001');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    resolveTrace({
      messageId: 'MSG-001', traceTopic: 'RMQ_SYS_TRACE_TOPIC',
      nodes: [{ nodeType: 'BROKER', name: 'stale-broker', status: 'STORED', timestamp: 20 }]
    });
    await waitFor(() => expect(screen.getByRole('group', { name: 'Trace nodes: 0' })).toBeInTheDocument());
    expect(screen.queryByText('stale-broker')).not.toBeInTheDocument();
  });

  it('ignores a candidate response after an active query field changes', async () => {
    const user = userEvent.setup();
    let resolveCandidates!: (value: { items: MessageView[]; total: number }) => void;
    vi.mocked(messageApi.byId).mockReturnValueOnce(new Promise((resolve) => { resolveCandidates = resolve; }));
    renderAtRoute(<MessageTracePage />, '/message-trace');
    await screen.findByRole('heading', { name: 'Message trace' });
    const messageId = screen.getByRole('textbox', { name: 'Message ID' });
    await user.type(messageId, 'MSG-001');
    await user.click(screen.getByRole('button', { name: 'Find trace candidates' }));
    await user.type(messageId, '-changed');
    resolveCandidates({ items: [message], total: 1 });

    await waitFor(() => expect(screen.queryByText('MSG-001')).not.toBeInTheDocument());
  });

  it('recovers topic discovery after a retry', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list)
      .mockRejectedValueOnce(new Error('nameserver unavailable'))
      .mockResolvedValueOnce(topicList);

    renderAtRoute(<MessageTracePage />, '/message-trace');
    expect(await screen.findByText('Topic discovery failed: nameserver unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry topics' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    expect(screen.getByRole('combobox', { name: 'Message topic' })).toHaveValue('orders');
    expect(screen.queryByText('Topic discovery failed: nameserver unavailable')).not.toBeInTheDocument();
  });
});
