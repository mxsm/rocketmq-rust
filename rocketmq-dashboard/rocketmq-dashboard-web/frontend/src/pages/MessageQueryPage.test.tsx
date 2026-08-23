import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { StrictMode } from 'react';
import { vi } from 'vitest';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import type { MessageView } from '../types/message';
import MessageQueryPage from './MessageQueryPage';

vi.mock('../api/message_api', () => ({
  messageApi: { list: vi.fn(), byKey: vi.fn(), byId: vi.fn(), trace: vi.fn(), resend: vi.fn() }
}));
vi.mock('../api/topic_api', () => ({ topicApi: { list: vi.fn() } }));

const message: MessageView = {
  topic: 'orders', messageId: 'MSG-001', keys: 'order:1', tags: 'TagA', bornTimestamp: 1_723_651_200_000,
  storeTimestamp: 1_723_651_201_000, bornHost: '10.0.0.1:10911', storeHost: '10.0.0.2:10911', queueId: 1,
  queueOffset: 42, storeSize: 128, reconsumeTimes: 0, bodyCRC: 1, sysFlag: 0, flag: 0,
  preparedTransactionOffset: 0, body: 'SECRET-BODY-CONTENT',
  properties: { KEYS: 'order:1', TAGS: 'TagA', STORE_MESSAGE_ID: 'STORE-001' }
};

describe('MessageQueryPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.list).mockResolvedValue({ items: [{ ...message, brokerName: 'broker-a', readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL' }], total: 1 } as never);
    vi.mocked(messageApi.list).mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.byKey).mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.byId).mockResolvedValue({ items: [message], total: 1 });
    vi.mocked(messageApi.trace).mockResolvedValue({ messageId: message.messageId, traceTopic: 'RMQ_SYS_TRACE_TOPIC', nodes: [] });
    vi.mocked(messageApi.resend).mockResolvedValue({
      message: 'Direct consume returned CR_SUCCESS', success: true, consumeResult: 'CR_SUCCESS'
    });
  });

  it('submits only the active query mode and never renders message bodies in the result table', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });

    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await waitFor(() => expect(messageApi.list).toHaveBeenCalledWith(expect.objectContaining({ topic: 'orders', begin: expect.any(Number), end: expect.any(Number) })));
    expect(screen.queryByText('SECRET-BODY-CONTENT')).not.toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'By message key' }));
    await user.type(screen.getByRole('textbox', { name: 'Message key' }), 'order:1');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    expect(messageApi.byKey).toHaveBeenCalledWith('orders', 'order:1');

    await user.click(screen.getByRole('tab', { name: 'By message ID' }));
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'MSG-001');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    expect(messageApi.byId).toHaveBeenCalledWith('orders', 'MSG-001');
    expect(await screen.findByRole('dialog', { name: 'Message detail' })).toBeInTheDocument();
  });

  it('filters and selects the message topic before querying', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list).mockResolvedValue({
      items: [
        { ...message, topic: 'orders', brokerName: 'broker-a' },
        { ...message, topic: 'payment-events', brokerName: 'broker-a' },
        { ...message, topic: 'audit-log', brokerName: 'broker-b' }
      ],
      total: 3
    } as never);

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Message topic' }));
    await user.type(screen.getByRole('textbox', { name: 'Message topic search' }), 'payment');

    const options = screen.getByRole('listbox', { name: 'Message topic' });
    expect(within(options).getByRole('option', { name: 'payment-events' })).toBeInTheDocument();
    expect(within(options).queryByRole('option', { name: 'orders' })).not.toBeInTheDocument();
    await user.click(within(options).getByRole('option', { name: 'payment-events' }));
    expect(screen.getByRole('button', { name: 'Message topic' })).toHaveTextContent('payment-events');

    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await waitFor(() => expect(messageApi.list).toHaveBeenCalledWith(expect.objectContaining({ topic: 'payment-events' })));
  });

  it('validates time ranges and confirms the exact resend request', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    const begin = screen.getByLabelText('Begin time');
    const end = screen.getByLabelText('End time');
    await user.clear(begin);
    await user.type(begin, '2026-08-15T15:00');
    await user.clear(end);
    await user.type(end, '2026-08-15T14:00');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    expect(await screen.findByText('End time must be after begin time.')).toBeInTheDocument();
    expect(messageApi.list).not.toHaveBeenCalled();

    await user.clear(begin);
    await user.type(begin, '2026-08-15T13:00');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    await user.type(within(dialog).getByRole('textbox', { name: 'Client ID optional' }), 'client-a');
    await user.click(within(dialog).getByRole('button', { name: 'Review resend' }));
    expect(messageApi.resend).not.toHaveBeenCalled();
    expect(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByText(/STORE-001.*orders/)).toBeInTheDocument();
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByRole('button', { name: 'Confirm resend' }));
    await waitFor(() => expect(messageApi.resend).toHaveBeenCalledWith('STORE-001', {
      topic: 'orders', consumerGroup: 'order-service', clientId: 'client-a'
    }));
  });

  it('fails closed for a DLQ message without canonical origin metadata', async () => {
    const user = userEvent.setup();
    vi.mocked(messageApi.byId).mockResolvedValue({
      items: [{ ...message, topic: '%DLQ%order-service', messageId: 'DLQ-001', properties: {} }], total: 1
    });

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('tab', { name: 'By message ID' }));
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'DLQ-001');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));

    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    expect(within(dialog).getByText(/Missing RETRY_TOPIC or origin message ID/)).toBeInTheDocument();
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    expect(within(dialog).getByRole('button', { name: 'Review resend' })).toBeDisabled();
    expect(messageApi.resend).not.toHaveBeenCalled();
  });

  it('explains the missing physical message ID for a normal message', async () => {
    const user = userEvent.setup();
    const messageWithoutStoreId = { ...message, properties: { KEYS: 'order:1' } };
    vi.mocked(messageApi.list).mockResolvedValue({ items: [messageWithoutStoreId], total: 1 });

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));

    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    expect(within(dialog).getByText(/Missing STORE_MESSAGE_ID/)).toBeInTheDocument();
    expect(within(dialog).queryByText(/DLQ message/)).not.toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: 'Review resend' })).toBeDisabled();
  });

  it('ignores a search response after an active filter changes', async () => {
    const user = userEvent.setup();
    let resolveList!: (value: { items: MessageView[]; total: number }) => void;
    vi.mocked(messageApi.list).mockReturnValueOnce(new Promise((resolve) => { resolveList = resolve; }));

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.clear(screen.getByLabelText('Begin time'));
    resolveList({ items: [message], total: 1 });

    await waitFor(() => expect(screen.queryByText('MSG-001')).not.toBeInTheDocument());
  });

  it('restores focus to the search action after an ID result auto-opens', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('tab', { name: 'By message ID' }));
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'MSG-001');
    const search = screen.getByRole('button', { name: 'Search messages' });
    await user.click(search);
    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.click(within(dialog).getByRole('button', { name: 'Close details' }));

    await waitFor(() => expect(search).toHaveFocus());
  });

  it('confirms the canonical topic and origin ID for a DLQ resend', async () => {
    const user = userEvent.setup();
    vi.mocked(messageApi.byId).mockResolvedValue({
      items: [{
        ...message, topic: '%DLQ%order-service', messageId: 'DLQ-001',
        properties: { RETRY_TOPIC: 'orders', DLQ_ORIGIN_MESSAGE_ID: 'MSG-001' }
      }], total: 1
    });

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('tab', { name: 'By message ID' }));
    await user.type(screen.getByRole('textbox', { name: 'Message ID' }), 'DLQ-001');
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    await user.click(within(dialog).getByRole('button', { name: 'Review resend' }));

    expect(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByText(/MSG-001.*orders/)).toBeInTheDocument();
  });

  it('does not show a completed resend in a newly selected message context', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: { message: string; success: boolean; consumeResult: string; remark?: string }) => void;
    const secondMessage = {
      ...message,
      messageId: 'MSG-002',
      keys: 'order:2',
      queueOffset: 43,
      properties: { ...message.properties, STORE_MESSAGE_ID: 'STORE-002' }
    };
    vi.mocked(messageApi.list).mockResolvedValue({ items: [message, secondMessage], total: 2 });
    vi.mocked(messageApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    let dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    await user.click(within(dialog).getByRole('button', { name: 'Review resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByRole('button', { name: 'Confirm resend' }));
    await user.click(within(dialog).getByRole('button', { name: 'Close details' }));
    await user.click(await screen.findByRole('row', { name: /MSG-002/ }));
    dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    expect(within(dialog).getByRole('textbox', { name: 'Consumer group' })).toBeDisabled();
    expect(within(dialog).getByRole('button', { name: 'Review resend' })).toBeDisabled();
    expect(messageApi.resend).toHaveBeenCalledTimes(1);
    resolveResend({ message: 'old resend completed', success: true, consumeResult: 'CR_SUCCESS' });

    await waitFor(() => expect(within(dialog).queryByText('old resend completed')).not.toBeInTheDocument());
    await waitFor(() => expect(within(dialog).getByRole('textbox', { name: 'Consumer group' })).toBeEnabled());
  });

  it('recovers topic discovery after a retry', async () => {
    const user = userEvent.setup();
    vi.mocked(topicApi.list)
      .mockRejectedValueOnce(new Error('nameserver unavailable'))
      .mockResolvedValueOnce({ items: [{ ...message, brokerName: 'broker-a' }], total: 1 } as never);

    renderAtRoute(<MessageQueryPage />, '/messages');
    expect(await screen.findByText('Topic discovery failed: nameserver unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry topics' }));

    await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(2));
    expect(screen.getByRole('button', { name: 'Message topic' })).toHaveTextContent('orders');
    expect(screen.queryByText('Topic discovery failed: nameserver unavailable')).not.toBeInTheDocument();
  });

  it('shows a broker CR_LATER outcome as a failed resend', async () => {
    const user = userEvent.setup();
    vi.mocked(messageApi.resend).mockResolvedValue({
      message: 'Direct consume returned CR_LATER', success: false, consumeResult: 'CR_LATER', remark: 'retry later'
    });

    renderAtRoute(<MessageQueryPage />, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    await user.click(within(dialog).getByRole('button', { name: 'Review resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByRole('button', { name: 'Confirm resend' }));

    const notice = await within(dialog).findByText('CR_LATER: retry later');
    expect(notice).toHaveClass('notice-danger');
  });

  it('unlocks resend after completion under React StrictMode', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: { message: string; success: boolean; consumeResult: string }) => void;
    vi.mocked(messageApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));
    renderAtRoute(<StrictMode><MessageQueryPage /></StrictMode>, '/messages');
    await screen.findByRole('heading', { name: 'Message search' });
    await user.click(screen.getByRole('button', { name: 'Search messages' }));
    await user.click(await screen.findByRole('row', { name: /MSG-001/ }));
    const dialog = await screen.findByRole('dialog', { name: 'Message detail' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Consumer group' }), 'order-service');
    await user.click(within(dialog).getByRole('button', { name: 'Review resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend message?' })).getByRole('button', { name: 'Confirm resend' }));
    resolveResend({ message: 'Direct consume returned CR_SUCCESS', success: true, consumeResult: 'CR_SUCCESS' });

    await waitFor(() => expect(within(dialog).getByRole('button', { name: 'Review resend' })).toBeEnabled());
  });
});
