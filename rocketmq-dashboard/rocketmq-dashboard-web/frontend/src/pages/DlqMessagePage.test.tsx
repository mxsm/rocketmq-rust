import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { StrictMode } from 'react';
import { vi } from 'vitest';
import { consumerApi } from '../api/consumer_api';
import { dlqApi } from '../api/dlq_api';
import { renderAtRoute } from '../test/render';
import type { MessageView } from '../types/message';
import DlqMessagePage from './DlqMessagePage';

vi.mock('../api/consumer_api', () => ({ consumerApi: { list: vi.fn() } }));
vi.mock('../api/dlq_api', () => ({ dlqApi: { list: vi.fn(), resend: vi.fn(), export: vi.fn() } }));

const dlqMessage: MessageView = {
  topic: '%DLQ%order-service', messageId: 'DLQ-001', keys: 'order:1', tags: 'TagA', bornTimestamp: 1_723_651_200_000,
  storeTimestamp: 1_723_651_201_000, bornHost: '10.0.0.1:10911', storeHost: '10.0.0.2:10911', queueId: 1,
  queueOffset: 42, storeSize: 128, reconsumeTimes: 16, bodyCRC: 1, sysFlag: 0, flag: 0,
  preparedTransactionOffset: 0, body: 'SECRET-DLQ-BODY', properties: { RETRY_TOPIC: 'orders', ORIGIN_MESSAGE_ID: 'MSG-001' }
};

const firstDlqPage = Array.from({ length: 20 }, (_, index) => ({
  ...dlqMessage,
  messageId: index === 0 ? dlqMessage.messageId : `DLQ-${String(index + 1).padStart(3, '0')}`,
  queueOffset: dlqMessage.queueOffset + index
}));

describe('DlqMessagePage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [{ group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 }], total: 1
    });
    vi.mocked(dlqApi.list)
      .mockResolvedValueOnce({ items: firstDlqPage, total: 20 })
      .mockResolvedValue({ items: [{ ...dlqMessage, messageId: 'DLQ-021', queueOffset: 62 }], total: 21 });
    vi.mocked(dlqApi.resend).mockResolvedValue([{ msgId: 'MSG-001', success: true, consumeResult: 'CR_SUCCESS' }]);
    vi.mocked(dlqApi.export).mockResolvedValue({ fileName: 'order-service-dlq.csv', rows: [dlqMessage], csv: 'messageId\nDLQ-001' });
    Object.defineProperty(URL, 'createObjectURL', { value: vi.fn(() => 'blob:dlq'), configurable: true });
    Object.defineProperty(URL, 'revokeObjectURL', { value: vi.fn(), configurable: true });
  });

  it('forwards server pagination exactly and resets page-local selection', async () => {
    const user = userEvent.setup();
    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await waitFor(() => expect(dlqApi.list).toHaveBeenCalledWith(expect.objectContaining({ consumerGroup: 'order-service', pageNum: 1, pageSize: 20 })));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    expect(screen.getByText('1 selected')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Next page' }));
    await waitFor(() => expect(dlqApi.list).toHaveBeenCalledWith(expect.objectContaining({ pageNum: 2, pageSize: 20 })));
    expect(screen.getByText('0 selected')).toBeInTheDocument();
    expect(await screen.findByText('DLQ-021')).toBeInTheDocument();
  });

  it('confirms batch resend, shows per-message outcomes, and exports the server response', async () => {
    const user = userEvent.setup();
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    expect(dlqApi.resend).not.toHaveBeenCalled();
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));

    await waitFor(() => expect(dlqApi.resend).toHaveBeenCalledWith({ messages: [{
      topicName: 'orders', consumerGroup: 'order-service', msgId: 'MSG-001', clientId: undefined
    }] }));
    expect(await screen.findByText('MSG-001: CR_SUCCESS')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Export current query' }));
    await waitFor(() => expect(dlqApi.export).toHaveBeenCalledWith(expect.objectContaining({ consumerGroup: 'order-service', pageNum: 1, pageSize: 20 })));
    expect(URL.createObjectURL).toHaveBeenCalledWith(expect.any(Blob));
    expect(anchorClick).toHaveBeenCalled();
    anchorClick.mockRestore();
  });

  it('clears rows and page-local selection when the consumer group changes', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [
        { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 },
        { group: 'payment-consumer', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 0 }
      ], total: 2
    });
    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Consumer group' }), 'payment-consumer');
    expect(screen.getByText('0 selected')).toBeInTheDocument();
    expect(screen.queryByText('DLQ-001')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Review selected resend' })).toBeDisabled();
  });

  it('re-fetches the previous page when an optimistic next page is empty', async () => {
    const user = userEvent.setup();
    vi.mocked(dlqApi.list)
      .mockReset()
      .mockResolvedValueOnce({ items: firstDlqPage, total: 20 })
      .mockResolvedValueOnce({ items: [], total: 20 })
      .mockResolvedValueOnce({ items: firstDlqPage, total: 20 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('button', { name: 'Next page' }));

    await waitFor(() => expect(dlqApi.list).toHaveBeenNthCalledWith(3, expect.objectContaining({ pageNum: 1, pageSize: 20 })));
    expect(await screen.findByText('DLQ-001')).toBeInTheDocument();
    expect(screen.getByLabelText('Page 1 of 1')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();
  });

  it('retries the exact page that failed to load', async () => {
    const user = userEvent.setup();
    vi.mocked(dlqApi.list)
      .mockReset()
      .mockResolvedValueOnce({ items: firstDlqPage, total: 20 })
      .mockRejectedValueOnce(new Error('page 2 unavailable'))
      .mockResolvedValueOnce({ items: [{ ...dlqMessage, messageId: 'DLQ-021', queueOffset: 62 }], total: 21 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('button', { name: 'Next page' }));
    expect(await screen.findByText('page 2 unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));

    await waitFor(() => expect(dlqApi.list).toHaveBeenNthCalledWith(3, expect.objectContaining({ pageNum: 2 })));
    expect(await screen.findByText('DLQ-021')).toBeInTheDocument();
  });

  it('shows every exact-ID match as one non-paginated result set', async () => {
    const user = userEvent.setup();
    const duplicateMatches = Array.from({ length: 25 }, (_, index) => ({
      ...dlqMessage,
      queueOffset: dlqMessage.queueOffset + index
    }));
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: duplicateMatches, total: 25 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.type(screen.getByRole('textbox', { name: 'Message ID optional' }), 'DLQ-001');
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));

    expect(await screen.findAllByRole('checkbox', { name: 'Select DLQ-001' })).toHaveLength(25);
    expect(screen.getByLabelText('Page 1 of 1')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();
    expect(dlqApi.list).toHaveBeenLastCalledWith(expect.objectContaining({ messageId: 'DLQ-001', pageNum: 1 }));
  });

  it('ignores an in-flight response after the consumer group changes', async () => {
    const user = userEvent.setup();
    let resolveList!: (value: { items: MessageView[]; total: number }) => void;
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [
        { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 },
        { group: 'payment-consumer', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 0 }
      ], total: 2
    });
    vi.mocked(dlqApi.list).mockReset().mockReturnValue(new Promise((resolve) => { resolveList = resolve; }));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Consumer group' }), 'payment-consumer');
    resolveList({ items: [dlqMessage], total: 1 });

    await waitFor(() => expect(screen.getByText('0 selected')).toBeInTheDocument());
    expect(screen.queryByText('DLQ-001')).not.toBeInTheDocument();
  });

  it('disables resend when canonical origin metadata is missing', async () => {
    const user = userEvent.setup();
    const unsafeMessage = { ...dlqMessage, properties: {} };
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: [unsafeMessage], total: 1 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));

    expect(await screen.findByText('Unsafe metadata')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'Select DLQ-001' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Review selected resend' })).toBeDisabled();
    expect(dlqApi.resend).not.toHaveBeenCalled();
  });

  it('renders failed resend outcomes with their safe backend remark', async () => {
    const user = userEvent.setup();
    vi.mocked(dlqApi.resend).mockResolvedValue([{
      msgId: 'MSG-001', success: false, consumeResult: 'FAILED', remark: 'BACKEND: DLQ resend request failed'
    }]);

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));

    const outcome = await screen.findByText('MSG-001: FAILED');
    expect(outcome.closest('li')).toHaveClass('is-danger');
    expect(screen.getByText('BACKEND: DLQ resend request failed')).toBeInTheDocument();
  });

  it('does not show a completed batch after the consumer group changes', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: Array<{ msgId: string; success: boolean; consumeResult: string; remark?: string }>) => void;
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [
        { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 },
        { group: 'payment-consumer', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 0 }
      ], total: 2
    });
    vi.mocked(dlqApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Consumer group' }), 'payment-consumer');
    resolveResend([{ msgId: 'MSG-001', success: true, consumeResult: 'CR_SUCCESS', remark: 'old batch completed' }]);

    await waitFor(() => expect(screen.queryByText('old batch completed')).not.toBeInTheDocument());
  });

  it('keeps the destructive batch locked until an invalidated request settles', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: Array<{ msgId: string; success: boolean; consumeResult: string }>) => void;
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: [dlqMessage], total: 1 });
    vi.mocked(dlqApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await screen.findByRole('checkbox', { name: 'Select DLQ-001' });

    expect(screen.getByRole('button', { name: 'Review selected resend' })).toBeDisabled();
    expect(dlqApi.resend).toHaveBeenCalledTimes(1);
    resolveResend([{ msgId: 'MSG-001', success: true, consumeResult: 'CR_SUCCESS' }]);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review selected resend' })).toBeEnabled());
  });

  it('drops pending outcomes when the selected canonical target changes', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: Array<{ msgId: string; success: boolean; consumeResult: string; remark?: string }>) => void;
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: [dlqMessage], total: 1 });
    vi.mocked(dlqApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    const checkbox = await screen.findByRole('checkbox', { name: 'Select DLQ-001' });
    await user.click(checkbox);
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    await user.click(checkbox);
    resolveResend([{ msgId: 'MSG-001', success: true, consumeResult: 'CR_SUCCESS', remark: 'old target completed' }]);

    await waitFor(() => expect(screen.queryByText('old target completed')).not.toBeInTheDocument());
  });

  it('deduplicates selected envelopes that map to the same original message', async () => {
    const user = userEvent.setup();
    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('checkbox', { name: 'Select DLQ-002' }));

    expect(screen.getByText('1 selected')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    await waitFor(() => expect(dlqApi.resend).toHaveBeenCalledWith({ messages: [{
      topicName: 'orders', consumerGroup: 'order-service', msgId: 'MSG-001', clientId: undefined
    }] }));
  });

  it('selects and resends only one physical row when client message identifiers collide', async () => {
    const user = userEvent.setup();
    const collidingRows = [
      { ...dlqMessage, properties: { RETRY_TOPIC: 'orders', ORIGIN_MESSAGE_ID: 'MSG-001' } },
      {
        ...dlqMessage,
        storeHost: '10.0.0.3:10911',
        queueOffset: 43,
        properties: { RETRY_TOPIC: 'payments', ORIGIN_MESSAGE_ID: 'MSG-002' }
      }
    ];
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: collidingRows, total: 2 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    const checkboxes = await screen.findAllByRole('checkbox', { name: 'Select DLQ-001' });
    await user.click(checkboxes[0]);

    expect(checkboxes[0]).toBeChecked();
    expect(checkboxes[1]).not.toBeChecked();
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    await waitFor(() => expect(dlqApi.resend).toHaveBeenCalledWith({ messages: [{
      topicName: 'orders', consumerGroup: 'order-service', msgId: 'MSG-001', clientId: undefined
    }] }));
  });

  it('recovers consumer-group discovery after a retry', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.list)
      .mockRejectedValueOnce(new Error('nameserver unavailable'))
      .mockResolvedValueOnce({
        items: [{ group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 }], total: 1
      });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    expect(await screen.findByText('Consumer-group discovery failed: nameserver unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry consumer groups' }));

    await waitFor(() => expect(consumerApi.list).toHaveBeenCalledTimes(2));
    expect(screen.getByRole('combobox', { name: 'Consumer group' })).toHaveValue('order-service');
    expect(screen.queryByText('Consumer-group discovery failed: nameserver unavailable')).not.toBeInTheDocument();
  });

  it('does not download an export after the query context changes', async () => {
    const user = userEvent.setup();
    let resolveExport!: (value: { fileName: string; rows: MessageView[]; csv: string }) => void;
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [
        { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 1 },
        { group: 'payment-consumer', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 1, diffTotal: 0 }
      ], total: 2
    });
    vi.mocked(dlqApi.export).mockReturnValueOnce(new Promise((resolve) => { resolveExport = resolve; }));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Export current query' }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Consumer group' }), 'payment-consumer');
    resolveExport({ fileName: 'old-group.csv', rows: [dlqMessage], csv: 'messageId\nDLQ-001' });

    await waitFor(() => expect(screen.getByRole('button', { name: 'Export current query' })).not.toBeDisabled());
    expect(anchorClick).not.toHaveBeenCalled();
    anchorClick.mockRestore();
  });

  it('re-enables optimistic paging when a refreshed full page may have new rows', async () => {
    const user = userEvent.setup();
    const secondPage = Array.from({ length: 20 }, (_, index) => ({
      ...dlqMessage,
      messageId: `DLQ-${String(index + 21).padStart(3, '0')}`,
      queueOffset: dlqMessage.queueOffset + index + 20
    }));
    vi.mocked(dlqApi.list)
      .mockReset()
      .mockResolvedValueOnce({ items: firstDlqPage, total: 20 })
      .mockResolvedValueOnce({ items: secondPage, total: 40 })
      .mockResolvedValueOnce({ items: [], total: 40 })
      .mockResolvedValueOnce({ items: secondPage, total: 40 })
      .mockResolvedValueOnce({ items: secondPage, total: 40 });

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('button', { name: 'Next page' }));
    expect(await screen.findByText('DLQ-021')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Next page' }));

    await waitFor(() => expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled());
    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() => expect(dlqApi.list).toHaveBeenCalledTimes(5));
    expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled();
  });

  it('keeps visible rows and offers the correct retry after an export failure', async () => {
    const user = userEvent.setup();
    vi.mocked(dlqApi.export).mockRejectedValueOnce(new Error('export backend unavailable'));

    renderAtRoute(<DlqMessagePage />, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    expect(await screen.findByText('DLQ-001')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Export current query' }));

    expect(await screen.findByText('export backend unavailable')).toBeInTheDocument();
    expect(screen.getByText('DLQ-001')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry export' })).toBeInTheDocument();
  });

  it('unlocks batch resend after completion under React StrictMode', async () => {
    const user = userEvent.setup();
    let resolveResend!: (value: Array<{ msgId: string; success: boolean; consumeResult: string }>) => void;
    vi.mocked(dlqApi.list).mockReset().mockResolvedValue({ items: [dlqMessage], total: 1 });
    vi.mocked(dlqApi.resend).mockReturnValueOnce(new Promise((resolve) => { resolveResend = resolve; }));
    renderAtRoute(<StrictMode><DlqMessagePage /></StrictMode>, '/messages/dlq');
    await screen.findByRole('heading', { name: 'Dead-letter messages' });
    await user.click(screen.getByRole('button', { name: 'Search DLQ messages' }));
    await user.click(await screen.findByRole('checkbox', { name: 'Select DLQ-001' }));
    await user.click(screen.getByRole('button', { name: 'Review selected resend' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Resend selected DLQ messages?' })).getByRole('button', { name: 'Confirm resend' }));
    resolveResend([{ msgId: 'MSG-001', success: true, consumeResult: 'CR_SUCCESS' }]);

    await waitFor(() => expect(screen.getByRole('button', { name: 'Review selected resend' })).toBeEnabled());
  });
});
