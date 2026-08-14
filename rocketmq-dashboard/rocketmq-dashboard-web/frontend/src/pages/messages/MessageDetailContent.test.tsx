import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { messageApi } from '../../api/message_api';
import type { MessageView } from '../../types/message';
import MessageDetailContent from './MessageDetailContent';

vi.mock('../../api/message_api', () => ({
  messageApi: { trace: vi.fn() }
}));

const message: MessageView = {
  topic: 'orders', messageId: 'MSG-001-LONG-IDENTIFIER', keys: 'order:1', tags: 'TagA',
  bornTimestamp: 1_723_651_200_000, storeTimestamp: 1_723_651_201_000,
  bornHost: '10.0.0.1:10911', storeHost: '10.0.0.2:10911', queueId: 3, queueOffset: 42,
  storeSize: 1_536, reconsumeTimes: 2, bodyCRC: 1, sysFlag: 0, flag: 0,
  preparedTransactionOffset: 0, body: '{"orderId":1289347}',
  properties: { WAIT: 'true', KEYS: 'order:1', TAGS: 'TagA', STORE_MESSAGE_ID: 'STORE-001' }
};

describe('MessageDetailContent', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(messageApi.trace).mockResolvedValue({
      messageId: message.messageId,
      traceTopic: 'RMQ_SYS_TRACE_TOPIC',
      nodes: [{ nodeType: 'BROKER', name: 'broker-a', status: 'STORED', timestamp: 1_723_651_201_000 }]
    });
  });

  it('keeps the body in the selected detail, sorts properties, copies identifiers, and loads trace once', async () => {
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, 'writeText');
    render(<MessageDetailContent message={message} />);

    expect(screen.getByRole('group', { name: 'Body size: 1.5 KB' })).toBeInTheDocument();
    expect(screen.queryByText('1289347')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Copy message ID' }));
    expect(writeText).toHaveBeenCalledWith(message.messageId);
    expect(await screen.findByText('Message ID copied.')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Properties' }));
    expect(screen.getAllByRole('row').slice(1).map((row) => row.textContent)).toEqual([
      'KEYSorder:1', 'STORE_MESSAGE_IDSTORE-001', 'TAGSTagA', 'WAITtrue'
    ]);

    await user.click(screen.getByRole('tab', { name: 'Body' }));
    expect(screen.getByText(/"orderId": 1289347/)).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Trace' }));
    expect(await screen.findByText('broker-a')).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'Overview' }));
    await user.click(screen.getByRole('tab', { name: 'Trace' }));
    await waitFor(() => expect(messageApi.trace).toHaveBeenCalledTimes(1));
    expect(messageApi.trace).toHaveBeenCalledWith('STORE-001', message.topic, 'RMQ_SYS_TRACE_TOPIC');
  });

  it('announces clipboard failures as errors', async () => {
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, 'writeText').mockRejectedValueOnce(new Error('clipboard denied'));
    render(<MessageDetailContent message={message} />);

    await user.click(screen.getByRole('button', { name: 'Copy message ID' }));

    const notice = await screen.findByRole('alert');
    expect(notice).toHaveTextContent('Unable to copy the message ID.');
    expect(notice).toHaveClass('notice-danger');
  });
});
