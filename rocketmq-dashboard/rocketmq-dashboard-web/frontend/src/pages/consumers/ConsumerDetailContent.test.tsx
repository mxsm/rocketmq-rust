import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { configApi } from '../../api/config_api';
import { consumerApi } from '../../api/consumer_api';
import { render } from '@testing-library/react';
import { ConsumerQueryScopeProvider } from './ConsumerQueryScopeProvider';
import ConsumerDetailContent from './ConsumerDetailContent';

vi.mock('../../api/consumer_api', () => ({
  consumerApi: { summary: vi.fn(), progress: vi.fn(), resetOffset: vi.fn() }
}));
vi.mock('../../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));

function renderContent(group: string, initialTab?: 'overview' | 'progress' | 'reset') {
  return render(
    <ConsumerQueryScopeProvider>
      <ConsumerDetailContent group={group} initialTab={initialTab} />
    </ConsumerQueryScopeProvider>
  );
}

describe('ConsumerDetailContent', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      currentNamesrv: '127.0.0.1:9876',
      namesrvAddrList: ['127.0.0.1:9876'],
      useVIPChannel: false,
      useTLS: false,
      currentProxyAddr: null,
      proxyAddrList: [],
      storageBackend: 'sqlite'
    });
    vi.mocked(consumerApi.summary).mockResolvedValue({
      group: 'order-service',
      displayGroupName: 'order-service',
      category: 'NORMAL',
      connectionCount: 3,
      consumeTps: 10,
      diffTotal: 12,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: 530,
      versionDesc: 'V5_3_0',
      brokerNames: ['broker-a'],
      brokerAddresses: ['10.0.0.1:10911'],
      updateTimestamp: 1_700_000_000_000,
      queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service',
      topicCount: 1,
      totalDiff: 12,
      topics: [
        {
          topic: 'orders',
          diffTotal: 12,
          lastTimestamp: 1_700_000_000_000,
          queues: [
            { brokerName: 'broker-a', queueId: 0, brokerOffset: 100, consumerOffset: 88, diffTotal: 12, clientInfo: '10.0.0.8@client-a', lastTimestamp: 1_700_000_000_000 }
          ]
        }
      ],
      queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.resetOffset).mockResolvedValue({ message: 'reset' });
  });

  it('renders overview metrics and grouped progress', async () => {
    const user = userEvent.setup();
    renderContent('order-service');

    expect(await screen.findByRole('tab', { name: 'Overview' })).toBeInTheDocument();
    expect(screen.getByText('CLUSTERING')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Progress' }));
    expect(await screen.findByText('orders')).toBeInTheDocument();
    expect(screen.getByText('broker-a')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
  });

  it('requires a valid timestamp before reset review', async () => {
    const user = userEvent.setup();
    renderContent('order-service');
    await screen.findByRole('tab', { name: 'Overview' });

    await user.click(screen.getByRole('tab', { name: 'Reset offset' }));
    await user.clear(screen.getByRole('spinbutton', { name: 'Reset timestamp' }));
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    expect(await screen.findByRole('alert')).toBeInTheDocument();
    expect(consumerApi.resetOffset).not.toHaveBeenCalled();
  });
});
