import { fireEvent, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { configApi } from '../../api/config_api';
import { consumerApi } from '../../api/consumer_api';
import { render } from '@testing-library/react';
import { ConsumerQueryScopeProvider } from './ConsumerQueryScopeProvider';
import ConsumerDetailContent from './ConsumerDetailContent';

vi.mock('../../api/consumer_api', () => ({
  consumerApi: { summary: vi.fn(), progress: vi.fn(), config: vi.fn(), resetOffset: vi.fn() }
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
    vi.mocked(consumerApi.config).mockResolvedValue({
      group: 'order-service',
      effective: {
        consumeEnable: true,
        consumeFromMinEnable: true,
        consumeBroadcastEnable: false,
        consumeMessageOrderly: false,
        retryQueueNums: 1,
        retryMaxTimes: 16,
        brokerId: 0,
        whichBrokerWhenConsumeSlowly: 1,
        notifyConsumerIdsChangedEnable: true,
        groupSysFlag: 0,
        consumeTimeoutMinute: 15,
        groupRetryPolicyJson: '{"retryPolicy":{"type":"CUSTOMIZED","next":[1000,5000]}}'
      },
      inconsistentFields: [],
      targets: [
        {
          brokerName: 'broker-a',
          brokerAddress: '10.0.0.1:10911',
          config: {
            consumeEnable: true,
            consumeFromMinEnable: true,
            consumeBroadcastEnable: false,
            consumeMessageOrderly: false,
            retryQueueNums: 1,
            retryMaxTimes: 16,
            brokerId: 0,
            whichBrokerWhenConsumeSlowly: 1,
            notifyConsumerIdsChangedEnable: true,
            groupSysFlag: 0,
            consumeTimeoutMinute: 15,
            groupRetryPolicyJson: '{"retryPolicy":{"type":"CUSTOMIZED","next":[1000,5000]}}'
          },
          subscriptionTopics: ['orders'],
          attributes: []
        }
      ],
      queryScope: { mode: 'nameServer' }
    });
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
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Select a valid reset time.');
    expect(consumerApi.resetOffset).not.toHaveBeenCalled();
  });

  it('resets to an explicit local time and explains that current time skips replay', async () => {
    const user = userEvent.setup();
    const resetTimestamp = new Date(2026, 7, 15, 10, 30, 0, 0).getTime();
    renderContent('order-service', 'reset');

    expect(await screen.findByText(/Selecting the current time moves the group to the queue tail/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
    await user.click(screen.getByRole('button', { name: 'Review reset' }));
    expect(screen.getByRole('alertdialog')).toHaveTextContent(new Date(resetTimestamp).toLocaleString());
    await user.click(screen.getByRole('button', { name: 'Confirm reset' }));

    await waitFor(() => expect(consumerApi.resetOffset).toHaveBeenCalledWith('order-service', {
      topic: 'orders',
      resetTimestamp,
      force: false
    }));
  });

  it('groups broker configuration and keeps the retry policy collapsed until requested', async () => {
    const user = userEvent.setup();
    renderContent('order-service');

    await user.click(await screen.findByRole('tab', { name: 'Configuration' }));

    expect(await screen.findByRole('heading', { name: 'Effective settings' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'broker-a' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Consumption' })).toBeInTheDocument();
    expect(screen.getByText('CUSTOMIZED · 2 retry intervals')).toBeInTheDocument();
    const retryPolicy = screen.getByText('Retry policy').closest('details');
    expect(retryPolicy).not.toHaveAttribute('open');

    await user.click(screen.getByText('Retry policy'));
    expect(retryPolicy).toHaveAttribute('open');
    expect(screen.getByText(/"retryPolicy"/)).toBeInTheDocument();
  });
});
