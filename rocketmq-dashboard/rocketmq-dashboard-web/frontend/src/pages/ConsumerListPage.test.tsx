import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { configApi } from '../api/config_api';
import { consumerApi } from '../api/consumer_api';
import { renderAtRoute } from '../test/render';
import type { ConsumerGroupListItem } from '../types/consumer';
import { ConsumerQueryScopeProvider } from './consumers/ConsumerQueryScopeProvider';
import ConsumerListPage from './ConsumerListPage';

vi.mock('../api/consumer_api', () => ({ consumerApi: { list: vi.fn() } }));
vi.mock('../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));

const consumer = (overrides: Partial<ConsumerGroupListItem> = {}): ConsumerGroupListItem => ({
  displayGroupName: 'order-service',
  rawGroupName: 'order-service',
  category: 'NORMAL',
  connectionCount: 6,
  consumeTps: 120,
  diffTotal: 8_700,
  messageModel: 'MESSAGE_MODEL_CLUSTERING',
  consumeType: 'CONSUME_PASSIVELY',
  version: 530,
  versionDesc: 'V5_3_0',
  brokerNames: ['broker-a'],
  brokerAddresses: ['10.0.0.1:10911'],
  updateTimestamp: 1_700_000_000_000,
  ...overrides
});

function renderPage() {
  return renderAtRoute(
    <ConsumerQueryScopeProvider><ConsumerListPage /></ConsumerQueryScopeProvider>,
    '/consumers'
  );
}

describe('ConsumerListPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      environmentId: 'environment-default',
      environmentName: 'Default',
      revision: 1,
      endpoints: [{ endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }],
      currentNamesrv: '127.0.0.1:9876',
      namesrvAddrList: ['127.0.0.1:9876'],
      useVIPChannel: false,
      useTLS: false,
      currentProxyAddr: null,
      proxyAddrList: [],
      storageBackend: 'sqlite',
      storageMode: 'singleNode'
    });
    vi.mocked(consumerApi.list).mockResolvedValue({
      items: [
        consumer(),
        consumer({ rawGroupName: 'payment-broadcast', displayGroupName: 'payment-broadcast', diffTotal: 0, messageModel: 'MESSAGE_MODEL_BROADCASTING' }),
        consumer({ rawGroupName: 'audit-puller', displayGroupName: 'audit-puller', consumeType: 'CONSUME_ACTIVELY', connectionCount: 0, diffTotal: 25 }),
        ...Array.from({ length: 8 }, (_, index) => consumer({ rawGroupName: `worker-${index}`, displayGroupName: `worker-${index}`, diffTotal: 0 }))
      ],
      total: 11,
      queryScope: { mode: 'nameServer' },
      capabilities: { connections: true, progress: true, configuration: true, runningInfo: true, jstack: true }
    });
  });

  it('renders enriched inventory columns, filters, and pagination', async () => {
    const user = userEvent.setup();
    renderPage();

    expect(await screen.findByRole('heading', { name: 'Consumer groups' })).toBeInTheDocument();
    for (const header of ['Consumer group', 'Category', 'Connections', 'Version', 'Consume type', 'Message model', 'TPS', 'Total lag', 'Targets', 'Updated', 'Actions']) {
      expect(screen.getByRole('columnheader', { name: header })).toBeInTheDocument();
    }

    await user.selectOptions(screen.getByRole('combobox', { name: 'Consume type filter' }), 'ACTIVELY');
    expect(screen.getByRole('row', { name: /audit-puller/ })).toBeInTheDocument();
    expect(screen.queryByRole('row', { name: /order-service/ })).not.toBeInTheDocument();
  });

  it('opens the full workspace from a row action link', async () => {
    renderPage();
    await screen.findByRole('heading', { name: 'Consumer groups' });
    const hrefs = screen.getAllByRole('link', { name: 'Open workspace' }).map((link) => link.getAttribute('href'));
    expect(hrefs).toContain('/consumers/order-service');
  });

  it('shows a retryable list error and refreshes', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.list)
      .mockRejectedValueOnce(new Error('consumer service unavailable'))
      .mockResolvedValue({
        items: [consumer()], total: 1,
        queryScope: { mode: 'nameServer' },
        capabilities: { connections: true, progress: true, configuration: true, runningInfo: true, jstack: true }
      });
    renderPage();

    expect(await screen.findByText('consumer service unavailable')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByRole('heading', { name: 'Consumer groups' })).toBeInTheDocument();
  });
});
