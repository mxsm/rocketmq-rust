import { screen } from '@testing-library/react';
import { Route, Routes } from 'react-router-dom';
import { vi } from 'vitest';
import { configApi } from '../api/config_api';
import { consumerApi } from '../api/consumer_api';
import { renderAtRoute } from '../test/render';
import { ConsumerQueryScopeProvider } from './consumers/ConsumerQueryScopeProvider';
import ConsumerDetailPage from './ConsumerDetailPage';

vi.mock('../api/consumer_api', () => ({
  consumerApi: { summary: vi.fn(), progress: vi.fn(), resetOffset: vi.fn() }
}));
vi.mock('../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));

describe('ConsumerDetailPage', () => {
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
      consumeTps: 0,
      diffTotal: 12,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 0,
      queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service',
      topicCount: 0,
      totalDiff: 12,
      topics: [],
      queryScope: { mode: 'nameServer' }
    });
  });

  it('resolves the group and renders the workspace on a direct route', async () => {
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );

    expect(await screen.findByRole('heading', { name: 'order-service' })).toBeInTheDocument();
    expect(await screen.findByRole('tab', { name: 'Overview' })).toBeInTheDocument();
  });

  it('opens the requested tab from the query string', async () => {
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service?tab=progress'
    );

    expect(await screen.findByRole('heading', { name: 'order-service' })).toBeInTheDocument();
    expect(await screen.findByRole('tab', { name: 'Progress' })).toBeInTheDocument();
  });
});
