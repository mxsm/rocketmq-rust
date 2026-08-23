import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from '@testing-library/react';
import { vi } from 'vitest';
import { configApi } from '../../api/config_api';
import type { DashboardConfigView } from '../../types/config';
import { ConsumerQueryScopeProvider, useConsumerQueryScope } from './ConsumerQueryScopeProvider';

vi.mock('../../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));

const configured: DashboardConfigView = {
  environmentId: 'environment-default',
  environmentName: 'Default',
  revision: 3,
  endpoints: [
    { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
    { endpointId: 'proxy-a', endpointType: 'proxy', address: 'proxy-a:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }
  ],
  currentNamesrv: '127.0.0.1:9876',
  namesrvAddrList: ['127.0.0.1:9876'],
  useVIPChannel: false,
  useTLS: false,
  currentProxyAddr: 'proxy-a:8081',
  proxyAddrList: ['proxy-a:8081'],
  storageBackend: 'sqlite',
  storageMode: 'singleNode'
};

function ScopeProbe() {
  const { scope, configLoading, proxyAvailable, setMode } = useConsumerQueryScope();
  return (
    <div>
      <output data-testid="scope">{scope.mode}{scope.proxyAddress ? `:${scope.proxyAddress}` : ''}</output>
      <output data-testid="loading">{String(configLoading)}</output>
      <output data-testid="available">{String(proxyAvailable)}</output>
      <button type="button" onClick={() => setMode('proxy')}>Use proxy</button>
      <button type="button" onClick={() => setMode('nameServer')}>Use nameserver</button>
    </div>
  );
}

describe('ConsumerQueryScopeProvider', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('derives the proxy scope from the configured current endpoint', async () => {
    vi.mocked(configApi.getConfig).mockResolvedValue(configured);
    render(<ConsumerQueryScopeProvider><ScopeProbe /></ConsumerQueryScopeProvider>);

    await waitFor(() => expect(screen.getByTestId('loading')).toHaveTextContent('false'));
    expect(screen.getByTestId('scope')).toHaveTextContent('nameServer');
    expect(screen.getByTestId('available')).toHaveTextContent('true');
  });

  it('persists the selected mode and resolves the proxy address', async () => {
    vi.mocked(configApi.getConfig).mockResolvedValue(configured);
    const user = userEvent.setup();
    render(<ConsumerQueryScopeProvider><ScopeProbe /></ConsumerQueryScopeProvider>);
    await waitFor(() => expect(screen.getByTestId('loading')).toHaveTextContent('false'));

    await user.click(screen.getByRole('button', { name: 'Use proxy' }));
    expect(screen.getByTestId('scope')).toHaveTextContent('proxy:proxy-a:8081');
    expect(window.localStorage.getItem('rocketmq.consumer.queryMode')).toBe('proxy');
  });

  it('fails closed when proxy mode has no current endpoint', async () => {
    vi.mocked(configApi.getConfig).mockResolvedValue({
      ...configured,
      endpoints: configured.endpoints.filter((endpoint) => endpoint.endpointType !== 'proxy')
    });
    render(<ConsumerQueryScopeProvider><ScopeProbe /></ConsumerQueryScopeProvider>);

    await waitFor(() => expect(screen.getByTestId('loading')).toHaveTextContent('false'));
    expect(screen.getByTestId('available')).toHaveTextContent('false');
  });

  it('refreshes endpoint scope after a persisted configuration update event', async () => {
    vi.mocked(configApi.getConfig)
      .mockResolvedValueOnce(configured)
      .mockResolvedValueOnce({
        ...configured,
        revision: 4,
        endpoints: [
          ...configured.endpoints.filter((endpoint) => endpoint.endpointType !== 'proxy'),
          { endpointId: 'proxy-b', endpointType: 'proxy', address: 'proxy-b:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }
        ]
      });
    const user = userEvent.setup();
    render(<ConsumerQueryScopeProvider><ScopeProbe /></ConsumerQueryScopeProvider>);
    await waitFor(() => expect(screen.getByTestId('loading')).toHaveTextContent('false'));
    await user.click(screen.getByRole('button', { name: 'Use proxy' }));
    expect(screen.getByTestId('scope')).toHaveTextContent('proxy:proxy-a:8081');

    act(() => { window.dispatchEvent(new CustomEvent('rocketmq-config-updated')); });

    await waitFor(() => expect(screen.getByTestId('scope')).toHaveTextContent('proxy:proxy-b:8081'));
    expect(configApi.getConfig).toHaveBeenCalledTimes(2);
  });
});
