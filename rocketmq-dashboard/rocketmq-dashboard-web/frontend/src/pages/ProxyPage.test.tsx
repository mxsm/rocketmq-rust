import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { configApi } from '../api/config_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import type { DashboardConfigView } from '../types/config';
import ProxyPage from './ProxyPage';

vi.mock('../api/config_api', () => ({
  configApi: {
    getConfig: vi.fn(),
    addProxy: vi.fn(),
    switchProxy: vi.fn(),
    deleteProxy: vi.fn()
  }
}));

const initialConfig: DashboardConfigView = {
  environmentId: 'environment-default',
  environmentName: 'Default',
  revision: 9,
  endpoints: [
    { endpointId: 'proxy-a', endpointType: 'proxy', address: 'proxy-a:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
    { endpointId: 'proxy-b', endpointType: 'proxy', address: 'proxy-b:8081', role: 'secondary', isEnabled: true, isActive: false, sortOrder: 1 }
  ],
  namesrvAddrList: [],
  currentNamesrv: null,
  useVIPChannel: false,
  useTLS: false,
  proxyAddrList: ['proxy-a:8081', 'proxy-b:8081'],
  currentProxyAddr: 'proxy-a:8081',
  storageBackend: 'file',
  storageMode: 'singleNode'
};

const mockedConfigApi = vi.mocked(configApi);

function configMutation(config: DashboardConfigView, message: string) {
  return { config, message };
}

describe('ProxyPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockedConfigApi.getConfig.mockResolvedValue(initialConfig);
    mockedConfigApi.addProxy.mockImplementation(({ address }) => Promise.resolve(configMutation({
      ...initialConfig,
      proxyAddrList: [...initialConfig.proxyAddrList, address]
    }, 'Proxy endpoint added.')));
    mockedConfigApi.switchProxy.mockImplementation(({ endpointId }) => Promise.resolve(configMutation({
      ...initialConfig,
      endpoints: initialConfig.endpoints.map((endpoint) => ({
        ...endpoint,
        isActive: endpoint.endpointId === endpointId,
        role: endpoint.endpointId === endpointId ? 'primary' : 'secondary'
      }))
    }, 'Current proxy updated.')));
    mockedConfigApi.deleteProxy.mockImplementation((endpointId) => Promise.resolve(configMutation({
      ...initialConfig,
      endpoints: initialConfig.endpoints.filter((endpoint) => endpoint.endpointId !== endpointId)
    }, 'Proxy endpoint deleted.')));
  });

  it('adds a normalized endpoint and prevents a normalized duplicate before persistence', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    const input = within(dialog).getByLabelText('Proxy address');
    await user.type(input, ' proxy-c:8081 ');
    await user.click(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    await waitFor(() => expect(mockedConfigApi.addProxy).toHaveBeenCalledWith({ address: 'proxy-c:8081', expectedRevision: 9 }));
    expect(onConfigUpdated).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'Add endpoint' }));
    const duplicateDialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    await user.type(within(duplicateDialog).getByLabelText('Proxy address'), ' PROXY-A:+08081 ');
    await user.click(within(duplicateDialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(await within(duplicateDialog).findByRole('alert')).toHaveTextContent('This proxy endpoint is already configured.');
    expect(mockedConfigApi.addProxy).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });

  it('keeps focus on the endpoint field when a blank add is rejected', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    const input = within(dialog).getByLabelText('Proxy address');
    await user.click(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('Enter a proxy endpoint.');
    expect(input).toHaveFocus();
    expect(mockedConfigApi.addProxy).not.toHaveBeenCalled();
  });

  it('keeps focus on the endpoint field after an add request is rejected', async () => {
    const user = userEvent.setup();
    mockedConfigApi.addProxy.mockRejectedValueOnce(new Error('Proxy endpoint was rejected.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    const input = within(dialog).getByLabelText('Proxy address');
    await user.type(input, 'proxy-c:8081');
    await user.click(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('Proxy endpoint was rejected.');
    expect(input).toHaveFocus();
    expect(mockedConfigApi.addProxy).toHaveBeenCalledWith({ address: 'proxy-c:8081', expectedRevision: 9 });
  });

  it('switches the current endpoint through the exact config API and reports the returned state', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProxyPage />, '/proxy');

    await screen.findByText('proxy-b:8081');
    await user.click(screen.getByRole('button', { name: 'Set current proxy proxy-b:8081' }));

    await waitFor(() => expect(mockedConfigApi.switchProxy).toHaveBeenCalledWith({ endpointId: 'proxy-b', expectedRevision: 9 }));
    expect(await screen.findByText('Current proxy updated.')).toBeInTheDocument();
    expect(screen.getByRole('status', { name: 'Current' })).toBeInTheDocument();
  });

  it('does not call the delete API when proxy deletion is cancelled', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Delete proxy proxy-b:8081' }));
    const confirmation = screen.getByRole('alertdialog');
    expect(confirmation).toHaveTextContent('Delete proxy proxy-b:8081?');
    await user.click(within(confirmation).getByRole('button', { name: 'Cancel' }));

    expect(mockedConfigApi.deleteProxy).not.toHaveBeenCalled();
  });

  it('deletes the selected endpoint only after confirmation with the exact config API argument', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Delete proxy proxy-b:8081' }));
    const confirmation = screen.getByRole('alertdialog');
    await user.click(within(confirmation).getByRole('button', { name: 'Delete proxy' }));

    await waitFor(() => expect(mockedConfigApi.deleteProxy).toHaveBeenCalledWith('proxy-b', 9));
    expect(await screen.findByText('Proxy endpoint deleted.')).toBeInTheDocument();
  });

  it('uses the API response as the source of truth when deleting the current endpoint', async () => {
    const user = userEvent.setup();
    mockedConfigApi.getConfig.mockResolvedValue({
      ...initialConfig,
      endpoints: [initialConfig.endpoints[0]],
      proxyAddrList: ['proxy-a:8081'],
      currentProxyAddr: 'proxy-a:8081'
    });
    mockedConfigApi.deleteProxy.mockResolvedValue(configMutation({
      ...initialConfig,
      endpoints: [],
      proxyAddrList: [],
      currentProxyAddr: null
    }, 'Proxy endpoint deleted.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Delete proxy proxy-a:8081' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete proxy' }));

    expect(await screen.findByText('No current endpoint')).toBeInTheDocument();
    expect(screen.queryByRole('status', { name: 'Current' })).not.toBeInTheDocument();
  });

  it('submits one add request while the first request is in flight', async () => {
    const user = userEvent.setup();
    let resolveAdd: ((value: ReturnType<typeof configMutation>) => void) | undefined;
    mockedConfigApi.addProxy.mockImplementationOnce(() => new Promise((resolve) => {
      resolveAdd = resolve;
    }));
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    await user.type(within(dialog).getByLabelText('Proxy address'), 'proxy-c:8081');
    await user.dblClick(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(mockedConfigApi.addProxy).toHaveBeenCalledTimes(1);
    resolveAdd?.(configMutation({ ...initialConfig, proxyAddrList: [...initialConfig.proxyAddrList, 'proxy-c:8081'] }, 'Proxy endpoint added.'));
    expect(await screen.findByText('Proxy endpoint added.')).toBeInTheDocument();
  });

  it('preserves an add draft and tells the operator to refresh before retrying a conflict', async () => {
    const user = userEvent.setup();
    mockedConfigApi.addProxy.mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Configuration revision is stale.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    const input = within(dialog).getByLabelText('Proxy address');
    await user.type(input, 'proxy-c:8081');
    await user.click(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('latest configuration revision is loaded');
    expect(input).toHaveValue('proxy-c:8081');
    expect(mockedConfigApi.addProxy).toHaveBeenCalledWith({ address: 'proxy-c:8081', expectedRevision: 9 });
  });

  it('loads the authoritative revision after an add conflict and retries only when the operator asks', async () => {
    const user = userEvent.setup();
    const refreshedConfig = { ...initialConfig, revision: 14 };
    mockedConfigApi.getConfig
      .mockResolvedValueOnce(initialConfig)
      .mockResolvedValueOnce(refreshedConfig);
    mockedConfigApi.addProxy
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Configuration revision is stale.'))
      .mockResolvedValueOnce(configMutation(refreshedConfig, 'Proxy endpoint added.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    await user.click(await screen.findByRole('button', { name: 'Add endpoint' }));
    const dialog = screen.getByRole('dialog', { name: 'Add proxy endpoint' });
    const input = within(dialog).getByLabelText('Proxy address');
    await user.type(input, 'proxy-c:8081');
    await user.click(within(dialog).getByRole('button', { name: 'Add proxy endpoint' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('latest configuration revision is loaded');
    expect(input).toHaveValue('proxy-c:8081');
    expect(mockedConfigApi.addProxy).toHaveBeenCalledTimes(1);

    await user.click(within(dialog).getByRole('button', { name: 'Retry add proxy endpoint' }));
    await waitFor(() => expect(mockedConfigApi.addProxy).toHaveBeenLastCalledWith({
      address: 'proxy-c:8081', expectedRevision: 14
    }));
  });

  it('refreshes an authoritative revision after a switch conflict and retries only on the next operator action', async () => {
    const user = userEvent.setup();
    const refreshedConfig = { ...initialConfig, revision: 14 };
    mockedConfigApi.getConfig
      .mockResolvedValueOnce(initialConfig)
      .mockResolvedValueOnce(refreshedConfig);
    mockedConfigApi.switchProxy
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Configuration revision is stale.'))
      .mockResolvedValueOnce(configMutation({
        ...refreshedConfig,
        endpoints: refreshedConfig.endpoints.map((endpoint) => ({
          ...endpoint,
          isActive: endpoint.endpointId === 'proxy-b',
          role: endpoint.endpointId === 'proxy-b' ? 'primary' : 'secondary'
        }))
      }, 'Current proxy updated.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    const switchButton = await screen.findByRole('button', { name: 'Set current proxy proxy-b:8081' });
    await user.click(switchButton);

    expect(await screen.findByText(/latest configuration revision is loaded/i)).toBeInTheDocument();
    expect(mockedConfigApi.switchProxy).toHaveBeenCalledTimes(1);
    expect(switchButton).toHaveTextContent('Retry set current');

    await user.click(switchButton);
    await waitFor(() => expect(mockedConfigApi.switchProxy).toHaveBeenLastCalledWith({ endpointId: 'proxy-b', expectedRevision: 14 }));
  });

  it('refreshes an authoritative revision after a delete conflict and requires a new explicit confirmation', async () => {
    const user = userEvent.setup();
    const refreshedConfig = { ...initialConfig, revision: 14 };
    mockedConfigApi.getConfig
      .mockResolvedValueOnce(initialConfig)
      .mockResolvedValueOnce(refreshedConfig);
    mockedConfigApi.deleteProxy
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Configuration revision is stale.'))
      .mockResolvedValueOnce(configMutation({
        ...refreshedConfig,
        endpoints: refreshedConfig.endpoints.filter((endpoint) => endpoint.endpointId !== 'proxy-b')
      }, 'Proxy endpoint deleted.'));
    renderAtRoute(<ProxyPage />, '/proxy');

    const deleteButton = await screen.findByRole('button', { name: 'Delete proxy proxy-b:8081' });
    await user.click(deleteButton);
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete proxy' }));

    expect(await screen.findByText(/latest configuration revision is loaded/i)).toBeInTheDocument();
    expect(mockedConfigApi.deleteProxy).toHaveBeenCalledTimes(1);
    expect(deleteButton).toHaveTextContent('Retry delete');

    await user.click(deleteButton);
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Retry delete proxy' }));
    await waitFor(() => expect(mockedConfigApi.deleteProxy).toHaveBeenLastCalledWith('proxy-b', 14));
  });
});
