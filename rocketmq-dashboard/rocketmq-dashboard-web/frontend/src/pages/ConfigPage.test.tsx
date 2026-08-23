import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { configApi } from '../api/config_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import type { DashboardConfigView, NameserverAvailabilityView } from '../types/config';
import ConfigPage from './ConfigPage';

vi.mock('../api/config_api', () => ({
  configApi: {
    getConfig: vi.fn(),
    getNameserverAvailability: vi.fn(),
    replaceNameservers: vi.fn(),
    addNameserver: vi.fn(),
    switchNameserver: vi.fn(),
    deleteNameserver: vi.fn(),
    setVipChannel: vi.fn(),
    setTls: vi.fn(),
    addProxy: vi.fn(),
    switchProxy: vi.fn(),
    deleteProxy: vi.fn()
  }
}));

const initialConfig: DashboardConfigView = {
  environmentId: 'environment-default',
  environmentName: 'Default',
  revision: 7,
  endpoints: [
    { endpointId: 'nameserver-a', endpointType: 'nameserver', address: '10.0.0.10:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
    { endpointId: 'nameserver-b', endpointType: 'nameserver', address: '10.0.0.11:9876', role: 'secondary', isEnabled: true, isActive: false, sortOrder: 1 }
  ],
  namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
  currentNamesrv: '10.0.0.10:9876',
  useVIPChannel: false,
  useTLS: false,
  proxyAddrList: [],
  currentProxyAddr: null,
  storageBackend: 'file',
  storageMode: 'singleNode'
};

const initialAvailability: NameserverAvailabilityView = {
  endpoints: [
    { address: '10.0.0.10:9876', status: 'available', checkedAt: 1_700_000_000_000 },
    { address: '10.0.0.11:9876', status: 'unavailable', checkedAt: 1_700_000_000_000 }
  ]
};

const mockedConfigApi = vi.mocked(configApi);

function mutationResult(config = initialConfig, message = 'Configuration updated') {
  return Promise.resolve({ config, message });
}

describe('ConfigPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockedConfigApi.getConfig.mockResolvedValue(initialConfig);
    mockedConfigApi.getNameserverAvailability.mockResolvedValue(initialAvailability);
    mockedConfigApi.replaceNameservers.mockImplementation((request) => mutationResult({
      ...initialConfig,
      namesrvAddrList: request.namesrvAddrList,
      currentNamesrv: request.currentNamesrv
    }, 'NameServers updated'));
    mockedConfigApi.addNameserver.mockResolvedValue(awaitableMutation());
    mockedConfigApi.switchNameserver.mockImplementation(({ endpointId }) => mutationResult({
      ...initialConfig,
      endpoints: initialConfig.endpoints.map((endpoint) => ({
        ...endpoint,
        isActive: endpoint.endpointId === endpointId,
        role: endpoint.endpointId === endpointId ? 'primary' : 'secondary'
      })),
      currentNamesrv: initialConfig.endpoints.find((endpoint) => endpoint.endpointId === endpointId)?.address ?? null
    }, 'NameServer switched'));
    mockedConfigApi.deleteNameserver.mockResolvedValue(awaitableMutation());
    mockedConfigApi.setVipChannel.mockImplementation(({ enabled }) => mutationResult({ ...initialConfig, useVIPChannel: enabled }, 'VIP updated'));
    mockedConfigApi.setTls.mockImplementation(({ enabled }) => mutationResult({ ...initialConfig, useTLS: enabled }, 'TLS updated'));
  });

  it('switches the selected NameServer by endpoint identifier and signals successful persistence', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);

    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');
    expect(screen.getByText('Active endpoint change pending')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Apply active endpoint' }));

    await waitFor(() => expect(mockedConfigApi.switchNameserver).toHaveBeenCalledWith({ endpointId: 'nameserver-b', expectedRevision: 7 }));
    expect(onConfigUpdated).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });

  it('only shows active endpoint actions while the selection has unsaved changes', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    expect(screen.queryByRole('button', { name: 'Apply active endpoint' })).not.toBeInTheDocument();
    expect(screen.queryByText('Configuration is current')).not.toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');

    const pendingStatus = screen.getByText('Active endpoint change pending').closest('[role="status"]');
    expect(pendingStatus).not.toBeNull();
    expect(within(pendingStatus as HTMLElement).getByText('10.0.0.10:9876')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Apply active endpoint' })).toBeEnabled();

    await user.click(screen.getByRole('button', { name: 'Discard change' }));

    expect(screen.getByLabelText('Current NameServer')).toHaveValue('10.0.0.10:9876');
    expect(screen.queryByRole('button', { name: 'Apply active endpoint' })).not.toBeInTheDocument();
  });

  it('shows the endpoint legend and refreshes independent NameServer availability', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');

    const table = await screen.findByRole('table', { name: 'NameServer endpoints' });
    const unavailableRow = within(table).getByRole('row', { name: /10\.0\.0\.11:9876/ });
    await waitFor(() => expect(within(unavailableRow).getByText('Unavailable')).toBeInTheDocument());
    expect(screen.getByLabelText('NameServer endpoint legend')).toHaveTextContent('Current');
    expect(screen.getByLabelText('NameServer endpoint legend')).toHaveTextContent('Checking');

    let resolveAvailability: ((value: NameserverAvailabilityView) => void) | undefined;
    mockedConfigApi.getNameserverAvailability.mockImplementationOnce(() => new Promise((resolve) => {
      resolveAvailability = resolve;
    }));

    await user.click(screen.getByRole('button', { name: 'Check all NameServer endpoints' }));
    expect(within(unavailableRow).getByText('Checking')).toBeInTheDocument();
    resolveAvailability?.({
      endpoints: initialAvailability.endpoints.map((endpoint) => ({ ...endpoint, status: 'available' }))
    });

    await waitFor(() => expect(within(unavailableRow).getByText('Available')).toBeInTheDocument());
    expect(mockedConfigApi.getNameserverAvailability).toHaveBeenCalledTimes(2);
  });

  it('adds a NameServer with the exact normalized address and clears the input only after success', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);
    mockedConfigApi.addNameserver.mockResolvedValue(awaitableMutation({
      ...initialConfig,
      namesrvAddrList: [...initialConfig.namesrvAddrList, '10.0.0.12:9876']
    }, 'NameServer added'));

    renderAtRoute(<ConfigPage />, '/config');
    const input = await screen.findByLabelText('Add NameServer');
    await user.type(input, ' 10.0.0.12:9876 ');
    await user.click(screen.getByRole('button', { name: 'Add NameServer' }));

    await waitFor(() => expect(mockedConfigApi.addNameserver).toHaveBeenCalledWith({ address: '10.0.0.12:9876', expectedRevision: 7 }));
    await waitFor(() => expect(input).toHaveValue(''));
    expect(onConfigUpdated).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });

  it('preserves a NameServer input after an add error', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);
    mockedConfigApi.addNameserver.mockRejectedValueOnce(new Error('NameServer unavailable'));

    renderAtRoute(<ConfigPage />, '/config');
    const input = await screen.findByLabelText('Add NameServer');
    await user.type(input, '10.0.0.12:9876');
    await user.click(screen.getByRole('button', { name: 'Add NameServer' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('NameServer unavailable');
    expect(input).toHaveValue('10.0.0.12:9876');
    expect(onConfigUpdated).not.toHaveBeenCalled();
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });

  it('refreshes authoritative settings while preserving a dirty current NameServer draft', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');

    expect(screen.getByLabelText('Current NameServer')).toHaveValue('10.0.0.11:9876');
    expect(screen.getByRole('button', { name: 'Reload OPS settings' })).toBeEnabled();
    expect(screen.getByLabelText('Add NameServer')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Add NameServer' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Remove 10.0.0.11:9876' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Reload OPS settings' }));
    await waitFor(() => expect(mockedConfigApi.getConfig).toHaveBeenCalledTimes(2));
    expect(screen.getByLabelText('Current NameServer')).toHaveValue('10.0.0.11:9876');
    expect(mockedConfigApi.addNameserver).not.toHaveBeenCalled();
    expect(mockedConfigApi.switchNameserver).not.toHaveBeenCalled();
  });

  it('submits one add mutation when Enter is repeated while the first request is pending', async () => {
    const user = userEvent.setup();
    let resolveAdd: ((value: Awaited<ReturnType<typeof awaitableMutation>>) => void) | undefined;
    mockedConfigApi.addNameserver.mockImplementationOnce(() => new Promise((resolve) => {
      resolveAdd = resolve;
    }));
    renderAtRoute(<ConfigPage />, '/config');
    const input = await screen.findByLabelText('Add NameServer');
    await user.type(input, '10.0.0.12:9876{enter}{enter}');

    expect(mockedConfigApi.addNameserver).toHaveBeenCalledTimes(1);
    expect(input).toBeDisabled();
    resolveAdd?.({ config: initialConfig, message: 'NameServer added' });
    await waitFor(() => expect(input).toHaveValue(''));
  });

  it('persists VIP and TLS toggles through their exact config APIs and signals each success', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);

    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });
    await user.click(screen.getByRole('button', { name: 'Security' }));
    await user.click(screen.getByRole('button', { name: 'Enable VIP channel' }));
    await user.click(screen.getByRole('button', { name: 'Enable TLS' }));

    await waitFor(() => expect(mockedConfigApi.setVipChannel).toHaveBeenCalledWith({ enabled: true, expectedRevision: 7 }));
    await waitFor(() => expect(mockedConfigApi.setTls).toHaveBeenCalledWith({ enabled: true, expectedRevision: 7 }));
    expect(onConfigUpdated).toHaveBeenCalledTimes(2);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });

  it('asks for confirmation before discarding unsaved NameServer changes to change sections', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');
    await user.click(screen.getByRole('button', { name: 'Security' }));

    expect(await screen.findByRole('alertdialog')).toHaveTextContent('Discard unsaved NameServer changes?');
    await user.click(screen.getByRole('button', { name: 'Discard changes' }));
    expect(await screen.findByRole('heading', { name: 'Security' })).toBeInTheDocument();
  });

  it('does not remove a NameServer when its confirmation is cancelled', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.click(screen.getByRole('button', { name: 'Remove 10.0.0.11:9876' }));
    expect(await screen.findByRole('alertdialog')).toHaveTextContent('Remove NameServer 10.0.0.11:9876?');
    await user.click(screen.getByRole('button', { name: 'Keep NameServer' }));

    expect(mockedConfigApi.deleteNameserver).not.toHaveBeenCalled();
  });

  it('protects the current NameServer from removal until another endpoint is selected', async () => {
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    expect(screen.queryByRole('button', { name: 'Remove 10.0.0.10:9876' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove 10.0.0.11:9876' })).toBeEnabled();
  });

  it('preserves the NameServer draft and tells the operator to refresh before retrying a conflict', async () => {
    const user = userEvent.setup();
    mockedConfigApi.switchNameserver.mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Configuration revision is stale.'));
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');
    await user.click(screen.getByRole('button', { name: 'Apply active endpoint' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('refresh before retrying');
    expect(screen.getByLabelText('Current NameServer')).toHaveValue('10.0.0.11:9876');
    expect(mockedConfigApi.switchNameserver).toHaveBeenCalledWith({ endpointId: 'nameserver-b', expectedRevision: 7 });
  });
});

function awaitableMutation(config = initialConfig, message = 'NameServer added') {
  return { config, message };
}
