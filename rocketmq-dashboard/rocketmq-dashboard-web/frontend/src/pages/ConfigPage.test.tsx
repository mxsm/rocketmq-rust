import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { configApi } from '../api/config_api';
import { renderAtRoute } from '../test/render';
import type { DashboardConfigView } from '../types/config';
import ConfigPage from './ConfigPage';

vi.mock('../api/config_api', () => ({
  configApi: {
    getConfig: vi.fn(),
    replaceNameservers: vi.fn(),
    addNameserver: vi.fn(),
    setVipChannel: vi.fn(),
    setTls: vi.fn(),
    addProxy: vi.fn(),
    switchProxy: vi.fn(),
    deleteProxy: vi.fn()
  }
}));

const initialConfig: DashboardConfigView = {
  namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
  currentNamesrv: '10.0.0.10:9876',
  useVIPChannel: false,
  useTLS: false,
  proxyAddrList: [],
  currentProxyAddr: null,
  storageBackend: 'file'
};

const mockedConfigApi = vi.mocked(configApi);

function mutationResult(config = initialConfig, message = 'Configuration updated') {
  return Promise.resolve({ config, message });
}

describe('ConfigPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockedConfigApi.getConfig.mockResolvedValue(initialConfig);
    mockedConfigApi.replaceNameservers.mockImplementation((request) => mutationResult({
      ...initialConfig,
      namesrvAddrList: request.namesrvAddrList,
      currentNamesrv: request.currentNamesrv
    }, 'NameServers updated'));
    mockedConfigApi.addNameserver.mockResolvedValue(awaitableMutation());
    mockedConfigApi.setVipChannel.mockImplementation(({ enabled }) => mutationResult({ ...initialConfig, useVIPChannel: enabled }, 'VIP updated'));
    mockedConfigApi.setTls.mockImplementation(({ enabled }) => mutationResult({ ...initialConfig, useTLS: enabled }, 'TLS updated'));
  });

  it('replaces NameServers with the selected current endpoint and signals successful persistence', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);

    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');
    await user.click(screen.getByRole('button', { name: 'Save NameServers' }));

    await waitFor(() => expect(mockedConfigApi.replaceNameservers).toHaveBeenCalledWith({
      namesrvAddrList: ['10.0.0.10:9876', '10.0.0.11:9876'],
      currentNamesrv: '10.0.0.11:9876'
    }));
    expect(onConfigUpdated).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
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

    await waitFor(() => expect(mockedConfigApi.addNameserver).toHaveBeenCalledWith({ address: '10.0.0.12:9876' }));
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

  it('locks rebase operations while a current NameServer selection draft is dirty', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.selectOptions(screen.getByLabelText('Current NameServer'), '10.0.0.11:9876');

    expect(screen.getByLabelText('Current NameServer')).toHaveValue('10.0.0.11:9876');
    expect(screen.getByRole('button', { name: 'Reload OPS settings' })).toBeDisabled();
    expect(screen.getByLabelText('Add NameServer')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Add NameServer' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Remove 10.0.0.10:9876' })).toBeDisabled();
    expect(mockedConfigApi.getConfig).toHaveBeenCalledTimes(1);
    expect(mockedConfigApi.addNameserver).not.toHaveBeenCalled();
    expect(mockedConfigApi.replaceNameservers).not.toHaveBeenCalled();
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

    await waitFor(() => expect(mockedConfigApi.setVipChannel).toHaveBeenCalledWith({ enabled: true }));
    await waitFor(() => expect(mockedConfigApi.setTls).toHaveBeenCalledWith({ enabled: true }));
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

    expect(mockedConfigApi.replaceNameservers).not.toHaveBeenCalled();
  });

  it('removes the current NameServer with the remaining endpoint as the exact current fallback', async () => {
    const user = userEvent.setup();
    const onConfigUpdated = vi.fn();
    window.addEventListener('rocketmq-config-updated', onConfigUpdated);
    renderAtRoute(<ConfigPage />, '/config');
    await screen.findByRole('heading', { name: 'OPS settings' });

    await user.click(screen.getByRole('button', { name: 'Remove 10.0.0.10:9876' }));
    await user.click(screen.getByRole('button', { name: 'Remove NameServer' }));

    await waitFor(() => expect(mockedConfigApi.replaceNameservers).toHaveBeenCalledWith({
      namesrvAddrList: ['10.0.0.11:9876'],
      currentNamesrv: '10.0.0.11:9876'
    }));
    expect(onConfigUpdated).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-config-updated', onConfigUpdated);
  });
});

function awaitableMutation(config = initialConfig, message = 'NameServer added') {
  return { config, message };
}
