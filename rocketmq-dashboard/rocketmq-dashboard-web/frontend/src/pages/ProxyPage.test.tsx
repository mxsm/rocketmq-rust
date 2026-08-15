import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { configApi } from '../api/config_api';
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
  namesrvAddrList: [],
  currentNamesrv: null,
  useVIPChannel: false,
  useTLS: false,
  proxyAddrList: ['proxy-a:8081', 'proxy-b:8081'],
  currentProxyAddr: 'proxy-a:8081',
  storageBackend: 'file'
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
    mockedConfigApi.switchProxy.mockImplementation(({ address }) => Promise.resolve(configMutation({
      ...initialConfig,
      currentProxyAddr: address
    }, 'Current proxy updated.')));
    mockedConfigApi.deleteProxy.mockImplementation((address) => Promise.resolve(configMutation({
      ...initialConfig,
      proxyAddrList: initialConfig.proxyAddrList.filter((proxyAddress) => proxyAddress !== address)
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

    await waitFor(() => expect(mockedConfigApi.addProxy).toHaveBeenCalledWith({ address: 'proxy-c:8081' }));
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
    expect(mockedConfigApi.addProxy).toHaveBeenCalledWith({ address: 'proxy-c:8081' });
  });

  it('switches the current endpoint through the exact config API and reports the returned state', async () => {
    const user = userEvent.setup();
    renderAtRoute(<ProxyPage />, '/proxy');

    await screen.findByText('proxy-b:8081');
    await user.click(screen.getByRole('button', { name: 'Set current proxy proxy-b:8081' }));

    await waitFor(() => expect(mockedConfigApi.switchProxy).toHaveBeenCalledWith({ address: 'proxy-b:8081' }));
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

    await waitFor(() => expect(mockedConfigApi.deleteProxy).toHaveBeenCalledWith('proxy-b:8081'));
    expect(await screen.findByText('Proxy endpoint deleted.')).toBeInTheDocument();
  });

  it('uses the API response as the source of truth when deleting the current endpoint', async () => {
    const user = userEvent.setup();
    mockedConfigApi.getConfig.mockResolvedValue({
      ...initialConfig,
      proxyAddrList: ['proxy-a:8081'],
      currentProxyAddr: 'proxy-a:8081'
    });
    mockedConfigApi.deleteProxy.mockResolvedValue(configMutation({
      ...initialConfig,
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
});
