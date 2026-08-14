import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { brokerApi } from '../../api/broker_api';
import { renderAtRoute } from '../../test/render';
import type { BrokerInfo } from '../../types/broker';
import BrokerDetailContent from './BrokerDetailContent';

vi.mock('../../api/broker_api', () => ({
  brokerApi: {
    runtime: vi.fn(),
    config: vi.fn(),
    updateConfig: vi.fn()
  }
}));

const broker: BrokerInfo = {
  clusterName: 'DefaultCluster',
  brokerName: 'broker-a',
  brokerId: 0,
  address: '127.0.0.1:10911',
  role: 'MASTER',
  version: '5.3.2',
  produceTps: 128.4,
  consumeTps: 98.2
};

const brokerB: BrokerInfo = {
  ...broker,
  brokerName: 'broker-b',
  address: '127.0.0.1:20911',
  version: '5.4.0'
};

describe('BrokerDetailContent', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(brokerApi.runtime).mockResolvedValue({
      brokerName: 'broker-a',
      address: broker.address,
      entries: { brokerVersion: '5.3.2', putTps: '128.4' }
    });
    vi.mocked(brokerApi.config).mockResolvedValue({
      brokerName: 'broker-a',
      address: broker.address,
      entries: { sendMessageThreadPoolNums: '16' }
    });
    vi.mocked(brokerApi.updateConfig).mockResolvedValue({ message: 'updated' });
  });

  it('shows the broker overview without eagerly loading runtime or configuration', () => {
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" broker={broker} />, '/brokers');

    expect(screen.getByText('DefaultCluster')).toBeInTheDocument();
    expect(screen.getByText('127.0.0.1:10911')).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Produce TPS: 128.4' })).toBeInTheDocument();
    expect(brokerApi.runtime).not.toHaveBeenCalled();
    expect(brokerApi.config).not.toHaveBeenCalled();
  });

  it('loads runtime data once on demand and caches it for the open session', async () => {
    const user = userEvent.setup();
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" broker={broker} />, '/brokers');

    await user.click(screen.getByRole('tab', { name: 'Runtime' }));
    expect(await screen.findByText('brokerVersion')).toBeInTheDocument();
    await user.click(screen.getByRole('tab', { name: 'Overview' }));
    await user.click(screen.getByRole('tab', { name: 'Runtime' }));

    expect(brokerApi.runtime).toHaveBeenCalledTimes(1);
  });

  it('retries a failed lazy runtime request', async () => {
    const user = userEvent.setup();
    vi.mocked(brokerApi.runtime).mockRejectedValueOnce(new Error('runtime unavailable'));
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" />, '/brokers');

    await user.click(screen.getByRole('tab', { name: 'Runtime' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('runtime unavailable');
    await user.click(screen.getByRole('button', { name: 'Retry' }));

    expect(await screen.findByText('brokerVersion')).toBeInTheDocument();
    expect(brokerApi.runtime).toHaveBeenCalledTimes(2);
  });

  it('validates JSON objects and protects configuration updates with confirmation', async () => {
    const user = userEvent.setup();
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" broker={broker} />, '/brokers');

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    expect(await screen.findByText('sendMessageThreadPoolNums')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));

    const editor = screen.getByRole('textbox', { name: 'Broker configuration JSON' });
    fireEvent.change(editor, { target: { value: '[]' } });
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    expect(screen.getByText('Broker config must be a JSON object.')).toBeInTheDocument();
    expect(editor).toHaveValue('[]');

    fireEvent.change(editor, { target: { value: '{"threads":8}' } });
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    expect(screen.getByText('Broker config values must be strings.')).toBeInTheDocument();

    fireEvent.change(editor, { target: { value: '{"threads":"8","enabled":"true"}' } });
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    expect(await screen.findByRole('alertdialog')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Apply configuration' }));

    await waitFor(() => expect(brokerApi.updateConfig).toHaveBeenCalledWith('broker-a', {
      entries: { threads: '8', enabled: 'true' }
    }));
  });

  it('preserves the editable draft when a configuration update fails', async () => {
    const user = userEvent.setup();
    vi.mocked(brokerApi.updateConfig).mockRejectedValueOnce(new Error('write rejected'));
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" broker={broker} />, '/brokers');

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    await screen.findByText('sendMessageThreadPoolNums');
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const editor = screen.getByRole('textbox', { name: 'Broker configuration JSON' });
    fireEvent.change(editor, { target: { value: '{"sendMessageThreadPoolNums":"24"}' } });
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    await user.click(await screen.findByRole('button', { name: 'Apply configuration' }));

    expect(await screen.findByText('write rejected')).toBeInTheDocument();
    expect(editor).toBeInTheDocument();
    expect(editor).toHaveValue('{"sendMessageThreadPoolNums":"24"}');
  });

  it('ignores late runtime responses after switching brokers', async () => {
    let resolveBrokerA: (value: Awaited<ReturnType<typeof brokerApi.runtime>>) => void = () => undefined;
    vi.mocked(brokerApi.runtime).mockImplementation((brokerName) => {
      if (brokerName === 'broker-a') {
        return new Promise((resolve) => { resolveBrokerA = resolve; });
      }
      return Promise.resolve({
        brokerName: 'broker-b',
        address: brokerB.address,
        entries: { activeBrokerMarker: 'broker-b-runtime' }
      });
    });

    const { rerender } = render(<BrokerDetailContent brokerName="broker-a" broker={broker} initialTab="runtime" />);
    await waitFor(() => expect(brokerApi.runtime).toHaveBeenCalledWith('broker-a'));

    rerender(<BrokerDetailContent brokerName="broker-b" broker={brokerB} initialTab="runtime" />);
    expect(await screen.findByText('broker-b-runtime')).toBeInTheDocument();

    resolveBrokerA({
      brokerName: 'broker-a',
      address: broker.address,
      entries: { staleBrokerMarker: 'broker-a-runtime' }
    });
    await waitFor(() => expect(screen.queryByText('broker-a-runtime')).not.toBeInTheDocument());
    expect(screen.getByText('broker-b-runtime')).toBeInTheDocument();
  });

  it('clears a pending configuration confirmation when switching brokers', async () => {
    const user = userEvent.setup();
    const { rerender } = render(<BrokerDetailContent brokerName="broker-a" broker={broker} />);

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    await screen.findByText('sendMessageThreadPoolNums');
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    fireEvent.change(screen.getByRole('textbox', { name: 'Broker configuration JSON' }), {
      target: { value: '{"sendMessageThreadPoolNums":"24"}' }
    });
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    expect(await screen.findByRole('alertdialog')).toHaveTextContent('broker-a');

    rerender(<BrokerDetailContent brokerName="broker-b" broker={brokerB} />);
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument());
    expect(brokerApi.updateConfig).not.toHaveBeenCalled();
  });

  it('clears the previous success message when a new edit begins', async () => {
    const user = userEvent.setup();
    renderAtRoute(<BrokerDetailContent brokerName="broker-a" broker={broker} />, '/brokers');

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));
    await screen.findByText('sendMessageThreadPoolNums');
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    await user.click(screen.getByRole('button', { name: 'Review changes' }));
    await user.click(await screen.findByRole('button', { name: 'Apply configuration' }));
    expect(await screen.findByText('Configuration updated.')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    expect(screen.queryByText('Configuration updated.')).not.toBeInTheDocument();
  });
});
