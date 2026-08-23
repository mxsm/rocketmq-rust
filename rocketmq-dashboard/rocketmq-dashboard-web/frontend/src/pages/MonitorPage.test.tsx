import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { StrictMode } from 'react';
import { vi } from 'vitest';
import { ApiClientError } from '../api/client';
import { configApi } from '../api/config_api';
import { monitorApi } from '../api/monitor_api';
import { renderAtRoute } from '../test/render';
import type { DashboardConfigView } from '../types/config';
import type { ConsumerMonitorView } from '../types/monitor';
import MonitorPage from './MonitorPage';

vi.mock('../api/monitor_api', () => ({
  monitorApi: {
    listConsumerMonitors: vi.fn(),
    saveConsumerMonitor: vi.fn(),
    deleteConsumerMonitor: vi.fn()
  }
}));

vi.mock('../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));

describe('MonitorPage', () => {
  const environmentId = 'environment-default';
  const config: DashboardConfigView = {
    environmentId,
    environmentName: 'Default',
    revision: 7,
    endpoints: [],
    currentNamesrv: null,
    namesrvAddrList: [],
    useVIPChannel: false,
    useTLS: false,
    currentProxyAddr: null,
    proxyAddrList: [],
    storageBackend: 'sqlite',
    storageMode: 'singleNode'
  };
  const rules: ConsumerMonitorView[] = [
    { environmentId, consumerGroup: 'order-service', minCount: 4, maxDiffTotal: 1200, revision: 11 },
    { environmentId, consumerGroup: 'payment-worker', minCount: 2, maxDiffTotal: 800, revision: 12 }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(configApi.getConfig).mockResolvedValue(config);
    vi.mocked(monitorApi.listConsumerMonitors).mockResolvedValue(rules);
    vi.mocked(monitorApi.saveConsumerMonitor).mockResolvedValue({ message: 'saved', item: rules[0] });
    vi.mocked(monitorApi.deleteConsumerMonitor).mockResolvedValue({ message: 'deleted', item: null });
  });

  it('renders API-backed rule metrics and a persisted rule list', async () => {
    renderAtRoute(<MonitorPage />, '/monitor');

    expect(screen.getByRole('status', { name: 'Loading monitor rules' })).toBeInTheDocument();
    expect(await screen.findByRole('heading', { name: 'Monitor' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Rules: 2' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Min Count range: 2–4' })).toBeInTheDocument();
    expect(screen.getByRole('row', { name: /order-service 4 1200/ })).toBeInTheDocument();
    expect(monitorApi.listConsumerMonitors).toHaveBeenCalledWith(environmentId);
    expect(screen.queryByText(/active alerts|event history|firing/i)).not.toBeInTheDocument();
  });

  it('loads initial monitor rules under React StrictMode', async () => {
    renderAtRoute(<StrictMode><MonitorPage /></StrictMode>, '/monitor');

    expect(await screen.findByRole('row', { name: /order-service 4 1200/ })).toBeInTheDocument();
    expect(screen.queryByRole('status', { name: 'Loading monitor rules' })).not.toBeInTheDocument();
  });

  it('shows retryable list errors and an empty persisted-rule state', async () => {
    const user = userEvent.setup();
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockRejectedValueOnce(new Error('monitor list unavailable'))
      .mockResolvedValueOnce([]);
    renderAtRoute(<MonitorPage />, '/monitor');

    expect(await screen.findByRole('alert')).toHaveTextContent('monitor list unavailable');
    await user.click(screen.getByRole('button', { name: 'Retry monitor rules' }));

    expect(await screen.findByText('No monitor rules')).toBeInTheDocument();
    expect(monitorApi.listConsumerMonitors).toHaveBeenCalledTimes(2);
  });

  it('creates and edits a persisted rule then refreshes the list', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Create rule' }));
    const createDialog = screen.getByRole('dialog', { name: 'Create rule' });
    await user.type(within(createDialog).getByRole('textbox', { name: 'Group' }), 'inventory-worker');
    await user.clear(within(createDialog).getByRole('spinbutton', { name: 'Min Count' }));
    await user.type(within(createDialog).getByRole('spinbutton', { name: 'Min Count' }), '5');
    await user.clear(within(createDialog).getByRole('spinbutton', { name: 'Max Diff Total' }));
    await user.type(within(createDialog).getByRole('spinbutton', { name: 'Max Diff Total' }), '3000');
    await user.click(within(createDialog).getByRole('button', { name: 'Save rule' }));

    await waitFor(() => expect(monitorApi.saveConsumerMonitor).toHaveBeenCalledWith({
      environmentId, consumerGroup: 'inventory-worker', minCount: 5, maxDiffTotal: 3000, expectedRevision: 0
    }));
    expect(monitorApi.listConsumerMonitors).toHaveBeenCalledTimes(2);

    await user.click(screen.getByRole('button', { name: 'Edit rule for order-service' }));
    const editDialog = screen.getByRole('dialog', { name: 'Edit rule' });
    const maxDiffTotal = within(editDialog).getByRole('spinbutton', { name: 'Max Diff Total' });
    await user.clear(maxDiffTotal);
    await user.type(maxDiffTotal, '1500');
    await user.click(within(editDialog).getByRole('button', { name: 'Save rule' }));

    await waitFor(() => expect(monitorApi.saveConsumerMonitor).toHaveBeenLastCalledWith({
      environmentId, consumerGroup: 'order-service', minCount: 4, maxDiffTotal: 1500, expectedRevision: 11
    }));
  });

  it('treats a refresh failure after a successful save as a list retry without resubmitting the rule', async () => {
    const user = userEvent.setup();
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockResolvedValueOnce(rules)
      .mockRejectedValueOnce(new Error('refresh unavailable'))
      .mockResolvedValueOnce(rules);
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Create rule' }));
    const dialog = screen.getByRole('dialog', { name: 'Create rule' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Group' }), 'inventory-worker');
    await user.click(within(dialog).getByRole('button', { name: 'Save rule' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('refresh unavailable');
    expect(screen.queryByRole('dialog', { name: 'Create rule' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry monitor rules' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Retry save' })).not.toBeInTheDocument();
    expect(monitorApi.saveConsumerMonitor).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'Retry monitor rules' }));
    await waitFor(() => expect(monitorApi.listConsumerMonitors).toHaveBeenCalledTimes(3));
    expect(monitorApi.saveConsumerMonitor).toHaveBeenCalledTimes(1);
  });

  it('closes a successfully saved rule dialog before its background refresh settles', async () => {
    const user = userEvent.setup();
    let resolveRefresh: (value: typeof rules) => void = () => undefined;
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockResolvedValueOnce(rules)
      .mockReturnValueOnce(new Promise((resolve) => { resolveRefresh = resolve; }));
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Create rule' }));
    const dialog = screen.getByRole('dialog', { name: 'Create rule' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Group' }), 'inventory-worker');
    await user.click(within(dialog).getByRole('button', { name: 'Save rule' }));

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Create rule' })).not.toBeInTheDocument());
    await act(async () => { resolveRefresh(rules); });
  });

  it('requires confirmation naming the group before deleting and refreshes after confirmation', async () => {
    const user = userEvent.setup();
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Delete rule for order-service' }));
    let confirmation = screen.getByRole('alertdialog', { name: 'Delete rule?' });
    expect(within(confirmation).getByText(/order-service/)).toBeInTheDocument();
    await user.click(within(confirmation).getByRole('button', { name: 'Cancel' }));
    expect(monitorApi.deleteConsumerMonitor).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'Delete rule for order-service' }));
    confirmation = screen.getByRole('alertdialog', { name: 'Delete rule?' });
    await user.click(within(confirmation).getByRole('button', { name: 'Delete rule' }));

    await waitFor(() => expect(monitorApi.deleteConsumerMonitor).toHaveBeenCalledWith(environmentId, 'order-service', 11));
    expect(monitorApi.listConsumerMonitors).toHaveBeenCalledTimes(2);
  });

  it('treats a refresh failure after a successful delete as a list retry without repeating deletion', async () => {
    const user = userEvent.setup();
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockResolvedValueOnce(rules)
      .mockRejectedValueOnce(new Error('refresh unavailable'))
      .mockResolvedValueOnce(rules);
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Delete rule for order-service' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Delete rule?' })).getByRole('button', { name: 'Delete rule' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('refresh unavailable');
    expect(screen.getByRole('button', { name: 'Retry monitor rules' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Retry delete' })).not.toBeInTheDocument();
    expect(monitorApi.deleteConsumerMonitor).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'Retry monitor rules' }));
    await waitFor(() => expect(monitorApi.listConsumerMonitors).toHaveBeenCalledTimes(3));
    expect(monitorApi.deleteConsumerMonitor).toHaveBeenCalledTimes(1);
  });

  it('ignores a stale list response after a refresh request supersedes it', async () => {
    const user = userEvent.setup();
    let resolveInitial: (value: typeof rules) => void = () => undefined;
    let resolveRefresh: (value: typeof rules) => void = () => undefined;
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockReturnValueOnce(new Promise((resolve) => { resolveInitial = resolve; }))
      .mockReturnValueOnce(new Promise((resolve) => { resolveRefresh = resolve; }));
    renderAtRoute(<MonitorPage />, '/monitor');

    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    resolveRefresh([{ environmentId, consumerGroup: 'new-rule', minCount: 9, maxDiffTotal: 999, revision: 1 }]);
    expect(await screen.findByRole('row', { name: /new-rule 9 999/ })).toBeInTheDocument();
    resolveInitial(rules);
    await waitFor(() => expect(screen.queryByRole('row', { name: /order-service 4 1200/ })).not.toBeInTheDocument());
  });

  it('preserves the monitor draft and tells the operator to refresh before retrying a revision conflict', async () => {
    const user = userEvent.setup();
    vi.mocked(monitorApi.saveConsumerMonitor).mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Rule revision is stale.'));
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Create rule' }));
    const dialog = screen.getByRole('dialog', { name: 'Create rule' });
    await user.type(within(dialog).getByRole('textbox', { name: 'Group' }), 'inventory-worker');
    await user.click(within(dialog).getByRole('button', { name: 'Save rule' }));

    expect(await within(dialog).findByRole('alert')).toHaveTextContent('refresh before retrying');
    expect(within(dialog).getByRole('textbox', { name: 'Group' })).toHaveValue('inventory-worker');
    expect(monitorApi.saveConsumerMonitor).toHaveBeenCalledWith({
      environmentId, consumerGroup: 'inventory-worker', minCount: 1, maxDiffTotal: 1000, expectedRevision: 0
    });
  });

  it('loads the current delete revision and requires a new confirmation before retrying', async () => {
    const user = userEvent.setup();
    const updatedRule = { ...rules[0], revision: 13, minCount: 6 };
    vi.mocked(monitorApi.deleteConsumerMonitor)
      .mockRejectedValueOnce(new ApiClientError('STORAGE_CONFLICT', 'Rule revision is stale.'))
      .mockResolvedValueOnce({ message: 'deleted', item: null });
    vi.mocked(monitorApi.listConsumerMonitors)
      .mockResolvedValueOnce(rules)
      .mockResolvedValueOnce([updatedRule, rules[1]]);
    renderAtRoute(<MonitorPage />, '/monitor');
    await screen.findByRole('heading', { name: 'Monitor' });

    await user.click(screen.getByRole('button', { name: 'Delete rule for order-service' }));
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete rule' }));

    expect(await screen.findByText(/current rule revision is loaded/i)).toBeInTheDocument();
    expect(monitorApi.deleteConsumerMonitor).toHaveBeenCalledWith(environmentId, 'order-service', 11);
    await user.click(screen.getByRole('button', { name: 'Review delete' }));
    expect(screen.getByRole('alertdialog')).toHaveTextContent('order-service');
    await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Delete rule' }));

    await waitFor(() => expect(monitorApi.deleteConsumerMonitor).toHaveBeenLastCalledWith(environmentId, 'order-service', 13));
  });
});
