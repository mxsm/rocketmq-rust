import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterEach, vi } from 'vitest';
import { configApi } from '../api/config_api';
import { consumerApi } from '../api/consumer_api';
import { brokerApi } from '../api/broker_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import type { ConsumerGroupListItem } from '../types/consumer';
import { ConsumerQueryScopeProvider, useConsumerQueryScope } from './consumers/ConsumerQueryScopeProvider';
import ConsumerListPage from './ConsumerListPage';
import { resetConsumerMutationLocksForTests } from '../components/consumerMutationLock';

vi.mock('../api/consumer_api', () => ({ consumerApi: { list: vi.fn(), create: vi.fn(), update: vi.fn(), config: vi.fn() } }));
vi.mock('../api/broker_api', () => ({ brokerApi: { list: vi.fn() } }));
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
    <MountableConsumerListPage />,
    '/consumers'
  );
}

let setScopeMode: ReturnType<typeof useConsumerQueryScope>['setMode'];
let setListMounted: (mounted: boolean) => void;

function MountableConsumerListPage() {
  const [mounted, setMounted] = useState(true);
  setListMounted = setMounted;
  return (
    <ConsumerQueryScopeProvider>
      <ScopeControls />
      {mounted ? <ConsumerListPage /> : null}
    </ConsumerQueryScopeProvider>
  );
}

function ScopeControls() {
  const { setMode } = useConsumerQueryScope();
  setScopeMode = setMode;
  return null;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function listView(name: string, mode: 'nameServer' | 'proxy') {
  return {
    items: [consumer({ rawGroupName: name, displayGroupName: name })],
    total: 1,
    queryScope: mode === 'proxy' ? { mode, proxyAddress: '127.0.0.1:8080' } : { mode },
    capabilities: { connections: true, progress: true, configuration: true, runningInfo: true, jstack: true }
  };
}

describe('ConsumerListPage', () => {
  afterEach(() => {
    resetConsumerMutationLocksForTests();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      environmentId: 'environment-default',
      environmentName: 'Default',
      revision: 1,
      endpoints: [
        { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
        { endpointId: 'proxy-1', endpointType: 'proxy', address: '127.0.0.1:8080', role: 'primary', isEnabled: true, isActive: true, sortOrder: 1 }
      ],
      currentNamesrv: '127.0.0.1:9876',
      namesrvAddrList: ['127.0.0.1:9876'],
      useVIPChannel: false,
      useTLS: false,
      currentProxyAddr: '127.0.0.1:8080',
      proxyAddrList: ['127.0.0.1:8080'],
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
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 0, address: '127.0.0.1:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
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

  it('keeps an inactive create completion dirty until its original scope returns', async () => {
    const user = userEvent.setup();
    const pendingCreate = deferred<{
      operation: 'CREATE'; consumerGroup: string; success: boolean; targetCount: number; message: string; targets: [];
    }>();
    const nameServerRefresh = deferred<ReturnType<typeof listView>>();
    let nameServerReads = 0;
    vi.mocked(consumerApi.list).mockImplementation((scope) => {
      if (scope?.mode === 'proxy') return Promise.resolve(listView('proxy-only', 'proxy'));
      nameServerReads += 1;
      return nameServerReads === 1
        ? Promise.resolve(listView('name-server-only', 'nameServer'))
        : nameServerRefresh.promise;
    });
    vi.mocked(consumerApi.create).mockImplementationOnce(() => pendingCreate.promise);
    renderPage();

    expect(await screen.findByText('name-server-only')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Create group' }));
    const dialog = await screen.findByRole('dialog', { name: 'Create consumer group' });
    await user.type(screen.getByLabelText('Consumer group'), 'created-in-a');
    await user.click(await screen.findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(dialog).getByRole('button', { name: 'Create group' }));
    await waitFor(() => expect(consumerApi.create).toHaveBeenCalledTimes(1));

    await act(async () => setListMounted(false));
    await act(async () => setScopeMode('proxy'));
    await act(async () => setListMounted(true));
    expect(await screen.findByText('proxy-only')).toBeInTheDocument();
    await act(async () => pendingCreate.resolve({ operation: 'CREATE', consumerGroup: 'created-in-a', success: true, targetCount: 1, message: 'created', targets: [] }));
    expect(screen.getByText('proxy-only')).toBeInTheDocument();
    expect(nameServerReads).toBe(1);

    await act(async () => setScopeMode('nameServer'));
    await waitFor(() => expect(nameServerReads).toBe(2));
    await act(async () => nameServerRefresh.resolve(listView('refreshed-a', 'nameServer')));
    expect(await screen.findByText('refreshed-a')).toBeInTheDocument();
    expect(consumerApi.create).toHaveBeenCalledTimes(1);
  });

  it('does not let an applied NameServer create refresh the visible proxy scope', async () => {
    const user = userEvent.setup();
    const pendingCreate = deferred<never>();
    const auditWarning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    vi.mocked(consumerApi.list).mockImplementation((scope) => Promise.resolve(
      scope?.mode === 'proxy' ? listView('proxy-only', 'proxy') : listView('name-server-only', 'nameServer')
    ));
    vi.mocked(consumerApi.create).mockImplementationOnce(() => pendingCreate.promise);
    renderPage();

    expect(await screen.findByText('name-server-only')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Create group' }));
    const dialog = await screen.findByRole('dialog', { name: 'Create consumer group' });
    await user.type(screen.getByLabelText('Consumer group'), 'applied-in-a');
    await user.click(await screen.findByRole('checkbox', { name: 'broker-a' }));
    await user.click(within(dialog).getByRole('button', { name: 'Create group' }));
    await waitFor(() => expect(consumerApi.create).toHaveBeenCalledTimes(1));

    await act(async () => setScopeMode('proxy'));
    expect(await screen.findByText('proxy-only')).toBeInTheDocument();
    const readsBeforeTerminal = vi.mocked(consumerApi.list).mock.calls.length;
    await act(async () => {
      window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Mutation applied.' }));
      pendingCreate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Mutation applied.', { mutationApplied: true }));
    });
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));
    expect(vi.mocked(consumerApi.list).mock.calls).toHaveLength(readsBeforeTerminal);
    expect(screen.getByText('proxy-only')).toBeInTheDocument();
    await act(async () => setScopeMode('nameServer'));
    await waitFor(() => expect(
      vi.mocked(consumerApi.list).mock.calls.filter(([scope]) => scope?.mode === 'nameServer')
    ).toHaveLength(2));
    expect(await screen.findByText('name-server-only')).toBeInTheDocument();
    expect(consumerApi.create).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });
});
