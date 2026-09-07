import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { Suspense, startTransition, useLayoutEffect, useRef } from 'react';
import { flushSync } from 'react-dom';
import { Route, Routes, useNavigate, useParams } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, vi } from 'vitest';
import { ApiClientError } from '../api/client';
import { brokerApi } from '../api/broker_api';
import { configApi } from '../api/config_api';
import { consumerApi } from '../api/consumer_api';
import type { ConsumerSummaryView } from '../types/consumer';
import { renderAtRoute } from '../test/render';
import { ConsumerQueryScopeProvider, useConsumerQueryScope } from './consumers/ConsumerQueryScopeProvider';
import ConsumerDetailPage from './ConsumerDetailPage';
import { resetConsumerMutationLocksForTests } from '../components/consumerMutationLock';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function ConsumerGroupSwitch() {
  const navigate = useNavigate();
  return <button type="button" onClick={() => navigate('/consumers/inventory-service')}>Open inventory group</button>;
}

function ConsumerABASwitch() {
  const navigate = useNavigate();
  return (
    <>
      <button type="button" onClick={() => navigate('/consumers/inventory-service')}>Switch to inventory group</button>
      <button type="button" onClick={() => navigate('/consumers/order-service')}>Switch to order group</button>
    </>
  );
}

function ConsumerScopeSwitch() {
  const { setMode } = useConsumerQueryScope();
  return <button type="button" onClick={() => setMode('proxy')}>Use proxy scope</button>;
}

function CommitWindowGroupSwitch({ onCommitted }: { onCommitted: () => void }) {
  const navigate = useNavigate();
  return (
    <button
      type="button"
      onClick={() => {
        flushSync(() => navigate('/consumers/inventory-service'));
        onCommitted();
      }}
    >
      Commit inventory group
    </button>
  );
}

function CommitWindowScopeSwitch({ onCommitted }: { onCommitted: () => void }) {
  const { setMode } = useConsumerQueryScope();
  return (
    <button
      type="button"
      onClick={() => {
        flushSync(() => setMode('proxy'));
        onCommitted();
      }}
    >
      Commit proxy scope
    </button>
  );
}

function CommitWindowUnmountSwitch({ onCommitted }: { onCommitted: () => void }) {
  const navigate = useNavigate();
  return (
    <button
      type="button"
      onClick={() => {
        flushSync(() => navigate('/consumers'));
        onCommitted();
      }}
    >
      Commit consumer list
    </button>
  );
}

function LayoutGroupSwitch() {
  const navigate = useNavigate();
  return (
    <button type="button" onClick={() => flushSync(() => navigate('/consumers/inventory-service'))}>
      Commit layout inventory group
    </button>
  );
}

function LayoutScopeSwitch() {
  const { setMode } = useConsumerQueryScope();
  return (
    <button type="button" onClick={() => flushSync(() => setMode('proxy'))}>
      Commit layout proxy scope
    </button>
  );
}

function LayoutIdentityObserver({
  group: expectedGroup,
  scopeMode,
  onSettled
}: {
  group: string;
  scopeMode: 'nameServer' | 'proxy';
  onSettled: (controls: { editDisabled: boolean; deleteDisabled: boolean }) => void;
}) {
  const { group } = useParams();
  const { scope } = useConsumerQueryScope();
  const settledIdentityRef = useRef('');

  useLayoutEffect(() => {
    if (group !== expectedGroup || scope.mode !== scopeMode) return;
    const identityKey = `${group}|${scope.mode}|${scope.proxyAddress ?? ''}`;
    if (settledIdentityRef.current === identityKey) return;
    settledIdentityRef.current = identityKey;
    const controls = Array.from(document.querySelectorAll('button'));
    const edit = controls.find((button) => button.textContent === 'Edit configuration');
    const remove = controls.find((button) => button.textContent === 'Delete group');
    onSettled({
      editDisabled: edit instanceof HTMLButtonElement && edit.disabled,
      deleteDisabled: remove instanceof HTMLButtonElement && remove.disabled
    });
  }, [group, scope.mode, scope.proxyAddress, expectedGroup, scopeMode, onSettled]);

  return null;
}

function ConcurrentTransitionSwitch() {
  const navigate = useNavigate();
  return (
    <>
      <button type="button" onClick={() => startTransition(() => navigate('/consumers/inventory-service'))}>
        Start suspended inventory transition
      </button>
      <button type="button" onClick={() => flushSync(() => navigate('/consumers/order-service'))}>
        Cancel suspended transition
      </button>
    </>
  );
}

function SuspendInventoryAfterDetail({ suspension, onInventoryRender }: {
  suspension: Promise<void>;
  onInventoryRender: () => void;
}) {
  const { group } = useParams();
  if (group === 'inventory-service') {
    onInventoryRender();
    throw suspension;
  }
  return null;
}

function ConcurrentConsumerWorkspace({ suspension, onInventoryRender }: {
  suspension: Promise<void>;
  onInventoryRender: () => void;
}) {
  return (
    <>
      <ConcurrentTransitionSwitch />
      <Suspense fallback={<p>Inventory transition pending</p>}>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><ConsumerDetailPage /><SuspendInventoryAfterDetail suspension={suspension} onInventoryRender={onInventoryRender} /></>}
          />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </Suspense>
    </>
  );
}

vi.mock('../api/consumer_api', () => ({
  consumerApi: {
    summary: vi.fn(), progress: vi.fn(), resetOffset: vi.fn(), brokers: vi.fn(), delete: vi.fn(), config: vi.fn(), update: vi.fn()
  }
}));
vi.mock('../api/config_api', () => ({ configApi: { getConfig: vi.fn() } }));
vi.mock('../api/broker_api', () => ({ brokerApi: { list: vi.fn() } }));

const consumerConfig = {
  group: 'order-service',
  effective: {
    consumeEnable: true,
    consumeFromMinEnable: true,
    consumeBroadcastEnable: false,
    consumeMessageOrderly: false,
    retryQueueNums: 1,
    retryMaxTimes: 16,
    brokerId: 0,
    whichBrokerWhenConsumeSlowly: 1,
    notifyConsumerIdsChangedEnable: true,
    groupSysFlag: 0,
    consumeTimeoutMinute: 15,
    groupRetryPolicyJson: '{}'
  },
  inconsistentFields: [],
  targets: [{
    brokerName: 'broker-a',
    brokerAddress: '127.0.0.1:10911',
    config: {
      consumeEnable: true,
      consumeFromMinEnable: true,
      consumeBroadcastEnable: false,
      consumeMessageOrderly: false,
      retryQueueNums: 1,
      retryMaxTimes: 16,
      brokerId: 0,
      whichBrokerWhenConsumeSlowly: 1,
      notifyConsumerIdsChangedEnable: true,
      groupSysFlag: 0,
      consumeTimeoutMinute: 15,
      groupRetryPolicyJson: '{}'
    },
    subscriptionTopics: [],
    attributes: []
  }],
  queryScope: { mode: 'nameServer' as const }
};

describe('ConsumerDetailPage', () => {
  beforeEach(() => {
    resetConsumerMutationLocksForTests();
    vi.clearAllMocks();
    window.localStorage.clear();
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
    vi.mocked(consumerApi.brokers).mockResolvedValue({
      items: [{ brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911' }]
    });
    vi.mocked(consumerApi.config).mockResolvedValue(consumerConfig);
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 0, address: '127.0.0.1:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
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
    expect(screen.getByRole('heading', { name: 'order-service' }).closest('[data-surface="frosted"]')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Back to groups' })).toHaveAttribute('href', '/consumers');
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

  afterEach(() => {
    resetConsumerMutationLocksForTests();
  });

  it('unmounts the old edit dialog when a successful edit settles in the commit-before-passive window', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const inventorySummary = {
      group: 'inventory-service',
      displayGroupName: 'inventory-service',
      category: 'NORMAL',
      connectionCount: 37,
      consumeTps: 5,
      diffTotal: 1,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 6,
      queryScope: { mode: 'nameServer' as const }
    };
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><CommitWindowGroupSwitch onCommitted={() => {
              pendingUpdate.resolve({
                operation: 'UPDATE',
                consumerGroup: 'order-service',
                success: true,
                targetCount: 1,
                message: 'saved',
                targets: [{ target: 'broker-a', kind: 'BROKER', success: true, message: 'saved' }]
              });
            }} /><ConsumerDetailPage /></>}
          />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const orderDialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(orderDialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockImplementationOnce(() => pendingUpdate.promise);
    vi.mocked(consumerApi.summary).mockResolvedValue(inventorySummary);
    vi.mocked(consumerApi.config).mockResolvedValue({ ...consumerConfig, group: 'inventory-service' });
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 0, address: '127.0.0.1:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
    });

    await user.click(within(orderDialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByText('Commit inventory group'));

    await screen.findByRole('heading', { name: 'inventory-service', hidden: true });
    expect(screen.queryByRole('dialog', { name: 'Edit inventory-service' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list', hidden: true })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 37', hidden: true })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration', hidden: true })).toBeEnabled());
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(vi.mocked(consumerApi.summary).mock.calls).not.toContainEqual(['order-service', { mode: 'nameServer' }]);
    expect(consumerApi.config).not.toHaveBeenCalled();
  });

  it('unmounts the old delete dialog when a stale NotFound settles in the commit-before-passive window', async () => {
    const user = userEvent.setup();
    const pendingDelete = deferred<Awaited<ReturnType<typeof consumerApi.delete>>>();
    const inventorySummary = {
      group: 'inventory-service',
      displayGroupName: 'inventory-service',
      category: 'NORMAL',
      connectionCount: 32,
      consumeTps: 2,
      diffTotal: 1,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 8,
      queryScope: { mode: 'nameServer' as const }
    };
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><CommitWindowGroupSwitch onCommitted={() => {
              pendingDelete.reject(new ApiClientError('NOT_FOUND', 'Consumer group was not found.'));
            }} /><ConsumerDetailPage /></>}
          />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const orderDialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(orderDialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(orderDialog).getByLabelText('Confirm consumer group'), 'order-service');
    vi.clearAllMocks();
    vi.mocked(consumerApi.delete).mockImplementationOnce(() => pendingDelete.promise);
    vi.mocked(consumerApi.summary).mockResolvedValue(inventorySummary);
    vi.mocked(consumerApi.brokers).mockResolvedValue({ items: [{ brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911' }] });

    await user.click(within(orderDialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByText('Commit inventory group'));

    await screen.findByRole('heading', { name: 'inventory-service', hidden: true });
    expect(screen.queryByRole('dialog', { name: 'Delete consumer group' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list', hidden: true })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 32', hidden: true })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit configuration', hidden: true })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Delete group', hidden: true })).toBeEnabled();
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
    expect(vi.mocked(consumerApi.summary).mock.calls).not.toContainEqual(['order-service', { mode: 'nameServer' }]);
  });

  it('keeps a pending A edit current when a suspended B render is discarded before commit', async () => {
    const user = userEvent.setup();
    const suspension = deferred<void>();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const inventoryRendered = vi.fn();
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <ConcurrentConsumerWorkspace suspension={suspension.promise} onInventoryRender={inventoryRendered} />
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const dialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockImplementationOnce(() => pendingUpdate.promise);

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Start suspended inventory transition', hidden: true }));
    await waitFor(() => expect(inventoryRendered).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: 'Cancel suspended transition', hidden: true }));

    await act(async () => {
      pendingUpdate.resolve({
        operation: 'UPDATE',
        consumerGroup: 'order-service',
        success: true,
        targetCount: 1,
        message: 'saved',
        targets: [{ target: 'broker-a', kind: 'BROKER', success: true, message: 'saved' }]
      });
      suspension.resolve();
    });

    expect(screen.getByRole('heading', { name: 'order-service' })).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Edit order-service' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeDisabled();
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(consumerApi.summary).not.toHaveBeenCalled();
    expect(consumerApi.config).not.toHaveBeenCalled();
  });

});
