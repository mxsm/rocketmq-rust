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

  it('invalidates destructive controls and navigates away when an applied delete is authoritatively absent', async () => {
    const user = userEvent.setup();
    vi.mocked(consumerApi.delete).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true })
    );
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await waitFor(() => expect(consumerApi.summary).toHaveBeenCalled());
    vi.clearAllMocks();
    vi.mocked(consumerApi.brokers).mockResolvedValue({
      items: [{ brokerName: 'broker-a', brokerAddress: '127.0.0.1:10911' }]
    });
    vi.mocked(consumerApi.summary).mockRejectedValueOnce(new ApiClientError('NOT_FOUND', 'Consumer group was not found.'));

    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const dialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'order-service');
    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));

    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    expect(await screen.findByRole('heading', { name: 'Consumer list' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete group' })).not.toBeInTheDocument();
  });

  it('closes an applied configuration edit and displays the one authoritative summary and config refresh without a retry', async () => {
    const user = userEvent.setup();
    const refreshedSummary = {
      group: 'order-service',
      displayGroupName: 'order-service',
      category: 'NORMAL',
      connectionCount: 9,
      consumeTps: 2,
      diffTotal: 4,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 1,
      queryScope: { mode: 'nameServer' as const }
    };
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes><Route path="/consumers/:group" element={<ConsumerDetailPage />} /></Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const dialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer configuration was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockResolvedValueOnce(refreshedSummary);
    vi.mocked(consumerApi.config).mockResolvedValueOnce(consumerConfig);

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));

    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(consumerApi.summary).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole('dialog', { name: 'Edit order-service' })).not.toBeInTheDocument();
    expect(await screen.findByRole('group', { name: 'Connections: 9' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Retry update' })).not.toBeInTheDocument();
  });

  it('keeps edit controls disabled when the authoritative applied-edit refresh fails', async () => {
    const user = userEvent.setup();
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes><Route path="/consumers/:group" element={<ConsumerDetailPage />} /></Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const dialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer configuration was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockRejectedValueOnce(new Error('authoritative summary unavailable'));

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));

    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Edit order-service' })).not.toBeInTheDocument());
    expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeDisabled();
  });

  it('drops a stale applied-edit refresh after the route changes to another consumer group', async () => {
    const user = userEvent.setup();
    const staleSummary = deferred<Awaited<ReturnType<typeof consumerApi.summary>>>();
    const staleConfig = deferred<Awaited<ReturnType<typeof consumerApi.config>>>();
    const inventorySummary = {
      group: 'inventory-service',
      displayGroupName: 'inventory-service',
      category: 'NORMAL',
      connectionCount: 17,
      consumeTps: 4,
      diffTotal: 2,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 2,
      queryScope: { mode: 'nameServer' as const }
    };
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route path="/consumers/:group" element={<><ConsumerGroupSwitch /><ConsumerDetailPage /></>} />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const dialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer configuration was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockImplementation((requestedGroup) => (
      requestedGroup === 'order-service' ? staleSummary.promise : Promise.resolve(inventorySummary)
    ));
    vi.mocked(consumerApi.progress).mockImplementation((requestedGroup) => Promise.resolve({
      group: requestedGroup,
      topicCount: 0,
      totalDiff: requestedGroup === 'inventory-service' ? 2 : 12,
      topics: [],
      queryScope: { mode: 'nameServer' as const }
    }));
    vi.mocked(consumerApi.config).mockImplementation((requestedGroup) => (
      requestedGroup === 'order-service' ? staleConfig.promise : Promise.resolve({ ...consumerConfig, group: requestedGroup })
    ));

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('button', { name: 'Open inventory group' }));
    await waitFor(() => expect(screen.getByRole('group', { name: 'Connections: 17' })).toBeInTheDocument());

    await act(async () => {
      staleSummary.resolve({
        group: 'order-service',
        displayGroupName: 'order-service',
        category: 'NORMAL',
        connectionCount: 99,
        consumeTps: 99,
        diffTotal: 99,
        messageModel: 'MESSAGE_MODEL_CLUSTERING',
        consumeType: 'CONSUME_PASSIVELY',
        version: null,
        versionDesc: '',
        brokerNames: [],
        brokerAddresses: [],
        updateTimestamp: 3,
        queryScope: { mode: 'nameServer' }
      });
      staleConfig.resolve(consumerConfig);
    });

    expect(screen.getByRole('heading', { name: 'inventory-service' })).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list' })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 17' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeEnabled();
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
  });

  it('discards a stale applied-delete NotFound after the consumer scope changes', async () => {
    const user = userEvent.setup();
    const staleSummary = deferred<Awaited<ReturnType<typeof consumerApi.summary>>>();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      environmentId: 'environment-default',
      environmentName: 'Default',
      revision: 1,
      endpoints: [
        { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
        { endpointId: 'proxy-1', endpointType: 'proxy', address: 'proxy-a:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 1 }
      ],
      currentNamesrv: '127.0.0.1:9876',
      namesrvAddrList: ['127.0.0.1:9876'],
      useVIPChannel: false,
      useTLS: false,
      currentProxyAddr: 'proxy-a:8081',
      proxyAddrList: ['proxy-a:8081'],
      storageBackend: 'sqlite',
      storageMode: 'singleNode'
    });
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <ConsumerScopeSwitch />
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const dialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'order-service');
    vi.clearAllMocks();
    vi.mocked(consumerApi.delete).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockImplementation((_group, requestedScope) => (
      requestedScope.mode === 'nameServer'
        ? staleSummary.promise
        : Promise.resolve({
          group: 'order-service',
          displayGroupName: 'order-service',
          category: 'NORMAL',
          connectionCount: 28,
          consumeTps: 1,
          diffTotal: 3,
          messageModel: 'MESSAGE_MODEL_CLUSTERING',
          consumeType: 'CONSUME_PASSIVELY',
          version: null,
          versionDesc: '',
          brokerNames: [],
          brokerAddresses: [],
          updateTimestamp: 4,
          queryScope: { mode: 'proxy' as const, proxyAddress: 'proxy-a:8081' }
        })
    ));
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service', topicCount: 0, totalDiff: 3, topics: [], queryScope: { mode: 'proxy', proxyAddress: 'proxy-a:8081' }
    });

    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('button', { name: 'Use proxy scope' }));
    await waitFor(() => expect(screen.getByRole('group', { name: 'Connections: 28' })).toBeInTheDocument());

    await act(async () => {
      staleSummary.reject(new ApiClientError('NOT_FOUND', 'Consumer group was not found.'));
    });

    expect(screen.getByRole('heading', { name: 'order-service' })).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list' })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 28' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Delete group' })).toBeEnabled();
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
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

  it('unmounts the old edit dialog when an applied edit settles in the commit-before-passive window', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const auditWarning = vi.fn();
    const inventorySummary = {
      group: 'inventory-service',
      displayGroupName: 'inventory-service',
      category: 'NORMAL',
      connectionCount: 41,
      consumeTps: 6,
      diffTotal: 2,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 6,
      queryScope: { mode: 'nameServer' as const }
    };
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><CommitWindowGroupSwitch onCommitted={() => {
              window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer edit was applied.' }));
              pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true }));
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
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));

    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('heading', { name: 'inventory-service', hidden: true })).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Edit inventory-service' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list', hidden: true })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 41', hidden: true })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration', hidden: true })).toBeEnabled());
    expect(vi.mocked(consumerApi.summary).mock.calls).toContainEqual(['order-service', { mode: 'nameServer' }]);
    expect(consumerApi.config).toHaveBeenCalledWith('order-service', { mode: 'nameServer' });
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('unmounts the old delete dialog when an applied delete settles in the commit-before-passive window', async () => {
    const user = userEvent.setup();
    const pendingDelete = deferred<Awaited<ReturnType<typeof consumerApi.delete>>>();
    const auditWarning = vi.fn();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      environmentId: 'environment-default',
      environmentName: 'Default',
      revision: 1,
      endpoints: [
        { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
        { endpointId: 'proxy-1', endpointType: 'proxy', address: 'proxy-a:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 1 }
      ],
      currentNamesrv: '127.0.0.1:9876',
      namesrvAddrList: ['127.0.0.1:9876'],
      useVIPChannel: false,
      useTLS: false,
      currentProxyAddr: 'proxy-a:8081',
      proxyAddrList: ['proxy-a:8081'],
      storageBackend: 'sqlite',
      storageMode: 'singleNode'
    });
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <CommitWindowScopeSwitch onCommitted={() => {
          window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer deletion was applied.' }));
          pendingDelete.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true }));
        }} />
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const nameServerDialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(nameServerDialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(nameServerDialog).getByLabelText('Confirm consumer group'), 'order-service');
    vi.clearAllMocks();
    vi.mocked(consumerApi.delete).mockImplementationOnce(() => pendingDelete.promise);
    vi.mocked(consumerApi.summary).mockResolvedValue({
      group: 'order-service',
      displayGroupName: 'order-service',
      category: 'NORMAL',
      connectionCount: 28,
      consumeTps: 1,
      diffTotal: 3,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 7,
      queryScope: { mode: 'proxy' as const, proxyAddress: 'proxy-a:8081' }
    });

    await user.click(within(nameServerDialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByText('Commit proxy scope'));
    await waitFor(() => expect(screen.getByRole('group', { name: 'Connections: 28', hidden: true })).toBeInTheDocument());
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));

    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('heading', { name: 'order-service', hidden: true })).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Delete consumer group' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Consumer list', hidden: true })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 28', hidden: true })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Delete group', hidden: true })).toBeEnabled());
    expect(vi.mocked(consumerApi.summary).mock.calls).toContainEqual(['order-service', { mode: 'nameServer' }]);
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
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

  it('disables B synchronously in a sibling layout effect while a pending A edit becomes applied', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const inventorySummary = deferred<ConsumerSummaryView>();
    const layoutSettled = vi.fn();
    const auditWarning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><LayoutGroupSwitch /><ConsumerDetailPage /><LayoutIdentityObserver group="inventory-service" scopeMode="nameServer" onSettled={(controls) => {
              layoutSettled(controls);
              window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer edit was applied.' }));
              pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true }));
            }} /></>}
          />
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
    vi.mocked(consumerApi.summary).mockImplementation((requestedGroup) => {
      if (requestedGroup === 'inventory-service') return inventorySummary.promise;
      return Promise.reject(new Error(`unexpected stale summary read for ${requestedGroup}`));
    });
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'inventory-service', topicCount: 0, totalDiff: 1, topics: [], queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.config).mockResolvedValue({ ...consumerConfig, group: 'inventory-service' });
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-b', brokerId: 0, address: '127.0.0.2:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
    });

    await user.click(within(orderDialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Commit layout inventory group', hidden: true }));

    await waitFor(() => expect(layoutSettled).toHaveBeenCalledTimes(1));
    expect(layoutSettled).toHaveBeenCalledWith({ editDisabled: true, deleteDisabled: true });
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole('dialog', { name: 'Edit inventory-service' })).not.toBeInTheDocument();
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(vi.mocked(consumerApi.summary).mock.calls).toContainEqual(['order-service', { mode: 'nameServer' }]);
    expect(consumerApi.config).toHaveBeenCalledWith('order-service', { mode: 'nameServer' });

    await act(async () => {
      inventorySummary.resolve({
        group: 'inventory-service', displayGroupName: 'inventory-service', category: 'NORMAL', connectionCount: 47,
        consumeTps: 7, diffTotal: 1, messageModel: 'MESSAGE_MODEL_CLUSTERING', consumeType: 'CONSUME_PASSIVELY',
        version: null, versionDesc: '', brokerNames: [], brokerAddresses: [], updateTimestamp: 10,
        queryScope: { mode: 'nameServer' }
      });
    });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    const inventoryDialog = await screen.findByRole('dialog', { name: 'Edit inventory-service' });
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(2));
    expect(within(inventoryDialog).getByRole('checkbox', { name: 'broker-b' })).not.toBeChecked();
    expect(consumerApi.config).toHaveBeenCalledWith('inventory-service', { mode: 'nameServer' });
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('disables the proxy scope synchronously in a sibling layout effect while a pending NameServer delete becomes applied', async () => {
    const user = userEvent.setup();
    const pendingDelete = deferred<Awaited<ReturnType<typeof consumerApi.delete>>>();
    const proxySummary = deferred<ConsumerSummaryView>();
    const layoutSettled = vi.fn();
    const auditWarning = vi.fn();
    vi.mocked(configApi.getConfig).mockResolvedValue({
      environmentId: 'environment-default', environmentName: 'Default', revision: 1,
      endpoints: [
        { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 },
        { endpointId: 'proxy-1', endpointType: 'proxy', address: 'proxy-a:8081', role: 'primary', isEnabled: true, isActive: true, sortOrder: 1 }
      ],
      currentNamesrv: '127.0.0.1:9876', namesrvAddrList: ['127.0.0.1:9876'], useVIPChannel: false, useTLS: false,
      currentProxyAddr: 'proxy-a:8081', proxyAddrList: ['proxy-a:8081'], storageBackend: 'sqlite', storageMode: 'singleNode'
    });
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <LayoutScopeSwitch />
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><ConsumerDetailPage /><LayoutIdentityObserver group="order-service" scopeMode="proxy" onSettled={(controls) => {
              layoutSettled(controls);
              window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer deletion was applied.' }));
              pendingDelete.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true }));
            }} /></>}
          />
        </Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const nameServerDialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(nameServerDialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(nameServerDialog).getByLabelText('Confirm consumer group'), 'order-service');

    vi.clearAllMocks();
    vi.mocked(consumerApi.delete).mockImplementationOnce(() => pendingDelete.promise);
    vi.mocked(consumerApi.summary).mockImplementation((_requestedGroup, requestedScope) => {
      if (requestedScope.mode === 'proxy') return proxySummary.promise;
      return Promise.reject(new Error('unexpected stale NameServer summary read'));
    });
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service', topicCount: 0, totalDiff: 2, topics: [], queryScope: { mode: 'proxy', proxyAddress: 'proxy-a:8081' }
    });

    await user.click(within(nameServerDialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Commit layout proxy scope', hidden: true }));

    await waitFor(() => expect(layoutSettled).toHaveBeenCalledTimes(1));
    expect(layoutSettled).toHaveBeenCalledWith({ editDisabled: true, deleteDisabled: true });
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole('dialog', { name: 'Delete consumer group' })).not.toBeInTheDocument();
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
    expect(vi.mocked(consumerApi.summary).mock.calls).toContainEqual(['order-service', { mode: 'nameServer' }]);

    await act(async () => {
      proxySummary.resolve({
        group: 'order-service', displayGroupName: 'order-service', category: 'NORMAL', connectionCount: 53,
        consumeTps: 9, diffTotal: 2, messageModel: 'MESSAGE_MODEL_CLUSTERING', consumeType: 'CONSUME_PASSIVELY',
        version: null, versionDesc: '', brokerNames: [], brokerAddresses: [], updateTimestamp: 11,
        queryScope: { mode: 'proxy', proxyAddress: 'proxy-a:8081' }
      });
    });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Delete group' })).toBeEnabled());
    expect(screen.queryByRole('dialog', { name: 'Delete consumer group' })).not.toBeInTheDocument();
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('drops an applied completion settled in the commit-before-passive unmount window', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const auditWarning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes>
          <Route
            path="/consumers/:group"
            element={<><CommitWindowUnmountSwitch onCommitted={() => {
              window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer edit was applied.' }));
              pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true }));
            }} /><ConsumerDetailPage /></>}
          />
          <Route path="/consumers" element={<h1>Consumer list</h1>} />
        </Routes>
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
    fireEvent.click(screen.getByText('Commit consumer list'));

    expect(await screen.findByRole('heading', { name: 'Consumer list' })).toBeInTheDocument();
    await waitFor(() => expect(auditWarning).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole('dialog', { name: 'Edit order-service', hidden: true })).not.toBeInTheDocument();
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(consumerApi.summary).toHaveBeenCalledWith('order-service', { mode: 'nameServer' });
    expect(consumerApi.config).toHaveBeenCalledWith('order-service', { mode: 'nameServer' });
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
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

  it('runs the A terminal applied refresh once after a suspended B render is discarded', async () => {
    const user = userEvent.setup();
    const suspension = deferred<void>();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const inventoryRendered = vi.fn();
    const auditWarning = vi.fn();
    const appliedSummary = {
      group: 'order-service',
      displayGroupName: 'order-service',
      category: 'NORMAL',
      connectionCount: 19,
      consumeTps: 1,
      diffTotal: 2,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 9,
      queryScope: { mode: 'nameServer' as const }
    };
    window.addEventListener('rocketmq-audit-warning', auditWarning);
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
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(1));
    await act(async () => undefined);
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockImplementationOnce(() => pendingUpdate.promise);
    vi.mocked(consumerApi.summary).mockResolvedValueOnce(appliedSummary);
    vi.mocked(consumerApi.config).mockResolvedValueOnce(consumerConfig);

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Start suspended inventory transition', hidden: true }));
    await waitFor(() => expect(inventoryRendered).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: 'Cancel suspended transition', hidden: true }));

    await act(async () => {
      window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer edit was applied.' }));
      pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true }));
      suspension.resolve();
    });

    await waitFor(() => expect(consumerApi.summary).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(1));
    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('dialog', { name: 'Edit order-service' })).not.toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Connections: 19' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Retry update' })).not.toBeInTheDocument();
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('navigates once for A NotFound after a suspended B render is discarded', async () => {
    const user = userEvent.setup();
    const suspension = deferred<void>();
    const pendingDelete = deferred<Awaited<ReturnType<typeof consumerApi.delete>>>();
    const inventoryRendered = vi.fn();
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <ConcurrentConsumerWorkspace suspension={suspension.promise} onInventoryRender={inventoryRendered} />
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Delete group' }));
    const dialog = await screen.findByRole('dialog', { name: 'Delete consumer group' });
    await user.click(await within(dialog).findByRole('checkbox', { name: 'broker-a' }));
    await user.type(within(dialog).getByLabelText('Confirm consumer group'), 'order-service');
    vi.clearAllMocks();
    vi.mocked(consumerApi.delete).mockImplementationOnce(() => pendingDelete.promise);
    vi.mocked(consumerApi.summary).mockRejectedValueOnce(new ApiClientError('NOT_FOUND', 'Consumer group was not found.'));

    await user.click(within(dialog).getByRole('button', { name: 'Delete consumer group' }));
    await waitFor(() => expect(consumerApi.delete).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Start suspended inventory transition', hidden: true }));
    await waitFor(() => expect(inventoryRendered).toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: 'Cancel suspended transition', hidden: true }));

    await act(async () => {
      pendingDelete.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer deletion was applied.', { mutationApplied: true }));
      suspension.resolve();
    });

    expect(await screen.findByRole('heading', { name: 'Consumer list' })).toBeInTheDocument();
    expect(screen.queryByRole('dialog', { name: 'Delete consumer group' })).not.toBeInTheDocument();
    expect(consumerApi.delete).toHaveBeenCalledTimes(1);
    expect(consumerApi.summary).toHaveBeenCalledTimes(1);
    expect(consumerApi.summary).toHaveBeenCalledWith('order-service', { mode: 'nameServer' });
  });

  it('retains an A edit lock through A-to-B-to-A until its terminal refresh settles once', async () => {
    const user = userEvent.setup();
    const pendingUpdate = deferred<Awaited<ReturnType<typeof consumerApi.update>>>();
    const pendingConfig = deferred<Awaited<ReturnType<typeof consumerApi.config>>>();
    const auditWarning = vi.fn();
    const orderSummary = {
      group: 'order-service', displayGroupName: 'order-service', category: 'NORMAL', connectionCount: 61,
      consumeTps: 2, diffTotal: 3, messageModel: 'MESSAGE_MODEL_CLUSTERING', consumeType: 'CONSUME_PASSIVELY',
      version: null, versionDesc: '', brokerNames: [], brokerAddresses: [], updateTimestamp: 12,
      queryScope: { mode: 'nameServer' as const }
    };
    const inventorySummary = {
      group: 'inventory-service', displayGroupName: 'inventory-service', category: 'NORMAL', connectionCount: 62,
      consumeTps: 4, diffTotal: 5, messageModel: 'MESSAGE_MODEL_CLUSTERING', consumeType: 'CONSUME_PASSIVELY',
      version: null, versionDesc: '', brokerNames: [], brokerAddresses: [], updateTimestamp: 13,
      queryScope: { mode: 'nameServer' as const }
    };
    window.addEventListener('rocketmq-audit-warning', auditWarning);
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <ConsumerABASwitch />
        <Routes>
          <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
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
    vi.mocked(consumerApi.summary).mockImplementation((requestedGroup) => Promise.resolve(
      requestedGroup === 'inventory-service' ? inventorySummary : orderSummary
    ));
    vi.mocked(consumerApi.config).mockImplementation(() => pendingConfig.promise);
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'order-service', topicCount: 0, totalDiff: 0, topics: [], queryScope: { mode: 'nameServer' }
    });

    await user.click(within(orderDialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Switch to inventory group', hidden: true }));
    await screen.findByRole('heading', { name: 'inventory-service' });
    fireEvent.click(screen.getByRole('button', { name: 'Switch to order group', hidden: true }));
    await screen.findByRole('heading', { name: 'order-service' });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeDisabled());

    const summaryCallsBeforeApplied = vi.mocked(consumerApi.summary).mock.calls.length;
    await act(async () => {
      window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: 'Consumer edit was applied.' }));
      pendingUpdate.reject(new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer edit was applied.', { mutationApplied: true }));
    });
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(1));
    expect(vi.mocked(consumerApi.summary).mock.calls).toHaveLength(summaryCallsBeforeApplied + 1);
    expect(vi.mocked(consumerApi.summary).mock.calls[vi.mocked(consumerApi.summary).mock.calls.length - 1]).toEqual(['order-service', { mode: 'nameServer' }]);
    expect(auditWarning).toHaveBeenCalledTimes(1);
    expect(consumerApi.update).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: /retry/i })).not.toBeInTheDocument();

    await act(async () => pendingConfig.resolve(consumerConfig));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeEnabled());
    window.removeEventListener('rocketmq-audit-warning', auditWarning);
  });

  it('applies the first applied-edit refresh for a new consumer identity exactly once', async () => {
    const user = userEvent.setup();
    const inventorySummary = {
      group: 'inventory-service',
      displayGroupName: 'inventory-service',
      category: 'NORMAL',
      connectionCount: 23,
      consumeTps: 3,
      diffTotal: 1,
      messageModel: 'MESSAGE_MODEL_CLUSTERING',
      consumeType: 'CONSUME_PASSIVELY',
      version: null,
      versionDesc: '',
      brokerNames: [],
      brokerAddresses: [],
      updateTimestamp: 5,
      queryScope: { mode: 'nameServer' as const }
    };
    const inventoryConfig = {
      ...consumerConfig,
      group: 'inventory-service',
      effective: { ...consumerConfig.effective!, retryMaxTimes: 31 },
      targets: consumerConfig.targets.map((target) => ({
        ...target,
        config: target.config ? { ...target.config, retryMaxTimes: 31 } : null
      }))
    };
    renderAtRoute(
      <ConsumerQueryScopeProvider>
        <Routes><Route path="/consumers/:group" element={<><ConsumerGroupSwitch /><ConsumerDetailPage /></>} /></Routes>
      </ConsumerQueryScopeProvider>,
      '/consumers/order-service'
    );
    await screen.findByRole('heading', { name: 'order-service' });
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    let dialog = await screen.findByRole('dialog', { name: 'Edit order-service' });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer configuration was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockResolvedValueOnce({
      ...inventorySummary,
      group: 'order-service',
      displayGroupName: 'order-service',
      connectionCount: 9
    });
    vi.mocked(consumerApi.config).mockResolvedValueOnce(consumerConfig);
    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.summary).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Edit order-service' })).not.toBeInTheDocument());
    await waitFor(() => expect(document.body).not.toHaveAttribute('data-scroll-locked'));
    vi.mocked(consumerApi.summary).mockResolvedValue(inventorySummary);
    vi.mocked(consumerApi.progress).mockResolvedValue({
      group: 'inventory-service', topicCount: 0, totalDiff: 1, topics: [], queryScope: { mode: 'nameServer' }
    });
    vi.mocked(consumerApi.config).mockResolvedValue(inventoryConfig);
    vi.mocked(brokerApi.list).mockResolvedValue({
      items: [{ clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 0, address: '127.0.0.1:10911', role: 'MASTER', version: 'V5_3_0', produceTps: 0, consumeTps: 0 }],
      total: 1
    });
    await user.click(screen.getByRole('button', { name: 'Open inventory group' }));
    await screen.findByRole('heading', { name: 'inventory-service' });
    await waitFor(() => expect(screen.getByRole('button', { name: 'Edit configuration' })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: 'Edit configuration' }));
    dialog = await screen.findByRole('dialog', { name: 'Edit inventory-service', hidden: true });
    await waitFor(() => expect(within(dialog).getByRole('checkbox', { name: 'broker-a' })).toBeChecked());
    vi.clearAllMocks();
    vi.mocked(consumerApi.update).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Consumer configuration was applied.', { mutationApplied: true })
    );
    vi.mocked(consumerApi.summary).mockResolvedValueOnce(inventorySummary);
    vi.mocked(consumerApi.config).mockResolvedValueOnce(inventoryConfig);

    await user.click(within(dialog).getByRole('button', { name: 'Update group' }));
    await waitFor(() => expect(consumerApi.update).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(consumerApi.summary).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(consumerApi.config).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('tab', { name: 'Configuration' }));

    expect(await screen.findAllByText('31')).toHaveLength(2);
    expect(consumerApi.config).toHaveBeenCalledTimes(1);
  });
});
