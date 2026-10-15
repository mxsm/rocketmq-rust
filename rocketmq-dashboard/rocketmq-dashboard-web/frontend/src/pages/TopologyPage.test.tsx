import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderAtRoute } from '../test/render';
import TopologyPage from './TopologyPage';
import { consumerGroup, fixtureConsumers, fixtureSnapshot } from './topology/topology-fixtures';
import { useTopologySnapshot, type TopologySnapshotState } from './topology/useTopologySnapshot';

vi.mock('./topology/useTopologySnapshot', () => ({ useTopologySnapshot: vi.fn() }));

const refresh = vi.fn();

function state(overrides: Partial<TopologySnapshotState> = {}): TopologySnapshotState & { refresh: () => void } {
  return {
    snapshot: fixtureSnapshot(),
    previous: null,
    changes: [],
    latestChangeCount: null,
    querying: false,
    error: null,
    nextRefreshAt: Date.now() + 10_000,
    refreshCount: 1,
    loadedAt: { config: 9_000, nameservers: 9_000, brokers: 9_000, topics: 9_000, producers: 9_000, consumers: 9_000 },
    refresh,
    ...overrides
  };
}

function renderPage(value = state()) {
  vi.mocked(useTopologySnapshot).mockReturnValue(value);
  return renderAtRoute(<TopologyPage />, '/topology');
}

describe('TopologyPage', () => {
  beforeEach(() => {
    refresh.mockReset();
  });

  it('holds the layout with a skeleton while the first snapshot loads', () => {
    renderPage(state({ snapshot: null, querying: true, refreshCount: 0 }));

    expect(screen.getByRole('status', { name: 'Loading topology' })).toBeInTheDocument();
    expect(screen.getByText('Querying snapshot…')).toBeInTheDocument();
  });

  it('shows an error with a retry when nothing could be loaded', async () => {
    const user = userEvent.setup();
    renderPage(state({ snapshot: null, error: 'Unable to reach the dashboard backend.' }));

    expect(screen.getByRole('alert')).toHaveTextContent('Unable to reach the dashboard backend.');
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it('draws clusters, the NameServer tier and the busiest groups with links to their pages', () => {
    renderPage();
    const map = screen.getByRole('group', { name: 'Cluster map' });

    expect(within(map).getByRole('region', { name: 'Cluster DefaultCluster' })).toBeInTheDocument();
    expect(within(map).getByRole('region', { name: 'Cluster cluster-east' })).toBeInTheDocument();
    expect(within(map).getByRole('region', { name: 'NameServers' })).toHaveTextContent('2 of 3 available');
    expect(within(map).getByRole('link', { name: /^Broker broker-c, .*no slave registered/ })).toHaveAttribute('href', '/brokers/broker-c');
    expect(within(map).getByRole('link', { name: /^Consumer group order-consumer,/ })).toHaveAttribute('href', '/consumers/order-consumer');
    expect(within(map).getByRole('link', { name: /^Producer group analytics-ingest,/ })).toHaveAttribute('href', '/producers');
    expect(map).toHaveAccessibleDescription(/no slave registered: broker-c/);
    expect(map.querySelectorAll('.topology-link[data-kind="produce"]').length).toBeGreaterThan(0);
  });

  it('summarises the snapshot in the signal rail and the panels', () => {
    renderPage();

    const rail = screen.getByRole('region', { name: 'Topology summary' });
    expect(rail).toHaveTextContent('NameServers2 of 3');
    expect(rail).toHaveTextContent('Brokers7 online');
    expect(rail).toHaveTextContent('Backlog12.84K');
    expect(screen.getAllByRole('meter', { name: 'Produce TPS' }).map((meter) => meter.getAttribute('aria-valuetext'))).toEqual([
      '624.10 TPS',
      '1,816.84 TPS'
    ]);
    expect(screen.getByText('Broker list').closest('li')).toHaveTextContent('40 msOK');
  });

  it('keeps the map and names the stale layers when a query failed', async () => {
    const user = userEvent.setup();
    const snapshot = fixtureSnapshot();
    renderPage(state({
      snapshot: {
        ...snapshot,
        sources: snapshot.sources.map((source) => (source.source === 'brokers' ? { ...source, status: 'failed', message: 'failed' } : source))
      }
    }));

    expect(screen.getByRole('region', { name: 'Cluster DefaultCluster' })).toBeInTheDocument();
    const notice = screen.getAllByRole('status').find((element) => element.classList.contains('topology-stale'))!;
    expect(notice).toHaveTextContent(/^Broker list could not be refreshed\. The map shows its data from /);
    expect(screen.getByText('Broker list').closest('li')).toHaveTextContent('Failed');
    await user.click(within(notice).getByRole('button', { name: 'Retry' }));
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it('guides to the NameServer settings when no broker is registered', () => {
    renderPage(state({ snapshot: fixtureSnapshot({ brokers: [], producers: [], consumers: [] }) }));

    expect(screen.getByText('No brokers are registered')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open OPS' })).toHaveAttribute('href', '/config');
    expect(screen.queryByRole('group', { name: 'Cluster map' })).not.toBeInTheDocument();
  });

  it('expands a column to every group and back', async () => {
    const user = userEvent.setup();
    const consumers = [
      ...fixtureConsumers,
      consumerGroup('extra-consumer-1', 1, 1, 0, ['broker-a']),
      consumerGroup('extra-consumer-2', 1, 1, 0, ['broker-c'])
    ];
    renderPage(state({ snapshot: fixtureSnapshot({ consumers }) }));
    const consumerLinks = () => screen.getAllByRole('link', { name: /^Consumer group / });

    expect(consumerLinks()).toHaveLength(6);
    await user.click(screen.getByRole('button', { name: 'Show all consumer groups' }));
    expect(consumerLinks()).toHaveLength(8);
    await user.click(screen.getByRole('button', { name: 'Show the top 6 consumer groups' }));
    expect(consumerLinks()).toHaveLength(6);
  });

  it('lists the changes of the latest refresh and announces one summary', () => {
    const { container } = renderPage(state({
      latestChangeCount: 2,
      changes: [
        { id: '2-0', at: 2, subject: 'broker-c', detail: 'slave no longer registered', tone: 'warning' },
        { id: '2-1', at: 2, subject: 'analytics-stream', detail: 'connections 14 → 16', tone: 'success' }
      ]
    }));

    const entries = [...container.querySelectorAll('.topology-change')].map((entry) => entry.textContent);
    expect(entries).toEqual(['broker-c slave no longer registered', 'analytics-stream connections 14 → 16']);
    expect(screen.getByText('2 changes since the last refresh.')).toBeInTheDocument();
  });
});
