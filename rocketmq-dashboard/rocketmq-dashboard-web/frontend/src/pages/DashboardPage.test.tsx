import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { brokerApi } from '../api/broker_api';
import { dashboardApi } from '../api/dashboard_api';
import { renderAtRoute } from '../test/render';
import type { DashboardHistorySeries, DashboardOverview, DashboardTopicCurrent } from '../types/dashboard';
import DashboardPage from './DashboardPage';

vi.mock('../api/dashboard_api', () => ({
  dashboardApi: {
    overview: vi.fn(),
    topicCurrent: vi.fn(),
    brokerHistory: vi.fn(),
    topicHistory: vi.fn()
  }
}));

vi.mock('../api/broker_api', () => ({
  brokerApi: {
    list: vi.fn(),
    runtime: vi.fn()
  }
}));

const overview: DashboardOverview = {
  currentNamesrv: '127.0.0.1:9876',
  brokerCount: 3,
  topicCount: 12,
  consumerGroupCount: 5,
  producerCount: 7,
  messageBacklog: 1_250,
  systemStatus: 'UP'
};

const topicCurrent: DashboardTopicCurrent = {
  totalTopics: 2,
  topTopics: [
    { topic: 'orders', totalMsg: 9_000, inTps: 120, outTps: 90 },
    { topic: 'payments', totalMsg: 4_500, inTps: 75, outTps: 64 }
  ]
};

const brokerHistory: DashboardHistorySeries = {
  date: '2026-08-15',
  metric: 'brokerCount',
  collected: true,
  points: [{ timestamp: 1_776_220_800_000, value: 3 }]
};

const topicHistory: DashboardHistorySeries = {
  date: '2026-08-15',
  metric: 'topic',
  topicName: null,
  collected: true,
  points: [{ timestamp: 1_776_220_800_000, value: 2 }]
};

function mockSuccessfulDashboard() {
  vi.mocked(dashboardApi.overview).mockResolvedValue(overview);
  vi.mocked(dashboardApi.topicCurrent).mockResolvedValue(topicCurrent);
  vi.mocked(dashboardApi.brokerHistory).mockResolvedValue(brokerHistory);
  vi.mocked(dashboardApi.topicHistory).mockResolvedValue(topicHistory);
  vi.mocked(brokerApi.list).mockResolvedValue({ items: [], total: 0 });
}

describe('DashboardPage', () => {
  beforeEach(() => {
    mockSuccessfulDashboard();
  });

  it('renders loading then operational metrics and evidence-based advisories', async () => {
    let resolveOverview: (value: DashboardOverview) => void = () => undefined;
    vi.mocked(dashboardApi.overview).mockReturnValue(new Promise((resolve) => { resolveOverview = resolve; }));
    renderAtRoute(<DashboardPage />);

    expect(screen.getByRole('status', { name: 'Loading dashboard' })).toBeInTheDocument();
    resolveOverview(overview);

    expect(await screen.findByRole('heading', { name: 'Operations overview' })).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'Brokers: 3' })).toBeInTheDocument();
    expect(screen.getByText('Consumer backlog requires review')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Inspect consumers' })).toHaveAttribute('href', '/consumers');
    expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    expect(screen.getByText(/Topic count/)).toBeInTheDocument();
  });

  it('keeps the dashboard usable when a history series is unavailable', async () => {
    vi.mocked(dashboardApi.brokerHistory).mockRejectedValue(new Error('history offline'));
    renderAtRoute(<DashboardPage />);

    expect(await screen.findByRole('heading', { name: 'Operations overview' })).toBeInTheDocument();
    expect(screen.getByText('Broker history unavailable')).toBeInTheDocument();
    expect(screen.getByRole('status', { name: 'Unavailable' })).toBeInTheDocument();
    expect(screen.getByText('Topic activity')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('shows a primary error and retries core dashboard requests', async () => {
    const user = userEvent.setup();
    vi.mocked(dashboardApi.overview).mockRejectedValueOnce(new Error('overview offline')).mockResolvedValue(overview);
    renderAtRoute(<DashboardPage />);

    expect(await screen.findByRole('alert')).toHaveTextContent('overview offline');
    await user.click(screen.getByRole('button', { name: 'Retry' }));

    expect(await screen.findByRole('heading', { name: 'Operations overview' })).toBeInTheDocument();
    expect(dashboardApi.overview).toHaveBeenCalledTimes(2);
  });

  it('queries selected history filters and refreshes without resetting them', async () => {
    const user = userEvent.setup();
    renderAtRoute(<DashboardPage />);
    expect(await screen.findByRole('heading', { name: 'Operations overview' })).toBeInTheDocument();

    const date = screen.getByLabelText('History date');
    const topic = screen.getByRole('combobox', { name: 'Topic history filter' });
    await user.clear(date);
    await user.type(date, '2026-08-14');
    await user.selectOptions(topic, 'payments');

    await waitFor(() => expect(dashboardApi.topicHistory).toHaveBeenLastCalledWith({ date: '2026-08-14', topicName: 'payments' }));
    const callsBeforeRefresh = vi.mocked(dashboardApi.overview).mock.calls.length;
    await user.click(screen.getByRole('button', { name: 'Refresh' }));

    await waitFor(() => expect(dashboardApi.overview).toHaveBeenCalledTimes(callsBeforeRefresh + 1));
    expect(date).toHaveValue('2026-08-14');
    expect(topic).toHaveValue('payments');
  });

  it('keeps history loading until the latest topic filter request resolves', async () => {
    const user = userEvent.setup();
    let resolveOrders: (value: DashboardHistorySeries) => void = () => undefined;
    let resolvePayments: (value: DashboardHistorySeries) => void = () => undefined;
    vi.mocked(dashboardApi.topicHistory).mockImplementation(({ topicName }) => {
      if (topicName === 'orders') return new Promise((resolve) => { resolveOrders = resolve; });
      if (topicName === 'payments') return new Promise((resolve) => { resolvePayments = resolve; });
      return Promise.resolve(topicHistory);
    });
    renderAtRoute(<DashboardPage />);
    await screen.findByRole('heading', { name: 'Operations overview' });

    const topic = screen.getByRole('combobox', { name: 'Topic history filter' });
    await user.selectOptions(topic, 'orders');
    await waitFor(() => expect(dashboardApi.topicHistory).toHaveBeenLastCalledWith(expect.objectContaining({ topicName: 'orders' })));
    await user.selectOptions(topic, 'payments');
    await waitFor(() => expect(dashboardApi.topicHistory).toHaveBeenLastCalledWith(expect.objectContaining({ topicName: 'payments' })));
    expect(screen.getByRole('status', { name: 'Loading topic history' })).toBeInTheDocument();

    resolveOrders({ ...topicHistory, topicName: 'orders', points: [{ timestamp: 1_776_220_800_000, value: 9_000 }] });
    await Promise.resolve();
    expect(screen.getByRole('status', { name: 'Loading topic history' })).toBeInTheDocument();

    resolvePayments({ ...topicHistory, topicName: 'payments', points: [{ timestamp: 1_776_220_800_000, value: 4_500 }] });
    await waitFor(() => expect(screen.queryByRole('status', { name: 'Loading topic history' })).not.toBeInTheDocument());
    expect(topic).toHaveValue('payments');
  });
});
