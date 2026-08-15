import type { ReactNode } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { vi } from 'vitest';
import App from './App';

function pageModule(title: string) {
  return {
    default: () => <h1>{title}</h1>
  };
}

vi.mock('./layouts/AppLayout', () => ({
  default: ({ children }: { children: ReactNode }) => (
    <>
      <nav aria-label="Dashboard navigation">Dashboard navigation</nav>
      {children}
    </>
  )
}));

vi.mock('./pages/AclPage', () => pageModule('ACL workspace'));
vi.mock('./pages/BrokerDetailPage', () => pageModule('Broker detail workspace'));
vi.mock('./pages/BrokerListPage', () => pageModule('Brokers workspace'));
vi.mock('./pages/ConfigPage', () => pageModule('Configuration workspace'));
vi.mock('./pages/ConsumerDetailPage', () => pageModule('Consumer detail workspace'));
vi.mock('./pages/ConsumerListPage', () => pageModule('Consumers workspace'));
vi.mock('./pages/DashboardPage', () => pageModule('Dashboard workspace'));
vi.mock('./pages/DlqMessagePage', () => pageModule('DLQ messages workspace'));
vi.mock('./pages/LoginPage', () => pageModule('Login workspace'));
vi.mock('./pages/MessageQueryPage', () => pageModule('Messages workspace'));
vi.mock('./pages/MessageTracePage', () => pageModule('Message trace workspace'));
vi.mock('./pages/MonitorPage', () => pageModule('Monitors workspace'));
vi.mock('./pages/ProducerListPage', () => pageModule('Producers workspace'));
vi.mock('./pages/ProxyPage', () => pageModule('Proxy workspace'));
vi.mock('./pages/TopicDetailPage', () => pageModule('Topic detail workspace'));
vi.mock('./pages/TopicListPage', () => pageModule('Topics workspace'));

function renderApp(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <App />
    </MemoryRouter>
  );
}

function LocationProbe() {
  const location = useLocation();
  return <output aria-label="Current route">{location.pathname}</output>;
}

const canonicalPages = [
  ['/login', 'Login workspace'],
  ['/proxy', 'Proxy workspace'],
  ['/dashboard', 'Dashboard workspace'],
  ['/topics', 'Topics workspace'],
  ['/topics/Orders', 'Topic detail workspace'],
  ['/consumers', 'Consumers workspace'],
  ['/consumers/reporting', 'Consumer detail workspace'],
  ['/producers', 'Producers workspace'],
  ['/brokers', 'Brokers workspace'],
  ['/brokers/broker-a', 'Broker detail workspace'],
  ['/messages', 'Messages workspace'],
  ['/messages/dlq', 'DLQ messages workspace'],
  ['/message-trace', 'Message trace workspace'],
  ['/acl', 'ACL workspace'],
  ['/monitors', 'Monitors workspace'],
  ['/config', 'Configuration workspace']
] as const;

describe('App routes', () => {
  it('shows a named loading state while a route module resolves', async () => {
    renderApp('/dashboard');

    expect(screen.getByRole('status', { name: 'Loading dashboard workspace' })).toBeInTheDocument();
    expect(await screen.findByRole('heading', { name: 'Dashboard workspace' })).toBeInTheDocument();
  });

  it.each(canonicalPages)('renders the %s canonical route', async (path, title) => {
    renderApp(path);

    expect(await screen.findByRole('heading', { name: title })).toBeInTheDocument();
  });

  it('keeps the login workspace outside dashboard navigation', async () => {
    renderApp('/login');

    expect(await screen.findByRole('heading', { name: 'Login workspace' })).toBeInTheDocument();
    expect(screen.queryByRole('navigation', { name: 'Dashboard navigation' })).not.toBeInTheDocument();
  });

  it.each([
    ['/ops', '/config'],
    ['/cluster', '/brokers'],
    ['/dlq', '/messages/dlq'],
    ['/', '/dashboard'],
    ['/does-not-exist', '/dashboard']
  ])('redirects %s to %s', async (path, destination) => {
    render(
      <MemoryRouter initialEntries={[path]} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <App />
        <LocationProbe />
      </MemoryRouter>
    );

    await waitFor(() => expect(screen.getByRole('status', { name: 'Current route' })).toHaveTextContent(destination));
  });
});
