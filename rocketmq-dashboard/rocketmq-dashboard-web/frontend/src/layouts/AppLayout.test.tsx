import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { useLocation } from 'react-router-dom';
import { authApi } from '../api/auth_api';
import { configApi } from '../api/config_api';
import { authenticatedSession, configuredDashboard } from '../test/fixtures';
import { renderAtRoute } from '../test/render';
import AppLayout from './AppLayout';

function LocationProbe() {
  const location = useLocation();
  return <output aria-label="Current route">{location.pathname}</output>;
}

vi.mock('../api/auth_api', () => ({
  authApi: {
    session: vi.fn(),
    logout: vi.fn()
  }
}));

vi.mock('../api/config_api', () => ({
  configApi: {
    getConfig: vi.fn()
  }
}));

describe('AppLayout', () => {
  beforeEach(() => {
    vi.mocked(authApi.session).mockResolvedValue(authenticatedSession);
    vi.mocked(configApi.getConfig).mockResolvedValue(configuredDashboard);
  });

  it('keeps the operations shell dark and exposes no theme switch', async () => {
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);

    await waitFor(() => expect(document.documentElement.dataset.theme).toBe('dark'));
    expect(screen.queryByRole('button', { name: /toggle theme/i })).not.toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: /primary/i })).toBeInTheDocument();
  });

  it('shows operational routes without treating login as primary navigation', async () => {
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);

    expect(await screen.findByRole('link', { name: 'Dashboard' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Cluster' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'ACL Management' })).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Login' })).not.toBeInTheDocument();
  });

  it('opens responsive navigation from the header', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);

    await user.click(screen.getByRole('button', { name: 'Open navigation' }));
    const navigation = screen.getByRole('dialog', { name: 'Navigation' });
    await user.click(within(navigation).getByRole('link', { name: 'Topic' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Navigation' })).not.toBeInTheDocument());
  });

  it('returns focus to the responsive navigation opener when the sheet closes', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);

    const trigger = screen.getByRole('button', { name: 'Open navigation' });
    await user.click(trigger);
    expect(screen.getByRole('dialog', { name: 'Navigation' })).toBeInTheDocument();
    await user.keyboard('{Escape}');

    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Navigation' })).not.toBeInTheDocument());
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it('dismisses mobile navigation when the viewport crosses into desktop layout', async () => {
    const user = userEvent.setup();
    const originalWidth = window.innerWidth;
    Object.defineProperty(window, 'innerWidth', { configurable: true, value: 768 });

    try {
      renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);
      const trigger = screen.getByRole('button', { name: 'Open navigation' });
      await user.click(trigger);
      expect(screen.getByRole('dialog', { name: 'Navigation' })).toBeInTheDocument();

      Object.defineProperty(window, 'innerWidth', { configurable: true, value: 1200 });
      fireEvent(window, new Event('resize'));

      await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Navigation' })).not.toBeInTheDocument());
      expect(trigger).not.toHaveFocus();
    } finally {
      Object.defineProperty(window, 'innerWidth', { configurable: true, value: originalWidth });
    }
  });

  it('marks only the most specific message route as current', async () => {
    renderAtRoute(<AppLayout><h1>DLQ content</h1></AppLayout>, '/messages/dlq');

    const currentLinks = (await screen.findAllByRole('link')).filter((link) => link.getAttribute('aria-current') === 'page');
    expect(currentLinks).toHaveLength(1);
    expect(currentLinks[0]).toHaveAccessibleName('DLQ Message');
  });

  it('refreshes the visible NameServer configuration', async () => {
    const user = userEvent.setup();
    vi.mocked(configApi.getConfig)
      .mockResolvedValueOnce(configuredDashboard)
      .mockResolvedValueOnce({
        ...configuredDashboard,
        revision: 2,
        endpoints: [{ endpointId: 'nameserver-2', endpointType: 'nameserver', address: '10.0.0.8:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }]
      });
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);

    expect(await screen.findByText(/127\.0\.0\.1:9876/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Refresh configuration' }));

    expect(await screen.findByText(/10\.0\.0\.8:9876/)).toBeInTheDocument();
    expect(configApi.getConfig).toHaveBeenCalledTimes(2);
  });

  it('refreshes the Header from typed endpoints after a persisted configuration update event', async () => {
    vi.mocked(configApi.getConfig)
      .mockResolvedValueOnce(configuredDashboard)
      .mockResolvedValueOnce({
        ...configuredDashboard,
        revision: 2,
        endpoints: [{ endpointId: 'nameserver-2', endpointType: 'nameserver', address: '10.0.0.9:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }]
      });
    renderAtRoute(<AppLayout><h1>Dashboard content</h1></AppLayout>);
    expect(await screen.findByText(/127\.0\.0\.1:9876/)).toBeInTheDocument();

    act(() => { window.dispatchEvent(new CustomEvent('rocketmq-config-updated')); });

    expect(await screen.findByText(/10\.0\.0\.9:9876/)).toBeInTheDocument();
  });

  it('navigates signed-out operators to the sign-in route', async () => {
    const user = userEvent.setup();
    vi.mocked(authApi.session).mockResolvedValue({ loginRequired: true, authenticated: false });
    renderAtRoute(
      <>
        <AppLayout><h1>Dashboard content</h1></AppLayout>
        <LocationProbe />
      </>
    );

    await user.click(await screen.findByRole('button', { name: 'Sign in' }));
    expect(screen.getByRole('status', { name: 'Current route' })).toHaveTextContent('/login');
  });
});
