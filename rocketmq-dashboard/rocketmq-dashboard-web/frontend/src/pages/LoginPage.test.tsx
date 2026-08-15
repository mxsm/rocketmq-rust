import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { authApi } from '../api/auth_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import LoginPage from './LoginPage';

const navigate = vi.fn();

vi.mock('../api/auth_api', () => ({
  authApi: {
    session: vi.fn(),
    login: vi.fn()
  }
}));

vi.mock('react-router-dom', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router-dom')>()),
  useNavigate: () => navigate
}));

const mockedAuthApi = vi.mocked(authApi);

const requiredSession = {
  loginRequired: true,
  authenticated: false
};

describe('LoginPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockedAuthApi.session.mockResolvedValue(requiredSession);
  });

  it('shows an accessible session-checking state before rendering credentials', () => {
    mockedAuthApi.session.mockReturnValue(new Promise(() => undefined));

    renderAtRoute(<LoginPage />, '/login');

    expect(screen.getByRole('status', { name: 'Checking session' })).toBeInTheDocument();
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
  });

  it('shows a retryable session error before rendering credentials, then shows them after retry', async () => {
    const user = userEvent.setup();
    mockedAuthApi.session.mockRejectedValueOnce(new Error('session unavailable')).mockResolvedValueOnce(requiredSession);

    renderAtRoute(<LoginPage />, '/login');

    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to load auth session.');
    expect(screen.queryByLabelText('Username')).not.toBeInTheDocument();
    expect(screen.queryByText('Authentication is required for this dashboard.')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Retry' }));

    expect(await screen.findByLabelText('Username')).toBeInTheDocument();
    expect(mockedAuthApi.session).toHaveBeenCalledTimes(2);
  });

  it('redirects with replacement when login is disabled', async () => {
    mockedAuthApi.session.mockResolvedValue({ loginRequired: false, authenticated: false });

    renderAtRoute(<LoginPage />, '/login');

    await waitFor(() => expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true }));
  });

  it('redirects with replacement when the current session is already authenticated', async () => {
    mockedAuthApi.session.mockResolvedValue({ loginRequired: true, authenticated: true, username: 'operator' });

    renderAtRoute(<LoginPage />, '/login');

    await waitFor(() => expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true }));
  });

  it('submits credentials and redirects with replacement after successful login', async () => {
    const user = userEvent.setup();
    mockedAuthApi.login.mockResolvedValue({ ...requiredSession, authenticated: true, username: 'operator' });

    renderAtRoute(<LoginPage />, '/login');
    const username = await screen.findByLabelText('Username');
    const password = screen.getByLabelText('Password');
    expect(username).toHaveValue('');
    expect(password).toHaveValue('');

    await user.type(username, 'operator');
    await user.type(password, 'correct-password');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));

    await waitFor(() => expect(mockedAuthApi.login).toHaveBeenCalledWith({ username: 'operator', password: 'correct-password' }));
    await waitFor(() => expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true }));
  });

  it('reports invalid credentials without exposing the password', async () => {
    const user = userEvent.setup();
    mockedAuthApi.login.mockRejectedValue(new ApiClientError('AUTH_ERROR', 'Invalid username or password.'));

    renderAtRoute(<LoginPage />, '/login');
    const username = await screen.findByLabelText('Username');
    const password = screen.getByLabelText('Password');
    await user.type(username, 'operator');
    await user.type(password, 'incorrect-password');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Invalid username or password.');
    expect(screen.queryByText('incorrect-password')).not.toBeInTheDocument();
  });

  it('preserves the username, clears the password, and restores password focus after login failure', async () => {
    const user = userEvent.setup();
    mockedAuthApi.login.mockRejectedValue(new ApiClientError('AUTH_ERROR', 'Invalid username or password.'));

    renderAtRoute(<LoginPage />, '/login');
    const username = await screen.findByLabelText('Username');
    const password = screen.getByLabelText('Password');
    await user.type(username, 'operator');
    await user.type(password, 'incorrect-password');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));

    await screen.findByRole('alert');
    expect(username).toHaveValue('operator');
    expect(password).toHaveValue('');
    await waitFor(() => expect(password).toHaveFocus());
  });

  it('prevents duplicate login submissions while a request is pending', async () => {
    const user = userEvent.setup();
    let resolveLogin: ((value: typeof requiredSession) => void) | undefined;
    mockedAuthApi.login.mockReturnValue(new Promise((resolve) => {
      resolveLogin = resolve;
    }));

    renderAtRoute(<LoginPage />, '/login');
    await user.type(await screen.findByLabelText('Username'), 'operator');
    await user.type(screen.getByLabelText('Password'), 'correct-password');
    const submit = screen.getByRole('button', { name: 'Sign in' });
    await user.click(submit);
    await user.click(submit);

    expect(mockedAuthApi.login).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: 'Signing in' })).toBeDisabled();
    resolveLogin?.(requiredSession);
    await waitFor(() => expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true }));
  });
});
