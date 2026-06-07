import { LogIn } from 'lucide-react';
import { FormEvent, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { authApi } from '../api/auth_api';
import { ApiClientError } from '../api/client';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';

export default function LoginPage() {
  const navigate = useNavigate();
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [checking, setChecking] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    authApi
      .session()
      .then((session) => {
        if (!session.loginRequired || session.authenticated) {
          navigate('/dashboard', { replace: true });
        }
      })
      .catch(() => {
        setError('Unable to load auth session.');
      })
      .finally(() => setChecking(false));
  }, [navigate]);

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setSubmitting(true);
    authApi
      .login({ username, password })
      .then(() => navigate('/dashboard', { replace: true }))
      .catch((err: unknown) => {
        setError(err instanceof ApiClientError ? err.message : 'Login failed.');
      })
      .finally(() => setSubmitting(false));
  };

  if (checking) {
    return (
      <main className="login-shell">
        <LoadingState label="Checking session" />
      </main>
    );
  }

  return (
    <main className="login-shell">
      <form className="login-panel" onSubmit={submit}>
        <div className="login-mark" aria-hidden="true">
          <LogIn size={22} />
        </div>
        <h1>RocketMQ Dashboard</h1>
        <label className="field">
          Username
          <input value={username} autoComplete="username" onChange={(event) => setUsername(event.target.value)} />
        </label>
        <label className="field">
          Password
          <input
            value={password}
            type="password"
            autoComplete="current-password"
            onChange={(event) => setPassword(event.target.value)}
          />
        </label>
        {error ? <ErrorState message={error} /> : null}
        <button type="submit" className="button" disabled={submitting || !username || !password}>
          {submitting ? 'Signing in' : 'Sign in'}
        </button>
      </form>
    </main>
  );
}
