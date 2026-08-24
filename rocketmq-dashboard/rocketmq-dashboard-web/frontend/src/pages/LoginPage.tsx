import { Activity, LogIn } from 'lucide-react';
import { FormEvent, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { authApi } from '../api/auth_api';
import { ApiClientError } from '../api/client';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import { Button } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import { Input } from '../components/ui/Input';
import { Label } from '../components/ui/Label';
import './LoginPage.css';

export default function LoginPage() {
  const navigate = useNavigate();
  const passwordInputRef = useRef<HTMLInputElement>(null);
  const submittingRef = useRef(false);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [checking, setChecking] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [sessionNotice, setSessionNotice] = useState<string | null>(null);
  const [credentialError, setCredentialError] = useState<string | null>(null);

  const checkSession = async () => {
    setChecking(true);
    setSessionError(null);
    setSessionNotice(null);
    try {
      const session = await authApi.session();
      if (!session.loginRequired || session.authenticated) {
        navigate('/dashboard', { replace: true });
      } else if (session.authReason) {
        setSessionNotice(sessionReason(session.authReason));
      }
    } catch {
      setSessionError('Unable to load auth session.');
    } finally {
      setChecking(false);
    }
  };

  useEffect(() => {
    void checkSession();
  }, [navigate]);

  useEffect(() => {
    if (credentialError && !submitting) {
      passwordInputRef.current?.focus();
    }
  }, [credentialError, submitting]);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (submittingRef.current) {
      return;
    }

    submittingRef.current = true;
    setCredentialError(null);
    setSubmitting(true);
    try {
      await authApi.login({ username, password });
      navigate('/dashboard', { replace: true });
    } catch (err) {
      setPassword('');
      setCredentialError(err instanceof ApiClientError ? err.message : 'Login failed.');
    } finally {
      submittingRef.current = false;
      setSubmitting(false);
    }
  };

  if (checking) {
    return (
      <main className="login-shell">
        <LoadingState label="Checking session" />
      </main>
    );
  }

  if (sessionError) {
    return (
      <main className="login-shell">
        <ErrorState message={sessionError} onRetry={() => void checkSession()} />
      </main>
    );
  }

  return (
    <main className="login-shell">
      <div className="login-brand" aria-label="RocketMQ Operations">
        <div className="login-brand-mark" aria-hidden="true">
          <Activity size={22} />
        </div>
        <div>
          <strong>RocketMQ</strong>
          <span>Operations</span>
        </div>
      </div>

      <div className="login-content">
        <p className="login-session-note" role="status">
          <LogIn size={18} aria-hidden="true" />
          Authentication is required for this dashboard.
        </p>
        {sessionNotice ? <p className="login-session-notice" role="status">{sessionNotice}</p> : null}
        <Card className="login-card">
          <CardHeader>
            <CardTitle>Sign in to RocketMQ Operations</CardTitle>
            <CardDescription>Use your dashboard credentials to continue.</CardDescription>
          </CardHeader>
          <CardContent>
            <form className="login-form" onSubmit={submit} aria-busy={submitting}>
              <div className="login-field">
                <Label htmlFor="login-username">Username</Label>
                <Input
                  id="login-username"
                  value={username}
                  autoComplete="username"
                  placeholder="Enter your username"
                  disabled={submitting}
                  onChange={(event) => setUsername(event.target.value)}
                />
              </div>
              <div className="login-field">
                <Label htmlFor="login-password">Password</Label>
                <Input
                  ref={passwordInputRef}
                  id="login-password"
                  value={password}
                  type="password"
                  autoComplete="current-password"
                  placeholder="Enter your password"
                  disabled={submitting}
                  onChange={(event) => setPassword(event.target.value)}
                />
              </div>
              {credentialError ? <ErrorState message={credentialError} /> : null}
              <Button type="submit" className="login-submit" loading={submitting} disabled={!username || !password}>
                {submitting ? 'Signing in' : 'Sign in'}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>

      <p className="login-footer">RocketMQ Dashboard Web</p>
    </main>
  );
}

function sessionReason(reason: NonNullable<import('../types/auth').SessionView['authReason']>) {
  switch (reason) {
    case 'expired': return 'Your dashboard session expired. Sign in again to continue.';
    case 'revoked': return 'Your dashboard session was revoked. Sign in again to continue.';
    default: return 'Your previous dashboard session is no longer valid. Sign in again to continue.';
  }
}
