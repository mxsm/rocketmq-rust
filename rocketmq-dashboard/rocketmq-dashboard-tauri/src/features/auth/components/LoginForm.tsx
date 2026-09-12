import { useState } from 'react';
import { useAuth } from '../hooks/useAuth';
import { Input } from '../../../components/ui/LegacyInput';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';

export function LoginForm() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const { isLoading, error, login, clearError } = useAuth();
    return <section className="ops-auth-card" aria-labelledby="login-title">
        <h1 id="login-title">Sign in</h1><p>Use your local dashboard account.</p>
        <form onSubmit={event => { event.preventDefault(); if (!isLoading) void login({ username, password }); }}>
            <Input label="Username" autoComplete="username" required disabled={isLoading} value={username} onChange={event => { setUsername(event.target.value); clearError(); }} />
            <Input label="Password" type="password" autoComplete="current-password" required disabled={isLoading} value={password} onChange={event => { setPassword(event.target.value); clearError(); }} />
            {error && <PageState kind="error" title="Sign-in failed" description={error} />}
            <Button type="submit" disabled={isLoading}>{isLoading ? 'Signing in…' : 'Sign in'}</Button>
        </form>
        <p className="ops-auth-footnote">First-time sign-in requires a password change before the dashboard becomes available.</p>
    </section>;
}
