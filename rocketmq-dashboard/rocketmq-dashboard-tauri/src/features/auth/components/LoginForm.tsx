import { useId, useState } from 'react';
import { motion, useReducedMotion } from 'motion/react';
import { ArrowRight, Loader2, Lock, User } from 'lucide-react';
import { useAuth } from '../hooks/useAuth';

export function LoginForm() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const { isLoading, error, login, clearError } = useAuth();
    const formId = useId();
    const reduceMotion = useReducedMotion();

    return (
        <motion.section
            className="auth-form-wrap"
            aria-labelledby={`${formId}-title`}
            initial={reduceMotion ? false : { opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.2, ease: 'easeOut' }}
        >
            <div className="auth-card">
                <span className="auth-card-light" aria-hidden="true" />
                <div className="auth-card-header">
                    <div className="auth-form-kicker-row">
                        <span className="auth-form-kicker">Admin access</span>
                        <span className="auth-form-chip">Local account</span>
                    </div>
                    <h1 id={`${formId}-title`}>Sign in to your account</h1>
                    <p>Use the local administrator credentials to access the dashboard.</p>
                </div>
                <form
                    className="auth-form"
                    aria-busy={isLoading}
                    onSubmit={event => {
                        event.preventDefault();
                        if (!isLoading) void login({ username, password });
                    }}
                >
                    <div className="auth-fields">
                        <div className="auth-field">
                            <label htmlFor={`${formId}-username`}>Username</label>
                            <div className="relative">
                                <div className="auth-field-icon"><User aria-hidden="true" /></div>
                                <input
                                    id={`${formId}-username`}
                                    className="auth-input"
                                    name="username"
                                    autoComplete="username"
                                    placeholder="Enter your username"
                                    required
                                    disabled={isLoading}
                                    value={username}
                                    onChange={event => { setUsername(event.target.value); clearError(); }}
                                />
                            </div>
                        </div>
                        <div className="auth-field">
                            <label htmlFor={`${formId}-password`}>Password</label>
                            <div className="relative">
                                <div className="auth-field-icon"><Lock aria-hidden="true" /></div>
                                <input
                                    id={`${formId}-password`}
                                    className="auth-input"
                                    name="password"
                                    type="password"
                                    autoComplete="current-password"
                                    placeholder="Enter your password"
                                    required
                                    disabled={isLoading}
                                    value={password}
                                    onChange={event => { setPassword(event.target.value); clearError(); }}
                                />
                            </div>
                        </div>
                    </div>
                    {error && <div className="auth-login-error" role="alert"><strong>Sign-in failed. </strong>{error}</div>}
                    <button className="auth-submit" type="submit" disabled={isLoading}>
                        {isLoading ? <><Loader2 className="animate-spin" aria-hidden="true" />Signing in…</> : <>Sign in<ArrowRight aria-hidden="true" /></>}
                    </button>
                </form>
                <div className="auth-footnote">
                    <p>First-time sign-in requires a password change before the dashboard becomes available.</p>
                </div>
            </div>
        </motion.section>
    );
}
