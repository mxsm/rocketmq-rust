import React, { useState } from 'react';
import { motion } from 'motion/react';
import { User, Lock, ArrowRight, Loader2 } from 'lucide-react';
import { useAuth } from '../hooks/useAuth';
import { ErrorAlert } from './ErrorAlert';

export const LoginForm: React.FC = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const { isLoading, error, shake, login, clearError } = useAuth();

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        await login({ username, password });
    };

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{
                opacity: 1,
                y: 0,
                x: shake ? [0, -10, 10, -10, 10, -5, 5, 0] : 0,
            }}
            transition={{
                opacity: { duration: 0.8, delay: 0.2, ease: 'easeOut' },
                y: { duration: 0.8, delay: 0.2, ease: 'easeOut' },
                x: { duration: 0.65 },
            }}
            className="auth-form-wrap"
        >
            <div className="auth-card">
                <div className="auth-card-header">
                    <span className="auth-form-kicker">Admin access</span>
                    <h1>
                        Sign in to your account
                    </h1>
                    <p>
                        Use the local administrator credentials to access the dashboard.
                    </p>
                </div>

                <form onSubmit={handleSubmit} className="auth-form">
                    <div className="auth-fields">
                        <div className="auth-field">
                            <label>
                                Username
                            </label>
                            <div className="relative">
                                <div className="auth-field-icon">
                                    <User className="h-4 w-4" />
                                </div>
                                <input
                                    type="text"
                                    value={username}
                                    onChange={(e) => {
                                        setUsername(e.target.value);
                                        if (error) {
                                            clearError();
                                        }
                                    }}
                                    className="auth-input"
                                    placeholder="Enter your username"
                                    required
                                    disabled={isLoading}
                                />
                            </div>
                        </div>

                        <div className="auth-field">
                            <label>
                                Password
                            </label>
                            <div className="relative">
                                <div className="auth-field-icon">
                                    <Lock className="h-4 w-4" />
                                </div>
                                <input
                                    type="password"
                                    value={password}
                                    onChange={(e) => {
                                        setPassword(e.target.value);
                                        if (error) {
                                            clearError();
                                        }
                                    }}
                                    className="auth-input"
                                    placeholder="Enter your password"
                                    required
                                    disabled={isLoading}
                                />
                            </div>
                        </div>
                    </div>

                    <ErrorAlert message={error} title="Login Failed" onClose={clearError} />

                    <button
                        type="submit"
                        disabled={isLoading}
                        className="auth-submit"
                    >
                        {isLoading ? (
                            <Loader2 className="w-5 h-5 animate-spin" />
                        ) : (
                            <>
                                Sign In
                                <ArrowRight className="ml-2 w-4 h-4" />
                            </>
                        )}
                    </button>
                </form>

                <div className="auth-footnote">
                    <p>
                        First-time login requires a password change before the dashboard becomes available.
                    </p>
                </div>
            </div>
        </motion.div>
    );
};
