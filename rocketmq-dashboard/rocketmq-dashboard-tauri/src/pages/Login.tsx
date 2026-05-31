import React from 'react';
import { motion } from 'motion/react';
import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';
import { AuthLayout } from '../app/layout/AuthLayout';
import { ChangePasswordDialog, LoginForm } from '../features/auth';

export const Login = () => {
    return (
        <>
            <AuthLayout>
                <motion.div
                    initial={{ opacity: 0, x: -50 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ duration: 0.8, ease: 'easeOut' }}
                    className="auth-hero"
                >
                    <div className="auth-hero-copy">
                        <span className="auth-kicker">RocketMQ Rust Operations</span>
                        <h1>Message infrastructure control without the clutter.</h1>
                        <p>
                            Monitor brokers, inspect topics and groups, trace messages, and manage access from one dense desktop console.
                        </p>
                    </div>

                    <div className="auth-signal-grid">
                        <div>
                            <span>Broker health</span>
                            <strong>Live</strong>
                        </div>
                        <div>
                            <span>Trace query</span>
                            <strong>Ready</strong>
                        </div>
                        <div>
                            <span>ACL posture</span>
                            <strong>Guarded</strong>
                        </div>
                    </div>

                    <div className="auth-product-frame">
                        <img
                            src={rocketImage}
                            alt="RocketMQ Rust"
                            className="auth-product-image"
                        />
                    </div>
                </motion.div>

                <LoginForm />
            </AuthLayout>
            <ChangePasswordDialog />
        </>
    );
};
