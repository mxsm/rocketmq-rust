import React from 'react';
import { motion, useReducedMotion } from 'motion/react';
import { Activity, Database, Radar, ShieldCheck, Waypoints } from 'lucide-react';
import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';
import { AuthLayout } from '../app/layout/AuthLayout';
import { ChangePasswordDialog, LoginForm } from '../features/auth';

export const Login = () => {
    const reduceMotion = useReducedMotion();

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
                        <span className="auth-kicker">
                            <Radar className="auth-kicker-icon" />
                            RocketMQ Rust Operations
                        </span>
                        <h1>RocketMQ-Rust Control Plane</h1>
                        <p>
                            A dimensional desktop console for brokers, topics, consumers, producers, traces, and access control.
                        </p>
                    </div>

                    <div className="auth-signal-grid">
                        <div>
                            <Activity className="auth-signal-icon" />
                            <span>Broker health</span>
                            <strong>Live</strong>
                        </div>
                        <div>
                            <Waypoints className="auth-signal-icon" />
                            <span>Trace query</span>
                            <strong>Ready</strong>
                        </div>
                        <div>
                            <ShieldCheck className="auth-signal-icon" />
                            <span>ACL posture</span>
                            <strong>Guarded</strong>
                        </div>
                    </div>

                    <div className="auth-3d-scene" aria-label="RocketMQ Rust 3D control module">
                        <div className="auth-stage-glow" />
                        <motion.div
                            className="auth-console-rig"
                            animate={
                                reduceMotion
                                    ? undefined
                                    : { rotateX: [58, 55, 58], rotateZ: [-9, -7, -9], y: [0, -7, 0] }
                            }
                            transition={
                                reduceMotion
                                    ? undefined
                                    : { duration: 7.5, repeat: Infinity, ease: 'easeInOut' }
                            }
                        >
                            <div className="auth-device-top">
                                <div className="auth-device-screen">
                                    <div className="auth-screen-header">
                                        <span />
                                        <span />
                                        <span />
                                    </div>
                                    <div className="auth-screen-bars">
                                        <i />
                                        <i />
                                        <i />
                                        <i />
                                    </div>
                                    <div className="auth-screen-chart">
                                        <span />
                                        <span />
                                        <span />
                                        <span />
                                    </div>
                                </div>
                                <div className="auth-device-node auth-device-node-a">
                                    <Database className="auth-device-icon" />
                                </div>
                                <div className="auth-device-node auth-device-node-b">
                                    <Activity className="auth-device-icon" />
                                </div>
                                <img src={rocketImage} alt="" className="auth-device-emblem" aria-hidden="true" />
                            </div>
                            <div className="auth-device-front" />
                            <div className="auth-device-side" />
                        </motion.div>

                        <motion.div
                            className="auth-floating-module auth-floating-module-a"
                            animate={
                                reduceMotion
                                    ? undefined
                                    : { y: [0, -12, 0], rotateY: [-14, -8, -14] }
                            }
                            transition={
                                reduceMotion
                                    ? undefined
                                    : { duration: 5.8, repeat: Infinity, ease: 'easeInOut' }
                            }
                        >
                            <span>TPS</span>
                            <strong>48.2k</strong>
                        </motion.div>
                        <motion.div
                            className="auth-floating-module auth-floating-module-b"
                            animate={
                                reduceMotion
                                    ? undefined
                                    : { y: [0, 10, 0], rotateY: [14, 8, 14] }
                            }
                            transition={
                                reduceMotion
                                    ? undefined
                                    : { duration: 6.4, repeat: Infinity, ease: 'easeInOut' }
                            }
                        >
                            <span>Topics</span>
                            <strong>2.1k</strong>
                        </motion.div>
                        <div className="auth-stage-floor" />
                    </div>
                </motion.div>

                <LoginForm />
            </AuthLayout>
            <ChangePasswordDialog />
        </>
    );
};
