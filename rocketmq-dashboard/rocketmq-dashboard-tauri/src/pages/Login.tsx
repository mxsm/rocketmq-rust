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
                    className="flex-1 flex flex-col items-center md:items-start text-center md:text-left"
                >
                    <div className="relative w-full max-w-[500px] aspect-square flex items-center justify-center">
                        <div className="absolute inset-0 rounded-full bg-[radial-gradient(circle_at_center,_rgba(56,189,248,0.16),_transparent_55%)] blur-[90px]" />
                        <div className="absolute inset-[12%] rounded-full border border-sky-300/10 bg-[radial-gradient(circle_at_top,_rgba(125,211,252,0.14),_transparent_45%)]" />
                        <img
                            src={rocketImage}
                            alt="RocketMQ Rust"
                            className="relative z-10 w-full h-full object-contain drop-shadow-2xl"
                        />
                    </div>
                </motion.div>

                <LoginForm />
            </AuthLayout>
            <ChangePasswordDialog />
        </>
    );
};
