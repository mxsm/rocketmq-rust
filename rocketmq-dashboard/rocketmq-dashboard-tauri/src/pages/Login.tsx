import React from 'react';
import { motion } from 'motion/react';
import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';
import { AuthLayout } from '../app/layout/AuthLayout';
import { LoginForm } from '../features/auth';

export const Login = () => {
    return (
        <AuthLayout>
            {/* Left Side: Illustration */}
            <motion.div
                initial={{ opacity: 0, x: -50 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.8, ease: "easeOut" }}
                className="flex-1 flex flex-col items-center md:items-start text-center md:text-left"
            >
                <div className="relative w-full max-w-[500px] aspect-square flex items-center justify-center">
                    {/* Glowing effect behind the rocket */}
                    <div className="absolute inset-0 bg-blue-500/10 blur-[120px] rounded-full" />
                    <img
                        src={rocketImage}
                        alt="RocketMQ Rust"
                        className="relative z-10 w-full h-full object-contain drop-shadow-2xl"
                    />
                </div>
            </motion.div>

            {/* Right Side: Login Form */}
            <LoginForm />
        </AuthLayout>
    );
};
