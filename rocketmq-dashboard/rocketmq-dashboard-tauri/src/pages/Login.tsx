import React, {useState} from 'react';
import {motion} from 'motion/react';
import {User, Lock, ArrowRight, Loader2} from 'lucide-react';
import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';
import {AuthLayout} from '../app/layout/AuthLayout';
import {useAppStore} from '../stores/app.store';

export const Login = () => {
    const [isLoading, setIsLoading] = useState(false);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const {setIsLoggedIn} = useAppStore();

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);
        // Mock login delay
        setTimeout(() => {
            setIsLoading(false);
            setIsLoggedIn(true);
        }, 1500);
    };

    return (
        <AuthLayout>
            {/* Left Side: Illustration */}
            <motion.div
                initial={{opacity: 0, x: -50}}
                animate={{opacity: 1, x: 0}}
                transition={{duration: 0.8, ease: "easeOut"}}
                className="flex-1 flex flex-col items-center md:items-start text-center md:text-left"
            >
                <div className="relative w-full max-w-[500px] aspect-square flex items-center justify-center">
                    {/* Glowing effect behind the rocket */}
                    <div className="absolute inset-0 bg-blue-500/10 blur-[120px] rounded-full"/>
                    <img
                        src={rocketImage}
                        alt="RocketMQ Rust"
                        className="relative z-10 w-full h-full object-contain drop-shadow-2xl"
                    />
                </div>
            </motion.div>

            {/* Right Side: Login Form */}
            <motion.div
                initial={{opacity: 0, y: 20}}
                animate={{opacity: 1, y: 0}}
                transition={{duration: 0.8, delay: 0.2, ease: "easeOut"}}
                className="w-full max-w-md"
            >
                <div className="bg-gray-900/80 backdrop-blur-2xl border border-gray-800 p-8 md:p-10 rounded-3xl shadow-2xl">
                    <div className="mb-10">
                        <h1 className="text-2xl font-bold text-white mb-2 tracking-tight">Sign in to your account</h1>
                        <p className="text-gray-400 text-sm">Welcome back! Please enter your details.</p>
                    </div>

                    <form onSubmit={handleSubmit} className="space-y-6">
                        <div className="space-y-4">
                            <div className="space-y-2">
                                <label className="text-xs font-semibold text-gray-300 uppercase tracking-wider ml-1">Username</label>
                                <div className="relative">
                                    <div className="absolute inset-y-0 left-0 pl-3.5 flex items-center pointer-events-none">
                                        <User className="h-4 w-4 text-gray-500"/>
                                    </div>
                                    <input
                                        type="text"
                                        value={username}
                                        onChange={(e) => setUsername(e.target.value)}
                                        className="block w-full pl-10 pr-4 py-3 bg-gray-950/50 border border-gray-700/50 rounded-xl text-white placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all text-sm font-medium hover:border-gray-600"
                                        placeholder="Enter your username"
                                        required
                                    />
                                </div>
                            </div>

                            <div className="space-y-2">
                                <label className="text-xs font-semibold text-gray-300 uppercase tracking-wider ml-1">Password</label>
                                <div className="relative">
                                    <div className="absolute inset-y-0 left-0 pl-3.5 flex items-center pointer-events-none">
                                        <Lock className="h-4 w-4 text-gray-500"/>
                                    </div>
                                    <input
                                        type="password"
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        className="block w-full pl-10 pr-4 py-3 bg-gray-950/50 border border-gray-700/50 rounded-xl text-white placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all text-sm font-medium hover:border-gray-600"
                                        placeholder="••••••••"
                                        required
                                    />
                                </div>
                            </div>
                        </div>

                        <div className="flex items-center justify-between text-sm">
                            <label className="flex items-center text-gray-400 hover:text-gray-300 cursor-pointer select-none group">
                                <input type="checkbox"
                                       className="w-4 h-4 rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500/40 transition-colors cursor-pointer"/>
                                <span className="ml-2 group-hover:text-white transition-colors">Remember me</span>
                            </label>
                            <a href="#" className="text-blue-400 hover:text-blue-300 font-medium transition-colors hover:underline">Forgot password?</a>
                        </div>

                        <button
                            type="submit"
                            disabled={isLoading}
                            className="w-full flex items-center justify-center py-3.5 px-4 bg-white text-black hover:bg-gray-100 rounded-xl font-bold shadow-lg transform hover:-translate-y-0.5 active:translate-y-0 transition-all disabled:opacity-70 disabled:cursor-not-allowed disabled:transform-none"
                        >
                            {isLoading ? (
                                <Loader2 className="w-5 h-5 animate-spin"/>
                            ) : (
                                <>
                                    Sign In
                                    <ArrowRight className="ml-2 w-4 h-4"/>
                                </>
                            )}
                        </button>
                    </form>

                    <div className="mt-8 pt-6 border-t border-gray-800 text-center">
                        <p className="text-sm text-gray-500">
                            Don't have an account? <a href="#" className="text-white hover:text-blue-400 font-medium transition-colors">Contact
                            Administrator</a>
                        </p>
                    </div>
                </div>
            </motion.div>
        </AuthLayout>
    );
};
