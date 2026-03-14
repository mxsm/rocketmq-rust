import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { KeyRound, Loader2, ShieldAlert } from 'lucide-react';
import { useAuth } from '../hooks/useAuth';
import { ErrorAlert } from './ErrorAlert';
import { useAppStore } from '../../../stores/app.store';

export const ChangePasswordDialog: React.FC = () => {
    const [oldPassword, setOldPassword] = useState('');
    const [newPassword, setNewPassword] = useState('');
    const [confirmPassword, setConfirmPassword] = useState('');
    const { currentUser, isLoggedIn, mustChangePassword } = useAppStore();
    const { changePassword, clearError, error, isLoading, logout } = useAuth();

    const isOpen = isLoggedIn && mustChangePassword;

    const handleSubmit = async (event: React.FormEvent) => {
        event.preventDefault();
        clearError();

        if (newPassword !== confirmPassword) {
            return;
        }

        const result = await changePassword({
            oldPassword,
            newPassword,
        });

        if (result.success) {
            setOldPassword('');
            setNewPassword('');
            setConfirmPassword('');
        }
    };

    const confirmPasswordError =
        confirmPassword.length > 0 && newPassword !== confirmPassword
            ? 'New password confirmation does not match'
            : '';

    return (
        <AnimatePresence>
            {isOpen && (
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    className="fixed inset-0 z-50 bg-[radial-gradient(circle_at_top,_rgba(96,165,250,0.18),_transparent_34%),linear-gradient(180deg,rgba(3,7,18,0.58),rgba(2,6,23,0.86))] backdrop-blur-xl flex items-center justify-center p-4 overflow-hidden"
                >
                    <div className="absolute -top-24 left-1/2 h-72 w-72 -translate-x-1/2 rounded-full bg-cyan-300/12 blur-3xl" />
                    <div className="absolute bottom-[-6rem] right-[-3rem] h-64 w-64 rounded-full bg-blue-500/10 blur-3xl" />
                    <motion.div
                        initial={{ opacity: 0, y: 20, scale: 0.96 }}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: 20, scale: 0.96 }}
                        transition={{ duration: 0.2, ease: 'easeOut' }}
                        className="relative w-full max-w-lg overflow-hidden rounded-[28px] border border-white/18 bg-white/10 backdrop-blur-3xl text-white shadow-[0_30px_120px_rgba(15,23,42,0.5)]"
                    >
                        <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(255,255,255,0.2),rgba(255,255,255,0.06)_28%,rgba(15,23,42,0.16)_100%)]" />
                        <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-white/70 to-transparent" />
                        <div className="absolute inset-x-0 top-0 h-32 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.18),transparent_72%)]" />

                        <div className="relative p-8 border-b border-white/12 bg-black/10">
                            <div className="flex items-start gap-4">
                                <div className="w-12 h-12 rounded-2xl border border-white/15 bg-white/10 text-amber-200 flex items-center justify-center shadow-[inset_0_1px_0_rgba(255,255,255,0.18)]">
                                    <ShieldAlert className="w-6 h-6" />
                                </div>
                                <div className="space-y-2">
                                    <h2 className="text-2xl font-semibold">Change initial password</h2>
                                    <p className="text-sm text-white/75">
                                        {currentUser?.username ?? 'Admin'} must update the bootstrap password before entering the dashboard.
                                    </p>
                                </div>
                            </div>
                        </div>

                        <form onSubmit={handleSubmit} className="relative p-8 space-y-5">
                            <div className="space-y-2">
                                <label className="text-xs font-semibold tracking-[0.18em] uppercase text-white/60">
                                    Current password
                                </label>
                                <div className="relative">
                                    <KeyRound className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/35" />
                                    <input
                                        type="password"
                                        value={oldPassword}
                                        onChange={(event) => setOldPassword(event.target.value)}
                                        className="w-full rounded-2xl border border-white/12 bg-white/8 pl-10 pr-4 py-3 text-sm text-white placeholder:text-white/35 outline-none backdrop-blur-xl shadow-[inset_0_1px_0_rgba(255,255,255,0.12)] focus:border-cyan-300/55 focus:bg-white/12"
                                        placeholder="Enter the current password"
                                        required
                                        disabled={isLoading}
                                    />
                                </div>
                            </div>

                            <div className="space-y-2">
                                <label className="text-xs font-semibold tracking-[0.18em] uppercase text-white/60">
                                    New password
                                </label>
                                <div className="relative">
                                    <KeyRound className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/35" />
                                    <input
                                        type="password"
                                        value={newPassword}
                                        onChange={(event) => setNewPassword(event.target.value)}
                                        className="w-full rounded-2xl border border-white/12 bg-white/8 pl-10 pr-4 py-3 text-sm text-white placeholder:text-white/35 outline-none backdrop-blur-xl shadow-[inset_0_1px_0_rgba(255,255,255,0.12)] focus:border-cyan-300/55 focus:bg-white/12"
                                        placeholder="At least 8 characters"
                                        required
                                        minLength={8}
                                        disabled={isLoading}
                                    />
                                </div>
                            </div>

                            <div className="space-y-2">
                                <label className="text-xs font-semibold tracking-[0.18em] uppercase text-white/60">
                                    Confirm new password
                                </label>
                                <div className="relative">
                                    <KeyRound className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/35" />
                                    <input
                                        type="password"
                                        value={confirmPassword}
                                        onChange={(event) => setConfirmPassword(event.target.value)}
                                        className="w-full rounded-2xl border border-white/12 bg-white/8 pl-10 pr-4 py-3 text-sm text-white placeholder:text-white/35 outline-none backdrop-blur-xl shadow-[inset_0_1px_0_rgba(255,255,255,0.12)] focus:border-cyan-300/55 focus:bg-white/12"
                                        placeholder="Repeat the new password"
                                        required
                                        minLength={8}
                                        disabled={isLoading}
                                    />
                                </div>
                            </div>

                            <ErrorAlert
                                message={confirmPasswordError || error}
                                title="Password Change Failed"
                                onClose={clearError}
                            />

                            <div className="flex flex-col-reverse sm:flex-row sm:justify-between gap-3 pt-2">
                                <button
                                    type="button"
                                    onClick={() => void logout()}
                                    className="px-4 py-3 rounded-2xl border border-white/12 bg-black/10 text-white/75 hover:text-white hover:bg-white/10 backdrop-blur-xl transition-colors"
                                    disabled={isLoading}
                                >
                                    Sign out
                                </button>
                                <button
                                    type="submit"
                                    disabled={isLoading || Boolean(confirmPasswordError)}
                                    className="inline-flex items-center justify-center gap-2 px-5 py-3 rounded-2xl border border-white/30 bg-white/80 text-black font-semibold backdrop-blur-xl shadow-[0_12px_40px_rgba(255,255,255,0.14)] hover:bg-white disabled:opacity-60 disabled:cursor-not-allowed"
                                >
                                    {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
                                    Update password
                                </button>
                            </div>
                        </form>
                    </motion.div>
                </motion.div>
            )}
        </AnimatePresence>
    );
};
