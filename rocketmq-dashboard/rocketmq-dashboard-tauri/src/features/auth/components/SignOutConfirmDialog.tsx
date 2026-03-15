import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { AlertTriangle, Loader2, LogOut } from 'lucide-react';
import { Button } from '../../../components/ui/button';

interface SignOutConfirmDialogProps {
    open: boolean;
    title: string;
    description: string;
    isSubmitting?: boolean;
    onConfirm: () => void | Promise<void>;
    onCancel: () => void;
}

export const SignOutConfirmDialog: React.FC<SignOutConfirmDialogProps> = ({
    open,
    title,
    description,
    isSubmitting = false,
    onConfirm,
    onCancel,
}) => {
    return (
        <AnimatePresence>
            {open ? (
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={isSubmitting ? undefined : onCancel}
                    className="absolute inset-0 flex items-center justify-center bg-[radial-gradient(circle_at_top,rgba(56,189,248,0.08),transparent_30%),linear-gradient(180deg,rgba(2,6,23,0.32),rgba(2,6,23,0.72))] p-4 backdrop-blur-md"
                    style={{ zIndex: 99999 }}
                >
                    <div className="pointer-events-none absolute inset-0 bg-black/10" />
                    <div className="pointer-events-none absolute left-1/2 top-0 h-72 w-72 -translate-x-1/2 rounded-full bg-cyan-300/12 blur-3xl" />
                    <div className="pointer-events-none absolute bottom-[-5rem] right-[-4rem] h-64 w-64 rounded-full bg-blue-500/10 blur-3xl" />
                    <motion.div
                        initial={{ opacity: 0, y: 20, scale: 0.96 }}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: 20, scale: 0.96 }}
                        transition={{ duration: 0.18, ease: 'easeOut' }}
                        onClick={(event) => event.stopPropagation()}
                        className="relative w-full max-w-[500px] overflow-hidden rounded-[28px] border border-white/16 bg-slate-900/85 text-white shadow-[0_30px_90px_rgba(15,23,42,0.44)] backdrop-blur-2xl"
                    >
                        <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(255,255,255,0.2),rgba(255,255,255,0.06)_32%,rgba(15,23,42,0.18)_100%)]" />
                        <div className="absolute inset-x-6 top-0 h-px bg-gradient-to-r from-transparent via-white/70 to-transparent" />
                        <div className="absolute inset-x-0 top-0 h-28 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.18),transparent_72%)]" />

                        <div className="relative border-b border-white/10 bg-black/10 px-6 py-5">
                            <div className="flex items-start gap-4">
                                <div className="flex h-11 w-11 items-center justify-center rounded-2xl border border-rose-400/25 bg-rose-500/12 text-rose-200 shadow-[inset_0_1px_0_rgba(255,255,255,0.14)]">
                                    <AlertTriangle className="h-5 w-5" />
                                </div>
                                <div className="space-y-2">
                                    <h2 className="text-xl font-semibold sm:whitespace-nowrap">{title}</h2>
                                    <p className="text-sm leading-6 text-slate-200/85">{description}</p>
                                </div>
                            </div>
                        </div>

                        <div className="relative flex flex-col-reverse gap-3 px-6 py-5 sm:flex-row sm:justify-end">
                            <Button
                                type="button"
                                variant="outline"
                                disabled={isSubmitting}
                                onClick={onCancel}
                                className="rounded-2xl border-white/12 bg-white/8 text-white hover:bg-white/12 hover:text-white dark:border-white/12 dark:bg-white/8 dark:text-white dark:hover:bg-white/12"
                            >
                                Cancel
                            </Button>
                            <Button
                                type="button"
                                disabled={isSubmitting}
                                onClick={() => void onConfirm()}
                                className="rounded-2xl border border-rose-300/15 bg-rose-500/92 text-white shadow-[0_16px_36px_rgba(244,63,94,0.24)] hover:bg-rose-400"
                            >
                                {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <LogOut className="h-4 w-4" />}
                                {isSubmitting ? 'Signing Out...' : 'Sign Out'}
                            </Button>
                        </div>
                    </motion.div>
                </motion.div>
            ) : null}
        </AnimatePresence>
    );
};
