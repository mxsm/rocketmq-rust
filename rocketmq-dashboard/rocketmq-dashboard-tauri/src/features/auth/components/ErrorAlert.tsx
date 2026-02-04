import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { AlertCircle } from 'lucide-react';

interface ErrorAlertProps {
    message: string;
    onClose?: () => void;
}

export const ErrorAlert: React.FC<ErrorAlertProps> = ({ message, onClose }) => {
    return (
        <AnimatePresence mode="wait">
            {message && (
                <motion.div
                    initial={{ opacity: 0, y: -10, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: -10, scale: 0.95 }}
                    transition={{ duration: 0.2 }}
                    className="flex items-start gap-3 p-4 bg-red-950/80 border border-red-500/50 rounded-xl shadow-[0_0_15px_rgba(239,68,68,0.2)] backdrop-blur-md"
                >
                    <div className="p-2 bg-red-500/10 rounded-full shrink-0">
                        <AlertCircle className="h-5 w-5 text-red-500" />
                    </div>
                    <div className="flex-1 pt-0.5">
                        <p className="font-bold text-white mb-1">Login Failed</p>
                        <p className="text-sm text-white font-medium">{message}</p>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
};
