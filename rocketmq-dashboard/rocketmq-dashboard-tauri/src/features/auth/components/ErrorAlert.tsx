import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { AlertCircle } from 'lucide-react';

interface ErrorAlertProps {
    message: string;
    title?: string;
    onClose?: () => void;
}

export const ErrorAlert: React.FC<ErrorAlertProps> = ({ message, title = 'Action Failed', onClose }) => {
    return (
        <AnimatePresence mode="wait">
            {message && (
                <motion.div
                    initial={{ opacity: 0, y: -10, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: -10, scale: 0.95 }}
                    transition={{ duration: 0.2 }}
                    className="auth-error"
                    role="alert"
                >
                    <div className="auth-error-icon">
                        <AlertCircle className="h-5 w-5" />
                    </div>
                    <div>
                        <p className="auth-error-title">{title}</p>
                        <p className="auth-error-message">{message}</p>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
};
