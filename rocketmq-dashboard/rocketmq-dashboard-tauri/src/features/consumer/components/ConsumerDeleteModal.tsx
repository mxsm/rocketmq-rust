import { useEffect, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { AlertCircle, LoaderCircle, Trash2, X } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { ConsumerService } from '../../../services/consumer.service';
import type { ConsumerGroupListItem, ConsumerMutationResult } from '../types/consumer.types';

interface ConsumerDeleteModalProps {
    isOpen: boolean;
    onClose: () => void;
    consumer: ConsumerGroupListItem | null;
    onDeleted: (result: ConsumerMutationResult) => void;
}

const getConsumerLabel = (consumer: ConsumerGroupListItem | null) =>
    consumer?.displayGroupName ?? consumer?.rawGroupName ?? '-';

const getErrorMessage = (error: unknown, fallback: string) => {
    if (typeof error === 'string' && error.trim().length > 0) {
        return error;
    }
    if (error instanceof Error && error.message.trim().length > 0) {
        return error.message;
    }
    if (error && typeof error === 'object' && 'message' in error) {
        const message = (error as { message?: unknown }).message;
        if (typeof message === 'string' && message.trim().length > 0) {
            return message;
        }
    }
    return fallback;
};

export const ConsumerDeleteModal = ({
    isOpen,
    onClose,
    consumer,
    onDeleted,
}: ConsumerDeleteModalProps) => {
    const [selectedBrokerNames, setSelectedBrokerNames] = useState<string[]>([]);
    const [isDeleting, setIsDeleting] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen) {
            return;
        }
        setError('');
        setSelectedBrokerNames([...(consumer?.brokerNames ?? [])].sort((left, right) => left.localeCompare(right)));
    }, [consumer, isOpen]);

    if (!isOpen) {
        return null;
    }

    const toggleBroker = (brokerName: string) => {
        setSelectedBrokerNames((current) =>
            current.includes(brokerName)
                ? current.filter((item) => item !== brokerName)
                : [...current, brokerName].sort((left, right) => left.localeCompare(right)),
        );
    };

    const handleDelete = async () => {
        if (!consumer) {
            return;
        }
        if (selectedBrokerNames.length === 0) {
            setError('Select at least one broker before deleting this consumer group.');
            return;
        }

        try {
            setIsDeleting(true);
            setError('');
            const result = await ConsumerService.deleteConsumerGroup({
                consumerGroup: consumer.rawGroupName,
                brokerNameList: selectedBrokerNames,
            });
            toast.success('Consumer group deleted from the selected brokers.');
            onDeleted(result);
        } catch (deleteError) {
            setError(getErrorMessage(deleteError, 'Failed to delete the consumer group.'));
        } finally {
            setIsDeleting(false);
        }
    };

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 0.3 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: 10 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: 10 }}
                    className="relative flex w-full max-w-2xl flex-col overflow-hidden rounded-xl border border-gray-100 bg-white shadow-2xl dark:border-gray-800 dark:bg-gray-900"
                >
                    <div className="z-10 flex shrink-0 items-center justify-between border-b border-gray-100 bg-white px-6 py-5 dark:border-gray-800 dark:bg-gray-900">
                        <div>
                            <h3 className="flex items-center text-xl font-bold text-gray-800 dark:text-white">
                                <Trash2 className="mr-2 h-5 w-5 text-red-500" />
                                Delete Consumer Group
                            </h3>
                            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                <span className="font-mono font-medium text-gray-700 dark:text-gray-300">
                                    {getConsumerLabel(consumer)}
                                </span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="rounded-full p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-gray-800 dark:hover:text-gray-300"
                        >
                            <X className="h-5 w-5" />
                        </button>
                    </div>

                    <div className="space-y-5 bg-gray-50/50 px-6 py-6 dark:bg-gray-950/50">
                        <div className="rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                            This action deletes the subscription group on the selected brokers and also removes the
                            associated retry / DLQ topics where the full broker coverage is selected.
                        </div>

                        {error && (
                            <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </div>
                        )}

                        <div className="space-y-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-gray-400 dark:text-gray-500">
                                Broker Scope
                            </div>
                            <div className="flex flex-wrap gap-2">
                                {(consumer?.brokerNames ?? []).map((brokerName) => {
                                    const selected = selectedBrokerNames.includes(brokerName);
                                    return (
                                        <button
                                            key={brokerName}
                                            type="button"
                                            onClick={() => toggleBroker(brokerName)}
                                            className={`rounded-full border px-3 py-1.5 text-sm transition ${
                                                selected
                                                    ? 'border-red-200 bg-red-50 text-red-700 dark:border-red-800 dark:bg-red-900/30 dark:text-red-300'
                                                    : 'border-gray-200 bg-white text-gray-600 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 dark:hover:bg-gray-700'
                                            }`}
                                        >
                                            {brokerName}
                                        </button>
                                    );
                                })}
                            </div>
                        </div>
                    </div>

                    <div className="z-10 flex shrink-0 items-center justify-end gap-3 border-t border-gray-100 bg-white px-6 py-4 dark:border-gray-800 dark:bg-gray-900">
                        <button
                            onClick={onClose}
                            className="rounded-xl border border-gray-200 px-4 py-2 text-sm font-medium text-gray-600 transition hover:bg-gray-50 dark:border-gray-700 dark:text-gray-300 dark:hover:bg-gray-800"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={() => void handleDelete()}
                            disabled={isDeleting}
                            className="flex items-center rounded-xl bg-red-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-red-500 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                            {isDeleting ? (
                                <LoaderCircle className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                                <Trash2 className="mr-2 h-4 w-4" />
                            )}
                            Delete Group
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};
