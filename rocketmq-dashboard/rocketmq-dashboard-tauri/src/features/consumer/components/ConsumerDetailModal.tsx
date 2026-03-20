import { useEffect, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { AlertCircle, FileBox, FileText, Layers, LoaderCircle, X } from 'lucide-react';
import { ConsumerService } from '../../../services/consumer.service';
import type {
    ConsumerGroupListItem,
    ConsumerTopicDetailView,
} from '../types/consumer.types';

interface ConsumerDetailModalProps {
    isOpen: boolean;
    onClose: () => void;
    consumer: ConsumerGroupListItem | null;
    address?: string;
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

const formatTimestamp = (timestamp: number) => {
    if (!timestamp || timestamp <= 0) {
        return '-';
    }
    return new Date(timestamp).toLocaleString();
};

export const ConsumerDetailModal = ({
    isOpen,
    onClose,
    consumer,
    address,
}: ConsumerDetailModalProps) => {
    const [data, setData] = useState<ConsumerTopicDetailView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !consumer) {
            return;
        }

        let cancelled = false;
        setIsLoading(true);
        setError('');
        setData(null);

        void ConsumerService.queryConsumerTopicDetail({
            consumerGroup: consumer.rawGroupName,
            address,
        })
            .then((response) => {
                if (!cancelled) {
                    setData(response);
                }
            })
            .catch((loadError) => {
                if (!cancelled) {
                    setError(getErrorMessage(loadError, 'Failed to load consumer details.'));
                }
            })
            .finally(() => {
                if (!cancelled) {
                    setIsLoading(false);
                }
            });

        return () => {
            cancelled = true;
        };
    }, [address, consumer, isOpen]);

    if (!isOpen) {
        return null;
    }

    const consumerLabel = getConsumerLabel(consumer);

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
                    className="relative flex max-h-[90vh] w-full max-w-5xl flex-col overflow-hidden rounded-xl border border-gray-100 bg-white shadow-2xl dark:border-gray-800 dark:bg-gray-900"
                >
                    <div className="z-10 flex shrink-0 items-center justify-between border-b border-gray-100 bg-white px-6 py-5 dark:border-gray-800 dark:bg-gray-900">
                        <div>
                            <h3 className="flex items-center text-xl font-bold text-gray-800 dark:text-white">
                                <FileText className="mr-2 h-5 w-5 text-blue-500" />
                                Consumer Details
                            </h3>
                            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                <span className="font-mono font-medium text-gray-700 dark:text-gray-300">
                                    {consumerLabel}
                                </span>
                                <span className="mx-2 text-gray-300 dark:text-gray-600">|</span>
                                <span>{address?.trim() || 'All brokers'}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="rounded-full p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-gray-800 dark:hover:text-gray-300"
                        >
                            <X className="h-5 w-5" />
                        </button>
                    </div>

                    <div className="flex-1 space-y-6 overflow-y-auto bg-gray-50/50 p-6 dark:bg-gray-950/50">
                        {isLoading ? (
                            <div className="flex min-h-[320px] items-center justify-center">
                                <div className="flex items-center gap-3 rounded-full bg-white px-4 py-2 text-sm text-gray-600 shadow-sm dark:bg-gray-900 dark:text-gray-300">
                                    <LoaderCircle className="h-4 w-4 animate-spin" />
                                    Loading consumer details...
                                </div>
                            </div>
                        ) : error ? (
                            <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </div>
                        ) : data && data.topics.length > 0 ? (
                            <>
                                <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Topic Count
                                        </div>
                                        <div className="mt-3 text-2xl font-bold text-gray-900 dark:text-white">
                                            {data.topicCount}
                                        </div>
                                    </div>
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Total Lag
                                        </div>
                                        <div className="mt-3 text-2xl font-bold text-orange-500 dark:text-orange-400">
                                            {data.totalDiff}
                                        </div>
                                    </div>
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Address Scope
                                        </div>
                                        <div className="mt-3 text-sm font-semibold text-gray-900 dark:text-white">
                                            {address?.trim() || 'All brokers'}
                                        </div>
                                    </div>
                                </div>

                                {data.topics.map((topic) => {
                                    const isRetryTopic = topic.topic.startsWith('%RETRY%');
                                    return (
                                        <section
                                            key={topic.topic}
                                            className="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm dark:border-gray-800 dark:bg-gray-900"
                                        >
                                            <div className="flex items-center justify-between border-b border-gray-200 bg-gray-50/50 px-6 py-4 dark:border-gray-800 dark:bg-gray-800/50">
                                                <div className="flex items-center space-x-2">
                                                    <div
                                                        className={`rounded-lg p-1.5 ${
                                                            isRetryTopic
                                                                ? 'bg-blue-100 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400'
                                                                : 'bg-purple-100 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400'
                                                        }`}
                                                    >
                                                        {isRetryTopic ? <Layers className="h-4 w-4" /> : <FileBox className="h-4 w-4" />}
                                                    </div>
                                                    <span className="font-mono text-sm font-semibold text-gray-700 dark:text-gray-300">
                                                        {topic.topic}
                                                    </span>
                                                </div>
                                                <div className="flex items-center space-x-6 text-xs font-mono">
                                                    <div className="flex items-center text-gray-500 dark:text-gray-400">
                                                        <span className="mr-2 uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500">
                                                            Total Lag:
                                                        </span>
                                                        <span className="rounded bg-amber-100 px-2 py-0.5 font-bold text-amber-700 dark:bg-amber-900/30 dark:text-amber-400">
                                                            {topic.diffTotal}
                                                        </span>
                                                    </div>
                                                    <div className="flex items-center text-gray-500 dark:text-gray-400">
                                                        <span className="mr-2 uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500">
                                                            Last Consume:
                                                        </span>
                                                        <span className="text-gray-900 dark:text-white">
                                                            {formatTimestamp(topic.lastTimestamp)}
                                                        </span>
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="overflow-x-auto">
                                                <table className="w-full text-left text-sm">
                                                    <thead>
                                                        <tr className="border-b border-gray-100 bg-gray-50 text-xs font-semibold uppercase tracking-wider text-gray-500 dark:border-gray-800 dark:bg-gray-800/50 dark:text-gray-400">
                                                            <th className="px-6 py-3">Broker</th>
                                                            <th className="px-6 py-3">Queue ID</th>
                                                            <th className="px-6 py-3 text-right">Broker Offset</th>
                                                            <th className="px-6 py-3 text-right">Consumer Offset</th>
                                                            <th className="px-6 py-3 text-right">Lag (Diff)</th>
                                                            <th className="px-6 py-3">Client Info</th>
                                                            <th className="px-6 py-3 text-right">Last Consume Time</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                                                        {topic.queueStatInfoList.map((queue) => (
                                                            <tr
                                                                key={`${topic.topic}-${queue.brokerName}-${queue.queueId}`}
                                                                className="transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/50"
                                                            >
                                                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">
                                                                    {queue.brokerName}
                                                                </td>
                                                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">
                                                                    {queue.queueId}
                                                                </td>
                                                                <td className="px-6 py-3 text-right font-mono text-gray-900 dark:text-gray-300">
                                                                    {queue.brokerOffset}
                                                                </td>
                                                                <td className="px-6 py-3 text-right font-mono text-gray-900 dark:text-gray-300">
                                                                    {queue.consumerOffset}
                                                                </td>
                                                                <td className="px-6 py-3 text-right font-mono font-bold text-amber-600 dark:text-amber-400">
                                                                    {queue.diffTotal}
                                                                </td>
                                                                <td className="px-6 py-3 text-xs text-gray-500 dark:text-gray-500">
                                                                    {queue.clientInfo || '-'}
                                                                </td>
                                                                <td className="px-6 py-3 text-right font-mono text-gray-500 dark:text-gray-400">
                                                                    {formatTimestamp(queue.lastTimestamp)}
                                                                </td>
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </section>
                                    );
                                })}
                            </>
                        ) : (
                            <div className="rounded-2xl border border-dashed border-gray-200 bg-white/70 px-6 py-16 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/70 dark:text-gray-400">
                                No consumer detail data was returned for this group.
                            </div>
                        )}
                    </div>

                    <div className="z-10 flex shrink-0 justify-end border-t border-gray-100 bg-white px-6 py-4 dark:border-gray-800 dark:bg-gray-900">
                        <button
                            onClick={onClose}
                            className="rounded-lg bg-gray-900 px-6 py-2 text-sm font-medium text-white shadow-md transition-all hover:bg-gray-800 hover:shadow-lg dark:border dark:border-gray-700 dark:!bg-gray-900 dark:!text-white dark:hover:!bg-gray-800"
                        >
                            Close
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};
