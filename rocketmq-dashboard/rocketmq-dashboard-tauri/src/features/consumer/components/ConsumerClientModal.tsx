import { useEffect, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { AlertCircle, ListChecks, LoaderCircle, Network, Users, X } from 'lucide-react';
import { ConsumerService } from '../../../services/consumer.service';
import type {
    ConsumerConnectionView,
    ConsumerGroupListItem,
} from '../types/consumer.types';

interface ConsumerClientModalProps {
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

export const ConsumerClientModal = ({
    isOpen,
    onClose,
    consumer,
    address,
}: ConsumerClientModalProps) => {
    const [data, setData] = useState<ConsumerConnectionView | null>(null);
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

        void ConsumerService.queryConsumerConnection({
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
                    setError(getErrorMessage(loadError, 'Failed to load consumer connections.'));
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
                    className="relative flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-xl border border-gray-100 bg-white shadow-2xl dark:border-gray-800 dark:bg-gray-900"
                >
                    <div className="z-10 flex shrink-0 items-center justify-between border-b border-gray-100 bg-white px-6 py-5 dark:border-gray-800 dark:bg-gray-900">
                        <div>
                            <h3 className="flex items-center text-xl font-bold text-gray-800 dark:text-white">
                                <Users className="mr-2 h-5 w-5 text-blue-500" />
                                Client Information
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
                                    Loading client information...
                                </div>
                            </div>
                        ) : error ? (
                            <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </div>
                        ) : data ? (
                            <>
                                <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Connections
                                        </div>
                                        <div className="mt-3 text-2xl font-bold text-gray-900 dark:text-white">
                                            {data.connectionCount}
                                        </div>
                                    </div>
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Consume Type
                                        </div>
                                        <div className="mt-3 text-sm font-semibold text-gray-900 dark:text-white">
                                            {data.consumeType}
                                        </div>
                                    </div>
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Message Model
                                        </div>
                                        <div className="mt-3 text-sm font-semibold text-gray-900 dark:text-white">
                                            {data.messageModel}
                                        </div>
                                    </div>
                                    <div className="rounded-2xl border border-gray-100 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                            Consume From
                                        </div>
                                        <div className="mt-3 text-sm font-semibold text-gray-900 dark:text-white">
                                            {data.consumeFromWhere}
                                        </div>
                                    </div>
                                </div>

                                <section className="overflow-hidden rounded-2xl border border-gray-200 bg-white shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2 border-b border-gray-100 px-5 py-4 dark:border-gray-800">
                                        <div className="rounded-lg bg-blue-50 p-1.5 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400">
                                            <Network className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">Client Connections</h4>
                                    </div>
                                    {data.connections.length === 0 ? (
                                        <div className="px-5 py-10 text-center text-sm text-gray-500 dark:text-gray-400">
                                            No active client connections were returned for this consumer group.
                                        </div>
                                    ) : (
                                        <div className="overflow-x-auto">
                                            <table className="w-full text-left text-sm">
                                                <thead>
                                                    <tr className="border-b border-gray-100 bg-gray-50 text-xs font-semibold uppercase tracking-wider text-gray-500 dark:border-gray-800 dark:bg-gray-800/50 dark:text-gray-400">
                                                        <th className="px-5 py-3">Client ID</th>
                                                        <th className="px-5 py-3">Client Addr</th>
                                                        <th className="px-5 py-3">Language</th>
                                                        <th className="px-5 py-3">Version</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                                                    {data.connections.map((item) => (
                                                        <tr
                                                            key={item.clientId}
                                                            className="transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/50"
                                                        >
                                                            <td className="px-5 py-3 font-mono text-xs text-gray-700 dark:text-gray-300">
                                                                {item.clientId}
                                                            </td>
                                                            <td className="px-5 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">
                                                                {item.clientAddr}
                                                            </td>
                                                            <td className="px-5 py-3 text-gray-700 dark:text-gray-300">
                                                                {item.language}
                                                            </td>
                                                            <td className="px-5 py-3 text-gray-700 dark:text-gray-300">
                                                                {item.versionDesc}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </section>

                                <section className="overflow-hidden rounded-2xl border border-gray-200 bg-white shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2 border-b border-gray-100 px-5 py-4 dark:border-gray-800">
                                        <div className="rounded-lg bg-purple-50 p-1.5 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400">
                                            <ListChecks className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">Subscriptions</h4>
                                    </div>
                                    {data.subscriptions.length === 0 ? (
                                        <div className="px-5 py-10 text-center text-sm text-gray-500 dark:text-gray-400">
                                            No subscriptions were returned for this consumer group.
                                        </div>
                                    ) : (
                                        <div className="overflow-x-auto">
                                            <table className="w-full text-left text-sm">
                                                <thead>
                                                    <tr className="border-b border-gray-100 bg-gray-50 text-xs font-semibold uppercase tracking-wider text-gray-500 dark:border-gray-800 dark:bg-gray-800/50 dark:text-gray-400">
                                                        <th className="px-5 py-3">Topic</th>
                                                        <th className="px-5 py-3">Sub String</th>
                                                        <th className="px-5 py-3">Expr Type</th>
                                                        <th className="px-5 py-3">Tags</th>
                                                        <th className="px-5 py-3">Codes</th>
                                                        <th className="px-5 py-3">Sub Version</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                                                    {data.subscriptions.map((item) => (
                                                        <tr
                                                            key={`${item.topic}-${item.subVersion}`}
                                                            className="transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/50"
                                                        >
                                                            <td className="px-5 py-3 font-mono text-xs text-gray-700 dark:text-gray-300">
                                                                {item.topic}
                                                            </td>
                                                            <td className="px-5 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">
                                                                {item.subString}
                                                            </td>
                                                            <td className="px-5 py-3 text-gray-700 dark:text-gray-300">
                                                                {item.expressionType}
                                                            </td>
                                                            <td className="px-5 py-3 text-gray-600 dark:text-gray-400">
                                                                {item.tagsSet.length > 0 ? item.tagsSet.join(', ') : '-'}
                                                            </td>
                                                            <td className="px-5 py-3 text-gray-600 dark:text-gray-400">
                                                                {item.codeSet.length > 0 ? item.codeSet.join(', ') : '-'}
                                                            </td>
                                                            <td className="px-5 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">
                                                                {item.subVersion}
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </section>
                            </>
                        ) : (
                            <div className="rounded-2xl border border-dashed border-gray-200 bg-white/70 px-6 py-16 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/70 dark:text-gray-400">
                                No consumer connection data was returned.
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
