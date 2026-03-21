import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    AlertCircle,
    Cpu,
    Hash,
    LoaderCircle,
    Network,
    RotateCcw,
    Settings,
    X,
} from 'lucide-react';
import { ConsumerService } from '../../../services/consumer.service';
import type {
    ConsumerConfigView,
    ConsumerGroupListItem,
} from '../types/consumer.types';

interface ConsumerConfigModalProps {
    isOpen: boolean;
    onClose: () => void;
    consumer: ConsumerGroupListItem | null;
    onEdit?: (consumer: ConsumerGroupListItem, preferredBrokerAddress?: string) => void;
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

export const ConsumerConfigModal = ({
    isOpen,
    onClose,
    consumer,
    onEdit,
}: ConsumerConfigModalProps) => {
    const brokerAddresses = useMemo(() => consumer?.brokerAddresses ?? [], [consumer]);
    const [selectedBrokerAddress, setSelectedBrokerAddress] = useState('');
    const [data, setData] = useState<ConsumerConfigView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !consumer) {
            return;
        }

        setSelectedBrokerAddress((current) => {
            if (current && brokerAddresses.includes(current)) {
                return current;
            }
            return brokerAddresses[0] ?? '';
        });
    }, [brokerAddresses, consumer, isOpen]);

    useEffect(() => {
        if (!isOpen || !consumer || !selectedBrokerAddress) {
            return;
        }

        let cancelled = false;
        setIsLoading(true);
        setError('');
        setData(null);

        void ConsumerService.queryConsumerConfig({
            consumerGroup: consumer.rawGroupName,
            address: selectedBrokerAddress,
        })
            .then((response) => {
                if (!cancelled) {
                    setData(response);
                }
            })
            .catch((loadError) => {
                if (!cancelled) {
                    setError(getErrorMessage(loadError, 'Failed to load consumer configuration.'));
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
    }, [consumer, isOpen, selectedBrokerAddress]);

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
                                <Settings className="mr-2 h-5 w-5 text-blue-500" />
                                Configuration
                            </h3>
                            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                <span className="font-mono font-medium text-gray-700 dark:text-gray-300">
                                    {consumerLabel}
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

                    <div className="flex-1 overflow-y-auto bg-gray-50/50 p-6 dark:bg-gray-950/50">
                        <div className="mb-6 flex flex-col gap-3 rounded-2xl border border-gray-100 bg-white/80 p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 md:flex-row md:items-center md:justify-between">
                            <div>
                                <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-400 dark:text-gray-500">
                                    Broker Address
                                </div>
                                <div className="mt-2 font-mono text-sm text-gray-700 dark:text-gray-300">
                                    {selectedBrokerAddress || '-'}
                                </div>
                            </div>
                            <label className="flex items-center gap-3 text-sm text-gray-600 dark:text-gray-300">
                                <Network className="h-4 w-4 text-blue-500" />
                                <select
                                    value={selectedBrokerAddress}
                                    onChange={(event) => setSelectedBrokerAddress(event.target.value)}
                                    className="rounded-xl border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                                >
                                    {brokerAddresses.map((address) => (
                                        <option key={address} value={address}>
                                            {address}
                                        </option>
                                    ))}
                                </select>
                            </label>
                        </div>

                        {isLoading ? (
                            <div className="flex min-h-[320px] items-center justify-center">
                                <div className="flex items-center gap-3 rounded-full bg-white px-4 py-2 text-sm text-gray-600 shadow-sm dark:bg-gray-900 dark:text-gray-300">
                                    <LoaderCircle className="h-4 w-4 animate-spin" />
                                    Loading consumer configuration...
                                </div>
                            </div>
                        ) : error ? (
                            <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                                <span>{error}</span>
                            </div>
                        ) : data ? (
                            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
                                <section className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2">
                                        <div className="rounded-lg bg-blue-50 p-1.5 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400">
                                            <Hash className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">Basic Information</h4>
                                    </div>
                                    <ConfigField label="Group Name" value={data.consumerGroup} mono />
                                    <ConfigField label="Broker Name" value={data.brokerName || '-'} mono />
                                    <ConfigField label="Broker Address" value={data.brokerAddress} mono />
                                    <ConfigField label="Broker ID" value={String(data.brokerId)} mono />
                                    <ConfigField label="System Flag" value={String(data.groupSysFlag)} mono />
                                </section>

                                <section className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2">
                                        <div className="rounded-lg bg-green-50 p-1.5 text-green-600 dark:bg-green-900/30 dark:text-green-400">
                                            <Activity className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">Control Settings</h4>
                                    </div>
                                    <ConfigToggleField label="Consume Enable" enabled={data.consumeEnable} />
                                    <ConfigField
                                        label="Consume From Min"
                                        value={data.consumeFromMinEnable ? 'Enabled' : 'Disabled'}
                                    />
                                    <ConfigToggleField
                                        label="Broadcast Consumption"
                                        enabled={data.consumeBroadcastEnable}
                                    />
                                    <ConfigToggleField
                                        label="Orderly Consumption"
                                        enabled={data.consumeMessageOrderly}
                                    />
                                    <ConfigField
                                        label="Notify Client Change"
                                        value={data.notifyConsumerIdsChangedEnable ? 'Enabled' : 'Disabled'}
                                    />
                                </section>

                                <section className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2">
                                        <div className="rounded-lg bg-orange-50 p-1.5 text-orange-600 dark:bg-orange-900/30 dark:text-orange-400">
                                            <RotateCcw className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">Retry & Throttling</h4>
                                    </div>
                                    <ConfigField label="Retry Queues" value={String(data.retryQueueNums)} mono />
                                    <ConfigField label="Max Retries" value={String(data.retryMaxTimes)} mono />
                                    <ConfigField
                                        label="Slow Consume Broker"
                                        value={String(data.whichBrokerWhenConsumeSlowly)}
                                        mono
                                    />
                                    <ConfigField
                                        label="Consume Timeout"
                                        value={`${data.consumeTimeoutMinute} minutes`}
                                        mono
                                    />
                                    <ConfigField
                                        label="Subscription Topics"
                                        value={String(data.subscriptionTopicCount)}
                                        mono
                                    />
                                </section>

                                <section className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                    <div className="flex items-center gap-2">
                                        <div className="rounded-lg bg-purple-50 p-1.5 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400">
                                            <Cpu className="h-4 w-4" />
                                        </div>
                                        <h4 className="font-semibold text-gray-900 dark:text-white">System Config</h4>
                                    </div>
                                    <div className="space-y-1">
                                        <label className="block text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">
                                            Retry Policy (JSON)
                                        </label>
                                        <div className="overflow-hidden rounded-lg border border-gray-800 bg-gray-900 p-3">
                                            <pre className="overflow-x-auto font-mono text-[10px] text-gray-300">
                                                {data.groupRetryPolicyJson}
                                            </pre>
                                        </div>
                                    </div>
                                    <div className="space-y-1">
                                        <label className="block text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">
                                            Subscription Topics
                                        </label>
                                        <div className="rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 text-sm text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
                                            {data.subscriptionTopics.length > 0
                                                ? data.subscriptionTopics.join(', ')
                                                : 'No subscription topics declared in the group config.'}
                                        </div>
                                    </div>
                                    <div className="space-y-1">
                                        <label className="block text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">
                                            Attributes
                                        </label>
                                        <div className="rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 text-sm text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
                                            {data.attributes.length > 0 ? (
                                                <div className="space-y-2">
                                                    {data.attributes.map((attribute) => (
                                                        <div
                                                            key={attribute.key}
                                                            className="flex items-start justify-between gap-3 font-mono text-xs"
                                                        >
                                                            <span className="text-gray-500 dark:text-gray-400">
                                                                {attribute.key}
                                                            </span>
                                                            <span className="break-all text-right text-gray-800 dark:text-gray-200">
                                                                {attribute.value}
                                                            </span>
                                                        </div>
                                                    ))}
                                                </div>
                                            ) : (
                                                'No custom attributes are configured.'
                                            )}
                                        </div>
                                    </div>
                                </section>
                            </div>
                        ) : (
                            <div className="rounded-2xl border border-dashed border-gray-200 bg-white/70 px-6 py-16 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/70 dark:text-gray-400">
                                No consumer configuration was returned.
                            </div>
                        )}
                    </div>

                    <div className="z-10 flex shrink-0 justify-end gap-3 border-t border-gray-100 bg-white px-6 py-4 dark:border-gray-800 dark:bg-gray-900">
                        {consumer && !isLoading && !error && onEdit ? (
                            <button
                                onClick={() => onEdit(consumer, selectedBrokerAddress)}
                                className="rounded-lg bg-gray-900 px-6 py-2 text-sm font-medium text-white shadow-md transition-all hover:bg-gray-800 hover:shadow-lg dark:border dark:border-gray-700 dark:!bg-gray-900 dark:!text-white dark:hover:!bg-gray-800"
                            >
                                Edit Group
                            </button>
                        ) : null}
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

interface ConfigFieldProps {
    label: string;
    value: string;
    mono?: boolean;
}

const ConfigField = ({ label, value, mono = false }: ConfigFieldProps) => (
    <div className="space-y-1">
        <label className="block text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">
            {label}
        </label>
        <div
            className={`rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 text-sm text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 ${
                mono ? 'font-mono' : ''
            }`}
        >
            {value}
        </div>
    </div>
);

const ConfigToggleField = ({
    label,
    enabled,
}: {
    label: string;
    enabled: boolean;
}) => (
    <div className="space-y-1">
        <label className="block text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">
            {label}
        </label>
        <div className="flex items-center justify-between rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 dark:border-gray-700 dark:bg-gray-800">
            <span className="text-sm text-gray-600 dark:text-gray-300">
                {enabled ? 'Enabled' : 'Disabled'}
            </span>
            <div
                className={`h-6 w-11 rounded-full p-0.5 transition ${
                    enabled ? 'bg-blue-500' : 'bg-gray-200 dark:bg-gray-700'
                }`}
            >
                <div
                    className={`h-5 w-5 rounded-full bg-white shadow-sm transition-transform ${
                        enabled ? 'translate-x-5' : 'translate-x-0'
                    }`}
                />
            </div>
        </div>
    </div>
);
