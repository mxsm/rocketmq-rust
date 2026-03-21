import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    AlertCircle,
    CheckSquare,
    Layers,
    LoaderCircle,
    Plus,
    Save,
    Settings,
    X,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { ClusterService } from '../../../services/cluster.service';
import { ConsumerService } from '../../../services/consumer.service';
import type { ClusterBrokerCardItem } from '../../cluster/types/cluster.types';
import type {
    ConsumerConfigView,
    ConsumerCreateOrUpdateRequest,
    ConsumerGroupListItem,
    ConsumerMutationResult,
} from '../types/consumer.types';

interface ConsumerEditorModalProps {
    isOpen: boolean;
    onClose: () => void;
    consumer: ConsumerGroupListItem | null;
    preferredBrokerAddress?: string;
    onSaved: (result: ConsumerMutationResult) => void;
}

interface ClusterBrokerOptionGroup {
    clusterName: string;
    brokers: string[];
}

interface ConsumerEditorFormState {
    consumerGroup: string;
    clusterNameList: string[];
    brokerNameList: string[];
    consumeEnable: boolean;
    consumeFromMinEnable: boolean;
    consumeBroadcastEnable: boolean;
    consumeMessageOrderly: boolean;
    retryQueueNums: number;
    retryMaxTimes: number;
    brokerId: number;
    whichBrokerWhenConsumeSlowly: number;
    notifyConsumerIdsChangedEnable: boolean;
    groupSysFlag: number;
    consumeTimeoutMinute: number;
}

const createDefaultFormState = (): ConsumerEditorFormState => ({
    consumerGroup: '',
    clusterNameList: [],
    brokerNameList: [],
    consumeEnable: true,
    consumeFromMinEnable: true,
    consumeBroadcastEnable: true,
    consumeMessageOrderly: false,
    retryQueueNums: 1,
    retryMaxTimes: 16,
    brokerId: 0,
    whichBrokerWhenConsumeSlowly: 1,
    notifyConsumerIdsChangedEnable: true,
    groupSysFlag: 0,
    consumeTimeoutMinute: 15,
});

const getConsumerLabel = (consumer: ConsumerGroupListItem | null) =>
    consumer?.displayGroupName ?? consumer?.rawGroupName ?? 'New Consumer Group';

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

const buildClusterOptions = (items: ClusterBrokerCardItem[]): ClusterBrokerOptionGroup[] => {
    const grouped = new Map<string, Set<string>>();
    for (const item of items) {
        const clusterName = item.clusterName?.trim();
        const brokerName = item.brokerName?.trim();
        if (!clusterName || !brokerName) {
            continue;
        }
        const brokers = grouped.get(clusterName) ?? new Set<string>();
        brokers.add(brokerName);
        grouped.set(clusterName, brokers);
    }

    return [...grouped.entries()]
        .map(([clusterName, brokers]) => ({
            clusterName,
            brokers: [...brokers].sort((left, right) => left.localeCompare(right)),
        }))
        .sort((left, right) => left.clusterName.localeCompare(right.clusterName));
};

const deriveClusterSelection = (
    options: ClusterBrokerOptionGroup[],
    brokerNames: string[],
): string[] => {
    const selected = new Set(brokerNames);
    return options
        .filter((option) => option.brokers.some((brokerName) => selected.has(brokerName)))
        .map((option) => option.clusterName);
};

const mergeConfigIntoForm = (
    config: ConsumerConfigView,
    clusterNameList: string[],
    brokerNameList: string[],
): ConsumerEditorFormState => ({
    consumerGroup: config.consumerGroup,
    clusterNameList,
    brokerNameList,
    consumeEnable: config.consumeEnable,
    consumeFromMinEnable: config.consumeFromMinEnable,
    consumeBroadcastEnable: config.consumeBroadcastEnable,
    consumeMessageOrderly: config.consumeMessageOrderly,
    retryQueueNums: config.retryQueueNums,
    retryMaxTimes: config.retryMaxTimes,
    brokerId: config.brokerId,
    whichBrokerWhenConsumeSlowly: config.whichBrokerWhenConsumeSlowly,
    notifyConsumerIdsChangedEnable: config.notifyConsumerIdsChangedEnable,
    groupSysFlag: config.groupSysFlag,
    consumeTimeoutMinute: config.consumeTimeoutMinute,
});

export const ConsumerEditorModal = ({
    isOpen,
    onClose,
    consumer,
    preferredBrokerAddress,
    onSaved,
}: ConsumerEditorModalProps) => {
    const isEditMode = Boolean(consumer);
    const [form, setForm] = useState<ConsumerEditorFormState>(createDefaultFormState);
    const [clusterOptions, setClusterOptions] = useState<ClusterBrokerOptionGroup[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        let cancelled = false;
        setIsLoading(true);
        setError('');

        const load = async () => {
            try {
                const clusterHome = await ClusterService.getClusterHomePage({ forceRefresh: false });
                const options = buildClusterOptions(clusterHome.items);
                if (cancelled) {
                    return;
                }
                setClusterOptions(options);

                if (!consumer) {
                    setForm(createDefaultFormState());
                    return;
                }

                const brokerNames = [...(consumer.brokerNames ?? [])].sort((left, right) =>
                    left.localeCompare(right),
                );
                const clusterNames = deriveClusterSelection(options, brokerNames);
                const configAddress = preferredBrokerAddress || consumer.brokerAddresses[0];
                if (!configAddress) {
                    setForm({
                        ...createDefaultFormState(),
                        consumerGroup: consumer.rawGroupName,
                        brokerNameList: brokerNames,
                        clusterNameList: clusterNames,
                    });
                    return;
                }

                const config = await ConsumerService.queryConsumerConfig({
                    consumerGroup: consumer.rawGroupName,
                    address: configAddress,
                });
                if (!cancelled) {
                    setForm(mergeConfigIntoForm(config, clusterNames, brokerNames));
                }
            } catch (loadError) {
                if (!cancelled) {
                    setError(getErrorMessage(loadError, 'Failed to load consumer group editor data.'));
                    setForm({
                        ...createDefaultFormState(),
                        consumerGroup: consumer?.rawGroupName ?? '',
                        brokerNameList: consumer?.brokerNames ?? [],
                    });
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void load();

        return () => {
            cancelled = true;
        };
    }, [consumer, isOpen, preferredBrokerAddress]);

    const brokerOptions = useMemo(
        () =>
            clusterOptions.flatMap((option) =>
                option.brokers.map((brokerName) => ({
                    brokerName,
                    clusterName: option.clusterName,
                })),
            ),
        [clusterOptions],
    );

    const updateField = <K extends keyof ConsumerEditorFormState>(
        key: K,
        value: ConsumerEditorFormState[K],
    ) => {
        setForm((current) => ({ ...current, [key]: value }));
    };

    const toggleCluster = (clusterName: string) => {
        setForm((current) => ({
            ...current,
            clusterNameList: current.clusterNameList.includes(clusterName)
                ? current.clusterNameList.filter((item) => item !== clusterName)
                : [...current.clusterNameList, clusterName].sort((left, right) => left.localeCompare(right)),
        }));
    };

    const toggleBroker = (brokerName: string) => {
        setForm((current) => ({
            ...current,
            brokerNameList: current.brokerNameList.includes(brokerName)
                ? current.brokerNameList.filter((item) => item !== brokerName)
                : [...current.brokerNameList, brokerName].sort((left, right) => left.localeCompare(right)),
        }));
    };

    const handleSubmit = async () => {
        setError('');
        const trimmedGroup = form.consumerGroup.trim();
        if (!trimmedGroup) {
            setError('Consumer group is required.');
            return;
        }
        if (form.clusterNameList.length === 0 && form.brokerNameList.length === 0) {
            setError('Select at least one cluster or broker target before saving.');
            return;
        }

        const request: ConsumerCreateOrUpdateRequest = {
            ...form,
            consumerGroup: trimmedGroup,
        };

        try {
            setIsSaving(true);
            const result = await ConsumerService.createOrUpdateConsumerGroup(request);
            toast.success(
                isEditMode
                    ? 'Consumer group configuration updated.'
                    : 'Consumer group created successfully.',
            );
            onSaved(result);
        } catch (saveError) {
            setError(getErrorMessage(saveError, 'Failed to save consumer group changes.'));
        } finally {
            setIsSaving(false);
        }
    };

    if (!isOpen) {
        return null;
    }

    const title = isEditMode ? 'Update Consumer Group' : 'Add Consumer Group';

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
                    className="relative flex max-h-[92vh] w-full max-w-6xl flex-col overflow-hidden rounded-xl border border-gray-100 bg-white shadow-2xl dark:border-gray-800 dark:bg-gray-900"
                >
                    <div className="z-10 flex shrink-0 items-center justify-between border-b border-gray-100 bg-white px-6 py-5 dark:border-gray-800 dark:bg-gray-900">
                        <div>
                            <h3 className="flex items-center text-xl font-bold text-gray-800 dark:text-white">
                                <Plus className="mr-2 h-5 w-5 text-blue-500" />
                                {title}
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

                    <div className="flex-1 overflow-y-auto bg-gray-50/50 p-6 dark:bg-gray-950/50">
                        {isLoading ? (
                            <div className="flex min-h-[360px] items-center justify-center">
                                <div className="flex items-center gap-3 rounded-full bg-white px-4 py-2 text-sm text-gray-600 shadow-sm dark:bg-gray-900 dark:text-gray-300">
                                    <LoaderCircle className="h-4 w-4 animate-spin" />
                                    Loading consumer group editor...
                                </div>
                            </div>
                        ) : (
                            <div className="space-y-6">
                                {error && (
                                    <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                        <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                                        <span>{error}</span>
                                    </div>
                                )}

                                <section className="grid grid-cols-1 gap-6 md:grid-cols-2">
                                    <div className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="flex items-center gap-2">
                                            <div className="rounded-lg bg-blue-50 p-1.5 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400">
                                                <Settings className="h-4 w-4" />
                                            </div>
                                            <h4 className="font-semibold text-gray-900 dark:text-white">Basic Information</h4>
                                        </div>
                                        <FormField label="Consumer Group">
                                            <input
                                                value={form.consumerGroup}
                                                onChange={(event) => updateField('consumerGroup', event.target.value)}
                                                readOnly={isEditMode}
                                                className="w-full rounded-xl border border-gray-200 bg-white px-3 py-2 text-sm text-gray-900 outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                                            />
                                        </FormField>
                                        <div className="grid grid-cols-2 gap-4">
                                            <ToggleField label="Consume Enable" checked={form.consumeEnable} onChange={(checked) => updateField('consumeEnable', checked)} />
                                            <ToggleField label="Consume From Min" checked={form.consumeFromMinEnable} onChange={(checked) => updateField('consumeFromMinEnable', checked)} />
                                            <ToggleField label="Broadcast Mode" checked={form.consumeBroadcastEnable} onChange={(checked) => updateField('consumeBroadcastEnable', checked)} />
                                            <ToggleField label="Orderly Consumption" checked={form.consumeMessageOrderly} onChange={(checked) => updateField('consumeMessageOrderly', checked)} />
                                        </div>
                                    </div>

                                    <div className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="flex items-center gap-2">
                                            <div className="rounded-lg bg-purple-50 p-1.5 text-purple-600 dark:bg-purple-900/30 dark:text-purple-400">
                                                <Layers className="h-4 w-4" />
                                            </div>
                                            <h4 className="font-semibold text-gray-900 dark:text-white">Target Scope</h4>
                                        </div>
                                        <p className="text-xs leading-5 text-gray-500 dark:text-gray-400">
                                            Select clusters, brokers, or both. Cluster selection applies the configuration to all brokers in that cluster, while broker selection lets you narrow or expand the exact write scope.
                                        </p>
                                        <OptionGroupList
                                            icon={<Layers className="h-4 w-4" />}
                                            title="Clusters"
                                            options={clusterOptions.map((option) => ({
                                                key: option.clusterName,
                                                label: `${option.clusterName} (${option.brokers.length})`,
                                                selected: form.clusterNameList.includes(option.clusterName),
                                                onToggle: () => toggleCluster(option.clusterName),
                                            }))}
                                        />
                                        <OptionGroupList
                                            icon={<CheckSquare className="h-4 w-4" />}
                                            title="Brokers"
                                            options={brokerOptions.map((option) => ({
                                                key: `${option.clusterName}:${option.brokerName}`,
                                                label: option.brokerName,
                                                description: option.clusterName,
                                                selected: form.brokerNameList.includes(option.brokerName),
                                                onToggle: () => toggleBroker(option.brokerName),
                                            }))}
                                        />
                                    </div>
                                </section>

                                <section className="grid grid-cols-1 gap-6 md:grid-cols-2">
                                    <div className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="flex items-center gap-2">
                                            <div className="rounded-lg bg-orange-50 p-1.5 text-orange-600 dark:bg-orange-900/30 dark:text-orange-400">
                                                <Save className="h-4 w-4" />
                                            </div>
                                            <h4 className="font-semibold text-gray-900 dark:text-white">Retry & Timeout</h4>
                                        </div>
                                        <NumberField label="Retry Queues" value={form.retryQueueNums} onChange={(value) => updateField('retryQueueNums', value)} />
                                        <NumberField label="Max Retries" value={form.retryMaxTimes} onChange={(value) => updateField('retryMaxTimes', value)} />
                                        <NumberField label="Consume Timeout (Minutes)" value={form.consumeTimeoutMinute} onChange={(value) => updateField('consumeTimeoutMinute', value)} />
                                        <NumberField label="Slow Consume Broker ID" value={form.whichBrokerWhenConsumeSlowly} onChange={(value) => updateField('whichBrokerWhenConsumeSlowly', value)} />
                                    </div>

                                    <div className="space-y-4 rounded-2xl border border-gray-200 bg-white p-5 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                                        <div className="flex items-center gap-2">
                                            <div className="rounded-lg bg-emerald-50 p-1.5 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-400">
                                                <Plus className="h-4 w-4" />
                                            </div>
                                            <h4 className="font-semibold text-gray-900 dark:text-white">Broker Policy</h4>
                                        </div>
                                        <NumberField label="Broker ID" value={form.brokerId} onChange={(value) => updateField('brokerId', value)} />
                                        <NumberField label="System Flag" value={form.groupSysFlag} onChange={(value) => updateField('groupSysFlag', value)} />
                                        <ToggleField label="Notify Consumer ID Changes" checked={form.notifyConsumerIdsChangedEnable} onChange={(checked) => updateField('notifyConsumerIdsChangedEnable', checked)} />
                                    </div>
                                </section>
                            </div>
                        )}
                    </div>

                    <div className="z-10 flex shrink-0 items-center justify-between border-t border-gray-100 bg-white px-6 py-4 dark:border-gray-800 dark:bg-gray-900">
                        <div className="text-xs text-gray-500 dark:text-gray-400">
                            {isEditMode
                                ? 'Updating a group keeps the current name and reapplies settings to the selected targets.'
                                : 'Creating a group writes the same subscription group config to all selected targets.'}
                        </div>
                        <div className="flex items-center gap-3">
                            <button
                                onClick={onClose}
                                className="rounded-xl border border-gray-200 px-4 py-2 text-sm font-medium text-gray-600 transition hover:bg-gray-50 dark:border-gray-700 dark:text-gray-300 dark:hover:bg-gray-800"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={() => void handleSubmit()}
                                disabled={isLoading || isSaving}
                                className="flex items-center rounded-xl bg-gray-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-gray-800 disabled:cursor-not-allowed disabled:opacity-60 dark:border dark:border-gray-700 dark:bg-gray-900 dark:text-white dark:hover:bg-gray-800"
                            >
                                {isSaving ? <LoaderCircle className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                                {isEditMode ? 'Save Changes' : 'Create Group'}
                            </button>
                        </div>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const FormField = ({ label, children }: { label: string; children: React.ReactNode }) => (
    <label className="block space-y-1">
        <span className="text-xs font-medium uppercase tracking-wider text-gray-500 dark:text-gray-400">{label}</span>
        {children}
    </label>
);

const NumberField = ({
    label,
    value,
    onChange,
}: {
    label: string;
    value: number;
    onChange: (value: number) => void;
}) => (
    <FormField label={label}>
        <input
            type="number"
            value={value}
            onChange={(event) => onChange(Number(event.target.value))}
            className="w-full rounded-xl border border-gray-200 bg-white px-3 py-2 text-sm text-gray-900 outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
        />
    </FormField>
);

const ToggleField = ({
    label,
    checked,
    onChange,
}: {
    label: string;
    checked: boolean;
    onChange: (checked: boolean) => void;
}) => (
    <label className="flex items-center justify-between gap-3 rounded-xl border border-gray-200 px-3 py-2.5 text-sm text-gray-700 dark:border-gray-700 dark:text-gray-200">
        <span>{label}</span>
        <button
            type="button"
            onClick={() => onChange(!checked)}
            className={`h-6 w-11 rounded-full p-0.5 transition ${checked ? 'bg-blue-500' : 'bg-gray-200 dark:bg-gray-700'}`}
        >
            <span className={`block h-5 w-5 rounded-full bg-white shadow-sm transition-transform ${checked ? 'translate-x-5' : 'translate-x-0'}`} />
        </button>
    </label>
);

const OptionGroupList = ({
    icon,
    title,
    options,
}: {
    icon: React.ReactNode;
    title: string;
    options: Array<{
        key: string;
        label: string;
        description?: string;
        selected: boolean;
        onToggle: () => void;
    }>;
}) => (
    <div className="space-y-3">
        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.18em] text-gray-400 dark:text-gray-500">
            {icon}
            <span>{title}</span>
        </div>
        {options.length === 0 ? (
            <div className="rounded-xl border border-dashed border-gray-200 px-3 py-4 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                No options available.
            </div>
        ) : (
            <div className="flex flex-wrap gap-2">
                {options.map((option) => (
                    <button
                        key={option.key}
                        type="button"
                        onClick={option.onToggle}
                        className={`rounded-full border px-3 py-1.5 text-sm transition ${
                            option.selected
                                ? 'border-blue-200 bg-blue-50 text-blue-700 dark:border-blue-800 dark:bg-blue-900/30 dark:text-blue-300'
                                : 'border-gray-200 bg-white text-gray-600 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 dark:hover:bg-gray-700'
                        }`}
                    >
                        {option.label}
                        {option.description ? <span className="ml-2 text-xs opacity-70">{option.description}</span> : null}
                    </button>
                ))}
            </div>
        )}
    </div>
);
