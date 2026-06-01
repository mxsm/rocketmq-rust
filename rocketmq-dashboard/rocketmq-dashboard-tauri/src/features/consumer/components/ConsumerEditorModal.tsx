import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    AlertCircle,
    Bell,
    CheckCircle2,
    CheckSquare,
    Hash,
    Layers,
    LoaderCircle,
    Plus,
    RotateCcw,
    Save,
    Settings,
    SlidersHorizontal,
    Target,
    X,
    type LucideIcon,
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

const EDITOR_SECTIONS = [
    {
        key: 'basic',
        label: 'Basic',
        description: 'Group identity and runtime switches',
        icon: Settings,
    },
    {
        key: 'targets',
        label: 'Targets',
        description: 'Clusters and exact broker scope',
        icon: Target,
    },
    {
        key: 'retry',
        label: 'Retry',
        description: 'Queues, retries, and timeout',
        icon: RotateCcw,
    },
    {
        key: 'broker',
        label: 'Broker Policy',
        description: 'Broker ID, flags, and notifications',
        icon: SlidersHorizontal,
    },
    {
        key: 'review',
        label: 'Review',
        description: 'Mutation summary before commit',
        icon: CheckCircle2,
    },
] as const;

type EditorSectionKey = (typeof EDITOR_SECTIONS)[number]['key'];

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

const formatCountLabel = (count: number, singular: string, plural = `${singular}s`) =>
    `${count} ${count === 1 ? singular : plural}`;

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
    const [activeSection, setActiveSection] = useState<EditorSectionKey>('basic');
    const [isLoading, setIsLoading] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (isOpen) {
            setActiveSection('basic');
        }
    }, [consumer, isOpen]);

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
            setActiveSection('basic');
            setError('Consumer group is required.');
            return;
        }
        if (form.clusterNameList.length === 0 && form.brokerNameList.length === 0) {
            setActiveSection('targets');
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
    const activeSectionConfig = EDITOR_SECTIONS.find((section) => section.key === activeSection) ?? EDITOR_SECTIONS[0];
    const ActiveIcon = activeSectionConfig.icon;
    const selectedTargetCount = form.clusterNameList.length + form.brokerNameList.length;
    const enabledControlCount = [
        form.consumeEnable,
        form.consumeFromMinEnable,
        form.consumeBroadcastEnable,
        form.notifyConsumerIdsChangedEnable,
    ].filter(Boolean).length;
    const clusterTargetLabel = formatCountLabel(form.clusterNameList.length, 'cluster');
    const brokerTargetLabel = formatCountLabel(form.brokerNameList.length, 'broker');

    const renderActiveSection = () => {
        switch (activeSection) {
            case 'targets':
                return (
                    <div className="consumer-editor-section-stack">
                        <OptionGroupList
                            icon={<Layers className="topic-icon" aria-hidden="true" />}
                            title="Clusters"
                            description="Select a cluster when every broker in that cluster should receive the same group config."
                            emptyLabel="Cluster discovery returned no selectable clusters."
                            options={clusterOptions.map((option) => ({
                                key: option.clusterName,
                                label: option.clusterName,
                                description: formatCountLabel(option.brokers.length, 'broker'),
                                selected: form.clusterNameList.includes(option.clusterName),
                                onToggle: () => toggleCluster(option.clusterName),
                            }))}
                        />
                        <OptionGroupList
                            icon={<CheckSquare className="topic-icon" aria-hidden="true" />}
                            title="Brokers"
                            description="Use broker targets for a narrower write scope or for mixed-cluster updates."
                            emptyLabel="No brokers were returned by the cluster inventory."
                            options={brokerOptions.map((option) => ({
                                key: `${option.clusterName}:${option.brokerName}`,
                                label: option.brokerName,
                                description: option.clusterName,
                                selected: form.brokerNameList.includes(option.brokerName),
                                onToggle: () => toggleBroker(option.brokerName),
                            }))}
                        />
                    </div>
                );
            case 'retry':
                return (
                    <div className="consumer-editor-number-grid">
                        <NumberField
                            label="Retry Queues"
                            value={form.retryQueueNums}
                            onChange={(value) => updateField('retryQueueNums', value)}
                        />
                        <NumberField
                            label="Max Retries"
                            value={form.retryMaxTimes}
                            onChange={(value) => updateField('retryMaxTimes', value)}
                        />
                        <NumberField
                            label="Consume Timeout"
                            unit="minutes"
                            value={form.consumeTimeoutMinute}
                            onChange={(value) => updateField('consumeTimeoutMinute', value)}
                        />
                        <NumberField
                            label="Slow Consume Broker ID"
                            value={form.whichBrokerWhenConsumeSlowly}
                            onChange={(value) => updateField('whichBrokerWhenConsumeSlowly', value)}
                        />
                    </div>
                );
            case 'broker':
                return (
                    <div className="consumer-editor-section-stack">
                        <div className="consumer-editor-number-grid">
                            <NumberField
                                label="Broker ID"
                                value={form.brokerId}
                                onChange={(value) => updateField('brokerId', value)}
                            />
                            <NumberField
                                label="System Flag"
                                value={form.groupSysFlag}
                                onChange={(value) => updateField('groupSysFlag', value)}
                            />
                        </div>
                        <ToggleField
                            label="Notify Consumer ID Changes"
                            description="Pushes group metadata changes to online consumer IDs when the broker supports it."
                            checked={form.notifyConsumerIdsChangedEnable}
                            onChange={(checked) => updateField('notifyConsumerIdsChangedEnable', checked)}
                        />
                    </div>
                );
            case 'review':
                return (
                    <div className="consumer-editor-review-grid">
                        <ReviewLine label="Consumer Group" value={form.consumerGroup.trim() || 'Not set'} mono />
                        <ReviewLine label="Target Scope" value={`${clusterTargetLabel} / ${brokerTargetLabel}`} />
                        <ReviewLine label="Delivery Controls" value={`${enabledControlCount} enabled / orderly ${form.consumeMessageOrderly ? 'on' : 'off'}`} />
                        <ReviewLine label="Retry Policy" value={`${form.retryQueueNums} queues / ${form.retryMaxTimes} max retries`} />
                        <ReviewLine label="Timeout" value={`${form.consumeTimeoutMinute} minutes`} />
                        <ReviewLine label="Broker Policy" value={`broker ${form.brokerId} / flag ${form.groupSysFlag}`} mono />
                    </div>
                );
            case 'basic':
            default:
                return (
                    <div className="consumer-editor-section-stack">
                        <TextField
                            label="Consumer Group"
                            value={form.consumerGroup}
                            required
                            readOnly={isEditMode}
                            placeholder="please_rename_unique_group_name"
                            onChange={(value) => updateField('consumerGroup', value)}
                        />
                        <div className="consumer-editor-toggle-grid">
                            <ToggleField
                                label="Consume Enable"
                                description="Allow consumers to receive messages for this group."
                                checked={form.consumeEnable}
                                onChange={(checked) => updateField('consumeEnable', checked)}
                            />
                            <ToggleField
                                label="Consume From Min"
                                description="Start from the minimum queue offset when there is no local progress."
                                checked={form.consumeFromMinEnable}
                                onChange={(checked) => updateField('consumeFromMinEnable', checked)}
                            />
                            <ToggleField
                                label="Broadcast Mode"
                                description="Enable broadcast-style consumption semantics."
                                checked={form.consumeBroadcastEnable}
                                onChange={(checked) => updateField('consumeBroadcastEnable', checked)}
                            />
                            <ToggleField
                                label="Orderly Consumption"
                                description="Preserve queue ordering when the group requires ordered delivery."
                                checked={form.consumeMessageOrderly}
                                onChange={(checked) => updateField('consumeMessageOrderly', checked)}
                            />
                        </div>
                    </div>
                );
        }
    };

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root">
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{ opacity: 0, scale: 0.96, y: 12 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.96, y: 12 }}
                    className="topic-status-dialog consumer-editor-dialog"
                >
                    <header className="topic-status-header consumer-editor-header">
                        <div className="topic-status-title-wrap">
                            <span className="topic-status-icon consumer-editor-title-icon">
                                <Plus className="topic-icon" aria-hidden="true" />
                            </span>
                            <div>
                                <span>Consumer group mutation</span>
                                <h3>{title}</h3>
                                <p>
                                    <strong>{getConsumerLabel(consumer)}</strong>
                                </p>
                            </div>
                        </div>

                        <div className="topic-status-header-actions consumer-editor-header-actions">
                            <EditorChip tone="success" label={isEditMode ? 'Update mode' : 'Create mode'} />
                            <EditorChip tone={selectedTargetCount > 0 ? 'info' : 'warning'} label={formatCountLabel(selectedTargetCount, 'target')} />
                            <button
                                type="button"
                                onClick={onClose}
                                className="topic-status-close"
                                aria-label="Close consumer group editor"
                            >
                                <X className="topic-icon" aria-hidden="true" />
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body consumer-editor-body">
                        {isLoading ? (
                            <div className="topic-status-state">
                                <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true" />
                                <strong>Loading consumer group editor</strong>
                                <span>Resolving cluster inventory and broker-side group configuration.</span>
                            </div>
                        ) : (
                            <>
                                {error ? (
                                    <div className="topic-status-state is-error consumer-editor-error">
                                        <AlertCircle className="topic-icon" aria-hidden="true" />
                                        <strong>Consumer group editor needs attention</strong>
                                        <span>{error}</span>
                                    </div>
                                ) : null}

                                <section className="consumer-editor-target-strip" aria-label="Selected consumer group target scope">
                                    <div className="consumer-editor-target-copy">
                                        <span className="consumer-editor-panel-icon is-violet">
                                            <Layers className="topic-icon" aria-hidden="true" />
                                        </span>
                                        <div>
                                            <span>Target Scope</span>
                                            <strong>{selectedTargetCount > 0 ? `${clusterTargetLabel} / ${brokerTargetLabel}` : 'No target selected'}</strong>
                                            <small>Cluster selection fans out to all brokers; broker selection applies exact write scope.</small>
                                        </div>
                                    </div>
                                    <div className="consumer-editor-target-pills">
                                        <TargetPreviewChips
                                            clusters={form.clusterNameList}
                                            brokers={form.brokerNameList}
                                            onOpenTargets={() => setActiveSection('targets')}
                                        />
                                    </div>
                                </section>

                                <section className="topic-status-kpi-grid consumer-editor-summary-grid" aria-label="Consumer group mutation summary">
                                    <EditorKpiCard
                                        label="Identity"
                                        value={form.consumerGroup.trim() || 'New group'}
                                        note={isEditMode ? 'name locked while editing' : 'name required before save'}
                                        tone="blue"
                                        icon={Hash}
                                    />
                                    <EditorKpiCard
                                        label="Controls"
                                        value={`${enabledControlCount} on`}
                                        note={`orderly ${form.consumeMessageOrderly ? 'enabled' : 'disabled'}`}
                                        tone="success"
                                        icon={Activity}
                                    />
                                    <EditorKpiCard
                                        label="Retry Policy"
                                        value={`${form.retryMaxTimes} max`}
                                        note={`${form.retryQueueNums} queue / ${form.consumeTimeoutMinute} min`}
                                        tone="warning"
                                        icon={RotateCcw}
                                    />
                                    <EditorKpiCard
                                        label="Broker Policy"
                                        value={`ID ${form.brokerId}`}
                                        note={form.notifyConsumerIdsChangedEnable ? 'notify changes enabled' : 'notify changes disabled'}
                                        tone="violet"
                                        icon={Bell}
                                    />
                                </section>

                                <section className="consumer-editor-workspace">
                                    <aside className="consumer-editor-nav-panel" aria-label="Consumer editor sections">
                                        <div className="consumer-editor-nav-heading">
                                            <strong>Create Flow</strong>
                                            <span>Review each section before committing.</span>
                                        </div>
                                        {EDITOR_SECTIONS.map((section, index) => {
                                            const SectionIcon = section.icon;
                                            return (
                                                <button
                                                    key={section.key}
                                                    type="button"
                                                    className={`consumer-editor-nav-row ${section.key === activeSection ? 'is-selected' : ''}`}
                                                    onClick={() => setActiveSection(section.key)}
                                                >
                                                    <span className="consumer-editor-nav-index">{index + 1}</span>
                                                    <span className="consumer-editor-nav-copy">
                                                        <strong>{section.label}</strong>
                                                        <small>{section.description}</small>
                                                    </span>
                                                    <SectionIcon className="topic-icon" aria-hidden="true" />
                                                </button>
                                            );
                                        })}
                                    </aside>

                                    <section className="consumer-editor-content-panel" aria-label={`${activeSectionConfig.label} editor section`}>
                                        <div className="consumer-editor-panel-header">
                                            <div className="consumer-editor-panel-title">
                                                <span className="consumer-editor-panel-icon">
                                                    <ActiveIcon className="topic-icon" aria-hidden="true" />
                                                </span>
                                                <div>
                                                    <h4>{activeSectionConfig.label}</h4>
                                                    <p>{activeSectionConfig.description}</p>
                                                </div>
                                            </div>
                                            <span>{isEditMode ? 'Update' : 'Create'}</span>
                                        </div>
                                        <div className="consumer-editor-panel-body">{renderActiveSection()}</div>
                                    </section>

                                    <aside className="consumer-editor-review-panel" aria-label="Commit preview">
                                        <div className="consumer-editor-review-heading">
                                            <span className="consumer-editor-panel-icon is-success">
                                                <CheckCircle2 className="topic-icon" aria-hidden="true" />
                                            </span>
                                            <div>
                                                <h4>Commit Preview</h4>
                                                <p>Same request payload is applied to selected targets.</p>
                                            </div>
                                        </div>
                                        <div className="consumer-editor-review-list">
                                            <ReviewLine label="Targets" value={`${clusterTargetLabel} / ${brokerTargetLabel}`} />
                                            <ReviewLine label="Delivery" value={`${enabledControlCount} controls enabled`} />
                                            <ReviewLine label="Retry" value={`${form.retryQueueNums} queues, ${form.retryMaxTimes} max`} />
                                            <ReviewLine label="Timeout" value={`${form.consumeTimeoutMinute} minutes`} />
                                        </div>
                                    </aside>
                                </section>
                            </>
                        )}
                    </div>

                    <footer className="topic-status-footer consumer-editor-footer">
                        <span>
                            {isEditMode
                                ? 'Updating a group keeps the current name and reapplies settings to the selected targets.'
                                : 'Creating a group writes the same subscription group config to all selected targets.'}
                        </span>
                        <div>
                            <button type="button" onClick={onClose} className="topic-status-secondary-button">
                                Cancel
                            </button>
                            <button
                                type="button"
                                onClick={() => void handleSubmit()}
                                disabled={isLoading || isSaving}
                                className="topic-status-primary-button consumer-editor-save-button"
                            >
                                {isSaving ? (
                                    <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true" />
                                ) : (
                                    <Save className="topic-icon" aria-hidden="true" />
                                )}
                                {isEditMode ? 'Save Changes' : 'Create Group'}
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const EditorChip = ({
    label,
    tone,
}: {
    label: string;
    tone: 'success' | 'warning' | 'info';
}) => (
    <span className={`consumer-editor-chip is-${tone}`}>
        <i aria-hidden="true" />
        {label}
    </span>
);

const EditorKpiCard = ({
    label,
    value,
    note,
    tone,
    icon: Icon,
}: {
    label: string;
    value: string;
    note: string;
    tone: 'success' | 'warning' | 'blue' | 'violet';
    icon: LucideIcon;
}) => (
    <div className={`topic-status-kpi consumer-editor-kpi is-${tone}`}>
        <div>
            <span>{label}</span>
            <strong title={value}>{value}</strong>
            <small>{note}</small>
        </div>
        <Icon className="topic-icon" aria-hidden="true" />
    </div>
);

const TargetPreviewChips = ({
    clusters,
    brokers,
    onOpenTargets,
}: {
    clusters: string[];
    brokers: string[];
    onOpenTargets: () => void;
}) => {
    const targets = [
        ...clusters.map((cluster) => ({ key: `cluster:${cluster}`, label: cluster, type: 'Cluster' })),
        ...brokers.map((broker) => ({ key: `broker:${broker}`, label: broker, type: 'Broker' })),
    ];

    if (targets.length === 0) {
        return (
            <button type="button" onClick={onOpenTargets} className="consumer-editor-target-empty">
                Select targets
            </button>
        );
    }

    const visibleTargets = targets.slice(0, 3);
    return (
        <>
            {visibleTargets.map((target) => (
                <button
                    key={target.key}
                    type="button"
                    onClick={onOpenTargets}
                    className="consumer-editor-target-chip"
                    title={`${target.type}: ${target.label}`}
                >
                    <span>{target.type}</span>
                    <strong>{target.label}</strong>
                </button>
            ))}
            {targets.length > visibleTargets.length ? (
                <button type="button" onClick={onOpenTargets} className="consumer-editor-target-more">
                    +{targets.length - visibleTargets.length}
                </button>
            ) : null}
        </>
    );
};

const TextField = ({
    label,
    value,
    required = false,
    readOnly = false,
    placeholder,
    onChange,
}: {
    label: string;
    value: string;
    required?: boolean;
    readOnly?: boolean;
    placeholder?: string;
    onChange: (value: string) => void;
}) => (
    <label className="consumer-editor-field is-wide">
        <span>{label}{required ? ' *' : ''}</span>
        <input
            value={value}
            readOnly={readOnly}
            placeholder={placeholder}
            onChange={(event) => onChange(event.target.value)}
            className={readOnly ? 'is-readonly' : ''}
        />
        {readOnly ? <small>Group name is locked while updating an existing consumer group.</small> : null}
    </label>
);

const NumberField = ({
    label,
    value,
    unit,
    onChange,
}: {
    label: string;
    value: number;
    unit?: string;
    onChange: (value: number) => void;
}) => (
    <label className="consumer-editor-field">
        <span>{label}</span>
        <input
            type="number"
            min="0"
            value={value}
            onChange={(event) => onChange(Number(event.target.value))}
        />
        {unit ? <small>{unit}</small> : null}
    </label>
);

const ToggleField = ({
    label,
    description,
    checked,
    onChange,
}: {
    label: string;
    description?: string;
    checked: boolean;
    onChange: (checked: boolean) => void;
}) => (
    <button
        type="button"
        onClick={() => onChange(!checked)}
        className={`consumer-editor-toggle ${checked ? 'is-enabled' : 'is-disabled'}`}
    >
        <span>
            <strong>{label}</strong>
            {description ? <small>{description}</small> : null}
        </span>
        <i aria-hidden="true" />
    </button>
);

const OptionGroupList = ({
    icon,
    title,
    description,
    emptyLabel,
    options,
}: {
    icon: ReactNode;
    title: string;
    description: string;
    emptyLabel: string;
    options: Array<{
        key: string;
        label: string;
        description?: string;
        selected: boolean;
        onToggle: () => void;
    }>;
}) => (
    <div className="consumer-editor-option-group">
        <div className="consumer-editor-option-heading">
            <span className="consumer-editor-panel-icon">{icon}</span>
            <div>
                <strong>{title}</strong>
                <small>{description}</small>
            </div>
        </div>
        {options.length === 0 ? (
            <div className="consumer-editor-empty-option">{emptyLabel}</div>
        ) : (
            <div className="consumer-editor-option-list">
                {options.map((option) => (
                    <button
                        key={option.key}
                        type="button"
                        onClick={option.onToggle}
                        className={`consumer-editor-option-chip ${option.selected ? 'is-selected' : ''}`}
                    >
                        <span>{option.label}</span>
                        {option.description ? <small>{option.description}</small> : null}
                    </button>
                ))}
            </div>
        )}
    </div>
);

const ReviewLine = ({
    label,
    value,
    mono = false,
}: {
    label: string;
    value: string;
    mono?: boolean;
}) => (
    <div className="consumer-editor-review-line">
        <span>{label}</span>
        <strong className={mono ? 'is-mono' : ''} title={value}>{value}</strong>
    </div>
);
