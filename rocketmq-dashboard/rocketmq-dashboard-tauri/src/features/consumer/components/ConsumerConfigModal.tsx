import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    AlertCircle,
    Cpu,
    Database,
    FileText,
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

const CONFIG_SECTIONS = [
    {
        key: 'overview',
        label: 'Overview',
        description: 'Identity and broker placement',
        icon: Hash,
    },
    {
        key: 'controls',
        label: 'Controls',
        description: 'Runtime switches',
        icon: Activity,
    },
    {
        key: 'retry',
        label: 'Retry',
        description: 'Queues and throttling',
        icon: RotateCcw,
    },
    {
        key: 'policy',
        label: 'Policy JSON',
        description: 'Retry schedule and topics',
        icon: Cpu,
    },
    {
        key: 'attributes',
        label: 'Attributes',
        description: 'Custom metadata',
        icon: Database,
    },
] as const;

type ConfigSectionKey = (typeof CONFIG_SECTIONS)[number]['key'];

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
    const [activeSection, setActiveSection] = useState<ConfigSectionKey>('overview');
    const [data, setData] = useState<ConsumerConfigView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !consumer) {
            return;
        }

        setActiveSection('overview');
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
    const activeSectionConfig = CONFIG_SECTIONS.find((section) => section.key === activeSection) ?? CONFIG_SECTIONS[0];
    const ActiveIcon = activeSectionConfig.icon;

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
                    className="topic-status-dialog consumer-config-dialog"
                >
                    <header className="topic-status-header consumer-config-header">
                        <div className="topic-status-title-wrap">
                            <span className="topic-status-icon">
                                <Settings className="topic-icon" aria-hidden="true" />
                            </span>
                            <div>
                                <span>Consumer group configuration</span>
                                <h3>Configuration</h3>
                                <p>
                                    <strong>{consumerLabel}</strong>
                                </p>
                            </div>
                        </div>

                        <div className="topic-status-header-actions consumer-config-header-actions">
                            {data && (
                                <>
                                    <span className={`consumer-config-chip ${data.consumeEnable ? 'is-healthy' : 'is-warning'}`}>
                                        <i aria-hidden="true" />
                                        {data.consumeEnable ? 'Enabled' : 'Disabled'}
                                    </span>
                                    <span className="consumer-config-chip is-info">
                                        {brokerAddresses.length || 0} broker{brokerAddresses.length === 1 ? '' : 's'}
                                    </span>
                                </>
                            )}
                            <button
                                type="button"
                                onClick={onClose}
                                className="topic-status-close"
                                aria-label="Close consumer configuration dialog"
                            >
                                <X className="topic-icon" aria-hidden="true" />
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body consumer-config-body">
                        <section className="consumer-config-broker-scope" aria-label="Broker address scope">
                            <div>
                                <span>Broker Address</span>
                                <strong>{selectedBrokerAddress || '-'}</strong>
                                {data?.brokerName ? <small>{data.brokerName}</small> : null}
                            </div>
                            <label className="consumer-config-broker-select">
                                <Network className="topic-icon" aria-hidden="true" />
                                <select
                                    value={selectedBrokerAddress}
                                    onChange={(event) => setSelectedBrokerAddress(event.target.value)}
                                    disabled={brokerAddresses.length === 0}
                                >
                                    {brokerAddresses.length > 0 ? (
                                        brokerAddresses.map((address) => (
                                            <option key={address} value={address}>
                                                {address}
                                            </option>
                                        ))
                                    ) : (
                                        <option value="">No broker address</option>
                                    )}
                                </select>
                            </label>
                        </section>

                        {isLoading ? (
                            <div className="topic-status-state">
                                <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true" />
                                <strong>Loading consumer configuration</strong>
                                <span>Querying group settings from the selected broker address.</span>
                            </div>
                        ) : error ? (
                            <div className="topic-status-state is-error">
                                <AlertCircle className="topic-icon" aria-hidden="true" />
                                <strong>Unable to load consumer configuration</strong>
                                <span>{error}</span>
                            </div>
                        ) : data ? (
                            <>
                                <section className="topic-status-kpi-grid consumer-config-summary-grid" aria-label="Consumer configuration summary">
                                    <ConfigKpiCard
                                        label="Consume"
                                        value={data.consumeEnable ? 'Enabled' : 'Disabled'}
                                        note="delivery accepted"
                                        tone={data.consumeEnable ? 'success' : 'warning'}
                                        icon={Activity}
                                    />
                                    <ConfigKpiCard
                                        label="Retry Max"
                                        value={String(data.retryMaxTimes)}
                                        note="max attempts"
                                        tone="warning"
                                        icon={RotateCcw}
                                    />
                                    <ConfigKpiCard
                                        label="Timeout"
                                        value={`${data.consumeTimeoutMinute} min`}
                                        note="consume timeout"
                                        tone="blue"
                                        icon={Cpu}
                                    />
                                    <ConfigKpiCard
                                        label="Topics"
                                        value={String(data.subscriptionTopicCount)}
                                        note="subscriptions"
                                        tone="violet"
                                        icon={Database}
                                    />
                                </section>

                                <section className="consumer-config-workspace">
                                    <aside className="consumer-config-nav-panel" aria-label="Configuration sections">
                                        {CONFIG_SECTIONS.map((section) => {
                                            const SectionIcon = section.icon;
                                            return (
                                                <button
                                                    key={section.key}
                                                    type="button"
                                                    className={`consumer-config-nav-row ${section.key === activeSection ? 'is-selected' : ''}`}
                                                    onClick={() => setActiveSection(section.key)}
                                                >
                                                    <span className="consumer-config-nav-icon">
                                                        <SectionIcon className="topic-icon" aria-hidden="true" />
                                                    </span>
                                                    <span className="consumer-config-nav-copy">
                                                        <strong>{section.label}</strong>
                                                        <small>{section.description}</small>
                                                    </span>
                                                </button>
                                            );
                                        })}
                                    </aside>

                                    <section className="consumer-config-content-panel" aria-label={`${activeSectionConfig.label} configuration`}>
                                        <div className="consumer-config-panel-header">
                                            <div className="consumer-config-panel-title">
                                                <span className="consumer-config-panel-icon">
                                                    <ActiveIcon className="topic-icon" aria-hidden="true" />
                                                </span>
                                                <div>
                                                    <h4>{activeSectionConfig.label}</h4>
                                                    <p>{activeSectionConfig.description}</p>
                                                </div>
                                            </div>
                                            <span>{data.brokerName || selectedBrokerAddress || 'broker scope'}</span>
                                        </div>

                                        {renderConfigSection(data, activeSection)}
                                    </section>
                                </section>
                            </>
                        ) : (
                            <div className="topic-status-state">
                                <Settings className="topic-icon" aria-hidden="true" />
                                <strong>No consumer configuration</strong>
                                <span>No consumer configuration was returned for the selected broker address.</span>
                            </div>
                        )}
                    </div>

                    <footer className="topic-status-footer consumer-config-footer">
                        <span>Configuration is read-only until Edit Group opens the scoped mutation flow.</span>
                        <div>
                            {consumer && !isLoading && !error && onEdit ? (
                                <button
                                    type="button"
                                    onClick={() => onEdit(consumer, selectedBrokerAddress)}
                                    className="topic-status-primary-button consumer-config-edit-button"
                                >
                                    Edit Group
                                </button>
                            ) : null}
                            <button type="button" onClick={onClose} className="topic-status-secondary-button">
                                Close
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const renderConfigSection = (data: ConsumerConfigView, activeSection: ConfigSectionKey) => {
    switch (activeSection) {
        case 'controls':
            return (
                <div className="consumer-config-toggle-grid">
                    <ConfigToggleField label="Consume Enable" enabled={data.consumeEnable} />
                    <ConfigToggleField label="Consume From Min" enabled={data.consumeFromMinEnable} />
                    <ConfigToggleField label="Broadcast Consumption" enabled={data.consumeBroadcastEnable} />
                    <ConfigToggleField label="Orderly Consumption" enabled={data.consumeMessageOrderly} />
                    <ConfigToggleField label="Notify Client Change" enabled={data.notifyConsumerIdsChangedEnable} />
                </div>
            );
        case 'retry':
            return (
                <div className="consumer-config-value-grid">
                    <ConfigValueField label="Retry Queues" value={String(data.retryQueueNums)} mono />
                    <ConfigValueField label="Max Retries" value={String(data.retryMaxTimes)} mono />
                    <ConfigValueField label="Slow Consume Broker" value={String(data.whichBrokerWhenConsumeSlowly)} mono />
                    <ConfigValueField label="Consume Timeout" value={`${data.consumeTimeoutMinute} minutes`} mono />
                    <ConfigValueField label="Subscription Topics" value={String(data.subscriptionTopicCount)} mono />
                </div>
            );
        case 'policy':
            return (
                <div className="consumer-config-policy-layout">
                    <div className="consumer-config-code-block">
                        <span>Retry Policy JSON</span>
                        <pre>{data.groupRetryPolicyJson || '{}'}</pre>
                    </div>
                    <div className="consumer-config-topic-list">
                        <span>Subscription Topics</span>
                        {data.subscriptionTopics.length > 0 ? (
                            <div>
                                {data.subscriptionTopics.map((topic) => (
                                    <b key={topic}>{topic}</b>
                                ))}
                            </div>
                        ) : (
                            <strong>No subscription topics declared in the group config.</strong>
                        )}
                    </div>
                </div>
            );
        case 'attributes':
            return data.attributes.length > 0 ? (
                <div className="consumer-config-attr-list">
                    {data.attributes.map((attribute) => (
                        <div key={attribute.key}>
                            <span>{attribute.key}</span>
                            <strong>{attribute.value}</strong>
                        </div>
                    ))}
                </div>
            ) : (
                <div className="consumer-config-empty-state">
                    <FileText className="topic-icon" aria-hidden="true" />
                    <strong>No custom attributes</strong>
                    <span>This broker did not return additional consumer group attributes.</span>
                </div>
            );
        case 'overview':
        default:
            return (
                <div className="consumer-config-value-grid">
                    <ConfigValueField label="Group Name" value={data.consumerGroup} mono wide />
                    <ConfigValueField label="Broker Name" value={data.brokerName || '-'} mono />
                    <ConfigValueField label="Broker Address" value={data.brokerAddress} mono />
                    <ConfigValueField label="Broker ID" value={String(data.brokerId)} mono />
                    <ConfigValueField label="System Flag" value={String(data.groupSysFlag)} mono />
                </div>
            );
    }
};

interface ConfigKpiCardProps {
    label: string;
    value: string;
    note: string;
    tone: 'success' | 'warning' | 'blue' | 'violet';
    icon: typeof Activity;
}

const ConfigKpiCard = ({ label, value, note, tone, icon: Icon }: ConfigKpiCardProps) => (
    <div className={`topic-status-kpi consumer-config-kpi is-${tone}`}>
        <div>
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{note}</small>
        </div>
        <Icon className="topic-icon" aria-hidden="true" />
    </div>
);

interface ConfigValueFieldProps {
    label: string;
    value: string;
    mono?: boolean;
    wide?: boolean;
}

const ConfigValueField = ({ label, value, mono = false, wide = false }: ConfigValueFieldProps) => (
    <div className={`consumer-config-field ${wide ? 'is-wide' : ''}`}>
        <span>{label}</span>
        <strong className={mono ? 'is-mono' : ''}>{value}</strong>
    </div>
);

const ConfigToggleField = ({
    label,
    enabled,
}: {
    label: string;
    enabled: boolean;
}) => (
    <div className={`consumer-config-toggle ${enabled ? 'is-enabled' : 'is-disabled'}`}>
        <div>
            <span>{label}</span>
            <strong>{enabled ? 'Enabled' : 'Disabled'}</strong>
        </div>
        <i aria-hidden="true" />
    </div>
);
