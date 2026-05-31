import React, {useEffect, useMemo, useState, ReactNode} from 'react';
import {motion, AnimatePresence} from 'motion/react';
import {
    Activity,
    Network,
    Users,
    Settings,
    Send,
    RotateCcw,
    FastForward,
    Trash2,
    FileBox,
    Clock,
    Layers,
    ArrowRightLeft,
    HelpCircle,
    AlertTriangle,
    Shield,
    Filter,
    Search,
    Plus,
    RefreshCw,
    MoreHorizontal,
    Database,
    X,
    Server,
    FileText,
    ChevronDown,
    Check,
    Save,
    ArrowDownCircle,
    ArrowUpCircle,
    Tag,
    Key
} from 'lucide-react';
import {toast} from 'sonner@2.0.3';
import {Button} from '../components/ui/LegacyButton';
import {Pagination} from './Pagination';
import {useTopicCatalog} from '../features/topic/hooks/useTopicCatalog';
import {TopicService} from '../services/topic.service';
import type {
    TopicCategory,
    TopicConfigView,
    TopicConsumerInfoView,
    TopicEditorSeed,
    TopicListItem,
    TopicRouteView,
    TopicSendMessageResult,
    TopicStatusView,
    TopicTargetOption,
} from '../features/topic/types/topic.types';
import {TOPIC_MESSAGE_TYPE_OPTIONS} from '../features/topic/types/topic.types';

interface Topic {
    name: string;
    type: string;
    clusters: string[];
    brokers: string[];
    readQueueCount: number;
    writeQueueCount: number;
    perm: number;
    order: boolean;
    systemTopic: boolean;
    messageType: string;
    operations: string[];
}

const TOPIC_PAGE_SIZE = 10;

const TOPIC_FILTER_ORDER: TopicCategory[] = [
    'NORMAL',
    'DELAY',
    'FIFO',
    'TRANSACTION',
    'UNSPECIFIED',
    'RETRY',
    'DLQ',
    'SYSTEM',
];

const buildDefaultTopicFilters = (): Record<TopicCategory, boolean> => ({
    NORMAL: true,
    DELAY: false,
    FIFO: true,
    TRANSACTION: true,
    UNSPECIFIED: true,
    RETRY: true,
    DLQ: true,
    SYSTEM: true,
});

const buildTopicOperations = (item: TopicListItem): string[] => {
    const baseOperations = ['Status', 'Router', 'Consumer Manage', 'Topic Config'];

    if (item.systemTopic) {
        return baseOperations;
    }

    if (item.category === 'RETRY' || item.category === 'DLQ') {
        return [...baseOperations, 'Delete'];
    }

    return [
        ...baseOperations,
        'Send Message',
        'Reset Consumer Offset',
        'Skip Message Accumulate',
        'Delete',
    ];
};

const mapTopicListItem = (item: TopicListItem): Topic => ({
    name: item.topic,
    type: item.category,
    clusters: item.clusters,
    brokers: item.brokers,
    readQueueCount: item.readQueueCount,
    writeQueueCount: item.writeQueueCount,
    perm: item.perm,
    order: item.order,
    systemTopic: item.systemTopic,
    messageType: item.messageType,
    operations: buildTopicOperations(item),
});

const toDateTimeLocalValue = (date: Date): string => {
    const adjusted = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
    return adjusted.toISOString().slice(0, 16);
};

const buildTopicEditorSeedFromConfig = (config: TopicConfigView): TopicEditorSeed => ({
    topicName: config.topicName,
    clusterNameList: config.clusterNameList,
    brokerNameList: config.brokerNameList,
    writeQueueNums: config.writeQueueNums,
    readQueueNums: config.readQueueNums,
    perm: config.perm,
    order: config.order,
    messageType: config.messageType || 'UNSPECIFIED',
});

const getErrorMessage = (error: unknown, fallback: string): string => {
    if (error instanceof Error) {
        return error.message;
    }

    if (typeof error === 'string') {
        return error;
    }

    if (error && typeof error === 'object' && 'message' in error && typeof error.message === 'string') {
        return error.message;
    }

    return fallback;
};

interface TopicRouterModalProps {
    isOpen: boolean;
    onClose: () => void;
    topic: Topic | null;
}

const TopicRouterModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [routeData, setRouteData] = useState<TopicRouteView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [selectedBrokerName, setSelectedBrokerName] = useState<string | null>(null);

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadRoute = async () => {
            setIsLoading(true);
            setError('');
            setSelectedBrokerName(null);

            try {
                const result = await TopicService.getTopicRoute({topic: topic.name});
                if (!cancelled) {
                    setRouteData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setRouteData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic route.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadRoute();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const brokers = routeData?.brokers ?? [];
    const queues = routeData?.queues ?? [];
    const selectedBroker = selectedBrokerName
        ? brokers.find((broker) => broker.brokerName === selectedBrokerName) ?? brokers[0]
        : brokers[0];
    const selectedBrokerQueues = selectedBroker
        ? queues.filter((queue) => queue.brokerName === selectedBroker.brokerName)
        : queues;
    const totalAddressCount = brokers.reduce((sum, broker) => sum + broker.addresses.length, 0);
    const totalReadQueues = queues.reduce((sum, queue) => sum + queue.readQueueNums, 0);
    const totalWriteQueues = queues.reduce((sum, queue) => sum + queue.writeQueueNums, 0);
    const permissionValues = [...new Set(queues.map((queue) => queue.perm))];
    const clusterNames = [...new Set(brokers.map((broker) => broker.clusterName))];
    const maxQueueCount = Math.max(...queues.flatMap((queue) => [queue.readQueueNums, queue.writeQueueNums]), 1);
    const formatRoutePermission = (perm: number) => {
        const canWrite = (perm & 0x2) === 0x2;
        const canRead = (perm & 0x4) === 0x4;

        if (canRead && canWrite) {
            return `Perm ${perm} · RW`;
        }

        if (canRead) {
            return `Perm ${perm} · R`;
        }

        if (canWrite) {
            return `Perm ${perm} · W`;
        }

        return `Perm ${perm}`;
    };
    const copyRouteSnapshot = async () => {
        const snapshot = JSON.stringify(
            {
                topic: routeData?.topic ?? topic?.name,
                brokers,
                queues,
            },
            null,
            2,
        );

        try {
            await navigator.clipboard.writeText(snapshot);
            toast.success('Topic route snapshot copied');
        } catch {
            toast.error('Failed to copy topic route snapshot');
        }
    };

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root topic-router-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog topic-router-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-router-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Network className="topic-icon"/>
                            </div>
                            <div>
                                <span>Topic route topology</span>
                                <h3 id="topic-router-title">Topic Router</h3>
                                <p>
                                    Route snapshot for <strong>{routeData?.topic ?? topic?.name ?? 'Unknown topic'}</strong>
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <button type="button" className="topic-status-secondary-button" onClick={() => void copyRouteSnapshot()} disabled={!routeData}>
                                Copy Route
                            </button>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close topic router">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body">
                        <section className="topic-status-kpi-grid" aria-label="Topic route summary">
                            <article className="topic-status-kpi is-info">
                                <div>
                                    <span>Brokers</span>
                                    <strong>{brokers.length}</strong>
                                    <small>{totalAddressCount} address routes</small>
                                </div>
                                <Server className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-success">
                                <div>
                                    <span>Queues</span>
                                    <strong>{totalReadQueues + totalWriteQueues}</strong>
                                    <small>{totalReadQueues} read / {totalWriteQueues} write</small>
                                </div>
                                <Database className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-blue">
                                <div>
                                    <span>Permission</span>
                                    <strong>{permissionValues.length ? permissionValues.join(', ') : '-'}</strong>
                                    <small>unique route permissions</small>
                                </div>
                                <Shield className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-violet">
                                <div>
                                    <span>Clusters</span>
                                    <strong>{clusterNames.length}</strong>
                                    <small>{clusterNames[0] ?? 'No cluster route'}</small>
                                </div>
                                <Network className="topic-icon" aria-hidden="true"/>
                            </article>
                        </section>

                        {isLoading && (
                            <div className="topic-status-state">
                                <Network className="topic-icon" aria-hidden="true"/>
                                <strong>Loading topic route</strong>
                                <span>Fetching broker and queue route data from the current NameServer.</span>
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="topic-status-state is-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Failed to load route data</strong>
                                <span>{error}</span>
                            </div>
                        )}

                        {!isLoading && !error && brokers.length === 0 && queues.length === 0 && (
                            <div className="topic-status-state">
                                <Database className="topic-icon" aria-hidden="true"/>
                                <strong>No route data returned</strong>
                                <span>The NameServer did not return broker or queue route rows for this topic.</span>
                            </div>
                        )}

                        {!isLoading && !error && (brokers.length > 0 || queues.length > 0) && (
                            <section className="topic-router-workspace">
                                <div className="topic-router-panel topic-router-broker-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Broker Inventory</h4>
                                            <p>NameServer broker route and address identities.</p>
                                        </div>
                                        <span>{brokers.length} brokers</span>
                                    </div>

                                    <div className="topic-router-broker-list">
                                        {brokers.map((broker) => {
                                            const isSelected = selectedBroker?.brokerName === broker.brokerName;

                                            return (
                                                <button
                                                    type="button"
                                                    key={`${broker.clusterName}-${broker.brokerName}`}
                                                    className={`topic-router-broker-card ${isSelected ? 'is-selected' : ''}`}
                                                    onClick={() => setSelectedBrokerName(broker.brokerName)}
                                                >
                                                    <span>
                                                        <strong>{broker.brokerName}</strong>
                                                        <small>{broker.clusterName}</small>
                                                    </span>
                                                    <b>{broker.addresses.length} addr</b>
                                                </button>
                                            );
                                        })}
                                    </div>

                                    {selectedBroker && (
                                        <div className="topic-router-address-list">
                                            <span>Address identities</span>
                                            {selectedBroker.addresses.map((address) => (
                                                <div key={`${selectedBroker.brokerName}-${address.brokerId}-${address.address}`}>
                                                    <strong>ID {address.brokerId}</strong>
                                                    <code>{address.address}</code>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>

                                <div className="topic-router-panel topic-router-map-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Route Map</h4>
                                            <p>Cluster to broker queue distribution.</p>
                                        </div>
                                    </div>

                                    {selectedBroker ? (
                                        <div className="topic-router-map">
                                            <div className="topic-router-map-card">
                                                <span>Cluster</span>
                                                <strong>{selectedBroker.clusterName}</strong>
                                                <small>{selectedBrokerQueues.length} queue routes</small>
                                            </div>
                                            <div className="topic-router-link" aria-hidden="true"/>
                                            <div className="topic-router-map-card is-broker">
                                                <span>Broker</span>
                                                <strong>{selectedBroker.brokerName}</strong>
                                                <small>{selectedBroker.addresses[0]?.address ?? 'No address'}</small>
                                            </div>
                                        </div>
                                    ) : (
                                        <div className="topic-status-state is-compact">
                                            <Network className="topic-icon" aria-hidden="true"/>
                                            <strong>No broker selected</strong>
                                            <span>Select a broker to inspect route topology.</span>
                                        </div>
                                    )}
                                </div>

                                <div className="topic-router-panel topic-router-queue-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Queue Routes</h4>
                                            <p>Read/write allocation by broker.</p>
                                        </div>
                                        <span>{queues.length} routes</span>
                                    </div>

                                    {queues.length === 0 ? (
                                        <div className="topic-status-state is-compact">
                                            <Database className="topic-icon" aria-hidden="true"/>
                                            <strong>No queue routes</strong>
                                            <span>No queue route data was returned for this topic.</span>
                                        </div>
                                    ) : (
                                        <>
                                            <div className="topic-router-table-head" aria-hidden="true">
                                                <span>Broker</span>
                                                <span>Read</span>
                                                <span>Write</span>
                                                <span>Perm</span>
                                            </div>
                                            <div className="topic-router-queue-list">
                                                {queues.map((queue, index) => {
                                                    const selected = selectedBroker?.brokerName === queue.brokerName;
                                                    const readWidth = `${Math.max(6, Math.round((queue.readQueueNums / maxQueueCount) * 100))}%`;
                                                    const writeWidth = `${Math.max(6, Math.round((queue.writeQueueNums / maxQueueCount) * 100))}%`;

                                                    return (
                                                        <button
                                                            type="button"
                                                            key={`${queue.brokerName}-${index}`}
                                                            className={`topic-router-queue-row ${selected ? 'is-selected' : ''}`}
                                                            onClick={() => setSelectedBrokerName(queue.brokerName)}
                                                        >
                                                            <span className="topic-router-queue-broker">
                                                                <strong>{queue.brokerName}</strong>
                                                                <small>{brokers.find((broker) => broker.brokerName === queue.brokerName)?.clusterName ?? 'Unknown cluster'}</small>
                                                            </span>
                                                            <span className="topic-router-queue-number">{queue.readQueueNums}</span>
                                                            <span className="topic-router-queue-number">{queue.writeQueueNums}</span>
                                                            <span className="topic-router-permission">{formatRoutePermission(queue.perm)}</span>
                                                            <span className="topic-router-bars" aria-hidden="true">
                                                                <i><b style={{width: readWidth}}/></i>
                                                                <i><b style={{width: writeWidth}}/></i>
                                                            </span>
                                                        </button>
                                                    );
                                                })}
                                            </div>
                                        </>
                                    )}
                                </div>
                            </section>
                        )}
                    </div>

                    <footer className="topic-status-footer">
                        <span>Route data is read-only and reflects the current NameServer response.</span>
                        <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                            Close
                        </button>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TOPIC_CONFIG_FIELD_LABELS: Record<string, string> = {
    readQueueNums: 'Read Queues',
    writeQueueNums: 'Write Queues',
    perm: 'Permission',
    order: 'Ordered Delivery',
    messageType: 'Message Type',
};

const formatAttributeLabel = (key: string) =>
    key
        .split(/[._-]/g)
        .filter(Boolean)
        .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
        .join(' ');

interface TopicConfigModalProps extends TopicRouterModalProps {
    onEdit: (seed: TopicEditorSeed) => void;
    onRefresh: () => Promise<void>;
}

const TopicConfigModal = ({isOpen, onClose, topic, onEdit, onRefresh}: TopicConfigModalProps) => {
    const [configData, setConfigData] = useState<TopicConfigView | null>(null);
    const [selectedBroker, setSelectedBroker] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [actionError, setActionError] = useState('');
    const [isDeletingBroker, setIsDeletingBroker] = useState(false);

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadConfig = async () => {
            setIsLoading(true);
            setError('');

            try {
                const result = await TopicService.getTopicConfig({
                    topic: topic.name,
                    brokerName: selectedBroker,
                });
                if (!cancelled) {
                    setConfigData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setConfigData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic configuration.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadConfig();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name, selectedBroker]);

    useEffect(() => {
        if (!isOpen) {
            return;
        }
        setSelectedBroker(null);
        setConfigData(null);
        setError('');
        setActionError('');
        setIsDeletingBroker(false);
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const brokerOptions = configData?.brokerNameList ?? [];
    const clusterOptions = configData?.clusterNameList ?? [];
    const attributes = Object.entries(configData?.attributes ?? {}).sort(([left], [right]) => left.localeCompare(right));
    const inconsistentFields = configData?.inconsistentFields ?? [];
    const activeBroker = selectedBroker ?? configData?.brokerName ?? '';
    const queueBalanceTotal = (configData?.writeQueueNums ?? 0) + (configData?.readQueueNums ?? 0);
    const writeQueuePercent = queueBalanceTotal > 0
        ? `${Math.max(6, Math.round(((configData?.writeQueueNums ?? 0) / queueBalanceTotal) * 100))}%`
        : '0%';
    const readQueuePercent = queueBalanceTotal > 0
        ? `${Math.max(6, Math.round(((configData?.readQueueNums ?? 0) / queueBalanceTotal) * 100))}%`
        : '0%';
    const configPermissionLabel = configData ? formatTopicPermission(configData.perm) : '-';
    const consistencyLabel = inconsistentFields.length > 0 ? `${inconsistentFields.length} drift fields` : 'Aligned';

    const copyConfigSnapshot = async () => {
        if (!configData) {
            return;
        }

        const snapshot = JSON.stringify(
            {
                topicName: configData.topicName,
                brokerName: activeBroker,
                clusterName: configData.clusterName,
                writeQueueNums: configData.writeQueueNums,
                readQueueNums: configData.readQueueNums,
                perm: configData.perm,
                order: configData.order,
                messageType: configData.messageType,
                attributes: configData.attributes,
                inconsistentFields: configData.inconsistentFields,
            },
            null,
            2,
        );

        try {
            await navigator.clipboard.writeText(snapshot);
            toast.success('Topic config snapshot copied');
        } catch {
            toast.error('Failed to copy topic config snapshot');
        }
    };

    const handleDeleteByBroker = async () => {
        if (!configData || !activeBroker) {
            return;
        }

        const confirmed = window.confirm(
            `Delete topic "${configData.topicName}" from broker "${activeBroker}" only? This will not remove the NameServer topic mapping.`,
        );
        if (!confirmed) {
            return;
        }

        setIsDeletingBroker(true);
        setActionError('');

        try {
            const result = await TopicService.deleteTopicByBroker({
                brokerName: activeBroker,
                topic: configData.topicName,
            });
            toast.success(result.message || `Deleted ${configData.topicName} from ${activeBroker}`);
            await onRefresh();
            onClose();
        } catch (deleteError) {
            setActionError(getErrorMessage(deleteError, 'Failed to delete topic from the selected broker.'));
        } finally {
            setIsDeletingBroker(false);
        }
    };

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root topic-config-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog topic-config-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-config-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Settings className="topic-icon"/>
                            </div>
                            <div>
                                <span>Broker-side topic config</span>
                                <h3 id="topic-config-title">Topic Configuration</h3>
                                <p>
                                    Config snapshot for <strong>{configData?.topicName ?? topic?.name ?? 'Unknown topic'}</strong>
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <button type="button" className="topic-status-secondary-button" onClick={() => void copyConfigSnapshot()} disabled={!configData}>
                                Copy Config
                            </button>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close topic configuration">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body">
                        <section className="topic-status-kpi-grid" aria-label="Topic configuration summary">
                            <article className="topic-status-kpi is-info">
                                <div>
                                    <span>Broker</span>
                                    <strong>{activeBroker || '-'}</strong>
                                    <small>selected anchor broker</small>
                                </div>
                                <Server className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-success">
                                <div>
                                    <span>Queues</span>
                                    <strong>{configData ? `${configData.writeQueueNums}/${configData.readQueueNums}` : '-'}</strong>
                                    <small>write / read queue count</small>
                                </div>
                                <Database className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-blue">
                                <div>
                                    <span>Permission</span>
                                    <strong>{configPermissionLabel}</strong>
                                    <small>current broker permission</small>
                                </div>
                                <Shield className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className={`topic-status-kpi ${inconsistentFields.length > 0 ? 'is-warning' : 'is-violet'}`}>
                                <div>
                                    <span>Consistency</span>
                                    <strong>{consistencyLabel}</strong>
                                    <small>{attributes.length} extra attributes</small>
                                </div>
                                <Key className="topic-icon" aria-hidden="true"/>
                            </article>
                        </section>

                        {isLoading && (
                            <div className="topic-status-state">
                                <Settings className="topic-icon" aria-hidden="true"/>
                                <strong>Loading topic configuration</strong>
                                <span>Fetching broker-side config from the current NameServer view.</span>
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="topic-status-state is-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Failed to load topic configuration</strong>
                                <span>{error}</span>
                            </div>
                        )}

                        {!isLoading && !error && configData && (
                            <section className="topic-config-workspace">
                                <div className="topic-config-panel topic-config-target-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Deployment Target</h4>
                                            <p>Switch broker context before editing or deleting.</p>
                                        </div>
                                    </div>

                                    <div className="topic-config-section">
                                        <span>Cluster Names</span>
                                        <div className="topic-config-chip-list">
                                            {(clusterOptions.length ? clusterOptions : [configData.clusterName].filter(Boolean) as string[]).map((clusterName) => (
                                                <span key={clusterName} className="topic-config-chip is-active">
                                                    {clusterName}
                                                </span>
                                            ))}
                                        </div>
                                    </div>

                                    <label className="topic-config-select-field">
                                        <span>Anchor Broker</span>
                                        <select value={activeBroker} onChange={(event) => setSelectedBroker(event.target.value)}>
                                            {brokerOptions.length > 0 ? (
                                                brokerOptions.map((brokerName) => (
                                                    <option key={brokerName} value={brokerName}>
                                                        {brokerName}
                                                    </option>
                                                ))
                                            ) : (
                                                <option value={activeBroker}>{activeBroker || 'No broker returned'}</option>
                                            )}
                                        </select>
                                        <ChevronDown className="topic-icon" aria-hidden="true"/>
                                    </label>

                                    <div className="topic-config-broker-list">
                                        {brokerOptions.map((brokerName) => (
                                            <button
                                                key={brokerName}
                                                type="button"
                                                onClick={() => setSelectedBroker(brokerName)}
                                                className={brokerName === activeBroker ? 'is-selected' : ''}
                                            >
                                                {brokerName}
                                            </button>
                                        ))}
                                    </div>

                                    <div className="topic-config-danger-note">
                                        <Trash2 className="topic-icon" aria-hidden="true"/>
                                        <div>
                                            <strong>Broker deletion is scoped</strong>
                                            <span>Delete removes this topic from the selected broker only.</span>
                                        </div>
                                    </div>
                                </div>

                                <div className="topic-config-panel topic-config-definition-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Definition & Queue Plan</h4>
                                            <p>Read-only broker config snapshot for the selected target.</p>
                                        </div>
                                    </div>

                                    <div className="topic-config-field is-wide">
                                        <span>Topic Name</span>
                                        <strong>{configData.topicName}</strong>
                                    </div>

                                    <div className="topic-config-field-grid">
                                        <div className="topic-config-field">
                                            <span>Message Type</span>
                                            <strong>{configData.messageType}</strong>
                                        </div>
                                        <div className="topic-config-field">
                                            <span>Ordered Delivery</span>
                                            <strong>{configData.order ? 'Enabled' : 'Disabled'}</strong>
                                        </div>
                                    </div>

                                    <div className="topic-config-queue-grid">
                                        <div>
                                            <span>Write Queues</span>
                                            <strong>{configData.writeQueueNums}</strong>
                                        </div>
                                        <div>
                                            <span>Read Queues</span>
                                            <strong>{configData.readQueueNums}</strong>
                                        </div>
                                        <div>
                                            <span>Permission</span>
                                            <strong>{configPermissionLabel}</strong>
                                        </div>
                                    </div>

                                    <div className="topic-config-balance">
                                        <div>
                                            <span>Queue balance</span>
                                            <strong>
                                                {configData.writeQueueNums === configData.readQueueNums
                                                    ? 'Write and read queues are aligned'
                                                    : 'Write and read queue counts differ'}
                                            </strong>
                                        </div>
                                        <div className="topic-config-balance-bars" aria-hidden="true">
                                            <i><b style={{width: writeQueuePercent}}/></i>
                                            <i><b style={{width: readQueuePercent}}/></i>
                                        </div>
                                    </div>
                                </div>

                                <aside className="topic-config-panel topic-config-attributes-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Attributes</h4>
                                            <p>Extra attributes and broker consistency.</p>
                                        </div>
                                    </div>

                                    <div className={`topic-config-consistency ${inconsistentFields.length > 0 ? 'is-warning' : 'is-aligned'}`}>
                                        <span>Consistency</span>
                                        <strong>{inconsistentFields.length > 0 ? 'Drift detected' : 'Aligned'}</strong>
                                        <small>
                                            {inconsistentFields.length > 0
                                                ? `${inconsistentFields.length} fields differ across brokers`
                                                : 'No broker drift detected'}
                                        </small>
                                    </div>

                                    {inconsistentFields.length > 0 && (
                                        <div className="topic-config-drift-list">
                                            {inconsistentFields.map((field) => (
                                                <span key={field}>{TOPIC_CONFIG_FIELD_LABELS[field] ?? field}</span>
                                            ))}
                                        </div>
                                    )}

                                    {attributes.length === 0 ? (
                                        <div className="topic-config-empty-attrs">
                                            <Key className="topic-icon" aria-hidden="true"/>
                                            <strong>No extra attributes</strong>
                                            <span>This broker returned only core topic configuration fields.</span>
                                        </div>
                                    ) : (
                                        <div className="topic-config-attr-list">
                                            {attributes.map(([key, value]) => (
                                                <div key={key}>
                                                    <span>{formatAttributeLabel(key)}</span>
                                                    <strong>{value}</strong>
                                                    <small>{key}</small>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </aside>
                            </section>
                        )}

                        {actionError && (
                            <div className="topic-status-state is-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Topic action failed</strong>
                                <span>{actionError}</span>
                            </div>
                        )}
                    </div>

                    <footer className="topic-status-footer topic-config-footer">
                        <span>Config is read-only here. Use Edit Topic to commit queue, permission, or type changes.</span>
                        <div className="topic-config-footer-actions">
                            <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                                Close
                            </button>
                            <button
                                type="button"
                                className="topic-status-secondary-button topic-config-danger-button"
                                onClick={() => void handleDeleteByBroker()}
                                disabled={!configData || isLoading || isDeletingBroker || !activeBroker}
                            >
                                <Trash2 className="topic-icon" aria-hidden="true"/>
                                {isDeletingBroker ? 'Deleting...' : 'Delete Broker'}
                            </button>
                            <button
                                type="button"
                                className="topic-status-primary-button topic-config-edit-button"
                                onClick={() => {
                                    if (configData) {
                                        onEdit(buildTopicEditorSeedFromConfig(configData));
                                    }
                                }}
                                disabled={!configData || isLoading}
                            >
                                <Save className="topic-icon" aria-hidden="true"/>
                                Edit Topic
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicStatusModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [statusData, setStatusData] = useState<TopicStatusView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [selectedOffsetKey, setSelectedOffsetKey] = useState<string | null>(null);

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadStatus = async () => {
            setIsLoading(true);
            setError('');
            setSelectedOffsetKey(null);

            try {
                const result = await TopicService.getTopicStats({topic: topic.name});
                if (!cancelled) {
                    setStatusData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setStatusData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic status.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadStatus();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const offsets = statusData?.offsets ?? [];
    const totalMinOffset = offsets.reduce((acc, curr) => acc + curr.minOffset, 0);
    const totalMaxOffset = offsets.reduce((acc, curr) => acc + curr.maxOffset, 0);
    const queueCount = statusData?.queueCount ?? offsets.length;
    const greatestOffset = offsets.reduce((max, row) => Math.max(max, row.maxOffset, row.minOffset), 0);
    const totalOffsetRange = offsets.reduce((acc, row) => acc + Math.max(row.maxOffset - row.minOffset, 0), 0);
    const totalMessageCount = statusData?.totalMessageCount ?? totalOffsetRange;
    const activeQueueCount = offsets.filter((row) => row.lastUpdateTimestamp > 0 || row.maxOffset > row.minOffset).length;
    const latestUpdateTimestamp = offsets.reduce((latest, row) => Math.max(latest, row.lastUpdateTimestamp), 0);
    const getOffsetKey = (row: TopicStatusView['offsets'][number], index: number) =>
        `${row.brokerName}-${row.queueId}-${index}`;
    const selectedOffset = selectedOffsetKey
        ? offsets.find((row, index) => getOffsetKey(row, index) === selectedOffsetKey) ?? offsets[0]
        : offsets[0];

    const formatTimestamp = (timestamp: number) => {
        if (!timestamp || timestamp <= 0) {
            return 'Never updated';
        }
        return new Intl.DateTimeFormat('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        }).format(new Date(timestamp));
    };

    const formatTimeOnly = (timestamp: number) => {
        if (!timestamp || timestamp <= 0) {
            return '--:--:--';
        }
        return new Intl.DateTimeFormat('zh-CN', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        }).format(new Date(timestamp));
    };

    const calcBarWidth = (value: number) => {
        if (greatestOffset <= 0) {
            return '0%';
        }
        return `${Math.max(4, Math.round((value / greatestOffset) * 100))}%`;
    };

    const copySelectedSnapshot = async () => {
        if (!selectedOffset) {
            return;
        }

        const snapshot = [
            `topic=${statusData?.topic ?? topic?.name ?? ''}`,
            `broker=${selectedOffset.brokerName}`,
            `queue=${selectedOffset.queueId}`,
            `minOffset=${selectedOffset.minOffset}`,
            `maxOffset=${selectedOffset.maxOffset}`,
            `updated=${formatTimestamp(selectedOffset.lastUpdateTimestamp)}`,
        ].join('\n');

        try {
            await navigator.clipboard.writeText(snapshot);
            toast.success('Topic status snapshot copied');
        } catch {
            toast.error('Failed to copy topic status snapshot');
        }
    };

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-status-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Activity className="topic-icon"/>
                            </div>
                            <div>
                                <span>Real-time offset health</span>
                                <h3 id="topic-status-title">Topic Status</h3>
                                <p>
                                    Snapshot for <strong>{statusData?.topic ?? topic?.name ?? 'Unknown topic'}</strong>
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <button type="button" className="topic-status-secondary-button" onClick={() => void copySelectedSnapshot()} disabled={!selectedOffset}>
                                Copy Snapshot
                            </button>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close topic status">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body">
                        <section className="topic-status-kpi-grid" aria-label="Topic status summary">
                            <article className="topic-status-kpi is-info">
                                <div>
                                    <span>Total Messages</span>
                                    <strong>{totalMessageCount.toLocaleString()}</strong>
                                    <small>reported by status API</small>
                                </div>
                                <Layers className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-success">
                                <div>
                                    <span>Active Queues</span>
                                    <strong>{activeQueueCount}/{queueCount}</strong>
                                    <small>queues with recent offsets</small>
                                </div>
                                <ArrowDownCircle className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-blue">
                                <div>
                                    <span>Total Min Offset</span>
                                    <strong>{totalMinOffset.toLocaleString()}</strong>
                                    <small>sum of queue low marks</small>
                                </div>
                                <ArrowDownCircle className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-violet">
                                <div>
                                    <span>Total Max Offset</span>
                                    <strong>{totalMaxOffset.toLocaleString()}</strong>
                                    <small>sum of queue high marks</small>
                                </div>
                                <ArrowUpCircle className="topic-icon" aria-hidden="true"/>
                            </article>
                        </section>

                        {isLoading && (
                            <div className="topic-status-state">
                                <Activity className="topic-icon" aria-hidden="true"/>
                                <strong>Loading topic status</strong>
                                <span>Fetching real-time offsets from the current NameServer.</span>
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="topic-status-state is-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Failed to load topic status</strong>
                                <span>{error}</span>
                            </div>
                        )}

                        {!isLoading && !error && offsets.length === 0 && (
                            <div className="topic-status-state">
                                <Database className="topic-icon" aria-hidden="true"/>
                                <strong>No queue offsets returned</strong>
                                <span>The NameServer did not return queue status rows for this topic.</span>
                            </div>
                        )}

                        {!isLoading && !error && offsets.length > 0 && (
                            <section className="topic-status-workspace">
                                <div className="topic-status-table-panel">
                                    <div className="topic-status-panel-header">
                                        <div>
                                            <h4>Queue Offsets</h4>
                                            <p>Compact scan by broker, queue id, min/max offset, and update time.</p>
                                        </div>
                                        <span>{formatTimestamp(latestUpdateTimestamp)}</span>
                                    </div>

                                    <div className="topic-status-table-head" aria-hidden="true">
                                        <span>Queue</span>
                                        <span>Broker</span>
                                        <span>Min</span>
                                        <span>Max</span>
                                        <span>Range</span>
                                    </div>

                                    <div className="topic-status-rows">
                                        {offsets.map((row, index) => {
                                            const rowKey = getOffsetKey(row, index);
                                            const isSelected = selectedOffset
                                                ? getOffsetKey(selectedOffset, offsets.indexOf(selectedOffset)) === rowKey
                                                : index === 0;
                                            const range = Math.max(row.maxOffset - row.minOffset, 0);

                                            return (
                                                <button
                                                    type="button"
                                                    key={rowKey}
                                                    className={`topic-status-row ${isSelected ? 'is-selected' : ''}`}
                                                    onClick={() => setSelectedOffsetKey(rowKey)}
                                                >
                                                    <span className="topic-status-queue-id">{row.queueId}</span>
                                                    <span className="topic-status-broker">
                                                        <strong>{row.brokerName}</strong>
                                                        <small>{formatTimeOnly(row.lastUpdateTimestamp)}</small>
                                                    </span>
                                                    <span className="topic-status-number">{row.minOffset.toLocaleString()}</span>
                                                    <span className="topic-status-number">{row.maxOffset.toLocaleString()}</span>
                                                    <span className="topic-status-range">
                                                        <i>
                                                            <b style={{width: calcBarWidth(row.maxOffset)}}/>
                                                        </i>
                                                        <small>{range.toLocaleString()}</small>
                                                    </span>
                                                </button>
                                            );
                                        })}
                                    </div>
                                </div>

                                <aside className="topic-status-inspector" aria-label="Selected queue details">
                                    {selectedOffset && (
                                        <>
                                            <span>Selected queue</span>
                                            <h4>Queue {selectedOffset.queueId}</h4>
                                            <p>{selectedOffset.brokerName}</p>

                                            <div className="topic-status-inspector-value">
                                                <span>Offset Range</span>
                                                <strong>
                                                    {selectedOffset.minOffset.toLocaleString()} → {selectedOffset.maxOffset.toLocaleString()}
                                                </strong>
                                                <i>
                                                    <b style={{width: calcBarWidth(selectedOffset.maxOffset)}}/>
                                                </i>
                                            </div>

                                            <div className="topic-status-inspector-grid">
                                                <div>
                                                    <span>Delta</span>
                                                    <strong>{Math.max(selectedOffset.maxOffset - selectedOffset.minOffset, 0).toLocaleString()}</strong>
                                                </div>
                                                <div>
                                                    <span>Queue ID</span>
                                                    <strong>{selectedOffset.queueId}</strong>
                                                </div>
                                            </div>

                                            <div className="topic-status-update-card">
                                                <Clock className="topic-icon" aria-hidden="true"/>
                                                <div>
                                                    <span>Last Updated</span>
                                                    <strong>{formatTimestamp(selectedOffset.lastUpdateTimestamp)}</strong>
                                                </div>
                                            </div>

                                            <button type="button" className="topic-status-primary-button" onClick={() => void copySelectedSnapshot()}>
                                                Copy Snapshot
                                            </button>
                                        </>
                                    )}
                                </aside>
                            </section>
                        )}
                    </div>

                    <footer className="topic-status-footer">
                        <span>Offsets are read-only snapshots from the current NameServer session.</span>
                        <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                            Close
                        </button>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicConsumerManageModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [consumerItems, setConsumerItems] = useState<TopicConsumerInfoView[]>([]);
    const [selectedConsumerGroup, setSelectedConsumerGroup] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        setConsumerItems([]);
        setSelectedConsumerGroup(null);
        setIsLoading(true);
        setError('');

        const loadConsumers = async () => {
            try {
                const response = await TopicService.getTopicConsumers({topic: topic.name});
                if (!cancelled) {
                    const items = response.items ?? [];
                    setConsumerItems(items);
                    setSelectedConsumerGroup(items[0]?.consumerGroup ?? null);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load consumer details.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadConsumers();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const totalLag = consumerItems.reduce((sum, item) => sum + item.totalDiff, 0);
    const totalInflight = consumerItems.reduce((sum, item) => sum + item.inflightDiff, 0);
    const totalConsumeTps = consumerItems.reduce((sum, item) => sum + item.consumeTps, 0);
    const selectedConsumer = consumerItems.find((item) => item.consumerGroup === selectedConsumerGroup) ?? consumerItems[0] ?? null;
    const maxLag = Math.max(0, ...consumerItems.map((item) => item.totalDiff));
    const selectedLagPercent = selectedConsumer && maxLag > 0
        ? `${Math.max(5, Math.round((selectedConsumer.totalDiff / maxLag) * 100))}%`
        : '0%';
    const laggingGroupCount = consumerItems.filter((item) => item.totalDiff > 0).length;
    const healthLabel = totalLag > 0 ? 'Lagging' : 'Caught up';

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root topic-consumer-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog topic-consumer-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-consumer-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Users className="topic-icon"/>
                            </div>
                            <div>
                                <span>Consumer runtime snapshot</span>
                                <h3 id="topic-consumer-title">Consumer Manage</h3>
                                <p>
                                    Monitor consumer-group lag and throughput for <strong>{topic?.name}</strong>
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <span className={`topic-consumer-health-chip ${totalLag > 0 ? 'is-warning' : 'is-healthy'}`}>
                                {isLoading ? 'Loading' : healthLabel}
                            </span>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close consumer manage">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body topic-consumer-body">
                        {isLoading && (
                            <div className="topic-status-state">
                                <Users className="topic-icon" aria-hidden="true"/>
                                <strong>Loading consumer runtime data</strong>
                                <span>Fetching the current NameServer snapshot.</span>
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="topic-status-state is-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Failed to load consumer runtime data</strong>
                                <span>{error}</span>
                            </div>
                        )}

                        {!isLoading && !error && consumerItems.length > 0 && (
                            <>
                                <section className="topic-status-kpi-grid topic-consumer-kpi-grid" aria-label="Consumer runtime summary">
                                    <article className="topic-status-kpi is-info">
                                        <div>
                                            <span>Consumer Groups</span>
                                            <strong>{consumerItems.length}</strong>
                                            <small>{laggingGroupCount} groups with lag</small>
                                        </div>
                                        <Users className="topic-icon" aria-hidden="true"/>
                                    </article>
                                    <article className={`topic-status-kpi ${totalLag > 0 ? 'is-warning' : 'is-success'}`}>
                                        <div>
                                            <span>Total Lag</span>
                                            <strong>{totalLag.toLocaleString()}</strong>
                                            <small>messages behind</small>
                                        </div>
                                        <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                    </article>
                                    <article className="topic-status-kpi is-violet">
                                        <div>
                                            <span>Inflight</span>
                                            <strong>{totalInflight.toLocaleString()}</strong>
                                            <small>currently processing</small>
                                        </div>
                                        <Clock className="topic-icon" aria-hidden="true"/>
                                    </article>
                                    <article className="topic-status-kpi is-success">
                                        <div>
                                            <span>Consume TPS</span>
                                            <strong>{totalConsumeTps.toLocaleString(undefined, {maximumFractionDigits: 2})}</strong>
                                            <small>messages / sec</small>
                                        </div>
                                        <Activity className="topic-icon" aria-hidden="true"/>
                                    </article>
                                </section>

                                <section className="topic-consumer-workspace">
                                    <div className="topic-consumer-panel topic-consumer-table-panel">
                                        <div className="topic-status-panel-header">
                                            <div>
                                                <h4>Current Runtime Snapshot</h4>
                                                <p>Group-level lag and TPS are available now; queue-level offset detail can attach later.</p>
                                            </div>
                                            <span>{consumerItems.length} active</span>
                                        </div>

                                        <div className="topic-consumer-table-head">
                                            <span>Consumer Group</span>
                                            <span>Total Lag</span>
                                            <span>Inflight</span>
                                            <span>Consume TPS</span>
                                        </div>

                                        <div className="topic-consumer-rows">
                                            {consumerItems.map((item) => {
                                                const isSelected = selectedConsumer?.consumerGroup === item.consumerGroup;
                                                const lagPercent = maxLag > 0
                                                    ? `${Math.max(5, Math.round((item.totalDiff / maxLag) * 100))}%`
                                                    : '0%';

                                                return (
                                                    <button
                                                        key={item.consumerGroup}
                                                        type="button"
                                                        className={`topic-consumer-row ${isSelected ? 'is-selected' : ''}`}
                                                        onClick={() => setSelectedConsumerGroup(item.consumerGroup)}
                                                    >
                                                        <span className="topic-consumer-group">
                                                            <span className="topic-consumer-avatar">
                                                                <Users className="topic-icon" aria-hidden="true"/>
                                                            </span>
                                                            <span>
                                                                <small>Consumer Group</small>
                                                                <strong>{item.consumerGroup}</strong>
                                                            </span>
                                                        </span>
                                                        <span className={`topic-consumer-lag ${item.totalDiff > 0 ? 'is-warning' : 'is-healthy'}`}>
                                                            <b>{item.totalDiff.toLocaleString()}</b>
                                                            <i><em style={{width: lagPercent}}/></i>
                                                        </span>
                                                        <span className="topic-consumer-number">{item.inflightDiff.toLocaleString()}</span>
                                                        <span className="topic-consumer-number">
                                                            {item.consumeTps.toLocaleString(undefined, {maximumFractionDigits: 2})}
                                                        </span>
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    </div>

                                    <aside className="topic-consumer-panel topic-consumer-inspector">
                                        <div className="topic-status-panel-header">
                                            <div>
                                                <h4>Selected Group</h4>
                                                <p>Backlog pressure for the active selection.</p>
                                            </div>
                                        </div>

                                        {selectedConsumer && (
                                            <>
                                                <div className="topic-consumer-selected-name">
                                                    <span>{selectedConsumer.totalDiff > 0 ? 'Lagging' : 'Caught up'}</span>
                                                    <strong>{selectedConsumer.consumerGroup}</strong>
                                                </div>

                                                <div className={`topic-consumer-pressure-card ${selectedConsumer.totalDiff > 0 ? 'is-warning' : 'is-healthy'}`}>
                                                    <span>Backlog Pressure</span>
                                                    <strong>{selectedConsumer.totalDiff.toLocaleString()} messages behind</strong>
                                                    <i><em style={{width: selectedLagPercent}}/></i>
                                                    <small>
                                                        {selectedConsumer.totalDiff > 0
                                                            ? 'Prioritize this group before offsets drift further.'
                                                            : 'No backlog pressure reported for this group.'}
                                                    </small>
                                                </div>

                                                <div className="topic-consumer-mini-grid">
                                                    <div>
                                                        <span>Inflight</span>
                                                        <strong>{selectedConsumer.inflightDiff.toLocaleString()}</strong>
                                                    </div>
                                                    <div>
                                                        <span>TPS</span>
                                                        <strong>{selectedConsumer.consumeTps.toLocaleString(undefined, {maximumFractionDigits: 2})}</strong>
                                                    </div>
                                                </div>
                                            </>
                                        )}

                                        <div className="topic-consumer-note">
                                            <Database className="topic-icon" aria-hidden="true"/>
                                            <span>Queue-level offset details can be added here when the backend view model becomes available.</span>
                                        </div>
                                    </aside>
                                </section>
                            </>
                        )}

                        {!isLoading && !error && consumerItems.length === 0 && (
                            <div className="topic-status-state is-compact">
                                <Users className="topic-icon" aria-hidden="true"/>
                                <strong>No active consumer snapshot</strong>
                                <span>No consumer runtime snapshot was returned for this topic.</span>
                            </div>
                        )}
                    </div>

                    <footer className="topic-status-footer topic-consumer-footer">
                        <span>Runtime data is read-only here. Use reset or skip actions from the topic card when needed.</span>
                        <div>
                            <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                                Close
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicSendMessageModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [tag, setTag] = useState('');
    const [messageKey, setMessageKey] = useState('');
    const [messageBody, setMessageBody] = useState('');
    const [traceEnabled, setTraceEnabled] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [isLoadingConfig, setIsLoadingConfig] = useState(false);
    const [error, setError] = useState('');
    const [sendResult, setSendResult] = useState<TopicSendMessageResult | null>(null);
    const [topicMessageType, setTopicMessageType] = useState('UNSPECIFIED');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        setTag('');
        setMessageKey('');
        setMessageBody('');
        setTraceEnabled(false);
        setIsSubmitting(false);
        setError('');
        setSendResult(null);
        setIsLoadingConfig(true);
        setTopicMessageType('UNSPECIFIED');

        let cancelled = false;

        const loadTopicConfig = async () => {
            try {
                const result = await TopicService.getTopicConfig({
                    topic: topic.name,
                    brokerName: null,
                });
                if (!cancelled) {
                    setTopicMessageType(result.messageType || 'UNSPECIFIED');
                }
            } catch (loadError) {
                if (!cancelled) {
                    setError(getErrorMessage(loadError, 'Failed to load topic configuration before sending.'));
                }
            } finally {
                if (!cancelled) {
                    setIsLoadingConfig(false);
                }
            }
        };

        void loadTopicConfig();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    const handleSubmit = async () => {
        if (!topic?.name) {
            return;
        }

        if (!messageBody.trim()) {
            setError('Message body is required before sending.');
            return;
        }

        setIsSubmitting(true);
        setError('');
        setSendResult(null);

        try {
            const result = await TopicService.sendTopicMessage({
                topic: topic.name,
                key: messageKey,
                tag,
                messageBody,
                traceEnabled,
            });
            setSendResult(result);
            toast.success(`Message sent to ${topic.name}`);
        } catch (submitError) {
            setError(submitError instanceof Error ? submitError.message : 'Failed to send topic message.');
        } finally {
            setIsSubmitting(false);
        }
    };

    const producerPathText = topicMessageType === 'TRANSACTION'
        ? 'Transaction producer path. Local transaction is committed immediately for test send.'
        : `Standard producer path for ${topicMessageType}.`;
    const canSubmit = Boolean(messageBody.trim()) && !isSubmitting && !isLoadingConfig;

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root topic-send-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog topic-send-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-send-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Send className="topic-icon"/>
                            </div>
                            <div>
                                <span>Producer test send</span>
                                <h3 id="topic-send-title">Send Message</h3>
                                <p>
                                    Compose and commit a test message to <strong>{topic?.name}</strong>
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <span className={`topic-send-type-chip ${topicMessageType === 'TRANSACTION' ? 'is-transaction' : ''}`}>
                                {isLoadingConfig ? 'Loading type' : topicMessageType}
                            </span>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close send message">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body topic-send-body">
                        <section className="topic-status-kpi-grid topic-send-kpi-grid" aria-label="Send message summary">
                            <article className="topic-status-kpi is-info">
                                <div>
                                    <span>Target</span>
                                    <strong>{topic?.name ?? '-'}</strong>
                                    <small>topic endpoint</small>
                                </div>
                                <FileText className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-blue">
                                <div>
                                    <span>Type</span>
                                    <strong>{isLoadingConfig ? '...' : topicMessageType}</strong>
                                    <small>producer route</small>
                                </div>
                                <Layers className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className={`topic-status-kpi ${traceEnabled ? 'is-success' : 'is-violet'}`}>
                                <div>
                                    <span>Trace</span>
                                    <strong>{traceEnabled ? 'On' : 'Off'}</strong>
                                    <small>message trace toggle</small>
                                </div>
                                <Activity className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className={`topic-status-kpi ${messageBody.trim() ? 'is-success' : 'is-warning'}`}>
                                <div>
                                    <span>Payload</span>
                                    <strong>{messageBody.trim() ? 'Ready' : 'Required'}</strong>
                                    <small>relaxed JSON accepted</small>
                                </div>
                                <Key className="topic-icon" aria-hidden="true"/>
                            </article>
                        </section>

                        <section className="topic-send-workspace">
                            <aside className="topic-send-panel topic-send-target-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Target Context</h4>
                                        <p>Broker-side message type is checked before commit.</p>
                                    </div>
                                </div>

                                <div className="topic-send-readonly-grid">
                                    <div>
                                        <FileText className="topic-icon" aria-hidden="true"/>
                                        <span>Topic</span>
                                        <strong>{topic?.name ?? '-'}</strong>
                                    </div>
                                    <div>
                                        <Layers className="topic-icon" aria-hidden="true"/>
                                        <span>Message Type</span>
                                        <strong>{isLoadingConfig ? 'Loading...' : topicMessageType}</strong>
                                    </div>
                                </div>

                                <label className="topic-send-field">
                                    <span><Tag className="topic-icon" aria-hidden="true"/>Tag</span>
                                    <input
                                        type="text"
                                        value={tag}
                                        onChange={(event) => setTag(event.target.value)}
                                        className="topic-send-input"
                                        placeholder="Optional tag..."
                                    />
                                </label>

                                <label className="topic-send-field">
                                    <span><Key className="topic-icon" aria-hidden="true"/>Key</span>
                                    <input
                                        type="text"
                                        value={messageKey}
                                        onChange={(event) => setMessageKey(event.target.value)}
                                        className="topic-send-input"
                                        placeholder="Optional key..."
                                    />
                                </label>

                                <label className={`topic-send-trace-toggle ${traceEnabled ? 'is-on' : ''}`}>
                                    <input
                                        type="checkbox"
                                        checked={traceEnabled}
                                        onChange={(event) => setTraceEnabled(event.target.checked)}
                                    />
                                    <i aria-hidden="true"/>
                                    <span>
                                        <Activity className="topic-icon" aria-hidden="true"/>
                                        Enable Message Trace
                                    </span>
                                </label>
                            </aside>

                            <main className="topic-send-panel topic-send-payload-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Message Payload</h4>
                                        <p>Required body. Standard JSON and relaxed JSON are supported.</p>
                                    </div>
                                </div>

                                <label className="topic-send-field topic-send-body-field">
                                    <span><FileText className="topic-icon" aria-hidden="true"/>Body <b>*</b></span>
                                    <textarea
                                        rows={8}
                                        value={messageBody}
                                        onChange={(event) => setMessageBody(event.target.value)}
                                        className="topic-send-textarea"
                                        placeholder="{ 1: 'value', nested: { 2: true } }"
                                    />
                                </label>

                                <div className="topic-send-helper">
                                    <Key className="topic-icon" aria-hidden="true"/>
                                    <span>
                                        Numeric keys like <code>{`{ 1: 'value' }`}</code> will be normalized before sending.
                                    </span>
                                </div>

                                <div className="topic-send-route-note">
                                    <Send className="topic-icon" aria-hidden="true"/>
                                    <span>{producerPathText}</span>
                                </div>
                            </main>

                            <aside className="topic-send-panel topic-send-result-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Commit Preview</h4>
                                        <p>Broker acknowledgement appears here.</p>
                                    </div>
                                </div>

                                {!sendResult && (
                                    <div className="topic-send-ready-card">
                                        <span>{canSubmit ? 'READY TO COMMIT' : 'WAITING FOR BODY'}</span>
                                        <strong>{canSubmit ? 'Payload ready' : 'No message body'}</strong>
                                        <small>Fill body and commit to receive message ID, queue, and offset.</small>
                                    </div>
                                )}

                                {sendResult && (
                                    <div className="topic-send-result-card">
                                        <Check className="topic-icon" aria-hidden="true"/>
                                        <span>Send Result</span>
                                        <strong>{sendResult.sendStatus}</strong>
                                        <code>
                                            {sendResult.messageId ? `Message ID: ${sendResult.messageId}` : 'Broker accepted without message id'}
                                        </code>
                                        {sendResult.localTransactionState && (
                                            <small>Local transaction: {sendResult.localTransactionState}</small>
                                        )}
                                        {sendResult.transactionId && (
                                            <code>Transaction ID: {sendResult.transactionId}</code>
                                        )}
                                        <small>
                                            Queue: {sendResult.brokerName ?? '-'} / {sendResult.queueId ?? '-'} / offset {sendResult.queueOffset}
                                        </small>
                                    </div>
                                )}

                                {error && (
                                    <div className="topic-send-error">
                                        <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                        <span>{error}</span>
                                    </div>
                                )}
                            </aside>
                        </section>
                    </div>

                    <footer className="topic-status-footer topic-send-footer">
                        <span>Commit sends once. Trace can be enabled before submit for observability.</span>
                        <div>
                            <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                                Close
                            </button>
                            <button
                                type="button"
                                onClick={() => void handleSubmit()}
                                disabled={!canSubmit}
                                className="topic-status-primary-button topic-send-commit-button"
                            >
                                <Send className="topic-icon" aria-hidden="true"/>
                                {isLoadingConfig ? 'Preparing...' : isSubmitting ? 'Sending...' : 'Commit'}
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicResetOffsetModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [consumerGroups, setConsumerGroups] = useState<string[]>([]);
    const [selectedConsumerGroup, setSelectedConsumerGroup] = useState('');
    const [selectedTime, setSelectedTime] = useState('');
    const [isLoadingGroups, setIsLoadingGroups] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        setConsumerGroups([]);
        setSelectedConsumerGroup('');
        setSelectedTime(toDateTimeLocalValue(new Date()));
        setIsLoadingGroups(true);
        setIsSubmitting(false);
        setError('');

        const loadConsumerGroups = async () => {
            try {
                const response = await TopicService.getTopicConsumerGroups({topic: topic.name});
                if (cancelled) {
                    return;
                }

                const groups = response.consumerGroups ?? [];
                setConsumerGroups(groups);
                setSelectedConsumerGroup(groups[0] ?? '');
            } catch (loadError) {
                if (!cancelled) {
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load consumer groups.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoadingGroups(false);
                }
            }
        };

        void loadConsumerGroups();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    const handleReset = async () => {
        if (!topic?.name) {
            return;
        }

        if (!selectedConsumerGroup) {
            setError('Select a consumer group before resetting offsets.');
            return;
        }

        if (!selectedTime) {
            setError('Select a reset time before submitting.');
            return;
        }

        const resetTime = new Date(selectedTime).getTime();
        if (Number.isNaN(resetTime)) {
            setError('The selected reset time is invalid.');
            return;
        }

        setIsSubmitting(true);
        setError('');

        try {
            const result = await TopicService.resetConsumerOffset({
                consumerGroupList: [selectedConsumerGroup],
                topic: topic.name,
                resetTime,
                force: true,
            });
            toast.success(result.message || `Offset reset requested for ${topic.name}`);
            onClose();
        } catch (resetError) {
            setError(resetError instanceof Error ? resetError.message : 'Failed to reset consumer offset.');
        } finally {
            setIsSubmitting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <RotateCcw className="w-5 h-5 mr-2 text-blue-500"/>
                                Reset Consumer Offset
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span> resetOffset
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-6 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm space-y-5">
                            {isLoadingGroups && (
                                <div className="rounded-xl border border-gray-200 bg-gray-50/80 px-4 py-3 text-sm text-gray-500 dark:border-gray-800 dark:bg-gray-800/60 dark:text-gray-400">
                                    Loading consumer groups for this topic...
                                </div>
                            )}

                            {!isLoadingGroups && error && (
                                <div className="rounded-xl border border-red-200 bg-red-50/80 px-4 py-3 text-sm text-red-600 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                    {error}
                                </div>
                            )}

                            {/* SubscriptionGroup */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <span className="text-red-500 mr-1">*</span>SubscriptionGroup
                                </label>
                                <div className="relative">
                                    <select
                                        value={selectedConsumerGroup}
                                        onChange={(event) => setSelectedConsumerGroup(event.target.value)}
                                        disabled={isLoadingGroups || consumerGroups.length === 0 || isSubmitting}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750">
                                        <option value="" disabled>
                                            {isLoadingGroups ? 'Loading groups...' : 'Select a group...'}
                                        </option>
                                        {consumerGroups.map((group) => (
                                            <option key={group} value={group}>
                                                {group}
                                            </option>
                                        ))}
                                    </select>
                                    <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                </div>
                                {!isLoadingGroups && consumerGroups.length === 0 && !error && (
                                    <p className="text-xs text-amber-600 dark:text-amber-400">
                                        No consumer groups are currently associated with this topic.
                                    </p>
                                )}
                            </div>

                            {/* Time */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <span className="text-red-500 mr-1">*</span>Time
                                </label>
                                <div className="relative">
                                    <input
                                        type="datetime-local"
                                        value={selectedTime}
                                        onChange={(event) => setSelectedTime(event.target.value)}
                                        disabled={isSubmitting}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-gray-400 dark:placeholder:text-gray-600 dark:[color-scheme:dark]"
                                    />
                                </div>
                                <p className="text-xs text-gray-500 dark:text-gray-400">
                                    The selected consumer group will rewind to the nearest offset at this timestamp.
                                </p>
                            </div>

                        </div>
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            onClick={() => void handleReset()}
                            disabled={isLoadingGroups || consumerGroups.length === 0 || isSubmitting}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            {isSubmitting ? 'Resetting...' : 'RESET'}
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicSkipMessageAccumulateModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [consumerGroups, setConsumerGroups] = useState<string[]>([]);
    const [selectedConsumerGroup, setSelectedConsumerGroup] = useState('');
    const [isLoadingGroups, setIsLoadingGroups] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        setConsumerGroups([]);
        setSelectedConsumerGroup('');
        setIsLoadingGroups(true);
        setIsSubmitting(false);
        setError('');

        const loadConsumerGroups = async () => {
            try {
                const response = await TopicService.getTopicConsumerGroups({topic: topic.name});
                if (cancelled) {
                    return;
                }

                const groups = response.consumerGroups ?? [];
                setConsumerGroups(groups);
                setSelectedConsumerGroup(groups[0] ?? '');
            } catch (loadError) {
                if (!cancelled) {
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load consumer groups.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoadingGroups(false);
                }
            }
        };

        void loadConsumerGroups();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    const handleSkip = async () => {
        if (!topic?.name) {
            return;
        }

        if (!selectedConsumerGroup) {
            setError('Select a consumer group before skipping accumulated messages.');
            return;
        }

        setIsSubmitting(true);
        setError('');

        try {
            const result = await TopicService.skipMessageAccumulate({
                consumerGroupList: [selectedConsumerGroup],
                topic: topic.name,
                resetTime: -1,
                force: true,
            });
            toast.success(result.message || `Skipped accumulated messages for ${topic.name}`);
            onClose();
        } catch (skipError) {
            setError(skipError instanceof Error ? skipError.message : 'Failed to skip accumulated messages.');
        } finally {
            setIsSubmitting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <FastForward className="w-5 h-5 mr-2 text-blue-500"/>
                                Skip Message Accumulate
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span> Skip Message Accumulate
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-6 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm space-y-5">
                            {isLoadingGroups && (
                                <div className="rounded-xl border border-gray-200 bg-gray-50/80 px-4 py-3 text-sm text-gray-500 dark:border-gray-800 dark:bg-gray-800/60 dark:text-gray-400">
                                    Loading consumer groups for this topic...
                                </div>
                            )}

                            {!isLoadingGroups && error && (
                                <div className="rounded-xl border border-red-200 bg-red-50/80 px-4 py-3 text-sm text-red-600 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                    {error}
                                </div>
                            )}

                            {/* SubscriptionGroup */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <Users className="w-4 h-4 mr-1.5 text-gray-400 dark:text-gray-500"/>
                                    <span className="text-red-500 mr-1">*</span>SubscriptionGroup
                                </label>
                                <div className="relative">
                                    <select
                                        value={selectedConsumerGroup}
                                        onChange={(event) => setSelectedConsumerGroup(event.target.value)}
                                        disabled={isLoadingGroups || consumerGroups.length === 0 || isSubmitting}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750">
                                        <option value="" disabled>
                                            {isLoadingGroups ? 'Loading groups...' : 'Select a group...'}
                                        </option>
                                        {consumerGroups.map((group) => (
                                            <option key={group} value={group}>
                                                {group}
                                            </option>
                                        ))}
                                    </select>
                                    <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                </div>
                                {!isLoadingGroups && consumerGroups.length === 0 && !error && (
                                    <p className="text-xs text-amber-600 dark:text-amber-400">
                                        No consumer groups are currently associated with this topic.
                                    </p>
                                )}
                            </div>

                            <div className="rounded-xl border border-amber-200 bg-amber-50/80 px-4 py-3 text-sm text-amber-700 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-300">
                                This operation skips the accumulated messages for the selected consumer group by moving its offset to the latest position.
                            </div>

                        </div>
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            onClick={() => void handleSkip()}
                            disabled={isLoadingGroups || consumerGroups.length === 0 || isSubmitting}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            {isSubmitting ? 'Submitting...' : 'Commit'}
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

interface TopicEditorModalProps {
    isOpen: boolean;
    onClose: () => void;
    targets: TopicTargetOption[];
    seed: TopicEditorSeed | null;
    mode: 'create' | 'update';
    onSaved: () => Promise<void>;
}

const TopicEditorModal = ({isOpen, onClose, targets, seed, mode, onSaved}: TopicEditorModalProps) => {
    const [selectedClusters, setSelectedClusters] = useState<string[]>([]);
    const [selectedBrokers, setSelectedBrokers] = useState<string[]>([]);
    const [isBrokerDropdownOpen, setIsBrokerDropdownOpen] = useState(false);
    const [topicName, setTopicName] = useState('');
    const [messageType, setMessageType] = useState('UNSPECIFIED');
    const [writeQueueNums, setWriteQueueNums] = useState(4);
    const [readQueueNums, setReadQueueNums] = useState(4);
    const [perm, setPerm] = useState(6);
    const [order, setOrder] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        setSelectedClusters(seed?.clusterNameList ?? []);
        setSelectedBrokers(seed?.brokerNameList ?? []);
        setTopicName(seed?.topicName ?? '');
        setMessageType(seed?.messageType || 'UNSPECIFIED');
        setWriteQueueNums(seed?.writeQueueNums ?? 4);
        setReadQueueNums(seed?.readQueueNums ?? 4);
        setPerm(seed?.perm ?? 6);
        setOrder(seed?.order ?? false);
        setIsBrokerDropdownOpen(false);
        setIsSubmitting(false);
        setError('');
    }, [isOpen, seed, mode]);

    const targetMap = new Map(targets.map((target) => [target.clusterName, target.brokerNames]));
    const availableClusters = targets.map((target) => target.clusterName);
    const availableBrokers = Array.from(new Set(selectedClusters.flatMap((clusterName) => targetMap.get(clusterName) ?? [])))
        .sort((left, right) => left.localeCompare(right));

    useEffect(() => {
        setSelectedBrokers((previous) => previous.filter((broker) => availableBrokers.includes(broker)));
    }, [selectedClusters.join('|'), availableBrokers.join('|')]);

    const toggleCluster = (clusterName: string) => {
        setSelectedClusters((previous) =>
            previous.includes(clusterName)
                ? previous.filter((item) => item !== clusterName)
                : [...previous, clusterName],
        );
    };

    const toggleBroker = (broker: string) => {
        setSelectedBrokers((previous) =>
            previous.includes(broker)
                ? previous.filter((item) => item !== broker)
                : [...previous, broker],
        );
    };

    const handleSubmit = async () => {
        if (!topicName.trim()) {
            setError('Topic name is required.');
            return;
        }

        if (selectedClusters.length === 0) {
            setError('Select at least one cluster target before submitting.');
            return;
        }

        if (writeQueueNums <= 0 || readQueueNums <= 0) {
            setError('Read and write queue counts must be greater than zero.');
            return;
        }

        if (!Number.isFinite(writeQueueNums) || !Number.isFinite(readQueueNums) || !Number.isFinite(perm)) {
            setError('Queue counts and permission must be valid numbers.');
            return;
        }

        setIsSubmitting(true);
        setError('');

        try {
            const result = await TopicService.createOrUpdateTopic({
                clusterNameList: selectedClusters,
                brokerNameList: selectedBrokers,
                topicName: topicName.trim(),
                writeQueueNums,
                readQueueNums,
                perm,
                order,
                messageType,
            });
            toast.success(result.message || `${mode === 'create' ? 'Created' : 'Updated'} topic ${topicName.trim()}`);
            await onSaved();
            onClose();
        } catch (submitError) {
            setError(getErrorMessage(submitError, 'Failed to save topic changes.'));
        } finally {
            setIsSubmitting(false);
        }
    };

    const isCreateMode = mode === 'create';
    const targetSummary = selectedClusters.length > 0
        ? `${selectedClusters.length} cluster${selectedClusters.length === 1 ? '' : 's'}`
        : 'No cluster';
    const brokerScopeSummary = selectedBrokers.length > 0
        ? `${selectedBrokers.length} broker${selectedBrokers.length === 1 ? '' : 's'}`
        : 'All brokers';
    const queueSummary = `${writeQueueNums || 0} / ${readQueueNums || 0}`;
    const queuePlanAligned = writeQueueNums === readQueueNums;
    const canSubmit = Boolean(topicName.trim())
        && selectedClusters.length > 0
        && writeQueueNums > 0
        && readQueueNums > 0
        && Number.isFinite(writeQueueNums)
        && Number.isFinite(readQueueNums)
        && Number.isFinite(perm);

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="topic-status-modal-root topic-editor-modal-root">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="topic-status-backdrop"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.96, y: 16}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.96, y: 16}}
                    className="topic-status-dialog topic-editor-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="topic-editor-title"
                >
                    <header className="topic-status-header">
                        <div className="topic-status-title-wrap">
                            <div className="topic-status-icon" aria-hidden="true">
                                <Settings className="topic-icon"/>
                            </div>
                            <div>
                                <span>Topic lifecycle action</span>
                                <h3 id="topic-editor-title">{isCreateMode ? 'Add Topic' : 'Update Topic'}</h3>
                                <p>
                                    {isCreateMode
                                        ? 'Choose deployment targets, define topic metadata, and preview queue permissions.'
                                        : <>Update settings for <strong>{seed?.topicName}</strong></>}
                                </p>
                            </div>
                        </div>
                        <div className="topic-status-header-actions">
                            <span className={`topic-editor-mode-chip ${isCreateMode ? 'is-create' : 'is-update'}`}>
                                {isCreateMode ? 'Create mode' : 'Update mode'}
                            </span>
                            <button type="button" className="topic-status-close" onClick={onClose} aria-label="Close topic editor">
                                <X className="topic-icon" aria-hidden="true"/>
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body topic-editor-body" onClick={() => setIsBrokerDropdownOpen(false)}>
                        {error && (
                            <div className="topic-status-state is-error topic-editor-error">
                                <AlertTriangle className="topic-icon" aria-hidden="true"/>
                                <strong>Topic validation failed</strong>
                                <span>{error}</span>
                            </div>
                        )}

                        <section className="topic-status-kpi-grid topic-editor-kpi-grid" aria-label="Topic editor summary">
                            <article className={`topic-status-kpi ${selectedClusters.length > 0 ? 'is-info' : 'is-warning'}`}>
                                <div>
                                    <span>Targets</span>
                                    <strong>{targetSummary}</strong>
                                    <small>{brokerScopeSummary}</small>
                                </div>
                                <Server className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-blue">
                                <div>
                                    <span>Topic</span>
                                    <strong>{topicName.trim() || 'Unnamed'}</strong>
                                    <small>{isCreateMode ? 'new topic' : 'name locked in update'}</small>
                                </div>
                                <FileText className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className={`topic-status-kpi ${queuePlanAligned ? 'is-success' : 'is-warning'}`}>
                                <div>
                                    <span>Queues</span>
                                    <strong>{queueSummary}</strong>
                                    <small>write / read queues</small>
                                </div>
                                <Database className="topic-icon" aria-hidden="true"/>
                            </article>
                            <article className="topic-status-kpi is-violet">
                                <div>
                                    <span>Permission</span>
                                    <strong>Perm {perm}</strong>
                                    <small>{order ? 'ordered delivery' : 'unordered delivery'}</small>
                                </div>
                                <Shield className="topic-icon" aria-hidden="true"/>
                            </article>
                        </section>

                        <section className="topic-editor-workspace">
                            <aside className="topic-editor-panel topic-editor-target-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Deployment Target</h4>
                                        <p>Select clusters first, then optionally narrow to brokers.</p>
                                    </div>
                                </div>

                                <div className="topic-editor-section">
                                    <span>Cluster Targets <b>*</b></span>
                                    <div className="topic-editor-chip-list">
                                        {availableClusters.map((clusterName) => (
                                            <button
                                                key={clusterName}
                                                type="button"
                                                className={`topic-editor-chip ${selectedClusters.includes(clusterName) ? 'is-active' : ''}`}
                                                onClick={() => toggleCluster(clusterName)}
                                            >
                                                {clusterName}
                                            </button>
                                        ))}
                                    </div>
                                </div>

                                <div className="topic-editor-section topic-editor-broker-section" onClick={(event) => event.stopPropagation()}>
                                    <span>Broker Scope</span>
                                    <button
                                        type="button"
                                        className={`topic-editor-select-trigger ${isBrokerDropdownOpen ? 'is-open' : ''}`}
                                        onClick={() => {
                                            if (availableBrokers.length > 0) {
                                                setIsBrokerDropdownOpen((previous) => !previous);
                                            }
                                        }}
                                        disabled={availableBrokers.length === 0}
                                    >
                                        <span>
                                            {selectedBrokers.length > 0
                                                ? brokerScopeSummary
                                                : availableBrokers.length === 0
                                                    ? 'Select cluster targets first'
                                                    : 'All brokers in selected clusters'}
                                        </span>
                                        <ChevronDown className="topic-icon" aria-hidden="true"/>
                                    </button>

                                    {selectedBrokers.length > 0 && (
                                        <div className="topic-editor-chip-list is-compact">
                                            {selectedBrokers.map((broker) => (
                                                <button key={broker} type="button" className="topic-editor-chip is-active" onClick={() => toggleBroker(broker)}>
                                                    {broker}
                                                    <X className="topic-icon" aria-hidden="true"/>
                                                </button>
                                            ))}
                                        </div>
                                    )}

                                    <AnimatePresence>
                                        {isBrokerDropdownOpen && availableBrokers.length > 0 && (
                                            <motion.div
                                                initial={{opacity: 0, y: 5}}
                                                animate={{opacity: 1, y: 0}}
                                                exit={{opacity: 0, y: 5}}
                                                className="topic-editor-dropdown"
                                            >
                                                {availableBrokers.map((broker) => (
                                                    <button key={broker} type="button" onClick={() => toggleBroker(broker)}>
                                                        <span className={selectedBrokers.includes(broker) ? 'is-checked' : ''}>
                                                            {selectedBrokers.includes(broker) && <Check className="topic-icon" aria-hidden="true"/>}
                                                        </span>
                                                        <strong>{broker}</strong>
                                                    </button>
                                                ))}
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>

                                <div className={`topic-editor-target-note ${selectedClusters.length > 0 ? 'is-ready' : ''}`}>
                                    <Server className="topic-icon" aria-hidden="true"/>
                                    <div>
                                        <strong>{selectedClusters.length > 0 ? 'Target ready' : 'Target required'}</strong>
                                        <span>{selectedClusters.length > 0 ? brokerScopeSummary : 'Choose at least one cluster before submit.'}</span>
                                    </div>
                                </div>
                            </aside>

                            <main className="topic-editor-panel topic-editor-definition-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Topic Definition</h4>
                                        <p>Topic identity, message type, queues, and permissions.</p>
                                    </div>
                                </div>

                                <label className="topic-editor-field is-wide">
                                    <span>Topic Name <b>*</b></span>
                                    <input
                                        type="text"
                                        value={topicName}
                                        readOnly={!isCreateMode}
                                        onChange={(event) => setTopicName(event.target.value)}
                                        className={`topic-editor-input ${!isCreateMode ? 'is-readonly' : ''}`}
                                    />
                                </label>

                                <label className="topic-editor-field is-wide">
                                    <span>Message Type</span>
                                    <div className="topic-editor-select-field">
                                        <select
                                            value={messageType}
                                            onChange={(event) => setMessageType(event.target.value)}
                                        >
                                            {TOPIC_MESSAGE_TYPE_OPTIONS.map((option) => (
                                                <option key={option} value={option}>{option}</option>
                                            ))}
                                        </select>
                                        <ChevronDown className="topic-icon" aria-hidden="true"/>
                                    </div>
                                </label>

                                <div className="topic-editor-queue-grid">
                                    <label className="topic-editor-field">
                                        <span>Write Queues <b>*</b></span>
                                    <input
                                        type="number"
                                        min={1}
                                        value={writeQueueNums}
                                        onChange={(event) => setWriteQueueNums(Number(event.target.value))}
                                            className="topic-editor-input"
                                    />
                                    </label>
                                    <label className="topic-editor-field">
                                        <span>Read Queues <b>*</b></span>
                                    <input
                                        type="number"
                                        min={1}
                                        value={readQueueNums}
                                        onChange={(event) => setReadQueueNums(Number(event.target.value))}
                                            className="topic-editor-input"
                                    />
                                    </label>
                                    <label className="topic-editor-field">
                                        <span>Permission <b>*</b></span>
                                    <input
                                        type="number"
                                        min={0}
                                        value={perm}
                                        onChange={(event) => setPerm(Number(event.target.value))}
                                            className="topic-editor-input"
                                    />
                                    </label>
                                </div>

                                <label className={`topic-editor-order-toggle ${order ? 'is-on' : ''}`}>
                                    <input
                                        type="checkbox"
                                        checked={order}
                                        onChange={(event) => setOrder(event.target.checked)}
                                    />
                                    <i aria-hidden="true">
                                        {order && <Check className="topic-icon" aria-hidden="true"/>}
                                    </i>
                                    <span>
                                        Ordered delivery
                                        <small>{order ? 'FIFO-sensitive consumers will receive ordered routing.' : 'Disabled for this topic.'}</small>
                                    </span>
                                </label>
                            </main>

                            <aside className="topic-editor-panel topic-editor-preview-panel">
                                <div className="topic-status-panel-header">
                                    <div>
                                        <h4>Commit Preview</h4>
                                        <p>Review the request before submit.</p>
                                    </div>
                                </div>

                                <div className="topic-editor-preview-card">
                                    <span>Request Payload</span>
                                    <code>clusterNameList: [{selectedClusters.join(', ') || '-'}]</code>
                                    <code>brokerNameList: [{selectedBrokers.join(', ') || '-'}]</code>
                                    <code>topicName: {topicName.trim() || '-'}</code>
                                    <code>writeQueueNums: {writeQueueNums}</code>
                                    <code>readQueueNums: {readQueueNums}</code>
                                    <code>perm: {perm}</code>
                                </div>

                                <div className={`topic-editor-plan-card ${queuePlanAligned ? 'is-ready' : 'is-warning'}`}>
                                    <Database className="topic-icon" aria-hidden="true"/>
                                    <div>
                                        <strong>{queuePlanAligned ? 'Queue plan aligned' : 'Queue counts differ'}</strong>
                                        <span>Write/read queues are {queueSummary}.</span>
                                    </div>
                                </div>

                                <div className={`topic-editor-plan-card ${canSubmit ? 'is-ready' : 'is-warning'}`}>
                                    <Shield className="topic-icon" aria-hidden="true"/>
                                    <div>
                                        <strong>{canSubmit ? 'Ready to submit' : 'Missing required fields'}</strong>
                                        <span>{canSubmit ? `${isCreateMode ? 'Create' : 'Update'} request is valid locally.` : 'Topic name, target cluster, and queue counts are required.'}</span>
                                    </div>
                                </div>
                            </aside>
                        </section>
                    </div>

                    <footer className="topic-status-footer topic-editor-footer">
                        <span>Create/Update validates topic name, target cluster, and queue counts before submit.</span>
                        <div>
                            <button type="button" className="topic-status-secondary-button" onClick={onClose}>
                                Cancel
                            </button>
                            <button
                                type="button"
                                onClick={() => void handleSubmit()}
                                disabled={isSubmitting}
                                className="topic-status-primary-button topic-editor-submit-button"
                            >
                                <Save className="topic-icon" aria-hidden="true"/>
                                {isSubmitting ? (isCreateMode ? 'Creating...' : 'Saving...') : (isCreateMode ? 'Create Topic' : 'Commit Changes')}
                            </button>
                        </div>
                    </footer>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

interface TopicDeleteModalProps extends TopicRouterModalProps {
    onDeleted: () => Promise<void>;
}

const TopicDeleteModal = ({isOpen, onClose, topic, onDeleted}: TopicDeleteModalProps) => {
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen) {
            return;
        }
        setIsSubmitting(false);
        setError('');
    }, [isOpen, topic?.name]);

    const handleDelete = async () => {
        if (!topic?.name) {
            return;
        }

        setIsSubmitting(true);
        setError('');

        try {
            const result = await TopicService.deleteTopic({
                topic: topic.name,
            });
            toast.success(result.message || `Deleted topic ${topic.name}`);
            await onDeleted();
            onClose();
        } catch (deleteError) {
            setError(deleteError instanceof Error ? deleteError.message : 'Failed to delete topic.');
        } finally {
            setIsSubmitting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Trash2 className="w-5 h-5 mr-2 text-red-500"/>
                                Delete Topic
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                This action removes <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span> from the current cluster mapping.
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-red-200 dark:border-red-900/50 shadow-sm space-y-4">
                            <div className="flex items-start gap-3">
                                <div className="rounded-lg bg-red-50 p-2 text-red-600 dark:bg-red-950/40 dark:text-red-300">
                                    <AlertTriangle className="w-5 h-5"/>
                                </div>
                                <div className="space-y-2">
                                    <div className="text-sm font-semibold text-gray-900 dark:text-white">
                                        Delete this topic from all discovered clusters
                                    </div>
                                    <div className="text-sm text-gray-600 dark:text-gray-400">
                                        The current backend delete path follows the topic route and removes this topic from every cluster returned by NameServer.
                                    </div>
                                </div>
                            </div>

                            {topic?.clusters?.length ? (
                                <div className="rounded-lg border border-gray-200 bg-gray-50/80 px-4 py-3 dark:border-gray-800 dark:bg-gray-800/60">
                                    <div className="text-xs font-semibold uppercase tracking-wider text-gray-500 dark:text-gray-400">
                                        Target Clusters
                                    </div>
                                    <div className="mt-2 flex flex-wrap gap-2">
                                        {topic.clusters.map((clusterName) => (
                                            <span
                                                key={clusterName}
                                                className="rounded-full border border-red-200 bg-red-50 px-2 py-0.5 text-xs text-red-700 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-300"
                                            >
                                                {clusterName}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            ) : null}

                            {error && (
                                <div className="rounded-lg border border-red-200 bg-red-50/80 px-3 py-2 text-sm text-red-600 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                    {error}
                                </div>
                            )}
                        </div>
                    </div>

                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={() => void handleDelete()}
                            disabled={isSubmitting}
                            className="px-6 py-2 bg-red-600 text-white rounded-lg text-sm font-medium hover:bg-red-500 transition-all shadow-md hover:shadow-lg flex items-center disabled:cursor-not-allowed disabled:opacity-70 dark:bg-red-600 dark:text-white dark:hover:bg-red-500"
                        >
                            <Trash2 className="w-4 h-4 mr-2"/>
                            {isSubmitting ? 'Deleting...' : 'Delete Topic'}
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TOPIC_DANGER_OPERATIONS = new Set(['Reset Consumer Offset', 'Skip Message Accumulate', 'Delete']);

const TOPIC_ROW_ACTIONS = ['Status', 'Router', 'Topic Config'];

const formatCompactNumber = (value: number) =>
    Intl.NumberFormat('en-US', {notation: 'compact', maximumFractionDigits: 1}).format(value);

const formatTopicPermission = (perm: number) => {
    const canWrite = (perm & 0x2) === 0x2;
    const canRead = (perm & 0x4) === 0x4;

    if (canRead && canWrite) {
        return 'RW';
    }

    if (canRead) {
        return 'R';
    }

    if (canWrite) {
        return 'W';
    }

    return `Perm ${perm}`;
};

const getTopicTone = (type: string) => {
    switch (type) {
        case 'SYSTEM':
            return 'system';
        case 'RETRY':
            return 'retry';
        case 'DLQ':
            return 'dlq';
        case 'FIFO':
            return 'fifo';
        case 'TRANSACTION':
            return 'transaction';
        case 'DELAY':
            return 'delay';
        default:
            return 'normal';
    }
};

const joinOrFallback = (items: string[], fallback: string) => (items.length > 0 ? items.join(', ') : fallback);

const TopicSummaryCard = ({
    label,
    value,
    hint,
    tone,
    icon,
}: {
    label: string;
    value: string;
    hint: string;
    tone: 'info' | 'blue' | 'success' | 'warning' | 'danger';
    icon: ReactNode;
}) => (
    <article className={`topic-summary-card is-${tone}`}>
        <div>
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{hint}</small>
        </div>
        <div className="topic-summary-icon" aria-hidden="true">
            {icon}
        </div>
    </article>
);

const TopicTypeBadge = ({type}: { type: string }) => (
    <span className={`topic-type-badge is-${getTopicTone(type)}`}>{type}</span>
);

const TopicActionButton = ({
    operation,
    icon: Icon,
    compact = false,
    onClick,
}: {
    operation: string;
    icon: React.ElementType;
    compact?: boolean;
    onClick: (event: React.MouseEvent<HTMLButtonElement>) => void;
}) => {
    const isDanger = TOPIC_DANGER_OPERATIONS.has(operation);
    const isStatus = operation === 'Status';

    return (
        <button
            type="button"
            aria-label={`${operation} topic`}
            onClick={onClick}
            className={`topic-action-button ${compact ? 'is-compact' : ''} ${isDanger ? 'is-danger' : ''} ${isStatus ? 'is-status' : ''}`}
        >
            <Icon className="topic-icon" aria-hidden="true"/>
            <span>{operation}</span>
        </button>
    );
};

export const TopicView = () => {
    const {data, error, isLoading, isRefreshPending, isRefreshing, refresh} = useTopicCatalog();
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedFilters, setSelectedFilters] = useState<Record<TopicCategory, boolean>>(buildDefaultTopicFilters);
    const [currentPage, setCurrentPage] = useState(1);
    const [selectedTopicName, setSelectedTopicName] = useState<string | null>(null);
    const [editorModal, setEditorModal] = useState<{ isOpen: boolean, mode: 'create' | 'update', seed: TopicEditorSeed | null }>({
        isOpen: false,
        mode: 'create',
        seed: null,
    });
    const [statusModal, setStatusModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [routerModal, setRouterModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [configModal, setConfigModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [consumerModal, setConsumerModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [sendMessageModal, setSendMessageModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [resetOffsetModal, setResetOffsetModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [skipAccumulateModal, setSkipAccumulateModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [deleteModal, setDeleteModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});

    useEffect(() => {
        if (error) {
            toast.error(error);
        }
    }, [error]);

    const topics = useMemo(() => (data?.items ?? []).map(mapTopicListItem), [data?.items]);
    const normalizedSearch = searchTerm.trim().toLowerCase();
    const filteredTopics = useMemo(
        () =>
            topics.filter((topic) => {
                const matchesType = selectedFilters[topic.type as TopicCategory] ?? true;
                if (!matchesType) {
                    return false;
                }

                if (!normalizedSearch) {
                    return true;
                }

                const searchableText = [
                    topic.name,
                    topic.type,
                    topic.messageType,
                    ...topic.clusters,
                    ...topic.brokers,
                ].join(' ').toLowerCase();

                return searchableText.includes(normalizedSearch);
            }),
        [normalizedSearch, selectedFilters, topics],
    );
    const totalPages = Math.max(1, Math.ceil(filteredTopics.length / TOPIC_PAGE_SIZE));
    const pagedTopics = filteredTopics.slice(
        (currentPage - 1) * TOPIC_PAGE_SIZE,
        currentPage * TOPIC_PAGE_SIZE,
    );
    const selectedTopic = filteredTopics.find((topic) => topic.name === selectedTopicName) ?? filteredTopics[0] ?? null;
    const topicTypeCounts = useMemo(
        () =>
            TOPIC_FILTER_ORDER.reduce((acc, key) => {
                acc[key] = topics.filter((topic) => topic.type === key).length;
                return acc;
            }, {} as Record<TopicCategory, number>),
        [topics],
    );
    const totalQueueCount = topics.reduce((sum, topic) => sum + topic.readQueueCount + topic.writeQueueCount, 0);
    const systemTopicCount = topics.filter((topic) => topic.systemTopic || topic.type === 'SYSTEM').length;
    const retryAndDlqCount = topics.filter((topic) => topic.type === 'RETRY' || topic.type === 'DLQ').length;
    const targetBrokerCount = data?.targets.reduce((sum, target) => sum + target.brokerNames.length, 0) ?? 0;
    const activeFilterCount = TOPIC_FILTER_ORDER.filter((key) => selectedFilters[key]).length;

    useEffect(() => {
        setCurrentPage(1);
    }, [normalizedSearch, selectedFilters]);

    useEffect(() => {
        if (currentPage > totalPages) {
            setCurrentPage(totalPages);
        }
    }, [currentPage, totalPages]);

    useEffect(() => {
        if (filteredTopics.length === 0) {
            if (selectedTopicName !== null) {
                setSelectedTopicName(null);
            }
            return;
        }

        if (!selectedTopicName || !filteredTopics.some((topic) => topic.name === selectedTopicName)) {
            setSelectedTopicName(filteredTopics[0].name);
        }
    }, [filteredTopics, selectedTopicName]);

    const toggleFilter = (key: TopicCategory) => {
        setSelectedFilters((prev) => ({...prev, [key]: !prev[key]}));
    };

    const handleOperation = (op: string, topic: Topic) => {
        if (op === 'Status') {
            setStatusModal({isOpen: true, topic});
        } else if (op === 'Router') {
            setRouterModal({isOpen: true, topic});
        } else if (op === 'Topic Config') {
            setConfigModal({isOpen: true, topic});
        } else if (op === 'Consumer Manage') {
            setConsumerModal({isOpen: true, topic});
        } else if (op === 'Send Message') {
            setSendMessageModal({isOpen: true, topic});
        } else if (op === 'Reset Consumer Offset') {
            setResetOffsetModal({isOpen: true, topic});
        } else if (op === 'Skip Message Accumulate') {
            setSkipAccumulateModal({isOpen: true, topic});
        } else if (op === 'Delete') {
            setDeleteModal({isOpen: true, topic});
        } else {
            toast(`${op} clicked for ${topic.name}`);
        }
    };

    const getActionIcon = (action: string) => {
        switch (action) {
            case 'Status':
                return Activity;
            case 'Router':
                return Network;
            case 'Consumer Manage':
                return Users;
            case 'Topic Config':
                return Settings;
            case 'Send Message':
                return Send;
            case 'Reset Consumer Offset':
                return RotateCcw;
            case 'Skip Message Accumulate':
                return FastForward;
            case 'Delete':
                return Trash2;
            default:
                return FileText;
        }
    };

    const getFilterIcon = (key: string) => {
        switch (key) {
            case 'NORMAL':
                return FileBox;
            case 'DELAY':
                return Clock;
            case 'FIFO':
                return Layers;
            case 'TRANSACTION':
                return ArrowRightLeft;
            case 'UNSPECIFIED':
                return HelpCircle;
            case 'RETRY':
                return RefreshCw;
            case 'DLQ':
                return AlertTriangle;
            case 'SYSTEM':
                return Shield;
            default:
                return Filter;
        }
    };

    const selectedClusterLabel = selectedTopic ? joinOrFallback(selectedTopic.clusters, 'No cluster route') : 'No topic selected';
    const selectedBrokerLabel = selectedTopic ? joinOrFallback(selectedTopic.brokers, 'No broker route') : 'No broker route';
    const visibleStart = filteredTopics.length === 0 ? 0 : (currentPage - 1) * TOPIC_PAGE_SIZE + 1;
    const visibleEnd = Math.min(currentPage * TOPIC_PAGE_SIZE, filteredTopics.length);

    return (
        <div className="topic-page animate-in fade-in slide-in-from-bottom-4 duration-500">
            <TopicStatusModal
                isOpen={statusModal.isOpen}
                onClose={() => setStatusModal({isOpen: false, topic: null})}
                topic={statusModal.topic}
            />
            <TopicRouterModal
                isOpen={routerModal.isOpen}
                onClose={() => setRouterModal({isOpen: false, topic: null})}
                topic={routerModal.topic}
            />
            <TopicConfigModal
                isOpen={configModal.isOpen}
                onClose={() => setConfigModal({isOpen: false, topic: null})}
                topic={configModal.topic}
                onRefresh={refresh}
                onEdit={(seed) => {
                    setConfigModal({isOpen: false, topic: null});
                    setEditorModal({isOpen: true, mode: 'update', seed});
                }}
            />
            <TopicEditorModal
                isOpen={editorModal.isOpen}
                onClose={() => setEditorModal({isOpen: false, mode: 'create', seed: null})}
                targets={data?.targets ?? []}
                seed={editorModal.seed}
                mode={editorModal.mode}
                onSaved={refresh}
            />
            <TopicConsumerManageModal
                isOpen={consumerModal.isOpen}
                onClose={() => setConsumerModal({isOpen: false, topic: null})}
                topic={consumerModal.topic}
            />
            <TopicSendMessageModal
                isOpen={sendMessageModal.isOpen}
                onClose={() => setSendMessageModal({isOpen: false, topic: null})}
                topic={sendMessageModal.topic}
            />
            <TopicResetOffsetModal
                isOpen={resetOffsetModal.isOpen}
                onClose={() => setResetOffsetModal({isOpen: false, topic: null})}
                topic={resetOffsetModal.topic}
            />
            <TopicSkipMessageAccumulateModal
                isOpen={skipAccumulateModal.isOpen}
                onClose={() => setSkipAccumulateModal({isOpen: false, topic: null})}
                topic={skipAccumulateModal.topic}
            />
            <TopicDeleteModal
                isOpen={deleteModal.isOpen}
                onClose={() => setDeleteModal({isOpen: false, topic: null})}
                topic={deleteModal.topic}
                onDeleted={refresh}
            />

            <section className="topic-summary-grid" aria-label="Topic summary">
                <TopicSummaryCard
                    label="Topics"
                    value={formatCompactNumber(topics.length)}
                    hint={`${topicTypeCounts.NORMAL ?? 0} normal / ${retryAndDlqCount} retry or DLQ`}
                    tone="info"
                    icon={<Database className="topic-icon"/>}
                />
                <TopicSummaryCard
                    label="Queues"
                    value={formatCompactNumber(totalQueueCount)}
                    hint="read and write allocation"
                    tone="blue"
                    icon={<Layers className="topic-icon"/>}
                />
                <TopicSummaryCard
                    label="System Topics"
                    value={formatCompactNumber(systemTopicCount)}
                    hint="guarded lifecycle actions"
                    tone="danger"
                    icon={<Shield className="topic-icon"/>}
                />
                <TopicSummaryCard
                    label="Targets"
                    value={formatCompactNumber(data?.targets.length ?? 0)}
                    hint={`${targetBrokerCount} brokers available`}
                    tone="success"
                    icon={<Server className="topic-icon"/>}
                />
            </section>

            <section className="topic-command-panel" aria-label="Topic filters and actions">
                <div className="topic-command-main">
                    <label className="topic-search-field">
                        <Search className="topic-icon" aria-hidden="true"/>
                        <span className="sr-only">Filter topics</span>
                        <input
                            type="text"
                            placeholder="Search topic, cluster, broker, or message type"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </label>

                    <div className="topic-command-actions">
                        <Button
                            variant="accent"
                            icon={Plus}
                            onClick={() => setEditorModal({isOpen: true, mode: 'create', seed: null})}
                        >
                            Add / Update
                        </Button>
                        <Button
                            variant="secondary"
                            icon={RefreshCw}
                            iconClassName={isRefreshPending ? 'rotate-180' : 'rotate-0'}
                            onClick={() => void refresh()}
                            disabled={isRefreshPending || isLoading}
                        >
                            {isRefreshing ? 'Refreshing...' : 'Refresh'}
                        </Button>
                    </div>
                </div>

                <div className="topic-filter-row">
                    <div className="topic-filter-label">
                        <Filter className="topic-icon" aria-hidden="true"/>
                        Types
                    </div>
                    {TOPIC_FILTER_ORDER.map((key) => {
                        const checked = selectedFilters[key];
                        const Icon = getFilterIcon(key);

                        return (
                            <button
                                type="button"
                                key={key}
                                onClick={() => toggleFilter(key)}
                                className={`topic-filter-chip is-${getTopicTone(key)} ${checked ? 'is-active' : ''}`}
                            >
                                <Icon className="topic-icon" aria-hidden="true"/>
                                <span>{key}</span>
                                <strong>{topicTypeCounts[key] ?? 0}</strong>
                            </button>
                        );
                    })}
                    <span className="topic-filter-count">
                        {activeFilterCount}/{TOPIC_FILTER_ORDER.length} active
                    </span>
                </div>
            </section>

            <section className="topic-workspace">
                <div className="topic-catalog-panel">
                    <div className="topic-panel-header">
                        <div>
                            <h2>Topic Catalog</h2>
                            <p>
                                {isLoading
                                    ? 'Loading topics from the current NameServer...'
                                    : `Showing ${visibleStart}-${visibleEnd} of ${filteredTopics.length} matched topics.`}
                            </p>
                        </div>
                        <div className="topic-panel-meta">
                            <span>{formatCompactNumber(topics.length)} total</span>
                            <span>{formatCompactNumber(filteredTopics.length)} matched</span>
                        </div>
                    </div>

                    <div className="topic-table-head" aria-hidden="true">
                        <span>Topic</span>
                        <span>Queues</span>
                        <span>Perm</span>
                        <span>Quick Actions</span>
                    </div>

                    <div className="topic-row-list">
                        <AnimatePresence mode="popLayout">
                            {pagedTopics.map((topic, index) => {
                                const isSelected = selectedTopic?.name === topic.name;

                                return (
                                    <motion.article
                                        layout
                                        key={topic.name}
                                        role="button"
                                        tabIndex={0}
                                        aria-pressed={isSelected}
                                        aria-label={`Select topic ${topic.name}`}
                                        initial={{opacity: 0, y: 14}}
                                        animate={{opacity: 1, y: 0}}
                                        exit={{opacity: 0, scale: 0.98}}
                                        transition={{delay: index * 0.025}}
                                        onClick={() => setSelectedTopicName(topic.name)}
                                        onKeyDown={(event) => {
                                            if (event.key === 'Enter' || event.key === ' ') {
                                                event.preventDefault();
                                                setSelectedTopicName(topic.name);
                                            }
                                        }}
                                        className={`topic-row is-${getTopicTone(topic.type)} ${isSelected ? 'is-selected' : ''}`}
                                    >
                                        <div className="topic-row-main">
                                            <div className="topic-row-icon">
                                                <Database className="topic-icon" aria-hidden="true"/>
                                            </div>
                                            <div className="topic-row-copy">
                                                <div className="topic-row-title">
                                                    <strong>{topic.name}</strong>
                                                    <TopicTypeBadge type={topic.type}/>
                                                </div>
                                                <span>{joinOrFallback(topic.clusters, 'No cluster')} · {joinOrFallback(topic.brokers, 'No broker')}</span>
                                            </div>
                                        </div>
                                        <div className="topic-row-metric">
                                            <strong>{topic.readQueueCount}/{topic.writeQueueCount}</strong>
                                            <span>R/W queues</span>
                                        </div>
                                        <div className="topic-row-metric">
                                            <strong>{formatTopicPermission(topic.perm)}</strong>
                                            <span>{topic.order ? 'ordered' : 'standard'}</span>
                                        </div>
                                        <div className="topic-row-actions">
                                            {TOPIC_ROW_ACTIONS.map((operation) => {
                                                const Icon = getActionIcon(operation);
                                                return (
                                                    <TopicActionButton
                                                        key={operation}
                                                        operation={operation === 'Topic Config' ? 'Config' : operation}
                                                        icon={Icon}
                                                        compact
                                                        onClick={(event) => {
                                                            event.stopPropagation();
                                                            handleOperation(operation, topic);
                                                        }}
                                                    />
                                                );
                                            })}
                                        </div>
                                    </motion.article>
                                );
                            })}
                        </AnimatePresence>
                    </div>

                    {filteredTopics.length === 0 && (
                        <div className="topic-empty-state">
                            <Database className="topic-icon" aria-hidden="true"/>
                            <strong>{isLoading ? 'Loading topics' : 'No topics matched'}</strong>
                            <span>
                                {isLoading
                                    ? 'The current NameServer topic catalog is being loaded.'
                                    : 'Adjust the search text or re-enable topic type filters.'}
                            </span>
                        </div>
                    )}
                </div>

                <aside className="topic-inspector-panel" aria-label="Selected topic details">
                    {selectedTopic ? (
                        <>
                            <div className="topic-inspector-header">
                                <span>Selected Topic</span>
                                <h2>{selectedTopic.name}</h2>
                                <div className="topic-inspector-tags">
                                    <TopicTypeBadge type={selectedTopic.type}/>
                                    <span>{selectedTopic.messageType}</span>
                                </div>
                            </div>

                            <div className="topic-inspector-grid">
                                <div>
                                    <span>Read Queues</span>
                                    <strong>{selectedTopic.readQueueCount}</strong>
                                </div>
                                <div>
                                    <span>Write Queues</span>
                                    <strong>{selectedTopic.writeQueueCount}</strong>
                                </div>
                                <div>
                                    <span>Permission</span>
                                    <strong>{formatTopicPermission(selectedTopic.perm)}</strong>
                                </div>
                                <div>
                                    <span>Ordering</span>
                                    <strong>{selectedTopic.order ? 'On' : 'Off'}</strong>
                                </div>
                            </div>

                            <div className="topic-inspector-section">
                                <div>
                                    <span>Cluster Route</span>
                                    <strong>{selectedClusterLabel}</strong>
                                </div>
                                <div>
                                    <span>Broker Route</span>
                                    <strong>{selectedBrokerLabel}</strong>
                                </div>
                            </div>

                            <div className="topic-inspector-actions">
                                <div className="topic-section-title">
                                    <Activity className="topic-icon" aria-hidden="true"/>
                                    Actions
                                </div>
                                <div className="topic-action-grid">
                                    {selectedTopic.operations.map((operation) => {
                                        const Icon = operation === 'Topic Config' ? Settings : getActionIcon(operation);
                                        return (
                                            <TopicActionButton
                                                key={operation}
                                                operation={operation === 'Topic Config' ? 'Config' : operation}
                                                icon={Icon}
                                                onClick={(event) => {
                                                    event.stopPropagation();
                                                    handleOperation(operation, selectedTopic);
                                                }}
                                            />
                                        );
                                    })}
                                    <TopicActionButton
                                        operation="More"
                                        icon={MoreHorizontal}
                                        onClick={(event) => {
                                            event.stopPropagation();
                                            toast(`More actions for ${selectedTopic.name}`);
                                        }}
                                    />
                                </div>
                            </div>
                        </>
                    ) : (
                        <div className="topic-empty-state is-compact">
                            <Database className="topic-icon" aria-hidden="true"/>
                            <strong>No topic selected</strong>
                            <span>Select a topic from the catalog to inspect route, queue, and lifecycle actions.</span>
                        </div>
                    )}
                </aside>
            </section>

            <div className="topic-footer">
                <span>
                    Page {currentPage} of {totalPages} · {filteredTopics.length} matched
                </span>
                <Pagination
                    currentPage={currentPage}
                    totalPages={totalPages}
                    onPageChange={setCurrentPage}
                />
            </div>
        </div>
    );
};
