import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    AlertCircle,
    Cpu,
    Database,
    ListChecks,
    LoaderCircle,
    Network,
    Radio,
    Server,
    Users,
    X,
} from 'lucide-react';
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

const getBrokerScope = (address?: string) => {
    const trimmed = address?.trim();
    return trimmed && trimmed.length > 0 ? trimmed : 'All brokers';
};

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

    const consumerLabel = getConsumerLabel(consumer);
    const brokerScope = getBrokerScope(address);

    const latestSubscriptionVersion = useMemo(() => {
        if (!data || data.subscriptions.length === 0) {
            return '-';
        }
        return Math.max(...data.subscriptions.map((item) => item.subVersion)).toString();
    }, [data]);

    const connectionCount = data?.connectionCount ?? data?.connections.length ?? 0;
    const hasConnections = connectionCount > 0;

    if (!isOpen) {
        return null;
    }

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
                    className="topic-status-dialog consumer-client-dialog"
                >
                    <header className="topic-status-header consumer-client-header">
                        <div className="topic-status-title-wrap">
                            <span className="topic-status-icon consumer-client-header-icon">
                                <Users className="topic-icon" aria-hidden="true" />
                            </span>
                            <div>
                                <span>Consumer client session</span>
                                <h3>Client Information</h3>
                                <p>
                                    <strong>{consumerLabel}</strong>
                                    <i aria-hidden="true">/</i>
                                    <span>{brokerScope}</span>
                                </p>
                            </div>
                        </div>

                        <div className="topic-status-header-actions consumer-client-header-actions">
                            <span className={`consumer-client-chip ${hasConnections ? 'is-healthy' : 'is-idle'}`}>
                                <i aria-hidden="true" />
                                {hasConnections ? 'Online' : 'Idle'}
                            </span>
                            {data && (
                                <span className="consumer-client-chip is-type">{data.consumeType}</span>
                            )}
                            <button
                                type="button"
                                onClick={onClose}
                                className="topic-status-close"
                                aria-label="Close client information dialog"
                            >
                                <X className="topic-icon" aria-hidden="true" />
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body consumer-client-body">
                        {isLoading ? (
                            <div className="topic-status-state">
                                <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true" />
                                <strong>Loading client information</strong>
                                <span>Querying consumer connections and subscriptions from the active broker scope.</span>
                            </div>
                        ) : error ? (
                            <div className="topic-status-state is-error">
                                <AlertCircle className="topic-icon" aria-hidden="true" />
                                <strong>Unable to load client information</strong>
                                <span>{error}</span>
                            </div>
                        ) : data ? (
                            <>
                                <section className="topic-status-kpi-grid consumer-client-summary-grid" aria-label="Client summary">
                                    <div className={`topic-status-kpi consumer-client-kpi ${hasConnections ? 'is-success' : 'is-warning'}`}>
                                        <div>
                                            <span>Connections</span>
                                            <strong>{connectionCount}</strong>
                                            <small>{hasConnections ? 'active client channel' : 'no active clients'}</small>
                                        </div>
                                        <Network className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className="topic-status-kpi consumer-client-kpi is-blue">
                                        <div>
                                            <span>Consume Type</span>
                                            <strong>{data.consumeType || '-'}</strong>
                                            <small>delivery mode</small>
                                        </div>
                                        <Radio className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className="topic-status-kpi consumer-client-kpi is-violet">
                                        <div>
                                            <span>Message Model</span>
                                            <strong>{data.messageModel || '-'}</strong>
                                            <small>allocation strategy</small>
                                        </div>
                                        <Cpu className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className="topic-status-kpi consumer-client-kpi">
                                        <div>
                                            <span>Subscriptions</span>
                                            <strong>{data.subscriptions.length}</strong>
                                            <small>latest {latestSubscriptionVersion}</small>
                                        </div>
                                        <ListChecks className="topic-icon" aria-hidden="true" />
                                    </div>
                                </section>

                                <section className="consumer-client-workspace">
                                    <aside className="consumer-client-rail" aria-label="Consumer client context">
                                        <div className="consumer-client-profile-card">
                                            <span className="consumer-client-profile-icon">
                                                <Users className="topic-icon" aria-hidden="true" />
                                            </span>
                                            <div className="consumer-client-profile-copy">
                                                <span>Consumer Group</span>
                                                <strong title={consumerLabel}>{consumerLabel}</strong>
                                                <small>{brokerScope}</small>
                                            </div>
                                        </div>

                                        <div className="consumer-client-meta-grid">
                                            <div>
                                                <span>Consume From</span>
                                                <strong>{data.consumeFromWhere || '-'}</strong>
                                            </div>
                                            <div>
                                                <span>Client Count</span>
                                                <strong>{data.connections.length}</strong>
                                            </div>
                                            <div>
                                                <span>Broker Scope</span>
                                                <strong>{brokerScope}</strong>
                                            </div>
                                            <div>
                                                <span>Topics</span>
                                                <strong>{data.subscriptions.length}</strong>
                                            </div>
                                        </div>

                                        <div className="consumer-client-note">
                                            <Activity className="topic-icon" aria-hidden="true" />
                                            <span>
                                                Connection and subscription rows are read-only snapshots returned by the active broker query.
                                            </span>
                                        </div>
                                    </aside>

                                    <div className="consumer-client-main">
                                        <section className="consumer-client-panel" aria-label="Client connections">
                                            <div className="consumer-client-panel-header">
                                                <div className="consumer-client-panel-title">
                                                    <span className="consumer-client-panel-icon">
                                                        <Server className="topic-icon" aria-hidden="true" />
                                                    </span>
                                                    <div>
                                                        <h4>Client Connections</h4>
                                                        <p>Connected client identities and runtime versions.</p>
                                                    </div>
                                                </div>
                                                <span className="consumer-client-panel-count">{data.connections.length} rows</span>
                                            </div>

                                            {data.connections.length === 0 ? (
                                                <div className="consumer-client-state">
                                                    <Network className="topic-icon" aria-hidden="true" />
                                                    <strong>No active client connections</strong>
                                                    <span>No clients were returned for this consumer group.</span>
                                                </div>
                                            ) : (
                                                <div className="consumer-client-table is-connections">
                                                    <div className="consumer-client-table-head">
                                                        <span>Client ID</span>
                                                        <span>Client Addr</span>
                                                        <span>Language</span>
                                                        <span>Version</span>
                                                    </div>
                                                    {data.connections.map((item) => (
                                                        <div className="consumer-client-row" key={item.clientId}>
                                                            <span data-label="Client ID" className="consumer-client-cell is-mono" title={item.clientId}>
                                                                {item.clientId}
                                                            </span>
                                                            <span data-label="Client Addr" className="consumer-client-cell is-mono">{item.clientAddr}</span>
                                                            <span data-label="Language" className="consumer-client-cell">{item.language}</span>
                                                            <span data-label="Version" className="consumer-client-cell">{item.versionDesc}</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            )}
                                        </section>

                                        <section className="consumer-client-panel" aria-label="Client subscriptions">
                                            <div className="consumer-client-panel-header">
                                                <div className="consumer-client-panel-title">
                                                    <span className="consumer-client-panel-icon is-violet">
                                                        <Database className="topic-icon" aria-hidden="true" />
                                                    </span>
                                                    <div>
                                                        <h4>Subscriptions</h4>
                                                        <p>Topic expressions and filter metadata currently registered.</p>
                                                    </div>
                                                </div>
                                                <span className="consumer-client-panel-count">{data.subscriptions.length} topics</span>
                                            </div>

                                            {data.subscriptions.length === 0 ? (
                                                <div className="consumer-client-state">
                                                    <ListChecks className="topic-icon" aria-hidden="true" />
                                                    <strong>No subscriptions returned</strong>
                                                    <span>This consumer group did not report any topic subscriptions.</span>
                                                </div>
                                            ) : (
                                                <div className="consumer-client-table is-subscriptions">
                                                    <div className="consumer-client-table-head">
                                                        <span>Topic</span>
                                                        <span>Sub String</span>
                                                        <span>Expr Type</span>
                                                        <span>Tags</span>
                                                        <span>Codes</span>
                                                        <span>Sub Version</span>
                                                    </div>
                                                    {data.subscriptions.map((item) => (
                                                        <div className="consumer-client-row" key={`${item.topic}-${item.subVersion}`}>
                                                            <span data-label="Topic" className="consumer-client-cell is-mono" title={item.topic}>
                                                                {item.topic}
                                                            </span>
                                                            <span data-label="Sub String" className="consumer-client-cell is-mono">{item.subString}</span>
                                                            <span data-label="Expr Type" className="consumer-client-cell">{item.expressionType}</span>
                                                            <span data-label="Tags" className="consumer-client-cell">
                                                                {item.tagsSet.length > 0 ? item.tagsSet.join(', ') : '-'}
                                                            </span>
                                                            <span data-label="Codes" className="consumer-client-cell">
                                                                {item.codeSet.length > 0 ? item.codeSet.join(', ') : '-'}
                                                            </span>
                                                            <span data-label="Sub Version" className="consumer-client-cell is-mono">{item.subVersion}</span>
                                                        </div>
                                                    ))}
                                                </div>
                                            )}
                                        </section>
                                    </div>
                                </section>
                            </>
                        ) : (
                            <div className="topic-status-state">
                                <Users className="topic-icon" aria-hidden="true" />
                                <strong>No consumer connection data</strong>
                                <span>The backend returned an empty client information response.</span>
                            </div>
                        )}
                    </div>

                    <footer className="topic-status-footer consumer-client-footer">
                        <span>Client data is scoped by consumer group and broker address.</span>
                        <div>
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
