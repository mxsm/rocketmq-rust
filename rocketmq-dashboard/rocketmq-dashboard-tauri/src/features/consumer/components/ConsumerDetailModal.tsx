import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    AlertCircle,
    Clock,
    Database,
    FileBox,
    FileText,
    Layers,
    LoaderCircle,
    Server,
    X,
} from 'lucide-react';
import { ConsumerService } from '../../../services/consumer.service';
import type {
    ConsumerGroupListItem,
    ConsumerTopicDetailItem,
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

const formatTimestamp = (timestamp: number) => {
    if (!timestamp || timestamp <= 0) {
        return '-';
    }
    return new Date(timestamp).toLocaleString();
};

const getTopicQueueCount = (topic: ConsumerTopicDetailItem) => topic.queueStatInfoList.length;

export const ConsumerDetailModal = ({
    isOpen,
    onClose,
    consumer,
    address,
}: ConsumerDetailModalProps) => {
    const [data, setData] = useState<ConsumerTopicDetailView | null>(null);
    const [selectedTopicName, setSelectedTopicName] = useState('');
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
        setSelectedTopicName('');

        void ConsumerService.queryConsumerTopicDetail({
            consumerGroup: consumer.rawGroupName,
            address,
        })
            .then((response) => {
                if (!cancelled) {
                    setData(response);
                    setSelectedTopicName(response.topics[0]?.topic ?? '');
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

    const consumerLabel = getConsumerLabel(consumer);
    const brokerScope = getBrokerScope(address);
    const topics = data?.topics ?? [];
    const queueCount = useMemo(
        () => topics.reduce((total, topic) => total + topic.queueStatInfoList.length, 0),
        [topics],
    );
    const selectedTopic = useMemo(
        () => topics.find((topic) => topic.topic === selectedTopicName) ?? topics[0] ?? null,
        [selectedTopicName, topics],
    );
    const lagStateClass = data && data.totalDiff > 0 ? 'is-warning' : 'is-healthy';

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
                    className="topic-status-dialog consumer-topic-detail-dialog"
                >
                    <header className="topic-status-header consumer-topic-detail-header">
                        <div className="topic-status-title-wrap">
                            <span className="topic-status-icon">
                                <FileText className="topic-icon" aria-hidden="true" />
                            </span>
                            <div>
                                <span>Consumer topic backlog</span>
                                <h3>Consumer Details</h3>
                                <p>
                                    <strong>{consumerLabel}</strong>
                                    <i aria-hidden="true">/</i>
                                    <span>{brokerScope}</span>
                                </p>
                            </div>
                        </div>

                        <div className="topic-status-header-actions consumer-topic-detail-header-actions">
                            <span className={`consumer-topic-detail-chip ${lagStateClass}`}>
                                <i aria-hidden="true" />
                                {data && data.totalDiff > 0 ? 'Lagging' : 'No lag'}
                            </span>
                            {data && (
                                <span className="consumer-topic-detail-chip is-info">
                                    {data.topicCount} topics
                                </span>
                            )}
                            <button
                                type="button"
                                onClick={onClose}
                                className="topic-status-close"
                                aria-label="Close consumer details dialog"
                            >
                                <X className="topic-icon" aria-hidden="true" />
                            </button>
                        </div>
                    </header>

                    <div className="topic-status-body consumer-topic-detail-body">
                        {isLoading ? (
                            <div className="topic-status-state">
                                <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true" />
                                <strong>Loading consumer details</strong>
                                <span>Querying topic offsets and queue lag from the active broker scope.</span>
                            </div>
                        ) : error ? (
                            <div className="topic-status-state is-error">
                                <AlertCircle className="topic-icon" aria-hidden="true" />
                                <strong>Unable to load consumer details</strong>
                                <span>{error}</span>
                            </div>
                        ) : data && topics.length > 0 ? (
                            <>
                                <section className="topic-status-kpi-grid consumer-topic-detail-summary-grid" aria-label="Consumer detail summary">
                                    <div className="topic-status-kpi consumer-topic-detail-kpi is-blue">
                                        <div>
                                            <span>Topic Count</span>
                                            <strong>{data.topicCount}</strong>
                                            <small>tracked topics</small>
                                        </div>
                                        <Database className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className={`topic-status-kpi consumer-topic-detail-kpi ${lagStateClass}`}>
                                        <div>
                                            <span>Total Lag</span>
                                            <strong>{data.totalDiff}</strong>
                                            <small>{data.totalDiff > 0 ? 'queue pressure detected' : 'all queues caught up'}</small>
                                        </div>
                                        <Activity className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className="topic-status-kpi consumer-topic-detail-kpi">
                                        <div>
                                            <span>Queue Rows</span>
                                            <strong>{queueCount}</strong>
                                            <small>offset records</small>
                                        </div>
                                        <Server className="topic-icon" aria-hidden="true" />
                                    </div>
                                    <div className="topic-status-kpi consumer-topic-detail-kpi is-violet">
                                        <div>
                                            <span>Address Scope</span>
                                            <strong>{brokerScope}</strong>
                                            <small>runtime source</small>
                                        </div>
                                        <Clock className="topic-icon" aria-hidden="true" />
                                    </div>
                                </section>

                                <section className="consumer-topic-detail-workspace">
                                    <aside className="consumer-topic-detail-topic-panel" aria-label="Topic backlog list">
                                        <div className="consumer-topic-detail-panel-header">
                                            <div>
                                                <h4>Topic Backlog</h4>
                                                <p>Select a topic to inspect queue-level offsets.</p>
                                            </div>
                                            <span>{topics.length} topics</span>
                                        </div>

                                        <div className="consumer-topic-detail-topic-list">
                                            {topics.map((topic) => {
                                                const selected = selectedTopic?.topic === topic.topic;
                                                const isRetryTopic = topic.topic.startsWith('%RETRY%');
                                                return (
                                                    <button
                                                        type="button"
                                                        key={topic.topic}
                                                        className={`consumer-topic-detail-topic-row ${selected ? 'is-selected' : ''} ${isRetryTopic ? 'is-retry' : 'is-normal'}`}
                                                        onClick={() => setSelectedTopicName(topic.topic)}
                                                    >
                                                        <span className="consumer-topic-detail-topic-icon">
                                                            {isRetryTopic ? (
                                                                <Layers className="topic-icon" aria-hidden="true" />
                                                            ) : (
                                                                <FileBox className="topic-icon" aria-hidden="true" />
                                                            )}
                                                        </span>
                                                        <span className="consumer-topic-detail-topic-copy">
                                                            <strong title={topic.topic}>{topic.topic}</strong>
                                                            <small>{getTopicQueueCount(topic)} queues</small>
                                                        </span>
                                                        <span className={`consumer-topic-detail-lag-pill ${topic.diffTotal > 0 ? 'is-warning' : 'is-healthy'}`}>
                                                            {topic.diffTotal}
                                                        </span>
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    </aside>

                                    <section className="consumer-topic-detail-queue-panel" aria-label="Selected topic queue details">
                                        {selectedTopic ? (
                                            <>
                                                <div className="consumer-topic-detail-panel-header">
                                                    <div>
                                                        <h4 title={selectedTopic.topic}>{selectedTopic.topic}</h4>
                                                        <p>Last consume {formatTimestamp(selectedTopic.lastTimestamp)}</p>
                                                    </div>
                                                    <span className={`consumer-topic-detail-lag-pill ${selectedTopic.diffTotal > 0 ? 'is-warning' : 'is-healthy'}`}>
                                                        Total lag {selectedTopic.diffTotal}
                                                    </span>
                                                </div>

                                                <div className="consumer-topic-detail-table">
                                                    <div className="consumer-topic-detail-table-head">
                                                        <span>Queue</span>
                                                        <span>Offsets</span>
                                                        <span>Lag</span>
                                                        <span>Client</span>
                                                        <span>Last Consume</span>
                                                    </div>

                                                    {selectedTopic.queueStatInfoList.length === 0 ? (
                                                        <div className="consumer-topic-detail-state">
                                                            <Server className="topic-icon" aria-hidden="true" />
                                                            <strong>No queue details</strong>
                                                            <span>This topic did not return queue-level offset rows.</span>
                                                        </div>
                                                    ) : (
                                                        selectedTopic.queueStatInfoList.map((queue) => (
                                                            <div
                                                                className="consumer-topic-detail-row"
                                                                key={`${selectedTopic.topic}-${queue.brokerName}-${queue.queueId}`}
                                                            >
                                                                <span data-label="Queue" className="consumer-topic-detail-cell is-queue">
                                                                    <strong>{queue.brokerName}</strong>
                                                                    <small>Queue {queue.queueId}</small>
                                                                </span>
                                                                <span data-label="Offsets" className="consumer-topic-detail-cell is-offset-pair">
                                                                    <span>
                                                                        <small>Broker</small>
                                                                        <strong>{queue.brokerOffset}</strong>
                                                                    </span>
                                                                    <span>
                                                                        <small>Consumer</small>
                                                                        <strong>{queue.consumerOffset}</strong>
                                                                    </span>
                                                                </span>
                                                                <span
                                                                    data-label="Lag"
                                                                    className={`consumer-topic-detail-cell is-number ${queue.diffTotal > 0 ? 'is-warning' : 'is-healthy'}`}
                                                                >
                                                                    {queue.diffTotal}
                                                                </span>
                                                                <span data-label="Client" className="consumer-topic-detail-cell is-client" title={queue.clientInfo || '-'}>
                                                                    {queue.clientInfo || '-'}
                                                                </span>
                                                                <span data-label="Last Consume" className="consumer-topic-detail-cell is-time" title={formatTimestamp(queue.lastTimestamp)}>
                                                                    {formatTimestamp(queue.lastTimestamp)}
                                                                </span>
                                                            </div>
                                                        ))
                                                    )}
                                                </div>
                                            </>
                                        ) : (
                                            <div className="consumer-topic-detail-state">
                                                <FileBox className="topic-icon" aria-hidden="true" />
                                                <strong>No topic selected</strong>
                                                <span>Select a topic to inspect queue-level offsets.</span>
                                            </div>
                                        )}
                                    </section>
                                </section>
                            </>
                        ) : (
                            <div className="topic-status-state">
                                <FileText className="topic-icon" aria-hidden="true" />
                                <strong>No consumer detail data</strong>
                                <span>No topic-level offset data was returned for this consumer group.</span>
                            </div>
                        )}
                    </div>

                    <footer className="topic-status-footer consumer-topic-detail-footer">
                        <span>Queue offsets are read-only and reflect the current consumer runtime snapshot.</span>
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
