import React, {useEffect, useMemo, useState} from 'react';
import {motion} from 'motion/react';
import {
    AlertCircle,
    ChevronDown,
    Cpu,
    Database,
    LoaderCircle,
    Network,
    RadioTower,
    Search,
    Server,
    Users,
} from 'lucide-react';
import {Pagination} from './Pagination';
import {useProducerConnections} from '../features/producer/hooks/useProducerConnections';

const PAGE_SIZE = 6;

export const ProducerView = () => {
    const {
        topicOptions,
        selectedTopic,
        setSelectedTopic,
        producerGroup,
        setProducerGroup,
        result,
        isTopicLoading,
        isSearchPending,
        isSearching,
        hasSearched,
        error,
        search,
    } = useProducerConnections();
    const [currentPage, setCurrentPage] = useState(1);
    const [selectedClientId, setSelectedClientId] = useState('');

    const clients = useMemo(() => result?.connections ?? [], [result]);
    const totalPages = Math.max(1, Math.ceil(clients.length / PAGE_SIZE));
    const pagedClients = useMemo(() => {
        const start = (currentPage - 1) * PAGE_SIZE;
        return clients.slice(start, start + PAGE_SIZE);
    }, [clients, currentPage]);
    const selectedClient = clients.find((client) => client.clientId === selectedClientId) ?? clients[0] ?? null;
    const languageCount = new Set(clients.map((client) => client.language).filter(Boolean)).size;
    const versionCount = new Set(clients.map((client) => client.versionDesc).filter(Boolean)).size;
    const runtimeLabel = selectedClient?.language ?? 'Pending';
    const versionLabel = selectedClient?.versionDesc ?? 'No client';
    const connectionCount = result?.connectionCount ?? clients.length;
    const canSearch = Boolean(selectedTopic.trim()) && Boolean(producerGroup.trim()) && !isTopicLoading && !isSearchPending;

    useEffect(() => {
        if (clients.length === 0) {
            setSelectedClientId('');
            return;
        }
        if (!clients.some((client) => client.clientId === selectedClientId)) {
            setSelectedClientId(clients[0].clientId);
        }
    }, [clients, selectedClientId]);

    const handleSearch = async () => {
        const ok = await search();
        if (ok) {
            setCurrentPage(1);
        }
    };

    return (
        <div className="producer-page">
            <section className="producer-summary-grid" aria-label="Producer summary">
                <div className="topic-summary-card">
                    <div>
                        <span>Topics Available</span>
                        <strong>{isTopicLoading ? '...' : topicOptions.length}</strong>
                        <small>selectable producer topics</small>
                    </div>
                    <span className="topic-summary-icon">
                        <Database className="topic-icon" aria-hidden="true"/>
                    </span>
                </div>
                <div className="topic-summary-card is-success">
                    <div>
                        <span>Connections</span>
                        <strong>{connectionCount}</strong>
                        <small>{hasSearched ? 'active producer clients' : 'run a scoped lookup'}</small>
                    </div>
                    <span className="topic-summary-icon">
                        <RadioTower className="topic-icon" aria-hidden="true"/>
                    </span>
                </div>
                <div className="topic-summary-card is-blue">
                    <div>
                        <span>Runtime Types</span>
                        <strong>{languageCount || '-'}</strong>
                        <small>{runtimeLabel === 'Pending' ? 'waiting for response' : `${runtimeLabel} selected`}</small>
                    </div>
                    <span className="topic-summary-icon">
                        <Cpu className="topic-icon" aria-hidden="true"/>
                    </span>
                </div>
                <div className="topic-summary-card is-warning">
                    <div>
                        <span>Client Versions</span>
                        <strong>{versionCount || '-'}</strong>
                        <small>{versionLabel}</small>
                    </div>
                    <span className="topic-summary-icon">
                        <Server className="topic-icon" aria-hidden="true"/>
                    </span>
                </div>
            </section>

            <form
                className="producer-command-panel"
                aria-label="Producer connection search"
                onSubmit={(event) => {
                    event.preventDefault();
                    void handleSearch();
                }}
            >
                <div className="producer-command-copy">
                    <strong>Producer scope</strong>
                </div>

                <div className="producer-query-controls">
                    <label className="producer-field is-topic">
                        <span>Topic</span>
                        <select
                            value={selectedTopic}
                            onChange={(event) => setSelectedTopic(event.target.value)}
                            disabled={isTopicLoading || topicOptions.length === 0}
                        >
                            {isTopicLoading && <option>Loading topics...</option>}
                            {!isTopicLoading && topicOptions.length === 0 && <option>No topics</option>}
                            {!isTopicLoading && topicOptions.map((topic) => (
                                <option key={topic} value={topic}>
                                    {topic}
                                </option>
                            ))}
                        </select>
                        <ChevronDown className="producer-field-icon" aria-hidden="true"/>
                    </label>

                    <label className="producer-field is-group">
                        <span>Producer Group</span>
                        <Search className="producer-field-icon is-left" aria-hidden="true"/>
                        <input
                            type="text"
                            placeholder="Please enter producer group..."
                            value={producerGroup}
                            onChange={(event) => setProducerGroup(event.target.value)}
                        />
                    </label>

                    <button type="submit" className="topic-action-button is-status producer-search-button" disabled={!canSearch}>
                        {isSearching ? (
                            <LoaderCircle className="topic-icon producer-spin" aria-hidden="true"/>
                        ) : (
                            <Search className="topic-icon" aria-hidden="true"/>
                        )}
                        <span>{isSearching ? 'Searching' : 'Search'}</span>
                    </button>
                </div>
            </form>

            {error && (
                <div className="producer-alert" role="alert">
                    <AlertCircle className="topic-icon" aria-hidden="true"/>
                    <span>{error}</span>
                </div>
            )}

            {result && !error && (
                <section className="producer-scope-strip" aria-label="Producer query result">
                    <div>
                        <span>Topic:</span>
                        <strong>{result.topic}</strong>
                        <i aria-hidden="true">/</i>
                        <span>Producer Group:</span>
                        <strong>{result.producerGroup}</strong>
                    </div>
                    <b>{result.connectionCount} connection{result.connectionCount === 1 ? '' : 's'}</b>
                </section>
            )}

            <section className="producer-workspace">
                <div className="producer-panel producer-list-panel">
                    <div className="topic-panel-header">
                        <div>
                            <h2>Connection Inventory</h2>
                            <p>Active producer clients returned by the NameServer route lookup.</p>
                        </div>
                        <div className="topic-panel-meta">
                            <span>{clients.length} client{clients.length === 1 ? '' : 's'}</span>
                        </div>
                    </div>

                    {hasSearched && !isSearching && clients.length === 0 ? (
                        <div className="producer-empty-state">
                            <Users className="topic-icon" aria-hidden="true"/>
                            <strong>No producer connections</strong>
                            <span>No active producer clients were found for this topic and producer group.</span>
                        </div>
                    ) : clients.length === 0 ? (
                        <div className="producer-empty-state">
                            <Search className="topic-icon" aria-hidden="true"/>
                            <strong>No lookup yet</strong>
                            <span>Select a topic, enter a producer group, and run Search to inspect active clients.</span>
                        </div>
                    ) : (
                        <div className="producer-client-list">
                            {pagedClients.map((client, index) => {
                                const selected = selectedClient?.clientId === client.clientId;
                                return (
                                    <motion.button
                                        type="button"
                                        key={client.clientId}
                                        initial={{opacity: 0, y: 14}}
                                        animate={{opacity: 1, y: 0}}
                                        transition={{duration: 0.24, delay: index * 0.03}}
                                        className={`producer-client-row ${selected ? 'is-selected' : ''}`}
                                        onClick={() => setSelectedClientId(client.clientId)}
                                    >
                                        <span className="producer-client-main">
                                            <span className="producer-client-icon">
                                                <Database className="topic-icon" aria-hidden="true"/>
                                            </span>
                                            <span className="producer-client-copy">
                                                <span>Client</span>
                                                <strong title={client.clientId}>{client.clientId}</strong>
                                            </span>
                                        </span>
                                        <span className="producer-client-cell">
                                            <span>Address</span>
                                            <strong>{client.clientAddr}</strong>
                                        </span>
                                        <span className="producer-client-cell">
                                            <span>Language</span>
                                            <b>{client.language || '-'}</b>
                                        </span>
                                        <span className="producer-client-cell">
                                            <span>Version</span>
                                            <strong>{client.versionDesc || client.version}</strong>
                                        </span>
                                    </motion.button>
                                );
                            })}
                        </div>
                    )}
                </div>

                <aside className="producer-panel producer-inspector-panel" aria-label="Selected producer client">
                    <div className="producer-inspector-header">
                        <span>Selected Client</span>
                        <strong>{selectedClient?.clientId ?? 'No client selected'}</strong>
                        {selectedClient && (
                            <div className="producer-badge-row">
                                <b>{selectedClient.language || 'Unknown'} runtime</b>
                                <b className="is-connected">Connected</b>
                            </div>
                        )}
                    </div>

                    {selectedClient ? (
                        <>
                            <div className="producer-route-card">
                                <span>Current route</span>
                                <strong>{result?.topic ?? selectedTopic}</strong>
                                <small>{result?.producerGroup ?? producerGroup}</small>
                            </div>
                            <div className="producer-detail-grid">
                                <div>
                                    <span>Client address</span>
                                    <strong>{selectedClient.clientAddr}</strong>
                                </div>
                                <div>
                                    <span>Client version</span>
                                    <strong>{selectedClient.versionDesc || selectedClient.version}</strong>
                                </div>
                                <div>
                                    <span>Protocol language</span>
                                    <strong>{selectedClient.language || '-'}</strong>
                                </div>
                                <div>
                                    <span>Connection state</span>
                                    <strong>Active</strong>
                                </div>
                            </div>
                            <div className="producer-note">
                                <Network className="topic-icon" aria-hidden="true"/>
                                <span>Producer inventory is read-only and reflects the latest query response.</span>
                            </div>
                        </>
                    ) : (
                        <div className="producer-empty-state is-compact">
                            <RadioTower className="topic-icon" aria-hidden="true"/>
                            <strong>Waiting for producer data</strong>
                            <span>Run a lookup to select a client and inspect endpoint metadata.</span>
                        </div>
                    )}
                </aside>
            </section>

            <footer className="producer-footer">
                <span>Producer connection data is read-only. Narrow the query by topic and producer group before inspecting clients.</span>
                <Pagination
                    currentPage={currentPage}
                    totalPages={totalPages}
                    onPageChange={setCurrentPage}
                />
            </footer>
        </div>
    );
};
