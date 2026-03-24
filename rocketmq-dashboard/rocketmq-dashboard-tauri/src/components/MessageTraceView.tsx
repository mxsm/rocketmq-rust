import React, { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  ChevronDown,
  Clock,
  Copy,
  Database,
  Hash,
  Info,
  Key,
  MessageSquare,
  RefreshCw,
  Search,
  Server,
  Users,
  X,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import type { MessageSummary } from '../features/message/types/message.types';
import type { MessageTraceDetail, MessageTraceNode } from '../features/message-trace/types/message-trace.types';
import { MessageService } from '../services/message.service';
import { MessageTraceService } from '../services/message-trace.service';

const DEFAULT_TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';

type TraceSearchTab = 'MessageKey' | 'MessageID';

interface TraceDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  traceTopic: string;
  message: MessageSummary | null;
}

const formatTimestamp = (value?: number | null) => {
  if (!value) {
    return '-';
  }
  return new Date(value).toLocaleString();
};

const formatSpan = (value?: number | null) => {
  if (value === null || value === undefined) {
    return '-';
  }
  return `${value} ms`;
};

const roleBadgeClass = (role: string) => {
  if (role === 'PRODUCER') {
    return 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/20 dark:text-blue-300 dark:border-blue-900/40';
  }
  if (role === 'TRANSACTION') {
    return 'bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/20 dark:text-amber-300 dark:border-amber-900/40';
  }
  return 'bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-900/20 dark:text-emerald-300 dark:border-emerald-900/40';
};

const statusBadgeClass = (status: string) => {
  if (status === 'success') {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-900/20 dark:text-emerald-300 dark:border-emerald-900/40';
  }
  return 'bg-red-50 text-red-700 border-red-200 dark:bg-red-900/20 dark:text-red-300 dark:border-red-900/40';
};

const copyToClipboard = (value: string) => {
  navigator.clipboard.writeText(value);
  toast.success('Copied to clipboard');
};

const TraceStatCard = ({ label, value }: { label: string; value: string }) => (
  <div className="rounded-2xl border border-gray-200 bg-white px-4 py-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
    <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">{label}</div>
    <div className="mt-2 font-mono text-lg font-semibold text-gray-900 dark:text-white">{value}</div>
  </div>
);

const TraceNodeCard = ({ node }: { node: MessageTraceNode }) => (
  <div className="rounded-2xl border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900">
    <div className="flex flex-wrap items-center gap-2">
      <span className={`rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider ${roleBadgeClass(node.role)}`}>
        {node.role}
      </span>
      <span className="rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
        {node.traceType}
      </span>
      <span className={`rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider ${statusBadgeClass(node.status)}`}>
        {node.status}
      </span>
      <span className="ml-auto font-mono text-xs text-gray-500 dark:text-gray-400">{formatTimestamp(node.timestamp)}</span>
    </div>

    <div className="mt-4 grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Group</div>
        <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{node.groupName || '-'}</div>
      </div>
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Client Host</div>
        <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{node.clientHost || '-'}</div>
      </div>
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Store Host</div>
        <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{node.storeHost || '-'}</div>
      </div>
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Cost / Retry</div>
        <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">
          {node.costTime} ms / {node.retryTimes}
        </div>
      </div>
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Transaction Check</div>
        <div className="mt-1 text-xs text-gray-900 dark:text-white">{node.fromTransactionCheck ? 'Yes' : 'No'}</div>
      </div>
    </div>
  </div>
);

const TraceDetailModal = ({ isOpen, onClose, traceTopic, message }: TraceDetailModalProps) => {
  const [detail, setDetail] = useState<MessageTraceDetail | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!isOpen || !message) {
      setDetail(null);
      setIsLoading(false);
      setError('');
      return;
    }

    let cancelled = false;
    setDetail(null);
    setError('');
    setIsLoading(true);

    void MessageTraceService.viewMessageTraceDetail({
      traceTopic,
      messageId: message.msgId,
    })
      .then((result) => {
        if (!cancelled) {
          setDetail(result);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : 'Failed to load trace detail');
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
  }, [isOpen, message, traceTopic]);

  return (
    <AnimatePresence>
      {isOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 sm:p-6">
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="absolute inset-0 bg-gray-900/40 backdrop-blur-sm"
          />

          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 20 }}
            transition={{ duration: 0.2 }}
            className="relative flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-2xl border border-gray-100 bg-white shadow-2xl dark:border-gray-800 dark:bg-gray-950"
          >
            <div className="sticky top-0 z-10 flex items-center justify-between border-b border-gray-100 bg-white px-6 py-4 dark:border-gray-800 dark:bg-gray-950">
              <div>
                <div className="flex items-center gap-2 text-lg font-bold text-gray-900 dark:text-white">
                  <Activity className="h-5 w-5 text-blue-500" />
                  Message Trace Detail
                </div>
                <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                  Trace Topic: <span className="font-mono">{traceTopic}</span>
                </div>
              </div>
              <button
                onClick={onClose}
                className="rounded-full p-2 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600 dark:hover:bg-gray-800 dark:hover:text-gray-300"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto bg-gray-50/50 p-6 dark:bg-gray-900/40">
              {isLoading ? (
                <div className="flex flex-col items-center justify-center rounded-2xl border border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
                  <RefreshCw className="mb-4 h-8 w-8 animate-spin text-blue-500" />
                  <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Loading trace detail...</p>
                </div>
              ) : error ? (
                <div className="flex items-start gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-4 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
                  <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  <div>{error}</div>
                </div>
              ) : detail ? (
                <div className="space-y-6">
                  <div className="rounded-3xl border border-gray-200 bg-white p-6 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                    <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                      <div className="space-y-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider text-blue-700 dark:border-blue-900/40 dark:bg-blue-900/20 dark:text-blue-300">
                            {detail.topic || message?.topic || '-'}
                          </span>
                          {detail.tags ? (
                            <span className="rounded-full border border-purple-200 bg-purple-50 px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider text-purple-700 dark:border-purple-900/40 dark:bg-purple-900/20 dark:text-purple-300">
                              Tag {detail.tags}
                            </span>
                          ) : null}
                          {detail.keys ? (
                            <span className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider text-amber-700 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-300">
                              Key {detail.keys}
                            </span>
                          ) : null}
                        </div>

                        <div>
                          <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Message ID</div>
                          <div className="mt-2 flex flex-wrap items-center gap-3">
                            <div className="font-mono text-sm font-semibold text-gray-900 dark:text-white">{detail.msgId}</div>
                            <button
                              onClick={() => copyToClipboard(detail.msgId)}
                              className="inline-flex items-center gap-1 rounded-lg border border-gray-200 bg-white px-2.5 py-1 text-xs font-medium text-gray-600 transition-colors hover:border-blue-200 hover:text-blue-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-blue-900/40 dark:hover:text-blue-300"
                            >
                              <Copy className="h-3.5 w-3.5" />
                              Copy
                            </button>
                          </div>
                        </div>
                      </div>

                      <div className="grid min-w-full grid-cols-2 gap-3 sm:min-w-[360px]">
                        <TraceStatCard label="Total Span" value={formatSpan(detail.totalSpanMs)} />
                        <TraceStatCard label="Timeline Nodes" value={String(detail.timeline.length)} />
                        <TraceStatCard label="Consumer Groups" value={String(detail.consumerGroups.length)} />
                        <TraceStatCard label="Transactions" value={String(detail.transactionChecks.length)} />
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.1fr_0.9fr]">
                    <div className="space-y-6">
                      <div className="rounded-3xl border border-gray-200 bg-white p-6 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-white">
                          <Database className="h-4 w-4 text-blue-500" />
                          Producer Summary
                        </div>
                        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Producer Group</div>
                            <div className="mt-1 font-mono text-sm text-gray-900 dark:text-white">{detail.producerGroup || '-'}</div>
                          </div>
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Trace Type</div>
                            <div className="mt-1 text-sm text-gray-900 dark:text-white">{detail.producerTraceType || '-'}</div>
                          </div>
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Client Host</div>
                            <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{detail.producerClientHost || '-'}</div>
                          </div>
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Store Host</div>
                            <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{detail.producerStoreHost || detail.storeHost || '-'}</div>
                          </div>
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Timestamp</div>
                            <div className="mt-1 font-mono text-xs text-gray-900 dark:text-white">{formatTimestamp(detail.producerTimestamp)}</div>
                          </div>
                          <div>
                            <div className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Cost / Status</div>
                            <div className="mt-1 text-sm text-gray-900 dark:text-white">
                              {detail.producerCostTime ?? '-'} ms / {detail.producerStatus || '-'}
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="rounded-3xl border border-gray-200 bg-white p-6 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-white">
                          <Activity className="h-4 w-4 text-blue-500" />
                          Timeline
                        </div>
                        <div className="space-y-4">
                          {detail.timeline.map((node, index) => (
                            <TraceNodeCard key={`${node.groupName}-${node.traceType}-${node.timestamp}-${index}`} node={node} />
                          ))}
                        </div>
                      </div>
                    </div>

                    <div className="space-y-6">
                      <div className="rounded-3xl border border-gray-200 bg-white p-6 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-white">
                          <Users className="h-4 w-4 text-emerald-500" />
                          Consumer Groups
                        </div>
                        {detail.consumerGroups.length > 0 ? (
                          <div className="space-y-4">
                            {detail.consumerGroups.map((group) => (
                              <div key={group.consumerGroup} className="rounded-2xl border border-gray-200 bg-gray-50/70 p-4 dark:border-gray-800 dark:bg-gray-800/40">
                                <div className="mb-3 flex items-center justify-between">
                                  <div className="font-mono text-sm font-semibold text-gray-900 dark:text-white">{group.consumerGroup}</div>
                                  <span className="rounded-full bg-emerald-50 px-2 py-1 text-[10px] font-bold uppercase tracking-wider text-emerald-700 dark:bg-emerald-900/20 dark:text-emerald-300">
                                    {group.nodes.length} node(s)
                                  </span>
                                </div>
                                <div className="space-y-3">
                                  {group.nodes.map((node, index) => (
                                    <TraceNodeCard key={`${group.consumerGroup}-${node.timestamp}-${index}`} node={node} />
                                  ))}
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="rounded-2xl border border-dashed border-gray-200 bg-gray-50 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:bg-gray-800/30 dark:text-gray-400">
                            No consumer trace nodes were returned for this message.
                          </div>
                        )}
                      </div>

                      <div className="rounded-3xl border border-gray-200 bg-white p-6 shadow-sm dark:border-gray-800 dark:bg-gray-900">
                        <div className="mb-4 flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-white">
                          <Server className="h-4 w-4 text-amber-500" />
                          Transaction Checks
                        </div>
                        {detail.transactionChecks.length > 0 ? (
                          <div className="space-y-3">
                            {detail.transactionChecks.map((node, index) => (
                              <TraceNodeCard key={`${node.traceType}-${node.timestamp}-${index}`} node={node} />
                            ))}
                          </div>
                        ) : (
                          <div className="rounded-2xl border border-dashed border-gray-200 bg-gray-50 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:bg-gray-800/30 dark:text-gray-400">
                            No transaction check nodes were returned for this message.
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          </motion.div>
        </div>
      ) : null}
    </AnimatePresence>
  );
};

export const MessageTraceView = () => {
  const [subTab, setSubTab] = useState<TraceSearchTab>('MessageKey');
  const [traceTopic, setTraceTopic] = useState(DEFAULT_TRACE_TOPIC);
  const [topic, setTopic] = useState('');
  const [key, setKey] = useState('');
  const [msgId, setMsgId] = useState('');
  const [searchResults, setSearchResults] = useState<MessageSummary[]>([]);
  const [selectedTrace, setSelectedTrace] = useState<MessageSummary | null>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [hasSearched, setHasSearched] = useState(false);

  const { data: topicCatalog, isLoading: isTopicCatalogLoading, error: topicCatalogError } = useTopicCatalog();

  const availableTopics = useMemo(
    () =>
      (topicCatalog?.items ?? [])
        .filter((item) => !item.systemTopic && item.category !== 'RETRY' && item.category !== 'DLQ')
        .map((item) => item.topic)
        .filter((value, index, array) => array.indexOf(value) === index),
    [topicCatalog],
  );

  const availableTraceTopics = useMemo(() => {
    const topics = (topicCatalog?.items ?? [])
      .map((item) => item.topic)
      .filter((value) => value === DEFAULT_TRACE_TOPIC || value.toUpperCase().includes('TRACE'))
      .filter((value, index, array) => array.indexOf(value) === index);

    if (!topics.includes(DEFAULT_TRACE_TOPIC)) {
      topics.unshift(DEFAULT_TRACE_TOPIC);
    }

    return topics;
  }, [topicCatalog]);

  useEffect(() => {
    if (!topic && availableTopics.length > 0) {
      setTopic(availableTopics[0]);
    }
  }, [availableTopics, topic]);

  useEffect(() => {
    if (!availableTraceTopics.includes(traceTopic)) {
      setTraceTopic(availableTraceTopics[0] || DEFAULT_TRACE_TOPIC);
    }
  }, [availableTraceTopics, traceTopic]);

  useEffect(() => {
    setSearchResults([]);
    setSelectedTrace(null);
    setSearchError('');
    setHasSearched(false);
  }, [subTab]);

  const handleSearch = async () => {
    if (subTab === 'MessageKey' && !topic.trim()) {
      setSearchError('Topic is required for Message Key trace search.');
      return;
    }

    if (subTab === 'MessageKey' && !key.trim()) {
      setSearchError('Message Key is required.');
      return;
    }

    if (subTab === 'MessageID' && !msgId.trim()) {
      setSearchError('Message ID is required.');
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);
    setSelectedTrace(null);

    try {
      const response = subTab === 'MessageKey'
        ? await MessageService.queryMessageByTopicKey({
            topic: topic.trim(),
            key: key.trim(),
          })
        : await MessageTraceService.queryMessageTraceById({
            traceTopic: traceTopic.trim(),
            messageId: msgId.trim(),
          });

      setSearchResults(response.items);
    } catch (error) {
      setSearchResults([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to search message trace');
    } finally {
      setIsSearching(false);
    }
  };

  const emptyStateCopy = subTab === 'MessageKey'
    ? 'Enter a business topic and key to resolve matching messages, then open trace detail by message id.'
    : 'Enter a message id to query trace directly from the selected trace topic.';

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 pb-12 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <TraceDetailModal
        isOpen={!!selectedTrace}
        onClose={() => setSelectedTrace(null)}
        traceTopic={traceTopic}
        message={selectedTrace}
      />

      <div className="mb-2 flex items-center justify-between px-2">
        <div className="flex items-center space-x-3">
          <div className="rounded-lg bg-blue-50 p-2 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400">
            <Activity className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-lg font-bold text-gray-900 dark:text-white">Message Trace</h2>
              <p className="text-xs text-gray-500 dark:text-gray-400">Query lifecycle events from the selected trace topic.</p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Trace Topic</span>
          <div className="relative">
            <select
              value={traceTopic}
              onChange={(event) => setTraceTopic(event.target.value)}
              className="cursor-pointer appearance-none rounded-lg border border-gray-200 bg-white py-1.5 pl-3 pr-8 text-xs font-medium text-gray-700 transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300"
            >
              {availableTraceTopics.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-gray-400" />
          </div>
        </div>
      </div>

      <div className="mb-6 flex justify-center">
        <div className="inline-flex rounded-xl border border-gray-200 bg-white p-1 shadow-sm transition-colors dark:border-gray-800 dark:bg-gray-900">
          <button
            onClick={() => setSubTab('MessageKey')}
            className={`rounded-lg px-6 py-2 text-sm font-medium transition-all ${
              subTab === 'MessageKey'
                ? 'bg-blue-50 text-blue-600 shadow-sm dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-200'
            }`}
          >
            Message Key
          </button>
          <button
            onClick={() => setSubTab('MessageID')}
            className={`rounded-lg px-6 py-2 text-sm font-medium transition-all ${
              subTab === 'MessageID'
                ? 'bg-blue-50 text-blue-600 shadow-sm dark:bg-blue-900/30 dark:text-blue-400'
                : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-200'
            }`}
          >
            Message ID
          </button>
        </div>
      </div>

      <div className="sticky top-0 z-20 rounded-2xl border border-gray-100 bg-white/90 p-5 shadow-sm backdrop-blur-xl transition-colors dark:border-gray-800 dark:bg-gray-900/90">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-end">
          {subTab === 'MessageKey' ? (
            <>
              <div className="min-w-[220px] flex-1 space-y-1.5">
                <label className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Topic</label>
                <div className="group relative">
                  <MessageSquare className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 transition-colors group-focus-within:text-blue-500 dark:text-gray-500" />
                  <select
                    value={topic}
                    onChange={(event) => setTopic(event.target.value)}
                    className="w-full cursor-pointer appearance-none rounded-xl border border-gray-200 bg-gray-50 py-2.5 pl-10 pr-8 text-sm transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                  >
                    {availableTopics.length === 0 ? (
                      <option value="">{isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}</option>
                    ) : (
                      availableTopics.map((item) => (
                        <option key={item} value={item}>
                          {item}
                        </option>
                      ))
                    )}
                  </select>
                  <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
                </div>
              </div>

              <div className="min-w-[240px] flex-[2] space-y-1.5">
                <label className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Message Key</label>
                <div className="group relative">
                  <Key className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 transition-colors group-focus-within:text-blue-500 dark:text-gray-500" />
                  <input
                    type="text"
                    value={key}
                    onChange={(event) => setKey(event.target.value)}
                    placeholder="Enter Message Key..."
                    className="w-full rounded-xl border border-gray-200 bg-gray-50 py-2.5 pl-10 pr-4 text-sm transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-white dark:placeholder:text-gray-500"
                  />
                </div>
              </div>
            </>
          ) : (
            <div className="min-w-[320px] flex-1 space-y-1.5">
              <label className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Message ID</label>
              <div className="group relative">
                <Hash className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 transition-colors group-focus-within:text-blue-500 dark:text-gray-500" />
                <input
                  type="text"
                  value={msgId}
                  onChange={(event) => setMsgId(event.target.value)}
                  placeholder="Enter Message ID..."
                  className="w-full rounded-xl border border-gray-200 bg-gray-50 py-2.5 pl-10 pr-4 text-sm font-mono transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-white dark:placeholder:text-gray-500"
                />
              </div>
            </div>
          )}

          <div className="flex items-center gap-3 pb-0.5">
            <button
              onClick={() => void handleSearch()}
              disabled={isSearching || (subTab === 'MessageKey' && isTopicCatalogLoading)}
              className="whitespace-nowrap rounded-xl bg-gray-900 px-8 py-2.5 text-sm font-medium text-white shadow-md transition-all hover:bg-gray-800 hover:shadow-lg active:scale-95 disabled:cursor-not-allowed disabled:opacity-70 dark:border dark:border-gray-700 dark:bg-gray-900 dark:text-white dark:hover:bg-gray-800"
            >
              <div className="flex items-center">
                <Search className="mr-2 h-4 w-4" />
                {isSearching ? 'Searching...' : 'Search'}
              </div>
            </button>
          </div>
        </div>
      </div>

      {topicCatalogError ? (
        <div className="flex items-start gap-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>Failed to load topic catalog: {topicCatalogError}</div>
        </div>
      ) : null}

      {searchError ? (
        <div className="flex items-start gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>{searchError}</div>
        </div>
      ) : null}

      <div className="flex items-center justify-between rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-600 shadow-sm dark:border-gray-800 dark:bg-gray-900 dark:text-gray-300">
        <div className="flex items-center gap-2">
          <Info className="h-4 w-4 text-blue-500" />
          <span>
            {subTab === 'MessageKey'
              ? 'Message Key mode first resolves matching messages, then loads trace detail by message id.'
              : 'Message ID mode queries trace data directly from the selected trace topic.'}
          </span>
        </div>
        <span className="font-mono text-xs text-gray-400 dark:text-gray-500">{hasSearched ? `${searchResults.length} result(s)` : 'ready'}</span>
      </div>

      {isSearching ? (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <RefreshCw className="mb-4 h-10 w-10 animate-spin text-blue-500" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Searching trace data...</p>
        </div>
      ) : searchResults.length > 0 ? (
        <div className="grid grid-cols-1 gap-6 md:grid-cols-2 2xl:grid-cols-3">
          {searchResults.map((row, index) => (
            <motion.div
              key={row.msgId}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.05 }}
              className="group flex flex-col overflow-hidden rounded-2xl border border-gray-100 bg-white shadow-sm transition-all duration-200 hover:shadow-md dark:border-gray-800 dark:bg-gray-900"
            >
              <div className="flex items-start justify-between border-b border-gray-50 bg-gradient-to-br from-gray-50/50 to-white px-5 py-4 dark:border-gray-800 dark:from-gray-800/50 dark:to-gray-900">
                <div className="min-w-0 flex-1">
                  <div className="mb-1 flex items-center gap-2">
                    <div className="rounded-lg bg-indigo-50 p-2 text-indigo-600 dark:bg-indigo-900/30 dark:text-indigo-400">
                      <Hash className="h-4 w-4" />
                    </div>
                    <span className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">Message ID</span>
                  </div>
                  <p className="truncate font-mono text-sm font-bold text-gray-900 dark:text-white" title={row.msgId}>
                    {row.msgId}
                  </p>
                </div>
                <button
                  onClick={() => copyToClipboard(row.msgId)}
                  className="rounded-lg p-2 text-gray-300 transition-colors hover:bg-gray-100 hover:text-blue-600 dark:text-gray-600 dark:hover:bg-gray-800 dark:hover:text-blue-400"
                >
                  <Copy className="h-3.5 w-3.5" />
                </button>
              </div>

              <div className="flex-1 space-y-4 p-5">
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/50">
                    <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">Topic</p>
                    <p className="font-mono text-xs font-medium text-gray-700 dark:text-gray-300">{row.topic || '-'}</p>
                  </div>
                  <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/50">
                    <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">Store Time</p>
                    <div className="flex items-center text-xs text-gray-500 dark:text-gray-400">
                      <Clock className="mr-1.5 h-3.5 w-3.5 text-gray-400 dark:text-gray-500" />
                      {formatTimestamp(row.storeTimestamp)}
                    </div>
                  </div>
                  <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/50">
                    <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">Tag</p>
                    <span className="inline-flex rounded border border-purple-200 bg-purple-50 px-2 py-0.5 text-xs font-medium text-purple-700 dark:border-purple-900/40 dark:bg-purple-900/20 dark:text-purple-300">
                      {row.tags || '-'}
                    </span>
                  </div>
                  <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/50">
                    <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">Key</p>
                    <p className="truncate font-mono text-xs font-medium text-gray-700 dark:text-gray-300" title={row.keys || '-'}>
                      {row.keys || '-'}
                    </p>
                  </div>
                </div>
              </div>

              <div className="flex justify-end border-t border-gray-100 bg-gray-50/30 px-5 py-3 dark:border-gray-800 dark:bg-gray-800/30">
                <button
                  onClick={() => setSelectedTrace(row)}
                  className="inline-flex items-center rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-gray-600 shadow-sm transition-all hover:border-blue-200 hover:bg-blue-50 hover:text-blue-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-blue-900/40 dark:hover:bg-blue-900/20 dark:hover:text-blue-300"
                >
                  <Activity className="mr-1.5 h-3.5 w-3.5" />
                  Trace Detail
                </button>
              </div>
            </motion.div>
          ))}
        </div>
      ) : hasSearched ? (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <Search className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">No trace candidates found</p>
          <p className="mt-2 max-w-2xl text-sm text-gray-500 dark:text-gray-400">
            {emptyStateCopy}
          </p>
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <CheckCircle2 className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Trace search is ready</p>
          <p className="mt-2 max-w-2xl text-sm text-gray-500 dark:text-gray-400">
            {emptyStateCopy}
          </p>
        </div>
      )}
    </div>
  );
};
