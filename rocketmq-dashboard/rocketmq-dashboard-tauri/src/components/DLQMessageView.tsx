import React, { useEffect, useMemo, useState } from 'react';
import { motion } from 'motion/react';
import { toast } from 'sonner@2.0.3';
import {
  AlertCircle,
  ArrowUpRight,
  Calendar,
  Clock,
  FileText,
  Hash,
  Info,
  Key,
  Search,
  Send,
  Tag,
  Users,
} from 'lucide-react';
import { MessageDetailModal } from './MessageDetailModal';
import { Pagination } from './Pagination';
import { Input } from './ui/input';
import { useConsumerCatalog } from '../features/consumer/hooks/useConsumerCatalog';
import type { DlqMessageSummary } from '../features/dlq/types/dlq.types';
import { DlqService } from '../services/dlq.service';

type DlqTab = 'Consumer' | 'Message ID';

const DEFAULT_PAGE_SIZE = 20;

const defaultPagination = {
  currentPage: 1,
  pageSize: DEFAULT_PAGE_SIZE,
  totalPages: 0,
  totalElements: 0,
};

const pad = (value: number) => value.toString().padStart(2, '0');

const formatDateTimeInput = (date: Date) =>
  `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes(),
  )}:${pad(date.getSeconds())}`;

const toSummary = (detail: Awaited<ReturnType<typeof DlqService.viewDlqMessageDetail>>): DlqMessageSummary => ({
  topic: detail.topic,
  msgId: detail.msgId,
  tags: detail.properties.TAGS ?? null,
  keys: detail.properties.KEYS ?? null,
  storeTimestamp: detail.storeTimestamp ?? 0,
});

export const DLQMessageView = () => {
  const [activeTab, setActiveTab] = useState<DlqTab>('Consumer');
  const [consumerGroup, setConsumerGroup] = useState('');
  const [messageId, setMessageId] = useState('');
  const [beginTime, setBeginTime] = useState(formatDateTimeInput(new Date(Date.now() - 3 * 60 * 60 * 1000)));
  const [endTime, setEndTime] = useState(formatDateTimeInput(new Date()));
  const [messages, setMessages] = useState<DlqMessageSummary[]>([]);
  const [selectedMessage, setSelectedMessage] = useState<DlqMessageSummary | null>(null);
  const [searchError, setSearchError] = useState('');
  const [hasSearched, setHasSearched] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [resendingMessageId, setResendingMessageId] = useState<string | null>(null);
  const [taskId, setTaskId] = useState('');
  const [pagination, setPagination] = useState(defaultPagination);
  const {
    items: consumerItems,
    isInitialLoading: isConsumerCatalogLoading,
    error: consumerCatalogError,
  } = useConsumerCatalog();

  const consumerGroupOptions = useMemo(
    () =>
      consumerItems
        .filter((item) => item.category !== 'SYSTEM')
        .map((item) => item.rawGroupName),
    [consumerItems],
  );

  useEffect(() => {
    if (!consumerGroup && consumerGroupOptions.length > 0) {
      setConsumerGroup(consumerGroupOptions[0]);
    }
  }, [consumerGroupOptions, consumerGroup]);

  useEffect(() => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTaskId('');
    setPagination(defaultPagination);
  }, [activeTab]);

  const formatTimestamp = (value: number) => {
    if (!value) {
      return '-';
    }
    return new Date(value).toLocaleString();
  };

  const parseDateTimeInput = (value: string): number | null => {
    const normalized = value.trim().replace(' ', 'T');
    if (!normalized) {
      return null;
    }

    const parsed = new Date(normalized);
    const timestamp = parsed.getTime();
    return Number.isNaN(timestamp) ? null : timestamp;
  };

  const resetConsumerPagingState = () => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTaskId('');
    setPagination(defaultPagination);
  };

  const queryDlqPage = async (pageNum = 1) => {
    if (!consumerGroup.trim()) {
      setSearchError('Consumer group is required.');
      return;
    }

    const begin = parseDateTimeInput(beginTime);
    const end = parseDateTimeInput(endTime);
    if (begin === null || end === null) {
      setSearchError('Begin and end must be valid date-time strings.');
      return;
    }
    if (end < begin) {
      setSearchError('End time must be greater than or equal to begin time.');
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);

    try {
      const response = await DlqService.queryDlqMessageByConsumerGroup({
        consumerGroup: consumerGroup.trim(),
        begin,
        end,
        pageNum,
        pageSize: pagination.pageSize,
        taskId: taskId || undefined,
      });

      setMessages(response.page.content);
      setTaskId(response.taskId);
      setPagination({
        currentPage: response.page.number + 1,
        pageSize: response.page.size,
        totalPages: response.page.totalPages,
        totalElements: response.page.totalElements,
      });
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query DLQ messages.');
    } finally {
      setIsSearching(false);
    }
  };

  const queryDlqByMessageId = async () => {
    if (!consumerGroup.trim()) {
      setSearchError('Consumer group is required.');
      return;
    }
    if (!messageId.trim()) {
      setSearchError('Message ID is required.');
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);
    setTaskId('');
    setPagination(defaultPagination);

    try {
      const detail = await DlqService.viewDlqMessageDetail({
        consumerGroup: consumerGroup.trim(),
        messageId: messageId.trim(),
      });
      setMessages([toSummary(detail)]);
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query DLQ message detail.');
    } finally {
      setIsSearching(false);
    }
  };

  const handleSearch = async () => {
    if (activeTab === 'Consumer') {
      await queryDlqPage(1);
      return;
    }

    await queryDlqByMessageId();
  };

  const handleResend = async (message: DlqMessageSummary) => {
    const normalizedConsumerGroup = consumerGroup.trim();
    if (!normalizedConsumerGroup) {
      setSearchError('Consumer group is required.');
      return;
    }

    const confirmed = window.confirm(
      `Request direct consume for DLQ message ${message.msgId} in consumer group ${normalizedConsumerGroup}?`,
    );
    if (!confirmed) {
      return;
    }

    setResendingMessageId(message.msgId);
    setSearchError('');

    try {
      const result = await DlqService.resendDlqMessage({
        consumerGroup: normalizedConsumerGroup,
        messageId: message.msgId,
      });

      if (result.success) {
        toast.success(result.message);
      } else {
        toast.error(result.message);
      }
    } catch (error) {
      const messageText = error instanceof Error ? error.message : 'Failed to resend DLQ message.';
      toast.error(messageText);
      setSearchError(messageText);
    } finally {
      setResendingMessageId(null);
    }
  };

  const renderEmptyCopy = () => {
    if (activeTab === 'Consumer') {
      return 'Enter a consumer group and time range to search DLQ messages.';
    }
    return 'Enter a consumer group and message id to load the DLQ message detail.';
  };

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 pb-12 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <MessageDetailModal
        isOpen={!!selectedMessage}
        onClose={() => setSelectedMessage(null)}
        message={selectedMessage}
      />

      <div className="mb-8 flex justify-center">
        <div className="inline-flex rounded-xl bg-gray-100 p-1 shadow-inner dark:bg-gray-800">
          {(['Consumer', 'Message ID'] as DlqTab[]).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`rounded-lg px-6 py-2 text-sm font-medium transition-all duration-200 ${
                activeTab === tab
                  ? 'bg-white text-gray-900 shadow-sm dark:bg-gray-700 dark:text-white'
                  : 'text-gray-500 hover:bg-gray-200/50 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-gray-700/50 dark:hover:text-gray-200'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>
      </div>

      <div className="flex items-start gap-3 rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800 dark:border-blue-900/40 dark:bg-blue-900/20 dark:text-blue-200">
        <Info className="mt-0.5 h-4 w-4 shrink-0" />
        <div>Phase 6 enables real single-message DLQ resend. Batch resend and export remain disabled.</div>
      </div>

      <div className="sticky top-0 z-20 flex flex-col justify-between gap-4 rounded-2xl border border-gray-100 bg-white/90 p-4 shadow-sm backdrop-blur-xl transition-colors dark:border-gray-800 dark:bg-gray-900/90 xl:flex-row xl:items-center">
        <div className="flex flex-1 flex-wrap items-center gap-4">
          <div className="flex min-w-[260px] flex-1 items-center space-x-2">
            <span className="whitespace-nowrap text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">
              Consumer Group:
            </span>
            <div className="relative flex-1">
              <Input
                value={consumerGroup}
                onChange={(event) => {
                  setConsumerGroup(event.target.value);
                  if (activeTab === 'Consumer') {
                    resetConsumerPagingState();
                  }
                }}
                list="dlq-consumer-group-options"
                placeholder={isConsumerCatalogLoading ? 'Loading consumer groups...' : 'Enter consumer group...'}
                className="border-gray-200 bg-gray-50 pr-10 font-mono text-gray-900 placeholder:text-gray-500 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
              />
              <Users className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
              <datalist id="dlq-consumer-group-options">
                {consumerGroupOptions.map((item) => (
                  <option key={item} value={item} />
                ))}
              </datalist>
            </div>
          </div>

          {activeTab === 'Consumer' ? (
            <>
              <div className="flex items-center space-x-2">
                <span className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Begin:</span>
                <div className="relative">
                  <Input
                    value={beginTime}
                    onChange={(event) => {
                      setBeginTime(event.target.value);
                      resetConsumerPagingState();
                    }}
                    className="w-48 border-gray-200 bg-gray-50 font-mono text-xs text-gray-900 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                  />
                  <Calendar className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
                </div>
              </div>

              <div className="flex items-center space-x-2">
                <span className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">End:</span>
                <div className="relative">
                  <Input
                    value={endTime}
                    onChange={(event) => {
                      setEndTime(event.target.value);
                      resetConsumerPagingState();
                    }}
                    className="w-48 border-gray-200 bg-gray-50 font-mono text-xs text-gray-900 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                  />
                  <Calendar className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
                </div>
              </div>
            </>
          ) : (
            <div className="flex min-w-[320px] flex-1 items-center space-x-2">
              <span className="whitespace-nowrap text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">
                Message ID:
              </span>
              <div className="relative flex-1">
                <Input
                  value={messageId}
                  onChange={(event) => setMessageId(event.target.value)}
                  placeholder="Enter Message ID..."
                  className="border-gray-200 bg-gray-50 pr-10 font-mono text-gray-900 placeholder:text-gray-500 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                />
                <Hash className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <button
            onClick={() => void handleSearch()}
            disabled={isSearching}
            className="flex items-center rounded-xl bg-gray-900 px-6 py-2 text-sm font-medium text-white shadow-md transition-all hover:bg-gray-800 hover:shadow-lg active:scale-95 disabled:cursor-not-allowed disabled:opacity-60 dark:border dark:border-gray-700 dark:bg-gray-900 dark:text-white dark:hover:bg-gray-800"
          >
            <Search className="mr-2 h-4 w-4" />
            {isSearching ? 'SEARCHING...' : 'SEARCH'}
          </button>

          {activeTab === 'Consumer' ? (
            <>
              <button
                disabled
                className="flex items-center rounded-xl border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-400 shadow-sm dark:border-gray-700 dark:bg-gray-800 dark:text-gray-500"
              >
                <Send className="mr-2 h-4 w-4" />
                Batch Resend
              </button>
              <button
                disabled
                className="flex items-center rounded-xl border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-400 shadow-sm dark:border-gray-700 dark:bg-gray-800 dark:text-gray-500"
              >
                <ArrowUpRight className="mr-2 h-4 w-4" />
                Batch Export
              </button>
            </>
          ) : null}
        </div>
      </div>

      {consumerCatalogError ? (
        <div className="flex items-start gap-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>Failed to load consumer groups: {consumerCatalogError}. Manual input is still available.</div>
        </div>
      ) : null}

      {searchError ? (
        <div className="flex items-start gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <div>{searchError}</div>
        </div>
      ) : null}

      <div>
        {messages.length > 0 ? (
          <div className="grid grid-cols-1 gap-6 md:grid-cols-2 2xl:grid-cols-3">
            {messages.map((message, index) => (
              <motion.div
                key={`${message.topic}-${message.msgId}`}
                initial={{ opacity: 0, y: 16 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.04 }}
                className="overflow-hidden rounded-2xl border border-gray-100 bg-white shadow-sm transition-all duration-200 hover:border-blue-200 hover:shadow-md dark:border-gray-800 dark:bg-gray-900 dark:hover:border-blue-800"
              >
                <div className="border-b border-gray-100 bg-gradient-to-br from-gray-50/70 to-white px-5 py-4 dark:border-gray-800 dark:from-gray-800/50 dark:to-gray-900">
                  <div className="mb-2 flex items-center justify-between">
                    <span className="text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                      Message ID
                    </span>
                    <span className="rounded-full border border-red-200 bg-red-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-red-500 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-300">
                      DLQ
                    </span>
                  </div>
                  <p className="break-all font-mono text-sm font-bold text-gray-900 dark:text-white">{message.msgId}</p>
                </div>

                <div className="space-y-4 p-5">
                  <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/40">
                    <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                      Topic
                    </div>
                    <div className="break-all font-mono text-xs text-gray-700 dark:text-gray-300">{message.topic}</div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/40">
                      <div className="mb-1 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                        <Tag className="h-3.5 w-3.5" />
                        Tags
                      </div>
                      <div className="break-all font-mono text-xs text-gray-700 dark:text-gray-300">
                        {message.tags || '-'}
                      </div>
                    </div>

                    <div className="rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/40">
                      <div className="mb-1 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                        <Key className="h-3.5 w-3.5" />
                        Keys
                      </div>
                      <div className="break-all font-mono text-xs text-gray-700 dark:text-gray-300">
                        {message.keys || '-'}
                      </div>
                    </div>
                  </div>

                  <div className="flex items-center rounded-xl border border-gray-100 bg-gray-50/50 p-3 dark:border-gray-800 dark:bg-gray-800/40">
                    <Clock className="mr-2.5 h-4 w-4 text-gray-400 dark:text-gray-500" />
                    <div>
                      <div className="text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                        Store Time
                      </div>
                      <div className="font-mono text-sm text-gray-900 dark:text-white">
                        {formatTimestamp(message.storeTimestamp)}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="border-t border-gray-100 bg-gray-50/30 px-5 py-3 dark:border-gray-800 dark:bg-gray-800/30">
                  <div className="grid grid-cols-2 gap-3">
                    <button
                      onClick={() => void handleResend(message)}
                      disabled={resendingMessageId === message.msgId}
                      className="flex items-center justify-center rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-700 shadow-sm transition-all hover:border-amber-300 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-60 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200 dark:hover:border-amber-800 dark:hover:bg-amber-900/30"
                    >
                      <Send className="mr-1.5 h-4 w-4" />
                      {resendingMessageId === message.msgId ? 'Resending...' : 'Resend'}
                    </button>
                    <button
                      onClick={() => setSelectedMessage(message)}
                      className="flex items-center justify-center rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 shadow-sm transition-all hover:border-blue-200 hover:bg-blue-50 hover:text-blue-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-blue-800 dark:hover:bg-blue-900/20 dark:hover:text-blue-400"
                    >
                      <FileText className="mr-1.5 h-4 w-4" />
                      Detail
                    </button>
                  </div>
                </div>
              </motion.div>
            ))}
          </div>
        ) : hasSearched ? (
          <div className="flex flex-col items-center justify-center rounded-3xl border border-dashed border-gray-200 bg-white py-20 text-center text-gray-500 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400">
            <div className="mb-4 rounded-full bg-gray-50 p-4 dark:bg-gray-800">
              <Search className="h-8 w-8 opacity-30" />
            </div>
            <p className="text-sm font-medium text-gray-700 dark:text-gray-200">No DLQ messages matched this query.</p>
            <p className="mt-1 text-xs opacity-70">Check the consumer group, time range, or message id and try again.</p>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center rounded-3xl border border-dashed border-gray-200 bg-white py-20 text-center text-gray-500 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400">
            <div className="mb-4 rounded-full bg-gray-50 p-4 dark:bg-gray-800">
              <Info className="h-8 w-8 opacity-30" />
            </div>
            <p className="text-sm font-medium text-gray-700 dark:text-gray-200">DLQ query is ready.</p>
            <p className="mt-1 text-xs opacity-70">{renderEmptyCopy()}</p>
          </div>
        )}

        {activeTab === 'Consumer' && pagination.totalPages > 1 ? (
          <div className="mt-6 flex items-center justify-center">
            <Pagination
              currentPage={pagination.currentPage}
              totalPages={pagination.totalPages}
              onPageChange={(page) => {
                if (!isSearching) {
                  void queryDlqPage(page);
                }
              }}
            />
          </div>
        ) : null}
      </div>
    </div>
  );
};
