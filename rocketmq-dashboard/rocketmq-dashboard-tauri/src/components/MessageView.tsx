import React, { useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  Calendar,
  ChevronDown,
  Clock3,
  Copy,
  FileText,
  Info,
  KeyRound,
  Search,
  Tag,
} from 'lucide-react';
import { motion } from 'framer-motion';
import { toast } from 'sonner@2.0.3';
import { MessageDetailModal } from './MessageDetailModal';
import { Pagination } from './Pagination';
import { Input } from './ui/input';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import type { MessageSummary } from '../features/message/types/message.types';
import { MessageService } from '../services/message.service';

type MessageTab = 'Topic' | 'Message Key' | 'Message ID';

const DEFAULT_PAGE_SIZE = 12;

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

export const MessageView = () => {
  const [activeTab, setActiveTab] = useState<MessageTab>('Topic');
  const [topic, setTopic] = useState('');
  const [msgKey, setMsgKey] = useState('');
  const [msgId, setMsgId] = useState('');
  const [startDate, setStartDate] = useState(() => formatDateTimeInput(new Date(Date.now() - 60 * 60 * 1000)));
  const [endDate, setEndDate] = useState(() => formatDateTimeInput(new Date()));
  const [messages, setMessages] = useState<MessageSummary[]>([]);
  const [selectedMessage, setSelectedMessage] = useState<MessageSummary | null>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [hasSearched, setHasSearched] = useState(false);
  const [topicTaskId, setTopicTaskId] = useState('');
  const [topicPagination, setTopicPagination] = useState(defaultPagination);
  const { data: topicCatalog, isLoading: isTopicCatalogLoading, error: topicCatalogError } = useTopicCatalog();

  const availableTopics = useMemo(
    () => topicCatalog?.items.map((item) => item.topic) ?? [],
    [topicCatalog],
  );

  useEffect(() => {
    if (!topic && availableTopics.length > 0) {
      setTopic(availableTopics[0]);
    }
  }, [availableTopics, topic]);

  useEffect(() => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTopicTaskId('');
    setTopicPagination(defaultPagination);
  }, [activeTab]);

  const formatTimestamp = (value: number) => {
    if (!value) {
      return '-';
    }
    return new Date(value).toLocaleString();
  };

  const resetTopicPagingState = () => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTopicTaskId('');
    setTopicPagination(defaultPagination);
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

  const queryTopicPage = async (pageNum = 1) => {
    if (!topic.trim()) {
      setSearchError('Topic is required.');
      return;
    }

    const begin = parseDateTimeInput(startDate);
    const end = parseDateTimeInput(endDate);
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
      const response = await MessageService.queryMessagePageByTopic({
        topic: topic.trim(),
        begin,
        end,
        pageNum,
        pageSize: topicPagination.pageSize,
        taskId: topicTaskId || undefined,
      });

      setMessages(response.page.content);
      setTopicTaskId(response.taskId);
      setTopicPagination({
        currentPage: response.page.number + 1,
        pageSize: response.page.size,
        totalPages: response.page.totalPages,
        totalElements: response.page.totalElements,
      });
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query messages by topic.');
    } finally {
      setIsSearching(false);
    }
  };

  const handleSearch = async () => {
    if (activeTab === 'Topic') {
      await queryTopicPage(1);
      return;
    }

    if (!topic.trim()) {
      setSearchError('Topic is required.');
      return;
    }

    if (activeTab === 'Message Key' && !msgKey.trim()) {
      setSearchError('Message Key is required.');
      return;
    }

    if (activeTab === 'Message ID' && !msgId.trim()) {
      setSearchError('Message ID is required.');
      return;
    }

    if (activeTab === 'Message ID') {
      setSearchError('');
      setHasSearched(false);
      setMessages([]);
      setSelectedMessage({
        topic: topic.trim(),
        msgId: msgId.trim(),
        queryMsgId: msgId.trim(),
        tags: null,
        keys: null,
        storeTimestamp: 0,
      });
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);

    try {
      const response = activeTab === 'Message Key'
        ? await MessageService.queryMessageByTopicKey({
            topic: topic.trim(),
            key: msgKey.trim(),
          })
        : await MessageService.queryMessageById({
            topic: topic.trim(),
            messageId: msgId.trim(),
          });

      setMessages(response.items);
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query messages.');
    } finally {
      setIsSearching(false);
    }
  };

  const renderTopicSelector = () => (
    <div className="flex items-center space-x-2">
      <span className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Topic:</span>
      <div className="relative group">
        <select
          value={topic}
          onChange={(event) => {
            setTopic(event.target.value);
            if (activeTab === 'Topic') {
              resetTopicPagingState();
            }
          }}
          className="w-48 cursor-pointer appearance-none rounded-xl border border-gray-200 bg-gray-50 py-2 pl-3 pr-8 text-sm font-medium text-gray-700 transition-all focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300"
        >
          {availableTopics.length === 0 ? (
            <option value="">
              {isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}
            </option>
          ) : (
            availableTopics.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))
          )}
        </select>
        <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
      </div>
    </div>
  );

  const renderSearchArea = () => {
    switch (activeTab) {
      case 'Topic':
        return (
          <>
            {renderTopicSelector()}
            <div className="mx-2 hidden h-8 w-px bg-gray-200 dark:bg-gray-700 xl:block" />
            <div className="flex items-center space-x-2">
              <span className="text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Begin:</span>
              <div className="relative">
                <Input
                  value={startDate}
                  onChange={(event) => {
                    setStartDate(event.target.value);
                    resetTopicPagingState();
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
                  value={endDate}
                  onChange={(event) => {
                    setEndDate(event.target.value);
                    resetTopicPagingState();
                  }}
                  className="w-48 border-gray-200 bg-gray-50 font-mono text-xs text-gray-900 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                />
                <Calendar className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400 dark:text-gray-500" />
              </div>
            </div>
          </>
        );
      case 'Message Key':
        return (
          <>
            {renderTopicSelector()}
            <div className="mx-2 hidden h-8 w-px bg-gray-200 dark:bg-gray-700 md:block" />
            <div className="flex flex-1 items-center space-x-2">
              <span className="whitespace-nowrap text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Key:</span>
              <Input
                value={msgKey}
                onChange={(event) => setMsgKey(event.target.value)}
                placeholder="Enter Message Key..."
                className="min-w-[200px] flex-1 border-gray-200 bg-gray-50 text-gray-900 placeholder:text-gray-500 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
              />
            </div>
            <div className="whitespace-nowrap rounded border border-orange-100 bg-orange-50 px-2 py-1 text-xs font-medium text-orange-500 dark:border-orange-900/30 dark:bg-orange-900/20 dark:text-orange-400">
              Only returns up to 64 messages
            </div>
          </>
        );
      case 'Message ID':
        return (
          <>
            {renderTopicSelector()}
            <div className="mx-2 hidden h-8 w-px bg-gray-200 dark:bg-gray-700 md:block" />
            <div className="flex flex-1 items-center space-x-2">
              <span className="whitespace-nowrap text-xs font-bold uppercase tracking-wider text-gray-500 dark:text-gray-400">Message ID:</span>
              <Input
                value={msgId}
                onChange={(event) => setMsgId(event.target.value)}
                placeholder="Enter Message ID..."
                className="min-w-[300px] flex-1 border-gray-200 bg-gray-50 font-mono text-gray-900 placeholder:text-gray-500 dark:border-gray-700 dark:bg-gray-800 dark:text-white"
              />
            </div>
          </>
        );
      default:
        return null;
    }
  };

  const renderEmptyCopy = () => {
    if (activeTab === 'Topic') {
      return 'Select a topic and time window to load paged results.';
    }
    if (activeTab === 'Message Key') {
      return 'Enter a topic and message key to start searching.';
    }
    return 'Enter a topic and message id to open the real detail dialog directly.';
  };

  const renderMessageCards = () => (
    <div className="grid grid-cols-1 gap-6 md:grid-cols-2 2xl:grid-cols-3">
      {messages.map((msg, index) => (
        <motion.div
          key={`${msg.topic}-${msg.msgId}`}
          initial={{ opacity: 0, y: 20, scale: 0.95 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          whileHover={{
            y: -4,
            boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.05), 0 8px 10px -6px rgba(0, 0, 0, 0.01)',
          }}
          transition={{
            duration: 0.3,
            delay: index * 0.05,
            ease: [0.22, 1, 0.36, 1],
          }}
          className="group flex flex-col overflow-hidden rounded-[20px] border border-gray-100 bg-white shadow-sm transition-all duration-300 hover:border-blue-200 dark:border-gray-800 dark:bg-gray-900 dark:hover:border-blue-800"
        >
          {/* Header - Message ID Focused */}
          <div className="border-b border-gray-100 bg-gradient-to-br from-gray-50/80 to-white p-5 dark:border-gray-800 dark:from-gray-800/80 dark:to-gray-900">
            <div className="mb-3 flex items-center gap-3">
              <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-gray-100 bg-white shadow-sm dark:border-gray-700 dark:bg-gray-800">
                <FileText className="h-5 w-5 text-blue-600 dark:text-blue-500" />
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center justify-between">
                  <span className="mb-0.5 text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                    Message ID
                  </span>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      navigator.clipboard.writeText(msg.msgId);
                      toast.success('Copied message ID');
                    }}
                    className="p-1 text-gray-300 transition-colors hover:text-blue-600 dark:text-gray-600 dark:hover:text-blue-400"
                    title="Copy message id"
                  >
                    <Copy className="h-3.5 w-3.5" />
                  </button>
                </div>
                <h3 className="truncate font-mono text-xs font-bold text-gray-900 dark:text-white" title={msg.msgId}>
                  {msg.msgId}
                </h3>
              </div>
            </div>
          </div>

          {/* Body Content */}
          <div className="flex-1 space-y-4 p-5">
            {/* Tag Section */}
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-blue-50 dark:bg-blue-900/30">
                <Tag className="h-4 w-4 text-blue-600 dark:text-blue-400" />
              </div>
              <div>
                <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                  Tag
                </div>
                <span className="inline-flex items-center rounded-md border border-gray-200 bg-white px-2.5 py-1 text-xs font-bold text-gray-700 shadow-sm dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
                  {msg.tags || '-'}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Key Section */}
              <div className="flex items-start gap-3">
                <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-amber-50 dark:bg-amber-900/30">
                  <KeyRound className="h-4 w-4 text-amber-600 dark:text-amber-400" />
                </div>
                <div className="min-w-0">
                  <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                    Key
                  </div>
                  <div
                    className="truncate font-mono text-xs font-medium text-gray-900 dark:text-gray-300"
                    title={msg.keys || '-'}
                  >
                    {msg.keys || '-'}
                  </div>
                </div>
              </div>

              {/* Time Section */}
              <div className="flex items-start gap-3">
                <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-emerald-50 dark:bg-emerald-900/30">
                  <Clock3 className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                </div>
                <div className="min-w-0">
                  <div className="mb-1 text-[10px] font-bold uppercase tracking-wider text-gray-400 dark:text-gray-500">
                    Store Time
                  </div>
                  <div
                    className="truncate font-mono text-xs font-medium text-gray-900 dark:text-gray-300"
                  >
                    {formatTimestamp(msg.storeTimestamp)}
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Action Footer - Previous Style (Full Width Black Button) */}
          <div className="border-t border-gray-100 bg-gray-50/50 p-4 dark:border-gray-800 dark:bg-gray-800/50">
            <button
              onClick={() => setSelectedMessage(msg)}
              className="flex w-full items-center justify-center space-x-2 rounded-xl bg-gray-900 px-4 py-2.5 text-xs font-bold text-white shadow-sm transition-colors hover:bg-gray-800 active:scale-[0.98] dark:bg-blue-600 dark:text-white dark:hover:bg-blue-500"
            >
              <FileText className="h-3.5 w-3.5" />
              <span>View Details</span>
            </button>
          </div>
        </motion.div>
      ))}
    </div>
  );

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 pb-12 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <MessageDetailModal
        isOpen={!!selectedMessage}
        onClose={() => setSelectedMessage(null)}
        message={selectedMessage}
      />

      <div className="mb-8 flex justify-center">
        <div className="inline-flex rounded-xl bg-gray-100 p-1 shadow-inner dark:bg-gray-800">
          {(['Topic', 'Message Key', 'Message ID'] as MessageTab[]).map((tab) => (
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

      <div className="sticky top-0 z-20 flex flex-col justify-between gap-4 rounded-2xl border border-gray-100 bg-white/90 p-4 shadow-sm backdrop-blur-xl transition-colors dark:border-gray-800 dark:bg-gray-900/90 xl:flex-row xl:items-center">
        <div className="flex flex-1 flex-wrap items-center gap-4">
          {renderSearchArea()}
        </div>

        <div>
          <button
            onClick={() => void handleSearch()}
            disabled={isSearching || isTopicCatalogLoading}
            className="flex items-center rounded-xl bg-gray-900 px-6 py-2 text-sm font-medium text-white shadow-md transition-all hover:bg-gray-800 hover:shadow-lg active:scale-95 dark:border dark:border-gray-700 dark:bg-gray-900 dark:text-white dark:hover:bg-gray-800"
          >
            <Search className="mr-2 h-4 w-4" />
            {isSearching ? 'SEARCHING...' : 'SEARCH'}
          </button>
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
            {activeTab === 'Topic'
              ? 'Topic tab now follows the Java dashboard flow: dynamic time defaults plus real topic/time pagination with backend taskId continuity.'
              : activeTab === 'Message Key'
                ? 'Message Key uses the real query path and returns up to 64 messages, matching the Java dashboard behavior.'
                : 'Message ID now opens the real detail dialog directly, and list results are shown as the new card layout.'}
          </span>
        </div>
        <span className="font-mono text-xs text-gray-400 dark:text-gray-500">
          {activeTab === 'Topic'
            ? hasSearched
              ? `${messages.length} item(s) on page ${topicPagination.currentPage} / ${Math.max(topicPagination.totalPages, 1)}`
              : 'ready'
            : activeTab === 'Message ID'
              ? selectedMessage
                ? 'detail open'
                : 'ready'
              : hasSearched
                ? `${messages.length} result(s)`
                : 'ready'}
        </span>
      </div>

      {isSearching ? (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <Search className="mb-4 h-10 w-10 animate-pulse text-blue-500" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Querying messages...</p>
        </div>
      ) : messages.length > 0 ? (
        <>
          {renderMessageCards()}

          {activeTab === 'Topic' && topicPagination.totalPages > 1 ? (
            <div className="flex items-center justify-center pt-2">
              <Pagination
                currentPage={topicPagination.currentPage}
                totalPages={topicPagination.totalPages}
                onPageChange={(page) => void queryTopicPage(page)}
              />
            </div>
          ) : null}
        </>
      ) : hasSearched ? (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <Search className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">No matching messages were found</p>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">Check the current topic, time range, key, or message id.</p>
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
          <Search className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
          <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Search is ready</p>
          <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">{renderEmptyCopy()}</p>
        </div>
      )}
    </div>
  );
};
