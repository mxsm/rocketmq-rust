import React, { useEffect, useMemo, useState } from 'react';
import { motion } from 'motion/react';
import {
  ChevronDown,
  Calendar,
  Search,
  FileText,
  Copy,
  Tag,
  Key,
  Clock,
  AlertCircle,
  Info,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Input } from './ui/input';
import { MessageService } from '../services/message.service';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import type { MessageSummary } from '../features/message/types/message.types';

export const MessageView = () => {
  const [activeTab, setActiveTab] = useState('Topic');
  const [topic, setTopic] = useState('');
  const [msgKey, setMsgKey] = useState('');
  const [msgId, setMsgId] = useState('');
  const [startDate, setStartDate] = useState('2026-01-27 00:00:00');
  const [endDate, setEndDate] = useState('2026-01-28 00:00:00');
  const [messages, setMessages] = useState<MessageSummary[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [hasSearched, setHasSearched] = useState(false);
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
    setSearchError('');
    setHasSearched(false);
  }, [activeTab]);

  const formatTimestamp = (value: number) => {
    if (!value) {
      return '-';
    }
    return new Date(value).toLocaleString();
  };

  const handleSearch = async () => {
    if (activeTab === 'Topic') {
      setMessages([]);
      setHasSearched(false);
      setSearchError('Topic + 时间范围分页查询属于后续 phase，本轮只接通了 Message Key / Message ID 的真实链路。');
      toast.message('Topic 查询将在后续阶段接入真实分页链路');
      return;
    }

    if (!topic.trim()) {
      setSearchError('请选择 Topic。');
      return;
    }

    if (activeTab === 'Message Key' && !msgKey.trim()) {
      setSearchError('请输入 Message Key。');
      return;
    }

    if (activeTab === 'Message ID' && !msgId.trim()) {
      setSearchError('请输入 Message ID。');
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
      setSearchError(error instanceof Error ? error.message : '消息查询失败');
    } finally {
      setIsSearching(false);
    }
  };
  
  const renderSearchArea = () => {
    switch (activeTab) {
      case 'Topic':
        return (
           <>
              <div className="flex items-center space-x-2">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Topic:</span>
                  <div className="relative group">
                      <select 
                          value={topic}
                          onChange={(e) => setTopic(e.target.value)}
                          className="pl-3 pr-8 py-2 w-48 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer text-gray-700 dark:text-gray-300 font-medium"
                      >
                          {availableTopics.length === 0 ? (
                            <option value="">
                              {isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}
                            </option>
                          ) : (
                            availableTopics.map((item) => (
                              <option key={item} value={item}>{item}</option>
                            ))
                          )}
                      </select>
                      <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                  </div>
              </div>
              <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2 hidden xl:block"></div>
              <div className="flex items-center space-x-2">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Begin:</span>
                  <div className="relative">
                      <Input 
                        value={startDate} 
                        onChange={(e) => setStartDate(e.target.value)}
                        className="w-48 font-mono text-xs bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white" 
                      />
                      <Calendar className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                  </div>
              </div>
              <div className="flex items-center space-x-2">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">End:</span>
                  <div className="relative">
                      <Input 
                        value={endDate} 
                        onChange={(e) => setEndDate(e.target.value)}
                        className="w-48 font-mono text-xs bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white" 
                      />
                      <Calendar className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                  </div>
              </div>
           </>
        );
      case 'Message Key':
        return (
          <>
             <div className="flex items-center space-x-2">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Topic:</span>
                  <div className="relative group">
                      <select 
                          value={topic}
                          onChange={(e) => setTopic(e.target.value)}
                          className="pl-3 pr-8 py-2 w-48 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer text-gray-700 dark:text-gray-300 font-medium"
                      >
                          {availableTopics.length === 0 ? (
                            <option value="">
                              {isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}
                            </option>
                          ) : (
                            availableTopics.map((item) => (
                              <option key={item} value={item}>{item}</option>
                            ))
                          )}
                      </select>
                      <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                  </div>
              </div>
              <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2 hidden md:block"></div>
              <div className="flex items-center space-x-2 flex-1">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap">Key:</span>
                  <Input 
                    value={msgKey} 
                    onChange={(e) => setMsgKey(e.target.value)}
                    placeholder="Enter Message Key..."
                    className="flex-1 min-w-[200px] bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white placeholder:text-gray-500" 
                  />
              </div>
              <div className="text-xs text-orange-500 dark:text-orange-400 font-medium bg-orange-50 dark:bg-orange-900/20 px-2 py-1 rounded border border-orange-100 dark:border-orange-900/30 whitespace-nowrap">
                Only return 64 messages
              </div>
          </>
        );
      case 'Message ID':
        return (
          <>
             <div className="flex items-center space-x-2">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Topic:</span>
                  <div className="relative group">
                      <select 
                          value={topic}
                          onChange={(e) => setTopic(e.target.value)}
                          className="pl-3 pr-8 py-2 w-48 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer text-gray-700 dark:text-gray-300 font-medium"
                      >
                          {availableTopics.length === 0 ? (
                            <option value="">
                              {isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}
                            </option>
                          ) : (
                            availableTopics.map((item) => (
                              <option key={item} value={item}>{item}</option>
                            ))
                          )}
                      </select>
                      <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                  </div>
              </div>
              <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2 hidden md:block"></div>
              <div className="flex items-center space-x-2 flex-1">
                  <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap">Message ID:</span>
                  <Input 
                    value={msgId} 
                    onChange={(e) => setMsgId(e.target.value)}
                    placeholder="Enter Message ID..."
                    className="flex-1 min-w-[300px] font-mono bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-900 dark:text-white placeholder:text-gray-500" 
                  />
              </div>
          </>
        );
      default: return null;
    }
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
        {/* Header Tabs */}
        <div className="flex justify-center mb-8">
           <div className="bg-gray-100 dark:bg-gray-800 p-1 rounded-xl inline-flex shadow-inner">
              {['Topic', 'Message Key', 'Message ID'].map(tab => (
                 <button
                    key={tab}
                    onClick={() => setActiveTab(tab)}
                    className={`px-6 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                       activeTab === tab 
                       ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-white shadow-sm' 
                       : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 hover:bg-gray-200/50 dark:hover:bg-gray-700/50'
                    }`}
                 >
                    {tab}
                 </button>
              ))}
           </div>
        </div>

        {/* Toolbar */}
        <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-4 bg-white dark:bg-gray-900 p-4 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
             {/* Search Inputs */}
             <div className="flex flex-wrap items-center gap-4 flex-1">
                {renderSearchArea()}
             </div>

             {/* Actions */}
             <div>
                 <button 
                    onClick={() => void handleSearch()}
                    disabled={isSearching || isTopicCatalogLoading}
                    className="flex items-center px-6 py-2 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                 >
                    <Search className="w-4 h-4 mr-2" />
                    {isSearching ? 'SEARCHING...' : 'SEARCH'}
                 </button>
             </div>
        </div>

        {topicCatalogError && (
          <div className="flex items-start gap-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <div>Topic 列表加载失败：{topicCatalogError}</div>
          </div>
        )}

        {searchError && (
          <div className="flex items-start gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <div>{searchError}</div>
          </div>
        )}

        {activeTab !== 'Topic' && (
          <div className="flex items-center justify-between rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-600 shadow-sm dark:border-gray-800 dark:bg-gray-900 dark:text-gray-300">
            <div className="flex items-center gap-2">
              <Info className="h-4 w-4 text-blue-500" />
              <span>本轮已接入真实查询链路，详情弹窗与 Topic 分页将在后续 phase 继续迁移。</span>
            </div>
            <span className="font-mono text-xs text-gray-400 dark:text-gray-500">
              {hasSearched ? `${messages.length} result(s)` : 'ready'}
            </span>
          </div>
        )}

        {/* Card Grid */}
        {activeTab === 'Topic' ? (
          <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
            <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-blue-50 text-blue-600 dark:bg-blue-900/20 dark:text-blue-300">
              <Info className="h-7 w-7" />
            </div>
            <p className="text-lg font-semibold text-gray-900 dark:text-white">Topic 时间分页查询尚未接入</p>
            <p className="mt-2 max-w-2xl text-sm text-gray-500 dark:text-gray-400">
              当前批次先保证 `Message Key / Message ID` 的真实闭环，避免在 Topic 分页、taskId 缓存和详情同时改动时把整个页面搞坏。
            </p>
          </div>
        ) : isSearching ? (
          <div className="flex flex-col items-center justify-center rounded-[28px] border border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
            <Search className="mb-4 h-10 w-10 animate-pulse text-blue-500" />
            <p className="text-sm font-medium text-gray-700 dark:text-gray-200">正在查询消息...</p>
          </div>
        ) : messages.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
            {messages.map((msg, index) => (
                <motion.div
                    key={msg.msgId}
                    initial={{ opacity: 0, y: 20, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    whileHover={{ 
                      y: -4, 
                      boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.05), 0 8px 10px -6px rgba(0, 0, 0, 0.01)" 
                    }}
                    transition={{ 
                      duration: 0.3, 
                      delay: index * 0.05,
                      ease: [0.22, 1, 0.36, 1]
                    }}
                    className="group bg-white dark:bg-gray-900 rounded-[20px] border border-gray-100 dark:border-gray-800 shadow-sm hover:border-blue-200 dark:hover:border-blue-800 transition-all duration-300 flex flex-col overflow-hidden"
                >
                    {/* Header - Message ID Focused */}
                    <div className="p-5 bg-gradient-to-br from-gray-50/80 to-white dark:from-gray-800/80 dark:to-gray-900 border-b border-gray-100 dark:border-gray-800">
                        <div className="flex items-center gap-3 mb-3">
                            <div className="h-10 w-10 rounded-xl bg-white dark:bg-gray-800 border border-gray-100 dark:border-gray-700 shadow-sm flex items-center justify-center shrink-0">
                                <FileText className="w-5 h-5 text-blue-600 dark:text-blue-500" />
                            </div>
                            <div className="min-w-0 flex-1">
                                <div className="flex items-center justify-between">
                                <span className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-0.5">Message ID</span>
                                    <button 
                                        onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(msg.msgId); toast.success("Copied ID"); }}
                                        className="text-gray-300 dark:text-gray-600 hover:text-blue-600 dark:hover:text-blue-400 transition-colors p-1"
                                    >
                                        <Copy className="w-3.5 h-3.5" />
                                    </button>
                                </div>
                                <h3 className="font-mono text-xs font-bold text-gray-900 dark:text-white truncate" title={msg.msgId}>
                                    {msg.msgId}
                                </h3>
                            </div>
                        </div>
                    </div>

                    {/* Body Content */}
                    <div className="p-5 flex-1 space-y-4">
                        {/* Tag Section */}
                        <div className="flex items-start gap-3">
                            <div className="mt-0.5 w-8 h-8 rounded-lg bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center shrink-0">
                                <Tag className="w-4 h-4 text-blue-600 dark:text-blue-400" />
                            </div>
                            <div>
                                <div className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1">Tag</div>
                                <span className="inline-flex items-center px-2.5 py-1 rounded-md bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs font-bold text-gray-700 dark:text-gray-300 shadow-sm">
                                    {msg.tags || '-'}
                                </span>
                            </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            {/* Key Section */}
                            <div className="flex items-start gap-3">
                                <div className="mt-0.5 w-8 h-8 rounded-lg bg-amber-50 dark:bg-amber-900/30 flex items-center justify-center shrink-0">
                                    <Key className="w-4 h-4 text-amber-600 dark:text-amber-400" />
                                </div>
                                <div className="min-w-0">
                                    <div className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1">Key</div>
                                    <div className="font-mono text-xs font-medium text-gray-900 dark:text-gray-300 truncate" title={msg.keys || '-'}>
                                        {msg.keys || '-'}
                                    </div>
                                </div>
                            </div>

                            {/* Time Section */}
                            <div className="flex items-start gap-3">
                                <div className="mt-0.5 w-8 h-8 rounded-lg bg-emerald-50 dark:bg-emerald-900/30 flex items-center justify-center shrink-0">
                                    <Clock className="w-4 h-4 text-emerald-600 dark:text-emerald-400" />
                                </div>
                                <div className="min-w-0">
                                    <div className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1">Store Time</div>
                                    <div className="font-mono text-xs font-medium text-gray-900 dark:text-gray-300 truncate" title={formatTimestamp(msg.storeTimestamp)}>
                                        {formatTimestamp(msg.storeTimestamp)}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Action Footer - Previous Style (Full Width Black Button) */}
                    <div className="p-4 bg-gray-50/50 dark:bg-gray-800/50 border-t border-gray-100 dark:border-gray-800">
                        <button 
                            disabled
                            className="w-full flex items-center justify-center space-x-2 px-4 py-2.5 rounded-xl bg-gray-300 text-white text-xs font-bold transition-colors shadow-sm cursor-not-allowed dark:bg-gray-700 dark:text-gray-300"
                        >
                            <FileText className="w-3.5 h-3.5" />
                            <span>Detail In Phase 2</span>
                        </button>
                    </div>
                </motion.div>
            ))}
          </div>
        ) : hasSearched ? (
          <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
            <Search className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
            <p className="text-sm font-medium text-gray-700 dark:text-gray-200">没有查到匹配消息</p>
            <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">请检查 Topic 与 Key / Message ID 是否匹配。</p>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center rounded-[28px] border border-dashed border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
            <Search className="mb-4 h-10 w-10 text-gray-300 dark:text-gray-600" />
            <p className="text-sm font-medium text-gray-700 dark:text-gray-200">输入查询条件后开始检索</p>
            <p className="mt-2 text-sm text-gray-500 dark:text-gray-400">当前批次已接通真实的 Message Key / Message ID 查询。</p>
          </div>
        )}
    </div>
  );
};
