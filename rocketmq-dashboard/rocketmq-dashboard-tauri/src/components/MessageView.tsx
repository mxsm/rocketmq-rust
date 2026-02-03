import React, { useState } from 'react';
import { motion } from 'motion/react';
import { 
  ChevronDown, 
  Calendar, 
  Search, 
  FileText, 
  Copy, 
  Tag, 
  Key, 
  Clock 
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { MessageDetailModal } from './MessageDetailModal';
import { Pagination } from './Pagination';
import { Input } from './ui/input';

export const MessageView = () => {
  const [activeTab, setActiveTab] = useState('Topic');
  const [topic, setTopic] = useState('TopicTest');
  const [msgKey, setMsgKey] = useState('');
  const [msgId, setMsgId] = useState('');
  const [startDate, setStartDate] = useState('2026-01-27 00:00:00');
  const [endDate, setEndDate] = useState('2026-01-28 00:00:00');
  const [selectedMessage, setSelectedMessage] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  
  // Mock Data
  const messages = Array.from({ length: 12 }).map((_, i) => ({
    msgId: `240E03B350D263C087AA69AB77E798719CBC18B4AAC28A91AB${100000 + i}`,
    tag: 'TagA',
    key: i % 3 === 0 ? `Key_${i}` : null,
    storeTime: '2026-01-27 21:46:42',
    topic: 'TopicTest'
  }));

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
                          <option>TopicTest</option>
                          <option>BenchmarkTest</option>
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
                          <option>TopicTest</option>
                          <option>BenchmarkTest</option>
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
                          <option>TopicTest</option>
                          <option>BenchmarkTest</option>
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
        {/* Detail Modal */}
        <MessageDetailModal 
          isOpen={!!selectedMessage} 
          onClose={() => setSelectedMessage(null)} 
          message={selectedMessage} 
        />

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
                    onClick={() => toast.success("Searching messages...")}
                    className="flex items-center px-6 py-2 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                 >
                    <Search className="w-4 h-4 mr-2" />
                    SEARCH
                 </button>
             </div>
        </div>

        {/* Card Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
            {messages.map((msg: any, index) => (
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
                                        onClick={(e) => { e.stopPropagation(); toast.success("Copied ID"); }}
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
                                    {msg.tag}
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
                                    <div className="font-mono text-xs font-medium text-gray-900 dark:text-gray-300 truncate" title={msg.key || '-'}>
                                        {msg.key || '-'}
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
                                    <div className="font-mono text-xs font-medium text-gray-900 dark:text-gray-300 truncate" title={msg.storeTime}>
                                        {msg.storeTime}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Action Footer - Previous Style (Full Width Black Button) */}
                    <div className="p-4 bg-gray-50/50 dark:bg-gray-800/50 border-t border-gray-100 dark:border-gray-800">
                        <button 
                            onClick={() => setSelectedMessage(msg)}
                            className="w-full flex items-center justify-center space-x-2 px-4 py-2.5 rounded-xl bg-gray-900 dark:bg-blue-600 text-white dark:text-white text-xs font-bold hover:bg-gray-800 dark:hover:bg-blue-500 transition-colors shadow-sm active:scale-[0.98]"
                        >
                            <FileText className="w-3.5 h-3.5" />
                            <span>View Details</span>
                        </button>
                    </div>
                </motion.div>
            ))}
        </div>

        <div className="flex items-center justify-center pt-6">
            <Pagination 
                currentPage={currentPage}
                totalPages={5} 
                onPageChange={setCurrentPage}
            />
        </div>
    </div>
  );
};
