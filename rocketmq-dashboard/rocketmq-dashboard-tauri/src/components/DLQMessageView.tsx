import React, { useState, useEffect } from 'react';
import { motion } from 'motion/react';
import { 
  AlertTriangle, 
  RefreshCw, 
  Search, 
  Users, 
  Calendar, 
  Hash, 
  Send, 
  ArrowUpRight, 
  Check, 
  RotateCcw, 
  FileText, 
  Clock 
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { MessageDetailModal } from './MessageDetailModal';
import { Pagination } from './Pagination';

export const DLQMessageView = () => {
  const [subTab, setSubTab] = useState('Consumer');
  const [consumerGroup, setConsumerGroup] = useState('please_rename_unique_group_name_4');
  const [messageId, setMessageId] = useState('');
  const [beginTime, setBeginTime] = useState('2026-01-28 00:00:00');
  const [endTime, setEndTime] = useState('2108-11-04 00:00:00');
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedMessage, setSelectedMessage] = useState(null);
  
  // Mock Data
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  // Initialize with some data for demonstration
  useEffect(() => {
    handleSearch();
  }, []);

  const handleSearch = () => {
    setIsLoading(true);
    // Simulate API call
    setTimeout(() => {
        setSearchResults([
            { msgId: '240E03B350D3A180958DA7182DD5640AA95418B4AAC28D12B6B300C2', tag: 'TagA', key: '-', storeTime: '2026-01-28 09:26:58' },
            { msgId: '240E03B350D3A180958DA7182DD5640AA95418B4AAC28D12B6B600C4', tag: 'TagA', key: 'Order_123', storeTime: '2026-01-28 09:26:58' },
            { msgId: '240E03B350D3A180958DA7182DD5640AA95418B4AAC28D12B6B600C6', tag: 'TagA', key: '-', storeTime: '2026-01-28 09:26:58' },
            { msgId: '240E03B350D3A180958DA7182DD5640AA95418B4AAC28D12B6B600C8', tag: 'TagB', key: 'Payment_999', storeTime: '2026-01-28 09:28:12' },
            { msgId: '240E03B350D3A180958DA7182DD5640AA95418B4AAC28D12B6B600D1', tag: 'TagC', key: '-', storeTime: '2026-01-28 09:30:45' },
        ]);
        setIsLoading(false);
        toast.success("Search completed");
    }, 600);
  };

  const toggleSelection = (id: any) => {
    const newSelection = new Set(selectedIds);
    if (newSelection.has(id)) {
        newSelection.delete(id);
    } else {
        newSelection.add(id);
    }
    setSelectedIds(newSelection);
  };

  const toggleAll = () => {
    if (selectedIds.size === searchResults.length) {
        setSelectedIds(new Set());
    } else {
        setSelectedIds(new Set(searchResults.map(r => r.msgId)));
    }
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
        <MessageDetailModal 
            isOpen={!!selectedMessage} 
            onClose={() => setSelectedMessage(null)} 
            message={selectedMessage} 
        />
        
        {/* Navigation Tabs */}
        <div className="flex justify-center mb-6">
            <div className="bg-white dark:bg-gray-900 p-1 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm inline-flex transition-colors">
                <button
                    onClick={() => setSubTab('Consumer')}
                    className={`px-6 py-2 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'Consumer'
                        ? 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400 shadow-sm'
                        : 'text-gray-500 hover:text-gray-900 hover:bg-gray-50 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-800'
                    }`}
                >
                    Consumer
                </button>
                <button
                    onClick={() => setSubTab('MessageID')}
                    className={`px-6 py-2 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'MessageID'
                        ? 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400 shadow-sm'
                        : 'text-gray-500 hover:text-gray-900 hover:bg-gray-50 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-800'
                    }`}
                >
                    Message ID
                </button>
            </div>
        </div>

        {/* Filters / Toolbar */}
        <div className="bg-white dark:bg-gray-900 p-5 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
             <div className="flex flex-col xl:flex-row xl:items-end gap-5">
                 
                 {/* Consumer Group Input */}
                 <div className="space-y-1.5 flex-1 min-w-[200px]">
                    <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Consumer Group</label>
                    <div className="relative group">
                        <Users className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                        <input 
                            type="text" 
                            placeholder="Select or enter consumer group..." 
                            className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono dark:text-white dark:placeholder:text-gray-500"
                            value={consumerGroup}
                            onChange={(e) => setConsumerGroup(e.target.value)}
                        />
                    </div>
                 </div>

                 {subTab === 'Consumer' ? (
                     <>
                        <div className="space-y-1.5 flex-1 min-w-[200px]">
                            <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Begin Time</label>
                            <div className="relative group">
                                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                                <input 
                                    type="text" 
                                    className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono dark:text-white"
                                    value={beginTime}
                                    onChange={(e) => setBeginTime(e.target.value)}
                                />
                            </div>
                        </div>
                        <div className="space-y-1.5 flex-1 min-w-[200px]">
                            <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">End Time</label>
                            <div className="relative group">
                                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                                <input 
                                    type="text" 
                                    className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono dark:text-white"
                                    value={endTime}
                                    onChange={(e) => setEndTime(e.target.value)}
                                />
                            </div>
                        </div>
                     </>
                 ) : (
                     <div className="space-y-1.5 flex-[2] min-w-[300px]">
                        <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Message ID</label>
                        <div className="relative group">
                            <Hash className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                            <input 
                                type="text" 
                                placeholder="Enter Message ID..." 
                                className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono dark:text-white dark:placeholder:text-gray-500"
                                value={messageId}
                                onChange={(e) => setMessageId(e.target.value)}
                            />
                        </div>
                     </div>
                 )}

                 <div className="flex items-center gap-3 pb-0.5">
                     <button 
                        onClick={handleSearch}
                        className="flex items-center px-6 py-2.5 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 whitespace-nowrap dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                     >
                        {isLoading ? <RefreshCw className="w-4 h-4 animate-spin mr-2" /> : <Search className="w-4 h-4 mr-2" />}
                        Search
                     </button>
                     
                     {subTab === 'Consumer' && (
                         <>
                            <button className="flex items-center px-4 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 rounded-xl text-sm font-medium hover:bg-gray-50 dark:hover:bg-gray-700 hover:border-gray-300 dark:hover:border-gray-600 transition-all shadow-sm active:scale-95 whitespace-nowrap">
                                <Send className="w-4 h-4 mr-2" />
                                Batch Resend
                            </button>
                            <button className="flex items-center px-4 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 rounded-xl text-sm font-medium hover:bg-gray-50 dark:hover:bg-gray-700 hover:border-gray-300 dark:hover:border-gray-600 transition-all shadow-sm active:scale-95 whitespace-nowrap">
                                <ArrowUpRight className="w-4 h-4 mr-2" />
                                Batch Export
                            </button>
                         </>
                     )}
                 </div>
             </div>
        </div>

        {/* Select Toggle */}
        {searchResults.length > 0 && subTab === 'Consumer' && (
            <div className="flex items-center px-4">
                 <button 
                    onClick={toggleAll}
                    className="flex items-center space-x-2 text-sm text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200 transition-colors"
                 >
                     <div className={`w-5 h-5 rounded border flex items-center justify-center transition-colors ${selectedIds.size === searchResults.length && searchResults.length > 0 ? 'bg-blue-500 border-blue-500' : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800'}`}>
                        {selectedIds.size === searchResults.length && searchResults.length > 0 && <Check className="w-3.5 h-3.5 text-white" />}
                     </div>
                     <span className="font-medium">Select All ({searchResults.length})</span>
                 </button>
                 <span className="mx-3 text-gray-300 dark:text-gray-700">|</span>
                 <span className="text-sm text-gray-500 dark:text-gray-400">{selectedIds.size} selected</span>
            </div>
        )}

        {/* Results Grid */}
        <div>
            {searchResults.length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-2 2xl:grid-cols-3 gap-6">
                    {searchResults.map((row, idx) => {
                        const isSelected = selectedIds.has(row.msgId);
                        return (
                            <motion.div
                                key={row.msgId}
                                initial={{ opacity: 0, y: 20 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ delay: idx * 0.05 }}
                                className={`group bg-white dark:bg-gray-900 rounded-2xl border shadow-sm hover:shadow-md transition-all duration-200 overflow-hidden flex flex-col ${isSelected ? 'ring-2 ring-blue-500 border-transparent' : 'border-gray-100 dark:border-gray-800 hover:border-blue-200 dark:hover:border-blue-800'}`}
                            >
                                {/* Header */}
                                <div className="px-5 py-4 border-b border-gray-50 dark:border-gray-800 bg-gradient-to-br from-gray-50/50 to-white dark:from-gray-800/50 dark:to-gray-900 flex items-start gap-3">
                                    {subTab === 'Consumer' && (
                                        <div 
                                            onClick={() => toggleSelection(row.msgId)}
                                            className={`mt-1 w-5 h-5 rounded border flex shrink-0 items-center justify-center cursor-pointer transition-colors ${isSelected ? 'bg-blue-500 border-blue-500' : 'border-gray-300 dark:border-gray-600 hover:border-blue-400 bg-white dark:bg-gray-800'}`}
                                        >
                                            {isSelected && <Check className="w-3.5 h-3.5 text-white" />}
                                        </div>
                                    )}
                                    <div className="min-w-0 flex-1">
                                        <div className="flex items-center justify-between mb-1">
                                            <h3 className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Message ID</h3>
                                        </div>
                                        <p className="font-mono text-sm font-bold text-gray-900 dark:text-white break-all leading-tight">{row.msgId}</p>
                                    </div>
                                </div>

                                {/* Content */}
                                <div className="p-5 space-y-4 flex-1">
                                    <div className="grid grid-cols-2 gap-4">
                                        {/* Tag */}
                                        <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                            <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Tag</p>
                                            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800">
                                                {row.tag || 'None'}
                                            </span>
                                        </div>
                                        {/* Key */}
                                        <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                            <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Key</p>
                                            <p className="font-mono text-xs font-medium text-gray-700 dark:text-gray-300 truncate" title={row.key}>{row.key}</p>
                                        </div>
                                    </div>
                                    
                                    {/* Store Time */}
                                    <div className="flex items-center p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                        <Clock className="w-4 h-4 text-gray-400 dark:text-gray-500 mr-2.5" />
                                        <div>
                                            <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold leading-none mb-0.5">Store Time</p>
                                            <p className="text-sm font-medium text-gray-900 dark:text-white font-mono">{row.storeTime}</p>
                                        </div>
                                    </div>
                                </div>

                                {/* Actions Footer */}
                                <div className="px-5 py-3 border-t border-gray-100 dark:border-gray-800 bg-gray-50/30 dark:bg-gray-800/30 flex items-center gap-2">
                                    <button 
                                        className="flex-1 flex items-center justify-center px-3 py-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-600 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-all shadow-sm"
                                        onClick={() => setSelectedMessage(row)}
                                    >
                                        <FileText className="w-3.5 h-3.5 mr-1.5" />
                                        Detail
                                    </button>
                                    <button 
                                        className="flex-1 flex items-center justify-center px-3 py-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-600 dark:text-gray-300 hover:text-green-600 dark:hover:text-green-400 hover:border-green-200 dark:hover:border-green-800 hover:bg-green-50 dark:hover:bg-green-900/20 transition-all shadow-sm"
                                        onClick={() => toast.success("Message Resent")}
                                    >
                                        <RotateCcw className="w-3.5 h-3.5 mr-1.5" />
                                        Resend
                                    </button>
                                    <button 
                                        className="flex-1 flex items-center justify-center px-3 py-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-600 dark:text-gray-300 hover:text-indigo-600 dark:hover:text-indigo-400 hover:border-indigo-200 dark:hover:border-indigo-800 hover:bg-indigo-50 dark:hover:bg-indigo-900/20 transition-all shadow-sm"
                                        onClick={() => toast.success("Exported")}
                                    >
                                        <ArrowUpRight className="w-3.5 h-3.5 mr-1.5" />
                                        Export
                                    </button>
                                </div>
                            </motion.div>
                        );
                    })}
                </div>
            ) : (
                <div className="flex flex-col items-center justify-center py-20 text-gray-400 dark:text-gray-500 bg-white dark:bg-gray-900 rounded-3xl border border-gray-100 dark:border-gray-800 border-dashed">
                    <div className="w-16 h-16 bg-gray-50 dark:bg-gray-800 rounded-full flex items-center justify-center mb-4">
                        <AlertTriangle className="w-8 h-8 opacity-20 text-red-500" />
                    </div>
                    <p className="text-sm font-medium">No DLQ messages found</p>
                    <p className="text-xs opacity-60 mt-1">Try adjusting the time range or consumer group</p>
                </div>
            )}
            
             {/* Pagination (Mock) */}
             {searchResults.length > 0 && (
                <div className="flex items-center justify-center mt-6">
                    <Pagination
                        currentPage={currentPage}
                        totalPages={16}
                        onPageChange={setCurrentPage}
                    />
                </div>
             )}
        </div>
    </div>
  );
};
