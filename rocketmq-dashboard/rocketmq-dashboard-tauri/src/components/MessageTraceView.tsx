import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { 
  X, 
  Activity, 
  CheckCircle2, 
  ChevronDown, 
  MessageSquare, 
  Key, 
  Hash, 
  Search, 
  Clock 
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Pagination } from './Pagination';

const TraceDetailModal = ({ isOpen, onClose, traceData }: any) => {
  if (!isOpen || !traceData) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4 sm:p-6">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="absolute inset-0 bg-gray-900/30 backdrop-blur-sm transition-opacity"
        />
        
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-5xl bg-white dark:bg-gray-900 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
        >
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900 sticky top-0 z-10">
            <h2 className="text-lg font-bold text-gray-900 dark:text-white flex items-center gap-2">
              <Activity className="w-5 h-5 text-blue-500" />
              Message Trace Detail
            </h2>
            <button 
              onClick={onClose}
              className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Body */}
          <div className="flex-1 overflow-y-auto p-6 space-y-6 bg-gray-50/30 dark:bg-gray-950/30">
            
            {/* Timeline Visualization */}
            <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-6 shadow-sm">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-6 text-center">Message Trace: {traceData.topic}</h3>
                
                <div className="relative pt-8 pb-4 px-4">
                    {/* Time Axis */}
                    <div className="absolute top-0 left-0 right-0 flex justify-between text-[10px] text-gray-400 dark:text-gray-500 font-mono">
                        <span>0.00ms</span>
                        <span>25.00ms</span>
                        <span>50.00ms</span>
                        <span>75.00ms</span>
                        <span>100.00ms</span>
                        <span>125.00ms</span>
                        <span>150.00ms</span>
                    </div>

                    {/* Timeline Bar Container */}
                    <div className="relative h-24 bg-gray-50 dark:bg-gray-900/50 rounded-lg border border-dashed border-gray-200 dark:border-gray-700 w-full flex items-center px-2">
                        {/* The Trace Bar */}
                        <motion.div 
                            initial={{ width: 0 }}
                            animate={{ width: '80%' }}
                            transition={{ duration: 0.8, ease: "easeOut" }}
                            className="h-16 bg-green-400 dark:bg-green-600 rounded-md shadow-sm relative group cursor-pointer flex items-center justify-center overflow-hidden"
                        >
                            <div className="absolute inset-0 bg-white/10 opacity-0 group-hover:opacity-100 transition-opacity" />
                            <span className="text-xs font-bold text-green-900 dark:text-green-100 drop-shadow-sm">{traceData.costTime || '136ms'}</span>
                        </motion.div>

                        {/* Dashed Lines for grid feel */}
                        <div className="absolute inset-0 pointer-events-none flex justify-between px-2">
                             <div className="h-full w-px bg-gray-200/50 dark:bg-gray-700/50"></div>
                             <div className="h-full w-px bg-gray-200/50 dark:bg-gray-700/50"></div>
                             <div className="h-full w-px bg-gray-200/50 dark:bg-gray-700/50"></div>
                             <div className="h-full w-px bg-gray-200/50 dark:bg-gray-700/50"></div>
                             <div className="h-full w-px bg-gray-200/50 dark:bg-gray-700/50"></div>
                        </div>
                    </div>

                    <div className="mt-2 text-xs text-gray-400 dark:text-gray-500 text-center font-mono">
                        {traceData.producerGroup || 'producer_group_name'}
                    </div>
                </div>
            </div>

            {/* Detailed Properties */}
            <div className="space-y-3">
               <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Trace Info</h3>
               <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm overflow-hidden">
                   <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 divide-y md:divide-y-0 md:divide-x divide-gray-100 dark:divide-gray-700">
                        {/* Column 1 */}
                        <div className="divide-y divide-gray-100 dark:divide-gray-700">
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Topic</label>
                                <div className="font-mono text-sm font-semibold text-gray-900 dark:text-white break-all">{traceData.topic}</div>
                            </div>
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Producer Group</label>
                                <div className="font-mono text-sm text-gray-700 dark:text-gray-300 break-all">{traceData.producerGroup}</div>
                            </div>
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Cost Time</label>
                                <div className="font-mono text-sm font-bold text-green-600 dark:text-green-400">{traceData.costTime}</div>
                            </div>
                        </div>

                        {/* Column 2 */}
                        <div className="divide-y divide-gray-100 dark:divide-gray-700">
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Tag</label>
                                <div className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-purple-50 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 border border-purple-100 dark:border-purple-800">
                                    {traceData.tag}
                                </div>
                            </div>
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Message Key</label>
                                <div className="font-mono text-sm text-gray-700 dark:text-gray-300 break-all">{traceData.key}</div>
                            </div>
                             <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Status</label>
                                <div className="inline-flex items-center gap-1.5 text-sm font-medium text-gray-900 dark:text-white">
                                    <CheckCircle2 className="w-4 h-4 text-green-500" />
                                    Normal_Msg
                                </div>
                            </div>
                        </div>

                        {/* Column 3 */}
                        <div className="divide-y divide-gray-100 dark:divide-gray-700">
                             <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Born Time</label>
                                <div className="font-mono text-xs text-gray-700 dark:text-gray-300">{traceData.storeTime}</div>
                            </div>
                            <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Store Time</label>
                                <div className="font-mono text-xs text-gray-700 dark:text-gray-300">{traceData.storeTime}</div>
                            </div>
                             <div className="p-4">
                                <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Store Host</label>
                                <div className="font-mono text-xs text-gray-700 dark:text-gray-300">172.20.48.1:10911</div>
                            </div>
                        </div>
                   </div>
                   
                   {/* Full Width Footer Row for Message ID */}
                   <div className="border-t border-gray-100 dark:border-gray-700 p-4 bg-gray-50/50 dark:bg-gray-900/50">
                        <label className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider block mb-1">Message ID</label>
                        <div className="flex items-center justify-between font-mono text-xs text-gray-600 dark:text-gray-300 bg-white dark:bg-gray-800 p-2 rounded border border-gray-200 dark:border-gray-700">
                            {traceData.msgId}
                            <button className="text-blue-500 hover:text-blue-600 dark:text-blue-400 dark:hover:text-blue-300 font-sans text-[10px] font-medium" onClick={() => toast.success("Copied ID")}>COPY</button>
                        </div>
                   </div>
               </div>
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};

export const MessageTraceView = () => {
  const [subTab, setSubTab] = useState('MessageKey');
  const [traceTopic, setTraceTopic] = useState('RMQ_SYS_TRACE_TOPIC');
  const [topic, setTopic] = useState('TopicTest');
  const [key, setKey] = useState('KEY0');
  const [msgId, setMsgId] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [selectedTrace, setSelectedTrace] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);

  const handleSearch = () => {
     toast.success("Searching traces...");
     // Mock Data
     setTimeout(() => {
         setSearchResults([
             { 
                 msgId: '240E03B350D263C087AA69AB77E79871813818B4AAC28AB52E0E0000', 
                 tag: 'TagA', 
                 key: 'KEY0', 
                 storeTime: '2026-01-27 22:25:29', 
                 topic: topic,
                 costTime: '136ms',
                 producerGroup: 'please_rename_unique_group_name'
             },
             { 
                 msgId: '240E03B350D3A180958DA7182DD5640A76F018B4AAC28CE246E00000', 
                 tag: 'TagA', 
                 key: 'KEY0', 
                 storeTime: '2026-01-28 08:33:59',
                 topic: topic,
                 costTime: '45ms',
                 producerGroup: 'please_rename_unique_group_name'
             },
             { 
                 msgId: '240E03B350D3A180958DA7182DD5640A5C5018B4AAC28CE387B50000', 
                 tag: 'TagA', 
                 key: 'KEY0', 
                 storeTime: '2026-01-28 08:35:21',
                 topic: topic,
                 costTime: '82ms',
                 producerGroup: 'please_rename_unique_group_name'
             },
             { 
                 msgId: '240E03B350D3A180958DA7182DD5640ADAAFA018B4AAC28CF6387B0000', 
                 tag: 'TagA', 
                 key: 'KEY0', 
                 storeTime: '2026-01-28 08:55:47',
                 topic: topic,
                 costTime: '12ms',
                 producerGroup: 'please_rename_unique_group_name'
             },
         ]);
     }, 600);
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
        
        <TraceDetailModal 
            isOpen={!!selectedTrace} 
            onClose={() => setSelectedTrace(null)} 
            traceData={selectedTrace} 
        />

        {/* Global Configuration for View */}
        <div className="flex items-center justify-between mb-2 px-2">
            <div className="flex items-center space-x-3">
                 <div className="p-2 bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-lg">
                    <Activity className="w-5 h-5" />
                 </div>
                 <div>
                    <h2 className="text-lg font-bold text-gray-900 dark:text-white">Message Trace</h2>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Track message lifecycle and latency</p>
                 </div>
            </div>
            
            <div className="flex items-center gap-2">
                <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Trace Topic:</span>
                <div className="relative">
                    <select 
                        value={traceTopic}
                        onChange={(e) => setTraceTopic(e.target.value)}
                        className="pl-3 pr-8 py-1.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-xs font-mono font-medium text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer"
                    >
                        <option>RMQ_SYS_TRACE_TOPIC</option>
                    </select>
                    <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
                </div>
            </div>
        </div>

        {/* Navigation Tabs */}
        <div className="flex justify-center mb-6">
            <div className="bg-white dark:bg-gray-900 p-1 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm inline-flex transition-colors">
                <button
                    onClick={() => setSubTab('MessageKey')}
                    className={`px-6 py-2 rounded-lg text-sm font-medium transition-all ${
                        subTab === 'MessageKey'
                        ? 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400 shadow-sm'
                        : 'text-gray-500 hover:text-gray-900 hover:bg-gray-50 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-800'
                    }`}
                >
                    Message Key
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

        {/* Toolbar / Search */}
        <div className="bg-white dark:bg-gray-900 p-5 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
             <div className="flex flex-col xl:flex-row xl:items-end gap-5">
                 
                 {/* Topic Selector */}
                 <div className="space-y-1.5 flex-1 min-w-[200px]">
                    <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Topic</label>
                    <div className="relative group">
                        <MessageSquare className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                        <select 
                            value={topic}
                            onChange={(e) => setTopic(e.target.value)}
                            className="pl-10 pr-8 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer dark:text-white"
                        >
                            <option>TopicTest</option>
                            <option>BenchmarkTest</option>
                        </select>
                        <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
                    </div>
                 </div>

                 {subTab === 'MessageKey' ? (
                     <div className="space-y-1.5 flex-[2] min-w-[200px]">
                        <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Message Key</label>
                        <div className="relative group">
                            <Key className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                            <input 
                                type="text" 
                                placeholder="Enter Message Key..." 
                                className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all dark:text-white dark:placeholder:text-gray-500"
                                value={key}
                                onChange={(e) => setKey(e.target.value)}
                            />
                        </div>
                     </div>
                 ) : (
                     <div className="space-y-1.5 flex-[2] min-w-[300px]">
                        <label className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Message ID</label>
                        <div className="relative group">
                            <Hash className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                            <input 
                                type="text" 
                                placeholder="Enter Message ID..." 
                                className="pl-10 pr-4 py-2.5 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono dark:text-white dark:placeholder:text-gray-500"
                                value={msgId}
                                onChange={(e) => setMsgId(e.target.value)}
                            />
                        </div>
                     </div>
                 )}

                 <div className="flex items-center gap-3 pb-0.5">
                     <button 
                        onClick={handleSearch}
                        className="flex items-center px-8 py-2.5 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 whitespace-nowrap dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                     >
                        <Search className="w-4 h-4 mr-2" />
                        Search
                     </button>
                 </div>
             </div>
        </div>

        {/* Results Grid */}
        <div>
            {searchResults.length > 0 ? (
                <>
                <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
                    {searchResults.map((row, idx) => (
                        <motion.div
                            key={idx}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: idx * 0.05 }}
                            className="group bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm hover:shadow-md transition-all duration-200 overflow-hidden flex flex-col"
                        >
                            {/* Card Header */}
                            <div className="px-5 py-4 border-b border-gray-50 dark:border-gray-800 bg-gradient-to-br from-gray-50/50 to-white dark:from-gray-800/50 dark:to-gray-900 flex justify-between items-start">
                                <div className="flex items-center space-x-3 overflow-hidden">
                                    <div className="p-2 bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 rounded-lg shrink-0">
                                        <Hash className="w-4 h-4" />
                                    </div>
                                    <div className="min-w-0">
                                        <h3 className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-0.5">Message ID</h3>
                                        <p className="font-mono text-sm font-bold text-gray-900 dark:text-white truncate" title={row.msgId}>{row.msgId}</p>
                                    </div>
                                </div>
                            </div>

                            {/* Content */}
                            <div className="p-5 space-y-4 flex-1">
                                <div className="grid grid-cols-2 gap-4">
                                     <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                         <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Tag</p>
                                         <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-800">
                                            {row.tag}
                                         </span>
                                     </div>
                                     <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                         <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Key</p>
                                         <p className="font-mono text-xs font-medium text-gray-700 dark:text-gray-300 truncate">{row.key}</p>
                                     </div>
                                </div>
                                
                                <div className="flex items-center justify-between p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800">
                                     <div className="flex items-center text-xs text-gray-500 dark:text-gray-400">
                                         <Clock className="w-3.5 h-3.5 mr-1.5 text-gray-400 dark:text-gray-500" />
                                         {row.storeTime}
                                     </div>
                                </div>
                            </div>

                            {/* Footer */}
                            <div className="px-5 py-3 border-t border-gray-100 dark:border-gray-800 bg-gray-50/30 dark:bg-gray-800/30 flex justify-end">
                                <button 
                                    onClick={() => setSelectedTrace(row)}
                                    className="flex items-center px-3 py-1.5 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-600 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-all shadow-sm"
                                >
                                    <Activity className="w-3.5 h-3.5 mr-1.5" />
                                    Trace Detail
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
                </>
            ) : (
                <div className="flex flex-col items-center justify-center py-20 text-gray-400 dark:text-gray-500 bg-white dark:bg-gray-900 rounded-3xl border border-gray-100 dark:border-gray-800 border-dashed">
                    <div className="w-16 h-16 bg-gray-50 dark:bg-gray-800 rounded-full flex items-center justify-center mb-4">
                        <Search className="w-8 h-8 opacity-20" />
                    </div>
                    <p className="text-sm font-medium">No traces found</p>
                    <p className="text-xs opacity-60 mt-1">Enter a Message Key or ID to search</p>
                </div>
            )}
        </div>
    </div>
  );
};
