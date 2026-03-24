import React, { useMemo, useState } from 'react';
import { motion } from 'motion/react';
import { ChevronDown, Search, Database, Network, AlertCircle, LoaderCircle } from 'lucide-react';
import { Pagination } from './Pagination';
import { useProducerConnections } from '../features/producer/hooks/useProducerConnections';

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

  const clients = result?.connections ?? [];
  const totalPages = Math.max(1, Math.ceil(clients.length / PAGE_SIZE));
  const pagedClients = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE;
    return clients.slice(start, start + PAGE_SIZE);
  }, [clients, currentPage]);

  const handleSearch = async () => {
    const ok = await search();
    if (ok) {
      setCurrentPage(1);
    }
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
        {/* Toolbar */}
        <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-4 bg-white dark:bg-gray-900 p-4 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
             {/* Search Inputs */}
             <div className="flex flex-wrap items-center gap-4 flex-1">
                <div className="flex items-center space-x-2">
                    <span className="text-xs font-bold text-red-500 uppercase tracking-wider">* TOPIC</span>
                    <div className="relative group">
                        <select 
                            value={selectedTopic}
                            onChange={(e) => setSelectedTopic(e.target.value)}
                            disabled={isTopicLoading || topicOptions.length === 0}
                            className="pl-3 pr-8 py-2 w-48 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer text-gray-700 dark:text-gray-300 font-medium hover:bg-white dark:hover:bg-gray-750"
                        >
                            {isTopicLoading && <option>Loading topics...</option>}
                            {!isTopicLoading && topicOptions.length === 0 && <option>No topics</option>}
                            {!isTopicLoading &&
                                topicOptions.map((topic) => (
                                    <option key={topic} value={topic}>
                                        {topic}
                                    </option>
                                ))}
                        </select>
                        <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none" />
                    </div>
                </div>

                <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2 hidden md:block"></div>

                <div className="flex items-center space-x-2 flex-1">
                     <span className="text-xs font-bold text-red-500 uppercase tracking-wider whitespace-nowrap">* PRODUCER_GROUP</span>
                     <div className="relative group flex-1 max-w-md">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                        <input 
                          type="text" 
                          placeholder="Please enter producer group..." 
                          className="pl-10 pr-4 py-2 w-full bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all dark:text-white dark:placeholder:text-gray-500"
                          value={producerGroup}
                          onChange={(e) => setProducerGroup(e.target.value)}
                        />
                    </div>
                </div>
             </div>

             {/* Actions */}
             <div>
                 <button 
                    onClick={() => void handleSearch()}
                    disabled={isSearchPending || isTopicLoading}
                    className="flex items-center px-6 py-2 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 disabled:opacity-60 disabled:cursor-not-allowed dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                 >
                    {isSearching ? (
                        <LoaderCircle className="w-4 h-4 mr-2 animate-spin" />
                    ) : (
                        <Search className={`w-4 h-4 mr-2 transition-transform duration-300 ${isSearchPending ? 'rotate-12 scale-110' : ''}`} />
                    )}
                    {isSearching ? 'SEARCHING...' : 'SEARCH'}
                 </button>
             </div>
        </div>

        {error && (
            <div className="flex items-start gap-3 rounded-2xl border border-red-200/70 bg-red-50/80 px-4 py-3 text-sm text-red-700 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>{error}</span>
            </div>
        )}

        {result && !error && (
            <div className="flex flex-wrap items-center gap-3 rounded-2xl border border-gray-100 bg-white/80 px-4 py-3 text-sm text-gray-600 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-300">
                <span className="font-semibold text-gray-900 dark:text-white">Topic:</span>
                <span className="font-mono">{result.topic}</span>
                <span className="text-gray-300 dark:text-gray-600">/</span>
                <span className="font-semibold text-gray-900 dark:text-white">Producer Group:</span>
                <span className="font-mono">{result.producerGroup}</span>
                <span className="ml-auto rounded-full bg-blue-50 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">
                    {result.connectionCount} connection{result.connectionCount === 1 ? '' : 's'}
                </span>
            </div>
        )}

        {/* Card Grid */}
        {hasSearched && !isSearching && clients.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-gray-200 bg-white/70 px-6 py-16 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/70 dark:text-gray-400">
                No active producer connections were found for this topic and producer group.
            </div>
        ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
            {pagedClients.map((client, index) => (
                <motion.div
                    key={client.clientId}
                    initial={{ opacity: 0, y: 20, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    whileHover={{ 
                      y: -5, 
                      scale: 1.01,
                      boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.05), 0 8px 10px -6px rgba(0, 0, 0, 0.01)" 
                    }}
                    transition={{ 
                      duration: 0.3, 
                      delay: index * 0.05,
                      ease: [0.22, 1, 0.36, 1]
                    }}
                    className="group bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm transition-all duration-200 overflow-hidden flex flex-col cursor-default"
                >
                    {/* Card Header */}
                    <div className="p-5 border-b border-gray-50 dark:border-gray-800 bg-gradient-to-br from-gray-50/50 to-white dark:from-gray-800/50 dark:to-gray-900">
                        <div className="flex justify-between items-start mb-2">
                             <div className="flex items-center space-x-2">
                                <div className="p-1.5 bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-lg">
                                    <Database className="w-4 h-4" />
                                </div>
                                <span className="text-xs font-bold text-blue-600 dark:text-blue-400 uppercase tracking-wide">Client</span>
                             </div>
                             <div className="px-2 py-1 rounded-md text-[10px] font-bold border uppercase tracking-wide bg-gray-50 dark:bg-gray-800 text-gray-500 dark:text-gray-400 border-gray-100 dark:border-gray-700">
                                 {client.versionDesc}
                             </div>
                        </div>
                        <h3 className="font-mono text-sm font-semibold text-gray-900 dark:text-white break-all leading-relaxed mt-2" title={client.clientId}>
                            {client.clientId}
                        </h3>
                    </div>

                    {/* Content */}
                    <div className="p-5 space-y-4 bg-white dark:bg-gray-900 flex-1">
                         <div className="flex items-center justify-between p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-blue-100 dark:group-hover:border-blue-900/50 transition-colors">
                             <div className="flex items-center space-x-3">
                                 <div className="p-2 bg-white dark:bg-gray-800 rounded-lg shadow-sm text-gray-400 dark:text-gray-500">
                                     <Network className="w-4 h-4" />
                                 </div>
                                 <div>
                                     <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold">Client Address</p>
                                     <p className="font-mono text-sm font-medium text-gray-900 dark:text-gray-300">{client.clientAddr}</p>
                                 </div>
                             </div>
                         </div>
                         
                         <div className="grid grid-cols-2 gap-4">
                             <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-purple-100 dark:group-hover:border-purple-900/50 transition-colors">
                                 <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Language</p>
                                 <div className="flex items-center">
                                     <span className="inline-flex items-center px-2 py-1 rounded text-xs font-bold bg-orange-50 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400 border border-orange-100 dark:border-orange-800">
                                        {client.language}
                                     </span>
                                 </div>
                             </div>
                             <div className="p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-teal-100 dark:group-hover:border-teal-900/50 transition-colors">
                                 <p className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Version</p>
                                 <p className="font-mono text-sm font-bold text-gray-700 dark:text-gray-300">{client.versionDesc}</p>
                             </div>
                         </div>
                    </div>
                </motion.div>
            ))}
        </div>
        )}

        <div className="flex items-center justify-center pt-6">
            <Pagination 
                currentPage={currentPage}
                totalPages={totalPages} 
                onPageChange={setCurrentPage}
            />
        </div>
    </div>
  );
};
