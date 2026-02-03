import React, { useState } from 'react';
import { motion } from 'motion/react';
import { ChevronDown, Search, Database, Network } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Pagination } from './Pagination';

export const ProducerView = () => {
  const [topic, setTopic] = useState('TopicTest');
  const [group, setGroup] = useState('please_rename_unique_group_name');
  const [currentPage, setCurrentPage] = useState(1);
  const [clients, setClients] = useState([
      { clientId: '172.20.48.1@40124#19349891391300', clientAddr: '172.20.48.1:61266', language: 'JAVA', version: 'V5_4_0', status: 'Online' },
      { clientId: '172.20.48.1@40125#19349891391301', clientAddr: '172.20.48.1:61267', language: 'GO', version: 'V5_3_0', status: 'Online' },
      { clientId: '172.20.48.1@40126#19349891391302', clientAddr: '172.20.48.1:61268', language: 'RUST', version: 'V5_4_0', status: 'Offline' },
      { clientId: '172.20.48.1@40127#19349891391303', clientAddr: '172.20.48.1:61269', language: 'CPP', version: 'V5_2_0', status: 'Online' },
      { clientId: '172.20.48.1@40128#19349891391304', clientAddr: '172.20.48.1:61270', language: 'JAVA', version: 'V5_4_0', status: 'Online' },
      { clientId: '172.20.48.1@40129#19349891391305', clientAddr: '172.20.48.1:61271', language: 'PYTHON', version: 'V5_4_0', status: 'Online' },
      { clientId: '172.20.48.1@40130#19349891391306', clientAddr: '172.20.48.1:61272', language: 'JAVA', version: 'V5_4_0', status: 'Online' }
  ]);

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
                            value={topic}
                            onChange={(e) => setTopic(e.target.value)}
                            className="pl-3 pr-8 py-2 w-48 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer text-gray-700 dark:text-gray-300 font-medium hover:bg-white dark:hover:bg-gray-750"
                        >
                            <option>TopicTest</option>
                            <option>BenchmarkTest</option>
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
                          value={group}
                          onChange={(e) => setGroup(e.target.value)}
                        />
                    </div>
                </div>
             </div>

             {/* Actions */}
             <div>
                 <button 
                    onClick={() => toast.success("Searching...")}
                    className="flex items-center px-6 py-2 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                 >
                    <Search className="w-4 h-4 mr-2" />
                    SEARCH
                 </button>
             </div>
        </div>

        {/* Card Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
            {clients.map((client, index) => (
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
                             <div className={`px-2 py-1 rounded-md text-[10px] font-bold border uppercase tracking-wide ${
                                 client.status === 'Online' 
                                 ? 'bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-400 border-green-100 dark:border-green-800' 
                                 : 'bg-gray-50 dark:bg-gray-800 text-gray-500 dark:text-gray-400 border-gray-100 dark:border-gray-700'
                             }`}>
                                 {client.status}
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
                                 <p className="font-mono text-sm font-bold text-gray-700 dark:text-gray-300">{client.version}</p>
                             </div>
                         </div>
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
