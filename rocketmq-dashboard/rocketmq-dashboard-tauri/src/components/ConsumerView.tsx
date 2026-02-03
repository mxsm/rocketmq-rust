import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { 
  Users, 
  X, 
  Settings, 
  RotateCcw, 
  Trash2, 
  Search, 
  Check, 
  RefreshCw, 
  Plus, 
  Clock, 
  FileText, 
  Activity, 
  Network, 
  ListChecks, 
  Hash, 
  Cpu, 
  Layers, 
  FileBox 
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Pagination } from './Pagination';

const ConsumerDetailModal = ({ isOpen, onClose, consumer }: any) => {
  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 0.3 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="absolute inset-0 bg-black"
        />
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 10 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 10 }}
          className="relative w-full max-w-5xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-xl shadow-2xl flex flex-col overflow-hidden border border-gray-100 dark:border-gray-800"
        >
          {/* Header */}
          <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10 shrink-0">
            <div>
              <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                <FileText className="w-5 h-5 mr-2 text-blue-500" />
                Consumer Details
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1 flex items-center">
                <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{consumer?.group}</span>
                <span className="mx-2 text-gray-300 dark:text-gray-600">|</span>
                <span>Address: 172.20.48.1:10911</span>
              </p>
            </div>
            <button 
              onClick={onClose}
              className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-6 bg-gray-50/50 dark:bg-gray-950/50 space-y-6">
            {/* Topic Section 1: RETRY */}
            <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                <div className="px-6 py-4 bg-gray-50/50 dark:bg-gray-800/50 border-b border-gray-200 dark:border-gray-800 flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                        <div className="p-1.5 bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-lg">
                           <Layers className="w-4 h-4" />
                        </div>
                        <span className="font-mono text-sm font-semibold text-gray-700 dark:text-gray-300">%RETRY%{consumer?.group}</span>
                    </div>
                    <div className="flex items-center space-x-6 text-xs font-mono">
                        <div className="flex items-center text-gray-500 dark:text-gray-400">
                            <span className="uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500 mr-2">Total Lag:</span>
                            <span className="text-gray-900 dark:text-white font-bold bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 px-2 py-0.5 rounded">0</span>
                        </div>
                        <div className="flex items-center text-gray-500 dark:text-gray-400">
                            <span className="uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500 mr-2">Last Consume:</span>
                            <span className="text-gray-900 dark:text-white">-</span>
                        </div>
                    </div>
                </div>
                
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="bg-gray-50 dark:bg-gray-800/50 text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-100 dark:border-gray-800">
                                <th className="px-6 py-3 font-medium">Broker</th>
                                <th className="px-6 py-3 font-medium">Queue ID</th>
                                <th className="px-6 py-3 font-medium text-right">Broker Offset</th>
                                <th className="px-6 py-3 font-medium text-right">Consumer Offset</th>
                                <th className="px-6 py-3 font-medium text-right">Lag (Diff)</th>
                                <th className="px-6 py-3 font-medium">Client Info</th>
                                <th className="px-6 py-3 font-medium text-right">Last Consume Time</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                            <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">mxsm</td>
                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">0</td>
                                <td className="px-6 py-3 font-mono text-gray-900 dark:text-gray-300 text-right">0</td>
                                <td className="px-6 py-3 font-mono text-gray-900 dark:text-gray-300 text-right">0</td>
                                <td className="px-6 py-3 font-mono text-gray-400 dark:text-gray-600 text-right">-</td>
                                <td className="px-6 py-3 text-gray-500 dark:text-gray-500 italic"></td>
                                <td className="px-6 py-3 font-mono text-gray-400 dark:text-gray-600 text-right">-</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

             {/* Topic Section 2: TopicTest */}
             <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                <div className="px-6 py-4 bg-gray-50/50 dark:bg-gray-800/50 border-b border-gray-200 dark:border-gray-800 flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                        <div className="p-1.5 bg-purple-100 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400 rounded-lg">
                           <FileBox className="w-4 h-4" />
                        </div>
                        <span className="font-mono text-sm font-semibold text-gray-700 dark:text-gray-300">TopicTest</span>
                    </div>
                    <div className="flex items-center space-x-6 text-xs font-mono">
                        <div className="flex items-center text-gray-500 dark:text-gray-400">
                            <span className="uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500 mr-2">Total Lag:</span>
                            <span className="text-gray-900 dark:text-white font-bold bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400 px-2 py-0.5 rounded">1000</span>
                        </div>
                        <div className="flex items-center text-gray-500 dark:text-gray-400">
                            <span className="uppercase font-semibold tracking-wider text-gray-400 dark:text-gray-500 mr-2">Last Consume:</span>
                            <span className="text-gray-900 dark:text-white">2026/1/21 12:24:40</span>
                        </div>
                    </div>
                </div>
                
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="bg-gray-50 dark:bg-gray-800/50 text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-100 dark:border-gray-800">
                                <th className="px-6 py-3 font-medium">Broker</th>
                                <th className="px-6 py-3 font-medium">Queue ID</th>
                                <th className="px-6 py-3 font-medium text-right">Broker Offset</th>
                                <th className="px-6 py-3 font-medium text-right">Consumer Offset</th>
                                <th className="px-6 py-3 font-medium text-right">Lag (Diff)</th>
                                <th className="px-6 py-3 font-medium">Client Info</th>
                                <th className="px-6 py-3 font-medium text-right">Last Consume Time</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                            {[0, 1, 2, 3].map((qid) => (
                                <tr key={qid} className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                                    <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">mxsm</td>
                                    <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">{qid}</td>
                                    <td className="px-6 py-3 font-mono text-gray-900 dark:text-gray-300 text-right">{280 + qid * 2}</td>
                                    <td className="px-6 py-3 font-mono text-gray-900 dark:text-gray-300 text-right">{30 + qid * 2}</td>
                                    <td className="px-6 py-3 font-mono text-amber-600 dark:text-amber-400 font-bold text-right">{250}</td>
                                    <td className="px-6 py-3 text-gray-500 dark:text-gray-500 italic"></td>
                                    <td className="px-6 py-3 font-mono text-gray-500 dark:text-gray-400 text-right">2026/1/21 12:24:40</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end z-10 shrink-0">
             <button
               onClick={onClose}
               className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
             >
               Close
             </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};

const ConsumerConfigModal = ({ isOpen, onClose, consumer }: any) => {
  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 0.3 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="absolute inset-0 bg-black"
        />
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 10 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 10 }}
          className="relative w-full max-w-4xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-xl shadow-2xl flex flex-col overflow-hidden border border-gray-100 dark:border-gray-800"
        >
          {/* Header */}
          <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10 shrink-0">
            <div>
              <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                <Settings className="w-5 h-5 mr-2 text-blue-500" />
                Configuration
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1 flex items-center">
                 <span className="font-mono font-medium text-gray-700 dark:text-gray-300">{consumer?.group}</span>
              </p>
            </div>
            <button 
              onClick={onClose}
              className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Content - Card Grid Layout */}
          <div className="flex-1 overflow-y-auto p-6 bg-gray-50/50 dark:bg-gray-950/50">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                
                {/* Card 1: Basic Info */}
                <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-5 space-y-4">
                     <div className="flex items-center space-x-2 mb-2">
                        <div className="p-1.5 bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-lg">
                            <Hash className="w-4 h-4" />
                        </div>
                        <h4 className="font-semibold text-gray-900 dark:text-white">Basic Information</h4>
                     </div>
                     
                     <div className="space-y-1">
                         <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Group Name</label>
                         <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-lg text-sm font-mono text-gray-600 dark:text-gray-300 break-all">
                             {consumer?.group}
                         </div>
                     </div>

                     <div className="space-y-1">
                         <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Broker Name</label>
                         <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-lg text-sm font-mono text-gray-600 dark:text-gray-300">
                             mxsm
                         </div>
                     </div>

                     <div className="space-y-1">
                        <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Broker ID</label>
                        <input type="number" defaultValue={0} className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all" />
                     </div>
                </div>

                {/* Card 2: Consumption Control */}
                <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-5 space-y-4">
                     <div className="flex items-center space-x-2 mb-2">
                        <div className="p-1.5 bg-green-50 dark:bg-green-900/30 text-green-600 dark:text-green-400 rounded-lg">
                            <Activity className="w-4 h-4" />
                        </div>
                        <h4 className="font-semibold text-gray-900 dark:text-white">Control Settings</h4>
                     </div>

                     <div className="space-y-4 pt-1">
                         <div className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                             <div>
                                 <label className="text-sm font-medium text-gray-700 dark:text-gray-200 block">Consume Enable</label>
                                 <span className="text-xs text-gray-400 dark:text-gray-500">Master switch for consumption</span>
                             </div>
                             <button className="w-11 h-6 bg-blue-500 rounded-full p-1 transition-colors relative shrink-0">
                                 <div className="w-4 h-4 bg-white rounded-full shadow-sm absolute right-1 top-1" />
                             </button>
                         </div>
                         <div className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                             <div>
                                 <label className="text-sm font-medium text-gray-700 dark:text-gray-200 block">Orderly Consumption</label>
                                 <span className="text-xs text-gray-400 dark:text-gray-500">Strict FIFO order</span>
                             </div>
                             <button className="w-11 h-6 bg-gray-200 dark:bg-gray-700 rounded-full p-1 transition-colors relative shrink-0">
                                 <div className="w-4 h-4 bg-white rounded-full shadow-sm absolute left-1 top-1" />
                             </button>
                         </div>
                          <div className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                             <div>
                                 <label className="text-sm font-medium text-gray-700 dark:text-gray-200 block">Broadcast Mode</label>
                                 <span className="text-xs text-gray-400 dark:text-gray-500">All consumers receive messages</span>
                             </div>
                             <button className="w-11 h-6 bg-blue-500 rounded-full p-1 transition-colors relative shrink-0">
                                 <div className="w-4 h-4 bg-white rounded-full shadow-sm absolute right-1 top-1" />
                             </button>
                         </div>
                     </div>
                </div>

                {/* Card 3: Retry & Throttling */}
                <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-5 space-y-4">
                     <div className="flex items-center space-x-2 mb-2">
                        <div className="p-1.5 bg-orange-50 dark:bg-orange-900/30 text-orange-600 dark:text-orange-400 rounded-lg">
                            <RotateCcw className="w-4 h-4" />
                        </div>
                        <h4 className="font-semibold text-gray-900 dark:text-white">Retry & Throttling</h4>
                     </div>

                     <div className="grid grid-cols-2 gap-4">
                         <div className="space-y-1">
                            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Retry Queues</label>
                            <input type="number" defaultValue={1} className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all" />
                         </div>
                         <div className="space-y-1">
                            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Max Retries</label>
                            <input type="number" defaultValue={16} className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all" />
                         </div>
                     </div>

                     <div className="space-y-1 pt-2">
                        <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Slow Consumption Broker</label>
                        <div className="flex items-center space-x-2">
                            <input type="number" defaultValue={1} className="flex-1 px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all" />
                            <span className="text-xs text-gray-400 dark:text-gray-500 shrink-0">Threshold ID</span>
                        </div>
                     </div>

                     <div className="space-y-1 pt-2">
                        <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Consume Timeout</label>
                        <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 flex justify-between items-center">
                            <span>15</span>
                            <span className="text-xs text-gray-400 dark:text-gray-500">Minutes</span>
                        </div>
                     </div>
                </div>

                {/* Card 4: Advanced JSON */}
                <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-5 space-y-4">
                     <div className="flex items-center space-x-2 mb-2">
                        <div className="p-1.5 bg-purple-50 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400 rounded-lg">
                            <Cpu className="w-4 h-4" />
                        </div>
                        <h4 className="font-semibold text-gray-900 dark:text-white">System Config</h4>
                     </div>

                     <div className="space-y-1">
                         <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Retry Policy (JSON)</label>
                         <div className="bg-gray-900 dark:bg-gray-950 rounded-lg p-3 overflow-hidden border border-gray-800">
                             <pre className="font-mono text-[10px] text-gray-300 overflow-x-auto custom-scrollbar">
{`{
  "type": "CUSTOMIZED",
  "exponentialRetryPolicy": null,
  "customizedRetryPolicy": null,
  "retryPolicy": {
    "maxRetries": 16
  }
}`}
                             </pre>
                         </div>
                     </div>

                     <div className="grid grid-cols-2 gap-4 pt-2">
                        <div className="space-y-1">
                             <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Cluster Name</label>
                             <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-lg text-xs font-mono text-gray-400 dark:text-gray-500">
                                 DefaultCluster
                             </div>
                        </div>
                        <div className="space-y-1">
                             <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">System Flag</label>
                             <div className="px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-100 dark:border-gray-700 rounded-lg text-xs font-mono text-gray-400 dark:text-gray-500">
                                 0
                             </div>
                        </div>
                     </div>
                </div>

            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end items-center gap-3 z-10 shrink-0">
             <button
               onClick={onClose}
               className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300 rounded-lg text-sm font-medium hover:bg-gray-50 dark:hover:bg-gray-700 transition-all shadow-sm"
             >
               Cancel
             </button>
             <button
               onClick={() => {
                   toast.success("Configuration updated successfully");
                   onClose();
               }}
               className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
             >
               Save Changes
             </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};

const ConsumerClientModal = ({ isOpen, onClose, consumer }: any) => {
  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 0.3 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="absolute inset-0 bg-black"
        />
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 10 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 10 }}
          className="relative w-full max-w-6xl max-h-[90vh] bg-white dark:bg-gray-900 rounded-xl shadow-2xl flex flex-col overflow-hidden border border-gray-100 dark:border-gray-800"
        >
          {/* Header */}
          <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10 shrink-0">
            <div>
              <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                <Users className="w-5 h-5 mr-2 text-blue-500" />
                Client Information
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1 flex items-center">
                 <span className="font-mono font-medium text-gray-700 dark:text-gray-300">{consumer?.group}</span>
              </p>
            </div>
            <button 
              onClick={onClose}
              className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-6 bg-gray-50/50 dark:bg-gray-950/50 space-y-6">
            
            {/* 1. Connection Overview */}
            <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-6">
                <h4 className="font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <Activity className="w-4 h-4 mr-2 text-gray-500 dark:text-gray-400" />
                    Connection Overview
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                     <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                         <span className="text-sm font-medium text-gray-600 dark:text-gray-300">Consume Type</span>
                         <span className="px-2 py-1 text-xs font-bold bg-green-50 dark:bg-green-900/30 text-green-600 dark:text-green-400 border border-green-100 dark:border-green-800 rounded uppercase tracking-wide">
                             CONSUME_PASSIVELY
                         </span>
                     </div>
                     <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                         <span className="text-sm font-medium text-gray-600 dark:text-gray-300">Message Model</span>
                         <span className="px-2 py-1 text-xs font-bold bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 border border-blue-100 dark:border-blue-800 rounded uppercase tracking-wide">
                             CLUSTERING
                         </span>
                     </div>
                     <div className="col-span-1 md:col-span-2 flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-100 dark:border-gray-700">
                         <span className="text-sm font-medium text-gray-600 dark:text-gray-300">Consume From Where</span>
                         <span className="px-2 py-1 text-xs font-bold bg-purple-50 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400 border border-purple-100 dark:border-purple-800 rounded uppercase tracking-wide">
                             CONSUME_FROM_FIRST_OFFSET
                         </span>
                     </div>
                </div>
            </div>

            {/* 2. Client Connections */}
            <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                <div className="px-6 py-4 border-b border-gray-100 dark:border-gray-800 flex items-center">
                    <Network className="w-4 h-4 mr-2 text-gray-500 dark:text-gray-400" />
                    <h4 className="font-semibold text-gray-900 dark:text-white">Client Connections</h4>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="bg-gray-50 dark:bg-gray-800/50 text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-100 dark:border-gray-800">
                                <th className="px-6 py-3 font-medium">Client ID</th>
                                <th className="px-6 py-3 font-medium">Client Address</th>
                                <th className="px-6 py-3 font-medium">Language</th>
                                <th className="px-6 py-3 font-medium text-right">Version</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                            <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">172.20.48.1@29136#17844441802200</td>
                                <td className="px-6 py-3 font-mono text-gray-900 dark:text-gray-300">172.20.48.1:59326</td>
                                <td className="px-6 py-3">
                                    <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold bg-orange-50 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400 border border-orange-100 dark:border-orange-800">JAVA</span>
                                </td>
                                <td className="px-6 py-3 font-mono text-gray-500 dark:text-gray-400 text-right">V5_4_0</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            {/* 3. Client Subscriptions */}
            <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                 <div className="px-6 py-4 border-b border-gray-100 dark:border-gray-800 flex items-center">
                    <ListChecks className="w-4 h-4 mr-2 text-gray-500 dark:text-gray-400" />
                    <h4 className="font-semibold text-gray-900 dark:text-white">Client Subscriptions</h4>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="bg-gray-50 dark:bg-gray-800/50 text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-100 dark:border-gray-800">
                                <th className="px-6 py-3 font-medium">Topic</th>
                                <th className="px-6 py-3 font-medium">Subscription Expression</th>
                                <th className="px-6 py-3 font-medium">Expression Type</th>
                                <th className="px-6 py-3 font-medium">Tags Set</th>
                                <th className="px-6 py-3 font-medium">Code Set</th>
                                <th className="px-6 py-3 font-medium text-right">Sub Version</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                            <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                                <td className="px-6 py-3 font-mono text-gray-900 dark:text-white">%RETRY%please_rename_unique_group_name_4</td>
                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">*</td>
                                <td className="px-6 py-3">
                                     <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 border border-blue-100 dark:border-blue-800">TAG</span>
                                </td>
                                <td className="px-6 py-3 text-gray-400 dark:text-gray-500">N/A</td>
                                <td className="px-6 py-3 text-gray-400 dark:text-gray-500">N/A</td>
                                <td className="px-6 py-3 font-mono text-gray-500 dark:text-gray-400 text-right">1769520097252</td>
                            </tr>
                            <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                                <td className="px-6 py-3 font-mono text-gray-900 dark:text-white">TopicTest</td>
                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400">*</td>
                                <td className="px-6 py-3">
                                     <span className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 border border-blue-100 dark:border-blue-800">TAG</span>
                                </td>
                                <td className="px-6 py-3 text-gray-400 dark:text-gray-500">N/A</td>
                                <td className="px-6 py-3 text-gray-400 dark:text-gray-500">N/A</td>
                                <td className="px-6 py-3 font-mono text-gray-500 dark:text-gray-400 text-right">1769520097262</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end z-10 shrink-0">
             <button
               onClick={onClose}
               className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
             >
               Close
             </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};

export const ConsumerView = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filters, setFilters] = useState<any>({
    NORMAL: true,
    FIFO: false,
    SYSTEM: true
  });
  const [proxy, setProxy] = useState('127.0.0.1:8080');
  const [enableProxy, setEnableProxy] = useState(false);
  const [detailModal, setDetailModal] = useState<{isOpen: boolean, consumer: any}>({ isOpen: false, consumer: null });
  const [configModal, setConfigModal] = useState<{isOpen: boolean, consumer: any}>({ isOpen: false, consumer: null });
  const [clientModal, setClientModal] = useState<{isOpen: boolean, consumer: any}>({ isOpen: false, consumer: null });
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 6;

  // Mock Data
  const consumers = [
    { group: 'please_rename_unique_group_name_4', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: 1000, updateTime: '2026-01-27T12:30:09.589+00:00' },
    { group: 'SELF_TEST_C_GROUP', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.335+00:00' },
    { group: 'CID_ONSAPI_PULL', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PULL', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.335+00:00' },
    { group: 'CID_ONSAPI_OWNER', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.336+00:00' },
    { group: 'CID_RMQ_SYS_TRANS', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.336+00:00' },
    { group: 'CID_ONSAPI_PERMISSION', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.336+00:00' },
    { group: 'CID_ONS-HTTP-PROXY', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.337+00:00' },
    { group: 'TOOLS_CONSUMER', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.338+00:00' },
    { group: 'FILTERSRV_CONSUMER', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: -1, updateTime: '2026-01-27T12:29:57.638+00:00' },
    { group: 'CONSUMER_GROUP_10', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: 0, updateTime: '2026-01-27T12:31:00.000+00:00' },
    { group: 'CONSUMER_GROUP_11', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: 0, updateTime: '2026-01-27T12:31:00.000+00:00' },
    { group: 'CONSUMER_GROUP_12', quantity: 0, version: 'V5_4_0', type: 'CLUSTERING', mode: 'PUSH', tps: 0, delay: 0, updateTime: '2026-01-27T12:31:00.000+00:00' },
  ];

  const toggleFilter = (key: string) => setFilters((prev: any) => ({ ...prev, [key]: !prev[key] }));

  // Pagination Logic
  const filteredConsumers = consumers.filter(c => c.group.toLowerCase().includes(searchTerm.toLowerCase()));
  const totalPages = Math.ceil(filteredConsumers.length / itemsPerPage);
  const currentConsumers = filteredConsumers.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage
  );

  const goToPage = (page: number) => {
    if (page >= 1 && page <= totalPages) {
      setCurrentPage(page);
    }
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
        <ConsumerDetailModal 
           isOpen={detailModal.isOpen} 
           onClose={() => setDetailModal({ isOpen: false, consumer: null })} 
           consumer={detailModal.consumer} 
        />
        <ConsumerConfigModal
           isOpen={configModal.isOpen}
           onClose={() => setConfigModal({ isOpen: false, consumer: null })}
           consumer={configModal.consumer}
        />
        <ConsumerClientModal
           isOpen={clientModal.isOpen}
           onClose={() => setClientModal({ isOpen: false, consumer: null })}
           consumer={clientModal.consumer}
        />
        {/* Toolbar */}
        <div className="flex flex-col xl:flex-row xl:items-center justify-between gap-4 bg-white dark:bg-gray-900 p-4 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
             {/* Left: Search & Filters */}
             <div className="flex flex-wrap items-center gap-4">
                <div className="relative group">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500 group-focus-within:text-blue-500 transition-colors" />
                    <input 
                      type="text" 
                      placeholder="SubscriptionGroup..." 
                      className="pl-10 pr-4 py-2 w-64 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all dark:text-white dark:placeholder:text-gray-500"
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                    />
                </div>

                <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2 hidden md:block"></div>

                <div className="flex items-center space-x-2">
                    {Object.entries(filters).map(([key, value]) => (
                        <button
                            key={key}
                            onClick={() => toggleFilter(key)}
                            className={`flex items-center px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                                value 
                                ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 border-blue-200 dark:border-blue-800 shadow-sm' 
                                : 'bg-white dark:bg-gray-800 text-gray-500 dark:text-gray-400 border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700'
                            }`}
                        >
                            {value && <Check className="w-3 h-3 mr-1.5" />}
                            {key}
                        </button>
                    ))}
                </div>
             </div>

             {/* Right: Actions */}
             <div className="flex flex-wrap items-center gap-3">
                 <div className="flex items-center space-x-2 bg-gray-50 dark:bg-gray-800 p-1 rounded-lg border border-gray-200 dark:border-gray-700">
                    <span className="text-xs text-gray-500 dark:text-gray-400 px-2">Proxy:</span>
                    <select 
                      onChange={(e) => setProxy(e.target.value)}
                      className="bg-transparent text-xs font-medium text-gray-700 dark:text-gray-300 outline-none pr-2 cursor-pointer dark:bg-gray-800"
                    >
                        <option>127.0.0.1:8080</option>
                    </select>
                 </div>
                 
                 <div className="flex items-center space-x-2 px-2">
                    <span className="text-xs font-medium text-gray-600 dark:text-gray-400">Enable Proxy</span>
                    <button 
                        onClick={() => setEnableProxy(!enableProxy)}
                        className={`w-9 h-5 rounded-full p-0.5 transition-colors duration-200 ease-in-out ${enableProxy ? 'bg-blue-500' : 'bg-gray-200 dark:bg-gray-700'}`}
                    >
                        <div className={`w-4 h-4 bg-white rounded-full shadow-sm transform transition-transform duration-200 ${enableProxy ? 'translate-x-4' : 'translate-x-0'}`} />
                    </button>
                 </div>

                 <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-2"></div>

                 <button className="flex items-center px-4 py-2 bg-gray-900 text-white rounded-xl text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg active:scale-95 dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800">
                    <Plus className="w-4 h-4 mr-2" />
                    Add/Update
                 </button>

                 <button className="p-2 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-gray-900 dark:hover:text-white rounded-lg transition-colors border border-transparent hover:border-gray-200 dark:hover:border-gray-700">
                    <RefreshCw className="w-4 h-4" />
                 </button>
             </div>
        </div>

        {/* Card Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-6">
            {currentConsumers.map((consumer, index) => (
                <motion.div
                    key={consumer.group}
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
                        <div className="flex justify-between items-start mb-3">
                            <div>
                                <h3 className="font-bold text-gray-900 dark:text-white text-lg break-all leading-tight">{consumer.group}</h3>
                                <div className="flex items-center mt-2 space-x-2">
                                     <span className="inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-bold bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 border border-blue-100 dark:border-blue-800 uppercase tracking-wide">
                                        {consumer.type}
                                     </span>
                                     <span className="inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-bold bg-purple-50 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400 border border-purple-100 dark:border-purple-800 uppercase tracking-wide">
                                        {consumer.mode}
                                     </span>
                                </div>
                            </div>
                            <div className="text-[10px] font-mono text-gray-400 dark:text-gray-500 bg-gray-50 dark:bg-gray-800 px-2 py-1 rounded border border-gray-100 dark:border-gray-700">
                                {consumer.version}
                            </div>
                        </div>
                    </div>

                    {/* Metrics */}
                    <div className="p-5 grid grid-cols-3 gap-4 bg-white dark:bg-gray-900">
                        <div className="flex flex-col items-center p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-blue-100 dark:group-hover:border-blue-900/50 transition-colors">
                            <span className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">TPS</span>
                            <span className="text-lg font-mono font-bold text-gray-900 dark:text-white">{consumer.tps}</span>
                        </div>
                        <div className="flex flex-col items-center p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-orange-100 dark:group-hover:border-orange-900/50 transition-colors">
                            <span className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Delay</span>
                            <span className={`text-lg font-mono font-bold ${consumer.delay > 0 ? 'text-orange-500' : 'text-gray-900 dark:text-white'}`}>{consumer.delay}</span>
                        </div>
                        <div className="flex flex-col items-center p-3 rounded-xl bg-gray-50/50 dark:bg-gray-800/50 border border-gray-100 dark:border-gray-800 group-hover:border-green-100 dark:group-hover:border-green-900/50 transition-colors">
                            <span className="text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500 font-semibold mb-1">Quantity</span>
                            <span className="text-lg font-mono font-bold text-gray-900 dark:text-white">{consumer.quantity}</span>
                        </div>
                    </div>

                    {/* Footer / Actions */}
                    <div className="mt-auto border-t border-gray-100 dark:border-gray-800 bg-gray-50/30 dark:bg-gray-800/30 p-3">
                        <div className="flex items-center justify-between text-xs text-gray-400 dark:text-gray-500 mb-3 px-2">
                            <div className="flex items-center">
                                <Clock className="w-3 h-3 mr-1" />
                                Updated: {new Date(consumer.updateTime).toLocaleTimeString()}
                            </div>
                        </div>
                        <div className="grid grid-cols-5 gap-2">
                             <button 
                                onClick={() => setClientModal({ isOpen: true, consumer })}
                                className="col-span-1 flex flex-col items-center justify-center p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 transition-all shadow-sm group/btn"
                                title="Client"
                             >
                                <Users className="w-4 h-4 mb-1" />
                                <span className="text-[9px] font-medium">Client</span>
                             </button>
                             <button 
                                onClick={() => setDetailModal({ isOpen: true, consumer })}
                                className="col-span-1 flex flex-col items-center justify-center p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 transition-all shadow-sm group/btn"
                                title="Detail"
                             >
                                <FileText className="w-4 h-4 mb-1" />
                                <span className="text-[9px] font-medium">Detail</span>
                             </button>
                             <button 
                                onClick={() => setConfigModal({ isOpen: true, consumer })}
                                className="col-span-1 flex flex-col items-center justify-center p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 transition-all shadow-sm group/btn"
                                title="Config"
                             >
                                <Settings className="w-4 h-4 mb-1" />
                                <span className="text-[9px] font-medium">Config</span>
                             </button>
                             <button 
                                onClick={() => toast("Refreshed")}
                                className="col-span-1 flex flex-col items-center justify-center p-2 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 hover:text-blue-600 dark:hover:text-blue-400 hover:border-blue-200 dark:hover:border-blue-800 transition-all shadow-sm group/btn"
                                title="Refresh"
                             >
                                <RefreshCw className="w-4 h-4 mb-1" />
                                <span className="text-[9px] font-medium">Sync</span>
                             </button>
                             <button 
                                onClick={() => toast("Deleted")}
                                className="col-span-1 flex flex-col items-center justify-center p-2 rounded-lg bg-white dark:bg-gray-800 border border-red-100 dark:border-red-900/30 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 hover:border-red-200 dark:hover:border-red-800 transition-all shadow-sm group/btn"
                                title="Delete"
                             >
                                <Trash2 className="w-4 h-4 mb-1" />
                                <span className="text-[9px] font-medium">Delete</span>
                             </button>
                        </div>
                    </div>
                </motion.div>
            ))}
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-center pt-6">
            <Pagination 
                currentPage={currentPage}
                totalPages={totalPages}
                onPageChange={goToPage}
            />
        </div>
    </div>
  );
};
