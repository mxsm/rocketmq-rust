import React, { useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { 
  Server, 
  Activity,
  RefreshCw,
  ChevronDown,
  Check,
  Crown,
  GitBranch,
  ArrowUpCircle,
  ArrowDownCircle,
  Database,
  Clock,
  HardDrive
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Button } from '../src/components/ui/LegacyButton';
import { SideSheet } from './ui/SideSheet';

export const ClusterView = () => {
  // Mock data for cluster view
  const clusterData = [
    {
      brokerName: 'mxsm',
      role: 'Master',
      id: '0',
      address: '172.20.48.1:10911',
      version: 'V5_4_0',
      produceTPS: '0.00',
      consumeTPS: '0.00',
      yesterdayProduce: 0,
      yesterdayConsume: 0,
      todayProduce: 0,
      todayConsume: 0,
    },
    {
      brokerName: 'broker-a',
      role: 'Slave',
      id: '1',
      address: '172.20.48.2:10911',
      version: 'V5_4_0',
      produceTPS: '12.50',
      consumeTPS: '11.20',
      yesterdayProduce: 145000,
      yesterdayConsume: 144800,
      todayProduce: 8500,
      todayConsume: 8490,
    }
  ];

  // Mock details data
  const statusData = {
    brokerName: 'mxsm',
    brokerId: 0,
    address: '172.20.48.1:10911',
    msgPutTotalTodayNow: 0,
    brokerActive: true,
    sendThreadPoolQueueHeadWaitTimeMills: 0,
    putMessageDistributeTime: '[<=0ms]:0 [0~10ms]:0 [10~50ms]:0 [50~100ms]:0',
    remainHowManyDataToFlush: '0 B',
    bootTimestamp: '1769503247028',
    commitLogDiskRatio: 0.26
  };

  const configData = {
    timerStopEnqueue: false,
    metricsOtelCardinalityLimit: 50000,
    serverSocketBacklog: 1024,
    channelNotActiveInterval: 60000,
    haHousekeepingInterval: 20000,
    consumerManageThreadPoolNums: 32,
    mappedFileSizeTimerLog: 104857600,
    splitRegistrationSize: 800,
    transactionCheckInterval: 30000,
    enableDetailStat: true
  };

  const [selectedCluster, setSelectedCluster] = useState('DefaultCluster');
  const [isSelectOpen, setIsSelectOpen] = useState(false);
  const [detailSheet, setDetailSheet] = useState({ isOpen: false, type: null, title: '', data: {} });

  const clusters = ['DefaultCluster', 'ProductionCluster', 'StagingCluster'];

  const openSheet = (type: any, brokerData: any) => {
    const data = type === 'Status' ? statusData : configData;
    setDetailSheet({
      isOpen: true,
      type,
      title: `${type} [${brokerData.brokerName}][${brokerData.id}]`,
      data
    });
  };

  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <SideSheet 
        isOpen={detailSheet.isOpen} 
        onClose={() => setDetailSheet({ ...detailSheet, isOpen: false })}
        title={detailSheet.title}
        data={detailSheet.data}
      />

      {/* Cluster Control Bar */}
      <div className="bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 p-2 shadow-sm flex items-center justify-between mb-8 sticky top-0 z-20 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90">
        <div className="flex items-center">
           {/* Custom Cluster Select */}
           <div className="relative">
              <button 
                onClick={() => setIsSelectOpen(!isSelectOpen)}
                onBlur={() => setTimeout(() => setIsSelectOpen(false), 200)}
                className={`flex items-center space-x-3 px-4 py-2.5 rounded-xl transition-all duration-200 outline-none ${isSelectOpen ? 'bg-gray-100 dark:bg-gray-800' : 'hover:bg-gray-50 dark:hover:bg-gray-800'}`}
              >
                <div className="w-8 h-8 rounded-lg bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400">
                   <Server className="w-4 h-4" />
                </div>
                <div className="text-left">
                   <div className="text-[10px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider">Current Cluster</div>
                   <div className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center">
                     {selectedCluster}
                     <ChevronDown className={`w-4 h-4 ml-2 text-gray-400 dark:text-gray-500 transition-transform duration-200 ${isSelectOpen ? 'rotate-180' : ''}`} />
                   </div>
                </div>
              </button>

              <AnimatePresence>
                {isSelectOpen && (
                  <motion.div
                    initial={{ opacity: 0, y: 10, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: 10, scale: 0.95 }}
                    transition={{ duration: 0.2 }}
                    className="absolute top-full left-0 mt-2 w-64 bg-white dark:bg-gray-800 rounded-xl shadow-xl border border-gray-100 dark:border-gray-700 overflow-hidden z-50 origin-top-left"
                  >
                    <div className="p-1.5 space-y-0.5">
                      {clusters.map((cluster) => (
                        <button
                          key={cluster}
                          onClick={() => {
                            setSelectedCluster(cluster);
                            setIsSelectOpen(false);
                          }}
                          className={`w-full flex items-center justify-between px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                            selectedCluster === cluster 
                              ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' 
                              : 'text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700'
                          }`}
                        >
                          <span>{cluster}</span>
                          {selectedCluster === cluster && <Check className="w-4 h-4 text-blue-600 dark:text-blue-400" />}
                        </button>
                      ))}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
           </div>
           
           <div className="h-8 w-px bg-gray-200 dark:bg-gray-700 mx-4"></div>
           
           <div className="flex items-center space-x-2 text-sm text-gray-500 dark:text-gray-400">
              <span className="flex h-2 w-2 relative">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
              </span>
              <span>All Systems Operational</span>
           </div>
        </div>

        <div className="flex items-center pr-4">
           <button 
             onClick={() => toast.success("Cluster status refreshed")}
             className="p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300 transition-colors"
             title="Refresh Status"
           >
             <RefreshCw className="w-4 h-4" />
           </button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        {clusterData.map((broker, index) => {
          const isMaster = broker.role === 'Master';
          return (
            <motion.div
              key={index}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.1, type: "spring" }}
              className={`rounded-2xl border shadow-sm hover:shadow-md transition-shadow duration-200 overflow-hidden flex flex-col ${isMaster ? 'border-amber-100 bg-amber-50/10 dark:border-amber-900/30 dark:bg-amber-900/5' : 'border-gray-100 bg-white dark:border-gray-800 dark:bg-gray-900'}`}
            >
              {/* Header */}
              <div className={`p-5 border-b flex justify-between items-start ${isMaster ? 'bg-gradient-to-r from-amber-50 to-orange-50/30 border-amber-100 dark:from-amber-900/20 dark:to-orange-900/10 dark:border-amber-900/30' : 'bg-gradient-to-br from-gray-50/50 to-white border-gray-50 dark:from-gray-800 dark:to-gray-900 dark:border-gray-800'}`}>
                 <div>
                    <div className="flex items-center space-x-2">
                      <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{broker.brokerName}</h3>
                      {isMaster ? (
                        <span className="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold bg-amber-100 text-amber-700 border border-amber-200 dark:bg-amber-900/40 dark:text-amber-400 dark:border-amber-800/50">
                          <Crown className="w-3 h-3 mr-1" />
                          Master
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold bg-gray-100 text-gray-600 border border-gray-200 dark:bg-gray-800 dark:text-gray-400 dark:border-gray-700">
                          <GitBranch className="w-3 h-3 mr-1" />
                          Slave
                        </span>
                      )}
                    </div>
                    <div className="mt-1.5 flex items-center text-xs text-gray-500 dark:text-gray-400 font-mono">
                      <Server className="w-3 h-3 mr-1.5 opacity-60" />
                      {broker.address}
                      <span className="mx-2 text-gray-300 dark:text-gray-600">|</span>
                      <span>ID: {broker.id}</span>
                    </div>
                 </div>
                 <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-white text-blue-700 border border-blue-100 shadow-sm dark:bg-gray-800 dark:text-blue-300 dark:border-gray-700">
                    {broker.version}
                 </span>
              </div>

              {/* Metrics Body */}
              <div className="p-5 flex-1 space-y-6">
                 {/* Real-time TPS */}
                 <div>
                    <div className="text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-2 flex items-center">
                      <Activity className="w-3 h-3 mr-1" /> Real-time TPS
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                       <div className="bg-blue-50/60 dark:bg-blue-900/10 rounded-xl p-3 border border-blue-100 dark:border-blue-900/30 relative overflow-hidden group">
                          <div className="absolute right-0 top-0 p-1 opacity-10 group-hover:opacity-20 transition-opacity">
                             <ArrowUpCircle className="w-12 h-12 text-blue-600 dark:text-blue-400" />
                          </div>
                          <div className="text-xs text-blue-600 dark:text-blue-400 font-medium mb-1">Produce</div>
                          <div className="text-xl font-bold text-gray-900 dark:text-gray-100 font-mono tracking-tight">{broker.produceTPS}</div>
                       </div>
                       <div className="bg-purple-50/60 dark:bg-purple-900/10 rounded-xl p-3 border border-purple-100 dark:border-purple-900/30 relative overflow-hidden group">
                          <div className="absolute right-0 top-0 p-1 opacity-10 group-hover:opacity-20 transition-opacity">
                             <ArrowDownCircle className="w-12 h-12 text-purple-600 dark:text-purple-400" />
                          </div>
                          <div className="text-xs text-purple-600 dark:text-purple-400 font-medium mb-1">Consume</div>
                          <div className="text-xl font-bold text-gray-900 dark:text-gray-100 font-mono tracking-tight">{broker.consumeTPS}</div>
                       </div>
                    </div>
                 </div>

                 {/* Message Counts */}
                 <div>
                    <div className="text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-3 flex items-center">
                      <Database className="w-3 h-3 mr-1" /> Message Statistics
                    </div>
                    
                    <div className="bg-gray-50 dark:bg-gray-800/50 rounded-xl p-4 space-y-4">
                      {/* Today Row */}
                      <div className="flex items-center justify-between">
                         <div className="flex items-center w-24">
                            <div className="w-8 h-8 rounded-lg bg-white dark:bg-gray-800 border border-gray-100 dark:border-gray-700 flex items-center justify-center shadow-sm text-gray-500 dark:text-gray-400 mr-2.5">
                               <Clock className="w-4 h-4" />
                            </div>
                            <span className="text-sm font-semibold text-gray-700 dark:text-gray-300">Today</span>
                         </div>
                         <div className="flex-1 grid grid-cols-2 gap-4">
                            <div className="flex flex-col items-end">
                               <span className="text-[10px] text-blue-500 dark:text-blue-400 font-medium uppercase mb-0.5">Produce</span>
                               <span className="text-sm font-mono font-bold text-gray-900 dark:text-gray-100">{broker.todayProduce}</span>
                            </div>
                            <div className="flex flex-col items-end border-l border-gray-200 dark:border-gray-700 pl-4">
                               <span className="text-[10px] text-purple-500 dark:text-purple-400 font-medium uppercase mb-0.5">Consume</span>
                               <span className="text-sm font-mono font-bold text-gray-900 dark:text-gray-100">{broker.todayConsume}</span>
                            </div>
                         </div>
                      </div>

                      <div className="border-t border-gray-200 dark:border-gray-700 border-dashed"></div>

                      {/* Yesterday Row */}
                      <div className="flex items-center justify-between opacity-80">
                         <div className="flex items-center w-24">
                            <div className="w-8 h-8 rounded-lg bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 flex items-center justify-center text-gray-400 dark:text-gray-500 mr-2.5">
                               <HardDrive className="w-4 h-4" />
                            </div>
                            <span className="text-sm font-medium text-gray-500 dark:text-gray-400">Yesterday</span>
                         </div>
                         <div className="flex-1 grid grid-cols-2 gap-4">
                            <div className="flex flex-col items-end">
                               <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{broker.yesterdayProduce}</span>
                            </div>
                            <div className="flex flex-col items-end border-l border-gray-200 dark:border-gray-700 pl-4">
                               <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{broker.yesterdayConsume}</span>
                            </div>
                         </div>
                      </div>
                    </div>
                 </div>
              </div>

              {/* Footer Actions */}
              <div className="p-4 bg-gray-50/50 dark:bg-gray-800/80 border-t border-gray-100 dark:border-gray-800 grid grid-cols-2 gap-3">
                 <Button variant="secondary" onClick={() => openSheet('Status', broker)} className="w-full justify-center">
                   Status
                 </Button>
                 <Button variant="primary" onClick={() => openSheet('Config', broker)} className="w-full justify-center">
                   Config
                 </Button>
              </div>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
};
