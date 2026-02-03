import React from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { X, FileText, Activity, Users, CheckCircle2, RotateCcw, Copy, Database, Server, Clock, HardDrive, Flag, RefreshCw, Hash, Tag, Globe, Key, Layers, Code, User, AlertCircle } from 'lucide-react';
import { toast } from 'sonner@2.0.3';

interface MessageDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  message: any;
}

export const MessageDetailModal = ({ isOpen, onClose, message }: MessageDetailModalProps) => {
  if (!isOpen) return null;

  // Mock data if message is missing some fields, based on screenshot
  const data = {
    topic: message?.topic || 'TopicTest',
    messageId: message?.msgId || '240E03B350D263C087AA69AB77E798719CBC18B4AAC28A91AB100000',
    storeHost: message?.storeHost || '172.20.48.1:10911',
    bornHost: message?.bornHost || '172.20.48.1:61266',
    storeTime: message?.storeTime || '2026-01-27 21:46:42',
    bornTime: message?.bornTime || '2026-01-27 21:46:42',
    queueId: message?.queueId || '3',
    queueOffset: message?.queueOffset || '288',
    storeSize: message?.storeSize || '264 bytes',
    reconsume: message?.reconsume || '0',
    bodyCRC: message?.bodyCRC || '613185359',
    sysFlag: message?.sysFlag || '0',
    flag: message?.flag || '0',
    transOffset: message?.transOffset || '0',
    // Properties
    msgRegion: 'DefaultRegion',
    uniqKey: message?.msgId || '240E03B350D263C087AA69AB77E798719CBC18B4AAC28A91AB100000',
    cluster: 'DefaultCluster',
    tags: message?.tag || 'TagA',
    wait: 'true',
    traceOn: 'true',
    // User Props
    orderSource: 'mobile_app',
    traceId: 't_88992211',
    userLevel: 'vip_gold',
    clientVersion: 'v5.0.1',
    // Body
    body: message?.body || 'Hello RocketMQ Key: Key_0',
    // Tracking
    consumerGroup: 'please_rename_unique_group_name_4',
    status: 'CONSUMED'
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success("Copied to clipboard");
  };

  const InfoItem = ({ icon: Icon, label, value, mono = false, copyable = false }: any) => (
    <div className="flex items-start p-3 hover:bg-gray-50 dark:hover:bg-gray-800/50 rounded-lg transition-colors group">
      <div className="mt-0.5 mr-3 text-gray-400 dark:text-gray-500">
        <Icon className="w-4 h-4" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-[10px] uppercase font-bold text-gray-400 dark:text-gray-500 tracking-wider mb-0.5">{label}</div>
        <div className={`text-sm text-gray-900 dark:text-gray-200 break-all flex items-center gap-2 ${mono ? 'font-mono' : ''}`}>
          {value}
          {copyable && (
            <button 
              onClick={() => copyToClipboard(value)}
              className="opacity-0 group-hover:opacity-100 transition-opacity p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
            >
              <Copy className="w-3 h-3 text-gray-400 dark:text-gray-500" />
            </button>
          )}
        </div>
      </div>
    </div>
  );

  const PropertyItem = ({ label, value }: any) => (
    <div className="flex items-center justify-between py-2 border-b border-gray-50 dark:border-gray-800 last:border-0">
      <span className="text-xs font-medium text-gray-500 dark:text-gray-400">{label}</span>
      <span className="text-xs font-mono text-gray-700 dark:text-gray-300 bg-gray-50 dark:bg-gray-800/50 px-2 py-0.5 rounded">{value}</span>
    </div>
  );

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 sm:p-6">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
          className="absolute inset-0 bg-gray-900/40 backdrop-blur-sm transition-opacity"
        />
        
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-5xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
        >
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900 sticky top-0 z-10">
            <h2 className="text-lg font-bold text-gray-900 dark:text-white flex items-center gap-2">
              <FileText className="w-5 h-5 text-blue-500" />
              Message Detail
            </h2>
            <button 
              onClick={onClose}
              className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Body Content */}
          <div className="flex-1 overflow-y-auto bg-gray-50/50 dark:bg-gray-950/50 p-6 space-y-6">
            
            {/* Message Info Section */}
            <div className="space-y-3">
              <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Info</h3>
              <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-2 grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-0">
                <InfoItem icon={Database} label="Topic" value={data.topic} mono />
                <InfoItem icon={Globe} label="BornHost" value={data.bornHost} mono />
                
                <InfoItem icon={Hash} label="Message ID" value={data.messageId} mono copyable />
                <InfoItem icon={Clock} label="BornTime" value={data.bornTime} mono />
                
                <InfoItem icon={Server} label="StoreHost" value={data.storeHost} mono />
                <InfoItem icon={Clock} label="StoreTime" value={data.storeTime} mono />
                
                <InfoItem icon={Layers} label="Queue ID" value={data.queueId} mono />
                <InfoItem icon={RefreshCw} label="Reconsume" value={data.reconsume} mono />
                
                <InfoItem icon={HardDrive} label="StoreSize" value={data.storeSize} mono />
                <InfoItem icon={Flag} label="SysFlag" value={data.sysFlag} mono />
                
                <InfoItem icon={Key} label="BodyCRC" value={data.bodyCRC} mono />
                <InfoItem icon={RefreshCw} label="Trans Offset" value={data.transOffset} mono />
                
                <InfoItem icon={Tag} label="Flag" value={data.flag} mono />
                <InfoItem icon={Layers} label="Queue Offset" value={data.queueOffset} mono />
              </div>
            </div>

            {/* Properties Section */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
               {/* System Properties */}
               <div className="space-y-3">
                  <div className="flex items-center justify-between px-1">
                    <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider flex items-center gap-2">
                       <Activity className="w-3.5 h-3.5" /> System Properties
                    </h3>
                    <span className="bg-gray-200 dark:bg-gray-800 text-gray-600 dark:text-gray-400 text-[10px] font-bold px-1.5 py-0.5 rounded">6</span>
                  </div>
                  <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 space-y-1">
                    <PropertyItem label="MSG_REGION" value={data.msgRegion} />
                    <PropertyItem label="UNIQ_KEY" value={data.uniqKey} />
                    <PropertyItem label="CLUSTER" value={data.cluster} />
                    <PropertyItem label="TAGS" value={data.tags} />
                    <PropertyItem label="WAIT" value={data.wait} />
                    <PropertyItem label="TRACE_ON" value={data.traceOn} />
                  </div>
               </div>

               {/* User Properties */}
               <div className="space-y-3">
                  <div className="flex items-center justify-between px-1">
                    <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider flex items-center gap-2">
                       <Users className="w-3.5 h-3.5" /> User Properties
                    </h3>
                    <span className="bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 text-[10px] font-bold px-1.5 py-0.5 rounded">4</span>
                  </div>
                  <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 space-y-1">
                    <PropertyItem label="order_source" value={data.orderSource} />
                    <PropertyItem label="trace_id" value={data.traceId} />
                    <PropertyItem label="user_level" value={data.userLevel} />
                    <PropertyItem label="client_version" value={data.clientVersion} />
                  </div>
               </div>
            </div>

            {/* Message Body */}
            <div className="space-y-3">
               <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Body</h3>
               <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 font-mono text-sm text-gray-800 dark:text-gray-200 overflow-x-auto">
                 {data.body}
               </div>
            </div>

            {/* Message Tracking */}
            <div className="space-y-3">
               <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Tracking</h3>
               <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                  <div className="divide-y divide-gray-100 dark:divide-gray-800">
                    <div className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
                       <div className="flex items-center gap-3">
                          <Users className="w-4 h-4 text-gray-400" />
                          <span className="text-sm font-medium text-gray-500 dark:text-gray-400">Consumer Group</span>
                       </div>
                       <div className="font-mono text-sm font-semibold text-gray-900 dark:text-white">
                          {data.consumerGroup}
                       </div>
                    </div>
                    
                    <div className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
                       <div className="flex items-center gap-3">
                          <Activity className="w-4 h-4 text-gray-400" />
                          <span className="text-sm font-medium text-gray-500 dark:text-gray-400">Status</span>
                       </div>
                       <div>
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400 text-xs font-bold uppercase border border-green-100 dark:border-green-900/30">
                             <CheckCircle2 className="w-3.5 h-3.5" />
                             {data.status}
                          </span>
                       </div>
                    </div>

                    <div className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
                       <div className="flex items-center gap-3">
                          <RotateCcw className="w-4 h-4 text-gray-400" />
                          <span className="text-sm font-medium text-gray-500 dark:text-gray-400">Operation</span>
                       </div>
                       <div>
                          <button 
                            onClick={() => toast.success("Message resend requested")}
                            className="flex items-center px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors shadow-sm"
                          >
                             <RotateCcw className="w-3.5 h-3.5 mr-1.5" />
                             Resend Message
                          </button>
                       </div>
                    </div>
                  </div>
               </div>
            </div>

          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-gray-50 dark:bg-gray-900/50 border-t border-gray-100 dark:border-gray-800 flex justify-end">
            <button
              onClick={onClose}
              className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
            >
              Close
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};
