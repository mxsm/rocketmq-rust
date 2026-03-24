import React, { useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import {
  X,
  FileText,
  Activity,
  Users,
  Copy,
  Database,
  Server,
  Clock,
  HardDrive,
  Flag,
  RefreshCw,
  Hash,
  Tag,
  Globe,
  Key,
  Layers,
  AlertCircle,
  Info,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { MessageService } from '../services/message.service';
import type { MessageDetail, MessageSummary } from '../features/message/types/message.types';

interface MessageDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  message: MessageSummary | null;
}

export const MessageDetailModal = ({ isOpen, onClose, message }: MessageDetailModalProps) => {
  const [detail, setDetail] = useState<MessageDetail | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [consumingGroup, setConsumingGroup] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen || !message) {
      setDetail(null);
      setError('');
      setIsLoading(false);
      setConsumingGroup(null);
      return;
    }

    let cancelled = false;
    setIsLoading(true);
    setError('');
    setDetail(null);

    void MessageService.viewMessageDetail({
      topic: message.topic,
      messageId: message.queryMsgId,
    })
      .then((result) => {
        if (!cancelled) {
          setDetail(result);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : 'Failed to load message detail');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [isOpen, message]);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast.success("Copied to clipboard");
  };

  const handleDirectConsume = async (consumerGroup: string) => {
    if (!detail) {
      return;
    }

    const confirmed = window.confirm(
      `Request direct consume for message ${detail.msgId} in consumer group ${consumerGroup}?`,
    );
    if (!confirmed) {
      return;
    }

    setConsumingGroup(consumerGroup);

    try {
      const result = await MessageService.consumeMessageDirectly({
        topic: detail.topic,
        consumerGroup,
        messageId: detail.msgId,
      });

      if (result.success) {
        toast.success(result.message);
      } else {
        toast.error(result.message);
      }
    } catch (consumeError) {
      const messageText =
        consumeError instanceof Error ? consumeError.message : 'Failed to request direct consume.';
      toast.error(messageText);
    } finally {
      setConsumingGroup(null);
    }
  };

  const formatTimestamp = (value?: number | null) => {
    if (!value) {
      return '-';
    }
    return new Date(value).toLocaleString();
  };

  const systemPropertyEntries = useMemo(() => {
    if (!detail) {
      return [];
    }
    return Object.entries(detail.properties).filter(([key]) => key === key.toUpperCase());
  }, [detail]);

  const userPropertyEntries = useMemo(() => {
    if (!detail) {
      return [];
    }
    return Object.entries(detail.properties).filter(([key]) => key !== key.toUpperCase());
  }, [detail]);

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

  if (!isOpen) return null;

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
            {isLoading ? (
              <div className="flex flex-col items-center justify-center rounded-xl border border-gray-200 bg-white px-6 py-20 text-center shadow-sm dark:border-gray-800 dark:bg-gray-900">
                <RefreshCw className="mb-4 h-8 w-8 animate-spin text-blue-500" />
                <p className="text-sm font-medium text-gray-700 dark:text-gray-200">Loading message detail...</p>
              </div>
            ) : error ? (
              <div className="flex items-start gap-3 rounded-xl border border-red-200 bg-red-50 px-4 py-4 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200">
                <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                <div>{error}</div>
              </div>
            ) : detail ? (
              <>
                <div className="space-y-3">
                  <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Info</h3>
                  <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-2 grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-0">
                    <InfoItem icon={Database} label="Topic" value={detail.topic} mono />
                    <InfoItem icon={Globe} label="BornHost" value={detail.bornHost || '-'} mono />

                    <InfoItem icon={Hash} label="Message ID" value={detail.msgId} mono copyable />
                    <InfoItem icon={Clock} label="BornTime" value={formatTimestamp(detail.bornTimestamp)} mono />

                    <InfoItem icon={Server} label="StoreHost" value={detail.storeHost || '-'} mono />
                    <InfoItem icon={Clock} label="StoreTime" value={formatTimestamp(detail.storeTimestamp)} mono />

                    <InfoItem icon={Layers} label="Queue ID" value={String(detail.queueId ?? '-')} mono />
                    <InfoItem icon={RefreshCw} label="Reconsume" value={String(detail.reconsumeTimes ?? '-')} mono />

                    <InfoItem icon={HardDrive} label="StoreSize" value={detail.storeSize ? `${detail.storeSize} bytes` : '-'} mono />
                    <InfoItem icon={Flag} label="SysFlag" value={String(detail.sysFlag ?? '-')} mono />

                    <InfoItem icon={Key} label="BodyCRC" value={String(detail.bodyCrc ?? '-')} mono />
                    <InfoItem icon={RefreshCw} label="Trans Offset" value={String(detail.preparedTransactionOffset ?? '-')} mono />

                    <InfoItem icon={Tag} label="Flag" value={String(detail.flag ?? '-')} mono />
                    <InfoItem icon={Layers} label="Queue Offset" value={String(detail.queueOffset ?? '-')} mono />
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between px-1">
                      <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider flex items-center gap-2">
                        <Activity className="w-3.5 h-3.5" /> System Properties
                      </h3>
                      <span className="bg-gray-200 dark:bg-gray-800 text-gray-600 dark:text-gray-400 text-[10px] font-bold px-1.5 py-0.5 rounded">
                        {systemPropertyEntries.length}
                      </span>
                    </div>
                    <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 space-y-1">
                      {systemPropertyEntries.length > 0 ? systemPropertyEntries.map(([key, value]) => (
                        <PropertyItem key={key} label={key} value={value} />
                      )) : (
                        <div className="text-sm text-gray-500 dark:text-gray-400">No system properties</div>
                      )}
                    </div>
                  </div>

                  <div className="space-y-3">
                    <div className="flex items-center justify-between px-1">
                      <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider flex items-center gap-2">
                        <Users className="w-3.5 h-3.5" /> User Properties
                      </h3>
                      <span className="bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 text-[10px] font-bold px-1.5 py-0.5 rounded">
                        {userPropertyEntries.length}
                      </span>
                    </div>
                    <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 space-y-1">
                      {userPropertyEntries.length > 0 ? userPropertyEntries.map(([key, value]) => (
                        <PropertyItem key={key} label={key} value={value} />
                      )) : (
                        <div className="text-sm text-gray-500 dark:text-gray-400">No user properties</div>
                      )}
                    </div>
                  </div>
                </div>

                <div className="space-y-3">
                  <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Body</h3>
                  <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4 space-y-3">
                    {detail.bodyText !== null && detail.bodyText !== undefined ? (
                      <div className="font-mono text-sm text-gray-800 dark:text-gray-200 overflow-x-auto whitespace-pre-wrap break-all">
                        {detail.bodyText}
                      </div>
                    ) : (
                      <div className="space-y-2">
                        <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200">
                          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                          <span>消息体不是有效 UTF-8，当前以 base64 形式展示。</span>
                        </div>
                        <div className="font-mono text-xs text-gray-800 dark:text-gray-200 overflow-x-auto break-all">
                          {detail.bodyBase64 || '-'}
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                <div className="space-y-3">
                  <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider px-1">Message Tracking</h3>
                  <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm p-4">
                    {detail.messageTrackList && detail.messageTrackList.length > 0 ? (
                      <div className="space-y-2">
                        {detail.messageTrackList.map((track) => (
                          <div key={`${track.consumerGroup}-${track.trackType}`} className="rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 text-sm dark:border-gray-800 dark:bg-gray-800/40">
                            <div className="font-medium text-gray-900 dark:text-white">{track.consumerGroup}</div>
                            <div className="mt-1 text-gray-600 dark:text-gray-300">{track.trackType}</div>
                            <button
                              onClick={() => void handleDirectConsume(track.consumerGroup)}
                              disabled={consumingGroup === track.consumerGroup}
                              className="mt-3 inline-flex items-center gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-medium text-amber-700 shadow-sm transition-all hover:border-amber-300 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-60 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200 dark:hover:border-amber-800 dark:hover:bg-amber-900/30"
                            >
                              <RefreshCw className={`h-3.5 w-3.5 ${consumingGroup === track.consumerGroup ? 'animate-spin' : ''}`} />
                              {consumingGroup === track.consumerGroup ? 'Requesting...' : 'Resend'}
                            </button>
                            {track.exceptionDesc && (
                              <div className="mt-1 font-mono text-xs text-red-600 dark:text-red-300 break-all">{track.exceptionDesc}</div>
                            )}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="flex items-start gap-3 rounded-xl border border-blue-200 bg-blue-50 px-4 py-4 text-sm text-blue-800 dark:border-blue-900/40 dark:bg-blue-900/20 dark:text-blue-200">
                        <Info className="mt-0.5 h-4 w-4 shrink-0 text-blue-600 dark:text-blue-400" />
                        <div>No consumer track records matched the current message.</div>
                      </div>
                    )}
                  </div>
                </div>
              </>
            ) : null}

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
