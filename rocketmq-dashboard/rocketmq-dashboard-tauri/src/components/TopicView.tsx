import React, {useEffect, useState, ReactNode} from 'react';
import {motion, AnimatePresence} from 'motion/react';
import {
    Activity,
    Network,
    Users,
    Settings,
    Send,
    RotateCcw,
    FastForward,
    Trash2,
    FileBox,
    Clock,
    Layers,
    ArrowRightLeft,
    HelpCircle,
    AlertTriangle,
    Shield,
    Filter,
    Search,
    Plus,
    RefreshCw,
    MoreHorizontal,
    Database,
    X,
    Server,
    FileText,
    ChevronDown,
    Check,
    Save,
    ArrowDownCircle,
    ArrowUpCircle,
    Tag,
    Key
} from 'lucide-react';
import {toast} from 'sonner@2.0.3';
import {Button} from '../components/ui/LegacyButton';
import {Pagination} from './Pagination';
import {useTopicCatalog} from '../features/topic/hooks/useTopicCatalog';
import {TopicService} from '../services/topic.service';
import type {
    TopicCategory,
    TopicConfigView,
    TopicListItem,
    TopicRouteView,
    TopicSendMessageResult,
    TopicStatusView,
} from '../features/topic/types/topic.types';

interface Topic {
    name: string;
    type: string;
    operations: string[];
}

const TOPIC_PAGE_SIZE = 10;

const TOPIC_FILTER_ORDER: TopicCategory[] = [
    'NORMAL',
    'DELAY',
    'FIFO',
    'TRANSACTION',
    'UNSPECIFIED',
    'RETRY',
    'DLQ',
    'SYSTEM',
];

const buildDefaultTopicFilters = (): Record<TopicCategory, boolean> => ({
    NORMAL: true,
    DELAY: false,
    FIFO: true,
    TRANSACTION: true,
    UNSPECIFIED: true,
    RETRY: true,
    DLQ: true,
    SYSTEM: true,
});

const buildTopicOperations = (item: TopicListItem): string[] => {
    const baseOperations = ['Status', 'Router', 'Consumer Manage', 'Topic Config'];

    if (item.systemTopic) {
        return baseOperations;
    }

    if (item.category === 'RETRY' || item.category === 'DLQ') {
        return [...baseOperations, 'Delete'];
    }

    return [
        ...baseOperations,
        'Send Message',
        'Reset Consumer Offset',
        'Skip Message Accumulate',
        'Delete',
    ];
};

const mapTopicListItem = (item: TopicListItem): Topic => ({
    name: item.topic,
    type: item.category,
    operations: buildTopicOperations(item),
});

interface TopicRouterModalProps {
    isOpen: boolean;
    onClose: () => void;
    topic: Topic | null;
}

const TopicRouterModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [routeData, setRouteData] = useState<TopicRouteView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadRoute = async () => {
            setIsLoading(true);
            setError('');

            try {
                const result = await TopicService.getTopicRoute({topic: topic.name});
                if (!cancelled) {
                    setRouteData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setRouteData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic route.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadRoute();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const brokers = routeData?.brokers ?? [];
    const queues = routeData?.queues ?? [];

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 1}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-gray-900/60 backdrop-blur-sm transition-opacity"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-4xl bg-white dark:bg-gray-900 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 bg-white dark:bg-gray-900 flex items-center justify-between border-b border-gray-100 dark:border-gray-800">
                        <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                            {topic?.name}Router
                        </h3>
                        <button
                            onClick={onClose}
                            className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 overflow-y-auto space-y-8 bg-gray-50/50 dark:bg-gray-950/50">
                        {isLoading && (
                            <div className="rounded-xl border border-gray-200 bg-white/80 px-6 py-10 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                Loading topic route data from the current NameServer...
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="rounded-xl border border-red-200 bg-red-50/80 px-6 py-6 text-sm text-red-600 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                {error}
                            </div>
                        )}

                        {/* Broker Datas Section */}
                        {!isLoading && !error && (
                        <div>
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                                <Server className="w-4 h-4 text-gray-500 dark:text-gray-400"/>
                                Broker Datas
                            </h4>
                            {brokers.length === 0 ? (
                                <div className="rounded-xl border border-dashed border-gray-200 bg-white/80 px-6 py-8 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                    No broker route data was returned for this topic.
                                </div>
                            ) : (
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {brokers.map((broker, i) => (
                                    <div key={i}
                                         className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm hover:shadow-md transition-shadow">
                                        <div className="flex items-center justify-between mb-4">
                                            <div className="flex flex-col gap-1">
                                                <span className="text-base font-bold text-gray-900 dark:text-white">{broker.brokerName}</span>
                                                <span className="text-xs text-gray-500 dark:text-gray-400">{broker.clusterName}</span>
                                            </div>
                                            <span
                                                className="text-xs bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 px-2.5 py-1 rounded-full font-medium border border-gray-200 dark:border-gray-700">
                        {broker.addresses.length} Addrs
                      </span>
                                        </div>
                                        <div className="space-y-2">
                                            {broker.addresses.map((addr, j) => (
                                                <div key={j}
                                                     className="flex items-center justify-between text-xs bg-gray-50 dark:bg-gray-800/50 p-3 rounded-lg border border-gray-100 dark:border-gray-800/50 font-mono">
                                                    <span className="text-gray-500 dark:text-gray-500 font-semibold">ID: {addr.brokerId}</span>
                                                    <span className="text-gray-700 dark:text-gray-300">{addr.address}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </div>
                            )}
                        </div>
                        )}

                        {/* Queue Datas Section */}
                        {!isLoading && !error && (
                        <div>
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                                <Database className="w-4 h-4 text-gray-500 dark:text-gray-400"/>
                                Queue Datas
                            </h4>
                            {queues.length === 0 ? (
                                <div className="rounded-xl border border-dashed border-gray-200 bg-white/80 px-6 py-8 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                    No queue route data was returned for this topic.
                                </div>
                            ) : (
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                {queues.map((queue, i) => (
                                    <div key={i}
                                         className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm hover:shadow-md transition-shadow flex flex-col">
                                        <div className="flex items-center space-x-3 mb-5">
                                            <div
                                                className="w-8 h-8 rounded-lg bg-blue-50 dark:bg-blue-900/20 flex items-center justify-center text-blue-600 dark:text-blue-400 border border-blue-100 dark:border-blue-900/30">
                                                <Server className="w-4 h-4"/>
                                            </div>
                                            <span className="text-sm font-bold text-gray-900 dark:text-white">{queue.brokerName}</span>
                                        </div>

                                        <div className="grid grid-cols-2 gap-4 mb-5 flex-1">
                                            <div
                                                className="bg-gray-50 dark:bg-gray-800/50 p-3 rounded-xl border border-gray-100 dark:border-gray-800 text-center group hover:border-blue-200 dark:hover:border-blue-800 transition-colors">
                                                <div className="text-[10px] uppercase text-gray-400 dark:text-gray-500 font-bold mb-1 tracking-wider">Read</div>
                                                <div
                                                    className="text-xl font-mono font-bold text-gray-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">{queue.readQueueNums}</div>
                                            </div>
                                            <div
                                                className="bg-gray-50 dark:bg-gray-800/50 p-3 rounded-xl border border-gray-100 dark:border-gray-800 text-center group hover:border-blue-200 dark:hover:border-blue-800 transition-colors">
                                                <div className="text-[10px] uppercase text-gray-400 dark:text-gray-500 font-bold mb-1 tracking-wider">Write
                                                </div>
                                                <div
                                                    className="text-xl font-mono font-bold text-gray-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">{queue.writeQueueNums}</div>
                                            </div>
                                        </div>

                                        <div className="flex items-center justify-between pt-4 border-t border-gray-100 dark:border-gray-800 mt-auto">
                                            <span className="text-xs text-gray-500 dark:text-gray-400 font-medium">Permission</span>
                                            <span
                                                className="text-xs font-bold text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/30 px-2.5 py-1 rounded-md border border-blue-100 dark:border-blue-900/30">
                         Perm: {queue.perm}
                       </span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                            )}
                        </div>
                        )}

                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const LegacyTopicConfigModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [selectedBrokers, setSelectedBrokers] = useState(['mxsm']);
    const [isBrokerDropdownOpen, setIsBrokerDropdownOpen] = useState(false);
    // Mock broker data
    const brokers = ['mxsm', 'broker-a', 'broker-b', 'broker-c'];

    const toggleBroker = (broker: string) => {
        if (selectedBrokers.includes(broker)) {
            setSelectedBrokers(selectedBrokers.filter(b => b !== broker));
        } else {
            setSelectedBrokers([...selectedBrokers, broker]);
        }
    };

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-2xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Settings className="w-5 h-5 mr-2 text-blue-500"/>
                                Topic Configuration
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                Manage settings for <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name || 'undefined'}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content - Scrollable */}
                    <div className="p-6 overflow-y-auto bg-gray-50/50 dark:bg-gray-950/50 space-y-6" onClick={() => setIsBrokerDropdownOpen(false)}>

                        {/* Card 1: Deployment Target */}
                        <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                <Server className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Deployment Target
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                                <div>
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Cluster Name</label>
                                    <div className="relative">
                                        <select
                                            className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer hover:bg-white dark:hover:bg-gray-750 hover:shadow-sm">
                                            <option>DefaultCluster</option>
                                        </select>
                                        <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                    </div>
                                </div>
                                <div className="relative">
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Broker Name (Multi-select)</label>
                                    <div
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            setIsBrokerDropdownOpen(!isBrokerDropdownOpen);
                                        }}
                                        className="w-full min-h-[42px] px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 cursor-pointer hover:border-blue-500 dark:hover:border-blue-500 hover:shadow-sm flex flex-wrap gap-1.5 items-center transition-all relative"
                                    >
                                        {selectedBrokers.length === 0 ? (
                                            <span className="text-gray-400 dark:text-gray-500">Select brokers...</span>
                                        ) : (
                                            selectedBrokers.map(b => (
                                                <span key={b}
                                                      className="bg-blue-50 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded text-xs border border-blue-100 dark:border-blue-800 flex items-center font-medium">
                               {b}
                                                    <X
                                                        className="w-3 h-3 ml-1 hover:text-blue-900 dark:hover:text-blue-100 cursor-pointer"
                                                        onClick={(e) => {
                                                            e.stopPropagation();
                                                            toggleBroker(b);
                                                        }}
                                                    />
                             </span>
                                            ))
                                        )}
                                        <ChevronDown
                                            className={`absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none transition-transform duration-200 ${isBrokerDropdownOpen ? 'rotate-180' : ''}`}/>
                                    </div>

                                    {/* Dropdown Menu */}
                                    <AnimatePresence>
                                        {isBrokerDropdownOpen && (
                                            <motion.div
                                                initial={{opacity: 0, y: 5}}
                                                animate={{opacity: 1, y: 0}}
                                                exit={{opacity: 0, y: 5}}
                                                className="absolute z-20 top-full left-0 right-0 mt-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-xl max-h-48 overflow-y-auto"
                                                onClick={(e) => e.stopPropagation()}
                                            >
                                                {brokers.map(broker => (
                                                    <div
                                                        key={broker}
                                                        onClick={() => toggleBroker(broker)}
                                                        className="px-3 py-2.5 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer flex items-center space-x-3 border-b border-gray-50 dark:border-gray-700 last:border-0"
                                                    >
                                                        <div
                                                            className={`w-4 h-4 border rounded flex items-center justify-center transition-colors ${selectedBrokers.includes(broker) ? 'bg-blue-600 border-blue-600 dark:bg-blue-500 dark:border-blue-500' : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900'}`}>
                                                            {selectedBrokers.includes(broker) && <Check className="w-3 h-3 text-white"/>}
                                                        </div>
                                                        <span
                                                            className={`text-sm ${selectedBrokers.includes(broker) ? 'text-gray-900 dark:text-white font-medium' : 'text-gray-600 dark:text-gray-300'}`}>
                                {broker}
                              </span>
                                                    </div>
                                                ))}
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            </div>
                        </div>

                        {/* Card 2: Topic Definition */}
                        <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                <FileText className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Topic Definition
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                                <div className="md:col-span-2">
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">
                                        Topic Name <span className="text-red-500">*</span>
                                    </label>
                                    <input
                                        type="text"
                                        value={topic?.name || 'TopicTest'}
                                        readOnly
                                        className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono"
                                    />
                                </div>
                                <div className="md:col-span-2">
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Message Type</label>
                                    <div className="relative">
                                        <select
                                            className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer shadow-sm">
                                            <option>UNSPECIFIED</option>
                                            <option>NORMAL</option>
                                            <option>FIFO</option>
                                            <option>DELAY</option>
                                            <option>TRANSACTION</option>
                                        </select>
                                        <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Card 3: Queue Configuration */}
                        <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                <Database className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Queue Configuration
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                                <div>
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">
                                        Write Queues <span className="text-red-500">*</span>
                                    </label>
                                    <input
                                        type="number"
                                        defaultValue={4}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all shadow-sm"
                                    />
                                </div>
                                <div>
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">
                                        Read Queues <span className="text-red-500">*</span>
                                    </label>
                                    <input
                                        type="number"
                                        defaultValue={4}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all shadow-sm"
                                    />
                                </div>
                                <div>
                                    <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">
                                        Permission (Perm) <span className="text-red-500">*</span>
                                    </label>
                                    <input
                                        type="number"
                                        defaultValue={6}
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all shadow-sm"
                                    />
                                </div>
                            </div>
                        </div>

                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={() => {
                                toast.success(`Topic changes committed for ${selectedBrokers.length} brokers`);
                                onClose();
                            }}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            <Save className="w-4 h-4 mr-2"/>
                            Commit Changes
                        </button>
                    </div>

                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TOPIC_CONFIG_FIELD_LABELS: Record<string, string> = {
    readQueueNums: 'Read Queues',
    writeQueueNums: 'Write Queues',
    perm: 'Permission',
    order: 'Ordered Delivery',
    messageType: 'Message Type',
};

const formatAttributeLabel = (key: string) =>
    key
        .split(/[._-]/g)
        .filter(Boolean)
        .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
        .join(' ');

const TopicConfigModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [configData, setConfigData] = useState<TopicConfigView | null>(null);
    const [selectedBroker, setSelectedBroker] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadConfig = async () => {
            setIsLoading(true);
            setError('');

            try {
                const result = await TopicService.getTopicConfig({
                    topic: topic.name,
                    brokerName: selectedBroker,
                });
                if (!cancelled) {
                    setConfigData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setConfigData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic configuration.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadConfig();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name, selectedBroker]);

    useEffect(() => {
        if (!isOpen) {
            return;
        }
        setSelectedBroker(null);
        setConfigData(null);
        setError('');
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const brokerOptions = configData?.brokerNameList ?? [];
    const clusterOptions = configData?.clusterNameList ?? [];
    const attributes = Object.entries(configData?.attributes ?? {}).sort(([left], [right]) => left.localeCompare(right));
    const inconsistentFields = configData?.inconsistentFields ?? [];
    const activeBroker = selectedBroker ?? configData?.brokerName ?? '';

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-4xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
                >
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Settings className="w-5 h-5 mr-2 text-blue-500"/>
                                Topic Configuration
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                Inspect the current broker-side config for <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name || 'undefined'}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    <div className="p-6 overflow-y-auto bg-gray-50/50 dark:bg-gray-950/50 space-y-6">
                        {isLoading && (
                            <div className="rounded-xl border border-gray-200 bg-white/80 px-6 py-10 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                Loading topic configuration from the current broker view...
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="rounded-xl border border-red-200 bg-red-50/80 px-6 py-6 text-sm text-red-600 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                {error}
                            </div>
                        )}

                        {!isLoading && !error && configData && (
                            <>
                                <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                                    <div className="flex flex-col gap-4">
                                        <div className="flex flex-col gap-1">
                                            <h4 className="text-sm font-bold text-gray-900 dark:text-white flex items-center uppercase tracking-wider">
                                                <Server className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Deployment Target
                                            </h4>
                                            <p className="text-xs text-gray-500 dark:text-gray-400">
                                                Switch the anchor broker to inspect the exact config returned by that broker.
                                            </p>
                                        </div>

                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                                            <div>
                                                <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Cluster Names</label>
                                                <div className="min-h-[42px] px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg flex flex-wrap gap-1.5 items-center">
                                                    {clusterOptions.map((clusterName) => (
                                                        <span
                                                            key={clusterName}
                                                            className="bg-blue-50 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded text-xs border border-blue-100 dark:border-blue-800 font-medium"
                                                        >
                                                            {clusterName}
                                                        </span>
                                                    ))}
                                                </div>
                                            </div>
                                            <div>
                                                <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Anchor Broker</label>
                                                <div className="relative">
                                                    <select
                                                        value={activeBroker}
                                                        onChange={(event) => setSelectedBroker(event.target.value)}
                                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer shadow-sm"
                                                    >
                                                        {brokerOptions.map((brokerName) => (
                                                            <option key={brokerName} value={brokerName}>
                                                                {brokerName}
                                                            </option>
                                                        ))}
                                                    </select>
                                                    <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="flex flex-wrap gap-1.5">
                                            {brokerOptions.map((brokerName) => (
                                                <button
                                                    key={brokerName}
                                                    type="button"
                                                    onClick={() => setSelectedBroker(brokerName)}
                                                    className={`px-2 py-1 rounded text-xs border transition-colors ${
                                                        brokerName === activeBroker
                                                            ? 'bg-blue-600 border-blue-600 text-white dark:bg-blue-500 dark:border-blue-500'
                                                            : 'bg-gray-50 border-gray-200 text-gray-600 dark:bg-gray-800 dark:border-gray-700 dark:text-gray-300'
                                                    }`}
                                                >
                                                    {brokerName}
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                </div>

                                {inconsistentFields.length > 0 && (
                                    <div className="rounded-xl border border-amber-200 bg-amber-50/80 px-5 py-4 text-sm text-amber-700 shadow-sm dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-300">
                                        <div className="flex items-center gap-2 font-semibold">
                                            <AlertTriangle className="w-4 h-4"/>
                                            Broker configs are not fully aligned
                                        </div>
                                        <div className="mt-2 flex flex-wrap gap-2">
                                            {inconsistentFields.map((field) => (
                                                <span
                                                    key={field}
                                                    className="rounded-full border border-amber-300/80 bg-white/80 px-2 py-0.5 text-xs dark:border-amber-800 dark:bg-amber-950/40"
                                                >
                                                    {TOPIC_CONFIG_FIELD_LABELS[field] ?? field}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                                    <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                        <FileText className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Topic Definition
                                    </h4>
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                                        <div className="md:col-span-2">
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Topic Name</label>
                                            <input
                                                type="text"
                                                value={configData.topicName}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 font-mono"
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Message Type</label>
                                            <input
                                                type="text"
                                                value={configData.messageType}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 font-mono"
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Ordered Delivery</label>
                                            <input
                                                type="text"
                                                value={configData.order ? 'Enabled' : 'Disabled'}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400"
                                            />
                                        </div>
                                    </div>
                                </div>

                                <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                                    <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                        <Database className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Queue Configuration
                                    </h4>
                                    <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                                        <div>
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Write Queues</label>
                                            <input
                                                type="text"
                                                value={String(configData.writeQueueNums)}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 font-mono"
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Read Queues</label>
                                            <input
                                                type="text"
                                                value={String(configData.readQueueNums)}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 font-mono"
                                            />
                                        </div>
                                        <div>
                                            <label className="block text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">Permission</label>
                                            <input
                                                type="text"
                                                value={String(configData.perm)}
                                                readOnly
                                                className="w-full px-3 py-2.5 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 font-mono"
                                            />
                                        </div>
                                    </div>
                                </div>

                                <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">
                                    <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center uppercase tracking-wider">
                                        <Key className="w-4 h-4 mr-2 text-gray-400 dark:text-gray-500"/> Topic Attributes
                                    </h4>
                                    {attributes.length === 0 ? (
                                        <div className="rounded-lg border border-dashed border-gray-200 bg-gray-50/80 px-4 py-6 text-center text-sm text-gray-500 dark:border-gray-700 dark:bg-gray-800/60 dark:text-gray-400">
                                            No extra topic attributes were returned for this broker.
                                        </div>
                                    ) : (
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                            {attributes.map(([key, value]) => (
                                                <div
                                                    key={key}
                                                    className="rounded-lg border border-gray-200 bg-gray-50/80 px-4 py-3 dark:border-gray-800 dark:bg-gray-800/50"
                                                >
                                                    <div className="text-xs font-semibold uppercase tracking-wider text-gray-500 dark:text-gray-400">
                                                        {formatAttributeLabel(key)}
                                                    </div>
                                                    <div className="mt-2 break-all font-mono text-sm text-gray-800 dark:text-gray-200">
                                                        {value}
                                                    </div>
                                                    <div className="mt-1 text-[11px] text-gray-400 dark:text-gray-500">
                                                        {key}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </>
                        )}
                    </div>

                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            disabled
                            className="px-6 py-2 bg-gray-200 text-gray-500 rounded-lg text-sm font-medium cursor-not-allowed flex items-center dark:bg-gray-800 dark:text-gray-500 dark:border dark:border-gray-700"
                        >
                            <Save className="w-4 h-4 mr-2"/>
                            Edit In Next Step
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicStatusModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [statusData, setStatusData] = useState<TopicStatusView | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        if (!isOpen || !topic?.name) {
            return;
        }

        let cancelled = false;

        const loadStatus = async () => {
            setIsLoading(true);
            setError('');

            try {
                const result = await TopicService.getTopicStats({topic: topic.name});
                if (!cancelled) {
                    setStatusData(result);
                }
            } catch (loadError) {
                if (!cancelled) {
                    setStatusData(null);
                    setError(loadError instanceof Error ? loadError.message : 'Failed to load topic status.');
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        void loadStatus();

        return () => {
            cancelled = true;
        };
    }, [isOpen, topic?.name]);

    if (!isOpen) return null;

    const offsets = statusData?.offsets ?? [];
    const totalMinOffset = offsets.reduce((acc, curr) => acc + curr.minOffset, 0);
    const totalMaxOffset = offsets.reduce((acc, curr) => acc + curr.maxOffset, 0);
    const queueCount = statusData?.queueCount ?? offsets.length;
    const greatestOffset = offsets.reduce((max, row) => Math.max(max, row.maxOffset, row.minOffset), 0);

    const formatTimestamp = (timestamp: number) => {
        if (!timestamp || timestamp <= 0) {
            return 'Never updated';
        }
        return new Intl.DateTimeFormat('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        }).format(new Date(timestamp));
    };

    const formatTimeOnly = (timestamp: number) => {
        if (!timestamp || timestamp <= 0) {
            return '--:--:--';
        }
        return new Intl.DateTimeFormat('zh-CN', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        }).format(new Date(timestamp));
    };

    const calcBarWidth = (value: number) => {
        if (greatestOffset <= 0) {
            return '0%';
        }
        return `${Math.max(4, Math.round((value / greatestOffset) * 100))}%`;
    };

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-5xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Activity className="w-5 h-5 mr-2 text-blue-500"/>
                                Topic Status
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                Real-time offset status for <span
                                className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name || 'OrderPlaced_TOPIC'}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 overflow-auto bg-gray-50/50 dark:bg-gray-950/50 flex-1 space-y-6">
                        {/* Summary Cards */}
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div
                                className="bg-white dark:bg-gray-900 p-4 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm flex items-center justify-between">
                                <div>
                                    <p className="text-xs font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider">Total Queues</p>
                                    <p className="text-2xl font-bold text-gray-900 dark:text-white mt-1">{queueCount}</p>
                                </div>
                                <div
                                    className="w-10 h-10 rounded-lg bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400">
                                    <Layers className="w-5 h-5"/>
                                </div>
                            </div>
                            <div
                                className="bg-white dark:bg-gray-900 p-4 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm flex items-center justify-between">
                                <div>
                                    <p className="text-xs font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider">Total Min Offset</p>
                                    <p className="text-2xl font-bold text-gray-900 dark:text-white mt-1 font-mono">{totalMinOffset.toLocaleString()}</p>
                                </div>
                                <div
                                    className="w-10 h-10 rounded-lg bg-green-50 dark:bg-green-900/30 flex items-center justify-center text-green-600 dark:text-green-400">
                                    <ArrowDownCircle className="w-5 h-5"/>
                                </div>
                            </div>
                            <div
                                className="bg-white dark:bg-gray-900 p-4 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm flex items-center justify-between">
                                <div>
                                    <p className="text-xs font-bold text-gray-400 dark:text-gray-500 uppercase tracking-wider">Total Max Offset</p>
                                    <p className="text-2xl font-bold text-gray-900 dark:text-white mt-1 font-mono">{totalMaxOffset.toLocaleString()}</p>
                                </div>
                                <div
                                    className="w-10 h-10 rounded-lg bg-purple-50 dark:bg-purple-900/30 flex items-center justify-center text-purple-600 dark:text-purple-400">
                                    <ArrowUpCircle className="w-5 h-5"/>
                                </div>
                            </div>
                        </div>

                        {isLoading && (
                            <div className="rounded-xl border border-gray-200 bg-white/80 px-6 py-10 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                Loading real-time topic status from the current NameServer...
                            </div>
                        )}

                        {!isLoading && error && (
                            <div className="rounded-xl border border-red-200 bg-red-50/80 px-6 py-6 text-sm text-red-600 shadow-sm dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                {error}
                            </div>
                        )}

                        {!isLoading && !error && offsets.length === 0 && (
                            <div className="rounded-xl border border-dashed border-gray-200 bg-white/80 px-6 py-10 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/80 dark:text-gray-400">
                                No queue offsets were returned for this topic.
                            </div>
                        )}

                        {!isLoading && !error && offsets.length > 0 && (
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                {offsets.map((row, i) => (
                                    <div key={`${row.brokerName}-${row.queueId}-${i}`}
                                         className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm hover:shadow-md transition-shadow duration-200 overflow-hidden flex flex-col">

                                        <div
                                            className="px-4 py-3 border-b border-gray-50 dark:border-gray-800 flex items-center justify-between bg-gray-50/30 dark:bg-gray-800/30">
                                            <div className="flex items-center space-x-2">
                                                <div
                                                    className="w-6 h-6 rounded bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 flex items-center justify-center text-xs font-bold text-gray-700 dark:text-gray-300 shadow-sm">
                                                    {row.queueId}
                                                </div>
                                                <span className="text-sm font-semibold text-gray-900 dark:text-white">{row.brokerName}</span>
                                            </div>
                                            <span
                                                className="text-[10px] font-mono text-gray-400 dark:text-gray-500 bg-white dark:bg-gray-800 px-1.5 py-0.5 rounded border border-gray-100 dark:border-gray-700">
                        ID: {row.queueId}
                     </span>
                                        </div>

                                        <div className="p-4 space-y-4 flex-1">
                                            <div className="space-y-1">
                                                <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                                                    <span>Min Offset</span>
                                                    <span className="font-mono text-gray-900 dark:text-white">{row.minOffset.toLocaleString()}</span>
                                                </div>
                                                <div className="w-full bg-gray-100 dark:bg-gray-800 rounded-full h-1.5 overflow-hidden">
                                                    <div className="bg-green-500 h-1.5 rounded-full" style={{width: calcBarWidth(row.minOffset)}}></div>
                                                </div>
                                            </div>
                                            <div className="space-y-1">
                                                <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                                                    <span>Max Offset</span>
                                                    <span className="font-mono text-gray-900 dark:text-white">{row.maxOffset.toLocaleString()}</span>
                                                </div>
                                                <div className="w-full bg-gray-100 dark:bg-gray-800 rounded-full h-1.5 overflow-hidden">
                                                    <div className="bg-purple-500 h-1.5 rounded-full" style={{width: calcBarWidth(row.maxOffset)}}></div>
                                                </div>
                                            </div>
                                            <div className="rounded-lg border border-gray-100 bg-gray-50/80 px-3 py-2 text-xs text-gray-500 dark:border-gray-800 dark:bg-gray-800/40 dark:text-gray-400">
                                                <div className="flex items-center justify-between gap-3">
                                                    <span>Last Updated</span>
                                                    <span className="font-mono text-right text-gray-700 dark:text-gray-300">{formatTimestamp(row.lastUpdateTimestamp)}</span>
                                                </div>
                                            </div>
                                        </div>

                                        <div
                                            className="px-4 py-2 bg-gray-50 dark:bg-gray-800/50 border-t border-gray-100 dark:border-gray-800 text-[10px] text-gray-400 dark:text-gray-500 flex items-center justify-between">
                    <span className="flex items-center">
                       <Clock className="w-3 h-3 mr-1"/>
                       Updated
                    </span>
                                            <span className="font-mono">{formatTimeOnly(row.lastUpdateTimestamp)}</span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 text-sm font-medium transition-colors shadow-sm"
                        >
                            Close
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicConsumerManageModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    if (!isOpen) return null;

    const subscriptionGroups = [
        {
            groupName: 'please_rename_unique_group_name_4',
            diffTotal: 1000,
            lastTimeStamp: '2026-01-21 12:24:40',
            details: [
                {broker: 'mxsm', queue: 0, client: '', brokerOffset: 286, consumerOffset: 36, diffTotal: 250, lastTimeStamp: '2026-01-21 12:24:40'},
                {broker: 'mxsm', queue: 1, client: '', brokerOffset: 284, consumerOffset: 34, diffTotal: 250, lastTimeStamp: '2026-01-21 12:24:40'},
                {broker: 'mxsm', queue: 2, client: '', brokerOffset: 284, consumerOffset: 34, diffTotal: 250, lastTimeStamp: '2026-01-21 12:24:40'},
                {broker: 'mxsm', queue: 3, client: '', brokerOffset: 288, consumerOffset: 38, diffTotal: 250, lastTimeStamp: '2026-01-21 12:24:40'}
            ]
        }
    ];

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-6xl bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh] border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Users className="w-5 h-5 mr-2 text-blue-500"/>
                                Consumer Manage
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                Manage consumer groups and view lag for <span
                                className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 overflow-auto bg-gray-50/50 dark:bg-gray-950/50 space-y-6">
                        {subscriptionGroups.map((group, idx) => (
                            <div key={idx}
                                 className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm overflow-hidden">
                                {/* Group Header */}
                                <div
                                    className="px-6 py-4 bg-white dark:bg-gray-900 border-b border-gray-100 dark:border-gray-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
                                    <div className="flex items-center space-x-3">
                                        <div
                                            className="w-10 h-10 rounded-lg bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400 shrink-0">
                                            <Users className="w-5 h-5"/>
                                        </div>
                                        <div>
                                            <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-bold tracking-wider">Consumer Group</div>
                                            <div className="font-mono font-bold text-gray-900 dark:text-white text-base break-all">{group.groupName}</div>
                                        </div>
                                    </div>

                                    <div className="flex items-center gap-6">
                                        <div className="text-right">
                                            <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-bold tracking-wider mb-1">Total Lag</div>
                                            <span
                                                className="inline-flex items-center px-2.5 py-0.5 rounded-full text-sm font-bold bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-400 border border-red-100 dark:border-red-800">
                         {group.diffTotal.toLocaleString()}
                       </span>
                                        </div>
                                        <div className="h-8 w-px bg-gray-100 dark:bg-gray-800 hidden md:block"></div>
                                        <div className="text-right hidden md:block">
                                            <div className="text-xs text-gray-500 dark:text-gray-400 uppercase font-bold tracking-wider mb-1">Last Update</div>
                                            <div className="text-sm font-mono text-gray-600 dark:text-gray-400">{group.lastTimeStamp}</div>
                                        </div>
                                    </div>
                                </div>

                                {/* Queue Details Table - Replaces the small cards for better readability */}
                                <div className="overflow-x-auto">
                                    <table className="w-full text-left text-sm">
                                        <thead
                                            className="bg-gray-50/50 dark:bg-gray-800/50 text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider font-semibold border-b border-gray-100 dark:border-gray-800">
                                        <tr>
                                            <th className="px-6 py-3 font-medium">Broker / Queue</th>
                                            <th className="px-6 py-3 font-medium text-right">Broker Offset</th>
                                            <th className="px-6 py-3 font-medium text-right">Consumer Offset</th>
                                            <th className="px-6 py-3 font-medium text-right">Lag</th>
                                            <th className="px-6 py-3 font-medium">Client</th>
                                            <th className="px-6 py-3 font-medium text-right">Last Updated</th>
                                        </tr>
                                        </thead>
                                        <tbody className="divide-y divide-gray-50 dark:divide-gray-800">
                                        {group.details.map((detail, dIdx) => (
                                            <tr key={dIdx} className="hover:bg-blue-50/30 dark:hover:bg-blue-900/10 transition-colors group">
                                                <td className="px-6 py-3">
                                                    <div className="flex items-center space-x-2">
                                                        <span className="font-semibold text-gray-900 dark:text-white">{detail.broker}</span>
                                                        <span className="text-gray-400 dark:text-gray-600">/</span>
                                                        <span
                                                            className="w-5 h-5 rounded bg-gray-100 dark:bg-gray-800 flex items-center justify-center text-xs font-mono text-gray-600 dark:text-gray-400">{detail.queue}</span>
                                                    </div>
                                                </td>
                                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400 text-right">{detail.brokerOffset}</td>
                                                <td className="px-6 py-3 font-mono text-gray-600 dark:text-gray-400 text-right">{detail.consumerOffset}</td>
                                                <td className="px-6 py-3 text-right">
                             <span
                                 className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-50 dark:bg-red-900/30 text-red-600 dark:text-red-400 border border-red-100 dark:border-red-800">
                               {detail.diffTotal}
                             </span>
                                                </td>
                                                <td className="px-6 py-3 text-gray-500 dark:text-gray-400 text-xs">{detail.client || '-'}</td>
                                                <td className="px-6 py-3 font-mono text-gray-400 dark:text-gray-500 text-xs text-right">{detail.lastTimeStamp.split(' ')[1]}</td>
                                            </tr>
                                        ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end">
                        <button
                            onClick={onClose}
                            className="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 text-sm font-medium transition-colors shadow-sm"
                        >
                            Close
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicSendMessageModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    const [tag, setTag] = useState('');
    const [messageKey, setMessageKey] = useState('');
    const [messageBody, setMessageBody] = useState('');
    const [traceEnabled, setTraceEnabled] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState('');
    const [sendResult, setSendResult] = useState<TopicSendMessageResult | null>(null);

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        setTag('');
        setMessageKey('');
        setMessageBody('');
        setTraceEnabled(false);
        setIsSubmitting(false);
        setError('');
        setSendResult(null);
    }, [isOpen, topic?.name]);

    const handleSubmit = async () => {
        if (!topic?.name) {
            return;
        }

        if (!messageBody.trim()) {
            setError('Message body is required before sending.');
            return;
        }

        setIsSubmitting(true);
        setError('');
        setSendResult(null);

        try {
            const result = await TopicService.sendTopicMessage({
                topic: topic.name,
                key: messageKey,
                tag,
                messageBody,
                traceEnabled,
            });
            setSendResult(result);
            toast.success(`Message sent to ${topic.name}`);
        } catch (submitError) {
            setError(submitError instanceof Error ? submitError.message : 'Failed to send topic message.');
        } finally {
            setIsSubmitting(false);
        }
    };

    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <Send className="w-5 h-5 mr-2 text-blue-500"/>
                                Send Message
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                Send a message to <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span>
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm space-y-4">
                            {/* Topic */}
                            <div className="grid grid-cols-[120px_1fr] items-center gap-4">
                                <label className="flex items-center justify-end gap-2 text-sm font-medium text-gray-600 dark:text-gray-400">
                                    <FileText className="w-4 h-4 text-gray-400 dark:text-gray-500"/>
                                    Topic:
                                </label>
                                <input
                                    type="text"
                                    value={topic?.name || ''}
                                    disabled
                                    className="w-full px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-500 dark:text-gray-400 cursor-not-allowed font-mono"
                                />
                            </div>

                            {/* Tag */}
                            <div className="grid grid-cols-[120px_1fr] items-center gap-4">
                                <label className="flex items-center justify-end gap-2 text-sm font-medium text-gray-600 dark:text-gray-400">
                                    <Tag className="w-4 h-4 text-gray-400 dark:text-gray-500"/>
                                    Tag:
                                </label>
                                <input
                                    type="text"
                                    value={tag}
                                    onChange={(event) => setTag(event.target.value)}
                                    className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder-gray-400 dark:placeholder-gray-600"
                                    placeholder="Optional tag..."
                                />
                            </div>

                            {/* Key */}
                            <div className="grid grid-cols-[120px_1fr] items-center gap-4">
                                <label className="flex items-center justify-end gap-2 text-sm font-medium text-gray-600 dark:text-gray-400">
                                    <Key className="w-4 h-4 text-gray-400 dark:text-gray-500"/>
                                    Key:
                                </label>
                                <input
                                    type="text"
                                    value={messageKey}
                                    onChange={(event) => setMessageKey(event.target.value)}
                                    className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder-gray-400 dark:placeholder-gray-600"
                                    placeholder="Optional key..."
                                />
                            </div>

                            {/* Message Body */}
                            <div className="grid grid-cols-[120px_1fr] gap-4">
                                <label className="flex items-center justify-end gap-2 text-sm font-medium text-gray-600 dark:text-gray-400 pt-2">
                                    <FileText className="w-4 h-4 text-gray-400 dark:text-gray-500"/>
                                    <span className="flex items-center">
                    <span className="text-red-500 mr-1">*</span>Body:
                  </span>
                                </label>
                                <textarea
                                    rows={5}
                                    value={messageBody}
                                    onChange={(event) => setMessageBody(event.target.value)}
                                    className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono resize-none placeholder-gray-400 dark:placeholder-gray-600"
                                    placeholder="{ 1: 'value', nested: { 2: true } }"
                                />
                            </div>
                            <div className="col-start-2 rounded-lg border border-blue-100 bg-blue-50/80 px-3 py-2 text-xs text-blue-700 dark:border-blue-900/40 dark:bg-blue-950/30 dark:text-blue-300">
                                Standard JSON and relaxed JSON are both supported here. Numeric keys like <span className="font-mono">{`{ 1: 'value' }`}</span> will be normalized before sending.
                            </div>

                            {/* Enable Message Trace */}
                            <div className="grid grid-cols-[120px_1fr] items-center gap-4 pt-1">
                                <div
                                    className="col-start-2 flex items-center bg-gray-50 dark:bg-gray-800 px-3 py-2 rounded-lg border border-gray-100 dark:border-gray-700">
                                    <label className="flex items-center space-x-3 cursor-pointer select-none w-full">
                                        <div className="relative">
                                            <input
                                                type="checkbox"
                                                checked={traceEnabled}
                                                onChange={(event) => setTraceEnabled(event.target.checked)}
                                                className="peer sr-only"
                                            />
                                            <div
                                                className="w-9 h-5 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600"></div>
                                        </div>
                                        <div className="flex items-center text-sm text-gray-700 dark:text-gray-300 font-medium">
                                            <Activity className="w-4 h-4 mr-2 text-blue-500"/>
                                            Enable Message Trace
                                        </div>
                                    </label>
                                </div>
                            </div>

                            {error && (
                                <div className="rounded-lg border border-red-200 bg-red-50/80 px-3 py-2 text-sm text-red-600 dark:border-red-900/60 dark:bg-red-950/40 dark:text-red-300">
                                    {error}
                                </div>
                            )}

                            {sendResult && (
                                <div className="rounded-lg border border-emerald-200 bg-emerald-50/80 px-3 py-3 text-sm text-emerald-700 dark:border-emerald-900/50 dark:bg-emerald-950/30 dark:text-emerald-300">
                                    <div className="font-semibold">Send result: {sendResult.sendStatus}</div>
                                    <div className="mt-1 font-mono text-xs break-all">
                                        {sendResult.messageId ? `Message ID: ${sendResult.messageId}` : 'Broker accepted the message without returning a message id.'}
                                    </div>
                                    <div className="mt-1 text-xs">
                                        Queue: {sendResult.brokerName ?? '-'} / {sendResult.queueId ?? '-'} / offset {sendResult.queueOffset}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            onClick={() => void handleSubmit()}
                            disabled={isSubmitting}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            <Send className="w-4 h-4 mr-2"/>
                            {isSubmitting ? 'Sending...' : 'Commit'}
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicResetOffsetModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <RotateCcw className="w-5 h-5 mr-2 text-blue-500"/>
                                Reset Consumer Offset
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span> resetOffset
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-6 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm space-y-5">

                            {/* SubscriptionGroup */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <span className="text-red-500 mr-1">*</span>SubscriptionGroup
                                </label>
                                <div className="relative">
                                    <select
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750">
                                        <option value="" disabled selected>Select a group...</option>
                                        <option>please_rename_unique_group_name_4</option>
                                        <option>OrderProcessingGroup</option>
                                        <option>NotificationServiceGroup</option>
                                    </select>
                                    <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                </div>
                            </div>

                            {/* Time */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <span className="text-red-500 mr-1">*</span>Time
                                </label>
                                <div className="relative">
                                    <input
                                        type="datetime-local"
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all placeholder:text-gray-400 dark:placeholder:text-gray-600 dark:[color-scheme:dark]"
                                    />
                                </div>
                            </div>

                        </div>
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            onClick={() => {
                                toast.success(`Offset reset requested for ${topic?.name}`);
                                onClose();
                            }}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            RESET
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicSkipMessageAccumulateModal = ({isOpen, onClose, topic}: TopicRouterModalProps) => {
    if (!isOpen) return null;

    return (
        <AnimatePresence>
            <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
                <motion.div
                    initial={{opacity: 0}}
                    animate={{opacity: 0.3}}
                    exit={{opacity: 0}}
                    onClick={onClose}
                    className="absolute inset-0 bg-black"
                />
                <motion.div
                    initial={{opacity: 0, scale: 0.95, y: 10}}
                    animate={{opacity: 1, scale: 1, y: 0}}
                    exit={{opacity: 0, scale: 0.95, y: 10}}
                    className="relative w-full max-w-lg bg-white dark:bg-gray-900 rounded-xl shadow-2xl overflow-hidden flex flex-col border border-gray-100 dark:border-gray-800"
                >
                    {/* Header */}
                    <div className="px-6 py-5 border-b border-gray-100 dark:border-gray-800 flex items-center justify-between bg-white dark:bg-gray-900 z-10">
                        <div>
                            <h3 className="text-xl font-bold text-gray-800 dark:text-white flex items-center">
                                <FastForward className="w-5 h-5 mr-2 text-blue-500"/>
                                Skip Message Accumulate
                            </h3>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                <span className="font-mono text-gray-700 dark:text-gray-300 font-medium">{topic?.name}</span> Skip Message Accumulate
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full transition-colors"
                        >
                            <X className="w-5 h-5"/>
                        </button>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-5 bg-gray-50/50 dark:bg-gray-950/50">
                        <div className="bg-white dark:bg-gray-900 p-6 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm">

                            {/* SubscriptionGroup */}
                            <div className="space-y-1.5">
                                <label className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
                                    <Users className="w-4 h-4 mr-1.5 text-gray-400 dark:text-gray-500"/>
                                    <span className="text-red-500 mr-1">*</span>SubscriptionGroup
                                </label>
                                <div className="relative">
                                    <select
                                        className="w-full px-3 py-2.5 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-700 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all appearance-none cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750">
                                        <option value="" disabled selected>Select a group...</option>
                                        <option>please_rename_unique_group_name_4</option>
                                        <option>OrderProcessingGroup</option>
                                        <option>NotificationServiceGroup</option>
                                    </select>
                                    <ChevronDown className="absolute right-3 top-3 w-4 h-4 text-gray-400 dark:text-gray-500 pointer-events-none"/>
                                </div>
                            </div>

                        </div>
                    </div>

                    {/* Footer */}
                    <div className="px-6 py-4 bg-white dark:bg-gray-900 border-t border-gray-100 dark:border-gray-800 flex justify-end space-x-3 z-10">
                        <button
                            onClick={onClose}
                            className="px-6 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                        >
                            Close
                        </button>
                        <button
                            onClick={() => {
                                toast.success(`Skipped message accumulate for ${topic?.name}`);
                                onClose();
                            }}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            Commit
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

export const TopicView = () => {
    const {data, error, isLoading, isRefreshing, refresh} = useTopicCatalog();
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedFilters, setSelectedFilters] = useState<Record<TopicCategory, boolean>>(buildDefaultTopicFilters);
    const [currentPage, setCurrentPage] = useState(1);
    const [statusModal, setStatusModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [routerModal, setRouterModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [configModal, setConfigModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [consumerModal, setConsumerModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [sendMessageModal, setSendMessageModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [resetOffsetModal, setResetOffsetModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});
    const [skipAccumulateModal, setSkipAccumulateModal] = useState<{ isOpen: boolean, topic: Topic | null }>({isOpen: false, topic: null});

    useEffect(() => {
        if (error) {
            toast.error(error);
        }
    }, [error]);

    const topics = (data?.items ?? []).map(mapTopicListItem);
    const normalizedSearch = searchTerm.trim().toLowerCase();
    const filteredTopics = topics.filter((topic) => {
        const matchesType = selectedFilters[topic.type as TopicCategory] ?? true;
        if (!matchesType) {
            return false;
        }

        if (!normalizedSearch) {
            return true;
        }

        return topic.name.toLowerCase().includes(normalizedSearch) || topic.type.toLowerCase().includes(normalizedSearch);
    });
    const totalPages = Math.max(1, Math.ceil(filteredTopics.length / TOPIC_PAGE_SIZE));
    const pagedTopics = filteredTopics.slice(
        (currentPage - 1) * TOPIC_PAGE_SIZE,
        currentPage * TOPIC_PAGE_SIZE,
    );

    useEffect(() => {
        setCurrentPage(1);
    }, [normalizedSearch, selectedFilters]);

    useEffect(() => {
        if (currentPage > totalPages) {
            setCurrentPage(totalPages);
        }
    }, [currentPage, totalPages]);

    const toggleFilter = (key: TopicCategory) => {
        setSelectedFilters((prev) => ({...prev, [key]: !prev[key]}));
    };

    const handleOperation = (op: string, topic: Topic) => {
        if (op === 'Status') {
            setStatusModal({isOpen: true, topic});
        } else if (op === 'Router') {
            setRouterModal({isOpen: true, topic});
        } else if (op === 'Topic Config') {
            setConfigModal({isOpen: true, topic});
        } else if (op === 'Consumer Manage') {
            setConsumerModal({isOpen: true, topic});
        } else if (op === 'Send Message') {
            setSendMessageModal({isOpen: true, topic});
        } else if (op === 'Reset Consumer Offset') {
            setResetOffsetModal({isOpen: true, topic});
        } else if (op === 'Skip Message Accumulate') {
            setSkipAccumulateModal({isOpen: true, topic});
        } else {
            toast(`${op} clicked for ${topic.name}`);
        }
    };

    const getActionIcon = (action: string) => {
        switch (action) {
            case 'Status':
                return Activity;
            case 'Router':
                return Network;
            case 'Consumer Manage':
                return Users;
            case 'Topic Config':
                return Settings;
            case 'Send Message':
                return Send;
            case 'Reset Consumer Offset':
                return RotateCcw;
            case 'Skip Message Accumulate':
                return FastForward;
            case 'Delete':
                return Trash2;
            default:
                return FileText;
        }
    };

    const getTopicColor = (type: string) => {
        if (type === 'SYSTEM') return 'text-red-600 bg-red-50 dark:bg-red-900/30 dark:text-red-400 border-red-100 dark:border-red-800 ring-red-500/20';
        if (type === 'RETRY') return 'text-orange-600 bg-orange-50 dark:bg-orange-900/30 dark:text-orange-400 border-orange-100 dark:border-orange-800 ring-orange-500/20';
        if (type === 'DLQ') return 'text-purple-600 bg-purple-50 dark:bg-purple-900/30 dark:text-purple-400 border-purple-100 dark:border-purple-800 ring-purple-500/20';
        return 'text-blue-600 bg-blue-50 dark:bg-blue-900/30 dark:text-blue-400 border-blue-100 dark:border-blue-800 ring-blue-500/20';
    };

    const getFilterIcon = (key: string) => {
        switch (key) {
            case 'NORMAL':
                return FileBox;
            case 'DELAY':
                return Clock;
            case 'FIFO':
                return Layers;
            case 'TRANSACTION':
                return ArrowRightLeft;
            case 'UNSPECIFIED':
                return HelpCircle;
            case 'RETRY':
                return RefreshCw;
            case 'DLQ':
                return AlertTriangle;
            case 'SYSTEM':
                return Shield;
            default:
                return Filter;
        }
    };

    return (
        <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 pb-12">
            <TopicStatusModal
                isOpen={statusModal.isOpen}
                onClose={() => setStatusModal({isOpen: false, topic: null})}
                topic={statusModal.topic}
            />
            <TopicRouterModal
                isOpen={routerModal.isOpen}
                onClose={() => setRouterModal({isOpen: false, topic: null})}
                topic={routerModal.topic}
            />
            <TopicConfigModal
                isOpen={configModal.isOpen}
                onClose={() => setConfigModal({isOpen: false, topic: null})}
                topic={configModal.topic}
            />
            <TopicConsumerManageModal
                isOpen={consumerModal.isOpen}
                onClose={() => setConsumerModal({isOpen: false, topic: null})}
                topic={consumerModal.topic}
            />
            <TopicSendMessageModal
                isOpen={sendMessageModal.isOpen}
                onClose={() => setSendMessageModal({isOpen: false, topic: null})}
                topic={sendMessageModal.topic}
            />
            <TopicResetOffsetModal
                isOpen={resetOffsetModal.isOpen}
                onClose={() => setResetOffsetModal({isOpen: false, topic: null})}
                topic={resetOffsetModal.topic}
            />
            <TopicSkipMessageAccumulateModal
                isOpen={skipAccumulateModal.isOpen}
                onClose={() => setSkipAccumulateModal({isOpen: false, topic: null})}
                topic={skipAccumulateModal.topic}
            />

            {/* Filter Toolbar */}
            <div
                className="bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 p-4 shadow-sm space-y-4 sticky top-0 z-10 backdrop-blur-xl bg-white/90 dark:bg-gray-900/90 transition-colors">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div className="relative flex-1 max-w-md">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 dark:text-gray-500"/>
                        <input
                            type="text"
                            placeholder="Filter topics..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="w-full pl-10 pr-4 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all shadow-inner dark:text-white dark:placeholder:text-gray-500"
                        />
                    </div>

                    <div className="flex items-center space-x-3">
                        <Button
                            variant="primary"
                            icon={Plus}
                            className="dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            Add/Update
                        </Button>
                        <Button
                            variant="secondary"
                            icon={RefreshCw}
                            onClick={() => void refresh()}
                            disabled={isRefreshing || isLoading}
                            className="dark:bg-gray-900 dark:text-white dark:border dark:border-gray-700 dark:hover:bg-gray-800"
                        >
                            {isRefreshing ? 'Refreshing...' : 'Refresh'}
                        </Button>
                    </div>
                </div>

                <div className="flex flex-wrap gap-2 pt-2 border-t border-gray-50 dark:border-gray-800">
                    <div className="flex items-center text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase mr-2">
                        <Filter className="w-3 h-3 mr-1"/>
                        Types:
                    </div>
                    {TOPIC_FILTER_ORDER.map((key) => {
                        const checked = selectedFilters[key];
                        const Icon = getFilterIcon(key);
                        return (
                            <button
                                key={key}
                                onClick={() => toggleFilter(key)}
                                className={`flex items-center px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-200 border ${
                                    checked
                                        ? 'bg-gray-900 text-white border-gray-900 dark:bg-gray-700 dark:text-white dark:border-gray-600 shadow-md transform scale-105'
                                        : 'bg-white text-gray-600 border-gray-200 hover:border-gray-300 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:border-gray-700 dark:hover:bg-gray-700'
                                }`}
                            >
                                <Icon className={`w-3 h-3 mr-1.5 ${checked ? 'text-gray-300 dark:text-gray-300' : 'text-gray-400 dark:text-gray-500'}`}/>
                                {key}
                            </button>
                        );
                    })}
                </div>
            </div>

            {/* Topic Grid */}
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
                <AnimatePresence mode="popLayout">
                    {pagedTopics.map((topic, index) => (
                        <motion.div
                            layout
                            key={topic.name}
                            initial={{opacity: 0, y: 20, scale: 0.95}}
                            animate={{opacity: 1, y: 0, scale: 1}}
                            exit={{opacity: 0, scale: 0.9, transition: {duration: 0.2}}}
                            transition={{delay: index * 0.05, type: "spring", stiffness: 300, damping: 25}}
                            whileHover={{
                                y: -4,
                                transition: {duration: 0.2}
                            }}
                            className={`bg-white dark:bg-gray-900 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm overflow-hidden flex flex-col group relative ring-1 ring-transparent hover:ring-2 transition-all ${
                                topic.type === 'SYSTEM' ? 'hover:ring-red-100 dark:hover:ring-red-900/50' :
                                    topic.type === 'DLQ' ? 'hover:ring-purple-100 dark:hover:ring-purple-900/50' : 'hover:ring-blue-100 dark:hover:ring-blue-900/50'
                            }`}
                        >
                            {/* Decorative background gradient */}
                            <div
                                className={`absolute top-0 right-0 w-32 h-32 bg-gradient-to-br opacity-5 dark:opacity-10 rounded-bl-full pointer-events-none transition-opacity duration-300 group-hover:opacity-10 dark:group-hover:opacity-20 ${
                                    topic.type === 'SYSTEM' ? 'from-red-500 to-transparent' :
                                        topic.type === 'DLQ' ? 'from-purple-500 to-transparent' : 'from-blue-500 to-transparent'
                                }`}/>

                            <div className="p-5 border-b border-gray-50 dark:border-gray-800 flex items-start justify-between relative z-0">
                                <div className="flex items-center space-x-4">
                                    <div className={`p-2.5 rounded-xl shadow-sm ${getTopicColor(topic.type)}`}>
                                        <Database className="w-5 h-5"/>
                                    </div>
                                    <div>
                                        <h3 className={`text-base font-bold tracking-tight ${topic.type === 'SYSTEM' ? 'text-red-700 dark:text-red-400' : 'text-gray-900 dark:text-white'}`}>
                                            {topic.name}
                                        </h3>
                                        <div className="flex items-center space-x-2 mt-1.5">
                        <span className={`text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded-md border ${
                            topic.type === 'SYSTEM' ? 'bg-red-50 text-red-600 border-red-100 dark:bg-red-900/20 dark:text-red-400 dark:border-red-900/50' :
                                topic.type === 'DLQ' ? 'bg-purple-50 text-purple-600 border-purple-100 dark:bg-purple-900/20 dark:text-purple-400 dark:border-purple-900/50' :
                                    'bg-gray-100 text-gray-500 border-gray-200 dark:bg-gray-800 dark:text-gray-400 dark:border-gray-700'
                        }`}>
                          {topic.type}
                        </span>
                                        </div>
                                    </div>
                                </div>

                                {/* Quick Action Menu Trigger (Visual only for now) */}
                                <button className="text-gray-300 hover:text-gray-600 dark:text-gray-600 dark:hover:text-gray-400 transition-colors p-1">
                                    <MoreHorizontal className="w-5 h-5"/>
                                </button>
                            </div>

                            <div className="p-4 flex-1 bg-gray-50/30 dark:bg-gray-950/30">
                                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
                                    {topic.operations.map((op) => {
                                        const Icon = getActionIcon(op);
                                        const isDanger = ['Reset Consumer Offset', 'Skip Message Accumulate', 'Delete'].includes(op);

                                        return (
                                            <button
                                                key={op}
                                                onClick={() => handleOperation(op, topic)}
                                                className={`flex flex-col items-center justify-center p-2.5 rounded-lg text-xs font-medium transition-all duration-200 border group/btn relative overflow-hidden ${
                                                    isDanger
                                                        ? 'bg-white border-red-50 text-red-600 hover:bg-red-50 hover:border-red-200 hover:shadow-sm dark:bg-gray-900 dark:border-red-900/30 dark:text-red-400 dark:hover:bg-red-900/20 dark:hover:border-red-800'
                                                        : 'bg-white border-gray-100 text-gray-600 hover:bg-blue-50 hover:text-blue-600 hover:border-blue-100 hover:shadow-sm dark:bg-gray-900 dark:border-gray-800 dark:text-gray-400 dark:hover:bg-blue-900/20 dark:hover:text-blue-400 dark:hover:border-blue-800'
                                                }`}
                                            >
                                                <Icon
                                                    className={`w-4 h-4 mb-1.5 transition-transform duration-200 group-hover/btn:scale-110 ${isDanger ? 'opacity-90' : 'opacity-70'}`}/>
                                                <span className="text-center text-[10px] leading-tight truncate w-full">{op}</span>
                                            </button>
                                        );
                                    })}
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </AnimatePresence>
            </div>

            {filteredTopics.length === 0 && (
                <div className="rounded-2xl border border-dashed border-gray-200 bg-white/70 px-6 py-10 text-center text-sm text-gray-500 shadow-sm dark:border-gray-800 dark:bg-gray-900/70 dark:text-gray-400">
                    {isLoading
                        ? 'Loading topics from the current NameServer...'
                        : 'No topics matched the current search keyword or type filters.'}
                </div>
            )}

            {/* Pagination */}
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
