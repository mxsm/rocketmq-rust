import React, {useState} from 'react';
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

const TopicRouterModal = ({isOpen, onClose, topic}: any) => {
    if (!isOpen) return null;

    // Mock data to match screenshot structure
    const mockBrokerDatas = [
        {
            broker: 'mxsm',
            addrs: [
                {index: 0, address: '172.20.48.1:10911'}
            ]
        }
    ];

    const mockQueueDatas = [
        {
            brokerName: 'mxsm',
            readQueueNums: 1,
            writeQueueNums: 1,
            perm: 6
        }
    ];

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

                        {/* Broker Datas Section */}
                        <div>
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                                <Server className="w-4 h-4 text-gray-500 dark:text-gray-400"/>
                                Broker Datas
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                                {mockBrokerDatas.map((broker, i) => (
                                    <div key={i}
                                         className="bg-white dark:bg-gray-900 p-5 rounded-xl border border-gray-200 dark:border-gray-800 shadow-sm hover:shadow-md transition-shadow">
                                        <div className="flex items-center justify-between mb-4">
                                            <span className="text-base font-bold text-gray-900 dark:text-white">{broker.broker}</span>
                                            <span
                                                className="text-xs bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 px-2.5 py-1 rounded-full font-medium border border-gray-200 dark:border-gray-700">
                        {broker.addrs.length} Addrs
                      </span>
                                        </div>
                                        <div className="space-y-2">
                                            {broker.addrs.map((addr, j) => (
                                                <div key={j}
                                                     className="flex items-center justify-between text-xs bg-gray-50 dark:bg-gray-800/50 p-3 rounded-lg border border-gray-100 dark:border-gray-800/50 font-mono">
                                                    <span className="text-gray-500 dark:text-gray-500 font-semibold">ID: {addr.index}</span>
                                                    <span className="text-gray-700 dark:text-gray-300">{addr.address}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Queue Datas Section */}
                        <div>
                            <h4 className="text-sm font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                                <Database className="w-4 h-4 text-gray-500 dark:text-gray-400"/>
                                Queue Datas
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                {mockQueueDatas.map((queue, i) => (
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
                        </div>

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

const TopicConfigModal = ({isOpen, onClose, topic}: any) => {
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

const TopicStatusModal = ({isOpen, onClose, topic}: any) => {
    if (!isOpen) return null;

    const mockData = [
        {
            queueId: 0,
            brokerName: 'mxsm',
            minOffset: 0,
            maxOffset: 0,
            lastUpdate: '1970-01-01 08:00:00'
        },
        {
            queueId: 1,
            brokerName: 'mxsm',
            minOffset: 124,
            maxOffset: 124,
            lastUpdate: '2026-01-27 10:30:45'
        },
        {
            queueId: 2,
            brokerName: 'mxsm',
            minOffset: 50,
            maxOffset: 52,
            lastUpdate: '2026-01-27 10:31:02'
        },
        {
            queueId: 3,
            brokerName: 'mxsm',
            minOffset: 1000,
            maxOffset: 1050,
            lastUpdate: '2026-01-27 10:32:15'
        },
        {
            queueId: 4,
            brokerName: 'broker-a',
            minOffset: 2500,
            maxOffset: 2510,
            lastUpdate: '2026-01-27 10:33:00'
        },
        {
            queueId: 5,
            brokerName: 'broker-b',
            minOffset: 100,
            maxOffset: 150,
            lastUpdate: '2026-01-27 10:33:05'
        }
    ];

    // Calculate summary stats
    const totalMinOffset = mockData.reduce((acc, curr) => acc + curr.minOffset, 0);
    const totalMaxOffset = mockData.reduce((acc, curr) => acc + curr.maxOffset, 0);
    const queueCount = mockData.length;

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

                        {/* Queue Grid */}
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                            {mockData.map((row, i) => (
                                <div key={i}
                                     className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 shadow-sm hover:shadow-md transition-shadow duration-200 overflow-hidden flex flex-col">

                                    {/* Card Header */}
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

                                    {/* Body */}
                                    <div className="p-4 space-y-4 flex-1">
                                        <div className="space-y-1">
                                            <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                                                <span>Min Offset</span>
                                                <span className="font-mono text-gray-900 dark:text-white">{row.minOffset}</span>
                                            </div>
                                            <div className="w-full bg-gray-100 dark:bg-gray-800 rounded-full h-1.5 overflow-hidden">
                                                <div className="bg-green-500 h-1.5 rounded-full" style={{width: '40%'}}></div>
                                            </div>
                                        </div>
                                        <div className="space-y-1">
                                            <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                                                <span>Max Offset</span>
                                                <span className="font-mono text-gray-900 dark:text-white">{row.maxOffset}</span>
                                            </div>
                                            <div className="w-full bg-gray-100 dark:bg-gray-800 rounded-full h-1.5 overflow-hidden">
                                                <div className="bg-purple-500 h-1.5 rounded-full" style={{width: '75%'}}></div>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Footer */}
                                    <div
                                        className="px-4 py-2 bg-gray-50 dark:bg-gray-800/50 border-t border-gray-100 dark:border-gray-800 text-[10px] text-gray-400 dark:text-gray-500 flex items-center justify-between">
                    <span className="flex items-center">
                       <Clock className="w-3 h-3 mr-1"/>
                       Updated
                    </span>
                                        <span className="font-mono">{row.lastUpdate.split(' ')[1]}</span>
                                    </div>
                                </div>
                            ))}
                        </div>
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

const TopicConsumerManageModal = ({isOpen, onClose, topic}: any) => {
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

const TopicSendMessageModal = ({isOpen, onClose, topic}: any) => {
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
                                    className="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-mono resize-none placeholder-gray-400 dark:placeholder-gray-600"
                                    placeholder="{ 'key': 'value' }"
                                ></textarea>
                            </div>

                            {/* Enable Message Trace */}
                            <div className="grid grid-cols-[120px_1fr] items-center gap-4 pt-1">
                                <div
                                    className="col-start-2 flex items-center bg-gray-50 dark:bg-gray-800 px-3 py-2 rounded-lg border border-gray-100 dark:border-gray-700">
                                    <label className="flex items-center space-x-3 cursor-pointer select-none w-full">
                                        <div className="relative">
                                            <input type="checkbox" className="peer sr-only"/>
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
                                toast.success(`Message sent to ${topic?.name}`);
                                onClose();
                            }}
                            className="px-6 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 transition-all shadow-md hover:shadow-lg flex items-center dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            <Send className="w-4 h-4 mr-2"/>
                            Commit
                        </button>
                    </div>
                </motion.div>
            </div>
        </AnimatePresence>
    );
};

const TopicResetOffsetModal = ({isOpen, onClose, topic}: any) => {
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

const TopicSkipMessageAccumulateModal = ({isOpen, onClose, topic}: any) => {
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
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedFilters, setSelectedFilters] = useState<any>({
        NORMAL: true,
        Delay: false,
        FIFO: true,
        TRANSACTION: true,
        UNSPECIFIED: true,
        RETRY: true,
        DLQ: true,
        SYSTEM: true
    });
    const [currentPage, setCurrentPage] = useState(1);
    const [statusModal, setStatusModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [routerModal, setRouterModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [configModal, setConfigModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [consumerModal, setConsumerModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [sendMessageModal, setSendMessageModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [resetOffsetModal, setResetOffsetModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});
    const [skipAccumulateModal, setSkipAccumulateModal] = useState<{ isOpen: boolean, topic: any }>({isOpen: false, topic: null});

    // Mock Topic Data
    const topics = [
        {name: 'SELF_TEST_TOPIC', type: 'SYSTEM', operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config']},
        {
            name: 'DefaultCluster',
            type: 'SYSTEM',
            operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config', 'Send Message', 'Reset Consumer Offset', 'Skip Message Accumulate', 'Delete']
        },
        {
            name: 'OrderPlaced_TOPIC',
            type: 'NORMAL',
            operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config', 'Send Message', 'Reset Consumer Offset', 'Skip Message Accumulate', 'Delete']
        },
        {name: 'UserSignup_EVENT', type: 'NORMAL', operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config', 'Send Message']},
        {name: 'rmq_sys_wheel_timer', type: 'SYSTEM', operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config']},
        {
            name: 'benchmark',
            type: 'NORMAL',
            operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config', 'Send Message', 'Reset Consumer Offset', 'Delete']
        },
        {name: 'RMQ_SYS_TRANS_HALF_TOPIC', type: 'SYSTEM', operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config']},
        {name: 'Payment_RETRY_TOPIC', type: 'RETRY', operations: ['Status', 'Router', 'Consumer Manage', 'Topic Config', 'Delete']},
    ];

    const toggleFilter = (key: string) => {
        setSelectedFilters((prev: any) => ({...prev, [key]: !prev[key]}));
    };

    const handleOperation = (op: string, topic: any) => {
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
            case 'Delay':
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
                            className="dark:bg-gray-900 dark:text-white dark:border dark:border-gray-700 dark:hover:bg-gray-800"
                        >
                            Refresh
                        </Button>
                    </div>
                </div>

                <div className="flex flex-wrap gap-2 pt-2 border-t border-gray-50 dark:border-gray-800">
                    <div className="flex items-center text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase mr-2">
                        <Filter className="w-3 h-3 mr-1"/>
                        Types:
                    </div>
                    {Object.entries(selectedFilters).map(([key, checked]: any) => {
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
                    {topics.map((topic, index) => (
                        <motion.div
                            layout
                            key={topic.name}
                            initial={{opacity: 0, y: 20, scale: 0.95}}
                            animate={{opacity: 1, y: 0, scale: 1}}
                            exit={{opacity: 0, scale: 0.9, transition: {duration: 0.2}}}
                            transition={{delay: index * 0.05, type: "spring", stiffness: 300, damping: 25}}
                            whileHover={{
                                y: -4,
                                shadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
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

            {/* Pagination */}
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
