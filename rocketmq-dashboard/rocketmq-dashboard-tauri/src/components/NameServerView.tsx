import React from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { Plus, Server, Trash2 } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { useNameServer } from '../features/nameserver/hooks/useNameServer';
import { Card } from '../components/ui/LegacyCard';
import { Button } from '../components/ui/LegacyButton';
import { Input } from '../components/ui/LegacyInput';
import { Toggle } from '../components/ui/LegacyToggle';

const NAMESERVER_TABLE_GRID_STYLE: React.CSSProperties = {
    gridTemplateColumns: 'minmax(0, 1fr) 176px 240px 176px',
};
const HEARTBEAT_WAVE_PATHS = [
    'M4 20 C10 20, 12 9, 18 9 S26 28, 33 20 S42 8, 50 15 S58 28, 66 20 S76 10, 84 16 S94 20, 108 20',
    'M4 20 C10 20, 12 13, 18 13 S26 24, 33 20 S42 12, 50 17 S58 24, 66 20 S76 13, 84 17 S94 20, 108 20',
    'M4 20 C10 20, 12 7, 18 7 S26 31, 33 20 S42 6, 50 14 S58 31, 66 20 S76 8, 84 15 S94 20, 108 20',
];
const OFFLINE_WAVE_PATH = 'M4 20 L108 20';

const NameServerHeartbeatWave = ({ isAlive }: { isAlive: boolean }) => (
    <div
        className={`relative flex h-12 w-36 items-center justify-center overflow-hidden rounded-full px-4 ${
            isAlive
                ? 'bg-emerald-50/90 ring-1 ring-emerald-100 dark:bg-emerald-950/30 dark:ring-emerald-900/40'
                : 'bg-gray-100/90 ring-1 ring-gray-200 dark:bg-gray-800/70 dark:ring-gray-700/70'
        }`}
    >
        <div
            className={`absolute inset-x-4 h-px ${
                isAlive ? 'bg-emerald-200/80 dark:bg-emerald-800/50' : 'bg-gray-300 dark:bg-gray-600'
            }`}
        />
        <svg viewBox="0 0 112 40" className="relative z-10 h-9 w-full overflow-visible">
            <motion.path
                d={isAlive ? HEARTBEAT_WAVE_PATHS[0] : OFFLINE_WAVE_PATH}
                fill="none"
                stroke={isAlive ? 'rgba(16, 185, 129, 0.95)' : 'rgba(107, 114, 128, 0.9)'}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                animate={
                    isAlive
                        ? {
                              d: HEARTBEAT_WAVE_PATHS,
                              opacity: [0.75, 1, 0.82, 1],
                              pathLength: [0.92, 1, 0.95, 1],
                          }
                        : {
                              opacity: [0.45, 0.7, 0.45],
                          }
                }
                transition={{
                    duration: isAlive ? 1.6 : 2.4,
                    repeat: Infinity,
                    ease: 'easeInOut',
                }}
            />
            {isAlive ? (
                <motion.circle
                    cx="104"
                    cy="20"
                    r="2.6"
                    fill="rgba(16, 185, 129, 1)"
                    animate={{ opacity: [0.45, 1, 0.45], scale: [0.9, 1.15, 0.9] }}
                    transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
                />
            ) : (
                <circle cx="104" cy="20" r="2.2" fill="rgba(107, 114, 128, 0.8)" />
            )}
        </svg>
    </div>
);

export const NameServerView = () => {
    const {
        data,
        isLoading,
        loadError,
        newAddress,
        pendingAction,
        setNewAddress,
        loadHomePage,
        addNameServer,
        switchNameServer,
        deleteNameServer,
        updateVipChannel,
        updateUseTls,
    } = useNameServer();

    const nameServers = data?.namesrvAddrList ?? [];
    const currentNameServer = data?.currentNamesrv ?? null;
    const servers = data?.servers ?? nameServers.map((address) => ({
        address,
        isCurrent: address === currentNameServer,
        isAlive: false,
    }));
    const isBusy = pendingAction !== null;

    const handleAsyncAction = async (action: () => Promise<string | undefined>) => {
        try {
            const message = await action();
            if (message) {
                toast.success(message);
            }
        } catch (error) {
            toast.error(error instanceof Error ? error.message : 'NameServer operation failed');
        }
    };

    return (
        <div className="max-w-4xl mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <Card
                title="Name Server Configuration"
                description="Manage your Name Server addresses and connection status."
            >
                <div className="space-y-6">
                    {loadError ? (
                        <div className="flex items-center justify-between gap-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-200">
                            <span>{loadError}</span>
                            <Button
                                variant="outline"
                                onClick={() => void handleAsyncAction(async () => {
                                    await loadHomePage();
                                    return 'NameServer settings reloaded';
                                })}
                                disabled={isBusy}
                                className="shrink-0"
                            >
                                Retry
                            </Button>
                        </div>
                    ) : null}

                    <div className="flex items-stretch gap-3">
                        <div className="flex-1">
                            <Input
                                placeholder="Enter new Name Server address (e.g. 192.168.1.50:9876)"
                                value={newAddress}
                                onChange={(event) => setNewAddress(event.target.value)}
                                disabled={isBusy}
                                className="h-full bg-white dark:bg-gray-800 dark:text-white dark:border-gray-700 shadow-sm"
                            />
                        </div>
                        <Button
                            variant="primary"
                            onClick={() => void handleAsyncAction(addNameServer)}
                            icon={Plus}
                            disabled={isBusy}
                            className="w-[164px] dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                        >
                            Add Server
                        </Button>
                    </div>

                    <div className="border border-gray-200 dark:border-gray-800 rounded-xl overflow-hidden bg-white dark:bg-gray-900 shadow-sm">
                        <div className="min-w-full">
                            <div
                                className="grid items-center gap-x-10 bg-gray-100 dark:bg-gray-800 border-b border-gray-100 dark:border-gray-800 px-10 py-4 text-[11px] font-medium text-gray-900 dark:text-gray-200 uppercase tracking-[0.18em]"
                                style={NAMESERVER_TABLE_GRID_STYLE}
                            >
                                <div className="min-w-0 whitespace-nowrap">Address</div>
                                <div className="text-center whitespace-nowrap">Status</div>
                                <div className="text-center whitespace-nowrap">Pulse</div>
                                <div className="text-right whitespace-nowrap">Actions</div>
                            </div>

                            {isLoading && !data ? (
                                <div className="px-6 py-8 text-sm text-gray-500 dark:text-gray-400">
                                    Loading NameServer settings...
                                </div>
                            ) : (
                                <motion.div
                                    className="divide-y divide-gray-100 dark:divide-gray-800"
                                    initial="hidden"
                                    animate="show"
                                    variants={{
                                        hidden: { opacity: 0 },
                                        show: {
                                            opacity: 1,
                                            transition: {
                                                staggerChildren: 0.08,
                                            },
                                        },
                                    }}
                                >
                                    <AnimatePresence mode="popLayout">
                                        {servers.map((server) => {
                                            const { address, isCurrent, isAlive } = server;
                                            const isSwitching = pendingAction === `switch:${address}`;
                                            const isDeleting = pendingAction === `delete:${address}`;

                                            return (
                                                <motion.div
                                                    layout
                                                    key={address}
                                                    variants={{
                                                        hidden: { opacity: 0, y: 10 },
                                                        show: { opacity: 1, y: 0 },
                                                    }}
                                                    exit={{ opacity: 0, height: 0, marginBottom: 0, transition: { duration: 0.2 } }}
                                                    whileHover={{ scale: 1.002 }}
                                                    transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                                                    className={`grid items-center gap-x-10 px-10 py-5 transition-colors relative group hover:bg-gray-50 dark:hover:bg-gray-800/50 ${
                                                        isCurrent ? 'bg-gray-100 dark:bg-gray-800' : 'bg-white dark:bg-gray-900'
                                                    }`}
                                                    style={NAMESERVER_TABLE_GRID_STYLE}
                                                >
                                                    {isCurrent && (
                                                        <motion.div
                                                            layoutId="active-row-indicator"
                                                            className="absolute left-0 top-0 bottom-0 w-1 bg-blue-500"
                                                        />
                                                    )}

                                                    <div className="flex-1 min-w-0 font-mono text-sm text-gray-700 dark:text-gray-300 flex items-center">
                                                        <Server
                                                            className={`w-4 h-4 mr-3 shrink-0 ${
                                                                isCurrent ? 'text-blue-500' : 'text-gray-400 dark:text-gray-500'
                                                            }`}
                                                        />
                                                        <span className="truncate">{address}</span>
                                                    </div>

                                                    <div className="flex justify-center px-2">
                                                        {isCurrent ? (
                                                            <span className="inline-flex items-center px-3.5 py-1 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-400 border border-green-200 dark:border-green-800 shadow-sm">
                                                                <motion.span
                                                                    animate={{ scale: [1, 1.2, 1], opacity: [1, 0.5, 1] }}
                                                                    transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
                                                                    className="w-1.5 h-1.5 rounded-full bg-green-500 mr-1.5"
                                                                />
                                                                Active
                                                            </span>
                                                        ) : (
                                                            <span className="inline-flex items-center px-3.5 py-1 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400 border border-gray-200 dark:border-gray-700">
                                                                Standby
                                                            </span>
                                                        )}
                                                    </div>

                                                    <div className="flex justify-center px-2">
                                                        <NameServerHeartbeatWave isAlive={isAlive} />
                                                    </div>

                                                    <div className="flex justify-end items-center space-x-3 px-2">
                                                        {!isCurrent && (
                                                            <motion.button
                                                                whileHover={{ scale: 1.05 }}
                                                                whileTap={{ scale: 0.95 }}
                                                                onClick={() => void handleAsyncAction(() => switchNameServer(address))}
                                                                disabled={isBusy}
                                                                className="text-xs font-medium px-3 py-1.5 rounded-md border transition-colors disabled:opacity-50 disabled:cursor-not-allowed bg-blue-50 text-blue-700 border-blue-200 hover:bg-blue-100 hover:border-blue-300 dark:bg-slate-800 dark:text-blue-200 dark:border-slate-600 dark:hover:bg-slate-700 dark:hover:border-blue-500/60"
                                                            >
                                                                {isSwitching ? 'Switching...' : 'Switch'}
                                                            </motion.button>
                                                        )}
                                                        <motion.button
                                                            whileHover={{ scale: isCurrent || isBusy ? 1 : 1.05 }}
                                                            whileTap={{ scale: isCurrent || isBusy ? 1 : 0.95 }}
                                                            onClick={() => void handleAsyncAction(() => deleteNameServer(address))}
                                                            className={`p-1.5 rounded-md border transition-colors ${
                                                                isCurrent
                                                                    ? 'text-gray-300 dark:text-gray-700 border-transparent cursor-not-allowed'
                                                                    : 'text-rose-600 border-rose-200 bg-rose-50 hover:bg-rose-100 hover:border-rose-300 dark:text-rose-200 dark:border-slate-600 dark:bg-slate-800 dark:hover:bg-slate-700 dark:hover:border-rose-500/60'
                                                            } ${isBusy ? 'opacity-50 cursor-not-allowed' : ''}`}
                                                            disabled={isCurrent || isBusy}
                                                        >
                                                            <Trash2 className="w-4 h-4" />
                                                        </motion.button>
                                                    </div>
                                                </motion.div>
                                            );
                                        })}
                                    </AnimatePresence>
                                </motion.div>
                            )}
                        </div>
                    </div>
                </div>
            </Card>

            <Card title="Connection Security" description="Configure secure transport layers.">
                <div className="divide-y divide-gray-100 dark:divide-gray-800">
                    <div className="flex items-center justify-between py-4">
                        <span className="text-sm font-medium text-gray-900 dark:text-gray-300">VIP Channel</span>
                        <Toggle
                            checked={data?.useVIPChannel ?? false}
                            onChange={(checked) => void handleAsyncAction(() => updateVipChannel(checked))}
                            disabled={isLoading || isBusy || !data}
                        />
                    </div>
                    <div className="flex items-center justify-between py-4">
                        <span className="text-sm font-medium text-gray-900 dark:text-gray-300">TLS Encryption</span>
                        <Toggle
                            checked={data?.useTLS ?? false}
                            onChange={(checked) => void handleAsyncAction(() => updateUseTls(checked))}
                            disabled={isLoading || isBusy || !data}
                        />
                    </div>
                </div>
            </Card>
        </div>
    );
};
