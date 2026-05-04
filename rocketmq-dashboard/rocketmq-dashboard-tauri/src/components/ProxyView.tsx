import React from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { Activity, CheckCircle2, Network, Plus, RefreshCw, Server, Trash2 } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { useProxyCatalog } from '../features/proxy/hooks/useProxyCatalog';
import { Card } from '../components/ui/LegacyCard';
import { Button } from '../components/ui/LegacyButton';
import { Input } from '../components/ui/LegacyInput';

export const ProxyView = () => {
    const {
        snapshot,
        isLoading,
        loadError,
        pendingAction,
        newAddress,
        setNewAddress,
        loadHomePage,
        addProxy,
        switchProxy,
        deleteProxy,
    } = useProxyCatalog();

    const proxies = snapshot?.proxyAddrList ?? [];
    const currentProxy = snapshot?.currentProxyAddr ?? null;
    const isBusy = pendingAction !== null;

    const handleAsyncAction = async (action: () => Promise<string | undefined>) => {
        try {
            const message = await action();
            if (message) {
                toast.success(message);
            }
        } catch (error) {
            toast.error(error instanceof Error ? error.message : 'Proxy operation failed');
        }
    };

    const statusLabel = currentProxy ? 'Configured' : 'Not Set';

    return (
        <div className="max-w-4xl mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
                    <div className="w-10 h-10 rounded-full bg-green-50 dark:bg-green-900/30 flex items-center justify-center">
                        <CheckCircle2 className="w-5 h-5 text-green-600 dark:text-green-400"/>
                    </div>
                    <div>
                        <div className="text-sm text-gray-500 dark:text-gray-400">Stored Proxies</div>
                        <div className="text-xl font-bold text-gray-900 dark:text-white">{proxies.length}</div>
                    </div>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
                    <div className="w-10 h-10 rounded-full bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center">
                        <Activity className="w-5 h-5 text-blue-600 dark:text-blue-400"/>
                    </div>
                    <div>
                        <div className="text-sm text-gray-500 dark:text-gray-400">Status</div>
                        <div className="text-xl font-bold text-gray-900 dark:text-white">{statusLabel}</div>
                    </div>
                </div>
                <div className="bg-white dark:bg-gray-900 rounded-2xl p-5 border border-gray-100 dark:border-gray-800 shadow-sm flex items-center space-x-4">
                    <div className="w-10 h-10 rounded-full bg-purple-50 dark:bg-purple-900/30 flex items-center justify-center">
                        <Network className="w-5 h-5 text-purple-600 dark:text-purple-400"/>
                    </div>
                    <div>
                        <div className="text-sm text-gray-500 dark:text-gray-400">Traffic Mode</div>
                        <div className="text-xl font-bold text-gray-900 dark:text-white">Local Catalog</div>
                    </div>
                </div>
            </div>

            <Card
                title="Proxy Server Address List"
                description="Manage the local proxy address catalog used by dashboard proxy-aware queries."
                headerAction={
                    <Button
                        variant="outline"
                        icon={RefreshCw}
                        onClick={() => void handleAsyncAction(async () => {
                            await loadHomePage();
                            return 'Proxy settings reloaded';
                        })}
                        disabled={isBusy || isLoading}
                    >
                        Refresh
                    </Button>
                }
            >
                <div className="space-y-8">
                    {loadError ? (
                        <div className="flex items-center justify-between gap-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-200">
                            <span>{loadError}</span>
                            <Button
                                variant="outline"
                                onClick={() => void handleAsyncAction(async () => {
                                    await loadHomePage();
                                    return 'Proxy settings reloaded';
                                })}
                                disabled={isBusy}
                                className="shrink-0"
                            >
                                Retry
                            </Button>
                        </div>
                    ) : null}

                    <div className="bg-gray-50/50 dark:bg-gray-900/50 rounded-xl p-5 border border-gray-100 dark:border-gray-800">
                        <label className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-3 block">Add New Proxy</label>
                        <div className="flex gap-3">
                            <div className="flex-1">
                                <Input
                                    placeholder="Enter proxy address (e.g. 192.168.1.50:8080)"
                                    value={newAddress}
                                    onChange={(event) => setNewAddress(event.target.value)}
                                    disabled={isBusy}
                                    className="bg-white dark:bg-gray-800 dark:text-white dark:border-gray-700 shadow-sm"
                                />
                            </div>
                            <Button
                                onClick={() => void handleAsyncAction(addProxy)}
                                icon={Plus}
                                disabled={isBusy}
                                className="dark:!bg-gray-900 dark:!text-white dark:border dark:border-gray-700 dark:hover:!bg-gray-800"
                            >
                                {pendingAction === 'add' ? 'Adding...' : 'Add Server'}
                            </Button>
                        </div>
                    </div>

                    <div>
                        <div className="flex items-center justify-between mb-4">
                            <h4 className="text-sm font-semibold text-gray-900 dark:text-white">Configured Servers</h4>
                            <span
                                className="text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded-full">{proxies.length} total</span>
                        </div>

                        {isLoading && !snapshot ? (
                            <div className="rounded-xl border border-gray-100 bg-white px-4 py-8 text-sm text-gray-500 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400">
                                Loading Proxy settings...
                            </div>
                        ) : proxies.length === 0 ? (
                            <div className="rounded-xl border border-dashed border-gray-200 bg-white px-4 py-10 text-center dark:border-gray-800 dark:bg-gray-900">
                                <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-gray-100 dark:bg-gray-800">
                                    <Server className="h-5 w-5 text-gray-500 dark:text-gray-400" />
                                </div>
                                <div className="text-sm font-medium text-gray-900 dark:text-white">No proxy address configured</div>
                                <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                                    Add a host:port address to make it available for proxy-aware dashboard queries.
                                </div>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                <AnimatePresence mode="popLayout">
                                    {proxies.map((proxy) => {
                                        const isCurrent = proxy === currentProxy;
                                        const isSwitching = pendingAction === `switch:${proxy}`;
                                        const isDeleting = pendingAction === `delete:${proxy}`;

                                        return (
                                            <motion.div
                                                layout
                                                key={proxy}
                                                initial={{ opacity: 0, y: 10, scale: 0.98 }}
                                                animate={{ opacity: 1, y: 0, scale: 1 }}
                                                exit={{ opacity: 0, scale: 0.95, transition: { duration: 0.2 } }}
                                                transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                                                className={`group flex items-center justify-between p-4 border rounded-xl hover:border-blue-200 dark:hover:border-blue-800 hover:shadow-md transition-all duration-200 ${
                                                    isCurrent
                                                        ? 'bg-blue-50/60 border-blue-100 dark:bg-blue-950/20 dark:border-blue-900/50'
                                                        : 'bg-white border-gray-100 dark:bg-gray-900 dark:border-gray-800'
                                                }`}
                                            >
                                                <div className="flex min-w-0 items-center space-x-4">
                                                    <div
                                                        className={`w-8 h-8 rounded-lg flex items-center justify-center transition-colors ${
                                                            isCurrent
                                                                ? 'bg-blue-100 dark:bg-blue-900/40'
                                                                : 'bg-gray-100 dark:bg-gray-800 group-hover:bg-blue-50 dark:group-hover:bg-blue-900/30'
                                                        }`}
                                                    >
                                                        <Server
                                                            className={`w-4 h-4 ${
                                                                isCurrent
                                                                    ? 'text-blue-600 dark:text-blue-300'
                                                                    : 'text-gray-500 dark:text-gray-400 group-hover:text-blue-600 dark:group-hover:text-blue-400'
                                                            }`}
                                                        />
                                                    </div>
                                                    <div className="min-w-0">
                                                        <div className="truncate text-sm font-medium text-gray-900 dark:text-white font-mono">
                                                            {proxy}
                                                        </div>
                                                        <div className="mt-0.5 flex items-center text-xs text-gray-500 dark:text-gray-400">
                                                            <span
                                                                className={`mr-1.5 h-1.5 w-1.5 rounded-full ${
                                                                    isCurrent ? 'bg-blue-500' : 'bg-gray-400 dark:bg-gray-500'
                                                                }`}
                                                            />
                                                            {isCurrent ? 'Current proxy address' : 'Stored proxy address'}
                                                        </div>
                                                    </div>
                                                </div>

                                                <div className="flex shrink-0 items-center space-x-2">
                                                    {isCurrent ? (
                                                        <span className="inline-flex items-center rounded-full border border-blue-200 bg-blue-100 px-3 py-1 text-xs font-medium text-blue-700 dark:border-blue-800 dark:bg-blue-900/30 dark:text-blue-200">
                                                            Current
                                                        </span>
                                                    ) : (
                                                        <Button
                                                            variant="outline"
                                                            onClick={() => void handleAsyncAction(() => switchProxy(proxy))}
                                                            disabled={isBusy}
                                                            className="h-8 px-3"
                                                        >
                                                            {isSwitching ? 'Using...' : 'Use'}
                                                        </Button>
                                                    )}
                                                    <button
                                                        type="button"
                                                        onClick={() => void handleAsyncAction(() => deleteProxy(proxy))}
                                                        disabled={isBusy}
                                                        className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-rose-200 bg-rose-50 text-rose-600 transition-colors hover:bg-rose-100 hover:border-rose-300 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-600 dark:bg-slate-800 dark:text-rose-200 dark:hover:bg-slate-700 dark:hover:border-rose-500/60"
                                                        aria-label={`Delete proxy ${proxy}`}
                                                    >
                                                        {isDeleting ? (
                                                            <RefreshCw className="h-4 w-4 animate-spin" />
                                                        ) : (
                                                            <Trash2 className="h-4 w-4" />
                                                        )}
                                                    </button>
                                                </div>
                                            </motion.div>
                                        );
                                    })}
                                </AnimatePresence>
                            </div>
                        )}
                    </div>

                    {currentProxy ? (
                        <div className="rounded-xl border border-gray-100 bg-gray-50/70 px-4 py-3 text-sm text-gray-600 dark:border-gray-800 dark:bg-gray-900/50 dark:text-gray-300">
                            Current Proxy:
                            <span className="ml-2 font-mono font-medium text-gray-900 dark:text-white">{currentProxy}</span>
                        </div>
                    ) : null}
                </div>
            </Card>
        </div>
    );
};
