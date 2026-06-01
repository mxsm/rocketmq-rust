import React from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
    Activity,
    CheckCircle2,
    Lock,
    Plus,
    RefreshCw,
    Route,
    Server,
    ShieldCheck,
    Trash2,
    Wifi
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { useNameServer } from '../features/nameserver/hooks/useNameServer';
import { Button } from '../components/ui/LegacyButton';
import { Input } from '../components/ui/LegacyInput';
import { Toggle } from '../components/ui/LegacyToggle';

const HEARTBEAT_SIGNAL_PATH = 'M10 22 H32 L39 13 L48 30 L57 18 L66 22 H78 L87 10 L96 31 L105 20 H122';
const OFFLINE_SIGNAL_PATH = 'M10 22 H122';
const HEARTBEAT_NODES = [0, 1, 2, 3];

const NameServerHeartbeatWave = ({ isAlive }: { isAlive: boolean }) => (
    <div
        className={`nameserver-pulse ${isAlive ? 'is-alive' : 'is-offline'}`}
        aria-label={isAlive ? 'Reachable' : 'No heartbeat'}
    >
        <span className="nameserver-pulse-grid" />
        <span className="nameserver-pulse-glow" />
        {isAlive ? (
            <motion.span
                className="nameserver-pulse-sweep"
                animate={{ x: ['-34px', '188px'], opacity: [0, 0.88, 0] }}
                transition={{ duration: 1.65, repeat: Infinity, ease: 'easeInOut' }}
            />
        ) : null}
        <svg viewBox="0 0 132 44" className="nameserver-pulse-wave" aria-hidden="true">
            <motion.path
                d={isAlive ? HEARTBEAT_SIGNAL_PATH : OFFLINE_SIGNAL_PATH}
                fill="none"
                stroke={isAlive ? 'rgba(45, 212, 191, 0.98)' : 'rgba(117, 131, 151, 0.72)'}
                strokeWidth="2.6"
                strokeLinecap="round"
                strokeLinejoin="round"
                animate={
                    isAlive
                        ? {
                              opacity: [0.62, 1, 0.72],
                              pathLength: [0.16, 1, 1],
                          }
                        : {
                              opacity: [0.36, 0.58, 0.36],
                          }
                }
                transition={{
                    duration: isAlive ? 1.65 : 2.6,
                    repeat: Infinity,
                    ease: 'easeInOut',
                }}
            />
        </svg>
        <span className="nameserver-pulse-nodes" aria-hidden="true">
            {HEARTBEAT_NODES.map((node) => (
                <motion.span
                    key={node}
                    animate={
                        isAlive
                            ? { opacity: [0.25, 1, 0.25], scale: [0.72, 1.22, 0.72] }
                            : { opacity: [0.22, 0.5, 0.22], scale: 1 }
                    }
                    transition={{
                        duration: isAlive ? 1.35 : 2.4,
                        repeat: Infinity,
                        ease: 'easeInOut',
                        delay: node * 0.18,
                    }}
                />
            ))}
        </span>
    </div>
);

const SecurityRow = ({
    icon: Icon,
    label,
    description,
    checked,
    disabled,
    onChange,
}: {
    icon: React.ElementType;
    label: string;
    description: string;
    checked: boolean;
    disabled: boolean;
    onChange: (checked: boolean) => void;
}) => (
    <div className="nameserver-security-row">
        <div className="nameserver-security-copy">
            <span className="nameserver-security-icon">
                <Icon className="nameserver-icon" />
            </span>
            <span>
                <strong>{label}</strong>
                <small>{description}</small>
            </span>
        </div>
        <Toggle checked={checked} onChange={onChange} disabled={disabled} />
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
    const aliveCount = servers.filter((server) => server.isAlive).length;
    const standbyCount = servers.filter((server) => !server.isCurrent).length;
    const selectedServer = servers.find((server) => server.isCurrent)?.address ?? currentNameServer ?? 'Not selected';

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
        <div className="nameserver-page">
            <section className="nameserver-hero">
                <div className="nameserver-hero-copy">
                    <div className="nameserver-kicker">
                        <Wifi className="nameserver-icon" />
                        NameServer control plane
                    </div>
                    <h2>Endpoint routing and transport security</h2>
                    <p>
                        Keep active NameServer routing, heartbeat posture, and connection security visible while making endpoint changes.
                    </p>
                </div>

                <div className="nameserver-hero-current">
                    <span>Current route</span>
                    <strong title={selectedServer}>{selectedServer}</strong>
                </div>
            </section>

            <section className="nameserver-command-bar" aria-label="Add NameServer">
                <div className="nameserver-command-input">
                    <span>namesrv endpoint</span>
                    <Input
                        placeholder="Enter address, for example 192.168.1.50:9876"
                        value={newAddress}
                        onChange={(event) => setNewAddress(event.target.value)}
                        disabled={isBusy}
                    />
                </div>
                <Button
                    variant="accent"
                    onClick={() => void handleAsyncAction(addNameServer)}
                    icon={Plus}
                    disabled={isBusy}
                    className="nameserver-command-button"
                >
                    Add Server
                </Button>
                <Button
                    variant="outline"
                    onClick={() => void handleAsyncAction(async () => {
                        await loadHomePage();
                        return 'NameServer settings reloaded';
                    })}
                    icon={RefreshCw}
                    disabled={isBusy}
                    className="nameserver-command-button"
                >
                    Refresh
                </Button>
            </section>

            {loadError ? (
                <div className="nameserver-alert" role="alert">
                    <span>{loadError}</span>
                    <Button
                        variant="outline"
                        onClick={() => void handleAsyncAction(async () => {
                            await loadHomePage();
                            return 'NameServer settings reloaded';
                        })}
                        disabled={isBusy}
                    >
                        Retry
                    </Button>
                </div>
            ) : null}

            <section className="nameserver-stats" aria-label="NameServer summary">
                <div className="nameserver-stat-card">
                    <span>Configured</span>
                    <strong>{servers.length}</strong>
                    <small>Total endpoints</small>
                </div>
                <div className="nameserver-stat-card">
                    <span>Reachable</span>
                    <strong>{aliveCount}/{servers.length || 0}</strong>
                    <small>Heartbeat checks</small>
                </div>
                <div className="nameserver-stat-card">
                    <span>Standby</span>
                    <strong>{standbyCount}</strong>
                    <small>Switchable routes</small>
                </div>
                <div className="nameserver-stat-card">
                    <span>Transport</span>
                    <strong>{data?.useTLS ? 'TLS' : data?.useVIPChannel ? 'VIP' : 'Plain'}</strong>
                    <small>{data?.useVIPChannel ? 'VIP channel enabled' : 'VIP channel off'}</small>
                </div>
            </section>

            <div className="nameserver-workspace">
                <section className="nameserver-panel nameserver-inventory">
                    <div className="nameserver-panel-header">
                        <div>
                            <h3>Endpoint inventory</h3>
                            <p>Active route, standby endpoints, heartbeat signal, and endpoint actions.</p>
                        </div>
                    </div>

                    <div className="nameserver-table">
                        <div className="nameserver-table-head">
                            <span>Address</span>
                            <span>Role</span>
                            <span>Pulse</span>
                            <span>Actions</span>
                        </div>

                        {isLoading && !data ? (
                            <div className="nameserver-empty">Loading NameServer settings...</div>
                        ) : servers.length ? (
                            <motion.div
                                className="nameserver-table-body"
                                initial="hidden"
                                animate="show"
                                variants={{
                                    hidden: { opacity: 0 },
                                    show: {
                                        opacity: 1,
                                        transition: { staggerChildren: 0.08 },
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
                                                exit={{ opacity: 0, height: 0, transition: { duration: 0.2 } }}
                                                transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                                                className={`nameserver-row ${isCurrent ? 'is-current' : ''}`}
                                            >
                                                <div className="nameserver-address-cell">
                                                    <span className="nameserver-node-icon">
                                                        <Server className="nameserver-icon" />
                                                    </span>
                                                    <div>
                                                        <strong title={address}>{address}</strong>
                                                        <small>{isAlive ? 'Heartbeat responding' : 'No heartbeat response'}</small>
                                                    </div>
                                                </div>

                                                <div className="nameserver-role-cell">
                                                    <span className={`nameserver-role-badge ${isCurrent ? 'is-active' : ''}`}>
                                                        {isCurrent ? (
                                                            <>
                                                                <CheckCircle2 className="nameserver-badge-icon" />
                                                                Active
                                                            </>
                                                        ) : (
                                                            'Standby'
                                                        )}
                                                    </span>
                                                </div>

                                                <div className="nameserver-pulse-cell">
                                                    <NameServerHeartbeatWave isAlive={isAlive} />
                                                </div>

                                                <div className="nameserver-actions-cell">
                                                    {!isCurrent && (
                                                        <Button
                                                            variant="outline"
                                                            onClick={() => void handleAsyncAction(() => switchNameServer(address))}
                                                            disabled={isBusy}
                                                        >
                                                            {isSwitching ? 'Switching...' : 'Switch'}
                                                        </Button>
                                                    )}
                                                    <button
                                                        type="button"
                                                        onClick={() => void handleAsyncAction(() => deleteNameServer(address))}
                                                        className="nameserver-delete-button"
                                                        disabled={isCurrent || isBusy}
                                                        aria-label={`Delete ${address}`}
                                                    >
                                                        <Trash2 className="nameserver-icon" />
                                                        <span>{isDeleting ? 'Deleting' : 'Delete'}</span>
                                                    </button>
                                                </div>
                                            </motion.div>
                                        );
                                    })}
                                </AnimatePresence>
                            </motion.div>
                        ) : (
                            <div className="nameserver-empty">No NameServer endpoints configured.</div>
                        )}
                    </div>
                </section>

                <aside className="nameserver-side-rail">
                    <section className="nameserver-panel">
                        <div className="nameserver-panel-header">
                            <div>
                                <h3>Route focus</h3>
                                <p>Selected endpoint for admin operations.</p>
                            </div>
                        </div>

                        <div className="nameserver-route-card">
                            <span className="nameserver-route-icon">
                                <Route className="nameserver-icon" />
                            </span>
                            <div>
                                <span>Current NameServer</span>
                                <strong title={selectedServer}>{selectedServer}</strong>
                            </div>
                        </div>

                        <div className="nameserver-mini-metrics">
                            <div>
                                <Activity className="nameserver-icon" />
                                <span>{aliveCount ? 'Responsive' : 'Waiting'}</span>
                            </div>
                            <div>
                                <ShieldCheck className="nameserver-icon" />
                                <span>{data?.useTLS ? 'Encrypted' : 'Local policy'}</span>
                            </div>
                        </div>
                    </section>

                    <section className="nameserver-panel">
                        <div className="nameserver-panel-header">
                            <div>
                                <h3>Connection security</h3>
                                <p>Transport settings remain available while managing endpoints.</p>
                            </div>
                        </div>

                        <div className="nameserver-security-list">
                            <SecurityRow
                                icon={Wifi}
                                label="VIP Channel"
                                description="Route through VIP-aware broker addresses."
                                checked={data?.useVIPChannel ?? false}
                                disabled={isLoading || isBusy || !data}
                                onChange={(checked) => void handleAsyncAction(() => updateVipChannel(checked))}
                            />
                            <SecurityRow
                                icon={Lock}
                                label="TLS Encryption"
                                description="Use encrypted transport for NameServer calls."
                                checked={data?.useTLS ?? false}
                                disabled={isLoading || isBusy || !data}
                                onChange={(checked) => void handleAsyncAction(() => updateUseTls(checked))}
                            />
                        </div>
                    </section>
                </aside>
            </div>
        </div>
    );
};
