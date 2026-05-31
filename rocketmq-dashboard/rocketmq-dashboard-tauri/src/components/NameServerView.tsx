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

const HEARTBEAT_WAVE_PATHS = [
    'M4 20 C10 20, 12 9, 18 9 S26 28, 33 20 S42 8, 50 15 S58 28, 66 20 S76 10, 84 16 S94 20, 108 20',
    'M4 20 C10 20, 12 13, 18 13 S26 24, 33 20 S42 12, 50 17 S58 24, 66 20 S76 13, 84 17 S94 20, 108 20',
    'M4 20 C10 20, 12 7, 18 7 S26 31, 33 20 S42 6, 50 14 S58 31, 66 20 S76 8, 84 15 S94 20, 108 20',
];
const OFFLINE_WAVE_PATH = 'M4 20 L108 20';

const NameServerHeartbeatWave = ({ isAlive }: { isAlive: boolean }) => (
    <div className={`nameserver-pulse ${isAlive ? 'is-alive' : ''}`} aria-label={isAlive ? 'Reachable' : 'No heartbeat'}>
        <span className="nameserver-pulse-baseline" />
        <svg viewBox="0 0 112 40" className="nameserver-pulse-wave" aria-hidden="true">
            <motion.path
                d={isAlive ? HEARTBEAT_WAVE_PATHS[0] : OFFLINE_WAVE_PATH}
                fill="none"
                stroke={isAlive ? 'rgba(52, 211, 153, 0.95)' : 'rgba(117, 131, 151, 0.86)'}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                animate={
                    isAlive
                        ? {
                              d: HEARTBEAT_WAVE_PATHS,
                              opacity: [0.72, 1, 0.82, 1],
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
                    fill="rgba(52, 211, 153, 1)"
                    animate={{ opacity: [0.45, 1, 0.45], scale: [0.9, 1.15, 0.9] }}
                    transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
                />
            ) : (
                <circle cx="104" cy="20" r="2.2" fill="rgba(117, 131, 151, 0.78)" />
            )}
        </svg>
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
