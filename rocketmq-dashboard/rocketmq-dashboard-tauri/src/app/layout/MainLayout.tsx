import React, {useState} from 'react';
import {
    LayoutDashboard,
    Settings,
    Server,
    Database,
    MessageSquare,
    Users,
    FileText,
    Shield,
    Activity,
    Network,
    RefreshCw,
    Moon,
    Sun,
    ChevronDown,
    LogOut,
    UserRound,
    CheckCircle2,
    MonitorDot,
    ArrowRight
} from 'lucide-react';
import {Toaster, toast} from 'sonner@2.0.3';
import {SidebarItem} from '../../components/ui/SidebarItem';
import rocketLogo from 'rocketmq-rust:asset/rocketmq-rust-logo.png';
import {useTheme} from '../../hooks/useTheme';
import {useAppStore} from '../../stores/app.store';
import {SignOutConfirmDialog, useAuth} from '../../features/auth';
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger
} from '../../components/ui/dropdown-menu';

interface MainLayoutProps {
    children: React.ReactNode;
}

const NAV_SECTIONS = [
    {
        title: 'Platform',
        items: [
            {tab: 'Dashboard', label: 'Dashboard', icon: LayoutDashboard},
            {tab: 'NameServer', label: 'NameServer', icon: Settings},
            {tab: 'Proxy', label: 'Proxy', icon: Network},
            {tab: 'Cluster', label: 'Cluster', icon: Server}
        ]
    },
    {
        title: 'Messaging',
        items: [
            {tab: 'Topic', label: 'Topics', icon: FileText},
            {tab: 'Consumer', label: 'Consumers', icon: Users},
            {tab: 'Producer', label: 'Producers', icon: Database},
            {tab: 'Message', label: 'Messages', icon: MessageSquare},
            {tab: 'MessageTrace', label: 'Trace', icon: Activity},
            {tab: 'DLQ', label: 'DLQ Message', icon: RefreshCw}
        ]
    },
    {
        title: 'Governance',
        items: [
            {tab: 'ACL', label: 'ACL', icon: Shield}
        ]
    }
] as const;

const PAGE_SUMMARIES: Record<string, string> = {
    Dashboard: 'Live broker health, throughput trends, topic distribution, and queue pressure.',
    NameServer: 'Connection endpoints, TLS/VIP posture, and active NameServer selection.',
    Proxy: 'Proxy endpoint catalog, reachability, and lifecycle controls.',
    Cluster: 'Broker topology, role details, and operational broker metadata.',
    Topic: 'Topic catalog, queue topology, permissions, and topic lifecycle actions.',
    Consumer: 'Consumer groups, subscriptions, retry policy, and client inspection.',
    Producer: 'Producer group discovery and client connection inventory.',
    Message: 'Message lookup by topic, key, ID, and time range.',
    MessageTrace: 'End-to-end trace timeline for produced and consumed messages.',
    DLQ: 'Dead-letter queue search, retry context, and payload inspection.',
    ACL: 'Access-control resources, credentials, and permission posture.',
    Account: 'Local administrator profile, active session, and account security.'
};

export const MainLayout = ({children}: MainLayoutProps) => {
    const {isDark, toggleTheme} = useTheme();
    const {activeTab, currentUser, pageTitle, setActiveTab} = useAppStore();
    const {logout} = useAuth();
    const [isAccountMenuOpen, setIsAccountMenuOpen] = useState(false);
    const [isLogoutDialogOpen, setIsLogoutDialogOpen] = useState(false);
    const [isLoggingOut, setIsLoggingOut] = useState(false);
    const initials = (currentUser?.username ?? 'AD').slice(0, 2).toUpperCase();
    const pageSummary = PAGE_SUMMARIES[activeTab] ?? 'Operational workspace for RocketMQ-Rust.';

    const handleLogout = async () => {
        setIsLogoutDialogOpen(false);
        setIsAccountMenuOpen(false);
        setIsLoggingOut(true);

        try {
            await logout();
            toast.success('Logged out successfully');
        } finally {
            setIsLoggingOut(false);
        }
    };

    return (
        <div className={`app-shell ${isDark ? 'dark' : ''}`}>
            <Toaster position="bottom-right" theme={isDark ? 'dark' : 'light'}/>

            <aside className="app-sidebar">
                <div className="app-brand">
                    <div className="app-brand-mark">
                        <img src={rocketLogo} alt="RocketMQ-Rust" />
                    </div>
                    <div className="app-brand-copy">
                        <span className="app-brand-name">RocketMQ-Rust</span>
                        <span className="app-brand-meta">Control plane</span>
                    </div>
                </div>

                <nav className="app-nav" aria-label="Primary navigation">
                    {NAV_SECTIONS.map((section) => (
                        <div className="app-nav-section" key={section.title}>
                            <div className="app-nav-section-title">{section.title}</div>
                            <div className="app-nav-items">
                                {section.items.map((item) => (
                                    <SidebarItem
                                        key={item.tab}
                                        icon={item.icon}
                                        label={item.label}
                                        active={activeTab === item.tab}
                                        onClick={() => setActiveTab(item.tab)}
                                    />
                                ))}
                            </div>
                        </div>
                    ))}
                </nav>

                <div className="app-sidebar-footer">
                    <button
                        onClick={toggleTheme}
                        className="app-sidebar-tool"
                        title="Theme"
                        aria-label="Toggle theme"
                    >
                        {isDark ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
                    </button>
                    <div className="app-version">
                        <span>Desktop</span>
                        <strong>v0.1.0</strong>
                    </div>
                </div>
            </aside>

            <section className="app-main">
                <header className="app-header">
                    <div className="app-page-heading">
                        <div className="app-page-kicker">
                            <span className="app-live-dot" />
                            Local session
                        </div>
                        <h1>{pageTitle}</h1>
                        <p>{pageSummary}</p>
                    </div>

                    <div className="app-header-actions">
                        <div className="app-status-pill">
                            <Shield className="w-4 h-4" />
                            <span>Secured</span>
                        </div>
                        <DropdownMenu open={isAccountMenuOpen} onOpenChange={setIsAccountMenuOpen}>
                            <DropdownMenuTrigger asChild>
                                <button
                                    className="app-user-trigger"
                                    disabled={isLoggingOut}
                                    aria-label="Open account menu"
                                >
                                    <span className="app-avatar">{initials}</span>
                                    <span className="app-user-copy">
                                        <strong>{currentUser?.username ?? 'Admin'}</strong>
                                        <span>Account</span>
                                    </span>
                                    <ChevronDown
                                        className={`app-user-chevron ${isAccountMenuOpen ? 'is-open' : ''}`}
                                    />
                                </button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent
                                side="bottom"
                                align="end"
                                sideOffset={12}
                                className="app-account-menu"
                            >
                                <DropdownMenuLabel className="app-account-label">
                                    <div className="app-account-identity">
                                        <span className="app-account-avatar">{initials}</span>
                                        <span className="app-account-copy">
                                            <span className="app-account-name">{currentUser?.username ?? 'Admin'}</span>
                                            <span className="app-account-subtitle">Local dashboard administrator</span>
                                        </span>
                                        <span className="app-account-session-chip">
                                            <span />
                                            Active
                                        </span>
                                    </div>
                                </DropdownMenuLabel>
                                <div className="app-menu-session-card">
                                    <div>
                                        <span>Session</span>
                                        <strong>Local secured session</strong>
                                    </div>
                                    <CheckCircle2 className="h-4 w-4" />
                                </div>
                                <DropdownMenuSeparator className="app-menu-separator" />
                                <DropdownMenuItem
                                    onSelect={() => setActiveTab('Account')}
                                    className="app-menu-item"
                                >
                                    <span className="app-menu-item-icon">
                                        <UserRound className="h-4 w-4" />
                                    </span>
                                    <span className="app-menu-item-copy">
                                        <span className="app-menu-item-title">View Profile</span>
                                        <span className="app-menu-item-meta">Account details</span>
                                    </span>
                                    <ArrowRight className="app-menu-item-arrow h-4 w-4" />
                                </DropdownMenuItem>
                                <DropdownMenuItem
                                    variant="destructive"
                                    onSelect={() => setIsLogoutDialogOpen(true)}
                                    className="app-menu-item app-menu-item-danger"
                                >
                                    <span className="app-menu-item-icon">
                                        <LogOut className="h-4 w-4" />
                                    </span>
                                    <span className="app-menu-item-copy">
                                        <span className="app-menu-item-title">Sign Out</span>
                                        <span className="app-menu-item-meta">End this workstation session</span>
                                    </span>
                                    <MonitorDot className="app-menu-item-arrow h-4 w-4" />
                                </DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>
                    </div>
                </header>

                <main className="app-content dashboard-main">
                    {children}
                </main>

                <SignOutConfirmDialog
                    open={isLogoutDialogOpen}
                    isSubmitting={isLoggingOut}
                    title="Sign out of RocketMQ-Rust Dashboard?"
                    description="This removes the active local session from the current workstation. You can sign back in at any time."
                    onCancel={() => setIsLogoutDialogOpen(false)}
                    onConfirm={() => void handleLogout()}
                />
            </section>
        </div>
    );
};
