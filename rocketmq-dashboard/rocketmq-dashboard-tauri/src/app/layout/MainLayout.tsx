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
    Globe,
    Network,
    RefreshCw,
    Moon,
    Sun,
    ChevronDown,
    LogOut,
    UserRound
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

export const MainLayout = ({children}: MainLayoutProps) => {
    const {isDark, toggleTheme} = useTheme();
    const {activeTab, currentUser, pageTitle, setActiveTab} = useAppStore();
    const {logout} = useAuth();
    const [isAccountMenuOpen, setIsAccountMenuOpen] = useState(false);
    const [isLogoutDialogOpen, setIsLogoutDialogOpen] = useState(false);
    const [isLoggingOut, setIsLoggingOut] = useState(false);
    const initials = (currentUser?.username ?? 'AD').slice(0, 2).toUpperCase();

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
        <div className={`flex h-screen font-sans text-gray-900 selection:bg-blue-100 ${isDark ? 'dark bg-gray-950 text-white' : 'bg-gray-50/50'}`}>
            <Toaster position="bottom-right" theme={isDark ? 'dark' : 'light'}/>

            {/* Sidebar */}
            <div
                className="w-64 bg-white/80 dark:bg-gray-900/80 backdrop-blur-xl border-r border-gray-200 dark:border-gray-800 flex flex-col h-full flex-shrink-0 transition-colors duration-300">
                <div className="p-5 flex items-center space-x-3">
                    <div className="w-12 h-12 rounded-lg flex items-center justify-center overflow-hidden">
                        <img src={rocketLogo} alt="RocketMQ-Rust" className="w-full h-full object-cover scale-125"/>
                    </div>
                    <span className="font-bold text-gray-900 dark:text-white tracking-tight">RocketMQ-Rust</span>
                </div>

                <div className="flex-1 overflow-y-auto px-4 py-2 space-y-8">
                    <div>
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 px-3">Platform</div>
                        <SidebarItem icon={Settings} label="NameServer" active={activeTab === 'NameServer'} onClick={() => setActiveTab('NameServer')}/>
                        <SidebarItem icon={LayoutDashboard} label="Dashboard" active={activeTab === 'Dashboard'} onClick={() => setActiveTab('Dashboard')}/>
                        <SidebarItem icon={Network} label="Proxy" active={activeTab === 'Proxy'} onClick={() => setActiveTab('Proxy')}/>
                        <SidebarItem icon={Server} label="Cluster" active={activeTab === 'Cluster'} onClick={() => setActiveTab('Cluster')}/>
                    </div>

                    <div>
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 px-3">Messaging</div>
                        <SidebarItem icon={FileText} label="Topic" active={activeTab === 'Topic'} onClick={() => setActiveTab('Topic')}/>
                        <SidebarItem icon={Users} label="Consumer" active={activeTab === 'Consumer'} onClick={() => setActiveTab('Consumer')}/>
                        <SidebarItem icon={Database} label="Producer" active={activeTab === 'Producer'} onClick={() => setActiveTab('Producer')}/>
                        <SidebarItem icon={MessageSquare} label="Message" active={activeTab === 'Message'} onClick={() => setActiveTab('Message')}/>
                        <SidebarItem icon={Activity} label="MessageTrace" active={activeTab === 'MessageTrace'} onClick={() => setActiveTab('MessageTrace')}/>
                        <SidebarItem icon={RefreshCw} label="DLQMessage" active={activeTab === 'DLQ'} onClick={() => setActiveTab('DLQ')}/>
                    </div>

                    <div>
                        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 px-3">Advanced</div>
                        <SidebarItem icon={Shield} label="ACL Management" active={activeTab === 'ACL'} onClick={() => setActiveTab('ACL')}/>
                    </div>
                </div>

                <div className="p-4 border-t border-gray-100 dark:border-gray-800 space-y-2">
                    <div className="flex items-center justify-between px-1">
                        <div className="flex space-x-1">
                            <button
                                onClick={toggleTheme}
                                className="p-2 text-gray-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                                title="Theme"
                            >
                                {isDark ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
                            </button>
                            <button
                                className="p-2 text-gray-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                                title="Language">
                                <Globe className="w-4 h-4"/>
                            </button>
                        </div>
                        <span className="text-[10px] font-mono text-gray-400">v0.1.0</span>
                    </div>

                    <DropdownMenu open={isAccountMenuOpen} onOpenChange={setIsAccountMenuOpen}>
                        <DropdownMenuTrigger asChild>
                            <button
                                className={`group flex w-full items-center space-x-3 rounded-xl px-2 py-2 transition-colors ${
                                    isAccountMenuOpen
                                        ? 'bg-gray-100 dark:bg-gray-800'
                                        : 'hover:bg-gray-100 dark:hover:bg-gray-800'
                                }`}
                            >
                                <div
                                    className="flex h-8 w-8 items-center justify-center rounded-full bg-gradient-to-tr from-gray-800 to-gray-900 text-xs font-medium text-white shadow-sm transition-all group-hover:shadow-md">
                                    {initials}
                                </div>
                                <div className="min-w-0 flex-1 text-left">
                                    <div className="truncate text-sm font-bold text-gray-900 dark:text-white">
                                        {currentUser?.username ?? 'Admin'}
                                    </div>
                                    <div className="truncate text-xs text-gray-500 transition-colors group-hover:text-gray-700 dark:group-hover:text-gray-400">
                                        Account &amp; security
                                    </div>
                                </div>
                                <ChevronDown
                                    className={`h-3.5 w-3.5 text-gray-400 transition-transform ${isAccountMenuOpen ? 'rotate-180' : ''}`}
                                />
                            </button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent
                            side="top"
                            align="start"
                            sideOffset={10}
                            className="w-60 rounded-2xl border border-white/10 bg-white/92 p-2 text-gray-900 shadow-[0_24px_64px_rgba(15,23,42,0.18)] backdrop-blur-2xl dark:bg-[#0f172a]/92 dark:text-white"
                        >
                            <DropdownMenuLabel className="px-3 py-2">
                                <div className="truncate text-sm font-semibold">{currentUser?.username ?? 'Admin'}</div>
                                <div className="text-xs font-normal text-gray-500 dark:text-gray-400">Local dashboard administrator</div>
                            </DropdownMenuLabel>
                            <DropdownMenuSeparator className="bg-gray-200/80 dark:bg-white/10" />
                            <DropdownMenuItem
                                onSelect={() => setActiveTab('Account')}
                                className="rounded-xl px-3 py-2 text-sm text-gray-700 focus:bg-gray-100 focus:text-gray-900 dark:text-gray-200 dark:focus:bg-white/8 dark:focus:text-white"
                            >
                                <UserRound className="h-4 w-4 text-sky-500" />
                                View Profile
                            </DropdownMenuItem>
                            <DropdownMenuItem
                                variant="destructive"
                                onSelect={() => setIsLogoutDialogOpen(true)}
                                className="rounded-xl px-3 py-2 text-sm"
                            >
                                <LogOut className="h-4 w-4" />
                                Sign Out
                            </DropdownMenuItem>
                        </DropdownMenuContent>
                    </DropdownMenu>
                </div>
            </div>

            {/* Main Content */}
            <div className="relative flex-1 flex flex-col min-w-0 overflow-hidden bg-[#F5F5F7] dark:bg-[#0A0A0A] transition-colors duration-300">
                {/* Top Header */}
                <header
                    className="h-16 px-8 flex items-center justify-between bg-white/50 dark:bg-gray-900/50 backdrop-blur-md border-b border-gray-200/60 dark:border-gray-800 sticky top-0 z-10 transition-colors duration-300">
                    <div className="flex items-center space-x-4">
                        <h1 className="text-xl font-semibold text-gray-900 dark:text-white">{pageTitle}</h1>
                    </div>

                    <div className="flex items-center space-x-4">
                    </div>
                </header>

                {/* Content Area */}
                <main className="flex-1 overflow-y-auto p-8">
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
            </div>
        </div>
    );
};
