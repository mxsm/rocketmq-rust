import React from 'react';
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
    ChevronDown
} from 'lucide-react';
import {Toaster, toast} from 'sonner@2.0.3';
import {SidebarItem} from '../../components/ui/SidebarItem';
import rocketLogo from 'rocketmq-rust:asset/rocketmq-rust-logo.png';
import {useTheme} from '../../hooks/useTheme';
import {useAppStore} from '../../stores/app.store';

interface MainLayoutProps {
    children: React.ReactNode;
}

export const MainLayout = ({children}: MainLayoutProps) => {
    const {isDark, toggleTheme} = useTheme();
    const {activeTab, setActiveTab, setIsLoggedIn, pageTitle} = useAppStore();

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
                        <SidebarItem icon={Settings} label="NameServer" active={activeTab === 'OPS'} onClick={() => setActiveTab('OPS')}/>
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

                    <button
                        onClick={() => {
                            toast.success("Logged out successfully");
                            setTimeout(() => setIsLoggedIn(false), 500);
                        }}
                        className="flex items-center space-x-3 w-full px-2 py-2 rounded-xl hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors group"
                    >
                        <div
                            className="w-8 h-8 rounded-full bg-gradient-to-tr from-gray-800 to-gray-900 flex items-center justify-center text-xs font-medium text-white shadow-sm group-hover:shadow-md transition-all">
                            AD
                        </div>
                        <div className="flex-1 text-left min-w-0">
                            <div className="text-sm font-bold text-gray-900 dark:text-white truncate">Admin User</div>
                            <div
                                className="text-xs text-gray-500 truncate group-hover:text-gray-700 dark:group-hover:text-gray-400 transition-colors">admin@rocketmq-rust.com
                            </div>
                        </div>
                        <ChevronDown className="w-3.5 h-3.5 text-gray-400"/>
                    </button>
                </div>
            </div>

            {/* Main Content */}
            <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-[#F5F5F7] dark:bg-[#0A0A0A] transition-colors duration-300">
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
            </div>
        </div>
    );
};
