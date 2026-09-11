import { Moon, Sun, ChevronRight, LogOut, UserRound } from 'lucide-react';
import rocketLogo from 'rocketmq-rust:asset/rocketmq-rust-logo.png';
import { useTheme } from '../../hooks/useTheme';
import { useAppStore } from '../../stores/app.store';
import { SidebarItem } from '../../components/ui/SidebarItem';
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator } from '../../components/ui/dropdown-menu';
import { navigationSections } from './navigation';

export function AppSidebar({ onSignOut, signingOut }: { onSignOut: () => void; signingOut: boolean }) {
    const { activeTab, currentUser, setActiveTab } = useAppStore();
    const { isDark, toggleTheme } = useTheme();
    const username = currentUser?.username ?? 'Account';
    return <aside className="desktop-sidebar">
        <div className="desktop-brand"><img src={rocketLogo} alt="" /><strong>RocketMQ-Rust</strong></div>
        <nav className="desktop-navigation" aria-label="Primary navigation">
            {navigationSections.map(section => <section key={section.title}>
                <h2>{section.title}</h2>
                {section.items.map(item => <SidebarItem key={item.tab} icon={item.icon} label={item.label}
                    active={activeTab === item.tab} onClick={() => setActiveTab(item.tab)} />)}
            </section>)}
        </nav>
        <div className="desktop-sidebar-footer">
            <button type="button" className="desktop-theme" onClick={toggleTheme} aria-label="Toggle theme">
                {isDark ? <Sun /> : <Moon />}<span>Theme</span><ChevronRight />
            </button>
            <DropdownMenu>
                <DropdownMenuTrigger asChild><button type="button" className="desktop-account" disabled={signingOut} aria-label="Open account menu">
                    <span className="desktop-avatar">{username.slice(0, 2).toUpperCase()}</span>
                    <span><strong>{username}</strong><small>Account</small></span><ChevronRight />
                </button></DropdownMenuTrigger>
                <DropdownMenuContent side="top" align="start" sideOffset={8} className="desktop-account-menu">
                    <DropdownMenuLabel>{username}</DropdownMenuLabel>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onSelect={() => setActiveTab('Account')}><UserRound />Account settings</DropdownMenuItem>
                    <DropdownMenuItem onSelect={() => setActiveTab('Sessions')}>Sessions</DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem variant="destructive" onSelect={onSignOut}><LogOut />Sign out</DropdownMenuItem>
                </DropdownMenuContent>
            </DropdownMenu>
        </div>
    </aside>;
}
