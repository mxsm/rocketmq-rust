import {
  Activity,
  Bell,
  Database,
  Gauge,
  MailSearch,
  Network,
  RadioTower,
  Send,
  Settings,
  ShieldCheck,
  Siren,
  Split,
  TimerReset,
  Users
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { NavLink } from 'react-router-dom';
import { useLocation } from 'react-router-dom';
import { cn } from '../lib/cn';

interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  match?: (pathname: string, hash: string) => boolean;
}

interface NavGroup {
  label: string;
  items: NavItem[];
}

const navGroups: NavGroup[] = [
  {
    label: 'Operate',
    items: [
      { to: '/config', label: 'OPS', icon: Settings, match: (pathname, hash) => pathname === '/config' && hash !== '#proxy' },
      { to: '/proxy', label: 'Proxy', icon: Split },
      { to: '/dashboard', label: 'Dashboard', icon: Gauge }
    ]
  },
  {
    label: 'Messaging',
    items: [
      { to: '/brokers', label: 'Cluster', icon: RadioTower },
      { to: '/topics', label: 'Topic', icon: Database },
      { to: '/consumers', label: 'Consumer', icon: Users },
      { to: '/producers', label: 'Producer', icon: Send },
      { to: '/messages', label: 'Message', icon: MailSearch },
      { to: '/messages/dlq', label: 'DLQ Message', icon: Siren },
      { to: '/message-trace', label: 'Message Trace', icon: TimerReset }
    ]
  },
  {
    label: 'Governance',
    items: [
      { to: '/acl', label: 'ACL Management', icon: ShieldCheck },
      { to: '/monitors', label: 'Monitor', icon: Bell }
    ]
  }
];

interface SidebarProps {
  className?: string;
  onNavigate?: () => void;
}

export default function Sidebar({ className, onNavigate }: SidebarProps) {
  const location = useLocation();

  return (
    <aside className={cn('sidebar', className)}>
      <div className="brand">
        <div className="brand-mark">
          <Activity size={18} aria-hidden="true" />
        </div>
        <div>
          <strong>RocketMQ</strong>
          <span>Operations</span>
        </div>
      </div>
      <nav aria-label="Primary navigation">
        {navGroups.map((group) => (
          <div key={group.label} className="nav-group">
            <span className="nav-group-label">{group.label}</span>
            {group.items.map((item) => {
              const Icon = item.icon;
              const isItemActive = item.match
                ? item.match(location.pathname, location.hash)
                : location.pathname === item.to || (item.to !== '/messages' && location.pathname.startsWith(`${item.to}/`));

              return (
                <NavLink
                  key={`${group.label}-${item.label}`}
                  to={item.to}
                  end={item.to === '/messages'}
                  onClick={onNavigate}
                  className={() => (isItemActive ? 'nav-link active' : 'nav-link')}
                >
                  <Icon size={17} aria-hidden="true" />
                  <span>{item.label}</span>
                </NavLink>
              );
            })}
          </div>
        ))}
      </nav>
      <div className="sidebar-footer">
        <Network size={16} aria-hidden="true" />
        <span>Web Dashboard</span>
      </div>
    </aside>
  );
}
