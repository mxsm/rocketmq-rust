import {
  Activity,
  Bell,
  Database,
  Gauge,
  KeyRound,
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
      { to: '/messages/dlq', label: 'DLQMessage', icon: Siren },
      { to: '/message-trace', label: 'MessageTrace', icon: TimerReset }
    ]
  },
  {
    label: 'Governance',
    items: [
      { to: '/acl', label: 'ACL Management', icon: ShieldCheck },
      { to: '/monitors', label: 'Monitor', icon: Bell },
      { to: '/login', label: 'Login', icon: KeyRound }
    ]
  }
];

export default function Sidebar() {
  const location = useLocation();

  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark">
          <Activity size={18} aria-hidden="true" />
        </div>
        <div>
          <strong>RocketMQ</strong>
          <span>Operations</span>
        </div>
      </div>
      <nav>
        {navGroups.map((group) => (
          <div key={group.label} className="nav-group">
            <span className="nav-group-label">{group.label}</span>
            {group.items.map((item) => {
              const Icon = item.icon;
              const isItemActive = item.match
                ? item.match(location.pathname, location.hash)
                : location.pathname === item.to || location.pathname.startsWith(`${item.to}/`);

              return (
                <NavLink
                  key={`${group.label}-${item.label}`}
                  to={item.to}
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
