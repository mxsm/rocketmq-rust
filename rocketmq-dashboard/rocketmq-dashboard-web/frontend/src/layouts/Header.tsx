import { ChevronDown, LockKeyhole, Menu, RefreshCw, Server, UserCircle } from 'lucide-react';
import type { Ref } from 'react';
import { useCallback, useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { authApi } from '../api/auth_api';
import { configApi } from '../api/config_api';
import { Badge } from '../components/ui/Badge';
import { Button } from '../components/ui/Button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '../components/ui/DropdownMenu';

interface HeaderProps {
  menuButtonRef?: Ref<HTMLButtonElement>;
  onMenuOpen: () => void;
}

const pageLabels = [
  ['/messages/dlq', 'DLQ Message', 'Messaging'],
  ['/message-trace', 'Message Trace', 'Messaging'],
  ['/dashboard', 'Dashboard', 'Operate'],
  ['/brokers', 'Cluster', 'Messaging'],
  ['/topics', 'Topic', 'Messaging'],
  ['/consumers', 'Consumer', 'Messaging'],
  ['/producers', 'Producer', 'Messaging'],
  ['/messages', 'Message', 'Messaging'],
  ['/acl', 'ACL Management', 'Governance'],
  ['/monitors', 'Monitor', 'Governance'],
  ['/config', 'OPS', 'Operate'],
  ['/proxy', 'Proxy', 'Operate']
] as const;

export default function Header({ menuButtonRef, onMenuOpen }: HeaderProps) {
  const [namesrv, setNamesrv] = useState<string>('unconfigured');
  const [tls, setTls] = useState(false);
  const [sessionLabel, setSessionLabel] = useState('session open');
  const [loginRequired, setLoginRequired] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const page = pageLabels.find(([path]) => location.pathname === path || location.pathname.startsWith(`${path}/`));

  const loadConfig = useCallback(() => {
    setRefreshing(true);
    return configApi
      .getConfig()
      .then((config) => {
        const nameserver = config.endpoints.find((endpoint) => (
          endpoint.endpointType === 'nameserver' && endpoint.isEnabled && endpoint.isActive
        ));
        setNamesrv(nameserver?.address ?? 'unconfigured');
        setTls(config.useTLS);
      })
      .catch(() => {
        setNamesrv('unavailable');
      })
      .finally(() => setRefreshing(false));
  }, []);

  useEffect(() => {
    loadConfig();
    authApi
      .session()
      .then((session) => {
        if (!session.loginRequired) {
          setSessionLabel('auth off');
          setLoginRequired(false);
          setAuthenticated(true);
        } else {
          setSessionLabel(session.authenticated ? session.username ?? 'signed in' : 'signed out');
          setLoginRequired(true);
          setAuthenticated(session.authenticated);
        }
      })
      .catch(() => setSessionLabel('auth unknown'));
  }, [loadConfig]);

  useEffect(() => {
    window.addEventListener('rocketmq-config-updated', loadConfig);
    return () => window.removeEventListener('rocketmq-config-updated', loadConfig);
  }, [loadConfig]);

  const handleAuthAction = () => {
    if (!loginRequired || !authenticated) {
      navigate('/login');
      return;
    }
    authApi.logout().then(() => {
      setAuthenticated(false);
      setSessionLabel('signed out');
      navigate('/login');
    });
  };

  return (
    <header className="topbar">
      <div className="topbar-leading">
        <Button ref={menuButtonRef} variant="ghost" size="icon" className="topbar-menu" aria-label="Open navigation" onClick={onMenuOpen}>
          <Menu size={18} aria-hidden="true" />
        </Button>
        <div className="topbar-title">
          <span>{page?.[2] ?? 'RocketMQ'}</span>
          <strong>{page?.[1] ?? 'Operations'}</strong>
        </div>
      </div>
      <div className="topbar-status">
        <Badge tone={namesrv === 'unconfigured' || namesrv === 'unavailable' ? 'warning' : 'success'}>
          <Server size={12} aria-hidden="true" />
          <span className="topbar-status-label">NameServer</span> {namesrv}
        </Badge>
        <Badge tone={tls ? 'success' : 'neutral'}>
          <LockKeyhole size={12} aria-hidden="true" />
          {tls ? 'TLS on' : 'TLS off'}
        </Badge>
      </div>
      <div className="topbar-actions">
        <Button type="button" variant="ghost" size="icon" loading={refreshing} aria-label="Refresh configuration" onClick={loadConfig}>
          <RefreshCw size={16} aria-hidden="true" />
        </Button>
        {loginRequired && authenticated ? (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="topbar-user">
                <UserCircle size={16} aria-hidden="true" />
                {sessionLabel}
                <ChevronDown size={14} aria-hidden="true" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Dashboard session</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={handleAuthAction}>Sign out</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        ) : loginRequired ? (
          <Button type="button" variant="secondary" onClick={handleAuthAction}>Sign in</Button>
        ) : (
          <Badge tone="neutral"><UserCircle size={12} aria-hidden="true" />{sessionLabel}</Badge>
        )}
      </div>
    </header>
  );
}
