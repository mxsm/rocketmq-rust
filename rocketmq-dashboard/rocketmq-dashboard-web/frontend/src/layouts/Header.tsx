import { Moon, RefreshCw, Sun } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { authApi } from '../api/auth_api';
import { configApi } from '../api/config_api';
import StatusBadge from '../components/StatusBadge';

interface HeaderProps {
  theme: 'light' | 'dark';
  onThemeToggle: () => void;
}

export default function Header({ theme, onThemeToggle }: HeaderProps) {
  const [namesrv, setNamesrv] = useState<string>('unconfigured');
  const [tls, setTls] = useState(false);
  const [sessionLabel, setSessionLabel] = useState('session open');
  const [loginRequired, setLoginRequired] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const navigate = useNavigate();

  const loadConfig = useCallback(() => {
    configApi
      .getConfig()
      .then((config) => {
        setNamesrv(config.currentNamesrv ?? 'unconfigured');
        setTls(config.useTLS);
      })
      .catch(() => {
        setNamesrv('unavailable');
      });
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
      <div className="topbar-title">
        <strong>RocketMQ Web</strong>
        <span>Rust Axum API</span>
      </div>
      <div className="topbar-status">
        <StatusBadge status="cluster default" tone="neutral" />
        <StatusBadge status={namesrv} tone={namesrv === 'unconfigured' ? 'warning' : 'success'} />
        <StatusBadge status={tls ? 'TLS on' : 'TLS off'} tone={tls ? 'success' : 'neutral'} />
        <StatusBadge status={sessionLabel} tone={sessionLabel === 'signed out' ? 'warning' : 'neutral'} />
      </div>
      <div className="topbar-actions">
        <button type="button" className="icon-button" title="Refresh config" onClick={loadConfig}>
          <RefreshCw size={16} aria-hidden="true" />
        </button>
        {loginRequired ? (
          <button type="button" className="button button-secondary" onClick={handleAuthAction}>
            {authenticated ? 'Sign out' : 'Sign in'}
          </button>
        ) : null}
        <button type="button" className="icon-button" title="Toggle theme" onClick={onThemeToggle}>
          {theme === 'dark' ? <Sun size={16} aria-hidden="true" /> : <Moon size={16} aria-hidden="true" />}
        </button>
      </div>
    </header>
  );
}
