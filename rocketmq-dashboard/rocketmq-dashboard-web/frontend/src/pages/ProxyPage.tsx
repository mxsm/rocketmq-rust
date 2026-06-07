import { CheckCircle2, Copy, Database, Plus, RefreshCw, Route, Server, Trash2, Zap } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { configApi } from '../api/config_api';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SelectMenu, { type SelectMenuOption } from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { ConfigMutationResult, DashboardConfigView } from '../types/config';

type NoticeTone = 'success' | 'warning' | 'danger';

export default function ProxyPage() {
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);
  const [proxyAddress, setProxyAddress] = useState('');

  const load = () => {
    setLoading(true);
    setError(null);
    configApi
      .getConfig()
      .then((data) => {
        setConfig(data);
        setNotice(null);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const proxyOptions = useMemo<SelectMenuOption[]>(() => {
    const proxies = config?.proxyAddrList ?? [];
    if (proxies.length === 0) {
      return [{ value: '', label: 'No proxy configured', disabled: true }];
    }
    return proxies.map((address) => ({ value: address, label: address }));
  }, [config]);

  const currentProxy = config?.currentProxyAddr ?? config?.proxyAddrList[0] ?? '';
  const storageBackend = String(config?.storageBackend ?? 'file').toLowerCase();

  const applyMutation = (result: ConfigMutationResult) => {
    setConfig(result.config);
    setNotice({ tone: 'success', message: result.message || 'Proxy configuration updated.' });
    if (result.config.currentProxyAddr) {
      localStorage.setItem('proxyAddr', result.config.currentProxyAddr);
    }
    window.dispatchEvent(new CustomEvent('rocketmq-config-updated'));
  };

  const runMutation = (mutation: Promise<ConfigMutationResult>, onSuccess?: () => void) => {
    setPending(true);
    setNotice(null);
    mutation
      .then((result) => {
        applyMutation(result);
        onSuccess?.();
      })
      .catch((requestError: Error) => {
        setNotice({ tone: 'danger', message: requestError.message });
      })
      .finally(() => setPending(false));
  };

  const addProxy = () => {
    const address = proxyAddress.trim();
    if (!address) {
      setNotice({ tone: 'warning', message: 'Input proxy address required.' });
      return;
    }
    runMutation(configApi.addProxy({ address }), () => setProxyAddress(''));
  };

  const switchProxy = (address: string) => {
    if (!address || address === config?.currentProxyAddr) {
      return;
    }
    runMutation(configApi.switchProxy({ address }));
  };

  const deleteProxy = (address: string) => {
    runMutation(configApi.deleteProxy(address));
  };

  const copyProxy = (address: string) => {
    copyText(address);
    setNotice({ tone: 'success', message: `Copied proxy ${address}.` });
  };

  if (loading) return <LoadingState label="Loading proxy configuration" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  if (!config) return null;

  const proxies = config.proxyAddrList;

  return (
    <>
      <PageHeader
        title="Proxy"
        description="ProxyServerAddressList parity with Java Dashboard, redesigned for dense RocketMQ operations."
        actions={
          <>
            <StatusBadge status={currentProxy || 'no proxy'} tone={currentProxy ? 'success' : 'neutral'} />
            <button type="button" className="icon-button" title="Reload proxy" onClick={load} disabled={pending}>
              <RefreshCw className={pending ? 'spin' : undefined} size={15} aria-hidden="true" />
            </button>
          </>
        }
      />

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}

      <section className="proxy-hero-grid">
        <div className="panel proxy-selector-panel">
          <div className="panel-heading">
            <div>
              <h2>ProxyServerAddressList</h2>
              <p>Select the active proxy endpoint used by pages that support proxy context.</p>
            </div>
            <StatusBadge status={`${proxies.length} endpoint${proxies.length === 1 ? '' : 's'}`} tone={proxies.length > 0 ? 'success' : 'neutral'} />
          </div>
          <div className="proxy-select-row">
            <label className="proxy-field">
              Current Proxy
              <SelectMenu
                value={currentProxy}
                options={proxyOptions}
                onChange={switchProxy}
                ariaLabel="Select current proxy"
                className="proxy-select-menu"
              />
            </label>
            <button type="button" className="button button-secondary" onClick={load} disabled={pending}>
              <RefreshCw className={pending ? 'spin' : undefined} size={15} aria-hidden="true" />
              Refresh
            </button>
          </div>
        </div>

        <div className="panel proxy-add-panel">
          <div className="panel-heading">
            <div>
              <h2>Add ProxyAddr</h2>
              <p>Persist a new proxy endpoint to the Rust Web config store.</p>
            </div>
          </div>
          <div className="proxy-add-row">
            <label className="proxy-field">
              ProxyAddr
              <input
                value={proxyAddress}
                placeholder="127.0.0.1:8080"
                onChange={(event) => setProxyAddress(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    addProxy();
                  }
                }}
              />
            </label>
            <button type="button" className="button proxy-add-button" onClick={addProxy} disabled={pending}>
              <Plus size={15} aria-hidden="true" />
              Add
            </button>
          </div>
        </div>

        <div className="panel proxy-status-panel">
          <div className="proxy-status-icon">
            <Zap size={20} aria-hidden="true" />
          </div>
          <span>Status</span>
          <strong>{currentProxy ? 'Enabled' : 'Disabled'}</strong>
          <small>{currentProxy || 'Add a proxy endpoint to enable proxy context.'}</small>
        </div>
      </section>

      <section className="panel proxy-endpoint-panel">
        <div className="proxy-table-toolbar">
          <div>
            <h2>Proxy endpoints</h2>
            <p>Current endpoint, standby endpoints, switch action, and guarded delete.</p>
          </div>
          <StatusBadge status={`storage ${storageBackend}`} />
        </div>

        {proxies.length > 0 ? (
          <>
            <div className="proxy-endpoint-table">
              <div className="proxy-endpoint-row proxy-endpoint-head">
                <span>Proxy address</span>
                <span>Role</span>
                <span>Persistence</span>
                <span>Operation</span>
              </div>
              {proxies.map((address) => {
                const isCurrent = address === config.currentProxyAddr;
                return (
                  <div className={isCurrent ? 'proxy-endpoint-row proxy-endpoint-current' : 'proxy-endpoint-row'} key={address}>
                    <code>{address}</code>
                    <StatusBadge status={isCurrent ? 'current' : 'standby'} tone={isCurrent ? 'success' : 'neutral'} />
                    <span className="proxy-storage">
                      <Database size={14} aria-hidden="true" />
                      {storageBackend}
                    </span>
                    <div className="proxy-actions">
                      <button type="button" className="button button-secondary" onClick={() => switchProxy(address)} disabled={pending || isCurrent}>
                        {isCurrent ? <CheckCircle2 size={14} aria-hidden="true" /> : <Route size={14} aria-hidden="true" />}
                        {isCurrent ? 'Current' : 'Switch'}
                      </button>
                      <button type="button" className="icon-button" title="Copy proxy" onClick={() => copyProxy(address)}>
                        <Copy size={15} aria-hidden="true" />
                      </button>
                      <ConfirmDialog
                        title="Delete proxy"
                        description={`Delete proxy ${address}? This removes it from ProxyServerAddressList.`}
                        confirmLabel="Delete"
                        onConfirm={() => deleteProxy(address)}
                      >
                        <button type="button" className="icon-button icon-button-danger" title="Delete proxy" disabled={pending}>
                          <Trash2 size={15} aria-hidden="true" />
                        </button>
                      </ConfirmDialog>
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="proxy-table-footer">
              <span>{proxies.length} rows / Java compatible proxy home data</span>
              <span>
                <Server size={14} aria-hidden="true" />
                {currentProxy || 'no current proxy'}
              </span>
            </div>
          </>
        ) : (
          <EmptyState title="No proxy endpoints" detail="Add a ProxyAddr to enable proxy-aware Consumer operations." />
        )}
      </section>
    </>
  );
}

function copyText(value: string) {
  if (!value) return;
  if (navigator.clipboard?.writeText) {
    void navigator.clipboard.writeText(value).catch(() => legacyCopyText(value));
    return;
  }
  legacyCopyText(value);
}

function legacyCopyText(value: string) {
  const textarea = document.createElement('textarea');
  textarea.value = value;
  textarea.setAttribute('readonly', 'true');
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  textarea.remove();
}
