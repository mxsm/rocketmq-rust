import { Plus, RefreshCw, Save, Trash2 } from 'lucide-react';
import { useEffect, useState } from 'react';
import { configApi } from '../api/config_api';
import ConfirmDialog from '../components/ConfirmDialog';
import DataTable from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusBadge from '../components/StatusBadge';
import type { DashboardConfigView } from '../types/config';

interface AddressRow {
  address: string;
}

export default function ConfigPage() {
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [operationMessage, setOperationMessage] = useState<string | null>(null);
  const [nameserver, setNameserver] = useState('');
  const [proxy, setProxy] = useState('');

  const load = () => {
    setLoading(true);
    setError(null);
    configApi
      .getConfig()
      .then(setConfig)
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const applyConfig = (nextConfig: DashboardConfigView, message?: string) => {
    setConfig(nextConfig);
    setOperationError(null);
    setOperationMessage(message ?? 'Configuration updated');
    window.dispatchEvent(new CustomEvent('rocketmq-config-updated'));
  };

  const runMutation = (mutation: Promise<{ message: string; config: DashboardConfigView }>, onSuccess?: () => void) =>
    mutation
      .then((result) => {
        applyConfig(result.config, result.message);
        onSuccess?.();
      })
      .catch((requestError: Error) => {
        setOperationMessage(null);
        setOperationError(requestError.message);
      });

  if (loading) return <LoadingState label="Loading config" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  if (!config) return null;

  const namesrvRows: AddressRow[] = config.namesrvAddrList.map((address) => ({ address }));
  const proxyRows: AddressRow[] = config.proxyAddrList.map((address) => ({ address }));
  const switchNameserver = (address: string) =>
    runMutation(configApi.replaceNameservers({ namesrvAddrList: config.namesrvAddrList, currentNamesrv: address }));
  const deleteNameserver = (address: string) => {
    const nextNameservers = config.namesrvAddrList.filter((item) => item !== address);
    const nextCurrentNamesrv = config.currentNamesrv === address ? nextNameservers[0] ?? null : config.currentNamesrv ?? null;
    return runMutation(configApi.replaceNameservers({ namesrvAddrList: nextNameservers, currentNamesrv: nextCurrentNamesrv }));
  };

  return (
    <>
      <PageHeader
        title="Operations Config"
        description="NameServer, VIP channel, TLS, Proxy, and persistence controls mapped to the Rust Web backend."
        actions={
          <>
            <StatusBadge status={`storage ${String(config.storageBackend).toLowerCase()}`} />
            <button type="button" className="icon-button" title="Reload config" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" />
            </button>
          </>
        }
      />
      {operationMessage ? <div className="notice notice-success">{operationMessage}</div> : null}
      {operationError ? <div className="notice notice-danger">{operationError}</div> : null}
      <div className="ops-grid">
        <section className="panel ops-main">
          <div className="panel-heading">
            <div>
              <h2>NameServer Address List</h2>
              <p>Equivalent to Java OPS NameServerAddressList, backed by <code>PUT /api/config/nameservers</code>.</p>
            </div>
            <StatusBadge status={config.currentNamesrv ?? 'unconfigured'} tone={config.currentNamesrv ? 'success' : 'warning'} />
          </div>
          <div className="action-row">
            <input
              className="inline-input inline-input-wide"
              value={nameserver}
              placeholder="127.0.0.1:9876"
              onChange={(event) => setNameserver(event.target.value)}
            />
            <button
              type="button"
              className="button"
              onClick={() => runMutation(configApi.addNameserver({ address: nameserver }), () => setNameserver(''))}
            >
              <Plus size={15} aria-hidden="true" /> Add
            </button>
          </div>
        </section>
        <section className="panel ops-side">
          <div className="panel-heading">
            <div>
              <h2>Runtime Flags</h2>
              <p>Persisted switches used by the Rust admin facade.</p>
            </div>
          </div>
          <div className="flag-row">
            <span>VIP Channel</span>
            <button
              type="button"
              className={config.useVIPChannel ? 'switch-button checked' : 'switch-button'}
              aria-pressed={config.useVIPChannel}
              onClick={() => runMutation(configApi.setVipChannel({ enabled: !config.useVIPChannel }))}
            >
              <span />
            </button>
            <button type="button" className="button button-secondary" onClick={() => runMutation(configApi.setVipChannel({ enabled: !config.useVIPChannel }))}>
              <Save size={15} aria-hidden="true" /> Update
            </button>
          </div>
          <div className="flag-row">
            <span>TLS</span>
            <button
              type="button"
              className={config.useTLS ? 'switch-button checked' : 'switch-button'}
              aria-pressed={config.useTLS}
              onClick={() => runMutation(configApi.setTls({ enabled: !config.useTLS }))}
            >
              <span />
            </button>
            <button type="button" className="button button-secondary" onClick={() => runMutation(configApi.setTls({ enabled: !config.useTLS }))}>
              <Save size={15} aria-hidden="true" /> Update
            </button>
          </div>
        </section>
      </div>
      <DataTable
        rows={namesrvRows}
        columns={[
          { header: 'NameServer', render: (row) => <code>{row.address}</code> },
          { header: 'Current', render: (row) => (row.address === config.currentNamesrv ? <StatusBadge status="current" tone="success" /> : <StatusBadge status="standby" />) },
          {
            header: 'Actions',
            render: (row) => (
              <div className="action-row">
                <button
                  type="button"
                  className="button button-secondary"
                  disabled={row.address === config.currentNamesrv}
                  onClick={() => switchNameserver(row.address)}
                >
                  Switch
                </button>
                <ConfirmDialog
                  title="Delete NameServer"
                  description={`Delete NameServer ${row.address}?`}
                  confirmLabel="Delete"
                  onConfirm={() => deleteNameserver(row.address)}
                >
                  <button type="button" className="icon-button" title="Delete NameServer">
                    <Trash2 size={15} aria-hidden="true" />
                  </button>
                </ConfirmDialog>
              </div>
            )
          }
        ]}
        getRowId={(row) => row.address}
        emptyTitle="No NameServers"
      />
      <section className="panel" id="proxy">
        <div className="panel-heading">
          <div>
            <h2>Proxy Endpoints</h2>
            <p>Rust endpoints: <code>POST /api/config/proxies</code>, <code>PUT /api/config/proxies/current</code>, and delete by address.</p>
          </div>
          <StatusBadge status={config.currentProxyAddr ?? 'no proxy'} tone={config.currentProxyAddr ? 'success' : 'neutral'} />
        </div>
        <div className="action-row">
          <input className="inline-input inline-input-wide" value={proxy} placeholder="127.0.0.1:8080" onChange={(event) => setProxy(event.target.value)} />
          <button
            type="button"
            className="button"
            onClick={() => runMutation(configApi.addProxy({ address: proxy }), () => setProxy(''))}
          >
            <Plus size={15} aria-hidden="true" /> Add Proxy
          </button>
        </div>
      </section>
      <DataTable
        rows={proxyRows}
        columns={[
          { header: 'Proxy', render: (row) => <code>{row.address}</code> },
          { header: 'Current', render: (row) => (row.address === config.currentProxyAddr ? <StatusBadge status="current" tone="success" /> : null) },
          {
            header: 'Actions',
            render: (row) => (
              <div className="action-row">
                <button
                  type="button"
                  className="button button-secondary"
                  disabled={row.address === config.currentProxyAddr}
                  onClick={() => runMutation(configApi.switchProxy({ address: row.address }))}
                >
                  Switch
                </button>
                <ConfirmDialog
                  title="Delete proxy"
                  description={`Delete proxy ${row.address}?`}
                  confirmLabel="Delete"
                  onConfirm={() => runMutation(configApi.deleteProxy(row.address))}
                >
                  <button type="button" className="icon-button" title="Delete proxy">
                    <Trash2 size={15} aria-hidden="true" />
                  </button>
                </ConfirmDialog>
              </div>
            )
          }
        ]}
        getRowId={(row) => row.address}
        emptyTitle="No proxies"
      />
    </>
  );
}
