import { CheckCircle2, Plus, RefreshCw, Route, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { configApi } from '../api/config_api';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusBadge from '../components/StatusBadge';
import { Button } from '../components/ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../components/ui/Dialog';
import { Input } from '../components/ui/Input';
import { Label } from '../components/ui/Label';
import type { ConfigMutationResult, DashboardConfigView } from '../types/config';
import { getProxyEndpointLabel, isDuplicateProxyAddress, normalizeProxyAddress } from './proxy/proxy-model';

type NoticeTone = 'success' | 'warning' | 'danger';

export default function ProxyPage() {
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [proxyAddress, setProxyAddress] = useState('');
  const [addError, setAddError] = useState<string | null>(null);
  const mutationInFlight = useRef(false);
  const proxyAddressInputRef = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setConfig(await configApi.getConfig());
      setNotice(null);
    } catch (requestError) {
      setError(getErrorMessage(requestError));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (addDialogOpen && addError) proxyAddressInputRef.current?.focus();
  }, [addDialogOpen, addError]);

  const applyMutation = (result: ConfigMutationResult) => {
    setConfig(result.config);
    setNotice({ tone: 'success', message: result.message || 'Proxy configuration updated.' });
    window.dispatchEvent(new CustomEvent('rocketmq-config-updated'));
  };

  const runMutation = async (request: () => Promise<ConfigMutationResult>, onSuccess?: () => void, onError?: (message: string) => void) => {
    if (mutationInFlight.current) return;

    mutationInFlight.current = true;
    setPending(true);
    setNotice(null);
    try {
      applyMutation(await request());
      onSuccess?.();
    } catch (requestError) {
      const message = getErrorMessage(requestError);
      setNotice({ tone: 'danger', message });
      onError?.(message);
    } finally {
      mutationInFlight.current = false;
      setPending(false);
    }
  };

  const addProxy = () => {
    const address = normalizeProxyAddress(proxyAddress);
    if (!address) {
      setAddError('Enter a proxy endpoint.');
      proxyAddressInputRef.current?.focus();
      return;
    }
    if (isDuplicateProxyAddress(address, config?.proxyAddrList ?? [])) {
      setAddError('This proxy endpoint is already configured.');
      return;
    }

    void runMutation(
      () => configApi.addProxy({ address }),
      () => {
        setProxyAddress('');
        setAddError(null);
        setAddDialogOpen(false);
      },
      (message) => {
        setAddError(message);
        proxyAddressInputRef.current?.focus();
      }
    );
  };

  const switchProxy = (address: string) => {
    if (pending || getProxyEndpointLabel(address, config?.currentProxyAddr) === 'Current') return;
    void runMutation(() => configApi.switchProxy({ address }));
  };

  const deleteProxy = (address: string) => {
    if (pending) return;
    void runMutation(() => configApi.deleteProxy(address));
  };

  const openAddDialog = () => {
    setAddError(null);
    setAddDialogOpen(true);
  };

  if (loading) return <LoadingState label="Loading proxy endpoints" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  if (!config) return null;

  const proxies = config.proxyAddrList;
  const currentProxy = config.currentProxyAddr ?? null;

  return (
    <>
      <PageHeader
        title="Proxy"
        description="Manage configured proxy endpoints and select the current endpoint."
        actions={(
          <>
            <Button type="button" variant="secondary" onClick={() => void load()} disabled={pending} aria-label="Refresh proxy endpoints">
              <RefreshCw size={15} aria-hidden="true" /> Refresh
            </Button>
            <Button type="button" onClick={openAddDialog} disabled={pending}>
              <Plus size={15} aria-hidden="true" /> Add endpoint
            </Button>
          </>
        )}
      />

      {notice ? <div className={`notice notice-${notice.tone}`} role={notice.tone === 'danger' ? 'alert' : 'status'}>{notice.message}</div> : null}

      <section className="proxy-summary" aria-label="Proxy endpoint summary">
        <div className="panel proxy-summary-card">
          <span>Total endpoints</span>
          <strong>{proxies.length}</strong>
        </div>
        <div className="panel proxy-summary-card">
          <span>Current endpoint</span>
          {currentProxy ? <StatusBadge status={currentProxy} tone="success" /> : <strong className="proxy-summary-empty">No current endpoint</strong>}
        </div>
      </section>

      <section className="panel proxy-endpoint-panel" aria-labelledby="proxy-endpoints-heading">
        <div className="proxy-table-toolbar">
          <div>
            <h2 id="proxy-endpoints-heading">Proxy endpoints</h2>
            <p>Select one endpoint as current or remove an endpoint that is no longer needed.</p>
          </div>
          <StatusBadge status={`${proxies.length} configured`} tone={proxies.length > 0 ? 'success' : 'neutral'} />
        </div>

        {proxies.length > 0 ? (
          <div className="proxy-table-scroll">
            <table className="proxy-endpoint-table">
              <thead>
                <tr>
                  <th scope="col">Endpoint</th>
                  <th scope="col">Status</th>
                  <th scope="col" className="proxy-actions-header">Actions</th>
                </tr>
              </thead>
              <tbody>
                {proxies.map((address) => {
                  const label = getProxyEndpointLabel(address, currentProxy);
                  const isCurrent = label === 'Current';
                  return (
                    <tr className={isCurrent ? 'proxy-endpoint-current' : undefined} key={address}>
                      <td><code>{address}</code></td>
                      <td><StatusBadge status={label} tone={isCurrent ? 'success' : 'neutral'} /></td>
                      <td className="proxy-actions">
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          onClick={() => switchProxy(address)}
                          disabled={pending || isCurrent}
                          aria-label={isCurrent ? `Current proxy ${address}` : `Set current proxy ${address}`}
                        >
                          {isCurrent ? <CheckCircle2 size={14} aria-hidden="true" /> : <Route size={14} aria-hidden="true" />}
                          {isCurrent ? 'Current' : 'Set current'}
                        </Button>
                        <ConfirmDialog
                          title="Delete proxy"
                          description={`Delete proxy ${address}? This removes the endpoint from the configured list.`}
                          confirmLabel="Delete proxy"
                          onConfirm={() => deleteProxy(address)}
                        >
                          <Button type="button" variant="destructive" size="sm" disabled={pending} aria-label={`Delete proxy ${address}`}>
                            <Trash2 size={14} aria-hidden="true" /> Delete
                          </Button>
                        </ConfirmDialog>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="No proxy endpoints" detail="Add an endpoint to begin managing proxy configuration." />
        )}
      </section>

      <Dialog open={addDialogOpen} onOpenChange={(open) => !pending && setAddDialogOpen(open)}>
        <DialogContent aria-label="Add proxy endpoint">
          <DialogHeader>
            <DialogTitle>Add proxy endpoint</DialogTitle>
            <DialogDescription>Add an endpoint to the configured proxy list.</DialogDescription>
          </DialogHeader>
          <form onSubmit={(event) => { event.preventDefault(); addProxy(); }}>
            <Label className="proxy-dialog-field" htmlFor="proxy-address">Proxy address</Label>
            <Input
              id="proxy-address"
              ref={proxyAddressInputRef}
              value={proxyAddress}
              placeholder="127.0.0.1:8080"
              onChange={(event) => { setProxyAddress(event.target.value); setAddError(null); }}
              disabled={pending}
            />
            {addError ? <div className="inline-validation" role="alert">{addError}</div> : null}
            <DialogFooter>
              <Button type="button" variant="secondary" onClick={() => setAddDialogOpen(false)} disabled={pending}>Cancel</Button>
              <Button type="submit" disabled={pending}>Add proxy endpoint</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}

function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Unable to update proxy configuration.';
}
