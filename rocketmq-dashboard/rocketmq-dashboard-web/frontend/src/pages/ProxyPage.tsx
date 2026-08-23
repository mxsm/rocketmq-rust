import { CheckCircle2, Plus, RefreshCw, Route, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { configApi } from '../api/config_api';
import { ApiClientError } from '../api/client';
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
import type { ConfigMutationResult, DashboardConfigView, EndpointView } from '../types/config';
import { getProxyEndpointLabel, isDuplicateProxyAddress, normalizeProxyAddress } from './proxy/proxy-model';

type NoticeTone = 'success' | 'warning' | 'danger';
type ProxyRetryMutation = { kind: 'switch' | 'delete'; endpointId: string };

export default function ProxyPage() {
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [proxyAddress, setProxyAddress] = useState('');
  const [addError, setAddError] = useState<string | null>(null);
  const [addConflictReady, setAddConflictReady] = useState(false);
  const [retryProxyMutation, setRetryProxyMutation] = useState<ProxyRetryMutation | null>(null);
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
    setAddConflictReady(false);
    setRetryProxyMutation(null);
    setNotice({ tone: 'success', message: result.message || 'Proxy configuration updated.' });
    window.dispatchEvent(new CustomEvent('rocketmq-config-updated'));
  };

  const runMutation = async (
    request: () => Promise<ConfigMutationResult>,
    onSuccess?: () => void,
    onError?: (message: string) => void,
    onConflict?: (error: ApiClientError) => Promise<void>
  ) => {
    if (mutationInFlight.current) return;

    mutationInFlight.current = true;
    setPending(true);
    setNotice(null);
    try {
      applyMutation(await request());
      onSuccess?.();
    } catch (requestError) {
      if (requestError instanceof ApiClientError && requestError.code === 'STORAGE_CONFLICT' && onConflict) {
        try {
          await onConflict(requestError);
          return;
        } catch (refreshError) {
          const message = `${requestError.message} Your draft is preserved; refresh before retrying.`;
          setNotice({ tone: 'danger', message });
          onError?.(message);
          return;
        }
      }
      const message = getErrorMessage(requestError);
      setNotice({ tone: 'danger', message });
      onError?.(message);
    } finally {
      mutationInFlight.current = false;
      setPending(false);
    }
  };

  const addProxy = () => {
    if (!config) return;
    const address = normalizeProxyAddress(proxyAddress);
    if (!address) {
      setAddError('Enter a proxy endpoint.');
      proxyAddressInputRef.current?.focus();
      return;
    }
    if (isDuplicateProxyAddress(address, config.proxyAddrList)) {
      setAddError('This proxy endpoint is already configured.');
      return;
    }

    void runMutation(
      () => configApi.addProxy({ address, expectedRevision: config.revision }),
      () => {
        setProxyAddress('');
        setAddError(null);
        setAddConflictReady(false);
        setAddDialogOpen(false);
      },
      (message) => {
        setAddError(message);
        proxyAddressInputRef.current?.focus();
      },
      async (conflict) => {
        const authoritative = await configApi.getConfig();
        setConfig(authoritative);
        setAddConflictReady(true);
        const message = `${conflict.message} The latest configuration revision is loaded and your proxy address is preserved. Review and retry add.`;
        setNotice({ tone: 'warning', message });
        setAddError(message);
        proxyAddressInputRef.current?.focus();
      }
    );
  };

  const refreshProxyConflict = async (conflict: ApiClientError, retry: ProxyRetryMutation) => {
    const authoritative = await configApi.getConfig();
    setConfig(authoritative);
    const endpoint = authoritative.endpoints.find((item) => item.endpointId === retry.endpointId);
    if (!endpoint) {
      setRetryProxyMutation(null);
      setNotice({
        tone: 'warning',
        message: `${conflict.message} The endpoint no longer exists in the latest configuration. Refresh before taking another action.`
      });
      return;
    }
    if (retry.kind === 'switch' && endpoint.isActive) {
      setRetryProxyMutation(null);
      setNotice({
        tone: 'success',
        message: `${conflict.message} The latest configuration already selects this proxy endpoint.`
      });
      return;
    }
    setRetryProxyMutation(retry);
    setNotice({
      tone: 'warning',
      message: `${conflict.message} The latest configuration revision is loaded. Review and explicitly retry ${retry.kind === 'switch' ? 'the switch' : 'the delete'}.`
    });
  };

  const switchProxy = (endpoint: EndpointView) => {
    if (!config || pending || endpoint.isActive) return;
    void runMutation(
      () => configApi.switchProxy({ endpointId: endpoint.endpointId, expectedRevision: config.revision }),
      undefined,
      undefined,
      (conflict) => refreshProxyConflict(conflict, { kind: 'switch', endpointId: endpoint.endpointId })
    );
  };

  const deleteProxy = (endpoint: EndpointView) => {
    if (!config || pending) return;
    void runMutation(
      () => configApi.deleteProxy(endpoint.endpointId, config.revision),
      undefined,
      undefined,
      (conflict) => refreshProxyConflict(conflict, { kind: 'delete', endpointId: endpoint.endpointId })
    );
  };

  const openAddDialog = () => {
    setAddError(null);
    setAddConflictReady(false);
    setAddDialogOpen(true);
  };

  if (loading) return <LoadingState label="Loading proxy endpoints" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  if (!config) return null;

  const proxies = config.endpoints.filter((endpoint) => endpoint.endpointType === 'proxy');
  const currentProxy = proxies.find((endpoint) => endpoint.isActive)?.address ?? null;

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
                {proxies.map((endpoint) => {
                  const { address } = endpoint;
                  const label = getProxyEndpointLabel(address, currentProxy);
                  const isCurrent = endpoint.isActive;
                  const retryingSwitch = retryProxyMutation?.kind === 'switch' && retryProxyMutation.endpointId === endpoint.endpointId;
                  const retryingDelete = retryProxyMutation?.kind === 'delete' && retryProxyMutation.endpointId === endpoint.endpointId;
                  return (
                    <tr className={isCurrent ? 'proxy-endpoint-current' : undefined} key={endpoint.endpointId}>
                      <td><code>{address}</code></td>
                      <td><StatusBadge status={label} tone={isCurrent ? 'success' : 'neutral'} /></td>
                      <td className="proxy-actions">
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          onClick={() => switchProxy(endpoint)}
                          disabled={pending || isCurrent}
                          aria-label={isCurrent ? `Current proxy ${address}` : `Set current proxy ${address}`}
                        >
                          {isCurrent ? <CheckCircle2 size={14} aria-hidden="true" /> : <Route size={14} aria-hidden="true" />}
                          {isCurrent ? 'Current' : retryingSwitch ? 'Retry set current' : 'Set current'}
                        </Button>
                        <ConfirmDialog
                          title="Delete proxy"
                          description={retryingDelete
                            ? `Delete proxy ${address} using the refreshed configuration revision?`
                            : `Delete proxy ${address}? This removes the endpoint from the configured list.`}
                          confirmLabel={retryingDelete ? 'Retry delete proxy' : 'Delete proxy'}
                          onConfirm={() => deleteProxy(endpoint)}
                        >
                          <Button type="button" variant="destructive" size="sm" disabled={pending} aria-label={`Delete proxy ${address}`}>
                            <Trash2 size={14} aria-hidden="true" /> {retryingDelete ? 'Retry delete' : 'Delete'}
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
              onChange={(event) => {
                setProxyAddress(event.target.value);
                setAddError(null);
              }}
              disabled={pending}
            />
            {addError ? <div className="inline-validation" role="alert">{addError}</div> : null}
            <DialogFooter>
              <Button type="button" variant="secondary" onClick={() => setAddDialogOpen(false)} disabled={pending}>Cancel</Button>
              <Button type="submit" disabled={pending}>
                {addConflictReady ? 'Retry add proxy endpoint' : 'Add proxy endpoint'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}

function getErrorMessage(error: unknown) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
    return `${error.message} Your draft is still preserved; refresh before retrying.`;
  }
  return error instanceof Error ? error.message : 'Unable to update proxy configuration.';
}
