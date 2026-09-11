import { useCallback, useMemo, useRef, useState } from 'react';
import { ArrowRight, CheckCircle2, Info, Plus, Trash2 } from 'lucide-react';
import { useProxyCatalog } from '../features/proxy/hooks/useProxyCatalog';
import { describeProxyChange } from '../features/proxy/proxyController';
import { resolveConsumerScope } from '../features/consumer/scope';
import { useAppStore } from '../stores/app.store';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/dialog';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { StatusBadge } from './layout/StatusBadge';
import '../features/proxy/proxy.css';

function ProxyAddress({ address }: { address: string }) {
    return <div className="ops-proxy-address"><span>{address}</span>{address.length > 60 &&
        <details><summary>View full address</summary><code>{address}</code></details>}</div>;
}

export const ProxyView = () => {
    const { settings, refreshing, refreshedAt, pendingChange, needsReview, loadError, changeError,
        failedChange, receipt, refresh, submit, dismissChangeError } = useProxyCatalog();
    const { consumerQueryMode, setConsumerQueryMode, setActiveTab } = useAppStore();
    const [adding, setAdding] = useState(false);
    const [newAddress, setNewAddress] = useState('');
    const [addressError, setAddressError] = useState('');
    const [deleting, setDeleting] = useState<string | null>(null);
    const addTrigger = useRef<HTMLButtonElement>(null);
    const addressInput = useRef<HTMLInputElement>(null);
    const deleteTrigger = useRef<HTMLButtonElement | null>(null);
    const busy = pendingChange !== null;
    const blocked = busy || needsReview || !settings;
    const proxies = settings?.proxy.proxyAddrList ?? [];
    const current = settings?.proxy.currentProxyAddr ?? null;
    const proxyScope = resolveConsumerScope(settings, 'proxy');
    const scope = resolveConsumerScope(settings, consumerQueryMode);
    const fallback = proxies.find(address => address !== deleting);
    const openAdd = useCallback(() => { dismissChangeError(); setAddressError(''); setAdding(true); }, [dismissChangeError]);
    const actions = useMemo(() => <Button ref={addTrigger} icon={Plus} onClick={openAdd} disabled={busy || !settings}>Add Proxy</Button>, [busy, settings, openAdd]);
    usePageRefresh({ refresh, pending: refreshing || busy, refreshedAt, actions });

    const reviewNotice = needsReview && <PageState kind="stale" title="Review changed connection settings"
        description="Your draft is preserved. Reload the current configuration, review it, then submit the change again."
        action={<Button variant="outline" onClick={refresh} disabled={refreshing || busy}>{refreshing ? 'Reloading…' : 'Reload settings'}</Button>} />;
    const actionError = changeError && <PageState kind="error"
        title={failedChange && !adding && deleting === null ? describeProxyChange(failedChange) : 'Change not completed'} description={changeError} />;

    return <div className="ops-proxy">
        {reviewNotice}
        {loadError && <PageState kind="error" title={settings ? 'Refresh failed' : 'Could not load Proxy settings'} description={loadError}
            action={!needsReview && <Button variant="outline" onClick={refresh} disabled={refreshing || busy}>Retry refresh</Button>} />}
        {!settings && !loadError && <PageState kind="loading" title="Loading Proxy settings" />}
        {receipt && <div className="ops-proxy-receipt" role="status"><CheckCircle2 aria-hidden="true" />
            <div><strong>{receipt.message}</strong><p>{describeProxyChange(receipt.change)} · Saved at revision {receipt.revision}.</p></div>
        </div>}
        {!adding && deleting === null && changeError && <div>
            {actionError}
            {failedChange && <div className="ops-proxy-error-actions">
                <Button variant="outline" disabled={blocked || refreshing} onClick={() => {
                    if (failedChange.kind === 'add') { setNewAddress(failedChange.address); setAdding(true); }
                    else if (failedChange.kind === 'delete') setDeleting(failedChange.address);
                    else void submit(failedChange);
                }}>{failedChange.kind === 'switch' ? 'Retry change' : 'Review change'}</Button>
                <Button variant="ghost" disabled={busy} onClick={dismissChangeError}>Dismiss</Button>
            </div>}
        </div>}

        <PageSection title="Current Proxy" className="ops-proxy-current"
            description="Used for Consumer queries only when Proxy mode is selected."
            action={<span className="ops-proxy-revision">Configuration revision <strong>{settings?.revision ?? '—'}</strong></span>}>
            <div className="ops-proxy-current-value">
                <ProxyAddress address={settings ? current ?? 'Not selected' : 'Not loaded'} />
                {current && <StatusBadge tone="accent">{needsReview ? 'Review required' : 'Current'}</StatusBadge>}
            </div>
        </PageSection>

        <PageSection title="Saved Proxies" description="Manage saved Proxy addresses. Select an endpoint for queries in Proxy mode.">
            <div className="ops-proxy-table-scroll" tabIndex={0} role="region" aria-label="Saved Proxy endpoints">
                <table className="ops-proxy-table">
                    <thead><tr><th scope="col">Proxy address</th><th scope="col">Selection</th><th scope="col">Actions</th></tr></thead>
                    <tbody>{proxies.map(address => <tr key={address}>
                        <th scope="row"><ProxyAddress address={address} /></th>
                        <td><StatusBadge tone={address === current ? 'accent' : 'neutral'}>{address === current ? 'Current' : 'Saved'}</StatusBadge></td>
                        <td><div className="ops-proxy-row-actions">
                            {address !== current && <Button disabled={blocked}
                                aria-label={`Use ${address}`} onClick={() => void submit({ kind: 'switch', address })}>Use</Button>}
                            <Button variant="outline" className="ops-proxy-delete" icon={Trash2} disabled={blocked}
                                aria-label={`Delete ${address}`} onClick={event => {
                                    deleteTrigger.current = event.currentTarget; dismissChangeError(); setDeleting(address);
                                }}>Delete</Button>
                        </div></td>
                    </tr>)}</tbody>
                </table>
            </div>
            {settings && proxies.length === 0 && <PageState kind="empty" title="No saved Proxies"
                description="Add a host:port endpoint to make Proxy mode available. NameServer mode remains available." />}
        </PageSection>

        <PageSection title="Consumer query scope" description="Choose the mode used when opening Consumers.">
            <fieldset className="ops-proxy-scope" disabled={blocked}>
                <legend className="sr-only">Consumer query mode</legend>
                <label data-selected={consumerQueryMode === 'name_server'}><input type="radio" name="proxy-consumer-mode"
                    value="name_server" checked={consumerQueryMode === 'name_server'} onChange={() => setConsumerQueryMode('name_server')} />NameServer</label>
                <label data-selected={consumerQueryMode === 'proxy'} data-disabled={!proxyScope}><input type="radio" name="proxy-consumer-mode"
                    value="proxy" checked={consumerQueryMode === 'proxy'} disabled={!proxyScope} onChange={() => setConsumerQueryMode('proxy')} />Proxy</label>
            </fieldset>
            <p className="ops-proxy-scope-description">{consumerQueryMode === 'proxy'
                ? 'Consumer queries use the selected Proxy in Proxy mode. Other dashboard queries keep their own scope.'
                : 'Consumer queries use NameServer discovery. Selecting a saved Proxy does not change this mode.'}</p>
            {consumerQueryMode === 'proxy' && !proxyScope && <PageState kind="partial" title="Select a Proxy before querying"
                description="Add or select an endpoint above, or choose NameServer mode." />}
            <div className="ops-proxy-scope-footer"><Info aria-hidden="true" /><p>Running Info and JStack require NameServer/Broker scope.</p>
                <Button variant="ghost" className="ops-proxy-open" disabled={blocked || !scope}
                    onClick={() => setActiveTab('Consumer')}>Open Consumers <ArrowRight aria-hidden="true" /></Button>
            </div>
        </PageSection>
        <p className="ops-proxy-note">Refresh reloads saved configuration. A selected endpoint does not indicate Proxy health.</p>
        {busy && <p className="ops-proxy-pending" role="status">Saving: {describeProxyChange(pendingChange)}…</p>}

        <Dialog open={adding} onOpenChange={open => { if (!busy) setAdding(open); }}>
            <DialogContent showCloseButton={!busy} onCloseAutoFocus={event => { event.preventDefault(); addTrigger.current?.focus(); }}
                onEscapeKeyDown={event => { if (busy) event.preventDefault(); }} onInteractOutside={event => { if (busy) event.preventDefault(); }}>
                <DialogHeader><DialogTitle>Add Proxy</DialogTitle><DialogDescription>Enter one Proxy remoting endpoint in host:port form.</DialogDescription></DialogHeader>
                <form className="ops-proxy-form" onSubmit={event => {
                    event.preventDefault(); addressInput.current?.focus();
                    if (!newAddress.trim()) { setAddressError('Enter a Proxy address.'); return; }
                    if (blocked) return;
                    void submit({ kind: 'add', address: newAddress.trim() }).then(saved => { if (saved) { setNewAddress(''); setAdding(false); } });
                }}>
                    <Input ref={addressInput} label="Proxy address" placeholder="127.0.0.1:8080" autoComplete="off" value={newAddress}
                        error={addressError} readOnly={busy} onChange={event => { setNewAddress(event.target.value); setAddressError(''); }} />
                    <p className="ops-proxy-form-revision">Configuration revision {settings?.revision ?? '—'}</p>
                    {reviewNotice}{actionError}
                    {loadError && <p role="alert">{loadError}</p>}
                    <DialogFooter><Button variant="outline" disabled={busy} onClick={() => setAdding(false)}>Cancel</Button>
                        <Button type="submit" disabled={blocked}>{busy ? 'Saving…' : 'Add endpoint'}</Button></DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
        <Dialog open={deleting !== null} onOpenChange={open => { if (!open && !busy) setDeleting(null); }}>
            <DialogContent showCloseButton={!busy} onCloseAutoFocus={event => {
                event.preventDefault(); (deleteTrigger.current?.isConnected ? deleteTrigger.current : addTrigger.current)?.focus();
            }} onEscapeKeyDown={event => { if (busy) event.preventDefault(); }} onInteractOutside={event => { if (busy) event.preventDefault(); }}>
                <DialogHeader><DialogTitle>Delete saved Proxy?</DialogTitle><DialogDescription>This removes the address from this dashboard. It does not stop the Proxy.</DialogDescription></DialogHeader>
                <p className="ops-proxy-delete-address">{deleting}</p>
                {deleting !== null && deleting === current && <PageState kind="partial" title="This is the selected Proxy"
                    description={fallback ? <>Subsequent Proxy queries will use <span className="ops-proxy-delete-address">{fallback}</span>.</>
                        : 'No Proxy will remain selected. Proxy queries will require a new endpoint; the query mode will not change automatically.'} />}
                {deleting !== null && !proxies.includes(deleting) && <PageState kind="partial" title="This endpoint is no longer saved" description="Close this dialog and review the current list." />}
                {reviewNotice}{actionError}
                <DialogFooter><Button variant="outline" autoFocus disabled={busy} onClick={() => setDeleting(null)}>Cancel</Button>
                    <Button variant="danger" disabled={blocked || deleting === null || !proxies.includes(deleting)} onClick={event => {
                        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
                        if (deleting !== null) void submit({ kind: 'delete', address: deleting }).then(saved => { if (saved) setDeleting(null); });
                    }}>{busy ? 'Deleting…' : 'Delete endpoint'}</Button></DialogFooter>
            </DialogContent>
        </Dialog>
    </div>;
};
