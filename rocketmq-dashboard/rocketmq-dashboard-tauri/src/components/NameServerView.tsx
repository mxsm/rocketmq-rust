import { useCallback, useMemo, useRef, useState } from 'react';
import { CheckCircle2, Info, Plus, Trash2 } from 'lucide-react';
import { useNameServer } from '../features/nameserver/hooks/useNameServer';
import { describeNameServerChange } from '../features/nameserver/nameServerController';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Toggle } from './ui/LegacyToggle';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/dialog';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { StatusBadge } from './layout/StatusBadge';
import '../features/nameserver/nameserver.css';

function Reachability({ alive, stale }: { alive?: boolean; stale: boolean }) {
    if (alive === undefined) return <StatusBadge>Not probed</StatusBadge>;
    return <StatusBadge tone={stale ? 'neutral' : alive ? 'success' : 'warning'}>
        {stale ? 'Previous: ' : ''}{alive ? 'Reachable' : 'Probe failed'}
    </StatusBadge>;
}

function AddressDetails({ address }: { address: string }) {
    if (address.length <= 60) return null;
    return <details className="ops-nameserver-address-details"><summary>View full address</summary><code>{address}</code></details>;
}

export const NameServerView = () => {
    const state = useNameServer();
    const { settings, observation, pendingChange, refreshing, needsReview, loadError, changeError, failedChange, receipt, refresh, submit, dismissChangeError } = state;
    const [adding, setAdding] = useState(false);
    const [newAddress, setNewAddress] = useState('');
    const [addressError, setAddressError] = useState('');
    const [deleting, setDeleting] = useState<string | null>(null);
    const addTrigger = useRef<HTMLButtonElement>(null);
    const addressInput = useRef<HTMLInputElement>(null);
    const deleteTrigger = useRef<HTMLButtonElement | null>(null);
    const busy = pendingChange !== null;
    const blocked = busy || needsReview || !settings;
    const current = settings?.nameserver.currentNamesrv;
    const addresses = settings?.nameserver.namesrvAddrList ?? [];
    const currentProbe = observation?.servers.find(server => server.address === current);
    const openAdd = useCallback(() => { dismissChangeError(); setAddressError(''); setAdding(true); }, [dismissChangeError]);
    const actions = useMemo(() => <Button ref={addTrigger} icon={Plus} onClick={openAdd} disabled={busy || !settings}>Add NameServer</Button>, [busy, settings, openAdd]);
    usePageRefresh({ refresh, pending: refreshing || busy, refreshedAt: observation?.receivedAt, actions });

    const reviewNotice = needsReview && <PageState kind="stale" title="Review changed connection settings"
        description="Your draft is preserved. Reload the current configuration, review it, then submit the change again."
        action={<Button variant="outline" onClick={refresh} disabled={refreshing || busy}>{refreshing ? 'Reloading…' : 'Reload settings'}</Button>} />;
    const actionError = changeError && <PageState kind="error"
        title={failedChange && !adding && deleting === null ? describeNameServerChange(failedChange) : 'Change not completed'} description={changeError} />;

    return <div className="ops-nameserver">
        {reviewNotice}
        {loadError && <PageState kind="error" title={settings ? 'Refresh failed' : 'Could not load NameServer settings'}
            description={loadError} action={!needsReview && <Button variant="outline" onClick={refresh} disabled={refreshing || busy}>Retry refresh</Button>} />}
        {!settings && !loadError && <PageState kind="loading" title="Loading NameServer settings" />}
        {receipt && <div className="ops-nameserver-receipt" role="status"><CheckCircle2 aria-hidden="true" />
            <span>{receipt.message} <small>Saved at revision {receipt.revision}.</small></span>
        </div>}
        {!adding && deleting === null && changeError && <div className="ops-nameserver-failed">
            {actionError}
            {failedChange && <div className="ops-nameserver-error-actions">
                <Button variant="outline" disabled={blocked || refreshing} onClick={() => {
                    if (failedChange.kind === 'add') {
                        setNewAddress(failedChange.address);
                        setAdding(true);
                    } else if (failedChange.kind === 'delete') {
                        setDeleting(failedChange.address);
                    } else {
                        void submit(failedChange);
                    }
                }}>{failedChange.kind === 'add' || failedChange.kind === 'delete' ? 'Review change' : 'Retry change'}</Button>
                <Button variant="ghost" disabled={busy} onClick={dismissChangeError}>Dismiss</Button>
            </div>}
        </div>}

        <section className="ops-nameserver-current" aria-label="Current NameServer selection">
            <div><span>Current endpoint{needsReview ? ' · review required' : ''}</span>
                <div className="ops-nameserver-current-value"><strong>{current ?? 'Not selected'}</strong>
                    {current && <Reachability alive={currentProbe?.isAlive} stale={Boolean(loadError)} />}
                </div>
                {current && <AddressDetails address={current} />}
            </div>
            <div className="ops-nameserver-revision"><span>Configuration revision</span><strong>{settings?.revision ?? '—'}</strong></div>
        </section>

        <PageSection title="NameServer endpoints" description="Configure and switch between NameServer endpoints.">
            <div className="ops-nameserver-table-scroll" tabIndex={0} role="region" aria-label="NameServer endpoint table">
                <table className="ops-nameserver-table">
                    <thead><tr><th scope="col">Address</th><th scope="col">Role</th><th scope="col">Reachability</th><th scope="col">Actions</th></tr></thead>
                    <tbody>{addresses.map(address => {
                        const isCurrent = address === current;
                        const probe = observation?.servers.find(server => server.address === address);
                        return <tr key={address}>
                            <th scope="row"><span className="ops-nameserver-address">{address}</span><AddressDetails address={address} /></th>
                            <td><StatusBadge tone={isCurrent ? 'accent' : 'neutral'}>{isCurrent ? 'Current' : 'Saved'}</StatusBadge></td>
                            <td><Reachability alive={probe?.isAlive} stale={Boolean(loadError)} /></td>
                            <td>{isCurrent ? <span className="ops-nameserver-no-action" title="Select another endpoint before deleting this one.">—</span> :
                                <div className="ops-nameserver-row-actions">
                                    <Button variant="outline" disabled={blocked} aria-label={`Use ${address}`} onClick={() => void submit({ kind: 'switch', address })}>Use</Button>
                                    <Button variant="ghost" className="ops-nameserver-delete" icon={Trash2} disabled={blocked} aria-label={`Delete ${address}`}
                                        onClick={event => { deleteTrigger.current = event.currentTarget; dismissChangeError(); setDeleting(address); }}>Delete</Button>
                                </div>}</td>
                        </tr>;
                    })}</tbody>
                </table>
            </div>
            {settings && addresses.length === 0 && <PageState kind="empty" title="No NameServer endpoints"
                description="Add an endpoint to connect this dashboard to a RocketMQ-Rust cluster." />}
            <p className="ops-nameserver-probe-note">{observation
                ? <>{loadError ? 'Previous probe results received' : 'Probe results received'} <time dateTime={new Date(observation.receivedAt).toISOString()}>{new Date(observation.receivedAt).toLocaleString()}</time>. Refreshes every 5 seconds while idle.</>
                : 'No probe results for this configuration yet.'}
                {' '}Current indicates the selected route; a failed probe can also mean transport or authentication failed.
            </p>
        </PageSection>

        <PageSection title="Connection settings" description="Transport options for RocketMQ admin requests.">
            <div className="ops-nameserver-settings">
                <div className="ops-nameserver-setting"><div><strong id="nameserver-vip-label">VIP channel</strong>
                    <p id="nameserver-vip-help">Use the alternative Broker request port. This does not change the NameServer port.</p></div>
                    <Toggle aria-labelledby="nameserver-vip-label" aria-describedby="nameserver-vip-help"
                        checked={settings?.nameserver.useVIPChannel ?? false} disabled={blocked}
                        onChange={enabled => void submit({ kind: 'vip', enabled })} />
                </div>
                <div className="ops-nameserver-setting"><div><strong id="nameserver-tls-label">TLS</strong>
                    <p id="nameserver-tls-help">Use TLS for admin connections. Endpoints must support the configured TLS transport.</p></div>
                    <Toggle aria-labelledby="nameserver-tls-label" aria-describedby="nameserver-tls-help"
                        checked={settings?.nameserver.useTLS ?? false} disabled={blocked}
                        onChange={enabled => void submit({ kind: 'tls', enabled })} />
                </div>
                <div className="ops-nameserver-setting"><div><strong>Credentials</strong><p>Configured when starting the app. Secret values are never displayed.</p></div>
                    <StatusBadge>{settings ? settings.credentialsConfigured ? 'Configured' : 'Not configured' : 'Unknown'}</StatusBadge>
                </div>
            </div>
        </PageSection>
        <div className="ops-nameserver-note"><Info aria-hidden="true" /><span>Changes apply to subsequent requests. Review again if the configuration changes.</span></div>
        {busy && <p className="ops-nameserver-pending" role="status">Saving: {describeNameServerChange(pendingChange)}…</p>}

        <Dialog open={adding} onOpenChange={open => { if (!busy) setAdding(open); }}>
            <DialogContent showCloseButton={!busy} onCloseAutoFocus={event => {
                // A revision change can replace the toolbar button while the dialog is open.
                event.preventDefault();
                addTrigger.current?.focus();
            }} onEscapeKeyDown={event => { if (busy) event.preventDefault(); }}
                onInteractOutside={event => { if (busy) event.preventDefault(); }}>
                <DialogHeader><DialogTitle>Add NameServer</DialogTitle><DialogDescription>Save a NameServer address or a semicolon-separated address group.</DialogDescription></DialogHeader>
                <form className="ops-nameserver-form" onSubmit={event => {
                    event.preventDefault();
                    addressInput.current?.focus();
                    if (!newAddress.trim()) { setAddressError('Enter a NameServer address.'); return; }
                    if (blocked) return;
                    void submit({ kind: 'add', address: newAddress.trim() }).then(saved => {
                        if (saved) { setNewAddress(''); setAdding(false); }
                    });
                }}>
                    <Input ref={addressInput} label="NameServer address" placeholder="127.0.0.1:9876" autoComplete="off" value={newAddress}
                        error={addressError} readOnly={busy} onChange={event => { setNewAddress(event.target.value); setAddressError(''); }} />
                    <p className="ops-nameserver-form-revision">Configuration revision {settings?.revision ?? '—'}</p>
                    {reviewNotice}{actionError}
                    {loadError && <p role="alert">{loadError}</p>}
                    <DialogFooter><Button variant="outline" disabled={busy} onClick={() => setAdding(false)}>Cancel</Button>
                        <Button type="submit" disabled={blocked}>{busy ? 'Saving…' : 'Add endpoint'}</Button></DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
        <Dialog open={deleting !== null} onOpenChange={open => { if (!open && !busy) setDeleting(null); }}>
            <DialogContent showCloseButton={!busy} onCloseAutoFocus={event => {
                event.preventDefault();
                (deleteTrigger.current?.isConnected ? deleteTrigger.current : addTrigger.current)?.focus();
            }} onEscapeKeyDown={event => { if (busy) event.preventDefault(); }}
                onInteractOutside={event => { if (busy) event.preventDefault(); }}>
                <DialogHeader><DialogTitle>Delete saved endpoint?</DialogTitle><DialogDescription>This removes the address from this dashboard. It does not stop the NameServer.</DialogDescription></DialogHeader>
                <p className="ops-nameserver-address">{deleting}</p>
                {reviewNotice}{actionError}
                {deleting !== null && deleting === current && <PageState kind="partial" title="This endpoint is now selected"
                    description="Select another endpoint before deleting this one." />}
                <DialogFooter><Button variant="outline" autoFocus disabled={busy} onClick={() => setDeleting(null)}>Cancel</Button>
                    <Button variant="danger" disabled={blocked || deleting === current} onClick={event => {
                        // Keep focus inside the modal while every action is disabled.
                        event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
                        if (deleting !== null) void submit({ kind: 'delete', address: deleting }).then(saved => { if (saved) setDeleting(null); });
                    }}>{busy ? 'Deleting…' : 'Delete endpoint'}</Button></DialogFooter>
            </DialogContent>
        </Dialog>
    </div>;
};
