import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { Button } from '../../../components/ui/LegacyButton';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../../../components/ui/tabs';
import { PageState } from '../../../components/layout/PageState';
import { BrokerEntries } from './BrokerEntries';
import { createBrokerConfigController } from '../brokerConfigController';
import type { BrokerIdentity } from '../brokerIdentity';

export function BrokerConfigEditor({ broker, revision, environmentId, onClose, onReturnFocus }: {
    broker: BrokerIdentity; revision: number; environmentId: string | null;
    onClose: () => void; onReturnFocus: () => void;
}) {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const controller = useMemo(() => createBrokerConfigController({ broker,
        isCurrent: () => {
            const current = ConnectionStore.getSnapshot();
            return current?.revision === revision && current.environmentId === environmentId;
        } }), [broker, revision, environmentId]);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    const [mode, setMode] = useState('fields');
    const bodyRef = useRef<HTMLDivElement>(null);
    const { original, text, pending, receipt, operation, error, requiresRead } = state;
    const busy = operation !== 'idle';
    const contextChanged = state.contextChanged || settings?.revision !== revision || settings.environmentId !== environmentId;
    const disabled = busy || contextChanged || requiresRead || !original;
    useEffect(() => {
        controller.start();
        void controller.refresh();
        return controller.stop;
    }, [controller]);
    useEffect(() => controller.observeContext(), [controller, settings?.revision, settings?.environmentId]);
    useEffect(() => {
        if (error || receipt || pending || contextChanged) bodyRef.current?.scrollTo({ top: 0 });
    }, [error, receipt, pending, contextChanged]);
    const draft = useMemo<Record<string, string> | null>(() => {
        try {
            const parsed: unknown = JSON.parse(text);
            return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
                && Object.values(parsed).every(value => typeof value === 'string') ? parsed as Record<string, string> : null;
        } catch { return null; }
    }, [text]);
    const readBackMessage = receipt?.readBack === 'confirmed'
        ? 'Readback matches every submitted value.'
        : receipt?.readBack === 'different' ? 'Readback differs. Review the returned values before another change.'
            : 'Readback was unavailable. An acknowledged write is not retried. Read the current configuration before another change.';
    return <Dialog open onOpenChange={open => { if (!open && !busy) onClose(); }}>
        <DialogContent className="ops-broker-editor" showCloseButton={!busy}
            onCloseAutoFocus={event => { event.preventDefault(); onReturnFocus(); }}
            onEscapeKeyDown={event => { if (busy) event.preventDefault(); }}
            onInteractOutside={event => { if (busy) event.preventDefault(); }}>
            <DialogHeader><DialogTitle>Edit Broker configuration</DialogTitle>
                <DialogDescription>Only changed values are submitted to the selected Broker.</DialogDescription>
            </DialogHeader>
            <div className="ops-broker-editor-target"><strong>{broker.brokerName} [{broker.brokerId}]</strong>
                <span>{broker.clusterName} · {broker.address}</span><small>Environment {environmentId ?? 'Not configured'} · Connection revision {revision}</small></div>
            <form className="ops-broker-editor-form" onSubmit={event => {
                event.preventDefault();
                event.currentTarget.closest<HTMLElement>('[role="dialog"]')?.focus();
                if (pending) void controller.submit(); else controller.review();
            }}>
                <div className="ops-broker-editor-body" ref={bodyRef}>
                    {contextChanged && <PageState kind="stale" title="Connection context changed"
                        description="This dialog retains the original Broker and operation result. Close it and select a Broker in the current environment before editing again." />}
                    {error && <PageState kind="error" title="Configuration operation could not complete" description={error} />}
                    {receipt && <section className="ops-broker-receipt" aria-label="Broker configuration receipt" role="status">
                        <strong>{receipt.written ? 'Broker acknowledged the write.' : 'Write was not confirmed.'}</strong>
                        <p>{readBackMessage}</p><p>Submitted keys: <code>{receipt.changedKeys.join(', ')}</code></p>
                    </section>}
                    {operation === 'reading' && <PageState kind="loading" title="Reading current Broker configuration" />}
                    {pending ? <section aria-label="Confirm Broker changes" className="ops-broker-review">
                        <h3>Review changes for {broker.brokerName}</h3>
                        <p>Only the following keys will be submitted to {broker.address}.</p>
                        <div className="ops-cluster-table-scroll" role="region" aria-label="Configuration changes" tabIndex={0}>
                            <table className="ops-broker-diff"><thead><tr><th scope="col">Key</th><th scope="col">Before</th><th scope="col">After</th></tr></thead>
                                <tbody>{Object.entries(pending).map(([key, value]) => <tr key={key}><th scope="row">{key}</th>
                                    <td><code>{original?.[key] ?? '(not set)'}</code></td><td><code>{value}</code></td></tr>)}</tbody>
                            </table>
                        </div>
                    </section> : original && <Tabs value={mode} onValueChange={setMode}>
                        <TabsList aria-label="Configuration editor mode"><TabsTrigger value="fields">Fields</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger></TabsList>
                        <TabsContent value="fields">{draft ? <BrokerEntries entries={draft} disabled={disabled}
                            onChange={(key, value) => controller.setText(JSON.stringify({ ...draft, [key]: value }, null, 2))} />
                            : <PageState kind="error" title="Field view needs a JSON object of string values" description="Correct the JSON in the JSON tab before continuing." />}</TabsContent>
                        <TabsContent value="json"><label className="ops-broker-json"><span>Configuration JSON · string values</span>
                            <textarea aria-label="Broker configuration JSON" value={text} readOnly={disabled} spellCheck={false}
                                onChange={event => controller.setText(event.target.value)} /></label>
                            <p className="ops-cluster-muted">Add properties through JSON. Removing existing keys is not supported.</p>
                        </TabsContent>
                    </Tabs>}
                </div>
                <DialogFooter>
                    <Button variant="outline" disabled={busy} onClick={onClose}>Close</Button>
                    {!pending && <Button variant="outline" disabled={busy || contextChanged} onClick={() => void controller.refresh()}>
                        {!requiresRead && original && text !== JSON.stringify(original, null, 2) ? 'Reload and discard draft' : 'Read current configuration'}</Button>}
                    {pending ? <><Button variant="outline" disabled={busy || contextChanged} onClick={controller.editAgain}>Back to editing</Button>
                        <Button type="submit" disabled={disabled}>{operation === 'writing' ? 'Applying…' : 'Confirm and apply'}</Button></>
                        : <Button type="submit" disabled={disabled}>Review changes</Button>}
                </DialogFooter>
            </form>
        </DialogContent>
    </Dialog>;
}
