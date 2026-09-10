import { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { ClusterService } from '../../../services/cluster.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type { BrokerConfigUpdateResult, ClusterBrokerCardItem } from '../types/cluster.types';
import { changedBrokerConfig } from '../config';

export function BrokerConfigEditor({ broker, onClose, onReadBack }: {
    broker: ClusterBrokerCardItem; onClose: () => void; onReadBack: (entries: Record<string, string>) => void;
}) {
    const [original, setOriginal] = useState<Record<string, string> | null>(null);
    const [text, setText] = useState('');
    const [error, setError] = useState('');
    const [busy, setBusy] = useState(false);
    const [pending, setPending] = useState<Record<string, string> | null>(null);
    const [result, setResult] = useState<BrokerConfigUpdateResult | null>(null);
    const [writeUnknown, setWriteUnknown] = useState(false);
    const generation = useRef(0);

    const refresh = async () => {
        const current = ++generation.current;
        setBusy(true);
        setError('');
        setPending(null);
        try {
            const config = await ClusterService.getClusterBrokerConfig({ brokerAddr: broker.address });
            if (current !== generation.current) return;
            setOriginal(config.entries);
            setText(JSON.stringify(config.entries, null, 2));
            setWriteUnknown(false);
            onReadBack(config.entries);
        } catch (error) {
            if (current === generation.current) setError(dashboardErrorMessage(error, 'Unable to read current Broker configuration.'));
        } finally { if (current === generation.current) setBusy(false); }
    };
    useEffect(() => {
        void refresh();
        return () => { generation.current++; };
    }, [broker.address]);

    const review = () => {
        if (!original || busy || writeUnknown) return;
        try {
            const patch = changedBrokerConfig(text, original);
            if (!Object.keys(patch).length) throw new Error('No configuration values have changed.');
            setPending(patch);
            setError('');
        } catch (error) { setError(error instanceof Error ? error.message : 'Invalid configuration JSON.'); }
    };
    const submit = async () => {
        if (!pending || busy) return;
        const current = ++generation.current;
        setBusy(true);
        setError('');
        setResult(null);
        try {
            const receipt = await ClusterService.updateBrokerConfig({ clusterName: broker.clusterName,
                brokerName: broker.brokerName, brokerId: broker.brokerId, brokerAddr: broker.address, entries: pending });
            if (current !== generation.current) return;
            setResult(receipt);
            setPending(null);
            if (receipt.entries) {
                setOriginal(receipt.entries);
                setText(JSON.stringify(receipt.entries, null, 2));
                onReadBack(receipt.entries);
            } else setWriteUnknown(true);
        } catch (error) {
            if (current !== generation.current) return;
            setPending(null);
            setWriteUnknown(true);
            setError(dashboardErrorMessage(error, 'Write result was not confirmed. Refresh the Broker before another change.'));
        } finally { if (current === generation.current) setBusy(false); }
    };
    return <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/40 p-5">
        <section role="dialog" aria-modal="true" aria-labelledby="broker-config-editor-title"
            className="flex max-h-[90vh] w-full max-w-4xl flex-col gap-4 overflow-auto rounded-xl bg-white p-6 shadow-xl dark:bg-gray-900 dark:text-gray-100">
            <header className="flex items-start justify-between gap-4"><div>
                <h2 id="broker-config-editor-title" className="text-xl font-semibold">Edit Broker configuration</h2>
                <p className="font-mono text-sm">{broker.clusterName} / {broker.brokerName} / {broker.brokerId} · {broker.address}</p>
            </div><button aria-label="Close editor" onClick={onClose}><X /></button></header>
            {error && <p role="alert" className="text-red-600 dark:text-red-400">{error}</p>}
            {result && <div role="status" className="rounded border p-3 text-sm">
                <strong>{result.written ? 'Broker acknowledged the write.' : 'Write was not confirmed.'}</strong>
                <p>{result.readBack === 'confirmed' ? 'Read-back matches every submitted value.' : result.readBack === 'different'
                    ? 'Read-back differs from the submitted values. Review the current configuration below.'
                    : 'Read-back failed. The acknowledged write remains applied. Refresh before another change.'}</p>
                <p>Submitted keys: {result.changedKeys.join(', ')}</p>
            </div>}
            <label className="flex min-h-0 flex-1 flex-col gap-2 text-sm">Configuration JSON · string values
                <textarea aria-label="Broker configuration JSON" value={text} disabled={busy || !original || Boolean(pending) || writeUnknown}
                    onChange={event => { setText(event.target.value); setResult(null); }} spellCheck={false}
                    className="min-h-64 w-full rounded border bg-transparent p-3 font-mono text-sm" />
            </label>
            {pending && <section aria-label="Confirm Broker changes" className="rounded border border-amber-400 p-4 text-sm">
                <h3 className="font-semibold">Confirm changes on {broker.brokerName} [{broker.brokerId}] at {broker.address}</h3>
                <ul className="my-3 max-h-40 overflow-auto">{Object.keys(pending).map(key => <li key={key} className="font-mono">{key}: {original?.[key] ?? '(not set)'} → {pending[key]}</li>)}</ul>
                <p>Only these keys will be submitted.</p>
                <div className="mt-3 flex gap-3"><button disabled={busy} onClick={() => setPending(null)}>Back to editing</button>
                    <button disabled={busy} onClick={() => void submit()} className="rounded bg-blue-600 px-4 py-2 text-white">{busy ? 'Applying…' : 'Confirm and apply'}</button></div>
            </section>}
            <footer className="flex justify-end gap-3 text-sm">
                <button disabled={busy} onClick={() => void refresh()} className="rounded border px-4 py-2">Refresh current configuration</button>
                <button disabled={busy || !original || Boolean(pending) || writeUnknown} onClick={review} className="rounded bg-blue-600 px-4 py-2 text-white disabled:opacity-50">Review changes</button>
            </footer>
        </section>
    </div>;
}
