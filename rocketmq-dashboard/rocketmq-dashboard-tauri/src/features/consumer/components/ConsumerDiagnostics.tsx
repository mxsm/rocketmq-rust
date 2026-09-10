import { useEffect, useRef, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { ConsumerRequestGeneration } from '../scope';
import type { ConsumerDiagnosticRequest, ConsumerDiagnosticResult } from '../types/consumer.types';

export function ConsumerDiagnostics({ request }: { request: ConsumerDiagnosticRequest }) {
    const [result, setResult] = useState<ConsumerDiagnosticResult | null>(null);
    const [pending, setPending] = useState(false);
    const [error, setError] = useState('');
    const generation = useRef(new ConsumerRequestGeneration());
    useEffect(() => () => generation.current.invalidate(), []);
    const query = async (kind: 'running' | 'jstack') => {
        if (pending) return;
        const current = generation.current.begin();
        setPending(true); setResult(null); setError('');
        try {
            const value = await (kind === 'jstack' ? ConsumerService.queryJstack(request) : ConsumerService.queryRunningInfo(request));
            if (current()) setResult(value);
        } catch (error) { if (current()) setError(dashboardErrorMessage(error, 'Unable to query client diagnostics.')); }
        finally { if (current()) setPending(false); }
    };
    return <section aria-label="Consumer diagnostics" className="space-y-3 rounded border p-4">
        <h4>Diagnostics: {request.consumerGroup} / {request.clientId}</h4>
        <p>Manually request current runtime data. Support is determined by the Broker and client response; opening this panel does not request a thread stack.</p>
        <div className="flex gap-4">
            <button type="button" disabled={pending} onClick={() => void query('running')}>Running information</button>
            <button type="button" disabled={pending} onClick={() => void query('jstack')}>Request thread stack</button>
        </div>
        {pending && <p role="status">Requesting diagnostics…</p>}
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {result && <>
            <p role="status">Status: {result.status}{result.reason ? ` — ${result.reason}` : ''}</p>
            {result.truncated && <p role="alert" className="text-amber-600">Output is truncated or a requested section was unavailable. This is not the complete diagnostic output.</p>}
            {result.status === 'available' && <>
                {([['Properties', result.properties], ['Subscriptions', result.subscriptions], ['Process queues', result.processQueues]] as const).map(([label, values]) =>
                    <details key={label}><summary>{label} ({values.length})</summary><pre className="max-h-64 overflow-auto whitespace-pre-wrap">{JSON.stringify(values, null, 2)}</pre></details>)}
                {result.jstack !== null && <details open><summary>Thread stack</summary><pre className="max-h-96 overflow-auto whitespace-pre-wrap">{result.jstack}</pre></details>}
            </>}
        </>}
    </section>;
}
