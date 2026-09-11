import { useEffect, useRef, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { ConsumerRequestGeneration } from '../scope';
import type { ConsumerDiagnosticRequest, ConsumerDiagnosticResult } from '../types/consumer.types';
import { consumerScopeKey } from '../scope';
import { checkConsumerIdentity } from '../consumerModel';
import { Button } from '../../../components/ui/LegacyButton';

export function ConsumerDiagnostics({ request, disabled = false }: { request: ConsumerDiagnosticRequest; disabled?: boolean }) {
    const [result, setResult] = useState<ConsumerDiagnosticResult | null>(null);
    const [pending, setPending] = useState(false);
    const [error, setError] = useState('');
    const generation = useRef(new ConsumerRequestGeneration());
    const busy = useRef(false);
    useEffect(() => {
        generation.current.invalidate(); busy.current = false; setResult(null); setError(''); setPending(false);
        return () => generation.current.invalidate();
    }, [request.consumerGroup, request.clientId, consumerScopeKey(request.scope)]);
    const query = async (kind: 'running' | 'jstack') => {
        if (busy.current || disabled || request.scope.mode === 'proxy') return;
        const current = generation.current.begin();
        busy.current = true; setPending(true); setResult(null); setError('');
        try {
            const value = await (kind === 'jstack' ? ConsumerService.queryJstack(request) : ConsumerService.queryRunningInfo(request));
            if (current()) {
                checkConsumerIdentity(value, request.consumerGroup);
                if (value.clientId !== request.clientId) throw new Error('Unexpected diagnostic client');
                setResult(value);
            }
        } catch (error) { if (current()) setError(dashboardErrorMessage(error, 'Unable to query client diagnostics.')); }
        finally { if (current()) { busy.current = false; setPending(false); } }
    };
    return <section aria-label="Consumer diagnostics" className="space-y-3 rounded border p-4">
        <h4>Diagnostics: {request.consumerGroup} / {request.clientId}</h4>
        <p>Manually request current runtime data. Support is determined by the Broker and client response; opening this panel does not request a thread stack.</p>
        <div className="ops-consumer-actions">
            <Button variant="outline" disabled={pending || disabled || request.scope.mode === 'proxy'} onClick={() => void query('running')}>Running information</Button>
            <Button variant="outline" disabled={pending || disabled || request.scope.mode === 'proxy'} onClick={() => void query('jstack')}>Request thread stack</Button>
        </div>
        {pending && <p role="status">Requesting diagnostics…</p>}
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {result && <>
            <p role="status">Status: {result.status}{result.reason ? ` — ${result.reason}` : ''}</p>
            {result.truncated && <p role="alert" className="text-amber-600">Output is truncated or a requested section was unavailable. This is not the complete diagnostic output.</p>}
            {result.status === 'available' && <>
                {([['Properties', result.properties], ['Subscriptions', result.subscriptions], ['Process queues', result.processQueues]] as const).map(([label, values]) =>
                    <details key={label}><summary>{label} ({values.length})</summary><pre className="ops-consumer-code" tabIndex={0}>{JSON.stringify(values, null, 2)}</pre></details>)}
                {result.jstack !== null && <details open><summary>Thread stack</summary><pre className="ops-consumer-code" tabIndex={0}>{result.jstack}</pre></details>}
            </>}
        </>}
    </section>;
}
