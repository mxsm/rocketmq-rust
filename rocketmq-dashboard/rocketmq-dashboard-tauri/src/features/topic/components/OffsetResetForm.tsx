import { useEffect, useRef, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { ConsumerRequestGeneration } from '../../consumer/scope';
import { currentLocalMinute, offsetResetRequest } from '../offsetReset';
import type { ResetOffsetRequest, TopicMutationResult } from '../types/topic.types';

interface Props {
    topic: string;
    group: string;
    disabled?: boolean;
    readProgress: (request: ResetOffsetRequest) => Promise<unknown>;
}

// Parent keys this form by entity/scope. Accepted writes survive unmount, but their callbacks do not.
export function OffsetResetForm({ topic, group, disabled = false, readProgress }: Props) {
    const [time, setTime] = useState(currentLocalMinute);
    const [force, setForce] = useState(false);
    const [confirmation, setConfirmation] = useState<ResetOffsetRequest | null>(null);
    const [receipt, setReceipt] = useState<TopicMutationResult | null>(null);
    const [progress, setProgress] = useState<unknown>(null);
    const [error, setError] = useState('');
    const [pending, setPending] = useState(false);
    const generation = useRef(new ConsumerRequestGeneration());
    const busy = useRef(false);
    useEffect(() => {
        generation.current.invalidate(); setConfirmation(null); setReceipt(null); setProgress(null); setError(''); setPending(false); busy.current = false;
        return () => generation.current.invalidate();
    }, [topic, group]);

    const review = () => {
        try { setConfirmation(offsetResetRequest(topic, group, time, force)); setError(''); }
        catch (error) { setError((error as Error).message); }
    };
    const submit = async () => {
        if (!confirmation || disabled || busy.current || confirmation.topic !== topic || confirmation.consumerGroupList[0] !== group) return;
        const request = confirmation;
        const current = generation.current.begin();
        busy.current = true; setPending(true); setError(''); setConfirmation(null);
        try {
            const result = await TopicService.resetConsumerOffset(request);
            if (!current()) return;
            setReceipt(result);
            try {
                const result = await readProgress(request);
                if (current()) setProgress(result);
            } catch {
                if (current()) setError('The reset receipt is retained, but progress could not be read. Refresh progress before another operation.');
            }
        } catch (error) {
            if (current()) setError(dashboardErrorMessage(error, 'Offset reset was not confirmed. Inspect progress before manually retrying.'));
        } finally { if (current()) { busy.current = false; setPending(false); } }
    };
    return <section className="space-y-4 p-4" aria-label="Reset offset">
        <p>Topic: <strong>{topic || 'Select a Topic'}</strong> · Group: <strong>{group || 'Select a group'}</strong></p>
        <label className="block">Local date and time ({Intl.DateTimeFormat().resolvedOptions().timeZone})
            <input className="block rounded border p-2 dark:bg-gray-900" type="datetime-local" value={time} disabled={pending || Boolean(receipt)} onChange={event => { setTime(event.target.value); setConfirmation(null); }} />
        </label>
        <label className="block"><input type="checkbox" checked={force} disabled={pending || Boolean(receipt)} onChange={event => { setForce(event.target.checked); setConfirmation(null); }} /> Force reset (allows moving offsets backwards)</label>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {receipt && <div role="status"><strong>{receipt.success ? 'Reset acknowledged' : 'Reset not confirmed'}</strong><p>{receipt.message}</p></div>}
        {progress !== null && <details open><summary>Progress read after reset</summary><pre className="max-h-52 overflow-auto">{JSON.stringify(progress, null, 2)}</pre></details>}
        {confirmation && <div className="rounded border border-amber-400 p-3" role="alert">
            <p>Confirm Topic: {confirmation.topic} · Group: {confirmation.consumerGroupList[0]}</p>
            <p>Time: {new Date(confirmation.resetTime).toLocaleString()} · Timestamp: {confirmation.resetTime} ms · Force: {String(confirmation.force)}</p>
            <button type="button" onClick={() => void submit()} disabled={pending || disabled}>Confirm offset reset</button>
            <button type="button" className="ml-4" onClick={() => setConfirmation(null)}>Cancel confirmation</button>
        </div>}
        {!confirmation && !receipt && <button type="button" disabled={pending || disabled || !topic || !group} onClick={review}>{pending ? 'Resetting and reading progress…' : 'Review offset reset'}</button>}
    </section>;
}
