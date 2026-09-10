import type { ConsumerMutationResult } from '../types/consumer.types';
import { failedConsumerBrokers } from '../mutation';

export const ConsumerMutationReceipt = ({ result, onReviewFailed, disabled }: {
    result: ConsumerMutationResult; onReviewFailed: () => void; disabled: boolean;
}) => <section aria-label="Consumer operation results" className="space-y-3 rounded-xl border p-4">
    <p className="text-sm font-semibold">{result.consumerGroup}: {result.success ? 'Completed' : 'Review incomplete results'}</p>
    <div className="max-h-60 overflow-auto"><table className="w-full text-left text-sm">
        <thead><tr><th>Target</th><th>Operation</th><th>Result</th></tr></thead>
        <tbody>{result.targets.map((target, index) => <tr key={`${target.kind}:${target.target}:${index}`}>
            <td className="p-2 font-mono">{target.target}</td><td>{target.kind}</td>
            <td className="p-2"><strong>{target.success ? 'Completed' : 'Not confirmed'}</strong>
                <p>{target.message}</p>{target.errorCode && <code>{target.errorCode}</code>}</td>
        </tr>)}</tbody>
    </table></div>
    <p className="text-sm">Completed changes remain applied. Refresh and review current state before submitting another operation.</p>
    {failedConsumerBrokers(result).length > 0 && <button type="button" disabled={disabled}
        className="rounded border px-3 py-2 text-sm" onClick={onReviewFailed}>Refresh and review failed Brokers only</button>}
</section>;
