import type { TopicBatchResult, TopicTargetReceipt } from '../types/topic.types';

export const failedBrokerNames = (result: TopicBatchResult): string[] =>
    result.targets.filter((target) => target.kind === 'broker' && !target.success).map((target) => target.name);

export const TopicMutationReceipt = ({ result, onReviewFailed, onReviewTarget }: {
    result: TopicBatchResult;
    onReviewFailed?: () => void;
    onReviewTarget?: (target: TopicTargetReceipt) => void;
}) => <section aria-label="Topic operation receipt" className="m-4 max-h-60 overflow-auto shrink-0 rounded-lg border border-gray-300 dark:border-gray-700 p-4 space-y-3">
    <div role="status"><strong>{result.operation}: {result.topic}</strong><p>{result.message}</p></div>
    <p className="text-sm">{result.targetCount} targets. Completed targets are not automatically retried.</p>
    <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead><tr><th>Target</th><th>Result</th><th>Details</th><th /></tr></thead>
        <tbody>{result.targets.map((target) => <tr key={`${target.kind}:${target.name}`}>
            <td className="py-2 pr-3">{target.kind}: {target.name}</td>
            <td className="pr-3">{target.success ? 'Completed' : 'Needs review'}</td>
            <td>{target.message}{target.errorCode && <code className="block">{target.errorCode}</code>}</td>
            <td>{!target.success && onReviewTarget && <button type="button" className="underline" onClick={() => onReviewTarget(target)}>Review target</button>}</td>
        </tr>)}</tbody>
    </table></div>
    {result.orderConfig && <p className="text-sm"><strong>ORDER_TOPIC_CONFIG: </strong>{result.orderConfig.message}{result.orderConfig.errorCode && <code className="block">{result.orderConfig.errorCode}</code>}</p>}
    {!result.orderConfig && result.operation !== 'delete_broker' && <p className="text-sm">Order configuration was not completed.</p>}
    {onReviewFailed && failedBrokerNames(result).length > 0 && <button type="button" className="underline text-sm" onClick={onReviewFailed}>Review failed Brokers only</button>}
</section>;
