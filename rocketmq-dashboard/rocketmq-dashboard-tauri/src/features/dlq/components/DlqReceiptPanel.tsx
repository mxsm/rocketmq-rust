import { Download } from 'lucide-react';
import { toast } from 'sonner';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { Button } from '../../../components/ui/LegacyButton';
import { dlqReceiptCsv, dlqReceiptRows, type DlqReceipt } from '../receipts';
import { downloadDlqCsv } from '../download';
import { visibleMessageText } from '../../message/messageModel';

export function DlqReceiptPanel({ receipt, review, canReview }: { receipt: DlqReceipt | null; review: () => void; canReview: boolean }) {
    const rows = receipt ? dlqReceiptRows(receipt) : [];
    const succeeded = rows.filter(row => row.outcome === 'success').length;
    const failed = rows.filter(row => row.outcome === 'failed').length;
    const unknown = rows.length - succeeded - failed;
    return <PageSection title="Resend results" description={receipt
        ? `${succeeded} succeeded · ${failed} failed · ${unknown} unknown. This batch is not atomic; no automatic retry is performed.`
        : 'Completed results remain available during this session when you change pages or connections.'}
        action={receipt && <div className="ops-message-actions"><Button variant="outline" disabled={!canReview} onClick={review}>Review failed targets</Button>
            <Button variant="outline" icon={Download} onClick={() => { downloadDlqCsv({ fileName: 'dlq-resend-results.csv', mimeType: 'text/csv;charset=utf-8', content: dlqReceiptCsv(receipt) }); toast.success('Results download requested'); }}>Export results</Button></div>}>
        {!receipt ? <PageState kind="empty" title="No resend results yet" /> : <>
            <p className="ops-message-note">Environment {receipt.environmentId ?? 'Not configured'} · Revision {receipt.revision} · Completed {new Date(receipt.completedAt).toLocaleString()} · Elapsed {((receipt.completedAt - receipt.startedAt) / 1000).toFixed(1)} s</p>
            {unknown > 0 && <PageState kind="partial" title="Some outcomes are unknown" description="A missing, conflicting or unconfirmed response does not prove that consumption failed. Check the Consumer before another attempt; unknown targets are excluded from failed-target selection." />}
            {receipt.error && <p role="alert" className="ops-message-note">{receipt.error}</p>}
            <div className="ops-message-scroll" role="region" aria-label="Per-message resend results" tabIndex={0}><table><thead><tr>{['DLQ request ID', 'Result', 'Consumer / Client', 'Original message', 'Message'].map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
                <tbody>{rows.map(({ request, result, outcome }) => <tr key={request.consumerGroup + ':' + request.messageId}>
                    <th scope="row"><ReceiptValue value={request.messageId} /></th><td className="ops-dlq-result"><span className="ops-dlq-outcome" data-outcome={outcome}>{outcome === 'success' ? 'Succeeded' : outcome === 'failed' ? 'Failed' : 'Unknown'}</span><small>{result?.consumeResult || 'Not confirmed'}</small></td>
                    <td><ReceiptValue value={request.consumerGroup} /><small><ReceiptValue value={request.clientId || 'Selected by Broker'} /></small></td>
                    <td><ReceiptValue value={result?.topic || 'Not reported'} /><small><ReceiptValue value={result?.msgId || 'Not reported'} /></small></td>
                    <td><ReceiptValue value={result?.message || 'No unambiguous result for this target.'} /><small><ReceiptValue value={result?.remark || ''} /></small></td>
                </tr>)}</tbody></table></div>
            <p className="ops-message-note">Review selects only confirmed failed targets visible in the current group and environment. Selection does not send messages.</p>
            {receipt.response && <details className="ops-dlq-reported"><summary>Reported response ({receipt.response.items.length} items)</summary>
                <p className="ops-message-note">Backend totals: {receipt.response.total} requested · {receipt.response.successCount} success · {receipt.response.failureCount} failure. Per-target confirmation above also checks identity and consume outcome.</p>
                <pre tabIndex={0}>{visibleMessageText(JSON.stringify(receipt.response.items.map(({ requestMessageId, consumerGroup, topic, msgId, success, consumeResult, message, remark }) =>
                    ({ requestMessageId, consumerGroup, topic, msgId, success, consumeResult, message, remark })), null, 2))}</pre>
            </details>}
        </>}
    </PageSection>;
}

function ReceiptValue({ value }: { value: string }) {
    return <span className="ops-dlq-value" tabIndex={value.length > 120 ? 0 : undefined}>{visibleMessageText(value)}</span>;
}
