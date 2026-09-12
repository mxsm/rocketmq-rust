import { useEffect, useRef, useState } from 'react';
import { Info } from 'lucide-react';
import { Button } from '../../components/ui/LegacyButton';
import { Input } from '../../components/ui/LegacyInput';
import { PageState } from '../../components/layout/PageState';
import type { MonitorRule } from '../../services/monitor.service';
import { monitorDraftError, reviewMonitorDraft, type MonitorDraft } from './monitorModel';

export const monitorNotice = 'Rules are saved locally for this environment. Alert evaluation and notifications are not enabled.';

export function MonitorEditor({ draft, current, ready, pending, onChange, onSave, onDelete, onCancel }: {
    draft: MonitorDraft; current: MonitorRule | null; ready: boolean; pending: boolean;
    onChange: (draft: MonitorDraft) => void; onSave: () => void; onDelete: () => void; onCancel: () => void;
}) {
    const firstInput = useRef<HTMLInputElement>(null);
    const [showErrors, setShowErrors] = useState(false);
    useEffect(() => { firstInput.current?.focus(); }, [draft.id]);
    const error = monitorDraftError(draft);
    const changed = ready && (draft.expectedRevision === 0 ? current !== null : current?.revision !== draft.expectedRevision);
    const needsReview = draft.needsReview || changed;
    const disabled = pending || !ready;
    return <section className="ops-monitor-editor" aria-labelledby="monitor-editor-heading">
        <h2 id="monitor-editor-heading">{draft.expectedRevision === 0 ? 'New consumer monitor rule' : 'Edit consumer monitor rule'}</h2>
        <p className="ops-monitor-note">Update the threshold configuration for the selected consumer group.</p>
        <form noValidate onSubmit={event => { event.preventDefault(); setShowErrors(true); if (!error && !disabled && !needsReview) onSave(); }}>
            <div className="ops-monitor-fields">
                <Input ref={firstInput} label="Consumer group" required maxLength={255} readOnly={draft.expectedRevision !== 0 || needsReview} disabled={pending}
                    value={draft.consumerGroup} onChange={event => onChange({ ...draft, consumerGroup: event.target.value })} />
                <Input label="Minimum online clients" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" required disabled={pending}
                    value={draft.minCount} onChange={event => onChange({ ...draft, minCount: event.target.value })} />
                <Input label="Maximum lag" type="number" min="0" max={Number.MAX_SAFE_INTEGER} step="1" required disabled={pending}
                    value={draft.maxDiffTotal} onChange={event => onChange({ ...draft, maxDiffTotal: event.target.value })} />
                <Input label="Revision" readOnly value={draft.expectedRevision === 0 ? 'New rule (0)' : draft.expectedRevision} />
            </div>
            {showErrors && error && <PageState kind="error" title="Check the rule values" description={error} />}
            {needsReview && <div className="ops-monitor-review" role="status">
                <h3>Review the current rule before saving</h3>
                <p>Your entered thresholds are retained. Refreshing the list does not change this draft or resubmit it.</p>
                {ready ? <>
                    <p>{current ? `Current stored rule: minimum ${current.minCount}, maximum lag ${current.maxDiffTotal}, revision ${current.revision}.` : 'No rule currently exists for this group.'}</p>
                    <Button variant="outline" disabled={pending} onClick={() => onChange(reviewMonitorDraft(draft, current))}>
                        {current ? `Keep my values and review against revision ${current.revision}` : 'Review my values as a new rule'}
                    </Button>
                </> : <p>Refresh must complete successfully before a new revision can be selected.</p>}
            </div>}
            <div className="ops-monitor-editor-actions">
                <Button type="submit" disabled={disabled || needsReview}>{pending ? 'Applying…' : 'Save changes'}</Button>
                <Button variant="outline" disabled={pending} onClick={onCancel}>Cancel</Button>
                {draft.expectedRevision > 0 && <span className="ops-monitor-danger-action"><Button variant="danger" disabled={disabled || needsReview} onClick={onDelete}>Delete rule</Button></span>}
            </div>
        </form>
        <p className="ops-monitor-notice"><Info aria-hidden="true" size={18} />{monitorNotice}</p>
    </section>;
}
