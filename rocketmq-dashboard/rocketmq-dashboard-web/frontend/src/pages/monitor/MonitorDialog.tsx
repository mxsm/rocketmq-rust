import { Save } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { Button } from '../../components/ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/Dialog';
import { Input } from '../../components/ui/Input';
import { Label } from '../../components/ui/Label';
import type { ConsumerMonitorUpsertRequest, ConsumerMonitorView } from '../../types/monitor';
import { parseConsumerMonitorDraft, type ConsumerMonitorDraft } from './monitor-model';

interface MonitorDialogProps {
  open: boolean;
  rule?: ConsumerMonitorView | null;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: ConsumerMonitorUpsertRequest) => Promise<void>;
}

const emptyDraft: ConsumerMonitorDraft = {
  consumerGroup: '',
  minCount: '1',
  maxDiffTotal: '1000'
};

export default function MonitorDialog({ open, rule, onOpenChange, onSubmit }: MonitorDialogProps) {
  const [draft, setDraft] = useState<ConsumerMonitorDraft>(emptyDraft);
  const [validationErrors, setValidationErrors] = useState<Partial<Record<keyof ConsumerMonitorDraft, string>>>({});
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const formId = useId();
  const requestGeneration = useRef(0);
  const ruleKey = rule?.consumerGroup;

  useEffect(() => {
    requestGeneration.current += 1;
    if (!open) return;
    setDraft(rule ? {
      consumerGroup: rule.consumerGroup,
      minCount: String(rule.minCount),
      maxDiffTotal: String(rule.maxDiffTotal)
    } : emptyDraft);
    setValidationErrors({});
    setSubmitError(null);
    setSubmitting(false);
  }, [open, ruleKey]);

  const submit = async () => {
    const parsed = parseConsumerMonitorDraft(draft);
    if (!parsed.ok) {
      setValidationErrors(parsed.errors);
      return;
    }

    const generation = requestGeneration.current;
    setValidationErrors({});
    setSubmitError(null);
    setSubmitting(true);
    try {
      await onSubmit(parsed.value);
      if (generation === requestGeneration.current) onOpenChange(false);
    } catch (error) {
      if (generation === requestGeneration.current) {
        setSubmitError(error instanceof Error ? error.message : 'Unable to save the monitor rule.');
      }
    } finally {
      if (generation === requestGeneration.current) setSubmitting(false);
    }
  };

  const updateDraft = (field: keyof ConsumerMonitorDraft, value: string) => {
    setDraft((current) => ({ ...current, [field]: value }));
    setValidationErrors((current) => ({ ...current, [field]: undefined }));
    setSubmitError(null);
  };

  const editing = Boolean(rule);
  const title = editing ? 'Edit rule' : 'Create rule';

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="entity-mutation-dialog">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>
            Persist a consumer-group threshold configuration. This workspace does not represent live alerts.
          </DialogDescription>
        </DialogHeader>

        <div className="form-grid monitor-form-grid">
          <div className="field field-wide">
            <Label htmlFor={`${formId}-group`}>Group</Label>
            <Input
              id={`${formId}-group`}
              value={draft.consumerGroup}
              disabled={editing}
              onChange={(event) => updateDraft('consumerGroup', event.target.value)}
            />
            {validationErrors.consumerGroup ? <div className="inline-validation" role="status">{validationErrors.consumerGroup}</div> : null}
          </div>
          <div className="field">
            <Label htmlFor={`${formId}-min-count`}>Min Count</Label>
            <Input
              id={`${formId}-min-count`}
              type="number"
              min="0"
              step="1"
              value={draft.minCount}
              onChange={(event) => updateDraft('minCount', event.target.value)}
            />
            {validationErrors.minCount ? <div className="inline-validation" role="status">{validationErrors.minCount}</div> : null}
          </div>
          <div className="field">
            <Label htmlFor={`${formId}-max-diff-total`}>Max Diff Total</Label>
            <Input
              id={`${formId}-max-diff-total`}
              type="number"
              min="0"
              step="1"
              value={draft.maxDiffTotal}
              onChange={(event) => updateDraft('maxDiffTotal', event.target.value)}
            />
            {validationErrors.maxDiffTotal ? <div className="inline-validation" role="status">{validationErrors.maxDiffTotal}</div> : null}
          </div>
        </div>

        {submitError ? (
          <div className="inline-validation" role="alert">
            <span>{submitError}</span>
            <Button type="button" variant="secondary" size="sm" onClick={() => void submit()} disabled={submitting}>
              Retry save
            </Button>
          </div>
        ) : null}

        <DialogFooter>
          <Button type="button" variant="secondary" onClick={() => onOpenChange(false)} disabled={submitting}>Cancel</Button>
          <Button type="button" onClick={() => void submit()} disabled={submitting}>
            <Save size={15} aria-hidden="true" /> {submitting ? 'Saving' : 'Save rule'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
