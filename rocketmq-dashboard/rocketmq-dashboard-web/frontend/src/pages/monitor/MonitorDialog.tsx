import { Save } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { ApiClientError } from '../../api/client';
import { Button } from '../../components/ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/Dialog';
import { Input } from '../../components/ui/Input';
import { Label } from '../../components/ui/Label';
import type { ConsumerMonitorUpsertRequest, ConsumerMonitorView } from '../../types/monitor';
import { parseConsumerMonitorDraft, type ConsumerMonitorDraft } from './monitor-model';

interface MonitorDialogProps {
  open: boolean;
  environmentId: string;
  rule?: ConsumerMonitorView | null;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: ConsumerMonitorUpsertRequest) => Promise<void>;
  onConflict?: (consumerGroup: string) => Promise<ConsumerMonitorView | null>;
}

const emptyDraft: ConsumerMonitorDraft = {
  consumerGroup: '',
  minCount: '1',
  maxDiffTotal: '1000'
};

export default function MonitorDialog({ open, environmentId, rule, onOpenChange, onSubmit, onConflict }: MonitorDialogProps) {
  const [draft, setDraft] = useState<ConsumerMonitorDraft>(emptyDraft);
  const [validationErrors, setValidationErrors] = useState<Partial<Record<keyof ConsumerMonitorDraft, string>>>({});
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [retryRevision, setRetryRevision] = useState<number | null>(null);
  const [retryRequiresAuthoritativeRule, setRetryRequiresAuthoritativeRule] = useState(false);
  const formId = useId();
  const requestGeneration = useRef(0);
  const preserveDraftOnRuleChange = useRef(false);
  const ruleKey = rule?.consumerGroup;

  useEffect(() => {
    if (!open) {
      requestGeneration.current += 1;
      preserveDraftOnRuleChange.current = false;
      setRetryRevision(null);
      setRetryRequiresAuthoritativeRule(false);
      return;
    }
    // Conflict refreshes update the selected row in the parent. Keep the
    // operator's open draft and its authoritative retry revision intact when
    // that row identity changes from an unselected concurrent create.
    if (preserveDraftOnRuleChange.current || retryRevision !== null || retryRequiresAuthoritativeRule) return;
    requestGeneration.current += 1;
    setDraft(rule ? {
      consumerGroup: rule.consumerGroup,
      minCount: String(rule.minCount),
      maxDiffTotal: String(rule.maxDiffTotal)
    } : emptyDraft);
    setValidationErrors({});
    setSubmitError(null);
    setSubmitting(false);
    setRetryRevision(null);
    setRetryRequiresAuthoritativeRule(false);
  }, [open, retryRequiresAuthoritativeRule, retryRevision, rule, ruleKey]);

  const submit = async () => {
    const parsed = parseConsumerMonitorDraft(draft);
    if (!parsed.ok) {
      setValidationErrors(parsed.errors);
      return;
    }

    const generation = requestGeneration.current;
    if (retryRequiresAuthoritativeRule && retryRevision === null) {
      setSubmitError('The current rule could not be loaded. Your draft is preserved; refresh before retrying.');
      return;
    }
    setValidationErrors({});
    setSubmitError(null);
    setSubmitting(true);
    try {
      await onSubmit({
        ...parsed.value,
        environmentId,
        expectedRevision: retryRevision ?? rule?.revision ?? 0
      });
      if (generation === requestGeneration.current) onOpenChange(false);
    } catch (error) {
      if (generation === requestGeneration.current) {
        if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
          // Set this before awaiting the parent refresh: a concurrent create
          // can synchronously select the authoritative row and otherwise
          // hydrate over the still-open operator draft.
          preserveDraftOnRuleChange.current = true;
          if (!onConflict) {
            setRetryRevision(null);
            setRetryRequiresAuthoritativeRule(true);
            setSubmitError(submitErrorMessage(error));
            return;
          }
          try {
            const authoritative = await onConflict(parsed.value.consumerGroup);
            if (generation !== requestGeneration.current) return;
            if (authoritative) {
              setRetryRevision(authoritative.revision);
              setRetryRequiresAuthoritativeRule(false);
              setSubmitError(`${error.message} The current revision is loaded and your draft is preserved. Reapply your draft, then retry save.`);
            } else {
              setRetryRevision(null);
              setRetryRequiresAuthoritativeRule(true);
              setSubmitError(`${error.message} The rule no longer exists. Your draft is preserved; refresh before retrying as a new rule.`);
            }
          } catch {
            if (generation === requestGeneration.current) {
              setRetryRevision(null);
              setRetryRequiresAuthoritativeRule(true);
              setSubmitError(`${error.message} Your draft is preserved; refresh before retrying.`);
            }
          }
        } else {
          setSubmitError(submitErrorMessage(error));
        }
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
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => void submit()}
              disabled={submitting || (retryRequiresAuthoritativeRule && retryRevision === null)}
            >
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

function submitErrorMessage(error: unknown) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
    return `${error.message} Your draft is still preserved; refresh before retrying.`;
  }
  return error instanceof Error ? error.message : 'Unable to save the monitor rule.';
}
