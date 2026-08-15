import * as Dialog from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import { useEffect, useState } from 'react';
import SelectMenu from '../../components/SelectMenu';
import { Button } from '../../components/ui/Button';
import { buildAclPolicyRequest, type AclPolicyDraft, type AclPolicyRow, type AclScope } from './acl-model';
import type { AclPolicyRequest } from '../../types/acl';

export interface AclPolicyDialogProps {
  open: boolean;
  policy?: AclPolicyRow | null;
  scope: AclScope;
  saving: boolean;
  error?: string | null;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: AclPolicyRequest, subject?: string) => void;
}

const actionOptions = ['All', 'Pub', 'Sub', 'Create', 'Update', 'Delete', 'Get', 'List'];
const emptyDraft: AclPolicyDraft = { subject: '', policyType: 'Custom', resources: '', actions: ['Pub', 'Sub'], sourceIps: '', decision: 'Allow' };

export default function AclPolicyDialog({ open, policy, scope, saving, error, onOpenChange, onSubmit }: AclPolicyDialogProps) {
  const [draft, setDraft] = useState<AclPolicyDraft>(emptyDraft);
  const [validationErrors, setValidationErrors] = useState<Partial<Record<keyof AclPolicyDraft, string>>>({});

  useEffect(() => {
    if (!open) return;
    setDraft(policy ? {
      subject: policy.subject,
      policyType: policy.policyType,
      resources: policy.resource,
      actions: [...policy.actions],
      sourceIps: policy.sourceIps.join(', '),
      decision: policy.decision
    } : emptyDraft);
    setValidationErrors({});
  }, [open, policy]);

  const editing = Boolean(policy);
  const updateDraft = <K extends keyof AclPolicyDraft>(field: K, value: AclPolicyDraft[K]) => {
    setDraft((current) => ({ ...current, [field]: value }));
    setValidationErrors((current) => ({ ...current, [field]: undefined }));
  };
  const toggleAction = (action: string) => updateDraft('actions', draft.actions.includes(action)
    ? draft.actions.filter((item) => item !== action)
    : [...draft.actions, action]);
  const submit = () => {
    const parsed = buildAclPolicyRequest(scope, draft, policy);
    if (!parsed.ok) {
      setValidationErrors(parsed.errors);
      return;
    }
    onSubmit(parsed.value, policy?.subject);
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content acl-dialog acl-policy-dialog">
          <div className="drawer-header">
            <div>
              <Dialog.Title>{editing ? 'Edit ACL Permission' : 'Add ACL Permission'}</Dialog.Title>
              <Dialog.Description className="dialog-description">Create or update an ACL policy on the confirmed broker.</Dialog.Description>
            </div>
            <Dialog.Close asChild><Button type="button" variant="ghost" size="icon" title="Close" aria-label="Close"><X size={15} aria-hidden="true" /></Button></Dialog.Close>
          </div>
          <div className="acl-dialog-form">
            <label className="acl-field">
              <span><strong>*</strong> Subject</span>
              <input value={draft.subject} disabled={editing} placeholder="User:rocketmq_app" onChange={(event) => updateDraft('subject', event.target.value)} />
              {validationErrors.subject ? <small className="inline-validation">{validationErrors.subject}</small> : null}
            </label>
            <div className="acl-dialog-grid">
              <label className="acl-field">
                <span><strong>*</strong> Policy Type</span>
                <SelectMenu value={draft.policyType} options={[{ value: 'Custom', label: 'Custom' }, { value: 'Default', label: 'Default' }]} onChange={(value) => updateDraft('policyType', value)} ariaLabel="Select ACL policy type" className="acl-select-menu" />
              </label>
              <label className="acl-field">
                <span><strong>*</strong> Decision</span>
                <SelectMenu value={draft.decision} options={[{ value: 'Allow', label: 'Allow' }, { value: 'Deny', label: 'Deny' }]} onChange={(value) => updateDraft('decision', value)} ariaLabel="Select ACL decision" className="acl-select-menu" />
              </label>
            </div>
            <label className="acl-field">
              <span><strong>*</strong> Resource</span>
              <input value={draft.resources} disabled={editing} placeholder="Topic:TopicTest1111, Group:please_rename_unique_group_name" onChange={(event) => updateDraft('resources', event.target.value)} />
              {validationErrors.resources ? <small className="inline-validation">{validationErrors.resources}</small> : <small>Multiple resources are comma-separated.</small>}
            </label>
            <div className="acl-field">
              <span>Operation Type</span>
              <div className="acl-action-toggle-grid">
                {actionOptions.map((action) => <Button type="button" variant="ghost" className={draft.actions.includes(action) ? 'active' : ''} key={action} aria-pressed={draft.actions.includes(action)} onClick={() => toggleAction(action)}>{action}</Button>)}
              </div>
              {validationErrors.actions ? <small className="inline-validation">{validationErrors.actions}</small> : null}
            </div>
            <label className="acl-field"><span>Source IP</span><input value={draft.sourceIps} placeholder="127.0.0.1, 192.168.1.1" onChange={(event) => updateDraft('sourceIps', event.target.value)} /></label>
          </div>
          <div className="dialog-actions">
            {error ? <div className="acl-dialog-error" role="alert">{error}</div> : null}
            <Dialog.Close asChild><Button type="button" variant="secondary" disabled={saving}>Cancel</Button></Dialog.Close>
            <Button type="button" disabled={saving} onClick={submit}>{saving ? 'Saving...' : error ? 'Retry' : 'Confirm'}</Button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
