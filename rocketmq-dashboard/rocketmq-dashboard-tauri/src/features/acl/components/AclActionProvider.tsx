import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { toast } from 'sonner';
import { ConnectionStore } from '../../../services/connection.store';
import { AclService } from '../../../services/acl.service';
import { useOperationOwner } from '../../../hooks/useOperationOwner';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';
import { AclActionContext } from '../aclActionContext';
import { aclWriteReceipt, aclWriteTarget, captureAclAction, type AclAction, type AclContext, type AclReceipt, type AclTarget, type AclWrite } from '../aclActions';
import { policyDraft } from '../policies';
import type { AclPolicyResult, AclScope, AclUserResult } from '../types';
import { AclUserForm } from './AclUserForm';
import { AclPolicyForm } from './AclPolicyForm';
import { AclReceiptPanel, AclTargetDetails } from './AclReceiptPanel';
import '../acl.css';

interface Request { action: AclAction; scope: AclScope; context: AclContext; trigger: HTMLElement | null }
const titles: Record<AclAction['kind'], string> = {
    user_create: 'Create Broker ACL user', user_update: 'Edit Broker ACL user', user_password: 'Change Broker ACL password', user_delete: 'Delete Broker ACL user',
    policy_create: 'Create resource policy', policy_update: 'Edit resource policy', policy_delete: 'Delete policy resource',
};
function dispatch(write: AclWrite): Promise<AclUserResult | AclPolicyResult> {
    switch (write.kind) {
        case 'user_create': return AclService.createUser(write.request);
        case 'user_update': return AclService.updateUser(write.request);
        case 'user_delete': return AclService.deleteUser(write.request.scope, write.request.username);
        case 'policy_create': return AclService.createPolicy(write.request);
        case 'policy_update': return AclService.updatePolicy(write.request);
        case 'policy_delete': return AclService.deletePolicy(write.request);
    }
}
function deletion(request: Request): AclWrite | null {
    if (request.action.kind === 'user_delete') return { kind: 'user_delete', request: { scope: request.scope, username: request.action.user.username } };
    if (request.action.kind === 'policy_delete') return { kind: 'policy_delete', request: request.action.request };
    return null;
}

function AclActionDialog({ request, close, applied }: { request: Request; close: () => void; applied: (receipt: AclReceipt) => void }) {
    const owner = useOperationOwner(request.context.revision, request.context.environmentId);
    const [review, setReview] = useState<AclWrite | null>(() => deletion(request));
    const [submitted, setSubmitted] = useState<AclTarget | null>(null);
    const [receipt, setReceipt] = useState<AclReceipt | null>(null);
    const attempted = useRef(false);
    const active = useRef(true);
    const body = useRef<HTMLDivElement>(null);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    useEffect(() => { body.current?.scrollTo({ top: 0 }); }, [review, submitted, receipt, owner.contextChanged, owner.state.error]);
    const confirm = async () => {
        if (!review || attempted.current || owner.blocked || !owner.controller.isCurrent()) return;
        attempted.current = true;
        const target = aclWriteTarget(review);
        setSubmitted(target);
        const writing = owner.controller.write(() => dispatch(review), 'ACL write was not confirmed. Inspect the original Broker before another operation.');
        setReview(null);
        const result = await writing;
        if (!active.current) return;
        const outcome = aclWriteReceipt(target, request.context, result);
        setReceipt(outcome);
        applied(outcome);
    };
    const action = request.action;
    const target = submitted ?? (review ? aclWriteTarget(review) : null);
    const user = action.kind === 'user_update' || action.kind === 'user_password' ? action.user : null;
    const isDelete = action.kind === 'user_delete' || action.kind === 'policy_delete';
    return <Dialog open onOpenChange={open => { if (!open && !owner.busy) close(); }}>
        <DialogContent className="ops-acl-dialog" showCloseButton={!owner.busy}
            onEscapeKeyDown={event => { if (owner.busy) event.preventDefault(); }} onInteractOutside={event => { if (owner.busy) event.preventDefault(); }}
            onCloseAutoFocus={event => { event.preventDefault(); if (request.trigger?.isConnected) request.trigger.focus(); else document.getElementById('main-content')?.focus(); }}>
            <DialogHeader><DialogTitle>{titles[action.kind]}</DialogTitle><DialogDescription>Review the exact Broker identity and change before applying it.</DialogDescription></DialogHeader>
            <div className="ops-acl-dialog-body" ref={body}>
                <p className="ops-acl-note">Environment {request.context.environmentId ?? 'Not configured'} · Connection revision {request.context.revision}</p>
                {!target && <p className="ops-acl-note">{request.scope.clusterName} / {request.scope.brokerName} · {request.scope.brokerAddr}</p>}
                {owner.contextChanged && <PageState kind="stale" title="Connection context changed" description="This operation remains attached to the original Broker. Reopen from the current scope to start another change." />}
                {owner.state.error && <PageState kind="error" title="ACL operation could not complete" description={owner.state.error} />}
                {!submitted && <div hidden={Boolean(review)}>
                    {(action.kind === 'user_create' || action.kind === 'user_update' || action.kind === 'user_password') && <AclUserForm scope={request.scope} user={user} passwordOnly={action.kind === 'user_password'} disabled={owner.blocked} onReview={setReview} />}
                    {(action.kind === 'policy_create' || action.kind === 'policy_update') && <AclPolicyForm scope={request.scope} policy={action.kind === 'policy_update' ? action.policy : null} initialSubject={action.kind === 'policy_create' ? action.subject ?? '' : ''} disabled={owner.blocked} onReview={setReview} />}
                </div>}
                {target && !receipt && <section className="ops-acl-form" aria-label="ACL change target"><h3>{submitted ? 'Submitted target' : 'Confirm change'}</h3><AclTargetDetails target={target} />
                    {(target.kind === 'user_create' || target.kind === 'user_update') && <p className="ops-acl-note">A new password was provided. Its value is excluded from this confirmation and the receipt.</p>}
                    <p className="ops-acl-note">{isDelete ? 'This removes access at the specified Broker. Verify the exact identity before deletion.' : 'This changes Broker access. Verify permissions and resource decisions before applying.'} The operation will submit once and will not retry automatically.</p>
                </section>}
                {receipt && <AclReceiptPanel receipt={receipt} />}
            </div>
            <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={close}>{receipt ? 'Close result' : 'Cancel'}</Button>
                {review && !isDelete && <Button variant="outline" disabled={owner.blocked} onClick={() => setReview(null)}>Back to edit</Button>}
                {review && <Button variant={isDelete ? 'danger' : 'primary'} disabled={owner.blocked} onClick={() => { void confirm(); }}>{isDelete ? 'Confirm deletion' : 'Apply change'}</Button>}
                {owner.busy && <span role="status" className="ops-acl-note">Applying change…</span>}
            </DialogFooter>
        </DialogContent>
    </Dialog>;
}

/** The session owns accepted writes and their receipt, even if the page or connection changes. */
export function AclActionProvider({ children }: { children: ReactNode }) {
    const [request, setRequest] = useState<Request | null>(null);
    const [receipt, setReceipt] = useState<AclReceipt | null>(null);
    const open = useCallback((action: AclAction, scope: AclScope, context: AclContext) => {
        try {
            const captured = captureAclAction(action, scope, context, ConnectionStore.getSnapshot());
            if (action.kind === 'policy_update') policyDraft(action.policy);
            const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
            setRequest(current => current ?? { ...captured, trigger });
        } catch (error) { toast.error(error instanceof Error ? error.message : 'The ACL action could not be opened.'); }
    }, []);
    return <AclActionContext.Provider value={{ open, receipt }}>{children}
        {request && <AclActionDialog request={request} close={() => setRequest(null)} applied={setReceipt} />}
    </AclActionContext.Provider>;
}
