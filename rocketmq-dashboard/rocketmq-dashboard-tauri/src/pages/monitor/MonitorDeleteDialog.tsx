import { useState } from 'react';
import { Button } from '../../components/ui/LegacyButton';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { PageState } from '../../components/layout/PageState';
import { MonitorTargetDetails } from './MonitorResult';
import type { MonitorTarget } from './monitorModel';

export function MonitorDeleteDialog({ target, pending, current, close, submit }: {
    target: MonitorTarget; pending: boolean; current: boolean; close: () => void; submit: () => Promise<void>;
}) {
    const [attempted, setAttempted] = useState(false);
    return <Dialog open onOpenChange={open => { if (!open && !pending) close(); }}>
        <DialogContent className="ops-monitor-delete-dialog" showCloseButton={!pending}
            onEscapeKeyDown={event => { if (pending) event.preventDefault(); }}
            onInteractOutside={event => { if (pending) event.preventDefault(); }}>
            <DialogHeader><DialogTitle>Delete consumer monitor rule</DialogTitle><DialogDescription>This removes the saved thresholds for the exact group and version shown below.</DialogDescription></DialogHeader>
            <MonitorTargetDetails target={target} />
            {!current && <PageState kind="stale" title="The target changed" description="Close this dialog, refresh and review the current rule before deletion." />}
            <p className="ops-monitor-note">The consumer group and its messages are unaffected. The deletion submits once.</p>
            <DialogFooter><Button variant="outline" disabled={pending} onClick={close}>Cancel</Button>
                <Button variant="danger" disabled={pending || !current || attempted} onClick={() => { setAttempted(true); void submit(); }}>{pending ? 'Deleting…' : 'Confirm deletion'}</Button>
            </DialogFooter>
        </DialogContent>
    </Dialog>;
}
