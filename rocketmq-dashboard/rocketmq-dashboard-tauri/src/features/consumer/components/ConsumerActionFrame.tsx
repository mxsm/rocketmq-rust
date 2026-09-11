import { useEffect, useRef, type ReactNode } from 'react';
import type { OperationOwner } from '../../../hooks/useOperationOwner';
import { PageState } from '../../../components/layout/PageState';

export function ConsumerActionBody({ owner, reveal, children }: { owner: OperationOwner; reveal?: unknown; children: ReactNode }) {
    const body = useRef<HTMLDivElement>(null);
    useEffect(() => { body.current?.scrollTo({ top: 0 }); }, [owner.state.error, owner.contextChanged, reveal]);
    return <div ref={body} className="ops-consumer-dialog-body">
        {owner.contextChanged && <PageState kind="stale" title="Connection or query scope changed" description="The original target and operation results stay here. Close this dialog and review current state before another operation." />}
        {owner.state.error && <PageState kind="error" title="Consumer operation could not complete" description={owner.state.error} />}
        {owner.state.operation === 'read' && <PageState kind="loading" title="Reading current Consumer information" />}{children}
    </div>;
}
