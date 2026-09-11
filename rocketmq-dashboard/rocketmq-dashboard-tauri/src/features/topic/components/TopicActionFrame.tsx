import { useEffect, useRef, type ReactNode } from 'react';
import { PageState } from '../../../components/layout/PageState';
import type { TopicOperationOwner } from '../hooks/useTopicOperation';

export function TopicActionBody({ owner, reveal, children }: { owner: TopicOperationOwner; reveal?: unknown; children: ReactNode }) {
    const body = useRef<HTMLDivElement>(null);
    useEffect(() => { body.current?.scrollTo({ top: 0 }); }, [owner.state.error, owner.contextChanged, reveal]);
    return <div className="ops-topic-action-body" ref={body}>
        {owner.contextChanged && <PageState kind="stale" title="Connection context changed"
            description="The original Topic and completed results remain here. Close this dialog and select a target in the current environment before another operation." />}
        {owner.state.error && <PageState kind="error" title="Topic operation could not complete" description={owner.state.error} />}
        {owner.state.operation === 'read' && <PageState kind="loading" title="Reading current Topic information" />}
        {children}
    </div>;
}
