import React from 'react';
import { AlertCircle, CircleHelp, Inbox, LoaderCircle } from 'lucide-react';

type PageStateKind = 'loading' | 'empty' | 'error' | 'partial' | 'stale';

interface PageStateProps {
    kind: PageStateKind;
    title: string;
    description?: React.ReactNode;
    action?: React.ReactNode;
}

const icons = { loading: LoaderCircle, empty: Inbox, error: AlertCircle, partial: CircleHelp, stale: CircleHelp };

export function PageState({ kind, title, description, action }: PageStateProps) {
    const Icon = icons[kind];
    return (
        <div className="ops-page-state" data-kind={kind} role={kind === 'error' ? 'alert' : 'status'}>
            <Icon aria-hidden="true" className={kind === 'loading' ? 'ops-loading-icon' : undefined} />
            <div className="ops-page-state-copy">
                <strong>{title}</strong>
                {description && <div>{description}</div>}
            </div>
            {action && <div className="ops-page-state-action">{action}</div>}
        </div>
    );
}
