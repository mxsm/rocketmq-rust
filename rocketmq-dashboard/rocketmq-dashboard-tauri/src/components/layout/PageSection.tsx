import React from 'react';
import { cn } from '../ui/utils';

interface PageSectionProps extends Omit<React.HTMLAttributes<HTMLElement>, 'title'> {
    title: string;
    description?: React.ReactNode;
    action?: React.ReactNode;
}

export function PageSection({ title, description, action, className, children, ...props }: PageSectionProps) {
    const headingId = React.useId();
    return (
        <section {...props} aria-labelledby={headingId} className={cn('ops-page-section', className)}>
            <header className="ops-section-header">
                <div><h2 id={headingId}>{title}</h2>{description && <p>{description}</p>}</div>
                {action && <div className="ops-section-actions">{action}</div>}
            </header>
            {children}
        </section>
    );
}

export function PageToolbar({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
    return <div {...props} className={cn('ops-page-toolbar', className)} />;
}

export function SplitPane({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
    return <div {...props} className={cn('ops-split-pane', className)} />;
}
