import React from 'react';
import { Badge } from '../ui/badge';

type StatusTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';

export function StatusBadge({ tone = 'neutral', children }: { tone?: StatusTone; children: React.ReactNode }) {
    return <Badge variant="outline" className="ops-status-badge" data-tone={tone}>{children}</Badge>;
}
