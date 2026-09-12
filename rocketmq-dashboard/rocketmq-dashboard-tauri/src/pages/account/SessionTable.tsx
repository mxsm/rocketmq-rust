import { useEffect, useState } from 'react';
import type { SessionView } from '../../services/auth.service';
import { StatusBadge } from '../../components/layout/StatusBadge';
import { sessionLabel, sessionStatus, sessionTimestamp } from './sessionModel';

export function SessionTable({ items, username }: { items: SessionView[]; username: string }) {
    const [, tick] = useState(0);
    useEffect(() => { const timer = window.setInterval(() => tick(value => value + 1), 1000); return () => window.clearInterval(timer); }, []);
    const now = Date.now();
    return <table className="ops-session-table"><caption className="sr-only">Local dashboard sessions for {username}</caption>
        <thead><tr>{['Session', 'Created', 'Expires', 'Last visit', 'Status'].map(label => <th key={label} scope="col">{label}</th>)}</tr></thead>
        <tbody>{items.map(session => {
            const status = sessionStatus(session, now);
            return <tr key={session.id} data-current={session.current === true}>
                <td><div className="ops-session-identity"><span className="ops-session-id">{sessionLabel(session.id)}</span>{session.current === true && <StatusBadge tone="accent">Current</StatusBadge>}</div></td>
                <td>{sessionTimestamp(session.createdAtMs)}</td><td>{sessionTimestamp(session.expiresAtMs)}</td><td>{sessionTimestamp(session.lastSeenAtMs)}</td>
                <td><StatusBadge tone={status.tone}>{status.label}</StatusBadge>{status.label === 'Revoked' && <span className="ops-session-revoked-at">{status.note}</span>}</td>
            </tr>;
        })}</tbody>
    </table>;
}
