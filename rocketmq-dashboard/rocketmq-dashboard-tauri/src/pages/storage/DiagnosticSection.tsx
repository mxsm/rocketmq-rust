import { useId, type ReactNode } from 'react';

export function DiagnosticSection({ title, rows, note, children }: { title: string; rows?: Array<[string, ReactNode]>; note?: ReactNode; children?: ReactNode }) {
    const id = useId();
    return <section className="ops-storage-section" aria-labelledby={id}><h2 id={id}>{title}</h2>
        {rows && <dl>{rows.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>}
        {children}
        {note && <div className="ops-storage-footnote">{note}</div>}
    </section>;
}
