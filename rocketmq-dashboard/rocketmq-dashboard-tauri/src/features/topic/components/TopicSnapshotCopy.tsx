import { useEffect, useRef, useState } from 'react';
import { Copy } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';

export function TopicSnapshotCopy({ label, value, compact = false }: { label: string; value: unknown; compact?: boolean }) {
    const text = JSON.stringify(value, null, 2);
    const generation = useRef(0);
    const [result, setResult] = useState('');
    useEffect(() => { generation.current++; setResult(''); return () => { generation.current++; }; }, [text]);
    const copy = async () => {
        const current = generation.current;
        try {
            await navigator.clipboard.writeText(text);
            if (generation.current === current) setResult('Copied');
        } catch {
            if (generation.current === current) setResult('Copy unavailable. Select the snapshot text instead.');
        }
    };
    return <span className="ops-topic-copy"><Button variant="ghost" icon={Copy} aria-label={label} onClick={() => void copy()}>{compact ? 'Copy' : label}</Button>
        {result && <span role="status" className="ops-topic-note">{result}</span>}</span>;
}
