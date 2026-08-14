import { Copy, Cpu, MapPin } from 'lucide-react';
import { useState } from 'react';
import { Button } from '../../components/ui/Button';
import type { ProducerConnectionInfo } from '../../types/producer';

interface ProducerDetailContentProps {
  connection: ProducerConnectionInfo;
  topic: string;
  producerGroup: string;
}

export default function ProducerDetailContent({ connection, topic, producerGroup }: ProducerDetailContentProps) {
  const [copyStatus, setCopyStatus] = useState<string | null>(null);

  const copy = async (value: string, label: string) => {
    try {
      if (!navigator.clipboard) throw new Error('Clipboard API unavailable');
      await navigator.clipboard.writeText(value);
      setCopyStatus(`${label} copied.`);
    } catch {
      setCopyStatus(`Unable to copy ${label.toLowerCase()}.`);
    }
  };

  return (
    <div className="entity-detail-content producer-detail-content">
      <dl className="entity-description-grid producer-client-description">
        <div className="detail-copy-row">
          <dt>Client ID</dt>
          <dd className="mono">{connection.clientId || '-'}</dd>
          <Button type="button" variant="ghost" size="icon" aria-label="Copy client ID" onClick={() => void copy(connection.clientId, 'Client ID')}>
            <Copy size={15} aria-hidden="true" />
          </Button>
        </div>
        <div className="detail-copy-row">
          <dt>Client address</dt>
          <dd className="mono">{connection.clientAddr || '-'}</dd>
          <Button type="button" variant="ghost" size="icon" aria-label="Copy client address" onClick={() => void copy(connection.clientAddr, 'Client address')}>
            <Copy size={15} aria-hidden="true" />
          </Button>
        </div>
        <div><dt>Language</dt><dd><Cpu size={14} aria-hidden="true" /> {connection.language || 'UNKNOWN'}</dd></div>
        <div><dt>Version</dt><dd>{connection.version || 'UNKNOWN'}</dd></div>
        <div><dt>Producer group</dt><dd className="mono">{producerGroup || '-'}</dd></div>
        <div><dt>Topic</dt><dd className="mono"><MapPin size={14} aria-hidden="true" /> {topic || '-'}</dd></div>
      </dl>
      {copyStatus ? <div className="notice notice-neutral" role="status">{copyStatus}</div> : null}
    </div>
  );
}
