import { useParams, useSearchParams } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import ConsumerDetailContent from './consumers/ConsumerDetailContent';

export default function ConsumerDetailPage() {
  const { group = '' } = useParams();
  const [searchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const initialTab = tabParam === 'progress' || tabParam === 'reset' || tabParam === 'overview'
    ? tabParam
    : 'overview';

  return (
    <div className="entity-workspace consumer-detail-page">
      <PageHeader
        title={group}
        description="Inspect API-backed group identity, queue progress, and protected offset maintenance."
      />
      <ConsumerDetailContent group={group} initialTab={initialTab} />
    </div>
  );
}
