import { useParams } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import ConsumerDetailContent from './consumers/ConsumerDetailContent';

export default function ConsumerDetailPage() {
  const { group = '' } = useParams();

  return (
    <div className="entity-workspace consumer-detail-page">
      <PageHeader
        title={group}
        description="Inspect API-backed group identity, queue progress, and protected offset maintenance."
      />
      <ConsumerDetailContent group={group} />
    </div>
  );
}
