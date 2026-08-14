import { useParams } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import TopicDetailContent from './topics/TopicDetailContent';

export default function TopicDetailPage() {
  const { topic = '' } = useParams();

  return (
    <div className="entity-detail-page">
      <PageHeader title={topic} description="Inspect topic offsets, route queues, and API-backed configuration." />
      <TopicDetailContent topicName={topic} />
    </div>
  );
}
