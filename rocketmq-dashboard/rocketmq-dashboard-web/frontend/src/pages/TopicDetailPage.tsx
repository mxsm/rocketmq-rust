import { useParams } from 'react-router-dom';
import EntityDetailPage from '../components/EntityDetailPage';
import TopicDetailContent from './topics/TopicDetailContent';

export default function TopicDetailPage() {
  const { topic = '' } = useParams();

  return (
    <EntityDetailPage
      className="entity-detail-page topic-detail-page"
      title={topic}
      description="Inspect topic offsets, route queues, and API-backed configuration."
      backTo="/topics"
      backLabel="Back to topics"
    >
      <TopicDetailContent topicName={topic} />
    </EntityDetailPage>
  );
}
