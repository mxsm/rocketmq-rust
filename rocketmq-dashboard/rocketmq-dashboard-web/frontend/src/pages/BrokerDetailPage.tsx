import { ArrowLeft } from 'lucide-react';
import { Link, useParams } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import { buttonVariants } from '../components/ui/Button';
import BrokerDetailContent from './brokers/BrokerDetailContent';

export default function BrokerDetailPage() {
  const { brokerName = '' } = useParams();

  return (
    <div className="broker-detail-page">
      <PageHeader
        title={brokerName}
        description="Inspect runtime evidence and safely review broker configuration."
        actions={
          <Link className={buttonVariants({ variant: 'outline', size: 'sm' })} to="/brokers">
            <ArrowLeft size={14} aria-hidden="true" /> Back to cluster
          </Link>
        }
      />
      <BrokerDetailContent brokerName={brokerName} />
    </div>
  );
}
