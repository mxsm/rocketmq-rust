import { ArrowLeft } from 'lucide-react';
import { Link, useParams } from 'react-router-dom';
import { useCallback, useEffect, useRef, useState } from 'react';
import { brokerApi } from '../api/broker_api';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import { buttonVariants } from '../components/ui/Button';
import type { BrokerInfo } from '../types/broker';
import BrokerDetailContent from './brokers/BrokerDetailContent';

export default function BrokerDetailPage() {
  const { brokerName = '' } = useParams();
  const [broker, setBroker] = useState<BrokerInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const requestRef = useRef(0);

  const loadBroker = useCallback(async () => {
    const requestId = ++requestRef.current;
    setLoading(true);
    setError(null);
    setBroker(null);
    try {
      const list = await brokerApi.list();
      if (requestId !== requestRef.current) return;
      setBroker(list.items.find((item) => item.brokerName === brokerName) ?? null);
    } catch (requestError) {
      if (requestId !== requestRef.current) return;
      setError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === requestRef.current) setLoading(false);
    }
  }, [brokerName]);

  useEffect(() => {
    void loadBroker();
    return () => { requestRef.current += 1; };
  }, [loadBroker]);

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
      {loading ? <LoadingState label="Loading broker overview" /> : null}
      {!loading && error ? <ErrorState message={error} onRetry={() => void loadBroker()} /> : null}
      {!loading && !error && !broker ? (
        <ErrorState
          message={`Broker ${brokerName} is not present in the current cluster inventory.`}
          onRetry={() => void loadBroker()}
          retryLabel="Refresh broker inventory"
        />
      ) : null}
      {!loading && !error && broker ? <BrokerDetailContent brokerName={brokerName} broker={broker} /> : null}
    </div>
  );
}
