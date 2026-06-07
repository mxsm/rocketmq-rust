import { useEffect, useState } from 'react';
import EmptyState from './EmptyState';
import ErrorState from './ErrorState';
import LoadingState from './LoadingState';
import PageHeader from './PageHeader';

interface PendingFeaturePanelProps {
  title: string;
  description: string;
  load: () => Promise<unknown>;
}

export default function PendingFeaturePanel({ title, description, load }: PendingFeaturePanelProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = () => {
    setLoading(true);
    setError(null);
    load()
      .then(() => setError(null))
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    refresh();
  }, []);

  if (loading) return <LoadingState label={`Loading ${title}`} />;

  return (
    <>
      <PageHeader title={title} description={description} />
      {error ? <ErrorState message={error} onRetry={refresh} /> : <EmptyState title="No data" />}
    </>
  );
}
