import { useEffect, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import ConsumerDeleteDialog from '../components/ConsumerDeleteDialog';
import EntityDetailPage from '../components/EntityDetailPage';
import ConsumerMutationDialog from '../components/ConsumerMutationDialog';
import { Button } from '../components/ui/Button';
import type { ConsumerGroupListItem, ConsumerSummaryView } from '../types/consumer';
import { useConsumerQueryScope } from './consumers/ConsumerQueryScopeProvider';
import ConsumerDetailContent from './consumers/ConsumerDetailContent';

export default function ConsumerDetailPage() {
  const { group = '' } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { scope } = useConsumerQueryScope();
  const tabParam = searchParams.get('tab');
  const initialTab = tabParam === 'clients' || tabParam === 'progress' || tabParam === 'config' || tabParam === 'reset' || tabParam === 'overview'
    ? tabParam
    : 'overview';
  const [summary, setSummary] = useState<ConsumerSummaryView | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setSummary(null);
    consumerApi.summary(group, scope)
      .then((result) => { if (!cancelled) setSummary(result); })
      .catch(() => { if (!cancelled) setSummary(null); });
    return () => { cancelled = true; };
  }, [group, scope.mode, scope.proxyAddress]);

  const listItem: ConsumerGroupListItem | null = summary ? {
    displayGroupName: summary.displayGroupName,
    rawGroupName: summary.group,
    category: summary.category,
    connectionCount: summary.connectionCount,
    consumeTps: summary.consumeTps,
    diffTotal: summary.diffTotal,
    messageModel: summary.messageModel,
    consumeType: summary.consumeType,
    version: summary.version,
    versionDesc: summary.versionDesc,
    brokerNames: summary.brokerNames,
    brokerAddresses: summary.brokerAddresses,
    updateTimestamp: summary.updateTimestamp
  } : null;

  return (
    <EntityDetailPage
      className="entity-workspace consumer-detail-page"
      title={group}
      description="Inspect API-backed group identity, connections, progress, configuration, and protected offset maintenance."
      backTo="/consumers"
      backLabel="Back to groups"
      actions={<>
        <Button type="button" variant="outline" size="sm" disabled={!listItem} onClick={() => setEditOpen(true)}>Edit configuration</Button>
        <Button type="button" variant="destructive" size="sm" disabled={!listItem} onClick={() => setDeleteOpen(true)}>Delete group</Button>
      </>}
    >
      <ConsumerDetailContent group={group} initialTab={initialTab} />

      <ConsumerMutationDialog
        open={editOpen}
        mode="edit"
        consumer={listItem}
        onOpenChange={setEditOpen}
        onSucceeded={() => { setEditOpen(false); setSummary(null); }}
      />
      <ConsumerDeleteDialog
        open={deleteOpen}
        consumer={listItem}
        onOpenChange={setDeleteOpen}
        onSucceeded={() => navigate('/consumers')}
      />
    </EntityDetailPage>
  );
}
