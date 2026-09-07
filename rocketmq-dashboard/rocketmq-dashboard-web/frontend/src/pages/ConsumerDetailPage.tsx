import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import ConsumerDeleteDialog from '../components/ConsumerDeleteDialog';
import EntityDetailPage from '../components/EntityDetailPage';
import ConsumerMutationDialog from '../components/ConsumerMutationDialog';
import { consumerMutationKey, useConsumerMutationLocked } from '../components/consumerMutationLock';
import { Button } from '../components/ui/Button';
import type { ConsumerGroupListItem, ConsumerOperationIdentity, ConsumerSummaryView } from '../types/consumer';
import { useConsumerQueryScope } from './consumers/ConsumerQueryScopeProvider';
import ConsumerDetailContent from './consumers/ConsumerDetailContent';

function consumerScopeKey(mode: string, proxyAddress: string | undefined) {
  return `${mode}:${proxyAddress ?? ''}`;
}

function sameIdentity(left: ConsumerOperationIdentity, right: ConsumerOperationIdentity) {
  return left.group === right.group
    && left.scopeKey === right.scopeKey
    && left.generation === right.generation;
}

export default function ConsumerDetailPage() {
  const { group = '' } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { scope } = useConsumerQueryScope();
  const scopeKey = consumerScopeKey(scope.mode, scope.proxyAddress);
  const editMutationLocked = useConsumerMutationLocked(consumerMutationKey('update', group, scopeKey));
  const deleteMutationLocked = useConsumerMutationLocked(consumerMutationKey('delete', group, scopeKey));
  const tabParam = searchParams.get('tab');
  const initialTab = tabParam === 'clients' || tabParam === 'progress' || tabParam === 'config' || tabParam === 'reset' || tabParam === 'overview'
    ? tabParam
    : 'overview';
  const [summary, setSummary] = useState<ConsumerSummaryView | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const committedIdentityRef = useRef<ConsumerOperationIdentity | null>(null);

  // Render only derives a candidate. A concurrent render may be discarded,
  // therefore the layout effect is the sole publisher of committed identity.
  const committedIdentity = committedIdentityRef.current;
  const activeIdentity: ConsumerOperationIdentity = committedIdentity
    && committedIdentity.group === group
    && committedIdentity.scopeKey === scopeKey
    ? committedIdentity
    : {
      group,
      scopeKey,
      generation: (committedIdentity?.generation ?? 0) + 1
    };

  const isCurrentIdentity = (identity: ConsumerOperationIdentity) => {
    const current = committedIdentityRef.current;
    return current?.group === identity.group
      && current.scopeKey === identity.scopeKey
      && current.generation === identity.generation;
  };

  useLayoutEffect(() => {
    committedIdentityRef.current = activeIdentity;
    setEditOpen(false);
    setDeleteOpen(false);
    return () => {
      if (committedIdentityRef.current && sameIdentity(committedIdentityRef.current, activeIdentity)) {
        committedIdentityRef.current = {
          ...activeIdentity,
          generation: activeIdentity.generation + 1
        };
      }
    };
  }, [activeIdentity]);

  useEffect(() => {
    const identity = activeIdentity;
    setSummary(null);
    consumerApi.summary(group, scope)
      .then((result) => { if (isCurrentIdentity(identity)) setSummary(result); })
      .catch(() => { if (isCurrentIdentity(identity)) setSummary(null); });
  }, [activeIdentity, scope]);

  const summaryScopeKey = summary
    ? consumerScopeKey(summary.queryScope.mode, summary.queryScope.proxyAddress)
    : null;
  const currentSummary = summary
    && summary.group === group
    && summaryScopeKey === scopeKey
    ? summary
    : null;
  const listItem: ConsumerGroupListItem | null = currentSummary ? {
    displayGroupName: currentSummary.displayGroupName,
    rawGroupName: currentSummary.group,
    category: currentSummary.category,
    connectionCount: currentSummary.connectionCount,
    consumeTps: currentSummary.consumeTps,
    diffTotal: currentSummary.diffTotal,
    messageModel: currentSummary.messageModel,
    consumeType: currentSummary.consumeType,
    version: currentSummary.version,
    versionDesc: currentSummary.versionDesc,
    brokerNames: currentSummary.brokerNames,
    brokerAddresses: currentSummary.brokerAddresses,
    updateTimestamp: currentSummary.updateTimestamp
  } : null;

  return (
    <EntityDetailPage
      className="entity-workspace consumer-detail-page"
      title={group}
      description="Inspect API-backed group identity, connections, progress, configuration, and protected offset maintenance."
      backTo="/consumers"
      backLabel="Back to groups"
      actions={<>
        <Button type="button" variant="outline" size="sm" disabled={!listItem || editMutationLocked} onClick={() => setEditOpen(true)}>Edit configuration</Button>
        <Button type="button" variant="destructive" size="sm" disabled={!listItem || deleteMutationLocked} onClick={() => setDeleteOpen(true)}>Delete group</Button>
      </>}
    >
      <ConsumerDetailContent group={group} initialTab={initialTab} />

      {listItem ? (
        <>
          <ConsumerMutationDialog
            open={editOpen}
            mode="edit"
            consumer={listItem}
            operationIdentity={activeIdentity}
            onOpenChange={setEditOpen}
            onSucceeded={() => { setEditOpen(false); setSummary(null); }}
          />
          <ConsumerDeleteDialog
            open={deleteOpen}
            consumer={listItem}
            operationIdentity={activeIdentity}
            onOpenChange={setDeleteOpen}
            onSucceeded={() => navigate('/consumers')}
          />
        </>
      ) : null}
    </EntityDetailPage>
  );
}
