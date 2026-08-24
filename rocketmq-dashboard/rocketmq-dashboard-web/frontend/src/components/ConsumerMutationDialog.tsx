import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { brokerApi } from '../api/broker_api';
import { handleAppliedAuditFailure, isAppliedAuditFailure } from '../api/client';
import { consumerApi } from '../api/consumer_api';
import { useConsumerQueryScope } from '../pages/consumers/ConsumerQueryScopeProvider';
import type {
  ConsumerGroupListItem,
  ConsumerOperationIdentity,
  ConsumerOperationResult,
  ConsumerQueryScope,
  ConsumerUpsertRequest
} from '../types/consumer';
import {
  beginConsumerMutation,
  completeConsumerMutation,
  consumerMutationKey,
  finishConsumerMutation,
  isConsumerMutationLocked,
  markConsumerMutationApplied,
  useConsumerMutationInFlight,
  useConsumerMutationLocked
} from './consumerMutationLock';
import { Button } from './ui/Button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface ConsumerMutationDialogProps {
  open: boolean;
  mode: 'create' | 'edit';
  consumer?: ConsumerGroupListItem | null;
  operationIdentity?: ConsumerOperationIdentity | null;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: ConsumerOperationResult) => void;
  onAppliedAuditFailure?: (identity: ConsumerOperationIdentity) => Promise<void> | void;
}

const defaultForm = (): Omit<ConsumerUpsertRequest, 'consumerGroup'> => ({
  clusterNameList: [],
  brokerNameList: [],
  consumeEnable: true,
  consumeFromMinEnable: true,
  consumeBroadcastEnable: false,
  consumeMessageOrderly: false,
  retryQueueNums: 1,
  retryMaxTimes: 16,
  brokerId: 0,
  whichBrokerWhenConsumeSlowly: 1,
  notifyConsumerIdsChangedEnable: true,
  groupSysFlag: 0,
  consumeTimeoutMinute: 15
});

function scopeKey(scope: ConsumerQueryScope) {
  return `${scope.mode}:${scope.proxyAddress ?? ''}`;
}

function sameIdentity(left: ConsumerOperationIdentity, right: ConsumerOperationIdentity) {
  return left.group === right.group
    && left.scopeKey === right.scopeKey
    && left.generation === right.generation;
}

export default function ConsumerMutationDialog({
  open,
  mode,
  consumer,
  operationIdentity,
  onOpenChange,
  onSucceeded,
  onAppliedAuditFailure
}: ConsumerMutationDialogProps) {
  const { scope } = useConsumerQueryScope();
  const currentScopeKey = scopeKey(scope);
  const consumerGroup = consumer?.rawGroupName ?? '';
  const targetGroup = mode === 'edit' ? consumerGroup : null;
  const [group, setGroup] = useState(consumer?.rawGroupName ?? '');
  const [form, setForm] = useState(defaultForm());
  const [brokers, setBrokers] = useState<string[]>([]);
  const [ready, setReady] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const mutationKind = mode === 'create' ? 'create' : 'update';
  const mutationTarget = mode === 'create' ? group.trim() : consumerGroup;
  const mutationLockKey = consumerMutationKey(mutationKind, mutationTarget, currentScopeKey);
  const scopedCreateTicket = useConsumerMutationInFlight('create', currentScopeKey);
  const matchingMutationLocked = useConsumerMutationLocked(mutationLockKey);
  const activeTicketRef = useRef<ReturnType<typeof beginConsumerMutation>>(null);
  const activeTicket = mode === 'create' ? scopedCreateTicket : activeTicketRef.current;
  const mutationLocked = mode === 'create'
    ? scopedCreateTicket !== null
    : matchingMutationLocked;
  const committedOperationRef = useRef<{
    guardKey: string;
    requestGeneration: number;
    open: boolean;
    scopeKey: string;
    targetGroup: string | null;
    identity: ConsumerOperationIdentity | null;
  }>({ guardKey: '', requestGeneration: 0, open: false, scopeKey: currentScopeKey, targetGroup: null, identity: null });
  const guardKey = [
    String(open),
    targetGroup ?? '',
    currentScopeKey,
    operationIdentity?.group ?? '',
    operationIdentity?.scopeKey ?? '',
    String(operationIdentity?.generation ?? '')
  ].join('\u0000');

  const committedOperation = committedOperationRef.current;
  const activeOperation = committedOperation.guardKey === guardKey
    ? committedOperation
    : {
      guardKey,
      requestGeneration: committedOperation.requestGeneration + 1,
      open,
      scopeKey: currentScopeKey,
      targetGroup,
      identity: operationIdentity ?? null
    };

  useLayoutEffect(() => {
    committedOperationRef.current = activeOperation;
    setGroup(consumerGroup);
    setForm(defaultForm());
    setBrokers([]);
    setError(null);
    setLoading(false);
    setReady(false);
    return () => {
      if (committedOperationRef.current.guardKey === activeOperation.guardKey
        && committedOperationRef.current.requestGeneration === activeOperation.requestGeneration) {
        committedOperationRef.current = {
          guardKey: '',
          requestGeneration: activeOperation.requestGeneration + 1,
          open: false,
          scopeKey: currentScopeKey,
          targetGroup: null,
          identity: null
        };
      }
    };
  }, [activeOperation]);

  const isCurrentOperation = (identity: ConsumerOperationIdentity, requestGeneration: number, submittedTarget: string) => {
    const current = committedOperationRef.current;
    const immutableTarget = activeTicketRef.current?.targetGroup ?? current.targetGroup;
    return requestGeneration === current.requestGeneration
      && current.open
      && current.scopeKey === identity.scopeKey
      && (!current.targetGroup || current.targetGroup === consumerGroup)
      && (!current.identity || sameIdentity(current.identity, identity))
      && (!immutableTarget || (
        immutableTarget === submittedTarget
        && immutableTarget === identity.group
      ));
  };

  const requestOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && (activeTicket || isConsumerMutationLocked(mutationLockKey))) return;
    onOpenChange(nextOpen);
  };

  const preventLockedDismiss = (event: { preventDefault: () => void }) => {
    if (activeTicket || isConsumerMutationLocked(mutationLockKey)) event.preventDefault();
  };

  useEffect(() => {
    if (!open) return;
    const requestGeneration = activeOperation.requestGeneration;
    const identity = activeOperation.identity ?? {
      group: targetGroup ?? consumerGroup,
      scopeKey: currentScopeKey,
      generation: requestGeneration
    };
    const operationTarget = activeOperation.targetGroup ?? identity.group;
    if (!isCurrentOperation(identity, requestGeneration, operationTarget)) return;
    let cancelled = false;
    void (async () => {
      try {
        const result = await brokerApi.list();
        if (cancelled || !isCurrentOperation(identity, requestGeneration, operationTarget)) return;
        const names = Array.from(new Set(result.items.map((broker) => broker.brokerName))).sort();
        setBrokers(names);
        if (mode === 'edit' && consumerGroup) {
          const config = await consumerApi.config(consumerGroup, scope);
          if (cancelled || !isCurrentOperation(identity, requestGeneration, operationTarget)) return;
          const effective = config.effective;
          if (effective) {
            setForm({
              clusterNameList: [],
              brokerNameList: config.targets
                .filter((target) => target.config !== null)
                .map((target) => target.brokerName),
              consumeEnable: effective.consumeEnable,
              consumeFromMinEnable: effective.consumeFromMinEnable,
              consumeBroadcastEnable: effective.consumeBroadcastEnable,
              consumeMessageOrderly: effective.consumeMessageOrderly,
              retryQueueNums: effective.retryQueueNums,
              retryMaxTimes: effective.retryMaxTimes,
              brokerId: effective.brokerId,
              whichBrokerWhenConsumeSlowly: effective.whichBrokerWhenConsumeSlowly,
              notifyConsumerIdsChangedEnable: effective.notifyConsumerIdsChangedEnable,
              groupSysFlag: effective.groupSysFlag,
              consumeTimeoutMinute: effective.consumeTimeoutMinute
            });
          }
        }
        if (!cancelled && isCurrentOperation(identity, requestGeneration, operationTarget)) setReady(true);
      } catch (reason) {
        if (!cancelled && isCurrentOperation(identity, requestGeneration, operationTarget)) {
          setError(reason instanceof Error ? reason.message : String(reason));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, consumerGroup, mode, currentScopeKey, operationIdentity, activeOperation]);

  const submit = async () => {
    if (mode === 'create' && !group.trim()) {
      setError('Consumer group is required.');
      return;
    }
    if (form.brokerNameList.length === 0) {
      setError('Select at least one broker target.');
      return;
    }
    const submittedGroup = mode === 'create' ? group.trim() : consumerGroup || group;
    const operation = committedOperationRef.current;
    const requestGeneration = operation.requestGeneration;
    const identity = operation.identity ?? {
      group: submittedGroup,
      scopeKey: currentScopeKey,
      generation: requestGeneration
    };
    if (!isCurrentOperation(identity, requestGeneration, submittedGroup)) return;
    const ticket = beginConsumerMutation(mutationKind, submittedGroup, currentScopeKey);
    if (!ticket) return;
    activeTicketRef.current = ticket;
    setLoading(true);
    setError(null);
    try {
      const payload: ConsumerUpsertRequest = {
        ...(mode === 'create' ? { consumerGroup: submittedGroup } : {}),
        ...form
      };
      const result = mode === 'create'
        ? await consumerApi.create(payload)
        : await consumerApi.update(submittedGroup, payload);
      if (mutationKind === 'create') completeConsumerMutation(ticket);
      if (!isCurrentOperation(identity, requestGeneration, submittedGroup)) return;
      onSucceeded(result);
      if (!isCurrentOperation(identity, requestGeneration, submittedGroup)) return;
      onOpenChange(false);
    } catch (reason) {
      if (isAppliedAuditFailure(reason)) {
        if (markConsumerMutationApplied(ticket)) {
          if (mutationKind === 'create') completeConsumerMutation(ticket);
          await handleAppliedAuditFailure(reason, {
            onApplied: () => {
              if (!isCurrentOperation(identity, requestGeneration, submittedGroup)) return;
              setError(null);
              if (!isCurrentOperation(identity, requestGeneration, submittedGroup)) return;
              onOpenChange(false);
            },
            refresh: async () => {
              // The list page owns create invalidation by ticket scope. A
              // detached dialog must not invoke a stale callback that could
              // read or write a newer route. Standalone/current consumers of
              // this component may still provide their local refresh hook.
              if (mutationKind !== 'create' || isCurrentOperation(identity, requestGeneration, submittedGroup)) {
                await onAppliedAuditFailure?.(identity);
              }
            }
          });
        }
        return;
      }
      if (isCurrentOperation(identity, requestGeneration, submittedGroup)) {
        setError(reason instanceof Error ? reason.message : String(reason));
      }
    } finally {
      finishConsumerMutation(ticket);
      if (activeTicketRef.current?.sequence === ticket.sequence) activeTicketRef.current = null;
      if (isCurrentOperation(identity, requestGeneration, submittedGroup)) setLoading(false);
    }
  };

  const setBrokerTarget = (brokerName: string, selected: boolean) => {
    setForm((current) => ({
      ...current,
      brokerNameList: selected
        ? Array.from(new Set([...current.brokerNameList, brokerName]))
        : current.brokerNameList.filter((value) => value !== brokerName)
    }));
  };

  return (
    <Dialog open={open} onOpenChange={requestOpenChange}>
      <DialogContent
        closeDisabled={mutationLocked}
        onEscapeKeyDown={preventLockedDismiss}
        onPointerDownOutside={preventLockedDismiss}
        onInteractOutside={preventLockedDismiss}
      >
        <DialogHeader>
          <DialogTitle>{mode === 'create' ? 'Create consumer group' : `Edit ${consumer?.rawGroupName ?? group}`}</DialogTitle>
          <DialogDescription>
            Configure subscription group targets and broker-side limits.
          </DialogDescription>
        </DialogHeader>

        <div className="consumer-mutation-form">
          {mode === 'create' ? (
            <div className="field">
              <Label htmlFor="consumer-mutation-group">Consumer group</Label>
              <Input id="consumer-mutation-group" value={group} disabled={mutationLocked} onChange={(event) => setGroup(event.target.value)} />
            </div>
          ) : null}

          <div className="field">
            <Label>Broker targets</Label>
            <div className="consumer-target-checkboxes">
              {brokers.map((broker) => (
                <label key={broker} className="compact-check">
                  <input
                    type="checkbox"
                    checked={form.brokerNameList.includes(broker)}
                    onChange={(event) => setBrokerTarget(broker, event.target.checked)}
                  />
                  {broker}
                </label>
              ))}
            </div>
          </div>

          <div className="form-grid consumer-mutation-grid">
            <NumberField label="Retry queue nums" value={form.retryQueueNums} onChange={(value) => setForm((current) => ({ ...current, retryQueueNums: value }))} />
            <NumberField label="Retry max times" value={form.retryMaxTimes} onChange={(value) => setForm((current) => ({ ...current, retryMaxTimes: value }))} />
            <NumberField label="Broker id" value={form.brokerId} onChange={(value) => setForm((current) => ({ ...current, brokerId: value }))} />
            <NumberField label="Slow broker id" value={form.whichBrokerWhenConsumeSlowly} onChange={(value) => setForm((current) => ({ ...current, whichBrokerWhenConsumeSlowly: value }))} />
            <NumberField label="Consume timeout minutes" value={form.consumeTimeoutMinute} onChange={(value) => setForm((current) => ({ ...current, consumeTimeoutMinute: value }))} />
          </div>

          <div className="consumer-mutation-flags">
            <CheckboxField label="Consume enabled" checked={form.consumeEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeEnable: checked }))} />
            <CheckboxField label="Consume from minimum" checked={form.consumeFromMinEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeFromMinEnable: checked }))} />
            <CheckboxField label="Broadcast consume" checked={form.consumeBroadcastEnable} onChange={(checked) => setForm((current) => ({ ...current, consumeBroadcastEnable: checked }))} />
            <CheckboxField label="Orderly consume" checked={form.consumeMessageOrderly} onChange={(checked) => setForm((current) => ({ ...current, consumeMessageOrderly: checked }))} />
          </div>
        </div>

        {error ? <div className="inline-validation" role="alert">{error}</div> : null}

        <DialogFooter>
          <Button type="button" variant="outline" disabled={mutationLocked} onClick={() => requestOpenChange(false)}>Cancel</Button>
          <Button type="button" disabled={mutationLocked || loading || (mode === 'edit' && !ready)} onClick={() => void submit()}>
            {loading ? 'Saving' : mode === 'create' ? 'Create group' : 'Update group'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function NumberField({ label, value, onChange }: { label: string; value: number; onChange: (value: number) => void }) {
  const id = `consumer-${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
  return (
    <div className="field">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="number"
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </div>
  );
}

function CheckboxField({ label, checked, onChange }: { label: string; checked: boolean; onChange: (checked: boolean) => void }) {
  return (
    <label className="compact-check">
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
      {label}
    </label>
  );
}
