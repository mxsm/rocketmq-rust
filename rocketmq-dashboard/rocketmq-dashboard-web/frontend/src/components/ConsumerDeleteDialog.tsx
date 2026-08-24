import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import { handleAppliedAuditFailure, isAppliedAuditFailure } from '../api/client';
import { useConsumerQueryScope } from '../pages/consumers/ConsumerQueryScopeProvider';
import type {
  ConsumerGroupListItem,
  ConsumerOperationIdentity,
  ConsumerOperationResult,
  ConsumerQueryScope
} from '../types/consumer';
import {
  beginConsumerMutation,
  consumerMutationKey,
  finishConsumerMutation,
  isConsumerMutationLocked,
  markConsumerMutationApplied,
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

interface ConsumerDeleteDialogProps {
  open: boolean;
  consumer: ConsumerGroupListItem | null;
  operationIdentity?: ConsumerOperationIdentity | null;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: ConsumerOperationResult) => void;
  onAppliedAuditFailure?: (identity: ConsumerOperationIdentity) => Promise<void> | void;
}

function scopeKey(scope: ConsumerQueryScope) {
  return `${scope.mode}:${scope.proxyAddress ?? ''}`;
}

function sameIdentity(left: ConsumerOperationIdentity, right: ConsumerOperationIdentity) {
  return left.group === right.group
    && left.scopeKey === right.scopeKey
    && left.generation === right.generation;
}

export default function ConsumerDeleteDialog({
  open,
  consumer,
  operationIdentity,
  onOpenChange,
  onSucceeded,
  onAppliedAuditFailure
}: ConsumerDeleteDialogProps) {
  const { scope } = useConsumerQueryScope();
  const currentScopeKey = scopeKey(scope);
  const group = consumer?.rawGroupName ?? '';
  const [brokers, setBrokers] = useState<string[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [confirmation, setConfirmation] = useState('');
  const [loading, setLoading] = useState(false);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ConsumerOperationResult | null>(null);
  const mutationLockKey = consumerMutationKey('delete', group, currentScopeKey);
  const mutationLocked = useConsumerMutationLocked(mutationLockKey);
  const committedOperationRef = useRef<{
    guardKey: string;
    requestGeneration: number;
    open: boolean;
    scopeKey: string;
    targetGroup: string;
    identity: ConsumerOperationIdentity | null;
  }>({ guardKey: '', requestGeneration: 0, open: false, scopeKey: currentScopeKey, targetGroup: '', identity: null });
  const guardKey = [
    String(open),
    group,
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
      targetGroup: group,
      identity: operationIdentity ?? null
    };

  useLayoutEffect(() => {
    committedOperationRef.current = activeOperation;
    setBrokers([]);
    setSelected([]);
    setConfirmation('');
    setError(null);
    setResult(null);
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
          targetGroup: '',
          identity: null
        };
      }
    };
  }, [activeOperation]);

  const isCurrentOperation = (identity: ConsumerOperationIdentity, requestGeneration: number, submittedTarget: string) => {
    const current = committedOperationRef.current;
    return requestGeneration === current.requestGeneration
      && current.open
      && current.scopeKey === identity.scopeKey
      && current.targetGroup === submittedTarget
      && current.targetGroup === group
      && current.targetGroup === identity.group
      && (!current.identity || sameIdentity(current.identity, identity));
  };

  const requestOpenChange = (nextOpen: boolean) => {
    if (!nextOpen && isConsumerMutationLocked(mutationLockKey)) return;
    onOpenChange(nextOpen);
  };

  const preventLockedDismiss = (event: { preventDefault: () => void }) => {
    if (isConsumerMutationLocked(mutationLockKey)) event.preventDefault();
  };

  useEffect(() => {
    if (!open || !group) return;
    const requestGeneration = activeOperation.requestGeneration;
    const identity = activeOperation.identity ?? {
      group: activeOperation.targetGroup,
      scopeKey: currentScopeKey,
      generation: requestGeneration
    };
    const operationTarget = activeOperation.targetGroup;
    if (!isCurrentOperation(identity, requestGeneration, operationTarget)) return;
    let cancelled = false;
    consumerApi
      .brokers(group)
      .then((result) => {
        if (cancelled || !isCurrentOperation(identity, requestGeneration, operationTarget)) return;
        setBrokers(result.items.map((item) => item.brokerName).sort());
        if (!cancelled && isCurrentOperation(identity, requestGeneration, operationTarget)) setReady(true);
      })
      .catch((reason: unknown) => {
        if (!cancelled && isCurrentOperation(identity, requestGeneration, operationTarget)) {
          setError(reason instanceof Error ? reason.message : String(reason));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [open, group, currentScopeKey, operationIdentity, activeOperation]);

  const toggle = (brokerName: string, checked: boolean) => {
    setSelected((current) => checked
      ? Array.from(new Set([...current, brokerName]))
      : current.filter((value) => value !== brokerName));
  };

  const submit = async () => {
    if (selected.length === 0) {
      setError('Select at least one broker target.');
      return;
    }
    if (confirmation.trim() !== group) {
      setError(`Type the exact group name "${group}" to confirm.`);
      return;
    }
    const operation = committedOperationRef.current;
    const requestGeneration = operation.requestGeneration;
    const identity = operation.identity ?? {
      group,
      scopeKey: currentScopeKey,
      generation: requestGeneration
    };
    if (!isCurrentOperation(identity, requestGeneration, group)) return;
    const ticket = beginConsumerMutation('delete', group, currentScopeKey);
    if (!ticket) return;
    setLoading(true);
    setError(null);
    try {
      const result = await consumerApi.delete(group, { brokerNames: selected });
      if (!isCurrentOperation(identity, requestGeneration, group)) return;
      setResult(result);
      const allSucceeded = result.success && result.targets.every((target) => target.success);
      if (allSucceeded) {
        if (!isCurrentOperation(identity, requestGeneration, group)) return;
        onSucceeded(result);
        if (!isCurrentOperation(identity, requestGeneration, group)) return;
        onOpenChange(false);
      }
    } catch (reason) {
      if (isAppliedAuditFailure(reason)) {
        if (markConsumerMutationApplied(ticket)) {
          await handleAppliedAuditFailure(reason, {
            onApplied: () => {
              if (!isCurrentOperation(identity, requestGeneration, group)) return;
              setError(null);
              if (!isCurrentOperation(identity, requestGeneration, group)) return;
              setResult(null);
              if (!isCurrentOperation(identity, requestGeneration, group)) return;
              onOpenChange(false);
            },
            refresh: async () => {
              await onAppliedAuditFailure?.(identity);
            }
          });
        }
        return;
      }
      if (isCurrentOperation(identity, requestGeneration, group)) {
        setError(reason instanceof Error ? reason.message : String(reason));
      }
    } finally {
      finishConsumerMutation(ticket);
      if (isCurrentOperation(identity, requestGeneration, group)) setLoading(false);
    }
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
          <DialogTitle>Delete consumer group</DialogTitle>
          <DialogDescription>
            Delete {group} from selected broker targets. This is destructive and cannot be undone.
          </DialogDescription>
        </DialogHeader>

        <div className="consumer-delete-targets">
          {brokers.map((broker) => (
            <label key={broker} className="compact-check">
              <input
                type="checkbox"
                checked={selected.includes(broker)}
                onChange={(event) => toggle(broker, event.target.checked)}
              />
              {broker}
            </label>
          ))}
        </div>

        <div className="field">
          <label htmlFor="consumer-delete-confirm">Confirm consumer group</label>
          <Input
            id="consumer-delete-confirm"
            value={confirmation}
            onChange={(event) => setConfirmation(event.target.value)}
          />
        </div>

        {result ? (
          <div className="consumer-operation-result" role="status">
            {result.targets.map((target) => (
              <div key={target.target} className={target.success ? 'notice notice-success' : 'notice notice-danger'}>
                <strong>{target.target}</strong> {target.kind} · {target.message}
              </div>
            ))}
          </div>
        ) : null}
        {error ? <div className="inline-validation" role="alert">{error}</div> : null}

        <DialogFooter>
          <Button type="button" variant="outline" disabled={mutationLocked} onClick={() => requestOpenChange(false)}>Cancel</Button>
          <Button type="button" variant="destructive" disabled={mutationLocked || loading || !ready || selected.length === 0 || confirmation.trim() !== group} onClick={() => void submit()}>
            {loading ? 'Deleting' : 'Delete consumer group'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
