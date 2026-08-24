import { Save } from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState, type FormEvent } from 'react';
import { handleAppliedAuditFailure } from '../api/client';
import type {
  TopicConfigView,
  TopicMutationRequest,
  TopicOperationResult,
  TopicTargetOptionView
} from '../types/topic';
import ErrorState from './ErrorState';
import LoadingState from './LoadingState';
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from './ui/AlertDialog';
import { Button } from './ui/Button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from './ui/Dialog';
import { Input } from './ui/Input';
import { Label } from './ui/Label';

interface TopicMutationDialogProps {
  open: boolean;
  mode: 'create' | 'edit';
  targets: TopicTargetOptionView[];
  config?: TopicConfigView | null;
  loadingConfig?: boolean;
  configError?: string | null;
  onRetryConfig?: () => void;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: TopicMutationRequest) => Promise<TopicOperationResult>;
  onAppliedAuditFailure?: () => Promise<void> | void;
}

interface TopicFormState {
  topic: string;
  clusterNameList: string[];
  brokerNameList: string[];
  readQueueCount: string;
  writeQueueCount: string;
  read: boolean;
  write: boolean;
  inherit: boolean;
  messageType: string;
  ordered: boolean;
}

interface TopicConfirmation {
  request: TopicMutationRequest;
  resolvedBrokers: string[];
  mode: 'create' | 'edit';
  topic: string;
}

const MIN_QUEUE_COUNT = 1;
const MAX_QUEUE_COUNT = 128;

export default function TopicMutationDialog(props: TopicMutationDialogProps) {
  const {
    open,
    onOpenChange,
    onSubmit,
    onAppliedAuditFailure
  } = props;
  const mode = props.mode;
  const targets = props.targets;
  const config = props.config ?? null;
  const loadingConfig = props.loadingConfig ?? false;
  const configError = props.configError ?? null;
  const onRetryConfig = props.onRetryConfig;
  const configTopic = config?.topicName ?? '';
  const [form, setForm] = useState<TopicFormState>(() => toFormState(mode, config));
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<TopicOperationResult | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [confirmation, setConfirmation] = useState<TopicConfirmation | null>(null);
  const pendingRef = useRef<number | null>(null);
  const requestRef = useRef(0);
  const mountedRef = useRef(false);
  const modeRef = useRef(mode);
  const topicRef = useRef('');
  const saveButtonRef = useRef<HTMLButtonElement>(null);
  const formId = useId();
  const clusterNames = useMemo(() => targets.map((target) => target.clusterName), [targets]);
  const brokerNames = useMemo(
    () => Array.from(new Set(targets.flatMap((target) => target.brokerNames))),
    [targets]
  );
  const selectedClusters = canonicalSelection(clusterNames, form.clusterNameList);
  const selectedBrokers = canonicalSelection(brokerNames, form.brokerNameList);
  const submittedClusters = selectedClusters;
  const submittedBrokers = selectedBrokers;
  const resolvedBrokers = resolveBrokers(targets, selectedClusters, selectedBrokers);

  modeRef.current = mode;
  topicRef.current = mode === 'edit' ? config?.topicName ?? '' : form.topic.trim();

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      requestRef.current += 1;
    };
  }, []);

  useEffect(() => {
    requestRef.current += 1;
    setConfirmation(null);
    if (open) {
      setForm(toFormState(mode, config));
      setError(null);
      setResult(null);
      setSubmitting(false);
    }
  }, [configTopic, mode, open]);

  const closeDialog = () => {
    requestRef.current += 1;
    setConfirmation(null);
    onOpenChange(false);
  };

  const reviewChanges = (event?: FormEvent) => {
    event?.preventDefault();
    if (mode === 'edit' && !config) {
      setError('Topic configuration is unavailable.');
      return;
    }
    if (!form.topic.trim()) {
      setError('Topic name cannot be empty.');
      return;
    }
    if (submittedClusters.length === 0 && submittedBrokers.length === 0) {
      setError('Choose at least one cluster or broker target.');
      return;
    }
    if (!isQueueCount(form.readQueueCount) || !isQueueCount(form.writeQueueCount)) {
      setError('Queue counts must be whole numbers from 1 through 128.');
      return;
    }
    if (!form.read && !form.write) {
      setError('Enable Read or Write permission.');
      return;
    }
    setError(null);
    setResult(null);
    const topic = form.topic.trim();
    setConfirmation({
      request: {
        topic,
        readQueueCount: Number(form.readQueueCount),
        writeQueueCount: Number(form.writeQueueCount),
        perm: permissionBits(form),
        brokerNameList: [...submittedBrokers],
        clusterNameList: [...submittedClusters],
        order: form.ordered,
        messageType: form.messageType
      },
      resolvedBrokers: [...resolvedBrokers],
      mode,
      topic
    });
  };

  const focusSave = () => {
    window.setTimeout(() => saveButtonRef.current?.focus(), 0);
  };

  const submit = async () => {
    const snapshot = confirmation;
    if (!snapshot) return;

    if (pendingRef.current !== null) {
      setConfirmation(null);
      setError('A topic save is already in progress.');
      focusSave();
      return;
    }

    const submittedMode = snapshot.mode;
    const submittedTopic = snapshot.topic;
    const requestId = requestRef.current + 1;
    requestRef.current = requestId;
    pendingRef.current = requestId;
    setSubmitting(true);
    setError(null);
    setResult(null);

    try {
      const operationResult = await onSubmit(snapshot.request);
      if (!isCurrentPresentation(requestId, submittedMode, submittedTopic)) return;

      setConfirmation(null);
      if (operationResult.success) {
        onOpenChange(false);
      } else {
        setResult(operationResult);
        focusSave();
      }
    } catch (requestError) {
      if (!isCurrentPresentation(requestId, submittedMode, submittedTopic)) return;

      if (await handleAppliedAuditFailure(requestError, {
        onApplied: () => {
          closeDialog();
          setError(null);
          setResult(null);
        },
        refresh: onAppliedAuditFailure
      })) return;

      setConfirmation(null);
      setError(requestError instanceof Error ? requestError.message : 'Unable to save the topic.');
      focusSave();
    } finally {
      if (pendingRef.current === requestId) pendingRef.current = null;
      if (isCurrentPresentation(requestId, submittedMode, submittedTopic)) setSubmitting(false);
    }
  };

  const isCurrentPresentation = (requestId: number, submittedMode: 'create' | 'edit', submittedTopic: string) => (
    mountedRef.current
    && requestRef.current === requestId
    && modeRef.current === submittedMode
    && topicRef.current === submittedTopic
  );

  const formUnavailable = mode === 'edit' && (loadingConfig || Boolean(configError) || !config);
  const title = mode === 'edit' ? 'Edit topic' : 'Create topic';

  return (
    <>
      <Dialog open={open} onOpenChange={(nextOpen) => { if (!nextOpen) closeDialog(); }}>
        <DialogContent className="entity-mutation-dialog">
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>
              {mode === 'edit'
                ? 'Review broker-backed configuration and choose the targets that should receive these values.'
                : 'Configure queue capacity, placement, permissions, and the message contract.'}
            </DialogDescription>
          </DialogHeader>

          {mode === 'edit' && loadingConfig ? (
            <LoadingState label="Loading topic configuration" />
          ) : mode === 'edit' && configError ? (
            <ErrorState message={configError} onRetry={onRetryConfig} retryLabel="Retry configuration" />
          ) : mode === 'edit' && !config ? (
            <ErrorState message="Topic configuration is unavailable." onRetry={onRetryConfig} retryLabel="Retry configuration" />
          ) : (
            <form noValidate onSubmit={reviewChanges}>
              <div className="form-grid topic-form-grid">
                <div className="field field-wide">
                  <Label htmlFor={`${formId}-topic`}>Topic name</Label>
                  <Input
                    id={`${formId}-topic`}
                    value={form.topic}
                    disabled={mode === 'edit' || submitting}
                    onChange={(event) => setForm((value) => ({ ...value, topic: event.target.value }))}
                  />
                </div>

                <TargetGroup
                  legend="Clusters"
                  names={clusterNames}
                  selected={selectedClusters}
                  disabled={submitting}
                  emptyLabel="No clusters discovered"
                  idPrefix={`${formId}-cluster`}
                  onToggle={(name, checked) => setForm((value) => ({
                    ...value,
                    clusterNameList: toggleSelection(value.clusterNameList, name, checked)
                  }))}
                />
                <TargetGroup
                  legend="Brokers"
                  names={brokerNames}
                  selected={selectedBrokers}
                  disabled={submitting}
                  emptyLabel="No brokers discovered"
                  idPrefix={`${formId}-broker`}
                  onToggle={(name, checked) => setForm((value) => ({
                    ...value,
                    brokerNameList: toggleSelection(value.brokerNameList, name, checked)
                  }))}
                />

                <div className="field">
                  <Label htmlFor={`${formId}-write-queues`}>Write queue count</Label>
                  <Input
                    id={`${formId}-write-queues`}
                    type="number"
                    min={MIN_QUEUE_COUNT}
                    max={MAX_QUEUE_COUNT}
                    step="1"
                    value={form.writeQueueCount}
                    disabled={submitting}
                    onChange={(event) => setForm((value) => ({ ...value, writeQueueCount: event.target.value }))}
                  />
                </div>
                <div className="field">
                  <Label htmlFor={`${formId}-read-queues`}>Read queue count</Label>
                  <Input
                    id={`${formId}-read-queues`}
                    type="number"
                    min={MIN_QUEUE_COUNT}
                    max={MAX_QUEUE_COUNT}
                    step="1"
                    value={form.readQueueCount}
                    disabled={submitting}
                    onChange={(event) => setForm((value) => ({ ...value, readQueueCount: event.target.value }))}
                  />
                </div>

                <fieldset className="field field-wide">
                  <legend className="ui-label">Permissions</legend>
                  <div className="action-row">
                    <PermissionCheck
                      id={`${formId}-read`}
                      label="Read"
                      checked={form.read}
                      disabled={submitting}
                      onChange={(checked) => setForm((value) => ({ ...value, read: checked }))}
                    />
                    <PermissionCheck
                      id={`${formId}-write`}
                      label="Write"
                      checked={form.write}
                      disabled={submitting}
                      onChange={(checked) => setForm((value) => ({ ...value, write: checked }))}
                    />
                    <PermissionCheck
                      id={`${formId}-inherit`}
                      label="Inherit"
                      checked={form.inherit}
                      disabled={submitting}
                      onChange={(checked) => setForm((value) => ({ ...value, inherit: checked }))}
                    />
                  </div>
                </fieldset>

                <div className="field">
                  <Label htmlFor={`${formId}-message-type`}>Message type</Label>
                  <select
                    id={`${formId}-message-type`}
                    className="ui-select-native"
                    value={form.messageType}
                    disabled={submitting}
                    onChange={(event) => setForm((value) => ({ ...value, messageType: event.target.value }))}
                  >
                    <option value="NORMAL">NORMAL</option>
                    <option value="FIFO">FIFO</option>
                    <option value="DELAY">DELAY</option>
                    <option value="TRANSACTION">TRANSACTION</option>
                  </select>
                </div>
                <PermissionCheck
                  id={`${formId}-ordered`}
                  label="Ordered topic"
                  checked={form.ordered}
                  disabled={submitting}
                  onChange={(checked) => setForm((value) => ({ ...value, ordered: checked }))}
                />
              </div>

              {config?.inconsistentFields.length ? (
                <div className="notice notice-warning">
                  Broker configurations disagree: {config.inconsistentFields.join(', ')}
                </div>
              ) : null}
              {error ? <div className="inline-validation" role="alert">{error}</div> : null}
              {result ? <OperationResult result={result} /> : null}

              <DialogFooter>
                <Button type="button" variant="secondary" onClick={closeDialog}>Cancel</Button>
                <Button ref={saveButtonRef} type="submit" loading={submitting}>
                  <Save size={15} aria-hidden="true" /> Save topic
                </Button>
              </DialogFooter>
            </form>
          )}

          {formUnavailable ? (
            <DialogFooter>
              <Button type="button" variant="secondary" onClick={closeDialog}>Cancel</Button>
            </DialogFooter>
          ) : null}
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={confirmation !== null}
        onOpenChange={(nextOpen) => { if (!nextOpen) setConfirmation(null); }}
      >
        <AlertDialogContent>
          <AlertDialogTitle>{confirmation?.mode === 'edit' ? 'Save topic changes?' : 'Create topic?'}</AlertDialogTitle>
          <AlertDialogDescription>
            {confirmation?.mode === 'edit' ? 'Save changes to' : 'Create'} {confirmation?.topic || 'this topic'} for{' '}
            {confirmation?.resolvedBrokers.length ?? 0} broker {confirmation?.resolvedBrokers.length === 1 ? 'target' : 'targets'}
            {confirmation?.request.clusterNameList.length
              ? ` resolved from ${confirmation.request.clusterNameList.join(', ')}`
              : ''}
            {confirmation?.request.brokerNameList.length
              ? ` and selected brokers ${confirmation.request.brokerNameList.join(', ')}`
              : ''}?
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={submitting}>Cancel</AlertDialogCancel>
            <Button
              type="button"
              disabled={submitting}
              onClick={() => void submit()}
            >
              {submitting ? 'Saving' : confirmation?.mode === 'edit' ? 'Save changes' : 'Create topic'}
            </Button>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

interface TargetGroupProps {
  legend: string;
  names: string[];
  selected: string[];
  disabled: boolean;
  emptyLabel: string;
  idPrefix: string;
  onToggle: (name: string, checked: boolean) => void;
}

function TargetGroup({ legend, names, selected, disabled, emptyLabel, idPrefix, onToggle }: TargetGroupProps) {
  return (
    <fieldset className="field">
      <legend className="ui-label">{legend}</legend>
      {names.length === 0 ? <span className="ui-dialog-description">{emptyLabel}</span> : (
        <div className="action-row">
          {names.map((name, index) => (
            <label className="compact-check" htmlFor={`${idPrefix}-${index}`} key={name}>
              <input
                id={`${idPrefix}-${index}`}
                type="checkbox"
                checked={selected.includes(name)}
                disabled={disabled}
                onChange={(event) => onToggle(name, event.target.checked)}
              />
              {name}
            </label>
          ))}
        </div>
      )}
    </fieldset>
  );
}

interface PermissionCheckProps {
  id: string;
  label: string;
  checked: boolean;
  disabled: boolean;
  onChange: (checked: boolean) => void;
}

function PermissionCheck({ id, label, checked, disabled, onChange }: PermissionCheckProps) {
  return (
    <label className="compact-check" htmlFor={id}>
      <input
        id={id}
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(event) => onChange(event.target.checked)}
      />
      {label}
    </label>
  );
}

function OperationResult({ result }: { result: TopicOperationResult }) {
  return (
    <div className={`notice ${result.success ? 'notice-success' : 'notice-danger'}`} role={result.success ? 'status' : 'alert'}>
      <strong>{result.message}</strong>
      <ul>
        {result.targets.map((target) => (
          <li key={target.target}>
            <strong>{target.target}</strong>: <span>{target.message}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function toFormState(mode: 'create' | 'edit', config: TopicConfigView | null): TopicFormState {
  if (mode === 'edit' && config) {
    return {
      topic: config.topicName,
      clusterNameList: config.clusterNameList,
      brokerNameList: config.brokerNameList,
      readQueueCount: String(config.readQueueNums),
      writeQueueCount: String(config.writeQueueNums),
      read: (config.perm & 4) !== 0,
      write: (config.perm & 2) !== 0,
      inherit: (config.perm & 1) !== 0,
      messageType: config.messageType || 'NORMAL',
      ordered: config.order
    };
  }

  return {
    topic: '',
    clusterNameList: [],
    brokerNameList: [],
    readQueueCount: '8',
    writeQueueCount: '8',
    read: true,
    write: true,
    inherit: false,
    messageType: 'NORMAL',
    ordered: false
  };
}

function isQueueCount(value: string) {
  const parsed = Number(value);
  return value.trim() !== ''
    && Number.isInteger(parsed)
    && parsed >= MIN_QUEUE_COUNT
    && parsed <= MAX_QUEUE_COUNT;
}

function permissionBits(form: Pick<TopicFormState, 'read' | 'write' | 'inherit'>) {
  return (form.read ? 4 : 0) | (form.write ? 2 : 0) | (form.inherit ? 1 : 0);
}

function toggleSelection(values: string[], name: string, checked: boolean) {
  return checked
    ? values.includes(name) ? values : [...values, name]
    : values.filter((value) => value !== name);
}

function canonicalSelection(available: string[], selected: string[]) {
  return available.filter((name) => selected.includes(name));
}

function resolveBrokers(
  targets: TopicTargetOptionView[],
  selectedClusters: string[],
  selectedBrokers: string[]
) {
  return Array.from(new Set([
    ...targets
      .filter((target) => selectedClusters.includes(target.clusterName))
      .flatMap((target) => target.brokerNames),
    ...selectedBrokers
  ]));
}
