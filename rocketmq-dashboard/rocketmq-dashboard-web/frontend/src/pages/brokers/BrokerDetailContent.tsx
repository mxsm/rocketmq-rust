import { AlertTriangle, CheckCircle2, Pencil, Save, X } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { brokerApi } from '../../api/broker_api';
import ErrorState from '../../components/ErrorState';
import KeyValueTable from '../../components/KeyValueTable';
import LoadingState from '../../components/LoadingState';
import MetricCard from '../../components/MetricCard';
import StatusBadge from '../../components/StatusBadge';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from '../../components/ui/AlertDialog';
import { Button } from '../../components/ui/Button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/Tabs';
import type { BrokerConfigView, BrokerInfo, BrokerRuntimeStats } from '../../types/broker';

export type BrokerDetailTab = 'overview' | 'runtime' | 'configuration';

interface BrokerDetailContentProps {
  brokerName: string;
  broker?: BrokerInfo;
  initialTab?: BrokerDetailTab;
}

function toRows(entries: Record<string, string> | undefined) {
  return Object.entries(entries ?? {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => ({ key, value }));
}

export default function BrokerDetailContent({ brokerName, broker, initialTab = 'overview' }: BrokerDetailContentProps) {
  const [activeTab, setActiveTab] = useState<BrokerDetailTab>(initialTab);
  const [runtime, setRuntime] = useState<BrokerRuntimeStats | null>(null);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [config, setConfig] = useState<BrokerConfigView | null>(null);
  const [configLoading, setConfigLoading] = useState(false);
  const [configError, setConfigError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [configDraft, setConfigDraft] = useState('{}');
  const [validationError, setValidationError] = useState<string | null>(null);
  const [pendingEntries, setPendingEntries] = useState<Record<string, string> | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const brokerNameRef = useRef(brokerName);
  const runtimeRequestRef = useRef(0);
  const configRequestRef = useRef(0);
  const saveRequestRef = useRef(0);
  brokerNameRef.current = brokerName;

  useEffect(() => {
    runtimeRequestRef.current += 1;
    configRequestRef.current += 1;
    saveRequestRef.current += 1;
    setActiveTab(initialTab);
    setRuntime(null);
    setRuntimeLoading(false);
    setRuntimeError(null);
    setConfig(null);
    setConfigLoading(false);
    setConfigError(null);
    setEditing(false);
    setConfigDraft('{}');
    setValidationError(null);
    setPendingEntries(null);
    setConfirmOpen(false);
    setSaving(false);
    setSaveMessage(null);
    setSaveError(null);
  }, [brokerName, initialTab]);

  const loadRuntime = useCallback(async () => {
    const requestBroker = brokerName;
    const requestId = ++runtimeRequestRef.current;
    setRuntimeLoading(true);
    setRuntimeError(null);
    try {
      const nextRuntime = await brokerApi.runtime(requestBroker);
      if (requestId !== runtimeRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setRuntime(nextRuntime);
    } catch (requestError) {
      if (requestId !== runtimeRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setRuntimeError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === runtimeRequestRef.current && brokerNameRef.current === requestBroker) setRuntimeLoading(false);
    }
  }, [brokerName]);

  const loadConfig = useCallback(async () => {
    const requestBroker = brokerName;
    const requestId = ++configRequestRef.current;
    setConfigLoading(true);
    setConfigError(null);
    try {
      const nextConfig = await brokerApi.config(requestBroker);
      if (requestId !== configRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setConfig(nextConfig);
      setConfigDraft(JSON.stringify(nextConfig.entries, null, 2));
    } catch (requestError) {
      if (requestId !== configRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setConfigError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (requestId === configRequestRef.current && brokerNameRef.current === requestBroker) setConfigLoading(false);
    }
  }, [brokerName]);

  useEffect(() => {
    if (activeTab === 'runtime' && !runtime && !runtimeLoading && !runtimeError) void loadRuntime();
    if (activeTab === 'configuration' && !config && !configLoading && !configError) void loadConfig();
  }, [activeTab, config, configError, configLoading, loadConfig, loadRuntime, runtime, runtimeError, runtimeLoading]);

  const runtimeRows = useMemo(() => toRows(runtime?.entries), [runtime]);
  const configRows = useMemo(() => toRows(config?.entries), [config]);

  const reviewChanges = () => {
    setValidationError(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(configDraft);
    } catch {
      setValidationError('Broker config must be a JSON object.');
      return;
    }

    if (parsed === null || Array.isArray(parsed) || typeof parsed !== 'object') {
      setValidationError('Broker config must be a JSON object.');
      return;
    }

    const parsedEntries = Object.entries(parsed);
    if (parsedEntries.some(([, value]) => typeof value !== 'string')) {
      setValidationError('Broker config values must be strings.');
      return;
    }

    const entries = Object.fromEntries(parsedEntries) as Record<string, string>;
    setPendingEntries(entries);
    setConfirmOpen(true);
  };

  const saveConfiguration = async () => {
    if (!pendingEntries) return;
    const requestBroker = brokerName;
    const requestEntries = pendingEntries;
    const requestId = ++saveRequestRef.current;
    setSaving(true);
    setSaveError(null);
    try {
      await brokerApi.updateConfig(requestBroker, { entries: requestEntries });
      if (requestId !== saveRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setConfig((current) => ({
        brokerName: requestBroker,
        address: current?.address ?? broker?.address ?? '',
        entries: requestEntries
      }));
      setConfigDraft(JSON.stringify(requestEntries, null, 2));
      setEditing(false);
      setConfirmOpen(false);
      setPendingEntries(null);
      setSaveMessage('Configuration updated.');
    } catch (requestError) {
      if (requestId !== saveRequestRef.current || brokerNameRef.current !== requestBroker) return;
      setSaveError(requestError instanceof Error ? requestError.message : String(requestError));
      setConfirmOpen(false);
    } finally {
      if (requestId === saveRequestRef.current && brokerNameRef.current === requestBroker) setSaving(false);
    }
  };

  return (
    <div className="broker-detail-content">
      <Tabs className="broker-detail-tabs" value={activeTab} onValueChange={(value) => setActiveTab(value as BrokerDetailTab)}>
        <TabsList aria-label="Broker detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="runtime">Runtime</TabsTrigger>
          <TabsTrigger value="configuration">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="broker-identity-grid">
            <IdentityItem label="Broker" value={brokerName} mono />
            <IdentityItem label="Cluster" value={broker?.clusterName ?? 'Available from cluster inventory'} />
            <IdentityItem label="Address" value={broker?.address ?? 'Available in runtime data'} mono />
            <IdentityItem label="Version" value={broker?.version || 'Available from cluster inventory'} />
            <IdentityItem label="Broker ID" value={broker ? String(broker.brokerId) : '—'} />
            <IdentityItem label="Role" value={broker?.role ?? 'Unknown'} status />
          </div>
          {broker ? (
            <div className="broker-throughput-grid">
              <MetricCard label="Produce TPS" value={broker.produceTps} detail="Current reported ingress" />
              <MetricCard label="Consume TPS" value={broker.consumeTps} detail="Current reported egress" />
              <MetricCard label="Combined TPS" value={Number((broker.produceTps + broker.consumeTps).toFixed(2))} detail="Produce plus consume" />
            </div>
          ) : null}
        </TabsContent>

        <TabsContent className="broker-detail-fill-tab" value="runtime">
          <section className="broker-detail-section">
            <div className="broker-detail-section-heading">
              <div>
                <h3>Runtime snapshot</h3>
                <p>Live broker counters and process-level values.</p>
              </div>
              {runtime ? <Button type="button" variant="outline" size="sm" onClick={() => void loadRuntime()}>Refresh</Button> : null}
            </div>
            {runtimeLoading ? <LoadingState label="Loading broker runtime" /> : null}
            {!runtimeLoading && runtimeError ? <ErrorState message={runtimeError} onRetry={() => void loadRuntime()} /> : null}
            {!runtimeLoading && !runtimeError && runtime ? <KeyValueTable rows={runtimeRows} emptyTitle="No runtime stats" /> : null}
          </section>
        </TabsContent>

        <TabsContent className="broker-detail-fill-tab" value="configuration">
          <section className="broker-detail-section">
            <div className="broker-detail-section-heading">
              <div>
                <h3>Broker configuration</h3>
                <p>Review the active key/value configuration before applying changes.</p>
              </div>
              {config && !editing ? (
                <Button type="button" variant="outline" size="sm" onClick={() => { setEditing(true); setValidationError(null); setSaveMessage(null); setSaveError(null); }}>
                  <Pencil size={14} aria-hidden="true" /> Edit configuration
                </Button>
              ) : null}
            </div>
            {configLoading ? <LoadingState label="Loading broker configuration" /> : null}
            {!configLoading && configError ? <ErrorState message={configError} onRetry={() => void loadConfig()} /> : null}
            {!configLoading && !configError && config && !editing ? <KeyValueTable rows={configRows} emptyTitle="No broker configuration" /> : null}
            {!configLoading && !configError && config && editing ? (
              <div className="broker-config-editor">
                <label htmlFor="broker-config-json">Broker configuration JSON</label>
                <textarea
                  id="broker-config-json"
                  aria-label="Broker configuration JSON"
                  value={configDraft}
                  onChange={(event) => setConfigDraft(event.target.value)}
                  spellCheck={false}
                />
                {validationError ? <div className="inline-validation" role="status"><AlertTriangle size={15} aria-hidden="true" />{validationError}</div> : null}
                {saveError ? <div className="inline-validation" role="alert"><AlertTriangle size={15} aria-hidden="true" />{saveError}</div> : null}
                <div className="broker-config-actions">
                  <Button type="button" variant="ghost" onClick={() => { setEditing(false); setConfigDraft(JSON.stringify(config.entries, null, 2)); setValidationError(null); setSaveError(null); }}>
                    <X size={14} aria-hidden="true" /> Cancel
                  </Button>
                  <Button type="button" onClick={reviewChanges}>
                    <Save size={14} aria-hidden="true" /> Review changes
                  </Button>
                </div>
              </div>
            ) : null}
            {saveMessage ? <div className="inline-success" role="status"><CheckCircle2 size={15} aria-hidden="true" />{saveMessage}</div> : null}
          </section>
        </TabsContent>
      </Tabs>

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogTitle>Apply broker configuration?</AlertDialogTitle>
          <AlertDialogDescription>
            This writes {pendingEntries ? Object.keys(pendingEntries).length : 0} configuration entries to {brokerName}.
          </AlertDialogDescription>
          <div className="dialog-actions">
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction disabled={saving} onClick={() => void saveConfiguration()}>
              Apply configuration
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function IdentityItem({ label, value, mono = false, status = false }: { label: string; value: string; mono?: boolean; status?: boolean }) {
  return (
    <div className="broker-identity-item">
      <span>{label}</span>
      {status ? <StatusBadge status={value} tone={value === 'Unknown' ? 'neutral' : 'info'} /> : <strong className={mono ? 'mono' : undefined}>{value}</strong>}
    </div>
  );
}
