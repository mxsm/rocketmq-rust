import { AlertTriangle, RefreshCw } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { configApi } from '../api/config_api';
import { opsApi } from '../api/ops_api';
import { ApiClientError, handleAppliedAuditFailure } from '../api/client';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusBadge from '../components/StatusBadge';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogTitle } from '../components/ui/AlertDialog';
import { Button } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import type { ConfigMutationResult, DashboardConfigView, NameserverAvailabilityView } from '../types/config';
import type { StorageStatusView } from '../types/ops';
import ConnectionSettingsSection from './settings/ConnectionSettingsSection';
import SettingsSectionNav, { type SettingsSection } from './settings/SettingsSectionNav';
import { isNameserverDraftDirty, normalizeNameserverDraft, type NameserverDraft } from './settings/settings-model';

type Notice = { tone: 'success' | 'danger' | 'warning'; message: string };

export default function ConfigPage() {
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [savedDraft, setSavedDraft] = useState<NameserverDraft | null>(null);
  const [nameserverDraft, setNameserverDraft] = useState<NameserverDraft | null>(null);
  const [newNameserver, setNewNameserver] = useState('');
  const [activeSection, setActiveSection] = useState<SettingsSection>('connection');
  const [requestedSection, setRequestedSection] = useState<SettingsSection | null>(null);
  const [nameserverToRemove, setNameserverToRemove] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice | null>(null);
  const [nameserverAvailability, setNameserverAvailability] = useState<NameserverAvailabilityView | null>(null);
  const [availabilityLoading, setAvailabilityLoading] = useState(false);
  const [availabilityError, setAvailabilityError] = useState<string | null>(null);
  const mutationInFlight = useRef(false);
  const availabilityRequest = useRef(0);

  const isDirty = useMemo(
    () => Boolean(nameserverDraft && savedDraft && isNameserverDraftDirty(nameserverDraft, savedDraft)),
    [nameserverDraft, savedDraft]
  );

  const load = (preserveDirtyDraft = false) => {
    if (isDirty && !preserveDirtyDraft) return;
    const draftToPreserve = preserveDirtyDraft && isDirty ? nameserverDraft : null;
    setLoading(true);
    setError(null);
    configApi
      .getConfig()
      .then((nextConfig) => {
        applyConfig(nextConfig, undefined, draftToPreserve);
        void checkNameserverAvailability();
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  function applyConfig(nextConfig: DashboardConfigView, message?: string, preservedDraft?: NameserverDraft | null) {
    const nextDraft = normalizeNameserverDraft({
      namesrvAddrList: nextConfig.namesrvAddrList,
      currentNamesrv: nextConfig.currentNamesrv ?? null
    });
    setConfig(nextConfig);
    setSavedDraft(nextDraft);
    setNameserverDraft(preservedDraft ?? nextDraft);
    if (message) setNotice({ tone: 'success', message });
  }

  async function checkNameserverAvailability() {
    const requestId = ++availabilityRequest.current;
    setAvailabilityLoading(true);
    setAvailabilityError(null);
    try {
      const result = await configApi.getNameserverAvailability();
      if (requestId === availabilityRequest.current) setNameserverAvailability(result);
    } catch (requestError) {
      if (requestId === availabilityRequest.current) {
        setAvailabilityError(requestError instanceof Error ? requestError.message : 'Unable to check NameServer availability.');
      }
    } finally {
      if (requestId === availabilityRequest.current) setAvailabilityLoading(false);
    }
  }

  async function runMutation(mutation: () => Promise<ConfigMutationResult>, onSuccess?: () => void) {
    if (mutationInFlight.current) return;

    mutationInFlight.current = true;
    setPending(true);
    setNotice(null);
    try {
      const result = await mutation();
      applyConfig(result.config, result.message || 'Configuration updated.');
      onSuccess?.();
      window.dispatchEvent(new CustomEvent('rocketmq-config-updated'));
      void checkNameserverAvailability();
    } catch (requestError) {
      if (await handleAppliedAuditFailure(requestError, {
        onApplied: () => {
          setNameserverToRemove(null);
          setNotice({ tone: 'warning', message: 'Configuration change was applied. Refreshing authoritative settings.' });
        },
        refresh: async () => {
          const authoritative = await configApi.getConfig();
          applyConfig(authoritative);
          await checkNameserverAvailability();
        }
      })) return;
      setNotice({ tone: 'danger', message: mutationErrorMessage(requestError, 'Configuration update failed.') });
      if (requestError instanceof ApiClientError && requestError.code === 'STORAGE_CONFLICT') {
        load(true);
      }
    } finally {
      mutationInFlight.current = false;
      setPending(false);
    }
  }

  function requestSectionChange(section: SettingsSection) {
    if (section === activeSection) return;
    if (isDirty) {
      setRequestedSection(section);
      return;
    }
    setActiveSection(section);
  }

  function discardChanges() {
    if (savedDraft) setNameserverDraft(savedDraft);
    if (requestedSection) setActiveSection(requestedSection);
    setRequestedSection(null);
  }

  function addNameserver() {
    if (!config || isDirty || mutationInFlight.current) return;
    const address = newNameserver.trim();
    if (!address) {
      setNotice({ tone: 'warning', message: 'NameServer address is required.' });
      return;
    }
    void runMutation(() => configApi.addNameserver({ address, expectedRevision: config.revision }), () => setNewNameserver(''));
  }

  function saveNameservers() {
    if (!config || !nameserverDraft) return;
    const activeEndpoint = config.endpoints.find((endpoint) => (
      endpoint.endpointType === 'nameserver' && endpoint.address === nameserverDraft.currentNamesrv
    ));
    if (!activeEndpoint) {
      setNotice({ tone: 'warning', message: 'Select a configured NameServer before applying the active endpoint.' });
      return;
    }
    void runMutation(() => configApi.switchNameserver({ endpointId: activeEndpoint.endpointId, expectedRevision: config.revision }));
  }

  function removeNameserver() {
    if (!config || isDirty || !nameserverDraft || !nameserverToRemove) return;
    const endpoint = config.endpoints.find((item) => (
      item.endpointType === 'nameserver' && item.address === nameserverToRemove
    ));
    if (!endpoint) {
      setNotice({ tone: 'warning', message: 'The selected NameServer no longer exists. Refresh before retrying.' });
      return;
    }
    void runMutation(() => configApi.deleteNameserver(endpoint.endpointId, config.revision), () => setNameserverToRemove(null));
  }

  if (loading) return <LoadingState label="Loading OPS settings" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  if (!config || !nameserverDraft) return null;

  const storageBackend = String(config.storageBackend).toLowerCase();

  return (
    <>
      <PageHeader
        title="OPS settings"
        description="Configure NameServer connectivity, transport security, and dashboard storage for this RocketMQ environment."
        actions={<><StatusBadge status={`storage ${storageBackend}`} /><Button type="button" variant="ghost" size="icon" title="Reload OPS settings" aria-label="Reload OPS settings" onClick={() => load(true)} disabled={pending}><RefreshCw className={pending ? 'spin' : undefined} size={15} aria-hidden="true" /></Button></>}
      />
      {notice ? <div className={`notice notice-${notice.tone}`} role={notice.tone === 'danger' ? 'alert' : 'status'}>{notice.message}</div> : null}
      <div className="settings-workspace">
        <SettingsSectionNav activeSection={activeSection} onSelect={requestSectionChange} />
        <div className="settings-section-content">
          {activeSection === 'connection' ? <ConnectionSettingsSection draft={nameserverDraft} savedCurrentNamesrv={savedDraft?.currentNamesrv ?? null} newNameserver={newNameserver} dirty={isDirty} pending={pending} availability={nameserverAvailability} availabilityLoading={availabilityLoading} availabilityError={availabilityError} onDraftChange={setNameserverDraft} onNewNameserverChange={setNewNameserver} onAdd={addNameserver} onRemove={setNameserverToRemove} onSave={saveNameservers} onReset={() => setNameserverDraft(savedDraft)} onCheckAvailability={() => void checkNameserverAvailability()} /> : null}
          {activeSection === 'security' ? <SecuritySection useVIPChannel={config.useVIPChannel} useTLS={config.useTLS} pending={pending} onToggleVip={() => void runMutation(() => configApi.setVipChannel({ enabled: !config.useVIPChannel, expectedRevision: config.revision }))} onToggleTls={() => void runMutation(() => configApi.setTls({ enabled: !config.useTLS, expectedRevision: config.revision }))} /> : null}
          {activeSection === 'storage' ? <StorageSection storageBackend={storageBackend} storageMode={config.storageMode} /> : null}
        </div>
      </div>
      <AlertDialog open={requestedSection !== null} onOpenChange={(open) => !open && setRequestedSection(null)}>
        <AlertDialogContent>
          <AlertDialogTitle className="dialog-title"><AlertTriangle size={18} aria-hidden="true" />Discard unsaved NameServer changes?</AlertDialogTitle>
          <AlertDialogDescription>Your draft NameServer selection has not been persisted. Discard it before changing settings sections?</AlertDialogDescription>
          <div className="dialog-actions"><AlertDialogCancel>Keep editing</AlertDialogCancel><AlertDialogAction onClick={discardChanges}>Discard changes</AlertDialogAction></div>
        </AlertDialogContent>
      </AlertDialog>
      <AlertDialog open={nameserverToRemove !== null} onOpenChange={(open) => !open && setNameserverToRemove(null)}>
        <AlertDialogContent>
          <AlertDialogTitle className="dialog-title"><AlertTriangle size={18} aria-hidden="true" />Remove NameServer {nameserverToRemove}?</AlertDialogTitle>
          <AlertDialogDescription>This removes the endpoint from the configured NameServer list and updates the current endpoint if needed.</AlertDialogDescription>
          <div className="dialog-actions"><AlertDialogCancel>Keep NameServer</AlertDialogCancel><AlertDialogAction onClick={removeNameserver}>Remove NameServer</AlertDialogAction></div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function mutationErrorMessage(error: unknown, fallback: string) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
    return `${error.message} Your draft is still preserved; refresh before retrying.`;
  }
  return error instanceof Error ? error.message : fallback;
}

interface SecuritySectionProps { useVIPChannel: boolean; useTLS: boolean; pending: boolean; onToggleVip: () => void; onToggleTls: () => void; }

function SecuritySection({ useVIPChannel, useTLS, pending, onToggleVip, onToggleTls }: SecuritySectionProps) {
  return <Card className="settings-card"><CardHeader><div><CardTitle>Security</CardTitle><CardDescription>Apply the available transport settings for NameServer and broker connections.</CardDescription></div></CardHeader><CardContent className="settings-card-content settings-toggle-list"><SettingToggle label="VIP channel" description="Prefer the VIP channel for client connections." enabled={useVIPChannel} pending={pending} onClick={onToggleVip} /><SettingToggle label="TLS" description="Enable TLS for dashboard connections." enabled={useTLS} pending={pending} onClick={onToggleTls} /></CardContent></Card>;
}

function SettingToggle({ label, description, enabled, pending, onClick }: { label: string; description: string; enabled: boolean; pending: boolean; onClick: () => void }) {
  const action = `${enabled ? 'Disable' : 'Enable'} ${label}`;
  return <div className="settings-toggle-row"><div><strong>{label}</strong><p>{description}</p></div><Button type="button" variant={enabled ? 'default' : 'outline'} onClick={onClick} disabled={pending} aria-pressed={enabled}>{action}</Button></div>;
}

function StorageSection({ storageBackend, storageMode }: { storageBackend: string; storageMode: DashboardConfigView['storageMode'] }) {
  const [snapshot, setSnapshot] = useState<StorageStatusView | null>(null);
  const [lastCheckedAt, setLastCheckedAt] = useState<number | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const requestSequence = useRef(0);

  const refresh = async () => {
    const requestId = ++requestSequence.current;
    setRefreshing(true);
    setRefreshError(null);
    try {
      const next = await opsApi.getStorageStatus();
      if (requestId === requestSequence.current) {
        setSnapshot(next);
        setLastCheckedAt(Date.now());
      }
    } catch (error) {
      if (requestId === requestSequence.current) {
        setRefreshError(error instanceof Error ? error.message : 'Unable to refresh storage status.');
      }
    } finally {
      if (requestId === requestSequence.current) setRefreshing(false);
    }
  };

  useEffect(() => { void refresh(); }, []);
  const status = snapshot?.status ?? storageBackend;
  const mode = snapshot?.mode ?? storageMode;

  return <Card className="settings-card">
    <CardHeader>
      <div><CardTitle>Storage</CardTitle><CardDescription>Authenticated storage status contains no connection or filesystem location details.</CardDescription></div>
      <Button type="button" variant="outline" size="sm" onClick={() => void refresh()} disabled={refreshing} aria-label="Refresh storage status"><RefreshCw className={refreshing ? 'spin' : undefined} size={14} aria-hidden="true" />Refresh</Button>
    </CardHeader>
    <CardContent className="settings-card-content">
      {refreshError ? <div className="notice notice-warning" role="status">{snapshot ? 'Storage status may be stale. ' : ''}{refreshError}</div> : null}
      <dl className="settings-storage-detail" aria-label="Storage status">
        <div><dt>Availability</dt><dd><StatusBadge status={status} tone={snapshot?.status === 'unavailable' ? 'danger' : snapshot?.status === 'degraded' ? 'warning' : 'success'} /></dd></div>
        <div><dt>Storage backend</dt><dd><StatusBadge status={snapshot?.backend ?? storageBackend} /></dd></div>
        <div><dt>Deployment mode</dt><dd><StatusBadge status={mode === 'multiNode' ? 'multi-node' : 'single-node'} tone={mode === 'multiNode' ? 'info' : 'neutral'} /></dd></div>
        <div><dt>Schema / format version</dt><dd>{snapshot?.schemaOrFormatVersion ?? 'Not reported'}</dd></div>
        <div><dt>Observation started</dt><dd>{formatTimestamp(snapshot?.observationStartedAt)}</dd></div>
        <div><dt>Latest successful write</dt><dd>{formatTimestamp(snapshot?.lastSuccessfulWriteAt)}</dd></div>
        <div><dt>Safe capacity</dt><dd>{formatBytes(snapshot?.safeAvailableBytes)}</dd></div>
        <div><dt>SQL pool</dt><dd>{snapshot?.poolSize === undefined ? 'Not applicable' : `${snapshot.poolSize} total / ${snapshot.idleConnections ?? 0} idle`}</dd></div>
        <div><dt>Last checked</dt><dd>{lastCheckedAt ? formatTimestamp(lastCheckedAt) : 'Checking…'}{refreshError && snapshot ? ' (stale)' : ''}</dd></div>
      </dl>
    </CardContent>
  </Card>;
}

function formatTimestamp(value: number | undefined) {
  return value ? new Date(value).toLocaleString() : 'Not observed in this process';
}

function formatBytes(value: number | undefined) {
  if (value === undefined) return 'Not applicable';
  if (value < 1024) return `${value} B`;
  return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}
