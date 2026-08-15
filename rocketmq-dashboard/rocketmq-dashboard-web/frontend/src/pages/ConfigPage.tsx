import { AlertTriangle, Plus, RefreshCw, RotateCcw, Save, Trash2 } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { configApi } from '../api/config_api';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import StatusBadge from '../components/StatusBadge';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogTitle } from '../components/ui/AlertDialog';
import { Button } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import { Input } from '../components/ui/Input';
import { Label } from '../components/ui/Label';
import type { ConfigMutationResult, DashboardConfigView } from '../types/config';
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
  const mutationInFlight = useRef(false);

  const load = () => {
    if (isDirty) return;
    setLoading(true);
    setError(null);
    configApi
      .getConfig()
      .then((nextConfig) => applyConfig(nextConfig))
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const isDirty = useMemo(
    () => Boolean(nameserverDraft && savedDraft && isNameserverDraftDirty(nameserverDraft, savedDraft)),
    [nameserverDraft, savedDraft]
  );

  function applyConfig(nextConfig: DashboardConfigView, message?: string) {
    const nextDraft = normalizeNameserverDraft({
      namesrvAddrList: nextConfig.namesrvAddrList,
      currentNamesrv: nextConfig.currentNamesrv ?? null
    });
    setConfig(nextConfig);
    setSavedDraft(nextDraft);
    setNameserverDraft(nextDraft);
    if (message) setNotice({ tone: 'success', message });
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
    } catch (requestError) {
      setNotice({ tone: 'danger', message: requestError instanceof Error ? requestError.message : 'Configuration update failed.' });
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
    if (isDirty || mutationInFlight.current) return;
    const address = newNameserver.trim();
    if (!address) {
      setNotice({ tone: 'warning', message: 'NameServer address is required.' });
      return;
    }
    void runMutation(() => configApi.addNameserver({ address }), () => setNewNameserver(''));
  }

  function saveNameservers() {
    if (!nameserverDraft) return;
    void runMutation(() => configApi.replaceNameservers(normalizeNameserverDraft(nameserverDraft)));
  }

  function removeNameserver() {
    if (isDirty || !nameserverDraft || !nameserverToRemove) return;
    const namesrvAddrList = nameserverDraft.namesrvAddrList.filter((address) => address !== nameserverToRemove);
    const currentNamesrv = nameserverDraft.currentNamesrv === nameserverToRemove
      ? namesrvAddrList[0] ?? null
      : nameserverDraft.currentNamesrv;
    void runMutation(() => configApi.replaceNameservers({ namesrvAddrList, currentNamesrv }), () => setNameserverToRemove(null));
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
        actions={<><StatusBadge status={`storage ${storageBackend}`} /><Button type="button" variant="ghost" size="icon" title="Reload OPS settings" aria-label="Reload OPS settings" onClick={load} disabled={pending || isDirty}><RefreshCw className={pending ? 'spin' : undefined} size={15} aria-hidden="true" /></Button></>}
      />
      {notice ? <div className={`notice notice-${notice.tone}`} role={notice.tone === 'danger' ? 'alert' : 'status'}>{notice.message}</div> : null}
      <div className="settings-workspace">
        <SettingsSectionNav activeSection={activeSection} onSelect={requestSectionChange} />
        <div className="settings-section-content">
          {activeSection === 'connection' ? <ConnectionSection draft={nameserverDraft} newNameserver={newNameserver} dirty={isDirty} pending={pending} onDraftChange={setNameserverDraft} onNewNameserverChange={setNewNameserver} onAdd={addNameserver} onRemove={setNameserverToRemove} onSave={saveNameservers} onReset={() => setNameserverDraft(savedDraft)} /> : null}
          {activeSection === 'security' ? <SecuritySection useVIPChannel={config.useVIPChannel} useTLS={config.useTLS} pending={pending} onToggleVip={() => void runMutation(() => configApi.setVipChannel({ enabled: !config.useVIPChannel }))} onToggleTls={() => void runMutation(() => configApi.setTls({ enabled: !config.useTLS }))} /> : null}
          {activeSection === 'storage' ? <StorageSection storageBackend={storageBackend} /> : null}
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

interface ConnectionSectionProps {
  draft: NameserverDraft;
  newNameserver: string;
  dirty: boolean;
  pending: boolean;
  onDraftChange: (draft: NameserverDraft) => void;
  onNewNameserverChange: (value: string) => void;
  onAdd: () => void;
  onRemove: (address: string) => void;
  onSave: () => void;
  onReset: () => void;
}

function ConnectionSection({ draft, newNameserver, dirty, pending, onDraftChange, onNewNameserverChange, onAdd, onRemove, onSave, onReset }: ConnectionSectionProps) {
  return <Card className="settings-card"><CardHeader><div><CardTitle>Connection</CardTitle><CardDescription>Choose the NameServer endpoint that the dashboard uses for RocketMQ operations.</CardDescription></div><StatusBadge status={draft.currentNamesrv ?? 'unconfigured'} tone={draft.currentNamesrv ? 'success' : 'warning'} /></CardHeader><CardContent className="settings-card-content"><div className="settings-field-grid"><label className="settings-field"><span>Current NameServer</span><select aria-label="Current NameServer" value={draft.currentNamesrv ?? ''} onChange={(event) => onDraftChange({ ...draft, currentNamesrv: event.target.value || null })}>{draft.namesrvAddrList.length === 0 ? <option value="">No NameServers configured</option> : null}{draft.namesrvAddrList.map((address) => <option key={address} value={address}>{address}</option>)}</select></label><div className="settings-field"><span>Configured NameServers</span><div className="settings-address-list" aria-label="Configured NameServers">{draft.namesrvAddrList.length === 0 ? <span className="settings-empty-value">No endpoints configured</span> : null}{draft.namesrvAddrList.map((address) => <span className="settings-address-item" key={address}><code>{address}</code><Button type="button" variant="ghost" size="icon" aria-label={`Remove ${address}`} title={`Remove ${address}`} disabled={pending || dirty} onClick={() => onRemove(address)}><Trash2 size={14} aria-hidden="true" /></Button></span>)}</div></div></div><div className="settings-add-row"><Label htmlFor="new-nameserver">Add NameServer</Label><div><Input id="new-nameserver" value={newNameserver} placeholder="127.0.0.1:9876" disabled={pending || dirty} onChange={(event) => onNewNameserverChange(event.target.value)} onKeyDown={(event) => event.key === 'Enter' && onAdd()} /><Button type="button" onClick={onAdd} disabled={pending || dirty}><Plus size={15} aria-hidden="true" />Add NameServer</Button></div></div><div className="settings-actions"><span>{dirty ? 'Unsaved NameServer changes' : 'NameServer configuration is current'}</span><div><Button type="button" variant="outline" onClick={onReset} disabled={!dirty || pending}><RotateCcw size={15} aria-hidden="true" />Reset draft</Button><Button type="button" onClick={onSave} disabled={!dirty || pending}><Save size={15} aria-hidden="true" />Save NameServers</Button></div></div></CardContent></Card>;
}

interface SecuritySectionProps { useVIPChannel: boolean; useTLS: boolean; pending: boolean; onToggleVip: () => void; onToggleTls: () => void; }

function SecuritySection({ useVIPChannel, useTLS, pending, onToggleVip, onToggleTls }: SecuritySectionProps) {
  return <Card className="settings-card"><CardHeader><div><CardTitle>Security</CardTitle><CardDescription>Apply the available transport settings for NameServer and broker connections.</CardDescription></div></CardHeader><CardContent className="settings-card-content settings-toggle-list"><SettingToggle label="VIP channel" description="Prefer the VIP channel for client connections." enabled={useVIPChannel} pending={pending} onClick={onToggleVip} /><SettingToggle label="TLS" description="Enable TLS for dashboard connections." enabled={useTLS} pending={pending} onClick={onToggleTls} /></CardContent></Card>;
}

function SettingToggle({ label, description, enabled, pending, onClick }: { label: string; description: string; enabled: boolean; pending: boolean; onClick: () => void }) {
  const action = `${enabled ? 'Disable' : 'Enable'} ${label}`;
  return <div className="settings-toggle-row"><div><strong>{label}</strong><p>{description}</p></div><Button type="button" variant={enabled ? 'default' : 'outline'} onClick={onClick} disabled={pending} aria-pressed={enabled}>{action}</Button></div>;
}

function StorageSection({ storageBackend }: { storageBackend: string }) {
  return <Card className="settings-card"><CardHeader><div><CardTitle>Storage</CardTitle><CardDescription>Persistence is reported by the connected dashboard backend and cannot be changed here.</CardDescription></div></CardHeader><CardContent className="settings-card-content"><dl className="settings-storage-detail"><div><dt>Storage backend</dt><dd><StatusBadge status={storageBackend} /></dd></div><div><dt>Configuration mode</dt><dd>Read-only</dd></div></dl></CardContent></Card>;
}
