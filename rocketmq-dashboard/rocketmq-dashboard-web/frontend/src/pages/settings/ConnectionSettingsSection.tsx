import {
  AlertTriangle,
  CircleCheck,
  CircleX,
  Database,
  LoaderCircle,
  Plus,
  RefreshCw,
  RotateCcw,
  Save,
  Trash2
} from 'lucide-react';
import { Badge } from '../../components/ui/Badge';
import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Label } from '../../components/ui/Label';
import type { NameserverAvailabilityView, NameserverEndpointAvailability } from '../../types/config';
import type { NameserverDraft } from './settings-model';

interface ConnectionSettingsSectionProps {
  draft: NameserverDraft;
  savedCurrentNamesrv: string | null;
  newNameserver: string;
  dirty: boolean;
  pending: boolean;
  availability: NameserverAvailabilityView | null;
  availabilityLoading: boolean;
  availabilityError: string | null;
  onDraftChange: (draft: NameserverDraft) => void;
  onNewNameserverChange: (value: string) => void;
  onAdd: () => void;
  onRemove: (address: string) => void;
  onSave: () => void;
  onReset: () => void;
  onCheckAvailability: () => void;
}

type DisplayAvailability = NameserverEndpointAvailability['status'] | 'checking';

export default function ConnectionSettingsSection({
  draft,
  savedCurrentNamesrv,
  newNameserver,
  dirty,
  pending,
  availability,
  availabilityLoading,
  availabilityError,
  onDraftChange,
  onNewNameserverChange,
  onAdd,
  onRemove,
  onSave,
  onReset,
  onCheckAvailability
}: ConnectionSettingsSectionProps) {
  const availabilityByAddress = new Map(availability?.endpoints.map((endpoint) => [endpoint.address, endpoint]));
  const currentAvailability = availabilityFor(draft.currentNamesrv, availabilityByAddress, availabilityLoading);

  return (
    <section className="settings-connection-panel" aria-labelledby="settings-connection-title">
      <h2 id="settings-connection-title" className="sr-only">Connection</h2>

      <section className="settings-current-section" aria-labelledby="current-nameserver-heading">
        <h3 id="current-nameserver-heading">Active NameServer</h3>
        <div className="settings-current-nameserver">
          <div className="settings-current-identity">
            <span className="settings-endpoint-icon"><Database size={18} aria-hidden="true" /></span>
            <label>
              <span className="sr-only">Current NameServer</span>
              <select
                aria-label="Current NameServer"
                value={draft.currentNamesrv ?? ''}
                onChange={(event) => onDraftChange({ ...draft, currentNamesrv: event.target.value || null })}
              >
                {draft.namesrvAddrList.length === 0 ? <option value="">No NameServers configured</option> : null}
                {draft.namesrvAddrList.map((address) => <option key={address} value={address}>{address}</option>)}
              </select>
            </label>
            {draft.currentNamesrv ? <Badge tone={dirty ? 'warning' : 'info'}>{dirty ? 'Pending' : 'Current'}</Badge> : null}
            <p>
              {dirty
                ? 'This endpoint is selected in your draft. Apply the change to make it active.'
                : 'Clients and dashboard operations use this endpoint.'}
            </p>
          </div>
          <div className="settings-current-status">
            <AvailabilityState status={currentAvailability.status} />
            <small>{lastCheckedLabel(currentAvailability.endpoint, availabilityLoading)}</small>
          </div>
        </div>
        {dirty ? (
          <div className="settings-current-draft" role="status" aria-live="polite">
            <AlertTriangle size={18} aria-hidden="true" />
            <div className="settings-current-draft-copy">
              <strong>Active endpoint change pending</strong>
              <p>
                <code>{savedCurrentNamesrv ?? 'None'}</code>
                <span aria-hidden="true"> → </span>
                <span className="sr-only"> will change to </span>
                <code>{draft.currentNamesrv ?? 'None'}</code>
              </p>
              <small>Apply or discard this selection before editing the endpoint inventory.</small>
            </div>
            <div className="settings-current-draft-actions">
              <Button type="button" variant="outline" onClick={onReset} disabled={pending}>
                <RotateCcw size={15} aria-hidden="true" />Discard change
              </Button>
              <Button type="button" onClick={onSave} disabled={pending}>
                <Save size={15} aria-hidden="true" />Apply active endpoint
              </Button>
            </div>
          </div>
        ) : null}
      </section>

      <section className="settings-endpoints-section" aria-labelledby="nameserver-endpoints-heading">
        <div className="settings-endpoints-heading">
          <div>
            <h3 id="nameserver-endpoints-heading">NameServer endpoints</h3>
            <p>Configured endpoints and their latest reachability check.</p>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={onCheckAvailability}
            disabled={availabilityLoading || pending}
            aria-label="Check all NameServer endpoints"
          >
            <RefreshCw className={availabilityLoading ? 'spin' : undefined} size={15} aria-hidden="true" />
            {availabilityLoading ? 'Checking' : 'Check all'}
          </Button>
        </div>

        <EndpointLegend />

        {availabilityError ? <div className="settings-availability-error" role="alert">{availabilityError}</div> : null}
        <div
          className="settings-endpoint-table-scroll"
          role="region"
          aria-label="NameServer endpoint availability"
          aria-busy={availabilityLoading}
        >
          <table className="settings-endpoint-table" aria-label="NameServer endpoints">
            <thead>
              <tr>
                <th scope="col">NameServer endpoint</th>
                <th scope="col">Role</th>
                <th scope="col">Availability</th>
                <th scope="col">Last checked</th>
                <th scope="col"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {draft.namesrvAddrList.map((address) => {
                const isCurrent = savedCurrentNamesrv === address;
                const isPending = dirty && draft.currentNamesrv === address;
                const endpointAvailability = availabilityFor(address, availabilityByAddress, availabilityLoading);
                return (
                  <tr key={address}>
                    <td>
                      <span className="settings-endpoint-address">
                        <span className="settings-endpoint-icon"><Database size={16} aria-hidden="true" /></span>
                        <code>{address}</code>
                      </span>
                    </td>
                    <td>
                      <Badge tone={isCurrent ? 'info' : isPending ? 'warning' : 'neutral'}>
                        {isCurrent ? 'Current' : isPending ? 'Pending' : 'Standby'}
                      </Badge>
                    </td>
                    <td><AvailabilityState status={endpointAvailability.status} /></td>
                    <td><span className="settings-last-checked">{lastCheckedLabel(endpointAvailability.endpoint, availabilityLoading)}</span></td>
                    <td className="settings-endpoint-actions">
                      {isCurrent ? (
                        <span className="settings-current-action" aria-label="Current endpoint cannot be removed">—</span>
                      ) : (
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          aria-label={`Remove ${address}`}
                          title={`Remove ${address}`}
                          disabled={pending || dirty}
                          onClick={() => onRemove(address)}
                        >
                          <Trash2 size={15} aria-hidden="true" />
                        </Button>
                      )}
                    </td>
                  </tr>
                );
              })}
              {draft.namesrvAddrList.length === 0 ? (
                <tr><td className="settings-endpoints-empty" colSpan={5}>No NameServer endpoints configured.</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <div className="settings-add-row">
        <div>
          <Label htmlFor="new-nameserver">Add NameServer</Label>
          <p>New endpoints are saved immediately after they are added.</p>
        </div>
        <div className="settings-add-control">
          <Input
            id="new-nameserver"
            value={newNameserver}
            placeholder="Enter host:port, e.g. 10.0.0.1:9876"
            disabled={pending || dirty}
            onChange={(event) => onNewNameserverChange(event.target.value)}
            onKeyDown={(event) => event.key === 'Enter' && onAdd()}
          />
          <Button type="button" onClick={onAdd} disabled={pending || dirty}>
            <Plus size={15} aria-hidden="true" />Add NameServer
          </Button>
        </div>
      </div>

    </section>
  );
}

function EndpointLegend() {
  return (
    <div className="settings-endpoint-legend" aria-label="NameServer endpoint legend">
      <span><Badge tone="info">Current</Badge><small>Active endpoint used by dashboard operations</small></span>
      <span><AvailabilityState status="available" /><small>Endpoint is reachable</small></span>
      <span><AvailabilityState status="unavailable" /><small>Endpoint cannot be reached</small></span>
      <span><AvailabilityState status="checking" /><small>Availability check in progress</small></span>
    </div>
  );
}

function AvailabilityState({ status }: { status: DisplayAvailability }) {
  const Icon = status === 'available' ? CircleCheck : status === 'unavailable' ? CircleX : LoaderCircle;
  const label = status === 'available' ? 'Available' : status === 'unavailable' ? 'Unavailable' : 'Checking';
  return (
    <span className={`settings-availability-state is-${status}`}>
      <Icon className={status === 'checking' ? 'spin' : undefined} size={16} aria-hidden="true" />
      {label}
    </span>
  );
}

function availabilityFor(
  address: string | null,
  availability: Map<string, NameserverEndpointAvailability>,
  loading: boolean
): { status: DisplayAvailability; endpoint?: NameserverEndpointAvailability } {
  if (loading || !address) return { status: 'checking' };
  const endpoint = availability.get(address);
  return { status: endpoint?.status ?? 'unavailable', endpoint };
}

function lastCheckedLabel(endpoint: NameserverEndpointAvailability | undefined, loading: boolean) {
  if (loading) return 'Checking now';
  if (!endpoint) return 'Not checked';
  const elapsedMinutes = Math.max(0, Math.floor((Date.now() - endpoint.checkedAt) / 60_000));
  if (elapsedMinutes < 1) return 'Just now';
  if (elapsedMinutes === 1) return '1 min ago';
  if (elapsedMinutes < 60) return `${elapsedMinutes} min ago`;
  const elapsedHours = Math.floor(elapsedMinutes / 60);
  if (elapsedHours === 1) return '1 hour ago';
  return `${elapsedHours} hours ago`;
}
