import { CheckCircle2, ShieldCheck } from 'lucide-react';
import SelectMenu from '../../components/SelectMenu';
import { Button } from '../../components/ui/Button';
import type { BrokerInfo } from '../../types/broker';
import { createAclScopeQuery, deriveAclScopeOptions, type AclScope } from './acl-model';

export interface AclScopePickerProps {
  brokers: BrokerInfo[];
  draftScope: AclScope;
  confirmedScope: AclScope | null;
  disabled?: boolean;
  onDraftScopeChange: (scope: AclScope) => void;
  onConfirm: (scope: AclScope) => void;
}

export default function AclScopePicker({
  brokers,
  draftScope,
  confirmedScope,
  disabled = false,
  onDraftScopeChange,
  onConfirm
}: AclScopePickerProps) {
  const options = deriveAclScopeOptions(brokers, draftScope.clusterName);
  const confirmedBroker = confirmedScope
    ? brokers.find((broker) => broker.clusterName === confirmedScope.clusterName && broker.brokerName === confirmedScope.brokerName)
    : undefined;
  const canConfirm = createAclScopeQuery(draftScope, brokers) !== null;

  return (
    <section className="acl-selector-panel" aria-label="ACL scope">
      <div className="acl-selector-grid">
        <label className="acl-field">
          <span><strong>*</strong> Cluster</span>
          <SelectMenu
            value={draftScope.clusterName}
            options={options.clusters}
            onChange={(clusterName) => onDraftScopeChange({ clusterName, brokerName: '' })}
            ariaLabel="Select ACL cluster"
            className="acl-select-menu"
          />
        </label>
        <label className="acl-field">
          <span><strong>*</strong> Broker</span>
          <SelectMenu
            value={draftScope.brokerName}
            options={options.brokers}
            onChange={(brokerName) => onDraftScopeChange({ ...draftScope, brokerName })}
            ariaLabel="Select ACL broker"
            className="acl-select-menu"
          />
        </label>
        <Button
          type="button"
          className="acl-confirm-button"
          disabled={disabled || !canConfirm}
          onClick={() => onConfirm(draftScope)}
        >
          <CheckCircle2 size={15} aria-hidden="true" /> Confirm
        </Button>
      </div>
      <div className="acl-scope-card">
        <ShieldCheck size={18} aria-hidden="true" />
        <div>
          <span>Active broker scope</span>
          <strong>{confirmedScope?.brokerName ?? 'No confirmed scope'}</strong>
          <small>
            {confirmedScope ? `${confirmedScope.clusterName} / ${confirmedBroker?.address ?? '-'}` : 'Confirm a cluster and broker before loading ACL data.'}
          </small>
        </div>
      </div>
    </section>
  );
}
