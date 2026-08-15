import { Edit3, Trash2 } from 'lucide-react';
import { useMemo } from 'react';
import AppDataTable, { type AppDataTableColumn } from '../../components/AppDataTable';
import ConfirmDialog from '../../components/ConfirmDialog';
import StatusBadge from '../../components/StatusBadge';
import type { AclPolicyRow } from './acl-model';

export interface AclPoliciesTableProps {
  rows: AclPolicyRow[];
  total: number;
  page: number;
  pageSize: number;
  loading: boolean;
  error?: string | null;
  onRetry?: () => void;
  disabled: boolean;
  onPageChange: (page: number) => void;
  onEdit: (policy: AclPolicyRow) => void;
  onDelete: (policy: AclPolicyRow) => void;
}

export default function AclPoliciesTable({ rows, total, page, pageSize, loading, error, onRetry, disabled, onPageChange, onEdit, onDelete }: AclPoliciesTableProps) {
  const columns = useMemo<AppDataTableColumn<AclPolicyRow>[]>(() => [
    { id: 'subject', header: 'Username/Subject', cell: (row) => <div className="acl-primary-cell"><code>{row.subject}</code><span>{row.brokerName || row.brokerAddr}</span></div> },
    { id: 'type', header: 'Policy Type', cell: (row) => <StatusBadge status={row.policyType} /> },
    { id: 'resource', header: 'Resource Name', cell: (row) => <code>{row.resource}</code> },
    { id: 'actions', header: 'Operation Type', cell: (row) => <div className="acl-chip-row">{row.actions.length ? row.actions.map((action) => <span key={action}>{action}</span>) : <span>None</span>}</div> },
    { id: 'sources', header: 'Source IP', cell: (row) => <code>{row.sourceIps.join(', ') || '-'}</code> },
    { id: 'decision', header: 'Decision', cell: (row) => <StatusBadge status={row.decision} tone={row.decision === 'Deny' ? 'danger' : 'success'} /> },
    { id: 'actions-menu', header: 'Operation', cell: (row) => {
      const target = `${row.subject} on ${row.resource}`;
      return <div className="acl-operation-row">
        <button type="button" className="button button-secondary acl-action-button" aria-label={`Modify ACL policy ${target}`} disabled={disabled} onClick={() => onEdit(row)}><Edit3 size={14} aria-hidden="true" /> Modify</button>
        {row.policyType === 'Custom' ? (
          <ConfirmDialog title="Delete ACL permission" description={`Delete ACL permission for ${row.subject} on resource ${row.resource}?`} confirmLabel="Delete" onConfirm={() => onDelete(row)}>
            <button type="button" className="button button-danger acl-action-button" aria-label={`Delete ACL policy ${target}`} disabled={disabled}><Trash2 size={14} aria-hidden="true" /> Delete</button>
          </ConfirmDialog>
        ) : (
          <button type="button" className="button button-danger acl-action-button" aria-label={`Delete ACL policy ${target} unavailable: only Custom policies can be deleted`} title="Only Custom ACL policies can be deleted." disabled><Trash2 size={14} aria-hidden="true" /> Delete</button>
        )}
      </div>;
    } }
  ], [disabled, onDelete, onEdit]);

  return <AppDataTable ariaLabel="ACL policies" rows={rows} columns={columns} getRowId={(row) => row.key} page={page} pageSize={pageSize} total={total} onPageChange={onPageChange} loading={loading} error={error} onRetry={onRetry} retryLabel="Retry" emptyTitle="No ACL permissions" />;
}
