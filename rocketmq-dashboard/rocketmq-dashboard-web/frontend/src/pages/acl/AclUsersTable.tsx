import { Edit3, Trash2 } from 'lucide-react';
import { useMemo } from 'react';
import AppDataTable, { type AppDataTableColumn } from '../../components/AppDataTable';
import ConfirmDialog from '../../components/ConfirmDialog';
import StatusBadge from '../../components/StatusBadge';
import { Button } from '../../components/ui/Button';
import type { AclUserView } from '../../types/acl';

export interface AclUsersTableProps {
  rows: AclUserView[];
  total: number;
  page: number;
  pageSize: number;
  loading: boolean;
  error?: string | null;
  onRetry?: () => void;
  showPasswords: boolean;
  disabled: boolean;
  onPageChange: (page: number) => void;
  onEdit: (user: AclUserView) => void;
  onDelete: (username: string) => void;
}

export default function AclUsersTable({ rows, total, page, pageSize, loading, error, onRetry, showPasswords, disabled, onPageChange, onEdit, onDelete }: AclUsersTableProps) {
  const columns = useMemo<AppDataTableColumn<AclUserView>[]>(() => [
    { id: 'username', header: 'Username', cell: (row) => <div className="acl-primary-cell"><code>{row.username}</code><span>access key</span></div> },
    { id: 'password', header: 'Password', cell: (row) => <code>{row.password ? (showPasswords ? row.password : maskPassword()) : 'Unavailable'}</code> },
    { id: 'type', header: 'User Type', cell: (row) => <StatusBadge status={normalizeUserType(row.userType)} tone={normalizeUserType(row.userType) === 'Super' ? 'warning' : 'neutral'} /> },
    { id: 'status', header: 'User Status', cell: (row) => <StatusBadge status={normalizeUserStatus(row.userStatus)} tone={normalizeUserStatus(row.userStatus) === 'enable' ? 'success' : 'danger'} /> },
    { id: 'broker', header: 'Broker', cell: (row) => <div className="acl-primary-cell"><span>{row.brokerName || '-'}</span><code>{row.brokerAddr || '-'}</code></div> },
    {
      id: 'actions', header: 'Operation', cell: (row) => <div className="acl-operation-row">
        <Button type="button" variant="secondary" className="acl-action-button" aria-label={`Modify ACL user ${row.username}`} disabled={disabled} onClick={() => onEdit(row)}><Edit3 size={14} aria-hidden="true" /> Modify</Button>
        <ConfirmDialog title="Delete ACL user" description={`Delete ACL user ${row.username} from the selected broker target?`} confirmLabel="Delete" onConfirm={() => onDelete(row.username)}>
          <Button type="button" variant="destructive" className="acl-action-button" aria-label={`Delete ACL user ${row.username}`} disabled={disabled}><Trash2 size={14} aria-hidden="true" /> Delete</Button>
        </ConfirmDialog>
      </div>
    }
  ], [disabled, onDelete, onEdit, showPasswords]);

  return <AppDataTable ariaLabel="ACL users" rows={rows} columns={columns} getRowId={(row) => `${row.brokerAddr}:${row.username}`} page={page} pageSize={pageSize} total={total} onPageChange={onPageChange} loading={loading} error={error} onRetry={onRetry} retryLabel="Retry" emptyTitle="No ACL users" />;
}

function maskPassword() { return '************'; }
function normalizeUserType(value?: string) {
  const normalized = (value || 'Normal').toLowerCase();
  if (normalized === 'super') return 'Super';
  if (normalized === 'normal') return 'Normal';
  return value || 'Normal';
}
function normalizeUserStatus(value?: string) { const normalized = (value || 'enable').toLowerCase(); return normalized.includes('disabled') || normalized === 'disable' ? 'disable' : 'enable'; }
