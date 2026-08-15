import * as Dialog from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import { useEffect, useState } from 'react';
import SelectMenu from '../../components/SelectMenu';
import { Button } from '../../components/ui/Button';
import type { AclUserUpsertRequest, AclUserView } from '../../types/acl';
import type { AclScope } from './acl-model';

export interface AclUserDialogProps {
  open: boolean;
  user?: AclUserView | null;
  scope: AclScope;
  saving: boolean;
  error?: string | null;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: AclUserUpsertRequest, username?: string) => void;
}

export default function AclUserDialog({ open, user, scope, saving, error, onOpenChange, onSubmit }: AclUserDialogProps) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [userType, setUserType] = useState('Normal');
  const [userStatus, setUserStatus] = useState('enable');

  useEffect(() => {
    if (!open) return;
    setUsername(user?.username ?? '');
    setPassword(user?.password ?? '');
    setUserType(normalizeUserType(user?.userType));
    setUserStatus(normalizeUserStatus(user?.userStatus));
  }, [open, user]);

  const editing = Boolean(user);
  const canSubmit = Boolean(username.trim() && password.trim() && userType && userStatus);

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content acl-dialog">
          <div className="drawer-header">
            <div>
              <Dialog.Title>{editing ? 'Edit User' : 'Add User'}</Dialog.Title>
              <Dialog.Description className="dialog-description">Create or update an ACL user on the confirmed broker.</Dialog.Description>
            </div>
            <Dialog.Close asChild><Button type="button" variant="ghost" size="icon" title="Close" aria-label="Close"><X size={15} aria-hidden="true" /></Button></Dialog.Close>
          </div>
          <div className="acl-dialog-form">
            <label className="acl-field"><span><strong>*</strong> Username</span><input value={username} disabled={editing} onChange={(event) => setUsername(event.target.value)} /></label>
            <label className="acl-field"><span><strong>*</strong> Password</span><input type="password" value={password} onChange={(event) => setPassword(event.target.value)} /></label>
            <div className="acl-dialog-grid">
              <label className="acl-field">
                <span><strong>*</strong> User Type</span>
                <SelectMenu value={userType} options={[{ value: 'Super', label: 'Super' }, { value: 'Normal', label: 'Normal' }]} onChange={setUserType} ariaLabel="Select ACL user type" className="acl-select-menu" />
              </label>
              <label className="acl-field">
                <span><strong>*</strong> User Status</span>
                <SelectMenu value={userStatus} options={[{ value: 'enable', label: 'enable' }, { value: 'disable', label: 'disable' }]} onChange={setUserStatus} ariaLabel="Select ACL user status" className="acl-select-menu" />
              </label>
            </div>
          </div>
          <div className="dialog-actions">
            {error ? <div className="acl-dialog-error" role="alert">{error}</div> : null}
            <Dialog.Close asChild><Button type="button" variant="secondary" disabled={saving}>Cancel</Button></Dialog.Close>
            <Button type="button" disabled={!canSubmit || saving} onClick={() => onSubmit({
              clusterName: scope.clusterName, brokerName: scope.brokerName, username, password, userType, userStatus
            }, user?.username)}>{saving ? 'Saving...' : error ? 'Retry' : 'Confirm'}</Button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function normalizeUserType(value?: string) {
  return (value || 'Normal').toLowerCase() === 'super' ? 'Super' : 'Normal';
}

function normalizeUserStatus(value?: string) {
  const normalized = (value || 'enable').toLowerCase();
  return normalized.includes('disabled') || normalized === 'disable' ? 'disable' : 'enable';
}
