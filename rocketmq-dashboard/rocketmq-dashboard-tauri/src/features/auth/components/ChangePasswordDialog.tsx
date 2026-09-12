import { useEffect, useRef, useState, type FormEvent } from 'react';
import { toast } from 'sonner';
import { useAuth } from '../hooks/useAuth';
import { useAppStore } from '../../../stores/app.store';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { PageState } from '../../../components/layout/PageState';

interface ChangePasswordDialogProps {
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
    onPasswordChanged?: () => void;
}
export function ChangePasswordDialog({ open = false, onOpenChange, onPasswordChanged }: ChangePasswordDialogProps) {
    const [oldPassword, setOldPassword] = useState('');
    const [newPassword, setNewPassword] = useState('');
    const [confirmation, setConfirmation] = useState('');
    const [operation, setOperation] = useState<'change' | 'logout' | null>(null);
    const inFlight = useRef(false);
    const { currentUser, isLoggedIn, mustChangePassword } = useAppStore();
    const { changePassword, clearError, error, isLoading, logout } = useAuth();
    const required = isLoggedIn && mustChangePassword;
    const isOpen = required || open;
    const busy = isLoading || operation !== null;
    const clearPasswords = () => { setOldPassword(''); setNewPassword(''); setConfirmation(''); };
    const close = () => {
        if (required || inFlight.current) return;
        clearError(); clearPasswords(); onOpenChange?.(false);
    };
    useEffect(() => { if (!isOpen) { clearError(); clearPasswords(); } }, [isOpen]);
    const mismatch = confirmation && confirmation !== newPassword ? 'New password confirmation does not match.' : '';
    const submit = async (event: FormEvent) => {
        event.preventDefault();
        if (inFlight.current || !oldPassword || newPassword.length < 8 || confirmation !== newPassword) return;
        inFlight.current = true; setOperation('change');
        try {
            const result = await changePassword({ oldPassword, newPassword });
            if (result.success) {
                clearPasswords();
                toast.success('Password updated. Sign in again with your new password.');
                onPasswordChanged?.(); onOpenChange?.(false);
            }
        } finally { inFlight.current = false; setOperation(null); }
    };
    const signOut = async () => {
        if (inFlight.current) return;
        inFlight.current = true; setOperation('logout');
        try { await logout(); clearPasswords(); }
        finally { inFlight.current = false; setOperation(null); }
    };
    return <Dialog open={isOpen} onOpenChange={next => { if (!next) close(); }}>
        <DialogContent showCloseButton={!required && !busy}
            onEscapeKeyDown={event => { if (required || inFlight.current) event.preventDefault(); }}
            onInteractOutside={event => { if (required || inFlight.current) event.preventDefault(); }}>
            <DialogHeader><DialogTitle>{required ? 'Change initial password' : 'Change password'}</DialogTitle>
                <DialogDescription>{required ? `${currentUser?.username ?? 'This account'} must update the initial password before entering the dashboard.` : 'Changing your password signs out all sessions, including this one.'}</DialogDescription>
            </DialogHeader>
            <form className="ops-auth-password-form" onSubmit={submit}>
                <Input label="Current password" type="password" autoComplete="current-password" required disabled={busy} value={oldPassword} onChange={event => setOldPassword(event.target.value)} />
                <Input label="New password" type="password" autoComplete="new-password" required minLength={8} placeholder="At least 8 characters" disabled={busy} value={newPassword} onChange={event => setNewPassword(event.target.value)} />
                <Input label="Confirm new password" type="password" autoComplete="new-password" required minLength={8} disabled={busy} value={confirmation} error={mismatch || undefined} onChange={event => setConfirmation(event.target.value)} />
                {error && <PageState kind="error" title="Password change failed" description={error} />}
                <DialogFooter><Button variant="outline" disabled={busy} onClick={required ? () => { void signOut(); } : close}>{operation === 'logout' ? 'Signing out…' : required ? 'Sign out' : 'Cancel'}</Button>
                    <Button type="submit" disabled={busy || Boolean(mismatch)}>{operation === 'change' ? 'Updating password…' : 'Update password'}</Button></DialogFooter>
            </form>
        </DialogContent>
    </Dialog>;
}
