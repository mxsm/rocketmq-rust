import { useCallback, useRef, useState } from 'react';
import { KeyRound, LogOut, UserRound } from 'lucide-react';
import { useAppStore } from '../../stores/app.store';
import { AuthService } from '../../services/auth.service';
import { useReadResource } from '../../hooks/useReadResource';
import { usePageRefresh } from '../../app/layout/pageToolbar';
import { useAuth } from '../../features/auth/hooks/useAuth';
import { ChangePasswordDialog } from '../../features/auth/components/ChangePasswordDialog';
import { SignOutConfirmDialog } from '../../features/auth/components/SignOutConfirmDialog';
import { Button } from '../../components/ui/LegacyButton';
import { PageState } from '../../components/layout/PageState';
import { StatusBadge } from '../../components/layout/StatusBadge';
import { SessionsPanel } from './SessionsPanel';
import { accountStatus, accountTimestamp } from './accountModel';
import './account.css';

function Detail({ label, value }: { label: string; value: string }) {
    return <div><dt>{label}</dt><dd tabIndex={0}>{value}</dd></div>;
}
export function AccountPage() {
    const { currentUser, sessionId } = useAppStore();
    const profile = useReadResource(AuthService.getCurrentUserProfile, 'Account details could not be loaded.');
    const { logout } = useAuth();
    const [passwordOpen, setPasswordOpen] = useState(false);
    const [signOutOpen, setSignOutOpen] = useState(false);
    const [signingOut, setSigningOut] = useState(false);
    const inFlight = useRef(false);
    const refresh = useCallback(() => { void profile.read(); }, [profile.read]);
    usePageRefresh({ refresh, pending: profile.pending, refreshedAt: profile.receivedAt });
    const username = profile.data?.username ?? currentUser?.username ?? 'Account';
    const status = accountStatus(profile.data, Boolean(profile.error));
    const unavailable = profile.pending ? 'Loading…' : 'Not available';
    const signOut = async () => {
        if (inFlight.current) return;
        inFlight.current = true; setSigningOut(true);
        try { await logout(); }
        finally { inFlight.current = false; setSigningOut(false); }
    };
    return <div className="ops-account">
        <section className="ops-account-summary" aria-label="Account overview">
            <div className="ops-account-identity"><span className="ops-account-avatar" aria-hidden="true"><UserRound size={26} /></span><div><h2>{username}</h2>
                <p className="ops-account-note">Local dashboard account</p></div><StatusBadge tone={status.tone}>{status.label}</StatusBadge></div>
            <div className="ops-account-actions"><Button variant="outline" icon={KeyRound} onClick={() => setPasswordOpen(true)}>Change password</Button>
                <Button variant="danger" icon={LogOut} onClick={() => setSignOutOpen(true)}>Sign out</Button></div>
        </section>
        {profile.error && <PageState kind="error" title={profile.data ? 'Account refresh failed; previous details retained' : 'Account details unavailable'} description={profile.error} action={<Button variant="outline" disabled={profile.pending} onClick={refresh}>Retry account details</Button>} />}
        <div className="ops-account-columns">
            <section className="ops-account-section" aria-labelledby="account-details"><h2 id="account-details">Basic details</h2><dl>
                <Detail label="Username" value={username} /><Detail label="User ID" value={profile.data ? String(profile.data.userId) : unavailable} />
                <Detail label="Created" value={profile.data ? accountTimestamp(profile.data.createdAt) : unavailable} />
                <Detail label="Updated" value={profile.data ? accountTimestamp(profile.data.updatedAt) : unavailable} /></dl>
            </section>
            <section className="ops-account-section" aria-labelledby="account-security"><h2 id="account-security">Security status</h2><dl>
                <Detail label="Account" value={profile.data ? profile.data.isActive ? 'Enabled' : 'Disabled' : unavailable} />
                <Detail label="Password" value={profile.data ? profile.data.mustChangePassword ? 'Change required' : 'No change required' : unavailable} />
                <Detail label="Last login" value={profile.data ? accountTimestamp(profile.data.lastLoginAt) : unavailable} /></dl>
                <p className="ops-account-notice">Authentication is local to this dashboard. Changing the password signs out all sessions. Session credentials are never displayed or copied.</p>
            </section>
        </div>
        {currentUser && <SessionsPanel key={sessionId} username={currentUser.username} />}
        <ChangePasswordDialog open={passwordOpen} onOpenChange={setPasswordOpen} />
        <SignOutConfirmDialog open={signOutOpen} isSubmitting={signingOut} title="Sign out of this dashboard?"
            description="Your local sign-in will be cleared. Sign in again to continue. If the dashboard service is unavailable, the local sign-in is still removed."
            onCancel={() => { if (!inFlight.current) setSignOutOpen(false); }} onConfirm={signOut} />
    </div>;
}
