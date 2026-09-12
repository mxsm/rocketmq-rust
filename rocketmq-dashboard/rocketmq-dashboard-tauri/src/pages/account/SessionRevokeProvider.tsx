import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react';
import { useAppStore } from '../../stores/app.store';
import { AuthService } from '../../services/auth.service';
import { dashboardErrorMessage } from '../../services/invoke';
import { SignOutConfirmDialog } from '../../features/auth';

const Context = createContext<{ open: (username: string) => void; pending: boolean } | null>(null);
/** Keep account revocation attached to the signed-in session across page and environment changes. */
export function SessionRevokeProvider({ children }: { children: ReactNode }) {
    const { currentUser } = useAppStore();
    const [username, setUsername] = useState<string | null>(null);
    const [pending, setPending] = useState(false);
    const [error, setError] = useState('');
    const inFlight = useRef(false);
    const active = useRef(true);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const confirm = async () => {
        if (!username || username !== currentUser?.username || inFlight.current) return;
        inFlight.current = true;
        setPending(true); setError('');
        try {
            const receipt = await AuthService.revokeUserSessions(username);
            if (active.current && receipt.currentSessionRevoked !== true) setError('Current-session revocation was not confirmed. Refresh the sessions before another attempt.');
            // A confirmed current-session revocation uses AuthService's existing authentication-invalid event.
        } catch (failure) {
            if (active.current) setError(dashboardErrorMessage(failure, 'Could not revoke sessions. No automatic retry was performed.'));
        } finally {
            inFlight.current = false;
            if (active.current) setPending(false);
        }
    };
    return <Context.Provider value={{ open: target => {
        if (!inFlight.current && target === currentUser?.username) { setError(''); setUsername(target); }
    }, pending }}>{children}
        <SignOutConfirmDialog open={username !== null} title="Sign out all sessions?"
            description={`All local dashboard sessions for ${username ?? ''}, including the current session, will be revoked. Sign in again to continue.`}
            error={error} confirmLabel="Sign out all sessions" isSubmitting={pending}
            onConfirm={confirm} onCancel={() => { if (!inFlight.current) setUsername(null); }} />
    </Context.Provider>;
}
export function useSessionRevoke() {
    const value = useContext(Context);
    if (!value) throw new Error('SessionRevokeProvider is required.');
    return value;
}
