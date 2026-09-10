const SESSION_KEY = 'rocketmq.dashboard.session_id';
export type AuthenticationFailure = 'invalid' | 'password-change-required';
const listeners = new Set<(reason: AuthenticationFailure) => void>();
let memorySession: string | null = null;

export class SessionStorageService {
    static getSessionId(): string | null {
        try {
            return window.localStorage.getItem(SESSION_KEY);
        } catch {
            return memorySession;
        }
    }

    static setSessionId(sessionId: string): void {
        memorySession = sessionId;
        try { window.localStorage.setItem(SESSION_KEY, sessionId); } catch {}
    }

    static clearSessionId(): void {
        memorySession = null;
        try { window.localStorage.removeItem(SESSION_KEY); } catch {}
    }

    static subscribeAuthenticationFailure(listener: (reason: AuthenticationFailure) => void): () => void {
        listeners.add(listener);
        return () => { listeners.delete(listener); };
    }

    static reportAuthenticationFailure(sessionId: string | null, reason: AuthenticationFailure): void {
        // A late rejection from an old request must not sign out a newer login.
        if (this.getSessionId() !== sessionId) return;
        if (reason === 'invalid') this.clearSessionId();
        for (const listener of listeners) listener(reason);
    }
}
