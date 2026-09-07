const SESSION_KEY = 'rocketmq.dashboard.session_id';

export class SessionStorageService {
    static getSessionId(): string | null {
        try {
            return window.localStorage.getItem(SESSION_KEY);
        } catch {
            return null;
        }
    }

    static setSessionId(sessionId: string): void {
        try {
            window.localStorage.setItem(SESSION_KEY, sessionId);
        } catch {}
    }

    static clearSessionId(): void {
        try {
            window.localStorage.removeItem(SESSION_KEY);
        } catch {}
    }
}
