const SESSION_KEY = 'rocketmq.dashboard.session_id';

export class SessionStorageService {
    static getSessionId(): string | null {
        try {
            return window.localStorage.getItem(SESSION_KEY);
        } catch (error) {
            console.warn('Failed to read auth session from localStorage', error);
            return null;
        }
    }

    static setSessionId(sessionId: string): void {
        try {
            window.localStorage.setItem(SESSION_KEY, sessionId);
        } catch (error) {
            console.warn('Failed to persist auth session to localStorage', error);
        }
    }

    static clearSessionId(): void {
        try {
            window.localStorage.removeItem(SESSION_KEY);
        } catch (error) {
            console.warn('Failed to clear auth session from localStorage', error);
        }
    }
}
