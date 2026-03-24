import { useEffect } from 'react';
import { AuthService } from '../../../services/auth.service';
import { SessionStorageService } from '../../../services/session.storage';
import { useAppStore } from '../../../stores/app.store';

export const useSessionBootstrap = () => {
    const {
        clearAuthSession,
        finishAuthBootstrap,
        setAuthSession,
        startAuthBootstrap,
    } = useAppStore();

    useEffect(() => {
        let isMounted = true;

        const bootstrap = async () => {
            startAuthBootstrap();

            const sessionId = SessionStorageService.getSessionId();
            if (!sessionId) {
                if (isMounted) {
                    clearAuthSession();
                    finishAuthBootstrap();
                }
                return;
            }

            try {
                const result = await AuthService.restoreSession(sessionId);
                if (!isMounted) {
                    return;
                }

                if (result.success && result.sessionId && result.currentUser) {
                    setAuthSession(result.sessionId, result.currentUser);
                } else {
                    SessionStorageService.clearSessionId();
                    clearAuthSession();
                }
            } catch (error) {
                console.error('Failed to restore auth session', error);
                SessionStorageService.clearSessionId();
                if (isMounted) {
                    clearAuthSession();
                }
            } finally {
                if (isMounted) {
                    finishAuthBootstrap();
                }
            }
        };

        void bootstrap();

        return () => {
            isMounted = false;
        };
    }, []);
};
