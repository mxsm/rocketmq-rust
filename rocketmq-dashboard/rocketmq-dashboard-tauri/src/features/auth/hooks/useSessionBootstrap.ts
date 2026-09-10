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
                if (!isMounted || SessionStorageService.getSessionId() !== sessionId) {
                    return;
                }

                setAuthSession(result.sessionId, result.currentUser);
            } catch (_error) {
                if (isMounted) SessionStorageService.reportAuthenticationFailure(sessionId, 'invalid');
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
