import { useCallback, useEffect, useState } from 'react';
import { ProxyService } from '../../../services/proxy.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type { ProxyHomePageInfo } from '../types/proxy.types';

export const useProxyCatalog = () => {
    const [snapshot, setSnapshot] = useState<ProxyHomePageInfo | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [loadError, setLoadError] = useState('');
    const [pendingAction, setPendingAction] = useState<string | null>(null);
    const [newAddress, setNewAddress] = useState('');

    const loadHomePage = useCallback(async () => {
        try {
            const nextSnapshot = await ProxyService.getHomePageInfo();
            setSnapshot(nextSnapshot);
            setLoadError('');
            return nextSnapshot;
        } catch (error) {
            const errorMessage = dashboardErrorMessage(error, 'Proxy operation failed');
            setLoadError(errorMessage);
            throw error;
        }
    }, []);

    useEffect(() => {
        let isMounted = true;

        const loadInitialState = async () => {
            try {
                await loadHomePage();
            } catch (error) {
                if (isMounted) {
                    setLoadError(dashboardErrorMessage(error, 'Proxy operation failed'));
                }
            } finally {
                if (isMounted) {
                    setIsLoading(false);
                }
            }
        };

        void loadInitialState();

        return () => {
            isMounted = false;
        };
    }, [loadHomePage]);

    const addProxy = async () => {
        const address = newAddress.trim();
        if (!address) {
            throw new Error('Please enter a valid Proxy address');
        }

        setPendingAction('add');

        try {
            const result = await ProxyService.addProxyAddr(address, snapshot?.settings.revision ?? -1);
            setSnapshot({ ...result.settings.proxy, settings: result.settings });
            setNewAddress('');
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const switchProxy = async (address: string) => {
        setPendingAction(`switch:${address}`);

        try {
            const result = await ProxyService.switchProxyAddr(address, snapshot?.settings.revision ?? -1);
            setSnapshot({ ...result.settings.proxy, settings: result.settings });
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    const deleteProxy = async (address: string) => {
        setPendingAction(`delete:${address}`);

        try {
            const result = await ProxyService.deleteProxyAddr(address, snapshot?.settings.revision ?? -1);
            setSnapshot({ ...result.settings.proxy, settings: result.settings });
            return result.message;
        } catch (error) {
            throw error;
        } finally {
            setPendingAction(null);
        }
    };

    return {
        snapshot,
        isLoading,
        loadError,
        pendingAction,
        newAddress,
        setNewAddress,
        loadHomePage,
        addProxy,
        switchProxy,
        deleteProxy,
    };
};
