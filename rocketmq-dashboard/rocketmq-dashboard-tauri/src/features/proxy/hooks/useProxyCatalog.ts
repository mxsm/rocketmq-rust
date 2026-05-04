import { useCallback, useEffect, useState } from 'react';
import { ProxyService } from '../../../services/proxy.service';
import type { ProxyConfigSnapshot } from '../types/proxy.types';

const toErrorMessage = (error: unknown) => {
    if (error instanceof Error) {
        return error.message;
    }

    if (typeof error === 'string') {
        return error;
    }

    return 'Proxy operation failed';
};

export const useProxyCatalog = () => {
    const [snapshot, setSnapshot] = useState<ProxyConfigSnapshot | null>(null);
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
            const errorMessage = toErrorMessage(error);
            setLoadError(errorMessage);
            throw new Error(errorMessage);
        }
    }, []);

    useEffect(() => {
        let isMounted = true;

        const loadInitialState = async () => {
            try {
                await loadHomePage();
            } catch (error) {
                console.error('Failed to load Proxy home page', error);
                if (isMounted) {
                    setLoadError(toErrorMessage(error));
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
            const result = await ProxyService.addProxyAddr(address);
            setSnapshot(result.snapshot);
            setNewAddress('');
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingAction(null);
        }
    };

    const switchProxy = async (address: string) => {
        setPendingAction(`switch:${address}`);

        try {
            const result = await ProxyService.switchProxyAddr(address);
            setSnapshot(result.snapshot);
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingAction(null);
        }
    };

    const deleteProxy = async (address: string) => {
        setPendingAction(`delete:${address}`);

        try {
            const result = await ProxyService.deleteProxyAddr(address);
            setSnapshot(result.snapshot);
            return result.message;
        } catch (error) {
            throw new Error(toErrorMessage(error));
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
