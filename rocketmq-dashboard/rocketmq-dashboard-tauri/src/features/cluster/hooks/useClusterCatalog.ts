import { useEffect, useState } from 'react';
import { ClusterService } from '../../../services/cluster.service';
import type {
    ClusterBrokerConfigView,
    ClusterBrokerStatusView,
    ClusterHomePageResponse,
} from '../types/cluster.types';

const toErrorMessage = (error: unknown) => {
    if (error instanceof Error) {
        return error.message;
    }

    if (typeof error === 'string') {
        return error;
    }

    return 'Failed to load cluster data';
};

export const useClusterCatalog = () => {
    const [data, setData] = useState<ClusterHomePageResponse | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [loadError, setLoadError] = useState('');
    const [pendingConfigAddr, setPendingConfigAddr] = useState<string | null>(null);
    const [pendingStatusAddr, setPendingStatusAddr] = useState<string | null>(null);

    const loadHomePage = async (mode: 'initial' | 'refresh' = 'refresh') => {
        if (mode === 'initial') {
            setIsLoading(true);
        } else {
            setIsRefreshing(true);
        }
        setLoadError('');

        try {
            const homePage = await ClusterService.getClusterHomePage({
                forceRefresh: mode === 'refresh',
            });
            setData(homePage);
            return homePage;
        } catch (error) {
            const errorMessage = toErrorMessage(error);
            setLoadError(errorMessage);
            throw new Error(errorMessage);
        } finally {
            setIsLoading(false);
            setIsRefreshing(false);
        }
    };

    useEffect(() => {
        let isMounted = true;

        loadHomePage('initial')
            .then((homePage) => {
                if (isMounted) {
                    setData(homePage);
                }
            })
            .catch((error) => {
                console.error('Failed to load Cluster home page', error);
                if (isMounted) {
                    setLoadError(toErrorMessage(error));
                }
            })
            .finally(() => {
                if (isMounted) {
                    setIsLoading(false);
                }
            });

        return () => {
            isMounted = false;
        };
    }, []);

    const getBrokerConfig = async (brokerAddr: string): Promise<ClusterBrokerConfigView> => {
        const normalizedAddr = brokerAddr.trim();
        if (!normalizedAddr) {
            throw new Error('Broker address cannot be empty');
        }

        setPendingConfigAddr(normalizedAddr);
        try {
            return await ClusterService.getClusterBrokerConfig({ brokerAddr: normalizedAddr });
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingConfigAddr(null);
        }
    };

    const getBrokerStatus = async (brokerAddr: string): Promise<ClusterBrokerStatusView> => {
        const normalizedAddr = brokerAddr.trim();
        if (!normalizedAddr) {
            throw new Error('Broker address cannot be empty');
        }

        setPendingStatusAddr(normalizedAddr);
        try {
            return await ClusterService.getClusterBrokerStatus({ brokerAddr: normalizedAddr });
        } catch (error) {
            throw new Error(toErrorMessage(error));
        } finally {
            setPendingStatusAddr(null);
        }
    };

    return {
        data,
        isLoading,
        isRefreshing,
        loadError,
        pendingConfigAddr,
        pendingStatusAddr,
        refresh: () => loadHomePage('refresh'),
        getBrokerConfig,
        getBrokerStatus,
    };
};
