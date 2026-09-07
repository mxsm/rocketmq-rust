import { useEffect, useState } from 'react';
import { ClusterService } from '../../../services/cluster.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type {
    ClusterBrokerConfigView,
    ClusterBrokerStatusView,
    ClusterHomePageResponse,
} from '../types/cluster.types';

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
            const errorMessage = dashboardErrorMessage(error, 'Failed to load cluster data');
            setLoadError(errorMessage);
            throw error;
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
                if (isMounted) {
                    setLoadError(dashboardErrorMessage(error, 'Failed to load cluster data'));
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
            throw error;
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
            throw error;
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
