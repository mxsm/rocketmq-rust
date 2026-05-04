import { useEffect, useState } from 'react';
import { DashboardService } from '../../../services/dashboard.service';
import type {
    DashboardBrokerOverviewResponse,
    DashboardTopicCurrentResponse,
} from '../types/dashboard.types';

const REFRESH_INDICATOR_DELAY_MS = 180;

const toErrorMessage = (error: unknown, fallback: string) => {
    if (error instanceof Error) {
        return error.message;
    }

    if (typeof error === 'string') {
        return error;
    }

    return fallback;
};

export const useDashboardCharts = () => {
    const [brokerOverview, setBrokerOverview] = useState<DashboardBrokerOverviewResponse | null>(null);
    const [topicCurrent, setTopicCurrent] = useState<DashboardTopicCurrentResponse | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [brokerError, setBrokerError] = useState('');
    const [topicError, setTopicError] = useState('');

    const load = async (mode: 'initial' | 'refresh' = 'refresh') => {
        let refreshIndicatorTimer: number | null = null;
        if (mode === 'initial') {
            setIsLoading(true);
        } else {
            refreshIndicatorTimer = window.setTimeout(() => {
                setIsRefreshing(true);
            }, REFRESH_INDICATOR_DELAY_MS);
        }

        setBrokerError('');
        setTopicError('');

        const [brokerResult, topicResult] = await Promise.allSettled([
            DashboardService.getBrokerOverview({ forceRefresh: mode === 'refresh' }),
            DashboardService.queryTopicCurrent(),
        ]);

        if (brokerResult.status === 'fulfilled') {
            setBrokerOverview(brokerResult.value);
        } else {
            setBrokerError(toErrorMessage(brokerResult.reason, 'Failed to load dashboard broker data'));
        }

        if (topicResult.status === 'fulfilled') {
            setTopicCurrent(topicResult.value);
        } else {
            setTopicError(toErrorMessage(topicResult.reason, 'Failed to load dashboard topic data'));
        }

        if (refreshIndicatorTimer !== null) {
            window.clearTimeout(refreshIndicatorTimer);
        }
        setIsLoading(false);
        setIsRefreshing(false);
    };

    useEffect(() => {
        let isMounted = true;

        load('initial')
            .catch((error) => {
                if (isMounted) {
                    const message = toErrorMessage(error, 'Failed to load dashboard charts');
                    setBrokerError((current) => current || message);
                    setTopicError((current) => current || message);
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

    return {
        brokerOverview,
        topicCurrent,
        isLoading,
        isRefreshing,
        brokerError,
        topicError,
        refresh: () => load('refresh'),
    };
};
