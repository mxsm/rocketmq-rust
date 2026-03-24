import { useEffect, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import type { TopicListResponse } from '../types/topic.types';

const defaultListRequest = {
    skipSysProcess: false,
    skipRetryAndDlq: false,
};

const REFRESH_INDICATOR_DELAY_MS = 180;

export const useTopicCatalog = () => {
    const [data, setData] = useState<TopicListResponse | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isRefreshPending, setIsRefreshPending] = useState(false);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [error, setError] = useState('');

    const load = async (mode: 'initial' | 'refresh' = 'refresh') => {
        let refreshIndicatorTimer: number | null = null;
        if (mode === 'initial') {
            setIsLoading(true);
        } else {
            setIsRefreshPending(true);
            refreshIndicatorTimer = window.setTimeout(() => {
                setIsRefreshing(true);
            }, REFRESH_INDICATOR_DELAY_MS);
        }
        setError('');

        try {
            const result = await TopicService.getTopicList(defaultListRequest);
            setData(result);
        } catch (loadError) {
            setError(loadError instanceof Error ? loadError.message : 'Failed to load topics');
        } finally {
            if (refreshIndicatorTimer !== null) {
                window.clearTimeout(refreshIndicatorTimer);
            }
            setIsLoading(false);
            setIsRefreshPending(false);
            setIsRefreshing(false);
        }
    };

    useEffect(() => {
        void load('initial');
    }, []);

    return {
        data,
        isLoading,
        isRefreshPending,
        isRefreshing,
        error,
        refresh: () => load('refresh'),
    };
};
