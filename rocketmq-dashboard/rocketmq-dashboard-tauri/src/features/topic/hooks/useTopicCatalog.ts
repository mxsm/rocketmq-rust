import { useEffect, useState } from 'react';
import { TopicService } from '../../../services/topic.service';
import type { TopicListResponse } from '../types/topic.types';

const defaultListRequest = {
    skipSysProcess: false,
    skipRetryAndDlq: false,
};

export const useTopicCatalog = () => {
    const [data, setData] = useState<TopicListResponse | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [error, setError] = useState('');

    const load = async (mode: 'initial' | 'refresh' = 'refresh') => {
        if (mode === 'initial') {
            setIsLoading(true);
        } else {
            setIsRefreshing(true);
        }
        setError('');

        try {
            const result = await TopicService.getTopicList(defaultListRequest);
            setData(result);
        } catch (loadError) {
            setError(loadError instanceof Error ? loadError.message : 'Failed to load topics');
        } finally {
            setIsLoading(false);
            setIsRefreshing(false);
        }
    };

    useEffect(() => {
        void load('initial');
    }, []);

    return {
        data,
        isLoading,
        isRefreshing,
        error,
        refresh: () => load('refresh'),
    };
};
