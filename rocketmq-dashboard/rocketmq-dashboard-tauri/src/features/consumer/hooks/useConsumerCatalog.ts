import { useEffect, useMemo, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import type {
    ConsumerGroupListItem,
    ConsumerGroupListResponse,
} from '../types/consumer.types';

const REFRESH_LABEL_DELAY_MS = 180;

export const useConsumerCatalog = (address?: string) => {
    const [response, setResponse] = useState<ConsumerGroupListResponse | null>(null);
    const [isInitialLoading, setIsInitialLoading] = useState(true);
    const [isRefreshPending, setIsRefreshPending] = useState(false);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [refreshingGroup, setRefreshingGroup] = useState('');
    const [error, setError] = useState('');

    const normalizedAddress = useMemo(() => {
        const value = address?.trim() ?? '';
        return value.length > 0 ? value : undefined;
    }, [address]);

    const load = async (mode: 'initial' | 'refresh' = 'initial') => {
        let refreshIndicatorTimer: number | null = null;

        if (mode === 'initial') {
            setIsInitialLoading(true);
        } else {
            setIsRefreshPending(true);
            refreshIndicatorTimer = window.setTimeout(() => {
                setIsRefreshing(true);
            }, REFRESH_LABEL_DELAY_MS);
        }

        setError('');
        try {
            const next = await ConsumerService.queryConsumerGroups({
                skipSysGroup: false,
                address: normalizedAddress,
            });
            setResponse(next);
            return next;
        } catch (loadError) {
            setError(loadError instanceof Error ? loadError.message : 'Failed to load consumer groups');
            return null;
        } finally {
            if (refreshIndicatorTimer !== null) {
                window.clearTimeout(refreshIndicatorTimer);
            }
            setIsInitialLoading(false);
            setIsRefreshPending(false);
            setIsRefreshing(false);
        }
    };

    useEffect(() => {
        void load('initial');
    }, [normalizedAddress]);

    const refresh = async () => load('refresh');

    const refreshGroup = async (consumerGroup: string) => {
        const group = consumerGroup.trim();
        if (!group) {
            return false;
        }

        setRefreshingGroup(group);
        setError('');
        try {
            const item = await ConsumerService.refreshConsumerGroup({
                consumerGroup: group,
                address: normalizedAddress,
            });

            setResponse((current) => {
                if (!current) {
                    return current;
                }
                const items = current.items.map((existing) =>
                    existing.rawGroupName === item.rawGroupName ? item : existing,
                );
                return { ...current, items };
            });
            return true;
        } catch (refreshError) {
            setError(refreshError instanceof Error ? refreshError.message : 'Failed to refresh consumer group');
            return false;
        } finally {
            setRefreshingGroup('');
        }
    };

    const replaceItems = (items: ConsumerGroupListItem[]) => {
        setResponse((current) => (current ? { ...current, items } : current));
    };

    return {
        response,
        items: response?.items ?? [],
        summary: response?.summary,
        isInitialLoading,
        isRefreshPending,
        isRefreshing,
        refreshingGroup,
        error,
        refresh,
        refreshGroup,
        replaceItems,
    };
};
