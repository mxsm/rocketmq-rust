import type { ConsumerQueryScope } from '../types/consumer.types';
import { ConsumerRequestGeneration, consumerScopeKey } from '../scope';
import { useEffect, useRef, useState } from 'react';
import { ConsumerService } from '../../../services/consumer.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type {
    ConsumerGroupListItem,
    ConsumerGroupListResponse,
} from '../types/consumer.types';

const REFRESH_LABEL_DELAY_MS = 180;

export const useConsumerCatalog = (scope: ConsumerQueryScope) => {
    const [response, setResponse] = useState<ConsumerGroupListResponse | null>(null);
    const [isInitialLoading, setIsInitialLoading] = useState(true);
    const [isRefreshPending, setIsRefreshPending] = useState(false);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [refreshingGroup, setRefreshingGroup] = useState('');
    const [error, setError] = useState('');

    const generation = useRef(new ConsumerRequestGeneration());
    const scopeKey = consumerScopeKey(scope);

    const load = async (mode: 'initial' | 'refresh' = 'initial') => {
        const current = generation.current.begin();
        let refreshIndicatorTimer: number | null = null;

        if (mode === 'initial') {
            setIsInitialLoading(true);
        } else {
            setIsRefreshPending(true);
            refreshIndicatorTimer = window.setTimeout(() => {
                if (current()) setIsRefreshing(true);
            }, REFRESH_LABEL_DELAY_MS);
        }

        setError('');
        try {
            const request = {
                skipSysGroup: false,
                scope,
            };
            const next = mode === 'initial'
                ? await ConsumerService.queryConsumerGroups(request)
                : await ConsumerService.refreshAllConsumerGroups(request);
            if (!current()) return null;
            setResponse(next);
            return next;
        } catch (loadError) {
            if (current()) setError(dashboardErrorMessage(loadError, 'Failed to load consumer groups'));
            return null;
        } finally {
            if (refreshIndicatorTimer !== null) {
                window.clearTimeout(refreshIndicatorTimer);
            }
            if (current()) {
                setIsInitialLoading(false);
                setIsRefreshPending(false);
                setIsRefreshing(false);
                setRefreshingGroup('');
            }
        }
    };

    useEffect(() => {
        setResponse(null);
        void load('initial');
        return () => generation.current.invalidate();
    }, [scopeKey]);

    const refresh = async () => load('refresh');

    const refreshGroup = async (consumerGroup: string) => {
        const group = consumerGroup.trim();
        if (!group || !response) {
            return false;
        }

        const current = generation.current.begin();
        setRefreshingGroup(group);
        setError('');
        try {
            const item = await ConsumerService.refreshConsumerGroup({
                consumerGroup: group,
                scope,
            });

            if (!current()) return false;
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
            if (current()) setError(dashboardErrorMessage(refreshError, 'Failed to refresh consumer group'));
            return false;
        } finally {
            if (current()) { setRefreshingGroup(''); setIsRefreshPending(false); setIsRefreshing(false); }
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
