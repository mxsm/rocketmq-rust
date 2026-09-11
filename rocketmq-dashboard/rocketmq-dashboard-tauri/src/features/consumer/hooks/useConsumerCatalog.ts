import { useCallback, useMemo, useState } from 'react';
import { useReadResource } from '../../../hooks/useReadResource';
import { ConsumerService } from '../../../services/consumer.service';
import { consumerScopeKey } from '../scope';
import type { ConsumerQueryScope } from '../types/consumer.types';

export function useConsumerCatalog(scope: ConsumerQueryScope) {
    const key = consumerScopeKey(scope);
    const stableScope = useMemo(() => scope, [key]);
    const load = useCallback(() => ConsumerService.queryConsumerGroups({ skipSysGroup: false, scope: stableScope }), [stableScope]);
    const state = useReadResource(load, 'Consumer catalog could not be read.');
    const [refreshingGroup, setRefreshingGroup] = useState('');
    const refresh = useCallback(() => state.read(() => ConsumerService.refreshAllConsumerGroups({ skipSysGroup: false, scope: stableScope })), [state.read, stableScope]);
    const refreshGroup = useCallback(async (consumerGroup: string) => {
        if (!consumerGroup.trim() || state.pending || !state.data) return false;
        setRefreshingGroup(consumerGroup);
        try {
            return await state.read(async previous => {
                const item = await ConsumerService.refreshConsumerGroup({ consumerGroup, scope: stableScope });
                if (item.rawGroupName !== consumerGroup) throw new Error('Unexpected Consumer response');
                return { ...previous!, items: previous!.items.map(value => value.rawGroupName === consumerGroup ? item : value) };
            });
        } finally { setRefreshingGroup(''); }
    }, [state.read, state.pending, state.data, stableScope]);
    return { ...state, response: state.data, items: state.data?.items ?? [], summary: state.data?.summary,
        isInitialLoading: state.pending && !state.data, isRefreshPending: state.pending, isRefreshing: state.pending && Boolean(state.data),
        refreshingGroup, refresh, refreshGroup };
}
