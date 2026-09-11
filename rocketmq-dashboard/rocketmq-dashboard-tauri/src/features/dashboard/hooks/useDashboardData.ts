import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { usePageRefresh } from '../../../app/layout/pageToolbar';
import { ClusterService } from '../../../services/cluster.service';
import { DashboardService } from '../../../services/dashboard.service';
import { HistoryService, type HistoryPage } from '../../../services/history.service';
import { TopicService } from '../../../services/topic.service';
import { localHistoryDay, todayLocal } from '../history';
import { mergeHistoryPages } from '../historySeries';
import { useDashboardRead } from './useDashboardRead';

const readOverview = () => DashboardService.getOverview();
const readBrokers = () => ClusterService.getClusterHomePage({ forceRefresh: true });
const readTopicCharts = () => DashboardService.queryTopicCurrent();
const readCollector = () => HistoryService.status();
const readTopics = () => TopicService.getTopicList({ skipSysProcess: false, skipRetryAndDlq: false });

function useHistoryRead(kind: 'broker' | 'topic', date: string, topic?: string, enabled = true) {
    const query = useCallback((beforeMs?: number) => HistoryService.query(kind, {
        ...localHistoryDay(date), ...(topic ? { topicName: topic } : {}), beforeMs, limit: 1000,
    }), [kind, date, topic]);
    const loader = useCallback(() => query(), [query]);
    const state = useDashboardRead<HistoryPage>(enabled ? loader : null, 'Stored history could not be read.');
    const loadOlder = useCallback(() => state.read(async previous => {
        if (previous?.nextBeforeMs == null) return previous ?? { samples: [], nextBeforeMs: null };
        return mergeHistoryPages(previous, await query(previous.nextBeforeMs));
    }), [state.read, query]);
    return { ...state, loadOlder };
}

export function useDashboardData() {
    const [date, setDate] = useState(todayLocal);
    const [historyTopic, setHistoryTopic] = useState('');
    const overview = useDashboardRead(readOverview, 'Cluster overview unavailable.');
    const brokers = useDashboardRead(readBrokers, 'Broker status could not be read.');
    const topicCharts = useDashboardRead(readTopicCharts, 'Topic analysis unavailable.');
    const collector = useDashboardRead(readCollector, 'History collector status unavailable.');
    const topics = useDashboardRead(readTopics, 'Topic suggestions unavailable. Enter a Topic name to continue.');
    const brokerHistory = useHistoryRead('broker', date);
    const topicHistory = useHistoryRead('topic', date);
    const messageHistory = useHistoryRead('topic', date, historyTopic, Boolean(historyTopic));
    const reads = useMemo(() => [overview.read, brokers.read, topicCharts.read, collector.read,
        topics.read, brokerHistory.read, topicHistory.read, messageHistory.read],
    [overview.read, brokers.read, topicCharts.read, collector.read, topics.read, brokerHistory.read, topicHistory.read, messageHistory.read]);
    const generation = useRef(0);
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    const regions = [overview, brokers, topicCharts, collector, topics, brokerHistory, topicHistory,
        ...(historyTopic ? [messageHistory] : [])];
    const pending = regions.some(region => region.pending);
    const initialSuccessAt = regions.every(region => !region.pending && !region.error && region.receivedAt !== null)
        ? Math.min(...regions.map(region => region.receivedAt!)) : null;
    useEffect(() => {
        if (initialSuccessAt !== null) setRefreshedAt(previous => previous ?? initialSuccessAt);
    }, [initialSuccessAt]);
    useEffect(() => {
        generation.current += 1;
        return () => { generation.current += 1; };
    }, [reads]);
    const refresh = useCallback(() => {
        const current = ++generation.current;
        void Promise.all(reads.map(read => read())).then(results => {
            if (generation.current === current && results.every(Boolean)) setRefreshedAt(Date.now());
        });
    }, [reads]);
    usePageRefresh({ refresh, pending, refreshedAt });
    return { overview, brokers, topicCharts, collector, topics, brokerHistory, topicHistory, messageHistory,
        date, setDate, historyTopic, setHistoryTopic, pending };
}

export type DashboardData = ReturnType<typeof useDashboardData>;
