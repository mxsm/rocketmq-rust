import { ProducerRequestGuard } from '../requestGuard';
import { useEffect, useState, useRef } from 'react';
import { ProducerService } from '../../../services/producer.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type { ProducerConnectionView } from '../types/producer.types';
const SEARCH_INDICATOR_DELAY_MS = 180;


export const useProducerConnections = () => {
    const [topicOptions, setTopicOptions] = useState<string[]>([]);
    const [selectedTopic, storeSelectedTopic] = useState('');
    const [producerGroup, storeProducerGroup] = useState('');
    const [result, setResult] = useState<ProducerConnectionView | null>(null);
    const [isTopicLoading, setIsTopicLoading] = useState(true);
    const [isSearchPending, setIsSearchPending] = useState(false);
    const [isSearching, setIsSearching] = useState(false);
    const [hasSearched, setHasSearched] = useState(false);
    const [error, setError] = useState('');
    const guard = useRef(new ProducerRequestGuard());
    const indicator = useRef<number | null>(null);
    const invalidate = () => {
        guard.current.invalidate();
        if (indicator.current !== null) window.clearTimeout(indicator.current);
        setIsSearchPending(false);
        setIsSearching(false);
        setResult(null);
        setHasSearched(false);
        setError('');
    };
    const setSelectedTopic = (topic: string) => { invalidate(); storeSelectedTopic(topic); };
    const setProducerGroup = (group: string) => { invalidate(); storeProducerGroup(group); };


    useEffect(() => {
        let cancelled = false;
        const loadTopics = async () => {
            setIsTopicLoading(true);
            setError('');

            try {
                const response = await ProducerService.getProducerTopicOptions();
                if (cancelled) return;
                setTopicOptions(response.topics);
                storeSelectedTopic((current) => {
                    if (current && response.topics.includes(current)) {
                        return current;
                    }
                    return response.topics[0] ?? '';
                });
            } catch (loadError) {
                if (cancelled) return;
                setError(dashboardErrorMessage(loadError, 'Failed to load producer topics'));
            } finally {
                if (!cancelled) setIsTopicLoading(false);
            }
        };

        void loadTopics();
        return () => {
            cancelled = true;
            guard.current.invalidate();
            if (indicator.current !== null) window.clearTimeout(indicator.current);
        };
    }, []);

    const search = async () => {
        const topic = selectedTopic.trim();
        const group = producerGroup.trim();

        if (!topic) {
            setError('Please select a topic first.');
            return false;
        }
        if (!group) {
            setError('Please enter a producer group.');
            return false;
        }

        const isCurrent = guard.current.begin();
        if (indicator.current !== null) window.clearTimeout(indicator.current);
        let searchIndicatorTimer: number | null = null;
        setIsSearchPending(true);
        searchIndicatorTimer = window.setTimeout(() => {
            if (isCurrent()) setIsSearching(true);
        }, SEARCH_INDICATOR_DELAY_MS);
        indicator.current = searchIndicatorTimer;
        setError('');

        try {
            const response = await ProducerService.queryProducerConnections({
                topic,
                producerGroup: group,
            });
            if (!isCurrent()) return false;
            setResult(response);
            setHasSearched(true);
            return true;
        } catch (searchError) {
            if (!isCurrent()) return false;
            setResult(null);
            setHasSearched(true);
            setError(dashboardErrorMessage(searchError, 'Failed to query producer connections'));
            return false;
        } finally {
            if (searchIndicatorTimer !== null) {
                window.clearTimeout(searchIndicatorTimer);
            }
            if (isCurrent()) {
                setIsSearchPending(false);
                setIsSearching(false);
            }
        }
    };

    return {
        topicOptions,
        selectedTopic,
        setSelectedTopic,
        producerGroup,
        setProducerGroup,
        result,
        isTopicLoading,
        isSearchPending,
        isSearching,
        hasSearched,
        error,
        search,
    };
};
