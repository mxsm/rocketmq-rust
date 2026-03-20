import { useEffect, useState } from 'react';
import { ProducerService } from '../../../services/producer.service';
import type { ProducerConnectionView } from '../types/producer.types';
const SEARCH_INDICATOR_DELAY_MS = 180;

const defaultProducerGroup = 'please_rename_unique_group_name';

export const useProducerConnections = () => {
    const [topicOptions, setTopicOptions] = useState<string[]>([]);
    const [selectedTopic, setSelectedTopic] = useState('');
    const [producerGroup, setProducerGroup] = useState(defaultProducerGroup);
    const [result, setResult] = useState<ProducerConnectionView | null>(null);
    const [isTopicLoading, setIsTopicLoading] = useState(true);
    const [isSearchPending, setIsSearchPending] = useState(false);
    const [isSearching, setIsSearching] = useState(false);
    const [hasSearched, setHasSearched] = useState(false);
    const [error, setError] = useState('');

    useEffect(() => {
        const loadTopics = async () => {
            setIsTopicLoading(true);
            setError('');

            try {
                const response = await ProducerService.getProducerTopicOptions();
                setTopicOptions(response.topics);
                setSelectedTopic((current) => {
                    if (current && response.topics.includes(current)) {
                        return current;
                    }
                    return response.topics[0] ?? '';
                });
            } catch (loadError) {
                setError(loadError instanceof Error ? loadError.message : 'Failed to load producer topics');
            } finally {
                setIsTopicLoading(false);
            }
        };

        void loadTopics();
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

        let searchIndicatorTimer: number | null = null;
        setIsSearchPending(true);
        searchIndicatorTimer = window.setTimeout(() => {
            setIsSearching(true);
        }, SEARCH_INDICATOR_DELAY_MS);
        setError('');

        try {
            const response = await ProducerService.queryProducerConnections({
                topic,
                producerGroup: group,
            });
            setResult(response);
            setHasSearched(true);
            return true;
        } catch (searchError) {
            setResult(null);
            setHasSearched(true);
            setError(searchError instanceof Error ? searchError.message : 'Failed to query producer connections');
            return false;
        } finally {
            if (searchIndicatorTimer !== null) {
                window.clearTimeout(searchIndicatorTimer);
            }
            setIsSearchPending(false);
            setIsSearching(false);
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
