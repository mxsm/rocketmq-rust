import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';
import { ProducerService } from '../../../services/producer.service';
import { ConnectionStore } from '../../../services/connection.store';
import { useNavigationState } from '../../../stores/app.store';
import { useReadResource } from '../../../hooks/useReadResource';
import { createProducerLookup } from '../producerLookup';

export function useProducerConnections() {
    const [selectedTopic, storeTopic] = useNavigationState('producerTopic', '');
    const [producerGroup, storeGroup] = useNavigationState('producerGroup', '');
    const loadTopics = useCallback(() => ProducerService.getProducerTopicOptions(), []);
    const topics = useReadResource(loadTopics, 'Topic suggestions could not be read. Enter a Topic manually.');
    const controller = useMemo(() => {
        const original = ConnectionStore.getSnapshot();
        return createProducerLookup(request => ProducerService.queryProducerConnections(request), () => {
            const current = ConnectionStore.getSnapshot();
            return current?.revision === original?.revision && current?.environmentId === original?.environmentId;
        });
    }, []);
    const lookup = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useEffect(() => { controller.start(); return controller.stop; }, [controller]);
    const setSelectedTopic = (value: string) => { controller.reset(); storeTopic(value); };
    const setProducerGroup = (value: string) => { controller.reset(); storeGroup(value); };
    const search = useCallback(() => controller.search({ topic: selectedTopic, producerGroup }), [controller, selectedTopic, producerGroup]);
    return { topics, selectedTopic, setSelectedTopic, producerGroup, setProducerGroup, ...lookup, search };
}
