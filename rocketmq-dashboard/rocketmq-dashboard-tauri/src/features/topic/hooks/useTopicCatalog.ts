import { TopicService } from '../../../services/topic.service';
import { useReadResource } from '../../../hooks/useReadResource';

const loadTopics = () => TopicService.getTopicList({ skipSysProcess: false, skipRetryAndDlq: false });
export function useTopicCatalog() {
    const state = useReadResource(loadTopics, 'Topics could not be read.');
    return { ...state, refresh: state.read, isLoading: state.pending };
}
