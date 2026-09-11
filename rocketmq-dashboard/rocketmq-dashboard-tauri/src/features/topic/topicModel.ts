import type { TopicCategory, TopicListItem, TopicConfigRequest, TopicTargetOption, TopicBatchResult } from './types/topic.types';

export const isProtectedTopic = (topic: TopicListItem) => topic.systemTopic || topic.category === 'SYSTEM';
export const canSendToTopic = (topic: TopicListItem) => !isProtectedTopic(topic) && topic.category !== 'RETRY' && topic.category !== 'DLQ';
export const topicPermission = (perm: number) => {
    const access = [perm & 4 ? 'Read' : '', perm & 2 ? 'Write' : ''].filter(Boolean).join(' / ');
    return access ? access + ' (' + perm + ')' : 'No read/write (' + perm + ')';
};
export const allTopicCategories = (): Record<TopicCategory, boolean> =>
    ({ NORMAL: true, FIFO: true, DELAY: true, TRANSACTION: true, UNSPECIFIED: true, RETRY: true, DLQ: true, SYSTEM: true });

export function validateTopicDraft(draft: TopicConfigRequest, targets: TopicTargetOption[]): TopicConfigRequest {
    const topicName = draft.topicName.trim();
    if (!topicName) throw new Error('Enter a Topic name.');
    const clusters = [...new Set(draft.clusterNameList)];
    if (!clusters.length || clusters.some(name => !targets.some(target => target.clusterName === name))) throw new Error('Select current cluster targets.');
    const available = targets.filter(target => clusters.includes(target.clusterName)).flatMap(target => target.brokerNames);
    const brokers = [...new Set(draft.brokerNameList)];
    if (!brokers.length) throw new Error('Select at least one Broker target.');
    if (brokers.some(name => !available.includes(name))) throw new Error('Selected Brokers must belong to the selected clusters.');
    if (![draft.readQueueNums, draft.writeQueueNums].every(value => Number.isInteger(value) && value > 0 && value <= 2147483647))
        throw new Error('Queue counts must be positive 32-bit integers.');
    if (!Number.isInteger(draft.perm) || draft.perm < 0 || draft.perm > 7) throw new Error('Permission must be an integer from 0 to 7.');
    return { ...draft, topicName, clusterNameList: clusters, brokerNameList: brokers };
}

export function failedTopicDraft(result: TopicBatchResult, previous: TopicConfigRequest, targets: TopicTargetOption[]) {
    if (result.topic !== previous.topicName || (result.operation !== 'create' && result.operation !== 'update'))
        throw new Error('This receipt does not match the Topic configuration draft.');
    const names = result.targets.filter(target => target.kind === 'broker' && !target.success).map(target => target.name);
    if (!names.length) throw new Error('There are no failed Broker targets to review.');
    const clusters = targets.filter(target => target.brokerNames.some(name => names.includes(name))).map(target => target.clusterName);
    const request = validateTopicDraft({ ...previous, clusterNameList: clusters, brokerNameList: names }, targets);
    const mode = result.operation === 'create' && !result.targets.some(target => target.success) ? 'create' as const : 'update' as const;
    return { request, mode };
}
