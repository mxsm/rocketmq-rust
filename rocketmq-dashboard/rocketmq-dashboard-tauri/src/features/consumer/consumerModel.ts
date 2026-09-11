import type { ClusterBrokerCardItem } from '../cluster/types/cluster.types';
import type { ConsumerConfigView, ConsumerCreateOrUpdateRequest, ConsumerMutationResult, ConsumerTopicDetailItem } from './types/consumer.types';
import { failedConsumerBrokers } from './mutation';

export const consumerIdentity = (name: string) => name.replace(/^%SYS%/, '').trim();
export function checkConsumerIdentity<T extends { consumerGroup: string }>(value: T, expected: string): T {
    if (value.consumerGroup !== consumerIdentity(expected)) throw new Error('The returned Consumer group does not match the selected group.');
    return value;
}
export const consumerTimestamp = (value: number) => Number.isFinite(value) && value > 0 ? new Date(value).toLocaleString() : 'Not reported';
export const consumerCount = (value: number) => Number.isFinite(value) && value >= 0 ? value.toLocaleString() : 'Unknown';
export function topicLagByBroker(topic: ConsumerTopicDetailItem): Map<string, number> {
    const values = new Map<string, number>();
    for (const queue of topic.queueStatInfoList) values.set(queue.brokerName, (values.get(queue.brokerName) ?? 0) + queue.diffTotal);
    return values;
}
export const consumerNumberFields = [
    ['retryQueueNums', 'Retry queues', 0, 2147483647], ['retryMaxTimes', 'Maximum retries (-1 or greater)', -1, 2147483647],
    ['consumeTimeoutMinute', 'Consume timeout (minutes)', 1, 2147483647], ['brokerId', 'Broker ID', 0, Number.MAX_SAFE_INTEGER],
    ['whichBrokerWhenConsumeSlowly', 'Slow-consume Broker ID', 0, Number.MAX_SAFE_INTEGER], ['groupSysFlag', 'Group system flag', -2147483648, 2147483647],
] as const;
export const consumerSwitches = [
    ['consumeEnable', 'Enable consumption'], ['consumeFromMinEnable', 'Consume from minimum offset'],
    ['consumeBroadcastEnable', 'Allow broadcast consumption'], ['consumeMessageOrderly', 'Orderly consumption'],
    ['notifyConsumerIdsChangedEnable', 'Notify clients of membership changes'],
] as const;
export function newConsumerDraft(group = ''): ConsumerCreateOrUpdateRequest {
    return { consumerGroup: group, clusterNameList: [], brokerNameList: [], consumeEnable: true, consumeFromMinEnable: true,
        consumeBroadcastEnable: true, consumeMessageOrderly: false, retryQueueNums: 1, retryMaxTimes: 16, brokerId: 0,
        whichBrokerWhenConsumeSlowly: 1, notifyConsumerIdsChangedEnable: true, groupSysFlag: 0, consumeTimeoutMinute: 15 };
}
export function consumerDraftFromConfig(config: ConsumerConfigView): ConsumerCreateOrUpdateRequest {
    const draft = newConsumerDraft(config.consumerGroup);
    for (const [field] of consumerNumberFields) draft[field] = config[field];
    for (const [field] of consumerSwitches) draft[field] = config[field];
    return { ...draft, brokerNameList: [config.brokerName] };
}
export function validateConsumerDraft(draft: ConsumerCreateOrUpdateRequest, targets: ClusterBrokerCardItem[]): ConsumerCreateOrUpdateRequest {
    const consumerGroup = draft.consumerGroup.trim();
    if (!consumerGroup || consumerGroup.startsWith('%SYS%')) throw new Error('Enter a mutable Consumer group name.');
    const brokerNameList = [...new Set(draft.brokerNameList)];
    if (!brokerNameList.length || brokerNameList.some(name => !targets.some(item => item.brokerName === name))) throw new Error('Select at least one currently available Broker.');
    for (const [field, label, min, max] of consumerNumberFields) {
        if (!Number.isSafeInteger(draft[field]) || draft[field] < min || draft[field] > max) throw new Error(label + ' is outside its supported integer range.');
    }
    // Cluster controls expand to explicit Brokers before review. Never expand again at dispatch.
    return { ...draft, consumerGroup, clusterNameList: [], brokerNameList };
}
export function reviewedFailedBrokers(receipt: ConsumerMutationResult, group: string, operation: ConsumerMutationResult['operation'], available: string[], submitted: string[]): string[] {
    if (receipt.consumerGroup !== group || receipt.operation !== operation) throw new Error('The receipt does not match this operation.');
    const failed = [...new Set(failedConsumerBrokers(receipt))];
    if (!failed.length || failed.some(name => !available.includes(name) || !submitted.includes(name))) throw new Error('Failed targets changed or are no longer available. Reopen the operation to inspect current state.');
    return failed;
}
