import { describe, expect, it } from 'vitest';
import { canSendToTopic, isProtectedTopic, validateTopicDraft, failedTopicDraft, topicPermission } from './topicModel';
import type { TopicBatchResult, TopicConfigRequest, TopicListItem } from './types/topic.types';

const targets = [{ clusterName: 'east', brokerNames: ['a', 'b'] }, { clusterName: 'west', brokerNames: ['c'] }];
const draft: TopicConfigRequest = { topicName: 'orders', clusterNameList: ['east'], brokerNameList: ['a', 'b'],
    readQueueNums: 4, writeQueueNums: 4, perm: 6, order: false, messageType: 'NORMAL' };
const receipt: TopicBatchResult = { operation: 'create', topic: 'orders', targetCount: 2, success: false, message: 'Partial',
    targets: [{ kind: 'broker', name: 'a', success: true, errorCode: null, message: 'Completed' },
        { kind: 'broker', name: 'b', success: false, errorCode: 'unavailable', message: 'Unavailable' }], orderConfig: null };

describe('Topic operation targets', () => {
    it('normalizes explicit targets without widening an empty Broker selection', () => {
        expect(validateTopicDraft({ ...draft, topicName: ' orders ', brokerNameList: ['a', 'a'] }, targets)).toMatchObject({ topicName: 'orders', brokerNameList: ['a'] });
        expect(() => validateTopicDraft({ ...draft, brokerNameList: [] }, targets)).toThrow();
    });
    it('rejects missing clusters and Brokers outside the selected clusters', () => {
        for (const patch of [{ clusterNameList: [] }, { clusterNameList: ['missing'] }, { brokerNameList: ['c'] }, { brokerNameList: ['removed'] }])
            expect(() => validateTopicDraft({ ...draft, ...patch }, targets)).toThrow();
    });
    it('validates integer queue and permission bounds', () => {
        for (const value of [NaN, Infinity, 0, -1, 1.5, 2147483648])
            expect(() => validateTopicDraft({ ...draft, readQueueNums: value }, targets)).toThrow();
        for (const value of [NaN, -1, 1.5, 8])
            expect(() => validateTopicDraft({ ...draft, perm: value }, targets)).toThrow();
        expect(validateTopicDraft({ ...draft, writeQueueNums: 2147483647, perm: 0 }, targets).perm).toBe(0);
        expect(topicPermission(4)).toBe('Read (4)');
        expect(topicPermission(2)).toBe('Write (2)');
        expect(topicPermission(6)).toBe('Read / Write (6)');
    });
    it('reviews only failed Brokers and changes a partial create to update', () => {
        expect(failedTopicDraft(receipt, draft, targets)).toEqual({ request: { ...draft, brokerNameList: ['b'] }, mode: 'update' });
        expect(draft.brokerNameList).toEqual(['a', 'b']);
    });
    it('retains create intent only when no target succeeded', () => {
        const allFailed = { ...receipt, targets: receipt.targets.map(target => ({ ...target, success: false })) };
        expect(failedTopicDraft(allFailed, draft, targets).mode).toBe('create');
        expect(failedTopicDraft({ ...allFailed, operation: 'update' }, draft, targets).mode).toBe('update');
    });
    it('rejects a different Topic receipt, removed failed targets and order-only failures', () => {
        expect(() => failedTopicDraft({ ...receipt, topic: 'another' }, draft, targets)).toThrow();
        expect(() => failedTopicDraft(receipt, draft, [{ clusterName: 'east', brokerNames: ['a'] }])).toThrow();
        expect(() => failedTopicDraft({ ...receipt, targets: receipt.targets.map(target => ({ ...target, success: true })) }, draft, targets)).toThrow();
    });
    it('protects system Topics independently of their category and limits retry/DLQ actions', () => {
        const topic: TopicListItem = { topic: 'orders', category: 'NORMAL', systemTopic: false, messageType: 'NORMAL',
            clusters: ['east'], brokers: ['a'], readQueueCount: 4, writeQueueCount: 4, perm: 6, order: false };
        expect(canSendToTopic(topic)).toBe(true);
        expect(isProtectedTopic({ ...topic, systemTopic: true })).toBe(true);
        expect(isProtectedTopic({ ...topic, category: 'SYSTEM' })).toBe(true);
        for (const category of ['RETRY', 'DLQ', 'SYSTEM'] as const) expect(canSendToTopic({ ...topic, category })).toBe(false);
    });
});
