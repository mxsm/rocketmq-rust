import { describe, expect, it } from 'vitest';
import { checkConsumerIdentity, consumerDraftFromConfig, newConsumerDraft, reviewedFailedBrokers, topicLagByBroker, validateConsumerDraft } from './consumerModel';
import type { ClusterBrokerCardItem } from '../cluster/types/cluster.types';
import type { ConsumerConfigView, ConsumerMutationResult, ConsumerTopicDetailItem } from './types/consumer.types';

const targets = [{ clusterName: 'east', brokerName: 'a' }, { clusterName: 'east', brokerName: 'b' }] as ClusterBrokerCardItem[];
const draft = { ...newConsumerDraft('workers'), brokerNameList: ['a', 'b'] };
const receipt: ConsumerMutationResult = { consumerGroup: 'workers', operation: 'upsert', targetCount: 3, success: false, targets: [
    { kind: 'BROKER', target: 'a', success: true, errorCode: null, message: 'Completed' },
    { kind: 'BROKER', target: 'b', success: false, errorCode: 'unavailable', message: 'Not confirmed' },
    { kind: 'INTERNAL_TOPIC_CLEANUP', target: '%DLQ%workers', success: false, errorCode: 'unavailable', message: 'Not confirmed' },
] };
describe('Consumer operation targets and identity', () => {
    it('requires explicit Brokers and never expands an empty selection to a cluster', () => {
        expect(() => validateConsumerDraft({ ...draft, clusterNameList: ['east'], brokerNameList: [] }, targets)).toThrow();
        expect(validateConsumerDraft({ ...draft, clusterNameList: ['east'], brokerNameList: ['a', 'a'] }, targets)).toMatchObject({ clusterNameList: [], brokerNameList: ['a'] });
        expect(() => validateConsumerDraft({ ...draft, brokerNameList: ['missing'] }, targets)).toThrow();
    });
    it('rejects invalid numbers without rounding or changing sentinel values', () => {
        for (const retryMaxTimes of [NaN, Infinity, 0.5, -2, 2147483648]) expect(() => validateConsumerDraft({ ...draft, retryMaxTimes }, targets)).toThrow();
        expect(validateConsumerDraft({ ...draft, retryMaxTimes: -1, retryQueueNums: 0 }, targets)).toMatchObject({ retryMaxTimes: -1, retryQueueNums: 0 });
        expect(() => validateConsumerDraft({ ...draft, consumeTimeoutMinute: 0 }, targets)).toThrow();
        expect(() => validateConsumerDraft({ ...draft, brokerId: Number.MAX_SAFE_INTEGER + 1 }, targets)).toThrow();
    });
    it('keeps the chosen configuration source as the initial single Broker target', () => {
        const config = { ...draft, consumerGroup: 'workers', brokerName: 'b', brokerAddress: 'broker-b:10911', retryMaxTimes: -1,
            consumeEnable: false, consumeMessageOrderly: true } as unknown as ConsumerConfigView;
        expect(consumerDraftFromConfig(config)).toMatchObject({ brokerNameList: ['b'], clusterNameList: [], retryMaxTimes: -1, consumeEnable: false, consumeMessageOrderly: true });
    });
    it('reviews only failed Brokers while excluding successes and cleanup targets', () => {
        expect(reviewedFailedBrokers(receipt, 'workers', 'upsert', ['a', 'b'], ['a', 'b'])).toEqual(['b']);
        expect(() => reviewedFailedBrokers({ ...receipt, targets: [receipt.targets[2]] }, 'workers', 'upsert', ['a', 'b'], ['a', 'b'])).toThrow();
    });
    it('rejects retries for another group, operation, new target or disappeared Broker', () => {
        expect(() => reviewedFailedBrokers(receipt, 'other', 'upsert', ['b'], ['b'])).toThrow();
        expect(() => reviewedFailedBrokers(receipt, 'workers', 'delete', ['b'], ['b'])).toThrow();
        expect(() => reviewedFailedBrokers(receipt, 'workers', 'upsert', ['a'], ['a', 'b'])).toThrow();
        expect(() => reviewedFailedBrokers(receipt, 'workers', 'upsert', ['a', 'b'], ['a'])).toThrow();
    });
    it('rejects protected input while allowing a normalized system identity only for reads', () => {
        expect(() => validateConsumerDraft({ ...draft, consumerGroup: ' %SYS%internal ' }, targets)).toThrow();
        expect(checkConsumerIdentity({ consumerGroup: 'internal' }, '%SYS%internal')).toEqual({ consumerGroup: 'internal' });
        expect(() => checkConsumerIdentity({ consumerGroup: 'another-group' }, 'workers')).toThrow();
    });
    it('aggregates only reported queues and preserves missing Broker coverage', () => {
        const topic = { queueStatInfoList: [{ brokerName: 'a', diffTotal: 7 }, { brokerName: 'a', diffTotal: 5 }, { brokerName: 'b', diffTotal: 0 }] } as ConsumerTopicDetailItem;
        const values = topicLagByBroker(topic);
        expect(values.get('a')).toBe(12); expect(values.get('b')).toBe(0); expect(values.has('missing')).toBe(false);
    });
});
