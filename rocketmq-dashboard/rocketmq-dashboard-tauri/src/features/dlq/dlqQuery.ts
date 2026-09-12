import { DlqService } from '../../services/dlq.service';
import { buildMessageQuery, createMessageQueryController, type MessageQueryDraft } from '../message/messageQuery';
import { checkMessageDetail } from '../message/messageModel';

export interface DlqQueryDraft extends Omit<MessageQueryDraft, 'topic'> { consumerGroup: string; }

export function dlqGroup(value: string) {
    const group = value.trim().replace(/^%DLQ%/, '').trim();
    if (!group) throw new Error('Enter a Consumer group.');
    return group;
}

export function buildDlqQuery(draft: DlqQueryDraft) {
    return buildMessageQuery({ ...draft, topic: '%DLQ%' + dlqGroup(draft.consumerGroup) });
}

/** Reuse message pagination and stale-read ownership, with DLQ-specific authenticated lookups. */
export function createDlqQueryController(contextIsCurrent: () => boolean,
    service: Pick<typeof DlqService, 'queryDlqMessageByConsumerGroup' | 'viewDlqMessageDetail'> = DlqService) {
    return createMessageQueryController({
        queryMessageByTopicKey: async ({ topic, key }) => {
            const result = await service.queryDlqMessageByConsumerGroup({ consumerGroup: dlqGroup(topic), key,
                begin: 0, end: Date.now(), pageNum: 1, pageSize: 64 });
            return { items: result.page.content, total: result.page.totalElements };
        },
        queryMessageById: async ({ topic, messageId }) => {
            const detail = checkMessageDetail(await service.viewDlqMessageDetail({ consumerGroup: dlqGroup(topic), messageId }),
                { topic, msgId: messageId, queryMsgId: messageId, storeTimestamp: 0 });
            return { items: [{ topic: detail.topic, msgId: detail.properties.UNIQ_KEY?.trim() || detail.msgId,
                queryMsgId: detail.msgId, tags: detail.properties.TAGS, keys: detail.properties.KEYS, storeTimestamp: detail.storeTimestamp ?? 0 }], total: 1 };
        },
        queryMessagePageByTopic: ({ topic, ...request }) => service.queryDlqMessageByConsumerGroup({ ...request, consumerGroup: dlqGroup(topic) }),
    }, contextIsCurrent);
}
