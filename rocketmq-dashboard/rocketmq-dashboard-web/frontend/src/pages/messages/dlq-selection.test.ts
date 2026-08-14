import { describe, expect, it } from 'vitest';
import type { MessageView } from '../../types/message';
import { dlqResendTarget, messageResendTarget, selectionForPage, toggleMessageSelection } from './dlq-selection';

const message = (properties: Record<string, string>): MessageView => ({
  topic: '%DLQ%order-service', messageId: 'DLQ-001', bornTimestamp: 1, storeTimestamp: 2,
  bornHost: 'born', storeHost: 'stored', queueId: 0, queueOffset: 1, storeSize: 1,
  reconsumeTimes: 16, bodyCRC: 1, sysFlag: 0, flag: 0, preparedTransactionOffset: 0,
  body: 'secret', properties
});

describe('DLQ page selection', () => {
  it('retains selections still present on the page and drops old-page identifiers', () => {
    expect([...selectionForPage(new Set(['old-1', 'same']), ['same', 'new-1'])]).toEqual(['same']);
  });

  it('adds and removes a message without mutating the current Set', () => {
    const current = new Set(['a']);
    expect([...toggleMessageSelection(current, 'b', true)]).toEqual(['a', 'b']);
    expect([...toggleMessageSelection(current, 'a', false)]).toEqual([]);
    expect([...current]).toEqual(['a']);
  });

  it('derives canonical DLQ resend metadata and fails closed when either property is absent', () => {
    expect(dlqResendTarget(message({ RETRY_TOPIC: 'orders', DLQ_ORIGIN_MESSAGE_ID: 'MSG-001' }))).toEqual({
      topicName: 'orders', msgId: 'MSG-001'
    });
    expect(dlqResendTarget(message({ RETRY_TOPIC: 'orders' }))).toBeNull();
    expect(dlqResendTarget(message({ ORIGIN_MESSAGE_ID: 'MSG-001' }))).toBeNull();
    expect(messageResendTarget(message({}))).toBeNull();
    expect(messageResendTarget({ ...message({}), topic: 'orders', messageId: 'MSG-002' })).toBeNull();
    expect(messageResendTarget({
      ...message({ STORE_MESSAGE_ID: 'STORE-002' }), topic: 'orders', messageId: 'MSG-002'
    })).toEqual({
      topic: 'orders', messageId: 'STORE-002'
    });
  });
});
