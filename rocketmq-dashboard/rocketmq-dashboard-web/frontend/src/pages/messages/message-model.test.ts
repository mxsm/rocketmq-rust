import { describe, expect, it } from 'vitest';
import type { MessageTraceNode, MessageView } from '../../types/message';
import { formatMessageSize, messageKeys, messageTags, sortedMessageProperties, sortTraceNodes, traceNodeTone } from './message-model';

const message: MessageView = {
  topic: 'orders',
  messageId: 'MSG-001',
  keys: null,
  tags: null,
  bornTimestamp: 1_723_651_200_000,
  storeTimestamp: 1_723_651_201_000,
  bornHost: '10.0.0.1:10911',
  storeHost: '10.0.0.2:10911',
  queueId: 3,
  queueOffset: 42,
  storeSize: 1_536,
  reconsumeTimes: 2,
  bodyCRC: 1,
  sysFlag: 0,
  flag: 0,
  preparedTransactionOffset: 0,
  body: '{"orderId":1289347}',
  properties: { TAGS: 'TagA', KEYS: 'order:1289347', WAIT: 'true' }
};

describe('message model', () => {
  it('formats API-backed size, tag, key, and sorted properties', () => {
    expect(formatMessageSize(message.storeSize)).toBe('1.5 KB');
    expect(messageTags(message)).toBe('TagA');
    expect(messageKeys(message)).toBe('order:1289347');
    expect(sortedMessageProperties(message)).toEqual([
      ['KEYS', 'order:1289347'],
      ['TAGS', 'TagA'],
      ['WAIT', 'true']
    ]);
  });

  it('sorts a copied trace-node array by timestamp without mutating the response', () => {
    const nodes: MessageTraceNode[] = [
      { nodeType: 'CONSUMER', name: 'order-service', status: 'SUCCESS', timestamp: 30 },
      { nodeType: 'PRODUCER', name: 'order-producer', status: 'SUCCESS', timestamp: 10 },
      { nodeType: 'BROKER', name: 'broker-a', status: 'STORED', timestamp: 20 }
    ];

    expect(sortTraceNodes(nodes).map((node) => node.timestamp)).toEqual([10, 20, 30]);
    expect(nodes.map((node) => node.timestamp)).toEqual([30, 10, 20]);
    expect(traceNodeTone({ nodeType: 'CONSUMER', name: 'order-service', status: 'CONSUMED', timestamp: 30 })).toBe('success');
  });
});
