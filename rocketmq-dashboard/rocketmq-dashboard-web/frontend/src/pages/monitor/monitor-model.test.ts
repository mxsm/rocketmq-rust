import { parseConsumerMonitorDraft } from './monitor-model';

describe('parseConsumerMonitorDraft', () => {
  it.each([
    ['-1', '1000'],
    ['1.5', '1000'],
    ['1', '-1000'],
    ['1', '1000.5']
  ])('rejects negative and non-integer threshold values (%s, %s)', (minCount, maxDiffTotal) => {
    expect(parseConsumerMonitorDraft({
      consumerGroup: 'order-service',
      minCount,
      maxDiffTotal
    }).ok).toBe(false);
  });

  it('maps a valid draft to the persisted monitor request DTO', () => {
    expect(parseConsumerMonitorDraft({
      consumerGroup: ' order-service ',
      minCount: '7',
      maxDiffTotal: '2400'
    })).toEqual({
      ok: true,
      value: {
        consumerGroup: 'order-service',
        minCount: 7,
        maxDiffTotal: 2400
      }
    });
  });
});
