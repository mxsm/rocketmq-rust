import { expect, it } from 'vitest';
import { captureDlqTargets, type DlqActionInput } from './dlqTargets';

const context = { revision: 4, environmentId: 'local' };
const input = (): DlqActionInput => ({ action: 'resend', context, group: '%DLQ%g', clientId: ' client ',
    messages: [{ topic: '%DLQ%g', msgId: 'unique', queryMsgId: 'physical', storeTimestamp: 0 }] });

it('captures physical identities once and preserves the chosen client independently of later edits', () => {
    const draft = input(); draft.messages.push({ ...draft.messages[0] });
    const captured = captureDlqTargets(draft, context);
    draft.messages[0].queryMsgId = 'changed'; draft.clientId = 'changed';
    expect(captured.messages).toEqual([{ consumerGroup: 'g', messageId: 'physical', clientId: 'client' }]);
});

it('rejects stale results before opening an operation in a different connection', () => {
    expect(() => captureDlqTargets(input(), { ...context, revision: 5 })).toThrow('connection changed');
    expect(() => captureDlqTargets(input(), { ...context, environmentId: 'other' })).toThrow('connection changed');
    expect(() => captureDlqTargets(input(), null)).toThrow('connection changed');
});

it('rejects wrong-group, empty and oversized target sets', () => {
    const draft = input(); draft.messages[0].topic = '%DLQ%other';
    expect(() => captureDlqTargets(draft, context)).toThrow('DLQ record');
    expect(() => captureDlqTargets({ ...input(), messages: [] }, context)).toThrow('1 and 256');
    expect(() => captureDlqTargets({ ...input(), messages: Array.from({ length: 257 }, (_, index) => ({ ...input().messages[0], queryMsgId: String(index) })) }, context)).toThrow('1 and 256');
});

it('preserves explicit automatic-client selection without inventing a target', () => {
    expect(captureDlqTargets({ ...input(), clientId: ' ' }, context).messages[0].clientId).toBeUndefined();
});
