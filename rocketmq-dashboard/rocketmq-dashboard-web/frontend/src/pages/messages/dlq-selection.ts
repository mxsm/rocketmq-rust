import type { MessageView } from '../../types/message';

export function selectionForPage(current: Set<string>, pageMessageIds: string[]) {
  const pageIds = new Set(pageMessageIds);
  return new Set([...current].filter((messageId) => pageIds.has(messageId)));
}

export function toggleMessageSelection(current: Set<string>, messageId: string, selected: boolean) {
  const next = new Set(current);
  if (selected) next.add(messageId);
  else next.delete(messageId);
  return next;
}

export function messageRowId(message: MessageView) {
  const storeMessageId = message.properties.STORE_MESSAGE_ID?.trim();
  if (storeMessageId) return JSON.stringify(['store', storeMessageId]);
  return JSON.stringify([message.topic, message.storeHost, message.queueId, message.queueOffset]);
}

export function messageTraceId(message: MessageView) {
  return message.properties.STORE_MESSAGE_ID?.trim() || message.messageId;
}

export function dlqResendTarget(message: MessageView) {
  const topicName = message.properties.RETRY_TOPIC?.trim();
  const msgId = (message.properties.ORIGIN_MESSAGE_ID || message.properties.DLQ_ORIGIN_MESSAGE_ID)?.trim();
  return topicName && msgId ? { topicName, msgId } : null;
}

export function uniqueDlqResendTargets(messages: MessageView[], selectedIds: Set<string>) {
  const targets = new Map<string, NonNullable<ReturnType<typeof dlqResendTarget>>>();
  for (const message of messages) {
    if (!selectedIds.has(messageRowId(message))) continue;
    const target = dlqResendTarget(message);
    if (target) targets.set(`${target.topicName}\u0000${target.msgId}`, target);
  }
  return [...targets.values()];
}

export function messageResendTarget(message: MessageView) {
  if (!message.topic.startsWith('%DLQ%')) {
    const storeMessageId = message.properties.STORE_MESSAGE_ID?.trim();
    return storeMessageId ? { topic: message.topic, messageId: storeMessageId } : null;
  }
  const target = dlqResendTarget(message);
  return target ? { topic: target.topicName, messageId: target.msgId } : null;
}
