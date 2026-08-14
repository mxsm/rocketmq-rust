import type { MessageTraceNode, MessageView } from '../../types/message';

export function messageTags(message: MessageView | null | undefined) {
  return message?.tags || message?.properties.TAGS || '-';
}

export function messageKeys(message: MessageView | null | undefined) {
  return message?.keys || message?.properties.KEYS || '-';
}

export function sortedMessageProperties(message: MessageView) {
  return Object.entries(message.properties).sort(([left], [right]) => left.localeCompare(right));
}

export function formatMessageSize(bytes: number) {
  if (!Number.isFinite(bytes) || bytes < 0) return '-';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${formatDecimal(bytes / 1024)} KB`;
  return `${formatDecimal(bytes / (1024 * 1024))} MB`;
}

export function formatMessageTimestamp(timestamp: number) {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return '-';
  return new Date(timestamp).toLocaleString();
}

export function formatMessageBody(body: string) {
  if (!body) return '';
  try {
    return JSON.stringify(JSON.parse(body), null, 2);
  } catch {
    return body;
  }
}

export function truncateIdentifier(value: string, maxLength = 30) {
  if (value.length <= maxLength) return value;
  const edge = Math.floor((maxLength - 3) / 2);
  return `${value.slice(0, edge)}...${value.slice(-edge)}`;
}

export function sortTraceNodes(nodes: MessageTraceNode[]) {
  return [...nodes].sort((left, right) => left.timestamp - right.timestamp);
}

export function traceNodeTone(node: MessageTraceNode) {
  const value = `${node.status} ${node.nodeType}`.toLowerCase();
  if (value.includes('fail') || value.includes('error') || value.includes('timeout')) return 'danger' as const;
  if (value.includes('success') || value.includes('sent') || value.includes('stored') || value.includes('consumed') || value.includes('ok')) return 'success' as const;
  return 'warning' as const;
}

function formatDecimal(value: number) {
  return value.toFixed(1).replace(/\.0$/, '');
}
