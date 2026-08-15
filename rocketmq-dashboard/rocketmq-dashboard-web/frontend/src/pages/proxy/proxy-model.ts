export type ProxyEndpointLabel = 'Current' | 'Available';

function toAsciiLowercase(value: string) {
  return value.replace(/[A-Z]/g, (character) => character.toLowerCase());
}

export function normalizeProxyAddress(address: string) {
  const trimmed = address.trim();
  const separatorIndex = trimmed.lastIndexOf(':');
  if (separatorIndex < 0) return trimmed;

  const host = trimmed.slice(0, separatorIndex).trim();
  const port = trimmed.slice(separatorIndex + 1).trim();
  if (!host || /\s/.test(host) || !/^\+?\d+$/.test(port)) return trimmed;

  const portNumber = Number(port);
  if (!Number.isInteger(portNumber) || portNumber < 0 || portNumber > 65_535) return trimmed;

  return `${toAsciiLowercase(host)}:${portNumber}`;
}

export function getProxyEndpointLabel(address: string, currentProxyAddress?: string | null): ProxyEndpointLabel {
  return normalizeProxyAddress(address) === normalizeProxyAddress(currentProxyAddress ?? '')
    && normalizeProxyAddress(currentProxyAddress ?? '')
    ? 'Current'
    : 'Available';
}

export function isDuplicateProxyAddress(address: string, proxyAddresses: string[]) {
  const normalizedAddress = normalizeProxyAddress(address);
  return normalizedAddress !== '' && proxyAddresses.some((proxyAddress) => normalizeProxyAddress(proxyAddress) === normalizedAddress);
}
