import { describe, expect, it } from 'vitest';
import {
  getProxyEndpointLabel,
  isDuplicateProxyAddress,
  normalizeProxyAddress
} from './proxy-model';

describe('proxy endpoint model', () => {
  it('canonicalizes host casing and decimal ports like the backend', () => {
    expect(normalizeProxyAddress(' PROXY-A:08081 ')).toBe('proxy-a:8081');
    expect(normalizeProxyAddress(' PROXY-A:+08081 ')).toBe('proxy-a:8081');
    expect(normalizeProxyAddress(' ÄPROXY-A:08081 ')).toBe('Äproxy-a:8081');
  });

  it('preserves invalid trimmed endpoint input for backend validation', () => {
    expect(normalizeProxyAddress(' proxy-a:not-a-port ')).toBe('proxy-a:not-a-port');
    expect(normalizeProxyAddress(' proxy-a:++8081 ')).toBe('proxy-a:++8081');
  });

  it('labels only the configured current endpoint as current', () => {
    expect(getProxyEndpointLabel('proxy-a:8081', 'proxy-a:8081')).toBe('Current');
    expect(getProxyEndpointLabel('proxy-b:8081', 'proxy-a:8081')).toBe('Available');
    expect(getProxyEndpointLabel('proxy-a:8081', null)).toBe('Available');
  });

  it('prevents additions that duplicate an existing normalized endpoint', () => {
    expect(isDuplicateProxyAddress(' PROXY-A:08081 ', ['proxy-a:8081', 'proxy-b:8081'])).toBe(true);
    expect(isDuplicateProxyAddress(' PROXY-A:+08081 ', ['proxy-a:8081', 'proxy-b:8081'])).toBe(true);
    expect(isDuplicateProxyAddress('proxy-c:8081', ['proxy-a:8081', 'proxy-b:8081'])).toBe(false);
  });
});
