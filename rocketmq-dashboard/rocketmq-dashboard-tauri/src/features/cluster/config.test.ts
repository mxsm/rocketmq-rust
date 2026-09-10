import { describe, expect, it } from 'vitest';
import { changedBrokerConfig } from './config';

describe('Broker configuration patches', () => {
    const original = { brokerPermission: '6', listenPort: '10911' };
    it('submits only explicitly changed string values', () => {
        expect(changedBrokerConfig(JSON.stringify({ ...original, brokerPermission: '4' }), original)).toEqual({ brokerPermission: '4' });
        expect(changedBrokerConfig(JSON.stringify(original), original)).toEqual({});
        expect(changedBrokerConfig('{"newProperty":""}', {})).toEqual({ newProperty: '' });
    });
    it('rejects invalid JSON, non-string values, missing keys and property injection', () => {
        for (const text of ['{', 'null', '[]', '{"brokerPermission":6}', '{"brokerPermission":true}', '{"": "x"}', '{"a=b":"x"}', '{"a":"b\\nc=d"}']) {
            expect(() => changedBrokerConfig(text, {})).toThrow();
        }
        expect(() => changedBrokerConfig('{"brokerPermission":"4"}', original)).toThrow('Removing configuration keys');
    });
});
