import type { ResetOffsetRequest } from './types/topic.types';

export function offsetResetRequest(topic: string, group: string, localTime: string, force: boolean): ResetOffsetRequest {
    if (!topic.trim() || !group.trim()) throw new Error('Select a Topic and Consumer group.');
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(localTime);
    if (!parts) throw new Error('Select a valid local date and time.');
    const [year, month, day, hour, minute] = parts.slice(1).map(Number);
    const date = new Date(localTime);
    const resetTime = date.getTime();
    if (!Number.isSafeInteger(resetTime) || resetTime < 0 || date.getFullYear() !== year || date.getMonth() + 1 !== month ||
        date.getDate() !== day || date.getHours() !== hour || date.getMinutes() !== minute) {
        throw new Error('The selected local time is invalid or does not exist in this time zone.');
    }
    return { topic, consumerGroupList: [group], resetTime, force };
}

export const currentLocalMinute = () => {
    const date = new Date();
    return new Date(date.getTime() - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
};
