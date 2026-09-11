import { createContext, useContext } from 'react';
import type { MessageSummary } from './types/message.types';

export type OpenDirectConsume = (message: MessageSummary, consumerGroup?: string) => void;
export const MessageActionContext = createContext<OpenDirectConsume | null>(null);
export function useDirectConsume() {
    const open = useContext(MessageActionContext);
    if (!open) throw new Error('Message actions require their authenticated session provider.');
    return open;
}
