import { createContext, useContext } from 'react';
import type { ConsumerGroupListItem, ConsumerQueryScope } from './types/consumer.types';

export type ConsumerAction = { kind: 'create'; scope: ConsumerQueryScope }
    | { kind: 'edit'; scope: ConsumerQueryScope; consumer: ConsumerGroupListItem; address?: string }
    | { kind: 'delete'; scope: ConsumerQueryScope; consumer: ConsumerGroupListItem }
    | { kind: 'reset'; scope: ConsumerQueryScope; consumer: ConsumerGroupListItem; topic: string };
export type OpenConsumerAction = (action: ConsumerAction, onClosed: () => void) => void;
export const ConsumerActionContext = createContext<OpenConsumerAction | null>(null);
export function useConsumerAction() {
    const open = useContext(ConsumerActionContext);
    if (!open) throw new Error('Consumer actions require their session provider.');
    return open;
}
