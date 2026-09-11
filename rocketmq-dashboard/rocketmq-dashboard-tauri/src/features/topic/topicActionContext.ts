import { createContext, useContext } from 'react';
import type { TopicListItem, TopicTargetOption } from './types/topic.types';

export type TopicAction =
    | { kind: 'create'; targets: TopicTargetOption[] }
    | { kind: 'edit'; topic: TopicListItem; targets: TopicTargetOption[]; brokerName?: string }
    | { kind: 'send'; topic: TopicListItem }
    | { kind: 'delete'; topic: TopicListItem }
    | { kind: 'reset'; topic: TopicListItem }
    | { kind: 'skip'; topic: TopicListItem }
    | { kind: 'delete_broker'; topic: TopicListItem; brokerName: string };
export type OpenTopicAction = (action: TopicAction, onClosed: () => void) => void;
export const TopicActionContext = createContext<OpenTopicAction | null>(null);
export function useTopicAction() {
    const open = useContext(TopicActionContext);
    if (!open) throw new Error('Topic operations require their dialog provider.');
    return open;
}
