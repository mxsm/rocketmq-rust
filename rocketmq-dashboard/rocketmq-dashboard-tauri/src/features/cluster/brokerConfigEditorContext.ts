import { createContext, useContext } from 'react';
import type { BrokerIdentity } from './brokerIdentity';

export type OpenBrokerConfigEditor = (broker: BrokerIdentity, onClosed: () => void) => void;
export const BrokerConfigEditorContext = createContext<OpenBrokerConfigEditor | null>(null);

export function useBrokerConfigEditor(): OpenBrokerConfigEditor {
    const open = useContext(BrokerConfigEditorContext);
    if (!open) throw new Error('Broker configuration editing requires its dialog provider.');
    return open;
}
