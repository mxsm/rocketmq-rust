import { createContext, useContext } from 'react';
import type { DlqReceipt } from './receipts';
import type { DlqActionInput } from './dlqTargets';

export type OpenDlqAction = (input: DlqActionInput) => void;
export const DlqActionContext = createContext<{ open: OpenDlqAction; receipt: DlqReceipt | null }>({ open: () => {}, receipt: null });
export const useDlqActions = () => useContext(DlqActionContext);
