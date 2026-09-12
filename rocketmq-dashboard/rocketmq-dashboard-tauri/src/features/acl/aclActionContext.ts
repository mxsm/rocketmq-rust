import { createContext, useContext } from 'react';
import type { AclAction, AclContext, AclReceipt } from './aclActions';
import type { AclScope } from './types';

interface AclActions { open: (action: AclAction, scope: AclScope, context: AclContext) => void; receipt: AclReceipt | null }
export const AclActionContext = createContext<AclActions>({ open: () => undefined, receipt: null });
export const useAclActions = () => useContext(AclActionContext);
