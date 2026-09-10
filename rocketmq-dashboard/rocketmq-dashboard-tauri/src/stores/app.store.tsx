import React, { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { subscribeAuditWarning } from '../services/invoke';
import { SessionStorageService } from '../services/session.storage';
import type { SessionUser } from '../features/auth/types/auth.types';

type Tab =
  | 'NameServer'
  | 'Proxy'
  | 'Dashboard'
  | 'Cluster'
  | 'Topic'
  | 'Consumer'
  | 'Producer'
  | 'Message'
  | 'MessageTrace'
  | 'DLQ'
  | 'ACL'
  | 'Account'
  | 'Audit';

interface AppState {
  isLoggedIn: boolean;
  isBootstrappingAuth: boolean;
  sessionId: string | null;
  currentUser: SessionUser | null;
  mustChangePassword: boolean;
  activeTab: Tab;
  setActiveTab: (tab: Tab) => void;
  setAuthSession: (sessionId: string, currentUser: SessionUser) => void;
  clearAuthSession: () => void;
  startAuthBootstrap: () => void;
  finishAuthBootstrap: () => void;
  pageTitle: string;
}

const AppContext = createContext<AppState | undefined>(undefined);

export const AppProvider = ({ children }: { children: ReactNode }) => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [isBootstrappingAuth, setIsBootstrappingAuth] = useState(true);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [currentUser, setCurrentUser] = useState<SessionUser | null>(null);
  const [mustChangePassword, setMustChangePassword] = useState(false);
  const [auditWarning, setAuditWarning] = useState<string | null>(null);
  useEffect(() => subscribeAuditWarning(setAuditWarning), []);
  const [activeTab, setActiveTab] = useState<Tab>('Dashboard');

  const getPageTitle = (tab: Tab) => {
    switch (tab) {
      case 'NameServer':
        return 'NameServer Management';
      case 'Proxy':
        return 'Proxy Management';
      case 'Dashboard':
        return 'System Dashboard';
      case 'Cluster':
        return 'Cluster Management';
      case 'Topic':
        return 'Topic Management';
      case 'Consumer':
        return 'Consumer Management';
      case 'Producer':
        return 'Producer Management';
      case 'Message':
        return 'Message Query';
      case 'MessageTrace':
        return 'Message Trace';
      case 'DLQ':
        return 'DLQ Message Management';
      case 'ACL':
        return 'ACL Management';
      case 'Audit':
        return 'Audit Events';
      case 'Account':
        return 'Account Overview';
      default:
        return tab;
    }
  };

  const setAuthSession = (nextSessionId: string, nextUser: SessionUser) => {
    setSessionId(nextSessionId);
    setCurrentUser(nextUser);
    setMustChangePassword(nextUser.mustChangePassword);
    setIsLoggedIn(true);
  };

  const clearAuthSession = () => {
    setSessionId(null);
    setCurrentUser(null);
    setMustChangePassword(false);
    setIsLoggedIn(false);
  };

  useEffect(() => SessionStorageService.subscribeAuthenticationFailure((reason) => {
    if (reason === 'invalid') {
      clearAuthSession();
    } else {
      setMustChangePassword(true);
      setCurrentUser((user) => user ? { ...user, mustChangePassword: true } : user);
    }
  }), []);

  return (
    <AppContext.Provider
      value={{
        isLoggedIn,
        isBootstrappingAuth,
        sessionId,
        currentUser,
        mustChangePassword,
        activeTab,
        setActiveTab,
        setAuthSession,
        clearAuthSession,
        startAuthBootstrap: () => setIsBootstrappingAuth(true),
        finishAuthBootstrap: () => setIsBootstrappingAuth(false),
        pageTitle: getPageTitle(activeTab),
      }}
    >
      {children}
      {auditWarning && <div role="alert" className="fixed bottom-4 left-4 right-4 z-[100] flex items-center justify-between gap-4 rounded-xl border border-amber-400 bg-amber-50 p-4 text-sm text-amber-950 shadow-lg">
        <p>{auditWarning}</p><button type="button" className="font-semibold underline" onClick={() => setAuditWarning(null)}>Dismiss</button>
      </div>}
    </AppContext.Provider>
  );
};

export const useAppStore = () => {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error('useAppStore must be used within an AppProvider');
  }
  return context;
};
