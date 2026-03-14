import React, { createContext, useContext, useState, ReactNode } from 'react';
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
  | 'ACL';

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
  markPasswordChanged: () => void;
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

  const markPasswordChanged = () => {
    setMustChangePassword(false);
    setCurrentUser((previousUser) =>
      previousUser
        ? {
            ...previousUser,
            mustChangePassword: false,
          }
        : previousUser
    );
  };

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
        markPasswordChanged,
        startAuthBootstrap: () => setIsBootstrappingAuth(true),
        finishAuthBootstrap: () => setIsBootstrappingAuth(false),
        pageTitle: getPageTitle(activeTab),
      }}
    >
      {children}
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
