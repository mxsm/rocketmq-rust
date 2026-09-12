import type { ConsumerQueryScope } from '../features/consumer/types/consumer.types';
import React, { createContext, useContext, useEffect, useState, useReducer, useRef, useSyncExternalStore, ReactNode } from 'react';
import { subscribeAuditWarning } from '../services/invoke';
import { SessionStorageService } from '../services/session.storage';
import type { SessionUser } from '../features/auth/types/auth.types';

import { ConnectionStore } from '../services/connection.store';
import { initialNavigation, navigationReducer, type Tab, type EntityTarget, type NavigationLocation } from './navigation';

interface AppState {
  isLoggedIn: boolean;
  isBootstrappingAuth: boolean;
  sessionId: string | null;
  currentUser: SessionUser | null;
  mustChangePassword: boolean;
  activeTab: Tab;
  navigation: NavigationLocation;
  canGoBack: boolean;
  consumerQueryMode: ConsumerQueryScope['mode'];
  setConsumerQueryMode: (mode: ConsumerQueryScope['mode']) => void;
  goBack: () => void;
  openTopic: (name: string, detail?: Extract<EntityTarget, { kind: 'topic' }>['detail']) => void;
  openConsumer: (name: string, detail?: Extract<EntityTarget, { kind: 'consumer' }>['detail'], scope?: ConsumerQueryScope) => void;
  openBroker: (address: string, detail?: Extract<EntityTarget, { kind: 'broker' }>['detail']) => void;
  openTrace: (messageId: string, topic: string) => void;
  openMessage: (messageId: string, topic: string) => void;
  pageStates: React.MutableRefObject<Map<number, Record<string, unknown>>>;
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
  const [navigationState, navigate] = useReducer(navigationReducer, initialNavigation);
  const [consumerQueryMode, setConsumerQueryMode] = useState<ConsumerQueryScope['mode']>('name_server');
  const pageStates = useRef(new Map<number, Record<string, unknown>>());
  const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
  const environmentId = settings?.environmentId ?? null;
  const connectionScope = `${environmentId}:${settings?.currentProxyId ?? ''}`;
  const previousEnvironment = useRef(connectionScope);
  const activeTab = navigationState.current.tab;
  useEffect(() => {
    if (previousEnvironment.current !== connectionScope) {
      previousEnvironment.current = connectionScope;
      pageStates.current.clear();
      navigate({ type: 'reset', environmentId });
    }
  }, [connectionScope, environmentId]);
  useEffect(() => {
    const retained = new Set([navigationState.current.id, ...navigationState.history.map((entry) => entry.id)]);
    for (const id of pageStates.current.keys()) if (!retained.has(id)) pageStates.current.delete(id);
  }, [navigationState]);
  const setActiveTab = (tab: Tab) => navigate({ type: 'open', tab, environmentId });

  const getPageTitle = (tab: Tab) => {
    switch (tab) {
      case 'NameServer':
        return 'NameServer';
      case 'Proxy':
        return 'Proxy';
      case 'Dashboard':
        return 'System Dashboard';
      case 'Cluster':
        return 'Cluster';
      case 'Topic':
        return 'Topics';
      case 'Consumer':
        return 'Consumers';
      case 'Producer':
        return 'Producers';
      case 'Message':
        return 'Messages';
      case 'MessageTrace':
        return 'Message Trace';
      case 'DLQ':
        return 'DLQ Message Management';
      case 'ACL':
        return 'ACL Management';
      case 'Audit':
        return 'Audit Events';
      case 'Sessions':
        return 'Account Sessions';
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
    pageStates.current.clear();
    navigate({ type: 'reset', environmentId: null });
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
        navigation: navigationState.current.environmentId === environmentId ? navigationState.current : { ...navigationState.current, target: null },
        canGoBack: navigationState.current.environmentId === environmentId && navigationState.history.length > 0,
        goBack: () => navigate({ type: 'back' }),
        openTopic: (name, detail = 'overview') => navigate({ type: 'open', tab: 'Topic', target: { kind: 'topic', name, detail }, environmentId }),
        openConsumer: (name, detail = 'overview', scope = { mode: 'name_server' }) => {
          setConsumerQueryMode(scope.mode);
          navigate({ type: 'open', tab: 'Consumer', target: { kind: 'consumer', name, detail, scope }, environmentId });
        },
        openBroker: (address, detail = 'overview') => navigate({ type: 'open', tab: 'Cluster', target: { kind: 'broker', address, detail }, environmentId }),
        openTrace: (name, topic) => navigate({ type: 'open', tab: 'MessageTrace', target: { kind: 'trace', name, topic }, environmentId }),
        openMessage: (name, topic) => navigate({ type: 'open', tab: 'Message', target: { kind: 'message', name, topic }, environmentId }),
        pageStates, consumerQueryMode, setConsumerQueryMode,
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

// Each history entry keeps its own list position and selection; secrets never belong here.
export function useNavigationState<T>(key: string, initial: T | (() => T)) {
  const { navigation, pageStates } = useAppStore();
  const id = navigation.id;
  const [value, setValue] = useState<T>(() => {
    const saved = pageStates.current.get(id);
    return saved && key in saved ? saved[key] as T : typeof initial === 'function' ? (initial as () => T)() : initial;
  });
  const update: React.Dispatch<React.SetStateAction<T>> = (next) => setValue((previous) => {
    const resolved = typeof next === 'function' ? (next as (value: T) => T)(previous) : next;
    const saved = pageStates.current.get(id) ?? {};
    pageStates.current.set(id, { ...saved, [key]: resolved });
    return resolved;
  });
  return [value, update] as const;
}
