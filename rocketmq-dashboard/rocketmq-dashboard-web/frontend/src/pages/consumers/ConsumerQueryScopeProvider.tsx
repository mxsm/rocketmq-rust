import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { configApi } from '../../api/config_api';
import type { ConsumerQueryMode, ConsumerQueryScope } from '../../types/consumer';

const STORAGE_KEY = 'rocketmq.consumer.queryMode';

interface ConsumerQueryScopeContextValue {
  scope: ConsumerQueryScope;
  configLoading: boolean;
  configError: string | null;
  proxyAvailable: boolean;
  setMode: (mode: ConsumerQueryMode) => void;
  refreshProxyConfig: () => Promise<void>;
  revision: number;
}

const ConsumerQueryScopeContext = createContext<ConsumerQueryScopeContextValue | null>(null);

function readStoredMode(): ConsumerQueryMode {
  return window.localStorage.getItem(STORAGE_KEY) === 'proxy' ? 'proxy' : 'nameServer';
}

export function ConsumerQueryScopeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ConsumerQueryMode>(readStoredMode);
  const [currentProxy, setCurrentProxy] = useState<string | null>(null);
  const [proxyList, setProxyList] = useState<string[]>([]);
  const [configLoading, setConfigLoading] = useState(true);
  const [configError, setConfigError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  const requestToken = useRef(0);
  const mountedRef = useRef(false);
  const lastProxyRef = useRef<string | null | undefined>(undefined);

  const applyConfig = useCallback((current: string | null, list: string[]) => {
    setProxyList(list);
    const previous = lastProxyRef.current;
    if (previous !== undefined && previous !== current) {
      setRevision((value) => value + 1);
    }
    lastProxyRef.current = current;
    setCurrentProxy(current);
  }, []);

  const loadConfig = useCallback(async () => {
    const token = ++requestToken.current;
    setConfigLoading(true);
    setConfigError(null);
    try {
      const config = await configApi.getConfig();
      if (token !== requestToken.current) return;
      applyConfig(config.currentProxyAddr ?? null, config.proxyAddrList);
    } catch (error) {
      if (token === requestToken.current) {
        setConfigError(error instanceof Error ? error.message : String(error));
      }
    } finally {
      if (token === requestToken.current) setConfigLoading(false);
    }
  }, [applyConfig]);

  useEffect(() => {
    mountedRef.current = true;
    void loadConfig();
    return () => {
      mountedRef.current = false;
      requestToken.current += 1;
    };
  }, [loadConfig]);

  const setMode = useCallback((next: ConsumerQueryMode) => {
    setModeState(next);
    window.localStorage.setItem(STORAGE_KEY, next);
  }, []);

  const refreshProxyConfig = useCallback(async () => {
    await loadConfig();
  }, [loadConfig]);

  const proxyAvailable = Boolean(
    currentProxy && currentProxy.trim() !== '' && proxyList.some((address) => address === currentProxy)
  );

  const scope = useMemo<ConsumerQueryScope>(() => {
    if (mode === 'proxy' && currentProxy) {
      return { mode: 'proxy', proxyAddress: currentProxy };
    }
    return { mode };
  }, [mode, currentProxy]);

  const value = useMemo<ConsumerQueryScopeContextValue>(
    () => ({
      scope,
      configLoading,
      configError,
      proxyAvailable,
      setMode,
      refreshProxyConfig,
      revision
    }),
    [scope, configLoading, configError, proxyAvailable, setMode, refreshProxyConfig, revision]
  );

  return (
    <ConsumerQueryScopeContext.Provider value={value}>
      {children}
    </ConsumerQueryScopeContext.Provider>
  );
}

export function useConsumerQueryScope(): ConsumerQueryScopeContextValue {
  const context = useContext(ConsumerQueryScopeContext);
  if (!context) {
    throw new Error('useConsumerQueryScope must be used within ConsumerQueryScopeProvider');
  }
  return context;
}
