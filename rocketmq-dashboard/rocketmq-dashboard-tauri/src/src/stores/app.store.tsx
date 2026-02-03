import React, { createContext, useContext, useState, ReactNode } from 'react';

type Tab = 'OPS' | 'Proxy' | 'Dashboard' | 'Cluster' | 'Topic' | 'Consumer' | 'Producer' | 'Message' | 'MessageTrace' | 'DLQ' | 'ACL';

interface AppState {
  isLoggedIn: boolean;
  setIsLoggedIn: (value: boolean) => void;
  activeTab: string;
  setActiveTab: (tab: string) => void;
  pageTitle: string;
}

const AppContext = createContext<AppState | undefined>(undefined);

export const AppProvider = ({ children }: { children: ReactNode }) => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [activeTab, setActiveTab] = useState('Dashboard');

  const getPageTitle = (tab: string) => {
    switch(tab) {
      case 'OPS': return 'NameServer Management';
      case 'Proxy': return 'Proxy Management';
      case 'Dashboard': return 'System Dashboard';
      case 'Cluster': return 'Cluster Management';
      case 'Topic': return 'Topic Management';
      case 'Consumer': return 'Consumer Management';
      case 'Producer': return 'Producer Management';
      case 'Message': return 'Message Query';
      case 'MessageTrace': return 'Message Trace';
      case 'DLQ': return 'DLQ Message Management';
      case 'ACL': return 'ACL Management';
      default: return tab;
    }
  };

  return (
    <AppContext.Provider value={{
      isLoggedIn,
      setIsLoggedIn,
      activeTab,
      setActiveTab,
      pageTitle: getPageTitle(activeTab)
    }}>
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
