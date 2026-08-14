import type { DashboardConfigView } from '../types/config';
import type { SessionView } from '../types/auth';

export const configuredDashboard: DashboardConfigView = {
  currentNamesrv: '127.0.0.1:9876',
  namesrvAddrList: ['127.0.0.1:9876'],
  useVIPChannel: false,
  useTLS: true,
  currentProxyAddr: null,
  proxyAddrList: [],
  storageBackend: 'sqlite'
};

export const authenticatedSession: SessionView = {
  loginRequired: true,
  authenticated: true,
  username: 'operator',
  sessionId: 'test-session',
  loginTime: 1_700_000_000_000
};
