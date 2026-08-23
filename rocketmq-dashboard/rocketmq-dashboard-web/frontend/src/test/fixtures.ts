import type { DashboardConfigView } from '../types/config';
import type { SessionView } from '../types/auth';

export const configuredDashboard: DashboardConfigView = {
  environmentId: '00000000-0000-7000-8000-000000000001',
  environmentName: 'Default',
  revision: 1,
  endpoints: [
    { endpointId: 'nameserver-1', endpointType: 'nameserver', address: '127.0.0.1:9876', role: 'primary', isEnabled: true, isActive: true, sortOrder: 0 }
  ],
  currentNamesrv: '127.0.0.1:9876',
  namesrvAddrList: ['127.0.0.1:9876'],
  useVIPChannel: false,
  useTLS: true,
  currentProxyAddr: null,
  proxyAddrList: [],
  storageBackend: 'sqlite',
  storageMode: 'singleNode'
};

export const authenticatedSession: SessionView = {
  loginRequired: true,
  authenticated: true,
  username: 'operator',
  sessionId: 'test-session',
  loginTime: 1_700_000_000_000
};
