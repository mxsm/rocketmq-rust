import { lazy, Suspense } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import RouteLoading from './components/RouteLoading';
import AppLayout from './layouts/AppLayout';
import { ConsumerQueryScopeProvider } from './pages/consumers/ConsumerQueryScopeProvider';

const AclPage = lazy(() => import('./pages/AclPage'));
const BrokerDetailPage = lazy(() => import('./pages/BrokerDetailPage'));
const BrokerListPage = lazy(() => import('./pages/BrokerListPage'));
const ConfigPage = lazy(() => import('./pages/ConfigPage'));
const ConsumerDetailPage = lazy(() => import('./pages/ConsumerDetailPage'));
const ConsumerListPage = lazy(() => import('./pages/ConsumerListPage'));
const DashboardPage = lazy(() => import('./pages/DashboardPage'));
const DlqMessagePage = lazy(() => import('./pages/DlqMessagePage'));
const LoginPage = lazy(() => import('./pages/LoginPage'));
const MessageQueryPage = lazy(() => import('./pages/MessageQueryPage'));
const MessageTracePage = lazy(() => import('./pages/MessageTracePage'));
const MonitorPage = lazy(() => import('./pages/MonitorPage'));
const ProducerListPage = lazy(() => import('./pages/ProducerListPage'));
const ProxyPage = lazy(() => import('./pages/ProxyPage'));
const TopicDetailPage = lazy(() => import('./pages/TopicDetailPage'));
const TopicListPage = lazy(() => import('./pages/TopicListPage'));

export default function App() {
  return (
    <Suspense fallback={<RouteLoading />}>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          path="/*"
          element={
            <AppLayout>
              <ConsumerQueryScopeProvider>
                <Routes>
                  <Route path="/" element={<Navigate to="/dashboard" replace />} />
                  <Route path="/ops" element={<Navigate to="/config" replace />} />
                  <Route path="/proxy" element={<ProxyPage />} />
                  <Route path="/dashboard" element={<DashboardPage />} />
                  <Route path="/cluster" element={<Navigate to="/brokers" replace />} />
                  <Route path="/topics" element={<TopicListPage />} />
                  <Route path="/topics/:topic" element={<TopicDetailPage />} />
                  <Route path="/consumers" element={<ConsumerListPage />} />
                  <Route path="/consumers/:group" element={<ConsumerDetailPage />} />
                  <Route path="/producers" element={<ProducerListPage />} />
                  <Route path="/brokers" element={<BrokerListPage />} />
                  <Route path="/brokers/:brokerName" element={<BrokerDetailPage />} />
                  <Route path="/messages" element={<MessageQueryPage />} />
                  <Route path="/messages/dlq" element={<DlqMessagePage />} />
                  <Route path="/dlq" element={<Navigate to="/messages/dlq" replace />} />
                  <Route path="/message-trace" element={<MessageTracePage />} />
                  <Route path="/acl" element={<AclPage />} />
                  <Route path="/monitors" element={<MonitorPage />} />
                  <Route path="/config" element={<ConfigPage />} />
                  <Route path="*" element={<Navigate to="/dashboard" replace />} />
                </Routes>
              </ConsumerQueryScopeProvider>
            </AppLayout>
          }
        />
      </Routes>
    </Suspense>
  );
}
