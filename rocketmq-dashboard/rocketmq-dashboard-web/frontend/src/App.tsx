import { Navigate, Route, Routes } from 'react-router-dom';
import AppLayout from './layouts/AppLayout';
import AclPage from './pages/AclPage';
import BrokerDetailPage from './pages/BrokerDetailPage';
import BrokerListPage from './pages/BrokerListPage';
import ConfigPage from './pages/ConfigPage';
import ConsumerDetailPage from './pages/ConsumerDetailPage';
import ConsumerListPage from './pages/ConsumerListPage';
import DashboardPage from './pages/DashboardPage';
import DlqMessagePage from './pages/DlqMessagePage';
import LoginPage from './pages/LoginPage';
import MessageQueryPage from './pages/MessageQueryPage';
import MessageTracePage from './pages/MessageTracePage';
import MonitorPage from './pages/MonitorPage';
import ProducerListPage from './pages/ProducerListPage';
import ProxyPage from './pages/ProxyPage';
import TopicDetailPage from './pages/TopicDetailPage';
import TopicListPage from './pages/TopicListPage';

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <AppLayout>
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
          </AppLayout>
        }
      />
    </Routes>
  );
}
