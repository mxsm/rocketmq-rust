import React, { useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../services/connection.store';
import {useAppStore} from '../../stores/app.store';
import {DashboardPage} from '../../pages/dashboard/DashboardPage';

// Import Views
import {ACLView} from '../../components/ACLView';
import {NameServerView} from '../../components/NameServerView';
import {ProxyView} from '../../components/ProxyView';
import {ClusterView} from '../../components/ClusterView';
import {TopicView} from '../../components/TopicView';
import {ConsumerView} from '../../components/ConsumerView';
import {ProducerView} from '../../components/ProducerView';
import {MessageView} from '../../components/MessageView';
import {MessageTraceView} from '../../components/MessageTraceView';
import {DLQMessageView} from '../../components/DLQMessageView';
import {Activity} from 'lucide-react';
import {MonitorPage} from '../../pages/monitor/MonitorPage';
import {AuditPage} from '../../pages/audit/AuditPage';
import {SessionsPanel} from '../../pages/account/SessionsPanel';
import {AccountPage} from '../../pages/account/AccountPage';

export const AppRouter = () => {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const { activeTab, navigation, canGoBack, goBack } = useAppStore();
    const key = ['NameServer', 'Proxy', 'Account', 'Sessions', 'Audit'].includes(activeTab) ? activeTab : `${activeTab}:${settings?.revision ?? 0}`;
    return <><div className="flex items-center gap-3 px-6 py-2">
        {canGoBack && <button type="button" className="text-sm underline" onClick={goBack}>Back</button>}
        {navigation.target && <span className="text-sm text-gray-500">{navigation.target.kind}: {'name' in navigation.target ? navigation.target.name : navigation.target.address}</span>}
    </div><RouteContent key={`${key}:${navigation.id}`} /></>;
};

const RouteContent = () => {
    const {activeTab, currentUser} = useAppStore();

    switch (activeTab) {
        case 'NameServer':
            return <NameServerView/>;
        case 'Proxy':
            return <ProxyView/>;
        case 'Dashboard':
            return <DashboardPage/>;
        case 'Cluster':
            return <ClusterView/>;
        case 'Topic':
            return <TopicView/>;
        case 'Consumer':
            return <ConsumerView/>;
        case 'Producer':
            return <ProducerView/>;
        case 'Message':
            return <MessageView/>;
        case 'MessageTrace':
            return <MessageTraceView/>;
        case 'DLQ':
            return <DLQMessageView/>;
        case 'ACL':
            return <ACLView/>;
        case 'Monitors':
            return <MonitorPage/>;
        case 'Audit':
            return <AuditPage/>;
        case 'Sessions':
            return currentUser ? <SessionsPanel username={currentUser.username}/> : null;
        case 'Account':
            return <AccountPage/>;
        default:
            return (
                <div className="flex flex-col items-center justify-center h-full text-gray-400">
                    <Activity className="w-12 h-12 mb-4 opacity-20"/>
                    <p className="text-lg font-medium">Coming Soon</p>
                    <p className="text-sm">The {activeTab} view is under development.</p>
                </div>
            );
    }
};
