import React from 'react';
import {useAppStore} from '../../stores/app.store';
import {DashboardPage} from '../../pages/dashboard/DashboardPage';

// Import Views
import {ACLView} from '../../components/ACLView';
import {OpsView} from '../../components/OpsView';
import {ProxyView} from '../../components/ProxyView';
import {ClusterView} from '../../components/ClusterView';
import {TopicView} from '../../components/TopicView';
import {ConsumerView} from '../../components/ConsumerView';
import {ProducerView} from '../../components/ProducerView';
import {MessageView} from '../../components/MessageView';
import {MessageTraceView} from '../../components/MessageTraceView';
import {DLQMessageView} from '../../components/DLQMessageView';
import {Activity} from 'lucide-react';

export const AppRouter = () => {
    const {activeTab} = useAppStore();

    switch (activeTab) {
        case 'OPS':
            return <OpsView/>;
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
