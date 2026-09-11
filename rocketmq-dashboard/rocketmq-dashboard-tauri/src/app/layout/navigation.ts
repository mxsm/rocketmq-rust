import { Activity, Database, FileText, Globe, LayoutDashboard, MonitorDot, Network, RefreshCw, Server, Shield, Users, UserRound, MessageSquare, Send, ScrollText } from 'lucide-react';
import type { Tab } from '../../stores/navigation';

export const navigationSections = [
    { title: 'Platform', items: [
        { tab: 'Dashboard', label: 'Dashboard', icon: LayoutDashboard },
        { tab: 'NameServer', label: 'NameServer', icon: Globe },
        { tab: 'Proxy', label: 'Proxy', icon: Network },
        { tab: 'Cluster', label: 'Cluster', icon: Server },
    ] },
    { title: 'Messaging', items: [
        { tab: 'Topic', label: 'Topics', icon: FileText },
        { tab: 'Consumer', label: 'Consumers', icon: Users },
        { tab: 'Producer', label: 'Producers', icon: Send },
        { tab: 'Message', label: 'Messages', icon: MessageSquare },
        { tab: 'MessageTrace', label: 'Trace', icon: Activity },
        { tab: 'DLQ', label: 'DLQ Message', icon: RefreshCw },
    ] },
    { title: 'Governance', items: [
        { tab: 'ACL', label: 'ACL', icon: Shield },
        { tab: 'Storage', label: 'Storage', icon: Database },
        { tab: 'Monitors', label: 'Monitors', icon: MonitorDot },
        { tab: 'Audit', label: 'Audit', icon: ScrollText },
        { tab: 'Sessions', label: 'Sessions', icon: UserRound },
    ] },
] as const;

export const pageDescriptions: Record<Tab, string> = {
    Dashboard: 'Operational overview of your RocketMQ cluster',
    NameServer: 'Manage routing endpoints and connection settings',
    Proxy: 'Manage endpoints for scoped Consumer queries',
    Cluster: 'Brokers, runtime status, and configuration',
    Topic: 'Manage topics, queues, and message operations',
    Consumer: 'Consumer groups, progress, and client diagnostics',
    Producer: 'Discover producer groups and inspect connections',
    Message: 'Find messages by key, ID, or time',
    MessageTrace: 'Follow message publication and consumption',
    DLQ: 'Inspect dead-letter messages and resend results',
    ACL: 'Broker users and resource policies',
    Storage: 'Local database information and collector diagnostics',
    Monitors: 'Consumer group thresholds for this environment',
    Audit: 'Review actions and their recorded outcomes',
    Sessions: 'Manage your local dashboard sessions',
    Account: 'Local account details and password settings',
};
