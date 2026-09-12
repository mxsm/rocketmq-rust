import { invokeAuthenticatedCommand } from './invoke';

export interface MonitorRule {
    consumerGroup: string;
    minCount: number;
    maxDiffTotal: number;
    revision: number;
    createdAtMs: number;
    updatedAtMs: number;
}
export interface SaveMonitorRule {
    consumerGroup: string;
    minCount: number;
    maxDiffTotal: number;
    expectedRevision: number;
}
export const MonitorService = {
    list: (): Promise<MonitorRule[]> => invokeAuthenticatedCommand('list_consumer_monitor_rules'),
    save: (request: SaveMonitorRule): Promise<{ message: string }> => invokeAuthenticatedCommand('save_consumer_monitor_rule', { request }),
    delete: (rule: Pick<MonitorRule, 'consumerGroup' | 'revision'>): Promise<{ message: string }> => invokeAuthenticatedCommand('delete_consumer_monitor_rule', { request: { consumerGroup: rule.consumerGroup, expectedRevision: rule.revision } }),
};
