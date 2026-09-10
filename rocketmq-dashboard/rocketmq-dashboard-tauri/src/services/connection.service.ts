import { invokeAuthenticatedCommand } from './invoke';
import type { ConnectionSettingsView } from './connection.store';
export interface ConnectionMutationResult { message: string; settings: ConnectionSettingsView }
export type NameServerSelection = { kind: 'existing_id' | 'address'; value: string };
export const getConnectionSettings = () => invokeAuthenticatedCommand<ConnectionSettingsView>('get_connection_settings');
export const replaceNameServers = (addresses: string[], currentEndpoint: NameServerSelection | null, expectedRevision: number) =>
    invokeAuthenticatedCommand<ConnectionMutationResult>('replace_name_servers', { request: { addresses, currentEndpoint, expectedRevision } });
