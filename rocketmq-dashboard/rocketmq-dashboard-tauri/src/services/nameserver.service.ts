import { invokeAuthenticatedCommand } from './invoke';
import type {
    NameServerHomePageInfo,
    NameServerMutationResult,
} from '../features/nameserver/types/nameserver.types';

export class NameServerService {
    static async getHomePageInfo(): Promise<NameServerHomePageInfo> {
        return invokeAuthenticatedCommand<NameServerHomePageInfo>('get_name_server_home_page');
    }

    static async addNameServer(address: string, expectedRevision: number): Promise<NameServerMutationResult> {
        return invokeAuthenticatedCommand<NameServerMutationResult>('add_name_server', { address, expectedRevision });
    }

    static async switchNameServer(address: string, expectedRevision: number): Promise<NameServerMutationResult> {
        return invokeAuthenticatedCommand<NameServerMutationResult>('switch_name_server', { address, expectedRevision });
    }

    static async deleteNameServer(address: string, expectedRevision: number): Promise<NameServerMutationResult> {
        return invokeAuthenticatedCommand<NameServerMutationResult>('delete_name_server', { address, expectedRevision });
    }

    static async updateVipChannel(enabled: boolean, expectedRevision: number): Promise<NameServerMutationResult> {
        return invokeAuthenticatedCommand<NameServerMutationResult>('update_vip_channel', { enabled, expectedRevision });
    }

    static async updateUseTls(enabled: boolean, expectedRevision: number): Promise<NameServerMutationResult> {
        return invokeAuthenticatedCommand<NameServerMutationResult>('update_use_tls', { enabled, expectedRevision });
    }
}
