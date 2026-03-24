import { invoke } from '@tauri-apps/api/core';
import type {
    NameServerHomePageInfo,
    NameServerMutationResult,
} from '../features/nameserver/types/nameserver.types';

export class NameServerService {
    static async getHomePageInfo(): Promise<NameServerHomePageInfo> {
        return invoke<NameServerHomePageInfo>('get_name_server_home_page');
    }

    static async addNameServer(address: string): Promise<NameServerMutationResult> {
        return invoke<NameServerMutationResult>('add_name_server', { address });
    }

    static async switchNameServer(address: string): Promise<NameServerMutationResult> {
        return invoke<NameServerMutationResult>('switch_name_server', { address });
    }

    static async deleteNameServer(address: string): Promise<NameServerMutationResult> {
        return invoke<NameServerMutationResult>('delete_name_server', { address });
    }

    static async updateVipChannel(enabled: boolean): Promise<NameServerMutationResult> {
        return invoke<NameServerMutationResult>('update_vip_channel', { enabled });
    }

    static async updateUseTls(enabled: boolean): Promise<NameServerMutationResult> {
        return invoke<NameServerMutationResult>('update_use_tls', { enabled });
    }
}
