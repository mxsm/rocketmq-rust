import { invoke } from '@tauri-apps/api/core';
import type { NameServerHomePageResponse, NameServerMutationResponse } from '../features/nameserver/types/nameserver.types';

export class NameServerService {
    static async getHomePage(): Promise<NameServerHomePageResponse> {
        return invoke<NameServerHomePageResponse>('get_name_server_home_page');
    }

    static async addNameServer(address: string): Promise<NameServerMutationResponse> {
        return invoke<NameServerMutationResponse>('add_name_server', { address });
    }

    static async switchNameServer(address: string): Promise<NameServerMutationResponse> {
        return invoke<NameServerMutationResponse>('switch_name_server', { address });
    }

    static async deleteNameServer(address: string): Promise<NameServerMutationResponse> {
        return invoke<NameServerMutationResponse>('delete_name_server', { address });
    }

    static async updateVIPChannel(enabled: boolean): Promise<NameServerMutationResponse> {
        return invoke<NameServerMutationResponse>('update_vip_channel', { enabled });
    }

    static async updateUseTLS(enabled: boolean): Promise<NameServerMutationResponse> {
        return invoke<NameServerMutationResponse>('update_use_tls', { enabled });
    }
}
